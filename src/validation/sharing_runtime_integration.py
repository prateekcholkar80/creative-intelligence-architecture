"""4.3 Critical/Must have/shared pilot: owned native BQ/regional-secret probe.

--run creates one expiring fixture dataset and one expiring regional secret, then
verifies cleanup. Production adapters execute with only fixture SQL references
relocated and explicit fixture IAP identities. No serving writes, IAM edits,
service enablement, model calls, deployed IAP proof or shared activation.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import secrets
import time
from unittest.mock import Mock, patch
from uuid import uuid4

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.cloud import bigquery, secretmanager_v1 as sm
import google_crc32c

from src.application import sharing_publication_reader as metadata
from src.application import sharing_query as query_module
from src.application.access_policy import AuthorizationError, Visibility
from src.application.access_reader import BigQueryAccessReader
from src.application.identity import ISSUER, IapIdentityAdapter, IapSettings, VerifiedIdentity, principal_for_subject
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_executor import SharingExecutorSettings
from src.application.sharing_intent import initial_attempt
from src.application.sharing_publication_codec import encode_publication, normalize
from src.application.sharing_publication_contract import (
    POLICY_ID, SigningPublicationStatus, initial_publication, propose_publication,
)
from src.application.sharing_publication_read_contract import SigningReadLimits
from src.application.sharing_publication_schema import TABLES as SIGNING_TABLES, STORAGE_VERSION
from src.application.sharing_refresh_contract import SigningRefreshSettings, SigningReplicaPhase
from src.application.sharing_retry import prepare_retry
from src.application.sharing_coordinator_contract import SharingCommitObservation
from src.application.sharing_runtime import RuntimeSharingSealer, SigningRuntimeError, SigningRuntimeSettings
from src.application.sharing_runtime_executor import RuntimeProtectedSharingExecutor
from src.application.sharing_secret_loader import REGIONAL_ENDPOINT, SharingSecretConfig, SharingSecretLoader, SharingSecretVersion
from src.application.sharing_signing_contract import SharingSealProfile, SharingSigningKey, SharingSigningPolicy
from src.application.workflow_contract import RETRY_WINDOW
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, DATASET_ID, _schema, _partition, validate_dataset
from src.validation.signing_read_integration import TTL_MS, json_rows, require
from src.validation.sharing_integration import FIXTURE_TABLES, fixture_rows, sharing_query

TABLES = dict(SIGNING_TABLES) | {n: WORKFLOW_TABLES[n] for n in FIXTURE_TABLES}
PROJECT_URL = f'https://cloudresourcemanager.googleapis.com/v1/projects/{PROJECT}'
PERMISSIONS = ('secretmanager.secrets.get', 'secretmanager.secrets.delete', 'secretmanager.versions.add',
               'secretmanager.versions.access', 'secretmanager.versions.disable', 'secretmanager.versions.enable')
INSTALLED_READ_SQL, INSTALLED_SHARING_SQL = metadata.read_sql, query_module._render_sql


class ProjectBindingError(RuntimeError):
    """Provider status/reason only; never expose response text or credentials."""
    def __init__(self, status, reason, service=None, consumer=None):
        super().__init__('PROJECT_BINDING_UNAVAILABLE')
        self.status, self.reason = status, reason
        self.service, self.consumer = service, consumer


def now():
    return datetime.now(timezone.utc)


def read_project_number(session):
    """Fixed ADC REST read; no CLI, configuration-file or caller-number fallback."""
    require(isinstance(session, AuthorizedSession), 'INVALID_PROJECT_BINDING_SESSION')
    with session.get(PROJECT_URL, timeout=10, allow_redirects=False, stream=True) as response:
        data = bytearray()
        for chunk in response.iter_content(chunk_size=4096):
            data.extend(chunk)
            require(len(data) <= 16384, 'PROJECT_BINDING_RESPONSE_TOO_LARGE')
        if response.status_code != 200:
            reason = service = consumer = None
            try:
                failure = json.loads(data)
                for detail in failure.get('error', {}).get('details', []):
                    if detail.get('@type') == 'type.googleapis.com/google.rpc.ErrorInfo':
                        candidate = detail.get('reason')
                        if type(candidate) is str and re.fullmatch(r'[A-Z_]{1,64}', candidate):
                            reason = candidate
                        fields = detail.get('metadata', {})
                        if fields.get('service') == 'cloudresourcemanager.googleapis.com':
                            service = fields['service']
                        candidate = fields.get('consumer')
                        if type(candidate) is str and re.fullmatch(r'projects/[1-9][0-9]{0,19}', candidate):
                            consumer = candidate
            except (ValueError, AttributeError, TypeError):
                pass
            raise ProjectBindingError(response.status_code, reason, service, consumer)
        body = json.loads(data)
    require(type(body) is dict and body.get('projectId') == PROJECT and body.get('lifecycleState') == 'ACTIVE'
            and type(body.get('projectNumber')) is str
            and re.fullmatch(r'[1-9][0-9]{0,19}', body['projectNumber']), 'PROJECT_BINDING_MISMATCH')
    return body['projectNumber']


@dataclass(frozen=True)
class ProbeScope:
    run_id: str

    def __post_init__(self):
        require(type(self.run_id) is str and re.fullmatch(r'[0-9a-f]{32}', self.run_id), 'INVALID_PROBE_SCOPE')

    @property
    def dataset(self):
        return f'{PROJECT}.ci_sharing_runtime_it_{self.run_id}'

    @property
    def labels(self):
        return dict(purpose='sharing_runtime_integration', run_id=self.run_id)

    def table(self, name):
        require(name in TABLES, 'UNRELATED_PROBE_TABLE')
        return self.dataset + '.' + name

    def secret(self, number):
        require(type(number) is str and re.fullmatch(r'[1-9][0-9]{0,19}', number), 'INVALID_PROJECT_NUMBER')
        return f'projects/{number}/locations/{LOCATION}/secrets/ci-sharing-runtime-it-{self.run_id}'

    def assert_sql(self, sql):
        refs = re.findall(r'`([^`]+)`', sql)
        require(bool(refs) and all(r in {self.table(n) for n in TABLES} for r in refs), 'UNSAFE_PROBE_SQL_SCOPE')

    def relocate(self, sql):
        for name in TABLES:
            sql = sql.replace(f'`{DATASET_ID}.{name}`', f'`{self.table(name)}`')
        self.assert_sql(sql)
        return sql

    def assert_dataset(self, actual):
        require(actual.project == PROJECT and f'{actual.project}.{actual.dataset_id}' == self.dataset
                and actual.location.lower() == LOCATION and actual.labels == self.labels
                and actual.default_table_expiration_ms == TTL_MS, 'PROBE_OWNERSHIP_MISMATCH')

    def assert_secret(self, actual, number, started):
        require(isinstance(actual, sm.Secret) and actual.name == self.secret(number)
                and dict(actual.labels) == self.labels and started <= actual.create_time
                # The server computes expire_time from TTL before it stamps create_time.
                # Permit only a shorter lifetime within the bounded create RPC window.
                and timedelta(milliseconds=TTL_MS)-timedelta(seconds=10)
                    <= actual.expire_time - actual.create_time <= timedelta(milliseconds=TTL_MS)
                and not actual.version_aliases and not actual.version_destroy_ttl,
                'SECRET_OWNERSHIP_MISMATCH')


class Probe:
    def __init__(self, client, secret_client, session):
        require(isinstance(client, bigquery.Client) and client.project == PROJECT and client.location == LOCATION
                and client.default_query_job_config is None, 'INVALID_PROBE_CLIENT')
        require(isinstance(secret_client, sm.SecretManagerServiceClient)
                and secret_client.api_endpoint == REGIONAL_ENDPOINT, 'INVALID_SECRET_PROBE_CLIENT')
        self.client, self.sm, self.session = client, secret_client, session
        self.scope, self.started = ProbeScope(uuid4().hex), now()
        self.number = None
        self.dataset_attempted = self.secret_attempted = False
        self.counter = 0
        self.runtimes = []
        self.pubs, self.sentinels = (), {}
        self.native_query = client.query
        self.native_get_job = client.get_job
        self.secret_access = secret_client.access_secret_version
        self.access_names = []
        self.report = dict(step='4.3', severity='Critical', necessity='Must have', scenario='shared pilot',
            project=PROJECT, location=LOCATION, fixture_dataset=self.scope.dataset, status='RUNNING',
            started_at=self.started.isoformat(),
            stage='PREFLIGHT', cleanup_dataset='NOT_CREATED', cleanup_secret='NOT_CREATED',
            serving_row_writes=0, iam_changes=0, service_enablement=0, execution_activated=False,
            scope='NATIVE_RUNTIME_BQ_SECRET_MANAGER_WITH_FIXTURE_IAP', cases=[], jobs=[])
        self.dispatch_ids = []

    def record(self, name, **details):
        self.report['cases'].append(dict(case=name, status='PASSED', **details))
        print(json.dumps(dict(event='sharing_runtime_probe_case', case=name, status='PASSED')), flush=True)

    def own(self):
        self.scope.assert_dataset(self.client.get_dataset(self.scope.dataset, retry=None, timeout=10))

    def own_secret(self):
        actual = self.sm.get_secret(request={'name': self.scope.secret(self.number)}, retry=None, timeout=10)
        self.scope.assert_secret(actual, self.number, self.started)
        return actual

    def preflight(self, names):
        self.own()
        for name in names:
            expected, actual = TABLES[name].table(PROJECT), self.client.get_table(self.scope.table(name), retry=None, timeout=10)
            require(_schema(actual.schema) == _schema(expected.schema)
                    and actual.project == PROJECT and actual.dataset_id == self.scope.dataset.split('.')[1]
                    and actual.table_id == name and actual.table_type == 'TABLE'
                    and actual.location.lower() == LOCATION and actual.clustering_fields == expected.clustering_fields
                    and _partition(actual) == _partition(expected)
                    and bool(actual.require_partition_filter) == bool(expected.require_partition_filter)
                    and actual.expires is not None and actual.range_partitioning is None
                    and actual.external_data_configuration is None and actual.table_constraints is None,
                    'FIXTURE_SCHEMA_MISMATCH')
        return {'status': 'VERIFIED'}

    def job_id(self):
        self.counter += 1
        return f'sharing_runtime_it_{self.scope.run_id}_{self.counter}'

    def execute(self, sql):
        self.own()
        self.scope.assert_sql(sql)
        job = self.client.query(sql, job_id=self.job_id(), project=PROJECT, location=LOCATION,
            retry=None, job_retry=None, timeout=10, job_config=bigquery.QueryJobConfig(use_legacy_sql=False,
                use_query_cache=False, maximum_bytes_billed=512*1024**2, job_timeout_ms=60000))
        rows = list(job.result(timeout=60, retry=None, job_retry=None))
        require(job.state == 'DONE' and job.error_result is None, 'FIXTURE_QUERY_FAILED')
        self.report['jobs'].append(dict(job_id=job.job_id, bytes_billed=job.total_bytes_billed))
        return [normalize(row) for row in rows]

    def load(self, name, rows):
        self.own()
        target = self.client.get_table(self.scope.table(name), retry=None, timeout=10)
        require(target.project == PROJECT and target.dataset_id == self.scope.dataset.split('.')[1]
                and target.table_id == name and _schema(target.schema) == _schema(TABLES[name].schema),
                'FIXTURE_SCHEMA_MISMATCH')
        job = self.client.load_table_from_json(json_rows(rows), target, job_id=self.job_id(), project=PROJECT,
            location=LOCATION, timeout=10, num_retries=0,
            job_config=bigquery.LoadJobConfig(schema=target.schema, write_disposition='WRITE_TRUNCATE'))
        job.result(timeout=60, retry=None)
        require(job.state == 'DONE' and job.error_result is None, 'FIXTURE_LOAD_FAILED')

    def checkpoint_rows(self):
        head = self.pubs[-1]
        return [dict(schema_version=STORAGE_VERSION, policy_id=POLICY_ID, consumer_id='fixture-consumer',
            checkpoint_revision=head.revision, publication_revision=head.revision, publication_digest=head.digest,
            instance_id='fixture-writer', updated_at=head.published_at, writer_id='fixture-operator',
            command_id=f'fixture-floor-{head.revision}')]

    def persist(self):
        rows = {n: [] for n in SIGNING_TABLES}
        for pub in self.pubs:
            body, history = encode_publication(pub, publisher_id='fixture-operator',
                command_id=f'fixture-publish-{pub.revision}', recorded_at=pub.published_at)
            rows['signing_publications'].append(body)
            rows['signing_key_history'].extend(history)
        head = self.pubs[-1]
        rows['signing_heads'] = [dict(schema_version=STORAGE_VERSION, policy_id=POLICY_ID,
            revision=head.revision, publication_digest=head.digest, control_revision=head.revision,
            published_at=head.published_at, publisher_id='fixture-operator', command_id='fixture-head')]
        rows['signing_checkpoints'] = self.checkpoint_rows()
        if not self.sentinels:
            self.sentinels = {n: [dict(r, policy_id='fixture-unrelated-policy') for r in group]
                              for n, group in rows.items()}
        for name, group in rows.items():
            self.load(name, group + self.sentinels[name])

    def publish(self, **changes):
        head = propose_publication(self.pubs[-1], published_at=now(), **changes)
        self.pubs += (head,)
        self.persist()
        return head

    def setup(self):
        self.report['stage'] = 'ADC_PROJECT_BINDING'
        self.number = read_project_number(self.session)
        self.report.update(project_number=self.number, fixture_secret=self.scope.secret(self.number))
        validate_dataset(self.client.get_dataset(DATASET_ID, retry=None, timeout=10))
        self.record('adc_project_binding_and_bigquery_region_verified')
        self.report['stage'] = 'OWNED_RESOURCES'
        data = bigquery.Dataset(self.scope.dataset)
        data.location, data.labels, data.default_table_expiration_ms = LOCATION, self.scope.labels, TTL_MS
        self.dataset_attempted = True
        self.client.create_dataset(data, exists_ok=False, retry=None, timeout=10)
        self.report['cleanup_dataset'] = 'PENDING'
        self.own()
        for name, spec in TABLES.items():
            template = spec.table(PROJECT)
            table = bigquery.Table(self.scope.table(name), schema=template.schema)
            table.clustering_fields, table.time_partitioning = template.clustering_fields, template.time_partitioning
            table.require_partition_filter = template.require_partition_filter
            self.client.create_table(table, exists_ok=False, retry=None, timeout=10)
        self.preflight(TABLES)
        self.secret_attempted = True
        self.sm.create_secret(request=dict(parent=f'projects/{self.number}/locations/{LOCATION}',
            secret_id=self.scope.secret(self.number).split('/')[-1], secret=sm.Secret(labels=self.scope.labels,
                ttl={'seconds': TTL_MS//1000})), retry=None, timeout=10)
        self.report['cleanup_secret'] = 'PENDING'
        self.own_secret()
        pins = []
        for key_id in ('old', 'new'):
            payload = secrets.token_bytes(32)
            version = self.sm.add_secret_version(request=dict(parent=self.scope.secret(self.number),
                payload=sm.SecretPayload(data=payload, data_crc32c=google_crc32c.value(payload))), retry=None, timeout=10)
            require(version.name.startswith(self.scope.secret(self.number) + '/versions/')
                    and re.fullmatch(r'[1-9][0-9]*', version.name.split('/')[-1])
                    and version.state == sm.SecretVersion.State.ENABLED, 'SECRET_VERSION_MISMATCH')
            pins.append(SharingSecretVersion(key_id, self.scope.secret(self.number).split('/')[-1],
                                             int(version.name.split('/')[-1])))
        permissions = self.sm.test_iam_permissions(request=dict(resource=self.scope.secret(self.number),
            permissions=PERMISSIONS), retry=None, timeout=10)
        require(set(permissions.permissions) == set(PERMISSIONS), 'SECRET_PERMISSIONS_MISSING')
        self.record('regional_owned_secret_numeric_versions_and_adc_permissions', permissions=list(PERMISSIONS))
        at = now()
        self.cutover = at + timedelta(seconds=120)
        old = SharingSigningKey('old', 'fixture-profile', at-timedelta(days=1), self.cutover, self.cutover+RETRY_WINDOW)
        new = SharingSigningKey('new', 'fixture-profile', self.cutover, at+timedelta(days=1), at+timedelta(days=1)+RETRY_WINDOW)
        self.settings = query_module.SharingQuerySettings(maximum_bytes_billed=512*1024**2)
        profile = SharingSealProfile.from_runtime('fixture-profile', self.settings)
        self.config = SharingSecretConfig(SharingSigningPolicy('old', (old, new), (profile,)), self.number, tuple(pins))
        self.pubs = (initial_publication(self.config, published_at=at),)
        self.persist()
        for name in FIXTURE_TABLES[:3]:
            self.load(name, [fixture_rows(case, at)[name] for case in ('apply', 'sentinel')])

    def runtime(self):
        runtime = RuntimeSharingSealer(reader=metadata.BigQuerySigningReader(client=self.client,
            limits=SigningReadLimits(max_age_seconds=120)), loader=SharingSecretLoader(client=self.sm),
            instance_id='native-' + uuid4().hex,
            settings=SigningRuntimeSettings('fixture-consumer', self.number, enabled=True,
                                           refresh=SigningRefreshSettings(120)))
        self.runtimes.append(runtime)
        return runtime

    def identity(self):
        subject = 'native-sharing-fixture:apply'
        audience = f'/projects/{self.number}/locations/{LOCATION}/services/fixture-runtime'
        epoch = int(now().timestamp())
        return VerifiedIdentity(principal_for_subject(ISSUER, subject), ISSUER, subject, audience, epoch-1, epoch+599)

    def executor(self, runtime):
        identity = self.identity()
        verifier = IapIdentityAdapter(IapSettings(identity.audience))
        verifier.authenticate = Mock(side_effect=lambda headers: self.identity())
        return RuntimeProtectedSharingExecutor(client=self.client, reader=BigQueryAccessReader(self.client),
            sealer=runtime, verifier=verifier, settings=SharingExecutorSettings(execution_enabled=True))

    def deny(self, function, *, code=None):
        try:
            function()
        except (SigningRuntimeError, AuthorizationError) as error:
            require(code is None or error.code == code, 'UNEXPECTED_RUNTIME_DENIAL')
            return
        raise RuntimeError('EXPECTED_RUNTIME_DENIAL')

    def version_state(self, key_id, *, enabled):
        self.own_secret()
        binding = next(v for v in self.config.versions if v.key_id == key_id)
        name = self.config.name(binding)
        method = self.sm.enable_secret_version if enabled else self.sm.disable_secret_version
        version = self.sm.get_secret_version(request={'name': name}, retry=None, timeout=10)
        changed = method(request=dict(name=name, etag=version.etag), retry=None, timeout=10)
        require(changed.name == name and changed.state == (sm.SecretVersion.State.ENABLED if enabled
                else sm.SecretVersion.State.DISABLED), 'SECRET_STATE_CHANGE_FAILED')

    def cases(self):
        self.report['stage'] = 'NATIVE_RUNTIME'
        runtime = self.runtime()
        runtime.refresh()
        self.record('cold_authoritative_checkpoint_and_pinned_key_load')
        query = replace(sharing_query('apply', retry_key=self.scope.run_id), settings=self.settings)
        token = runtime.seal(query)
        original = initial_attempt(query)
        restarted = self.runtime()
        require(restarted.resolve(token, self.identity()) == original, 'RESTART_TOKEN_CHANGED')
        self.record('restart_original_token_and_job_id_preserved')
        self.load('signing_checkpoints', self.sentinels['signing_checkpoints'])
        self.deny(runtime.refresh, code='SIGNING_RUNTIME_CHECKPOINT_REQUIRED')
        require(runtime._sealer is None, 'MISSING_CHECKPOINT_USED_OLD_KEYS')
        self.load('signing_checkpoints', self.checkpoint_rows()+self.sentinels['signing_checkpoints'])
        self.record('missing_durable_floor_denies_without_cache')
        print(json.dumps(dict(event='sharing_runtime_probe_waiting_for_planned_cutover',
                              seconds=max(0, int((self.cutover-now()).total_seconds())))), flush=True)
        while now() < self.cutover:
            time.sleep(min(2, max(0, (self.cutover-now()).total_seconds())))
        rotated_config = replace(self.config, policy=replace(self.config.policy, active_key_id='new'))
        self.publish(config=rotated_config, status=SigningPublicationStatus.ACTIVE)
        require(restarted.resolve(token, self.identity()) == original, 'ROTATED_ORIGINAL_CHANGED')
        self.record('native_planned_rotation_retains_original_cohort')
        # Supplied rollback here only exercises token format/retention; native
        # retry admission was proved separately and is never claimed by this case.
        retry = prepare_retry(original, commit_observation=SharingCommitObservation(query.command.audit_id,
            SharingCommitStatus.NOT_COMMITTED, original.job_id), expected_workspace_revision=1, observed_at=now())
        require(restarted.resolve_retry(restarted.seal_retry(retry), self.identity()) == retry, 'RETAINED_RETRY_CHANGED')
        self.record('retained_retry_seal_roundtrip_without_dispatch')
        executor = self.executor(restarted)
        lost = False
        def lose_once(sql, **kwargs):
            nonlocal lost
            job = self.guarded_query(sql, **kwargs)
            if kwargs.get('job_id') == original.job_id and not lost:
                lost = True
                raise ServiceUnavailable('FIXTURE_DISCARDED_RESPONSE')
            return job
        with patch.object(self.client, 'query', side_effect=lose_once):
            executor.submit({}, token)
        parent = self.native_get_job(original.job_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        parent.result(timeout=60, retry=None, job_retry=None)
        recovered = executor.recover({}, token)
        require(recovered.observation.status is SharingCommitStatus.COMMITTED and recovered.result is not None,
                'ORIGINAL_JOB_RECOVERY_FAILED')
        require(self.dispatch_ids == [original.job_id], 'RECOVERY_RESUBMITTED_MUTATION')
        self.report['jobs'].append(dict(job_id=parent.job_id, bytes_billed=parent.total_bytes_billed,
                                       native_status=recovered.observation.status.value))
        self.record('native_dispatch_discarded_response_original_job_recovery')
        self.version_state('old', enabled=False)
        self.deny(restarted.refresh)
        require(restarted._sealer is None and restarted.state.head.digest == self.pubs[-1].digest,
                'DISABLED_SECRET_USED_CACHE_OR_LOST_FLOOR')
        self.version_state('old', enabled=True)
        require(restarted.resolve(token, self.identity()) == original, 'FRESH_SECRET_RELOAD_FAILED')
        self.record('disabled_version_denies_then_fresh_reload_recovers')
        self.version_state('old', enabled=False)
        cfg = replace(rotated_config, policy=replace(rotated_config.policy,
            keys=tuple(replace(k, revoked=True) if k.key_id == 'old' else k for k in rotated_config.policy.keys)))
        self.publish(config=cfg, status=SigningPublicationStatus.ACTIVE)
        before = len(self.access_names)
        self.deny(lambda: executor.recover({}, token), code='SHARING_SIGNING_KEY_REVOKED')
        require(all(not n.endswith(f'/versions/{self.config.versions[0].version}')
                    for n in self.access_names[before:]), 'REVOKED_SECRET_WAS_ACCESSED')
        self.record('revoked_retained_token_denied_disabled_old_version_not_fetched')
        current_token = executor.issue_intent({}, workspace_id='it_apply', conversation_id='conversation_apply',
            visibility=Visibility.PRIVATE, expected_membership_revision=1, expected_conversation_revision=2,
            expected_workspace_revision=2, retry_key=self.scope.run_id+'-new')
        current = restarted.resolve(current_token, self.identity())
        self.record('new_active_cohort_issues_after_old_revocation')
        def remove_access(job_id, **kwargs):
            if job_id == current.job_id:
                self.execute(f"UPDATE `{self.scope.table('memberships')}` SET status='REVOKED', revision=2, "
                    "updated_at=CURRENT_TIMESTAMP() WHERE workspace_id='it_apply'")
            return self.native_get_job(job_id, **kwargs)
        with patch.object(self.client, 'get_job', side_effect=remove_access):
            self.deny(lambda: executor.recover({}, current_token))
        self.execute(f"UPDATE `{self.scope.table('memberships')}` SET status='ACTIVE', revision=3, "
            "updated_at=CURRENT_TIMESTAMP() WHERE workspace_id='it_apply'")
        self.record('native_access_revocation_during_recovery_denies_release')
        self.publish(config=cfg, status=SigningPublicationStatus.STOPPED)
        before = len(self.access_names)
        self.deny(lambda: executor.recover({}, current_token), code='SIGNING_REPLICA_NOT_READY')
        require(restarted.state.phase is SigningReplicaPhase.STOPPED and len(self.access_names) == before,
                'STOPPED_POLICY_LOADED_KEYS')
        self.record('native_policy_stop_denies_without_secret_access')
        for name, expected in self.sentinels.items():
            rows = self.execute(f"SELECT * FROM `{self.scope.table(name)}` WHERE policy_id='fixture-unrelated-policy'")
            require(sorted(rows, key=lambda r:json.dumps(json_rows([r]), sort_keys=True)) ==
                    sorted(expected, key=lambda r:json.dumps(json_rows([r]), sort_keys=True)),
                    'UNRELATED_CONTROL_FIXTURE_CHANGED')
        for name in FIXTURE_TABLES[:3]:
            rows = self.execute(f"SELECT * FROM `{self.scope.table(name)}` WHERE workspace_id='it_sentinel'")
            require(rows == [fixture_rows('sentinel', self.pubs[0].published_at)[name]], 'UNRELATED_BUSINESS_FIXTURE_CHANGED')
        self.record('unrelated_fixture_rows_preserved')

    def guarded_query(self, sql, **kwargs):
        self.own()
        sql = self.scope.relocate(sql)
        if sql == self.scope.relocate(INSTALLED_SHARING_SQL()):
            self.dispatch_ids.append(kwargs.get('job_id'))
        return self.native_query(sql, **kwargs)

    def guarded_access(self, *, request, **kwargs):
        require(request['name'] in {self.config.name(v) for v in self.config.versions}, 'UNOWNED_SECRET_ACCESS')
        self.access_names.append(request['name'])
        return self.secret_access(request=request, **kwargs)

    def run(self):
        with ExitStack() as stack:
            stack.enter_context(patch.object(metadata, 'read_sql', side_effect=lambda:self.scope.relocate(INSTALLED_READ_SQL())))
            stack.enter_context(patch.object(metadata, 'inspect', side_effect=lambda client:self.preflight(SIGNING_TABLES)))
            stack.enter_context(patch.object(query_module, '_render_sql', side_effect=lambda:self.scope.relocate(INSTALLED_SHARING_SQL())))
            stack.enter_context(patch.object(RuntimeProtectedSharingExecutor, '_schema_preflight', side_effect=lambda:self.preflight(FIXTURE_TABLES)))
            stack.enter_context(patch.object(self.client, 'query', side_effect=self.guarded_query))
            stack.enter_context(patch.object(self.sm, 'access_secret_version', side_effect=self.guarded_access))
            self.cases()
        self.report['secret_accesses'] = len(self.access_names)

    def cleanup(self):
        for runtime in self.runtimes:
            runtime.close()
        errors = []
        if self.dataset_attempted:
            try:
                try:self.own()
                except NotFound:pass
                else:self.client.delete_dataset(self.scope.dataset, delete_contents=True, retry=None, timeout=10)
                try:self.client.get_dataset(self.scope.dataset, retry=None, timeout=10)
                except NotFound:self.report['cleanup_dataset'] = 'DELETED_OR_ABSENT'
                else:raise RuntimeError('DATASET_CLEANUP_NOT_CONFIRMED')
            except Exception as error:
                self.report['cleanup_dataset'] = 'REFUSED_OR_FAILED'
                errors.append(error)
        if self.secret_attempted:
            try:
                try:actual = self.own_secret()
                except NotFound:pass
                else:self.sm.delete_secret(request=dict(name=actual.name, etag=actual.etag), retry=None, timeout=10)
                try:self.sm.get_secret(request={'name':self.scope.secret(self.number)}, retry=None, timeout=10)
                except NotFound:self.report['cleanup_secret'] = 'DELETED_OR_ABSENT'
                else:raise RuntimeError('SECRET_CLEANUP_NOT_CONFIRMED')
            except Exception as error:
                self.report['cleanup_secret'] = 'REFUSED_OR_FAILED'
                errors.append(error)
        if errors:raise RuntimeError('PROBE_CLEANUP_FAILED')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    with AuthorizedSession(credentials, max_refresh_attempts=0, refresh_timeout=10) as session, \
         bigquery.Client(project=PROJECT, location=LOCATION, credentials=credentials) as client:
        secret_client = sm.SecretManagerServiceClient(credentials=credentials,
            client_options={'api_endpoint':REGIONAL_ENDPOINT})
        probe = Probe(client, secret_client, session)
        try:
            probe.setup();probe.run();probe.report['status'] = 'PASSED'
        except Exception as error:
            code = getattr(error, 'code', None)
            if not isinstance(code, str):code = str(error) if isinstance(error, RuntimeError) else ''
            probe.report.update(status='FAILED', error_code=code if re.fullmatch(r'[A-Z_]+', code) else type(error).__name__)
            if isinstance(error, ProjectBindingError):
                probe.report.update(provider_status=error.status, provider_reason=error.reason,
                                    provider_service=error.service, provider_consumer=error.consumer)
        finally:
            try:probe.cleanup()
            except Exception:probe.report['status'] = 'FAILED'
            secret_client.transport.close()
            probe.report['secret_accesses'] = len(probe.access_names)
            args.output_dir.mkdir(parents=True, exist_ok=True)
            path = args.output_dir / (probe.scope.run_id + '.json')
            path.write_text(json.dumps(probe.report, indent=2)+'\n', encoding='utf-8')
            print(json.dumps(dict(event='sharing_runtime_probe_complete', status=probe.report['status'],
                cases=len(probe.report['cases']), cleanup_dataset=probe.report['cleanup_dataset'],
                cleanup_secret=probe.report['cleanup_secret'], report=str(path))), flush=True)
        require(probe.report['status'] == 'PASSED', 'SHARING_RUNTIME_PROBE_FAILED')


if __name__ == '__main__':main()
