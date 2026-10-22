"""4.2 Critical/Must have/shared pilot: isolated native SQL/codec verification.

Opt in with --run --output-dir PATH. Uses ADC; creates four explicit-schema
fixture tables in a fresh owned dataset with a two-hour expiry, then deletes it.
Never writes serving controls, accesses secrets, changes IAM or enables execution.
This proves read SQL/decoding only, not fenced writes or deployed runtime refresh.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
import json
from pathlib import Path
import re
from uuid import uuid4

from google.cloud import bigquery

from src.application.sharing_jobs import matches_sharing_job
from src.application.sharing_publication_codec import decode_read_result, encode_publication, normalize
from src.application.sharing_publication_contract import POLICY_ID, initial_publication, propose_publication
from src.application.sharing_publication_read_contract import SigningReadLimits, SigningReadRequest
from src.application.sharing_publication_reader import read_config, read_sql
from src.application.sharing_publication_schema import STORAGE_VERSION, TABLES
from src.application.sharing_query import SharingQuerySettings
from src.application.sharing_refresh_contract import SigningReadTicket
from src.application.sharing_secret_loader import SharingSecretConfig, SharingSecretVersion
from src.application.sharing_signing_contract import SharingSealProfile, SharingSigningKey, SharingSigningPolicy
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import (
    DATASET_ID, LOCATION, PROJECT, _partition, _schema, validate_dataset,
)

TTL_MS = 2 * 60 * 60 * 1000
FIXTURE_PROJECT_NUMBER = '1'  # Deliberately fictitious metadata; no Secret Manager access.


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


@dataclass(frozen=True)
class ProbeScope:
    run_id: str

    def __post_init__(self):
        require(type(self.run_id) is str and re.fullmatch(r'[0-9a-f]{32}', self.run_id), 'INVALID_PROBE_SCOPE')

    @property
    def dataset(self):
        return f'{PROJECT}.ci_signing_read_it_{self.run_id}'

    @property
    def labels(self):
        return dict(purpose='signing_read_integration', run_id=self.run_id)

    def table(self, name):
        require(name in TABLES, 'UNRELATED_PROBE_TABLE')
        return self.dataset + '.' + name

    def assert_sql(self, sql):
        references = re.findall(r'`([^`]+)`', sql)
        require(bool(references) and all(ref in {self.table(n) for n in TABLES} for ref in references),
                'UNSAFE_PROBE_SQL_SCOPE')

    def relocate(self, sql):
        for name in TABLES:
            source = f'`{DATASET_ID}.{name}`'
            require(source in sql, 'INVALID_READ_TEMPLATE')
            sql = sql.replace(source, f'`{self.table(name)}`')
        self.assert_sql(sql)
        return sql

    def assert_owned(self, actual):
        require(actual.project == PROJECT and f'{actual.project}.{actual.dataset_id}' == self.dataset
                and actual.location.lower() == LOCATION and actual.labels == self.labels
                and actual.default_table_expiration_ms == TTL_MS, 'PROBE_OWNERSHIP_MISMATCH')


def publications(at, length=5):
    key = SharingSigningKey('fixture-key', 'fixture-profile', at - timedelta(days=1),
                            at + timedelta(days=1), at + timedelta(days=8))
    profile = SharingSealProfile.from_runtime('fixture-profile', SharingQuerySettings())
    config = SharingSecretConfig(SharingSigningPolicy(key.key_id, (key,), (profile,)),
        FIXTURE_PROJECT_NUMBER, (SharingSecretVersion(key.key_id, 'fixture-no-secret-access', 1),))
    first = initial_publication(config, published_at=at - timedelta(minutes=5))
    pubs = [first]
    for index in range(1, length):
        pubs.append(propose_publication(pubs[-1], config=config, status=first.status,
                                       published_at=first.published_at + timedelta(seconds=index)))
    return tuple(pubs)


def fixture_rows(pubs):
    rows = {name: [] for name in TABLES}
    base = dict(schema_version=STORAGE_VERSION, policy_id=POLICY_ID)
    for publication in pubs:
        body, history = encode_publication(publication, publisher_id='fixture-operator',
            command_id=f'fixture-publish-{publication.revision}', recorded_at=publication.published_at)
        rows['signing_publications'].append(body)
        rows['signing_key_history'].extend(history)
    head, floor = pubs[-1], pubs[1]
    rows['signing_heads'] = [base | dict(revision=head.revision, control_revision=7,
        publication_digest=head.digest, published_at=head.published_at,
        publisher_id='fixture-operator', command_id='fixture-head')]
    rows['signing_checkpoints'] = [base | dict(consumer_id='fixture-consumer', checkpoint_revision=2,
        publication_revision=floor.revision, publication_digest=floor.digest, instance_id='fixture-instance',
        updated_at=head.published_at, writer_id='fixture-runtime', command_id='fixture-floor')]
    return rows


def json_rows(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if type(value) is dict:
        return {k: json_rows(v) for k, v in value.items()}
    if type(value) in (tuple, list):
        return [json_rows(v) for v in value]
    return value


class Probe:
    def __init__(self, client):
        require(client.project == PROJECT and client.location == LOCATION
                and client.default_query_job_config is None, 'INVALID_PROBE_CLIENT')
        self.client = client
        self.scope = ProbeScope(uuid4().hex)
        self.owned = False
        self.pubs = publications(datetime.now(timezone.utc))
        self.rows = fixture_rows(self.pubs)
        self.counter = 0
        self.report = dict(step='4.2', severity='Critical', necessity='Must have', scenario='shared pilot',
            project=PROJECT, location=LOCATION, fixture_dataset=self.scope.dataset,
            status='RUNNING', cleanup='NOT_CREATED', serving_row_writes=0, secret_accesses=0,
            scope='NATIVE_READ_SQL_AND_CODEC', stage='PREFLIGHT', cases=[], jobs=[])

    def own(self):
        self.scope.assert_owned(self.client.get_dataset(self.scope.dataset, retry=None, timeout=10))

    def job_id(self):
        self.counter += 1
        return f'signing_read_it_{self.scope.run_id}_{self.counter}'

    def setup(self):
        validate_dataset(self.client.get_dataset(DATASET_ID, retry=None, timeout=10))
        dataset = bigquery.Dataset(self.scope.dataset)
        dataset.location, dataset.labels, dataset.default_table_expiration_ms = LOCATION, self.scope.labels, TTL_MS
        self.client.create_dataset(dataset, exists_ok=False, retry=None, timeout=10)
        self.owned = True
        self.report['cleanup'] = 'PENDING'
        self.own()
        for name, spec in TABLES.items():
            template = spec.table(PROJECT)
            fixture = bigquery.Table(self.scope.table(name), schema=template.schema)
            fixture.clustering_fields = template.clustering_fields
            self.client.create_table(fixture, exists_ok=False, retry=None, timeout=10)
            actual = self.client.get_table(fixture, retry=None, timeout=10)
            require(actual.project == PROJECT and actual.table_id == name and actual.table_type == 'TABLE'
                    and actual.dataset_id == dataset.dataset_id and actual.location.lower() == LOCATION
                    and _schema(actual.schema) == _schema(template.schema)
                    and actual.clustering_fields == template.clustering_fields
                    and _partition(actual) is None and actual.range_partitioning is None
                    and actual.external_data_configuration is None and actual.table_constraints is None
                    and actual.expires is not None, 'INVALID_FIXTURE_TABLE')
            self.load(name, self.rows[name])

    def load(self, name, rows):
        self.report['stage'] = 'LOAD_' + name.upper()
        self.own()
        destination = self.client.get_table(self.scope.table(name), retry=None, timeout=10)
        require(_schema(destination.schema) == _schema(TABLES[name].schema), 'INVALID_FIXTURE_SCHEMA')
        # Truncation is restricted to the fresh owned fixture, never a serving target.
        config = bigquery.LoadJobConfig(schema=destination.schema, write_disposition='WRITE_TRUNCATE')
        job = self.client.load_table_from_json(json_rows(rows), destination, job_config=config,
            job_id=self.job_id(), project=PROJECT, location=LOCATION, timeout=10, num_retries=0)
        job.result(timeout=60, retry=None)
        require(job.state == 'DONE' and job.error_result is None, 'FIXTURE_LOAD_FAILED')

    def request(self, known=None):
        return SigningReadRequest('fixture-consumer', FIXTURE_PROJECT_NUMBER,
            SigningReadTicket('fixture-instance', self.counter + 1,
                0 if known is None else known.revision, None if known is None else known.digest), known)

    def read(self, request):
        self.report['stage'] = 'READ'
        self.own()
        sql = self.scope.relocate(read_sql())
        config = read_config(request, SigningReadLimits())
        expected = bigquery.QueryJobConfig.from_api_repr(deepcopy(config.to_api_repr()))
        job_id = self.job_id()
        job = self.client.query(sql, job_config=config, job_id=job_id, project=PROJECT,
            location=LOCATION, retry=None, job_retry=None, timeout=10)
        self.report['last_job_id'] = job_id
        require(matches_sharing_job(job, job_id=job_id, sql=sql, config=expected), 'INVALID_NATIVE_JOB')
        rows = list(islice(job.result(max_results=2, page_size=2, timeout=20, retry=None, job_retry=None), 2))
        require(job.state == 'DONE' and job.error_result is None
                and matches_sharing_job(job, job_id=job_id, sql=sql, config=expected), 'INVALID_NATIVE_JOB')
        self.report['jobs'].append(dict(job_id=job_id, bytes_processed=job.total_bytes_processed,
                                       bytes_billed=job.total_bytes_billed))
        return decode_read_result(request, [normalize(row) for row in rows], now=datetime.now(timezone.utc))

    def record(self, name, **details):
        self.report['cases'].append(dict(case=name, status='PASSED', **details))
        print(json.dumps(dict(event='signing_read_probe_case', case=name, status='PASSED')), flush=True)

    def cases(self):
        cold = self.read(self.request())
        require(cold.checkpoint_anchor.digest == self.pubs[1].digest
                and [p.digest for p in cold.publications] == [p.digest for p in self.pubs[1:]], 'INVALID_COLD_READ')
        self.record('cold_checkpoint_complete_chain')
        warm = self.read(self.request(self.pubs[3]))
        require(warm.checkpoint_anchor.digest == self.pubs[1].digest
                and [p.digest for p in warm.publications] == [p.digest for p in self.pubs[3:]], 'INVALID_WARM_READ')
        self.record('known_anchor_with_older_checkpoint')
        self.load('signing_checkpoints', [])
        genesis = self.read(self.request())
        require(genesis.checkpoint_anchor is None and len(genesis.publications) == 5, 'INVALID_GENESIS_READ')
        self.record('genesis_without_checkpoint')
        self.load('signing_checkpoints', self.rows['signing_checkpoints'])
        invalid = []
        for group, change, name in (
            ('signing_heads', {'schema_version': 'unsupported'}, 'unsupported_head_version'),
            ('signing_heads', {'control_revision': 0}, 'invalid_control_fence'),
            ('signing_publications', {'config_digest': 'a' * 64}, 'changed_config_digest'),
            ('signing_publications', {'config_canonical_json': 'x' * 65537}, 'oversized_canonical_config'),
            ('signing_key_history', {'secret_version': 2}, 'changed_history_binding'),
            ('signing_checkpoints', {'publication_revision': 99}, 'checkpoint_above_head')):
            changed = deepcopy(self.rows[group])
            changed[-1].update(change)
            invalid.append((group, changed, name))
        invalid.extend([
            ('signing_heads', self.rows['signing_heads'] * 2, 'duplicate_heads'),
            ('signing_checkpoints', self.rows['signing_checkpoints'] * 2, 'duplicate_checkpoints'),
            ('signing_publications', self.rows['signing_publications'][:2] + self.rows['signing_publications'][3:],
             'missing_intermediate_publication'),
            ('signing_key_history', self.rows['signing_key_history'][:-1], 'missing_head_history')])
        extra, _ = encode_publication(propose_publication(self.pubs[-1], config=self.pubs[-1].config,
            status=self.pubs[-1].status, published_at=self.pubs[-1].published_at + timedelta(seconds=1)),
            publisher_id='fixture-operator', command_id='fixture-unheaded', recorded_at=self.pubs[-1].published_at
            + timedelta(seconds=1))
        invalid.append(('signing_publications', self.rows['signing_publications'] + [extra], 'unheaded_publication'))
        for group, changed, name in invalid:
            self.load(group, changed)
            try:
                self.read(self.request())
            except ContractError as error:
                self.record(name, rejection=error.code)
            else:
                raise RuntimeError('INVALID_FIXTURE_ACCEPTED')
            self.load(group, self.rows[group])

    def cleanup(self):
        if self.owned:
            self.own()
            self.client.delete_dataset(self.scope.dataset, delete_contents=True, retry=None, timeout=10)
            self.report['cleanup'] = 'DELETED'
            self.owned = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    with bigquery.Client(project=PROJECT, location=LOCATION) as client:
        probe = Probe(client)
        try:
            probe.setup()
            probe.cases()
            probe.report['status'] = 'PASSED'
        except Exception as error:
            probe.report['status'] = 'FAILED'
            code = getattr(error, 'code', None)
            if not isinstance(code, str):
                code = str(error) if isinstance(error, RuntimeError) else ''
            probe.report['error_code'] = code if re.fullmatch(r'[A-Z_]+', code) else type(error).__name__
        finally:
            try:
                probe.cleanup()
            except Exception:
                probe.report.update(status='FAILED', cleanup='FAILED')
            args.output_dir.mkdir(parents=True, exist_ok=True)
            path = args.output_dir / (probe.scope.run_id + '.json')
            path.write_text(json.dumps(probe.report, indent=2) + '\n', encoding='utf-8')
            print(json.dumps(dict(event='signing_read_probe_complete', status=probe.report['status'],
                cases=len(probe.report['cases']), cleanup=probe.report['cleanup'], report=str(path))), flush=True)
        require(probe.report['status'] == 'PASSED', 'SIGNING_READ_PROBE_FAILED')


if __name__ == '__main__':
    main()
