"""Submission/recovery fixtures with real SDK metadata; no cloud writes or races."""
import base64
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import hmac
import json
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Conflict, Forbidden, NotFound, ServiceUnavailable
from google.cloud import bigquery

from src.application.access_policy import AuthorizationError
from src.application.access_reader import AccessReadError, BigQueryAccessReader
from src.application.identity import IapIdentityAdapter, IdentityError, ISSUER, VerifiedIdentity
from src.application.sharing_coordinator_contract import SharingCommitStatus, prepare_sharing
from src.application.sharing_executor import (
    ProtectedSharingExecutor, SharingExecutorError, SharingExecutorSettings, _normalize,
)
from src.application.sharing_intent import SharingIntentSealer, initial_attempt
from src.application.workflow_contract import ContractError, canonical_json_bytes
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from tests.test_conversation_contract import CONVERSATION, LATER, conversation
from tests.test_identity import AUDIENCE
from tests.test_membership_contract import ADMIN, SUBJECT, TARGET, TIME, WORKSPACE, membership, workspace
from tests.test_setup_workflow_tables import dataset, table
from tests.test_sharing_query import query

EPOCH = int(TIME.timestamp())
KEY = b'offline-only-fixture-key-material-32bytes'


def identity(principal=TARGET, subject=SUBJECT):
    return VerifiedIdentity(principal, ISSUER, subject, AUDIENCE, EPOCH - 1, EPOCH + 599)


def native_job(attempt, *, state='DONE', error=None, rows=None):
    job = bigquery.QueryJob(attempt.job_id, attempt.query.sql, client=Mock(project=PROJECT),
                           job_config=attempt.query.job_config())
    job._properties['jobReference'].update(projectId=PROJECT, location=LOCATION)
    job._properties['status'] = dict(state=state)
    if error is not None:
        job._properties['status']['errorResult'] = error
    job._properties['statistics'] = dict(query=dict(statementType='SCRIPT'))
    job.result = Mock(return_value=rows or [])
    return job


def child(attempt, statement, *, error=None, state='DONE', **changes):
    job = bigquery.QueryJob('child_' + statement, statement, client=Mock(project=PROJECT))
    job._properties['jobReference'].update(dict(projectId=PROJECT, location=LOCATION) | changes)
    job._properties['statistics'] = dict(parentJobId=attempt.job_id, query=dict(statementType=statement))
    job._properties['status'] = dict(state=state)
    if error is not None:
        job._properties['status']['errorResult'] = error
    return job


def applied(attempt):
    plan = prepare_sharing(attempt.query.command, workspace_rows=[workspace()],
        membership_rows=[membership()], conversation_rows=[conversation()], receipt_rows=[],
        expected_workspace_revision=7, observed_at=LATER,
        commit_observation=attempt.query.commit_observation).plan
    return dict(state='APPLIED', observed_at=LATER, workspace=dict(plan.workspace_after),
                membership=membership(), conversation=dict(plan.conversation_after),
                receipt=dict(plan.audit_candidate))


class ClientFixture:
    project = PROJECT
    default_query_job_config = None

    def __init__(self):
        self.member, self.workspace, self.conversation = membership(), workspace(), conversation()
        self.jobs = {}
        self.children = []
        self.submissions, self.lookups, self.schemas, self.child_calls = [], [], [], []
        self.submit_error = None
        self.lookup_error = None
        self.tables = {name: table(name) for name in ('workspaces', 'memberships', 'conversations', 'audit_events')}

    def get_dataset(self, name, **kwargs):
        self.schemas.append((name, kwargs))
        return dataset()

    def get_table(self, ref, **kwargs):
        self.schemas.append((ref.table_id, kwargs))
        return self.tables[ref.table_id]

    def query(self, sql, **kwargs):
        if 'BEGIN TRANSACTION;' in sql:
            self.submissions.append((sql, kwargs))
            if self.submit_error:
                raise self.submit_error
            if kwargs['job_id'] in self.jobs:
                raise Conflict('already exists')
            raise ServiceUnavailable('no scripted outcome')
        rows = (self.member if '.memberships`' in sql else self.workspace if '.workspaces`' in sql
                else self.conversation)
        return Mock(result=Mock(return_value=[deepcopy(rows)]))

    def get_job(self, job_id, **kwargs):
        self.lookups.append((job_id, kwargs))
        if self.lookup_error:
            raise self.lookup_error
        if job_id not in self.jobs:
            raise NotFound('missing')
        return self.jobs[job_id]

    def list_jobs(self, **kwargs):
        self.child_calls.append(kwargs)
        return iter(self.children)


class SharingExecutorTests(unittest.TestCase):
    def setUp(self):
        for patcher in (patch('src.application.identity.time.time', return_value=EPOCH),
                        patch('src.application.sharing_intent._now', return_value=TIME),
                        patch('src.application.sharing_executor._now', return_value=TIME)):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = ClientFixture()
        self.verifier = Mock(spec=IapIdentityAdapter)
        self.verifier.authenticate.return_value = identity()
        self.sealer = SharingIntentSealer(key=KEY, key_id='fixture-key')
        self.executor = ProtectedSharingExecutor(client=self.client, verifier=self.verifier,
            reader=BigQueryAccessReader(self.client), sealer=self.sealer,
            settings=SharingExecutorSettings(execution_enabled=True))
        self.token = self.sealer.seal(query())
        self.attempt = self.sealer.resolve(self.token, identity())
        self.headers = {'x-goog-iap-jwt-assertion': 'opaque-fixture'}

    def good_job(self):
        job = native_job(self.attempt, rows=[applied(self.attempt)])
        self.client.jobs[self.attempt.job_id] = job
        return job

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises((ContractError, SharingExecutorError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_default_disables_execution_before_identity_or_io(self):
        executor = ProtectedSharingExecutor(client=self.client, verifier=self.verifier,
            reader=BigQueryAccessReader(self.client), sealer=self.sealer)
        for action in (executor.submit, executor.recover):
            self.assert_code('SHARING_EXECUTION_DISABLED', action, self.headers, self.token)
        self.verifier.authenticate.assert_not_called()
        self.assertEqual(self.client.schemas + self.client.submissions + self.client.lookups, [])

    def test_sealed_intent_roundtrip_stable_across_fresh_server_instances(self):
        second = SharingIntentSealer(key=KEY, key_id='fixture-key')
        self.assertEqual(second.resolve(self.token, identity()), self.attempt)
        self.assertEqual(self.sealer.seal(query()), self.token)
        self.assertLessEqual(len(self.token), 8192)
        self.assertEqual(len(self.attempt.job_id), 73)
        self.assertNotIn('fixture-retry', self.token)
        self.assertNotIn(str(KEY), repr(self.sealer))
        with self.assertRaises(FrozenInstanceError):
            self.attempt.job_id = 'another'

    def test_signature_tampering_and_wrong_key_fail_before_io(self):
        parts = self.token.split('.')
        raw = json.loads(base64.urlsafe_b64decode(parts[1] + '=' * (-len(parts[1]) % 4)))
        raw['expected_workspace_revision'] = 8
        parts[1] = base64.urlsafe_b64encode(canonical_json_bytes(raw)).decode().rstrip('=')
        self.assert_code('INVALID_SHARING_SEAL', self.executor.submit, self.headers, '.'.join(parts))
        other = SharingIntentSealer(key=b'x'*32, key_id='fixture-key')
        self.assert_code('INVALID_SHARING_SEAL', other.resolve, self.token, identity())
        self.assertEqual(self.client.submissions + self.client.lookups, [])

    def test_token_cannot_replace_principal_or_move_original_window(self):
        from tests.test_membership_contract import ADMIN_SUBJECT
        self.assert_code('SHARING_IDENTITY_MISMATCH', self.sealer.resolve,
                         self.token, identity(ADMIN, ADMIN_SUBJECT))
        with patch('src.application.sharing_intent._now', return_value=TIME+timedelta(days=7)):
            self.assert_code('SHARING_RETRY_EXPIRED', self.sealer.resolve, self.token, identity())
        with patch('src.application.sharing_intent._now', return_value=TIME-timedelta(seconds=1)):
            self.assert_code('SHARING_INTENT_NOT_YET_VALID', self.sealer.resolve, self.token, identity())

    def test_invalid_token_shapes_and_noncanonical_base64_fail(self):
        for value in (None, '', 'x'*8193, self.token+'=', self.token.replace('share1.', 'share2.', 1)):
            self.assert_code('INVALID_SHARING_SEAL', self.sealer.resolve, value, identity())
        # Invalid low padding bits: same decoded signature, different textual token.
        parts = self.token.split('.')
        alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
        parts[2] = parts[2][:-1] + alphabet[alphabet.index(parts[2][-1]) ^ 1]
        self.assert_code('INVALID_SHARING_SEAL', self.sealer.resolve, '.'.join(parts), identity())

    def test_valid_signature_does_not_accept_noncanonical_or_extra_payload(self):
        parts = self.token.split('.')
        payload = json.loads(base64.urlsafe_b64decode(parts[1]+'='*(-len(parts[1]) % 4)))
        for raw in (json.dumps(payload).encode(), canonical_json_bytes(payload | {'extra': 'value'})):
            body = base64.urlsafe_b64encode(raw).decode().rstrip('=')
            sig = base64.urlsafe_b64encode(hmac.digest(KEY, b'ci-sharing-intent-v1\0'+raw, 'sha256')).decode().rstrip('=')
            self.assert_code('INVALID_SHARING_SEAL', self.sealer.resolve, 'share1.'+body+'.'+sig, identity())

    def test_signing_key_settings_and_initial_attempt_are_strict(self):
        for key in (b'', b'x'*31, b'x'*65, 'x'*32, bytearray(b'x'*32)):
            self.assert_code('INVALID_SHARING_SIGNING_KEY', SharingIntentSealer, key=key, key_id='fixture')
        self.assert_code('INVALID_SHARING_INITIAL_ATTEMPT', initial_attempt, query(SharingCommitStatus.UNKNOWN))
        self.assert_code('SHARING_ATTEMPT_MISMATCH', replace, self.attempt, job_id='changed')

    def test_configuration_or_sql_changes_invalidate_old_seal(self):
        from src.application.sharing_query import SharingQuerySettings
        changed = SharingIntentSealer(key=KEY, key_id='fixture-key', settings=SharingQuerySettings(maximum_bytes_billed=100000))
        self.assert_code('SHARING_ATTEMPT_MISMATCH', changed.resolve, self.token, identity())
        with patch('src.application.sharing_query._render_sql', return_value=query().sql+'\n-- changed'):
            self.assert_code('SHARING_ATTEMPT_MISMATCH', self.sealer.resolve, self.token, identity())

    def test_issue_uses_server_identity_time_trace_and_current_permission(self):
        from src.application.access_policy import Visibility
        token = self.executor.issue_intent(self.headers, workspace_id=WORKSPACE, conversation_id=CONVERSATION,
            visibility=Visibility.TEAM, expected_membership_revision=3, expected_conversation_revision=3,
            expected_workspace_revision=7, retry_key='request-retry')
        accepted = self.sealer.resolve(token, identity()).query.command
        self.assertEqual(accepted.principal_id, TARGET.principal_id)
        self.assertEqual(accepted.accepted_at, TIME)
        self.assertTrue(accepted.trace_id.startswith('share_'))
        self.assertEqual(self.client.submissions, [])

    def test_fresh_signin_is_required_for_every_submit_and_recover(self):
        self.good_job()
        self.executor.submit(self.headers, self.token)
        self.executor.recover(self.headers, self.token)
        self.assertEqual(self.verifier.authenticate.call_count, 2)
        self.verifier.authenticate.side_effect = IdentityError('INVALID_ASSERTION')
        before = len(self.client.submissions)
        with self.assertRaises(IdentityError):
            self.executor.submit(self.headers, self.token)
        self.assertEqual(len(self.client.submissions), before)

    def test_expired_identity_rejects_before_database_io(self):
        with patch('src.application.identity.time.time', return_value=EPOCH+1000):
            with self.assertRaises(IdentityError):
                self.executor.submit(self.headers, self.token)
        self.assertEqual(self.client.schemas + self.client.submissions + self.client.lookups, [])

    def test_revoked_noncreator_and_nonowner_deny_before_dispatch(self):
        for changes in (dict(status='REVOKED'), dict(roles=['ADMIN'])):
            self.client.member.update(changes)
            with self.assertRaises(AuthorizationError):
                self.executor.submit(self.headers, self.token)
            self.client.member = membership()
        self.client.conversation['owner_id'] = ADMIN.principal_id
        with self.assertRaises(AuthorizationError):
            self.executor.recover(self.headers, self.token)
        self.assertEqual(self.client.submissions + self.client.lookups, [])

    def test_missing_or_duplicate_current_memberships_deny_recovery(self):
        original = self.client.query
        for rows in ([], [membership(), membership()]):
            self.client.query = lambda sql, **kwargs: Mock(result=Mock(return_value=rows)) if '.memberships`' in sql else original(sql, **kwargs)
            with self.assertRaises((AuthorizationError, AccessReadError)):
                self.executor.recover(self.headers, self.token)
        self.assertEqual(self.client.lookups, [])

    def test_schema_preflight_checks_all_targets_before_single_submit(self):
        self.good_job()
        outcome = self.executor.submit(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(outcome.result.resulting_revision, 4)
        self.assertEqual(len(self.client.schemas), 5)
        self.assertEqual(len(self.client.submissions), 1)
        for _, kwargs in self.client.schemas:
            self.assertIsNone(kwargs['retry'])
            self.assertEqual(kwargs['timeout'], 5)

    def test_incompatible_destination_schema_blocks_dispatch(self):
        self.client.tables['audit_events'].schema = [bigquery.SchemaField('different', 'STRING')]
        self.assert_code('SHARING_STORE_UNAVAILABLE', self.executor.submit, self.headers, self.token)
        self.assertEqual(self.client.submissions + self.client.lookups, [])

    def test_submit_and_result_disable_rpc_job_retries_and_pin_scope(self):
        job = self.good_job()
        self.executor.submit(self.headers, self.token)
        _, kwargs = self.client.submissions[0]
        self.assertEqual(kwargs['job_id'], self.attempt.job_id)
        self.assertEqual(kwargs['location'], LOCATION)
        self.assertEqual(kwargs['project'], PROJECT)
        self.assertIsNone(kwargs['retry'])
        self.assertIsNone(kwargs['job_retry'])
        self.assertEqual(kwargs['api_method'], bigquery.enums.QueryApiMethod.INSERT)
        result_args = job.result.call_args.kwargs
        self.assertIsNone(result_args['retry'])
        self.assertIsNone(result_args['job_retry'])
        self.assertEqual(result_args['max_results'], 2)

    def test_lost_submission_response_reconciles_same_job_without_retry(self):
        self.good_job()
        self.client.submit_error = ServiceUnavailable('lost response with private content')
        outcome = self.executor.submit(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(self.client.lookups[0][0], self.attempt.job_id)

    def test_repeated_submissions_use_same_id_and_recover_never_submits(self):
        self.good_job()
        first = self.executor.submit(self.headers, self.token)
        second = self.executor.submit(self.headers, self.token)
        recovered = self.executor.recover(self.headers, self.token)
        self.assertEqual(first, second)
        self.assertEqual(first, recovered)
        self.assertEqual(len(self.client.submissions), 2)
        self.assertEqual({kwargs['job_id'] for _, kwargs in self.client.submissions}, {self.attempt.job_id})

    def test_job_not_found_timeout_forbidden_and_pending_are_unknown(self):
        for failure in (NotFound('absent'), TimeoutError('uncertain'), Forbidden('unavailable'), ServiceUnavailable('unknown')):
            self.client.lookup_error = failure
            outcome = self.executor.recover(self.headers, self.token)
            self.assertEqual(outcome.observation.status, SharingCommitStatus.UNKNOWN)
            self.assertIsNone(outcome.result)
        self.client.lookup_error = None
        self.client.jobs[self.attempt.job_id] = native_job(self.attempt, state='RUNNING')
        self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.submissions, [])

    def test_mismatched_native_job_scope_or_sql_never_provides_success(self):
        for changes in (dict(projectId='other'), dict(location='us'), dict(jobId='other')):
            job = self.good_job()
            job._properties['jobReference'].update(changes)
            self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.UNKNOWN)
            job.result.assert_not_called()
        job = self.good_job()
        job._properties['configuration']['query']['query'] = 'SELECT 1'
        self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.UNKNOWN)

    def test_mismatched_job_parameters_settings_sessions_and_parent_are_unknown(self):
        for changes in (dict(useQueryCache=True), dict(createSession=True), dict(queryParameters=[]),
                        dict(maximumBytesBilled='1'), dict(connectionProperties=[{'key':'session_id','value':'x'}])):
            job = self.good_job()
            job._properties['configuration']['query'].update(changes)
            self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.UNKNOWN)
            job.result.assert_not_called()
        job = self.good_job()
        job._properties['statistics']['parentJobId'] = 'other-parent'
        self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.UNKNOWN)

    def test_nested_sdk_rows_normalize_and_result_projection_excludes_identity(self):
        row = applied(self.attempt)
        sdk = lambda values: bigquery.Row(tuple(values.values()), {key: i for i, key in enumerate(values)})
        for name in ('workspace', 'membership', 'conversation', 'receipt'):
            row[name] = sdk(row[name])
        job = self.good_job()
        job.result.return_value = [sdk(row)]
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertNotIn(SUBJECT, repr(outcome))
        self.assertNotIn('identity_subject', repr(outcome))

    def test_malformed_duplicate_or_missing_results_stay_unknown(self):
        for rows in ([], [applied(self.attempt), applied(self.attempt)], [dict(state='APPLIED')]):
            job = self.good_job()
            job.result.return_value = rows
            outcome = self.executor.recover(self.headers, self.token)
            self.assertEqual(outcome.observation.status, SharingCommitStatus.UNKNOWN)
            self.assertIsNone(outcome.result)
        job = self.good_job()
        job.result.side_effect = TimeoutError('private token and unknown state')
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertNotIn('private token', repr(outcome))

    def test_revocation_during_job_result_denies_historical_success(self):
        job = self.good_job()
        def revoke(**kwargs):
            self.client.member.update(status='REVOKED', revision=4)
            return [applied(self.attempt)]
        job.result.side_effect = revoke
        with self.assertRaises(AuthorizationError):
            self.executor.recover(self.headers, self.token)

    def test_window_expiring_during_preflight_prevents_submission(self):
        original = self.client.get_table
        def expire(reference, **kwargs):
            value = original(reference, **kwargs)
            if reference.table_id == 'audit_events':
                patcher = patch('src.application.sharing_executor._now', return_value=TIME+timedelta(days=7))
                patcher.start()
                self.addCleanup(patcher.stop)
            return value
        self.client.get_table = expire
        self.assert_code('SHARING_RETRY_EXPIRED', self.executor.submit, self.headers, self.token)
        self.assertEqual(self.client.submissions, [])

    def test_real_signed_iap_assertion_connects_to_executor_and_tampering_denies(self):
        from types import SimpleNamespace
        from google.auth import jwt
        from src.application.identity import ASSERTION_HEADER, IapSettings
        from tests.test_identity import signer_and_public_key
        signer, public = signer_and_public_key()
        self.executor.verifier = IapIdentityAdapter(IapSettings(AUDIENCE))
        request = Mock(return_value=SimpleNamespace(status=200, data=json.dumps({'fixture-key': public}).encode()))
        token = jwt.encode(signer, dict(iss=ISSUER, aud=AUDIENCE, sub=SUBJECT,
                                       iat=EPOCH-1, exp=EPOCH+599)).decode()
        self.good_job()
        with patch('src.application.identity._BoundedKeyRequest', return_value=request), \
             patch('google.auth._helpers.utcnow', return_value=TIME.replace(tzinfo=None)):
            outcome = self.executor.recover({ASSERTION_HEADER: token}, self.token)
            self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
            parts = token.split('.')
            raw = json.loads(base64.urlsafe_b64decode(parts[1]+'='*(-len(parts[1]) % 4)))
            raw['sub'] = 'attacker'
            parts[1] = base64.urlsafe_b64encode(json.dumps(raw).encode()).decode().rstrip('=')
            previous = len(self.client.lookups)
            with self.assertRaises(IdentityError):
                self.executor.recover({ASSERTION_HEADER: '.'.join(parts)}, self.token)
            self.assertEqual(len(self.client.lookups), previous)

    def test_failed_parent_with_successful_rollback_proves_noncommit(self):
        self.client.jobs[self.attempt.job_id] = native_job(self.attempt, error={'reason':'invalidQuery'})
        self.client.children = [child(self.attempt, 'BEGIN_TRANSACTION'), child(self.attempt, 'ROLLBACK_TRANSACTION')]
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.NOT_COMMITTED)
        self.assertIsNone(outcome.result)
        self.assertEqual(self.client.submissions, [])
        self.assertEqual(self.client.child_calls[0]['parent_job'], self.attempt.job_id)

    def test_failed_final_statement_after_commit_does_not_authorize_retry(self):
        self.client.jobs[self.attempt.job_id] = native_job(self.attempt, error={'reason':'backendError'})
        self.client.children = [child(self.attempt, 'COMMIT_TRANSACTION'), child(self.attempt, 'SELECT', error={'reason':'backendError'})]
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertIsNone(outcome.result)

    def test_failed_job_error_alone_or_failed_rollback_never_proves_noncommit(self):
        self.client.jobs[self.attempt.job_id] = native_job(self.attempt, error={'reason':'stopped'})
        for children in ([], [child(self.attempt, 'SELECT', error={'reason':'invalidQuery'})],
                         [child(self.attempt, 'ROLLBACK_TRANSACTION', error={'reason':'backendError'})]):
            self.client.children = children
            self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.UNKNOWN)

    def test_foreign_truncated_or_conflicting_child_metadata_is_unknown(self):
        self.client.jobs[self.attempt.job_id] = native_job(self.attempt, error={'reason':'invalidQuery'})
        foreign = child(self.attempt, 'ROLLBACK_TRANSACTION', location='us')
        for children in ([foreign], [child(self.attempt, 'ROLLBACK_TRANSACTION', state='RUNNING')],
                         [child(self.attempt, 'COMMIT_TRANSACTION'), child(self.attempt, 'ROLLBACK_TRANSACTION')],
                         [child(self.attempt, 'SELECT')]*64):
            self.client.children = children
            self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.UNKNOWN)

    def test_errors_array_does_not_override_successful_terminal_result(self):
        job = self.good_job()
        job._properties['status']['errors'] = [{'reason':'backendError'}]
        self.assertEqual(self.executor.recover(self.headers, self.token).observation.status, SharingCommitStatus.COMMITTED)

    def test_nested_normalization_is_bounded(self):
        for value in ({str(i): i for i in range(33)}, list(range(9)), [[[[[1]]]]]):
            self.assert_code('INVALID_SHARING_RESULT_SHAPE', _normalize, value)

    def test_client_overrides_cross_project_and_untyped_adapters_are_rejected(self):
        self.client.default_query_job_config = bigquery.QueryJobConfig(destination=f'{PROJECT}.ci_application.conversations')
        with self.assertRaises(ValueError):
            ProtectedSharingExecutor(client=self.client, verifier=self.verifier,
                reader=BigQueryAccessReader(self.client), sealer=self.sealer)
        for changes in (dict(execution_enabled=1), dict(rpc_timeout_seconds=True),
                        dict(result_timeout_seconds=61), dict(rpc_timeout_seconds=0)):
            self.assert_code('INVALID_SHARING_EXECUTOR_SETTINGS', SharingExecutorSettings, **changes)


if __name__ == '__main__':
    unittest.main()
