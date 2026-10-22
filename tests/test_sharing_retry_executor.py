"""Offline protected retry adapter tests using SDK jobs; no native race proof."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import timedelta
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Conflict, Forbidden, NotFound, ServiceUnavailable
from google.cloud import bigquery

from src.application.access_policy import AuthorizationError
from src.application.access_reader import BigQueryAccessReader
from src.application.identity import IapIdentityAdapter, IdentityError
from src.application.sharing_coordinator_contract import SharingCommitObservation, SharingCommitStatus
from src.application.sharing_executor import (
    ProtectedSharingExecutor, SharingExecutorError, SharingExecutorSettings,
    SharingRetryIssuance, SharingRetryIssuanceState,
)
from src.application.sharing_intent import SharingIntentSealer, initial_attempt
from src.application.sharing_query import SharingResultState
from src.application.sharing_retry import MAX_RETRIES, SharingRetrySealer, prepare_retry
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from tests.test_membership_contract import ADMIN, SUBJECT, TIME, membership
from tests.test_sharing_coordinator_contract import command
from tests.test_sharing_executor import ClientFixture, EPOCH, KEY, child, identity, native_job
from tests.test_sharing_query import query, result_row
from tests.test_sharing_receipt import receipt_row

NOW = TIME + timedelta(seconds=10)


class RetryClientFixture(ClientFixture):
    """Supplied cloud response fixtures, not a Python simulation of transactions."""
    def __init__(self):
        super().__init__()
        self.snapshot = dict(observed_at=TIME+timedelta(seconds=1),
            workspaces=[deepcopy(self.workspace)], memberships=[deepcopy(self.member)],
            conversations=[deepcopy(self.conversation)], audit_events=[])
        self.reads, self.read_jobs, self.children_by_job, self.lookup_failures = [], [], {}, {}
        self.insert_rows = None
        self.after_read = self.after_insert = self.before_lookup = None
        self.lost_response = False

    def query(self, sql, **kwargs):
        if sql.startswith('SELECT\n  CURRENT_TIMESTAMP() AS observed_at,'):
            self.reads.append((sql, kwargs))
            job = bigquery.QueryJob(kwargs['job_id'], sql, client=self, job_config=kwargs['job_config'])
            job._properties['jobReference'].update(projectId=PROJECT, location=LOCATION)
            job._properties['status'] = dict(state='DONE')
            def result(**options):
                if self.after_read:
                    self.after_read(job)
                return [deepcopy(self.snapshot)]
            job.result = Mock(side_effect=result)
            self.read_jobs.append(job)
            return job
        if 'BEGIN TRANSACTION;' in sql:
            self.submissions.append((sql, kwargs))
            if kwargs['job_id'] in self.jobs:
                raise Conflict('existing native job')
            if self.insert_rows is None:
                raise ServiceUnavailable('private fixture detail')
            job = bigquery.QueryJob(kwargs['job_id'], sql, client=self, job_config=kwargs['job_config'])
            job._properties['jobReference'].update(projectId=PROJECT, location=LOCATION)
            job._properties['status'] = dict(state='DONE')
            job.result = Mock(return_value=deepcopy(self.insert_rows))
            self.jobs[job.job_id] = job
            if self.after_insert:
                self.after_insert(job)
            if self.lost_response:
                raise ServiceUnavailable('lost response with private fixture detail')
            return job
        return super().query(sql, **kwargs)

    def get_job(self, job_id, **kwargs):
        if self.before_lookup:
            self.before_lookup(job_id)
        if job_id in self.lookup_failures:
            self.lookups.append((job_id, kwargs))
            raise self.lookup_failures[job_id]
        return super().get_job(job_id, **kwargs)

    def list_jobs(self, **kwargs):
        self.child_calls.append(kwargs)
        return iter(self.children_by_job.get(kwargs['parent_job'], []))


class SharingRetryExecutorTests(unittest.TestCase):
    def setUp(self):
        for patcher in (patch('src.application.identity.time.time', return_value=EPOCH+10),
                        patch('src.application.sharing_intent._now', return_value=NOW),
                        patch('src.application.sharing_retry._now', return_value=NOW),
                        patch('src.application.sharing_executor._now', return_value=NOW)):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = RetryClientFixture()
        self.verifier = Mock(spec=IapIdentityAdapter)
        self.verifier.authenticate.return_value = identity()
        self.sealer = SharingRetrySealer(key=KEY, key_id='fixture-key')
        self.executor = ProtectedSharingExecutor(client=self.client, verifier=self.verifier,
            reader=BigQueryAccessReader(self.client), sealer=self.sealer,
            settings=SharingExecutorSettings(execution_enabled=True))
        self.original = initial_attempt(query())
        self.original_token = self.sealer.seal(query())
        self.headers = {'x-goog-iap-jwt-assertion': 'opaque-fixture'}
        self.failed(self.original)

    def failed(self, attempt, *, children=None):
        self.client.jobs[attempt.job_id] = native_job(attempt, error={'reason': 'invalidQuery'})
        self.client.children_by_job[attempt.job_id] = (
            [child(attempt, 'BEGIN_TRANSACTION'), child(attempt, 'ROLLBACK_TRANSACTION')]
            if children is None else children)

    def candidate(self, predecessor=None, *, fence=7):
        predecessor = self.original if predecessor is None else predecessor
        attempt = prepare_retry(predecessor, expected_workspace_revision=fence,
            observed_at=self.client.snapshot['observed_at'], commit_observation=SharingCommitObservation(
                predecessor.query.command.audit_id, SharingCommitStatus.NOT_COMMITTED, predecessor.job_id))
        return attempt, self.sealer.seal_retry(attempt)

    def issue(self, token=None):
        return self.executor.issue_retry(self.headers, self.original_token if token is None else token)

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises((ContractError, SharingExecutorError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def fresh_fence(self, value):
        self.client.workspace['revision'] = value
        self.client.snapshot['workspaces'][0]['revision'] = value

    def committed_receipt(self):
        self.client.snapshot = receipt_row()
        self.client.snapshot['observed_at'] = NOW
        self.client.workspace = deepcopy(self.client.snapshot['workspaces'][0])
        self.client.member = deepcopy(self.client.snapshot['memberships'][0])
        self.client.conversation = deepcopy(self.client.snapshot['conversations'][0])

    def applied_row(self, attempt):
        row = result_row()
        row['workspace']['revision'] = attempt.query.expected_workspace_revision+1
        return row

    def test_disabled_default_stops_all_new_methods_before_authentication(self):
        self.executor.settings = SharingExecutorSettings()
        _, token = self.candidate()
        for method, value in ((self.executor.issue_retry, self.original_token),
                              (self.executor.submit_retry, token),
                              (self.executor.recover_retry, token),
                              (self.executor.recover_receipt, token)):
            self.assert_code('SHARING_EXECUTION_DISABLED', method, self.headers, value)
        self.verifier.authenticate.assert_not_called()
        self.assertEqual(self.client.lookups+self.client.submissions+self.client.reads+self.client.schemas, [])

    def test_retry_sealer_must_be_explicitly_configured(self):
        _, token = self.candidate()
        self.executor.sealer = SharingIntentSealer(key=KEY, key_id='fixture-key')
        for method, value in ((self.executor.issue_retry, self.original_token),
                              (self.executor.submit_retry, token),
                              (self.executor.recover_retry, token)):
            self.assert_code('SHARING_RETRY_NOT_CONFIGURED', method, self.headers, value)
        self.assertEqual(self.client.schemas+self.client.lookups+self.client.reads+self.client.submissions, [])

    def test_authentication_and_tampered_seals_fail_before_io(self):
        _, token = self.candidate()
        for method, value in ((self.executor.issue_retry, self.original_token),
                              (self.executor.submit_retry, token),
                              (self.executor.recover_retry, token)):
            with self.assertRaises(ContractError):
                method(self.headers, value+'x')
        self.verifier.authenticate.side_effect = IdentityError('INVALID_ASSERTION')
        with self.assertRaises(IdentityError):
            self.issue()
        self.assertEqual(self.client.lookups+self.client.schemas+self.client.reads+self.client.submissions, [])

    def test_initial_submit_cannot_bypass_retry_gate(self):
        _, token = self.candidate()
        self.assert_code('INVALID_SHARING_SEAL', self.executor.submit, self.headers, token)
        self.assertEqual(self.client.submissions+self.client.schemas+self.client.lookups, [])

    def test_issue_uses_verified_rollback_and_server_fence_without_dml(self):
        self.fresh_fence(9)
        issued = self.issue()
        self.assertIs(issued.state, SharingRetryIssuanceState.READY)
        attempt = self.sealer.resolve_retry(issued.token, identity())
        self.assertEqual(attempt.query.expected_workspace_revision, 9)
        self.assertEqual(attempt.original, self.original)
        self.assertEqual(attempt.steps[0].predecessor_observation.query_job_id, self.original.job_id)
        self.assertEqual(self.client.submissions, [])
        self.assertEqual([job for job, _ in self.client.lookups], [self.original.job_id])
        self.assertEqual(len(self.client.schemas), 5)
        self.client.jobs[self.original.job_id].result.assert_not_called()
        with self.assertRaises(TypeError):
            self.executor.issue_retry(self.headers, self.original_token, expected_workspace_revision=10)

    def test_replicas_and_different_observation_times_converge_on_one_id(self):
        first = self.sealer.resolve_retry(self.issue().token, identity())
        self.client.snapshot['observed_at'] += timedelta(seconds=1)
        self.executor.sealer = SharingRetrySealer(key=KEY, key_id='fixture-key')
        second = self.sealer.resolve_retry(self.issue().token, identity())
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(self.client.submissions, [])

    def test_missing_metadata_timeout_and_forbidden_never_issue_retry(self):
        for error in (NotFound('missing'), TimeoutError('uncertain'), Forbidden('denied'),
                      ServiceUnavailable('private provider detail')):
            self.client.lookup_failures[self.original.job_id] = error
            result = self.issue()
            self.assertIs(result.state, SharingRetryIssuanceState.RECOVERY_REQUIRED)
            self.assertIsNone(result.token)
            self.assertNotIn('private', repr(result))
        self.assertEqual(self.client.submissions, [])

    def test_pending_or_successful_parent_with_missing_receipt_is_recovery_only(self):
        for state, rows in (('RUNNING', None), ('DONE', [result_row()])):
            self.client.jobs[self.original.job_id] = native_job(self.original, state=state, rows=rows)
            self.assertIs(self.issue().state, SharingRetryIssuanceState.RECOVERY_REQUIRED)
        self.assertEqual(self.client.submissions, [])

    def test_ambiguous_failed_or_foreign_children_do_not_prove_rollback(self):
        variants = [[], [child(self.original, 'ROLLBACK_TRANSACTION', error={'reason':'backendError'})],
            [child(self.original, 'COMMIT_TRANSACTION'), child(self.original, 'ROLLBACK_TRANSACTION')],
            [child(self.original, 'ROLLBACK_TRANSACTION', location='us')],
            [child(self.original, 'ROLLBACK_TRANSACTION', state='RUNNING')],
            [child(self.original, 'SELECT')]*64]
        for children in variants:
            self.failed(self.original, children=children)
            self.assertIs(self.issue().state, SharingRetryIssuanceState.RECOVERY_REQUIRED)
        self.assertEqual(self.client.submissions, [])

    def test_parent_scope_sql_parameters_and_settings_must_match(self):
        for mutate in (lambda job: job._properties['jobReference'].update(location='us'),
                       lambda job: job._properties['configuration']['query'].update(query='SELECT 1'),
                       lambda job: job._properties['configuration']['query'].update(queryParameters=[]),
                       lambda job: job._properties['configuration']['query'].update(useQueryCache=True),
                       lambda job: job._properties['statistics'].update(parentJobId='other')):
            self.failed(self.original)
            mutate(self.client.jobs[self.original.job_id])
            self.assertIs(self.issue().state, SharingRetryIssuanceState.RECOVERY_REQUIRED)
        self.assertEqual(self.client.child_calls+self.client.submissions, [])

    def test_failure_after_commit_cannot_issue_without_receipt(self):
        self.failed(self.original, children=[child(self.original, 'COMMIT_TRANSACTION')])
        self.assertIs(self.issue().state, SharingRetryIssuanceState.RECOVERY_REQUIRED)
        self.assertEqual(self.client.submissions, [])

    def test_receipt_reuse_wins_over_unknown_or_positive_rollback(self):
        self.committed_receipt()
        for missing in (False, True):
            if missing:
                self.client.jobs.clear()
            result = self.issue()
            self.assertIs(result.state, SharingRetryIssuanceState.REUSED)
            self.assertIsNone(result.token)
            self.assertEqual(result.result.resulting_revision, 4)
        self.assertEqual(self.client.submissions, [])

    def test_historical_receipt_preserves_later_visibility(self):
        self.committed_receipt()
        self.client.conversation.update(visibility='PRIVATE', access_revision=5, updated_at=NOW)
        self.client.snapshot['conversations'] = [deepcopy(self.client.conversation)]
        before = deepcopy(self.client.snapshot)
        result = self.issue()
        self.assertIs(result.state, SharingRetryIssuanceState.REUSED)
        self.assertEqual(result.result.resulting_revision, 4)
        self.assertEqual(self.client.snapshot, before)
        self.assertEqual(self.client.submissions, [])

    def test_changed_envelope_under_receipt_prefix_is_a_conflict(self):
        self.committed_receipt()
        token = self.sealer.seal(query(command=command(trace_id='changed-envelope')))
        self.assert_code('SHARING_IDEMPOTENCY_CONFLICT', self.issue, token)
        self.assertEqual(self.client.submissions, [])

    def test_stale_business_pins_cannot_be_refreshed_by_issuance(self):
        for table, name, code in (('memberships', 'revision', 'STALE_MEMBERSHIP_REVISION'),
                                  ('conversations', 'access_revision', 'STALE_CONVERSATION_REVISION')):
            row = self.client.snapshot[table][0]
            row[name] += 1
            self.assert_code(code, self.issue)
            row[name] -= 1
        self.assertEqual(self.client.submissions, [])

    def test_duplicate_or_malformed_snapshot_controls_do_not_issue(self):
        for table in ('workspaces', 'memberships', 'conversations'):
            row = deepcopy(self.client.snapshot)
            self.client.snapshot[table] *= 2
            with self.assertRaises(ContractError):
                self.issue()
            self.client.snapshot = row
        self.client.snapshot['workspaces'][0]['workspace_id'] = 'other'
        with self.assertRaises(ContractError):
            self.issue()
        self.assertEqual(self.client.submissions, [])

    def test_incompatible_schema_blocks_issue_and_submit_before_jobs(self):
        _, token = self.candidate()
        self.client.tables['audit_events'].schema = [bigquery.SchemaField('wrong', 'STRING')]
        self.assert_code('SHARING_STORE_UNAVAILABLE', self.issue)
        self.assert_code('SHARING_STORE_UNAVAILABLE', self.executor.submit_retry, self.headers, token)
        self.assertEqual(self.client.lookups+self.client.reads+self.client.submissions, [])

    def test_revoked_noncreator_or_nonowner_denies_all_retry_methods(self):
        _, token = self.candidate()
        calls = ((self.executor.issue_retry, self.original_token),
                 (self.executor.submit_retry, token), (self.executor.recover_retry, token))
        for changes in (dict(status='REVOKED'), dict(roles=['ADMIN'])):
            self.client.member.update(changes)
            for method, value in calls:
                with self.assertRaises(AuthorizationError):
                    method(self.headers, value)
            self.client.member = membership()
        self.client.conversation['owner_id'] = ADMIN.principal_id
        with self.assertRaises(AuthorizationError):
            self.issue()
        self.assertEqual(self.client.lookups+self.client.reads+self.client.submissions, [])

    def test_revocation_during_snapshot_blocks_token_and_submission(self):
        _, token = self.candidate()
        for method, value in ((self.executor.issue_retry, self.original_token),
                              (self.executor.submit_retry, token)):
            self.client.member = membership()
            self.client.after_read = lambda job: self.client.member.update(status='REVOKED')
            with self.assertRaises(AuthorizationError):
                method(self.headers, value)
        self.assertEqual(self.client.submissions, [])

    def test_expiry_during_preflight_stops_native_lookup(self):
        original = self.client.get_table
        def expire(ref, **kwargs):
            value = original(ref, **kwargs)
            if ref.table_id == 'audit_events':
                patcher = patch('src.application.identity.time.time', return_value=EPOCH+1000)
                patcher.start()
                self.addCleanup(patcher.stop)
            return value
        self.client.get_table = expire
        with self.assertRaises(IdentityError):
            self.issue()
        self.assertEqual(self.client.lookups+self.client.reads+self.client.submissions, [])

    def test_intent_expiry_during_snapshot_stops_issuance_and_submit(self):
        _, token = self.candidate()
        for method, value in ((self.executor.issue_retry, self.original_token),
                              (self.executor.submit_retry, token)):
            patcher = patch('src.application.sharing_executor._now', return_value=TIME+timedelta(days=7))
            self.client.after_read = lambda job: patcher.start()
            try:
                self.assert_code('SHARING_RETRY_EXPIRED', method, self.headers, value)
            finally:
                patcher.stop()
        self.assertEqual(self.client.submissions, [])

    def test_sealed_supplied_rollback_is_not_native_proof(self):
        attempt, token = self.candidate()
        self.client.jobs.clear()
        result = self.executor.submit_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(result.observation.query_job_id, attempt.job_id)
        self.assertEqual(self.client.submissions, [])

    def test_submit_checks_proof_and_inserts_only_sealed_id_with_no_retries(self):
        self.fresh_fence(9)
        issued = self.issue()
        attempt = self.sealer.resolve_retry(issued.token, identity())
        self.client.insert_rows = [self.applied_row(attempt)]
        result = self.executor.submit_retry(self.headers, issued.token)
        self.assertIs(result.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(result.result.resulting_revision, 4)
        self.assertEqual(len(self.client.submissions), 1)
        sql, options = self.client.submissions[0]
        self.assertEqual((sql, options['job_id']), (attempt.query.sql, attempt.job_id))
        self.assertEqual((options['project'], options['location']), (PROJECT, LOCATION))
        self.assertIsNone(options['retry'])
        self.assertIsNone(options['job_retry'])
        self.assertEqual(options['api_method'], bigquery.enums.QueryApiMethod.INSERT)
        self.assertEqual(options['timeout'], 5)
        config = deepcopy(options['job_config'].to_api_repr())
        # QueryJob construction adds SQL to the supplied mutable SDK config.
        self.assertEqual(config['query'].pop('query'), attempt.query.sql)
        self.assertEqual(config, attempt.query.job_config().to_api_repr())
        params = {p.name: p.value for p in options['job_config'].query_parameters}
        self.assertEqual(params['expected_workspace_revision'], 9)
        self.assertEqual(params['accepted_at'], TIME)
        self.assertEqual(params['commit_status'], 'NOT_SUBMITTED')

    def test_lost_response_and_duplicates_recover_same_job_without_second_insert(self):
        attempt, token = self.candidate()
        self.client.insert_rows = [self.applied_row(attempt)]
        self.client.lost_response = True
        first = self.executor.submit_retry(self.headers, token)
        second = self.executor.submit_retry(self.headers, token)
        recovered = self.executor.recover_retry(self.headers, token)
        self.assertEqual(first, second)
        self.assertEqual(first, recovered)
        self.assertIs(first.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertNotIn('private', repr(first))

    def test_lost_response_and_unavailable_results_keep_same_id_on_resubmit(self):
        attempt, token = self.candidate()
        self.client.insert_rows = [self.applied_row(attempt)]
        self.client.lost_response = True
        self.client.lookup_failures[attempt.job_id] = NotFound('unknown outcome')
        for _ in range(2):
            result = self.executor.submit_retry(self.headers, token)
            self.assertIs(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual({options['job_id'] for _, options in self.client.submissions}, {attempt.job_id})
        self.assertEqual(len(self.client.submissions), 2)
        # The fixture accepts one ID; the second insert is a collision, never a fresh ID.
        self.assertEqual(set(self.client.jobs), {self.original.job_id, attempt.job_id})

    def test_known_failed_retry_does_not_resubmit_or_need_ancestor_metadata(self):
        attempt, token = self.candidate()
        self.failed(attempt)
        self.client.jobs.pop(self.original.job_id)
        result = self.executor.submit_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.NOT_COMMITTED)
        self.assertEqual([value for value, _ in self.client.lookups], [attempt.job_id])
        self.assertEqual(self.client.submissions+self.client.reads, [])

    def test_competing_receipt_does_not_attribute_commit_to_retry_job(self):
        attempt, token = self.candidate()
        self.committed_receipt()
        result = self.executor.submit_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertIs(result.result.state, SharingResultState.REUSED)
        self.assertEqual(result.observation.query_job_id, attempt.job_id)
        self.assertEqual(result.result.resulting_revision, 4)
        self.assertEqual(self.client.submissions, [])

    def test_stale_sealed_fence_or_business_pins_do_not_change_attempt(self):
        attempt, token = self.candidate()
        self.fresh_fence(8)
        self.assert_code('STALE_WORKSPACE_REVISION', self.executor.submit_retry, self.headers, token)
        self.fresh_fence(7)
        for table, name, code in (('memberships', 'revision', 'STALE_MEMBERSHIP_REVISION'),
                                  ('conversations', 'access_revision', 'STALE_CONVERSATION_REVISION')):
            self.client.snapshot[table][0][name] += 1
            self.assert_code(code, self.executor.submit_retry, self.headers, token)
            self.client.snapshot[table][0][name] -= 1
        self.assertEqual(self.sealer.resolve_retry(token, identity()).job_id, attempt.job_id)
        self.assertEqual(self.client.submissions, [])

    def test_changed_native_rollback_evidence_blocks_a_valid_seal(self):
        _, token = self.candidate()
        self.failed(self.original, children=[child(self.original, 'SELECT')])
        result = self.executor.submit_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.submissions, [])

    def test_every_ancestor_is_freshly_verified_for_second_retry(self):
        first, _ = self.candidate()
        self.failed(first)
        second, token = self.candidate(first)
        self.client.jobs.pop(self.original.job_id)
        result = self.executor.submit_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.submissions, [])
        self.failed(self.original)
        self.client.insert_rows = [self.applied_row(second)]
        result = self.executor.submit_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.COMMITTED)
        checked = [options['parent_job'] for options in self.client.child_calls]
        self.assertIn(self.original.job_id, checked)
        self.assertIn(first.job_id, checked)

    def test_unknown_retry_cannot_issue_next_even_with_original_rollback(self):
        _, token = self.candidate()
        issued = self.issue(token)
        self.assertIs(issued.state, SharingRetryIssuanceState.RECOVERY_REQUIRED)
        self.assertIsNone(issued.token)
        self.assertEqual(self.client.submissions, [])

    def test_native_verified_chain_still_respects_retry_limit(self):
        token = self.original_token
        for _ in range(MAX_RETRIES):
            issued = self.issue(token)
            self.assertIs(issued.state, SharingRetryIssuanceState.READY)
            attempt = self.sealer.resolve_retry(issued.token, identity())
            self.failed(attempt)
            token = issued.token
        self.assert_code('INVALID_SHARING_RETRY_CHAIN', self.issue, token)
        self.assertEqual(self.client.submissions, [])
        self.committed_receipt()
        self.assertIs(self.issue(token).state, SharingRetryIssuanceState.REUSED)

    def test_recovery_reads_only_current_job_without_ancestors(self):
        attempt, token = self.candidate()
        self.client.jobs = {attempt.job_id: native_job(attempt, rows=[self.applied_row(attempt)])}
        result = self.executor.recover_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual([value for value, _ in self.client.lookups], [attempt.job_id])
        self.assertEqual(self.client.schemas+self.client.reads+self.client.submissions, [])
        self.assertNotIn(SUBJECT, repr(result))

    def test_unknown_recovery_never_submits_or_remints(self):
        attempt, token = self.candidate()
        result = self.executor.recover_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(result.observation.query_job_id, attempt.job_id)
        self.assertEqual(self.client.submissions+self.client.reads, [])

    def test_revocation_during_result_denies_historical_success(self):
        attempt, token = self.candidate()
        job = native_job(attempt, rows=[self.applied_row(attempt)])
        def revoke(**kwargs):
            self.client.member.update(status='REVOKED')
            return [self.applied_row(attempt)]
        job.result.side_effect = revoke
        self.client.jobs[attempt.job_id] = job
        with self.assertRaises(AuthorizationError):
            self.executor.recover_retry(self.headers, token)
        self.assertEqual(self.client.submissions, [])

    def test_retry_receipt_recovery_needs_no_job_metadata(self):
        _, token = self.candidate()
        self.client.jobs.clear()
        self.committed_receipt()
        result = self.executor.recover_receipt(self.headers, token)
        self.assertIs(result.state, SharingResultState.REUSED)
        self.assertEqual(self.client.lookups+self.client.submissions, [])

    def test_receipt_query_native_binding_failures_stop_issue_and_submit(self):
        _, token = self.candidate()
        for mutate in (lambda job: job._properties['jobReference'].update(location='us'),
                       lambda job: job._properties['configuration']['query'].update(queryParameters=[]),
                       lambda job: job._properties['status'].update(state='RUNNING')):
            self.client.after_read = mutate
            self.assert_code('SHARING_RECEIPT_UNAVAILABLE', self.issue)
            self.assert_code('SHARING_RECEIPT_UNAVAILABLE', self.executor.submit_retry, self.headers, token)
        self.assertEqual(self.client.submissions, [])

    def test_receipt_read_uses_fixed_scope_and_disables_retries(self):
        self.issue()
        _, options = self.client.reads[0]
        self.assertEqual((options['project'], options['location']), (PROJECT, LOCATION))
        self.assertIsNone(options['retry'])
        self.assertIsNone(options['job_retry'])
        self.assertFalse(options['job_config'].use_query_cache)
        self.assertFalse(options['job_config'].create_session)
        self.assertEqual(options['job_config'].maximum_bytes_billed, 64*1024**2)
        results = self.client.read_jobs[0].result.call_args.kwargs
        self.assertEqual(results['max_results'], 2)
        self.assertIsNone(results['retry'])
        self.assertIsNone(results['job_retry'])

    def test_unknown_insert_outcome_is_safe_and_not_automatically_retried(self):
        attempt, token = self.candidate()
        result = self.executor.submit_retry(self.headers, token)
        self.assertIs(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(result.observation.query_job_id, attempt.job_id)
        self.assertEqual(len(self.client.submissions), 1)
        self.assertNotIn('private', repr(result))

    def test_issuance_projection_is_immutable_and_rejects_conflicting_fields(self):
        issued = self.issue()
        with self.assertRaises(FrozenInstanceError):
            issued.token = 'changed'
        variants = [dict(state='READY', token=issued.token),
                    dict(state=SharingRetryIssuanceState.READY),
                    dict(state=SharingRetryIssuanceState.RECOVERY_REQUIRED, token=issued.token),
                    dict(state=SharingRetryIssuanceState.REUSED)]
        for values in variants:
            self.assert_code('INVALID_SHARING_RETRY_ISSUANCE', SharingRetryIssuance, **values)

    def test_real_signed_iap_assertion_connects_to_retry_issuance(self):
        import json
        from types import SimpleNamespace
        from google.auth import jwt
        from src.application.identity import ASSERTION_HEADER, ISSUER, IapSettings
        from tests.test_identity import AUDIENCE, signer_and_public_key
        signer, public = signer_and_public_key()
        self.executor.verifier = IapIdentityAdapter(IapSettings(AUDIENCE))
        request = Mock(return_value=SimpleNamespace(status=200,
            data=json.dumps({'fixture-key': public}).encode()))
        token = jwt.encode(signer, dict(iss=ISSUER, aud=AUDIENCE, sub=SUBJECT,
                                       iat=EPOCH-1, exp=EPOCH+599)).decode()
        with patch('src.application.identity._BoundedKeyRequest', return_value=request), \
             patch('google.auth._helpers.utcnow', return_value=NOW.replace(tzinfo=None)):
            result = self.executor.issue_retry({ASSERTION_HEADER: token}, self.original_token)
        self.assertIs(result.state, SharingRetryIssuanceState.READY)
        self.assertEqual(self.client.submissions, [])


if __name__ == '__main__':
    unittest.main()
