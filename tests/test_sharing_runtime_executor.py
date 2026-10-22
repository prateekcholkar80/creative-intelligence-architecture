"""4.3 composed SDK/identity/sealer gates; all provider RPCs mocked offline."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from threading import Event
from types import SimpleNamespace
import json
import unittest
from unittest.mock import Mock, patch

from google.auth import jwt
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, secretmanager_v1 as sm

from src.application.access_policy import AuthorizationError, Visibility
from src.application.access_reader import BigQueryAccessReader
from src.application.identity import IapIdentityAdapter, IapSettings, IdentityError, ASSERTION_HEADER, ISSUER
from src.application.operation_budget import CancellationSignal, OperationSettings, OperationStopped, operation_scope
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_executor import (
    ProtectedSharingExecutor, SharingExecutorError, SharingExecutorSettings, SharingRetryIssuanceState,
)
from src.application.sharing_intent import initial_attempt
from src.application.sharing_publication_read_contract import SigningReadRequest
from src.application.sharing_publication_reader import BigQuerySigningReader
from src.application.sharing_query import SharingResultState
from src.application.sharing_refresh_contract import SigningReadTicket, SigningReplicaPhase
from src.application.sharing_runtime import RuntimeSharingSealer, SigningRuntimeError, SigningRuntimeSettings
from src.application.sharing_runtime_executor import RuntimeProtectedSharingExecutor
from src.application.sharing_secret_loader import SharingSecretLoader, REGIONAL_ENDPOINT
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from tests import test_sharing_publication as publications
from tests import test_sharing_signing as signing
from tests import test_sharing_secret_loader as secrets
from tests import test_signing_storage_io as storage
from tests.test_sharing_executor import EPOCH, identity, native_job, child
from tests.test_sharing_retry_executor import RetryClientFixture
from tests.test_sharing_query import query, result_row
from tests.test_sharing_retry import retry
from tests.test_sharing_receipt import receipt_row
from tests.test_identity import AUDIENCE, signer_and_public_key
from tests.test_membership_contract import SUBJECT


class RuntimeExecutorTests(unittest.TestCase):
    def setUp(self):
        self.now, self.tick = signing.NOW, 100.0
        for module in ('sharing_runtime', 'sharing_publication_reader', 'sharing_secret_loader',
                       'sharing_retained_sealer', 'sharing_intent', 'sharing_retry', 'sharing_executor'):
            p = patch('src.application.' + module + '._now', side_effect=lambda: self.now)
            p.start()
            self.addCleanup(p.stop)
        p = patch('src.application.sharing_runtime.time.monotonic', side_effect=lambda: self.tick)
        p.start()
        self.addCleanup(p.stop)
        p = patch('src.application.identity.time.time', return_value=EPOCH + 10)
        p.start()
        self.addCleanup(p.stop)
        self.pubs = (publications.initial(),)
        self.checkpoint = self.pubs[-1]
        self.material = dict(signing.MATERIAL)
        self.requests = []
        self.cloud = RetryClientFixture()
        self.client = bigquery.Client(project=PROJECT, location=LOCATION, credentials=AnonymousCredentials())
        self.addCleanup(self.client.close)
        self.client.get_dataset = Mock(return_value=storage.workflow.dataset())
        self.client.get_table = Mock(side_effect=self.table)
        self.client.get_job, self.client.list_jobs = self.cloud.get_job, self.cloud.list_jobs
        self.client.query = Mock(side_effect=self.query)
        self.secret_client = sm.SecretManagerServiceClient(credentials=AnonymousCredentials(),
            client_options={'api_endpoint': REGIONAL_ENDPOINT})
        self.addCleanup(self.secret_client.transport.close)
        self.secret_client.access_secret_version = Mock(side_effect=self.fetch)
        self.runtime = RuntimeSharingSealer(reader=BigQuerySigningReader(client=self.client),
            loader=SharingSecretLoader(client=self.secret_client), instance_id='executor-process-A',
            settings=SigningRuntimeSettings('pilot-runtime', secrets.PROJECT_NUMBER, enabled=True))
        self.verifier = Mock(spec=IapIdentityAdapter)
        self.verifier.authenticate.return_value = identity()
        self.executor = self.make_executor()
        self.headers = {ASSERTION_HEADER: 'opaque-offline-assertion'}
        self.legacy = signing.SharingRetrySealer(key=signing.KEY, key_id='old')
        self.original = initial_attempt(query())
        self.token = self.legacy.seal(query())

    def make_executor(self, **changes):
        return RuntimeProtectedSharingExecutor(client=self.client, verifier=self.verifier,
            reader=BigQueryAccessReader(self.client), sealer=self.runtime,
            settings=SharingExecutorSettings(**(dict(execution_enabled=True) | changes)))

    def table(self, ref, **kwargs):
        return (storage.table(ref.table_id) if ref.table_id in storage.TABLES
                else self.cloud.get_table(ref, **kwargs))

    def query(self, sql, **kwargs):
        if kwargs.get('job_id', '').startswith('signing_read_'):
            values = {p.name: p.value for p in kwargs['job_config'].query_parameters}
            ticket = SigningReadTicket(values['instance_id'], values['generation'],
                                       values['base_revision'], values['base_digest'] or None)
            request = SigningReadRequest(values['consumer_id'], values['project_number'], ticket,
                                        self.runtime.state.head)
            self.requests.append(request)
            first = ticket.base_revision or (1 if self.checkpoint is None else self.checkpoint.revision)
            pubs = tuple(p for p in self.pubs if p.revision >= first or p == self.checkpoint)
            row = storage.result(pubs, request=request, checkpoint=self.checkpoint)
            row['observed_at'] = self.now
            job = bigquery.QueryJob(kwargs['job_id'], sql, self.client, job_config=kwargs['job_config'])
            job._properties['jobReference'].update(projectId=PROJECT, location=LOCATION)
            job._properties['status'] = {'state': 'DONE'}
            job.result = Mock(return_value=[row])
            return job
        return self.cloud.query(sql, **kwargs)

    def fetch(self, *, request, **kwargs):
        cfg = self.pubs[-1].config
        binding = next(v for v in cfg.versions if cfg.name(v) == request['name'])
        return secrets.response(request['name'], self.material[binding.key_id])

    def advance(self, **changes):
        head = publications.next_publication(self.pubs[-1], **changes)
        self.pubs += (head,)
        self.now, self.checkpoint = head.published_at, head
        return head

    def revoke(self):
        parent = self.pubs[-1]
        keys = tuple(replace(k, revoked=True) if k.key_id == 'old' else k for k in parent.config.policy.keys)
        return self.advance(config=replace(parent.config, policy=replace(parent.config.policy, keys=keys)))

    def stop(self):
        return self.advance(status=publications.STOPPED)

    def failed(self, attempt=None):
        attempt = self.original if attempt is None else attempt
        self.cloud.jobs[attempt.job_id] = native_job(attempt, error={'reason': 'invalidQuery'})
        self.cloud.children_by_job[attempt.job_id] = [child(attempt, 'BEGIN_TRANSACTION'),
                                                    child(attempt, 'ROLLBACK_TRANSACTION')]

    def candidate(self):
        attempt = retry(self.original)
        return attempt, self.legacy.seal_retry(attempt)

    def committed(self, attempt=None):
        attempt = self.original if attempt is None else attempt
        row = result_row()
        row['workspace']['revision'] = attempt.query.expected_workspace_revision + 1
        self.cloud.jobs[attempt.job_id] = native_job(attempt, rows=[row])
        return self.cloud.jobs[attempt.job_id]

    def assert_code(self, code, call, *args):
        with self.assertRaises((SigningRuntimeError, ContractError, SharingExecutorError)) as caught:
            call(*args)
        self.assertEqual(caught.exception.code, code)

    def preflight_action(self, action):
        original = self.table
        fired = False
        def table(ref, **kwargs):
            nonlocal fired
            value = original(ref, **kwargs)
            if ref.table_id == 'audit_events' and not fired:
                fired = True
                action()
            return value
        self.client.get_table.side_effect = table

    def test_both_default_controls_deny_before_business_io(self):
        self.executor = RuntimeProtectedSharingExecutor(client=self.client, verifier=self.verifier,
            reader=BigQueryAccessReader(self.client), sealer=self.runtime)
        self.assert_code('SHARING_EXECUTION_DISABLED', self.executor.submit, self.headers, self.token)
        self.verifier.authenticate.assert_not_called()
        self.client.query.assert_not_called()
        self.runtime = RuntimeSharingSealer(reader=BigQuerySigningReader(client=self.client),
            loader=SharingSecretLoader(client=self.secret_client), instance_id='disabled-process',
            settings=SigningRuntimeSettings('pilot-runtime', secrets.PROJECT_NUMBER))
        self.executor = self.make_executor()
        self.assert_code('SIGNING_RUNTIME_DISABLED', self.executor.submit, self.headers, self.token)
        self.client.query.assert_not_called()

    def test_composition_requires_runtime_and_same_bigquery_client(self):
        with self.assertRaises(ContractError):
            RuntimeProtectedSharingExecutor(client=self.client, verifier=self.verifier,
                reader=BigQueryAccessReader(self.client), sealer=self.legacy)
        other = bigquery.Client(project=PROJECT, location=LOCATION, credentials=AnonymousCredentials())
        self.addCleanup(other.close)
        with self.assertRaises(ContractError):
            RuntimeProtectedSharingExecutor(client=other, verifier=self.verifier,
                reader=BigQueryAccessReader(other), sealer=self.runtime)

    def test_legacy_executor_cannot_accidentally_bypass_runtime_gates(self):
        with self.assertRaises(ContractError) as caught:
            ProtectedSharingExecutor(client=self.client, verifier=self.verifier,
                reader=BigQueryAccessReader(self.client), sealer=self.runtime)
        self.assertEqual(caught.exception.code, 'SHARING_RUNTIME_GATES_REQUIRED')
        self.client.query.assert_not_called()

    def test_submit_brackets_native_dispatch_with_fresh_gates(self):
        self.cloud.insert_rows = [result_row()]
        outcome = self.executor.submit(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.cloud.submissions), 1)
        self.assertEqual(self.cloud.submissions[0][1]['job_id'], self.original.job_id)
        self.assertEqual(len(self.requests), 4)  # Resolve, dispatch entry/exit, release.
        self.assertEqual([r.ticket.generation for r in self.requests], [1, 2, 3, 4])
        self.assertTrue(all(r.known_head is not None for r in self.requests[1:]))

    def test_stop_during_schema_preflight_prevents_dispatch(self):
        self.preflight_action(self.stop)
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.submit, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])
        self.assertEqual(self.runtime.state.phase, SigningReplicaPhase.STOPPED)

    def test_key_revocation_during_preflight_prevents_dispatch(self):
        self.preflight_action(self.revoke)
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', self.executor.submit, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])

    def test_missing_checkpoint_at_dispatch_denies_without_fallback(self):
        self.preflight_action(lambda: setattr(self, 'checkpoint', None))
        self.assert_code('SIGNING_RUNTIME_CHECKPOINT_REQUIRED', self.executor.submit, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])
        self.assertIsNone(self.runtime._sealer)

    def test_revoked_access_during_preflight_prevents_dispatch(self):
        self.preflight_action(lambda: self.cloud.member.update(status='REVOKED', revision=4))
        with self.assertRaises(AuthorizationError):
            self.executor.submit(self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])

    def test_access_removed_during_dispatch_key_loading_denies_insert(self):
        actual = self.fetch
        def revoke_on_gate_load(**kwargs):
            response = actual(**kwargs)
            if self.secret_client.access_secret_version.call_count == 4:
                self.cloud.member.update(status='REVOKED', revision=4)
            return response
        self.secret_client.access_secret_version.side_effect = revoke_on_gate_load
        with self.assertRaises(AuthorizationError):
            self.executor.submit(self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])
        self.assertIsNone(self.runtime._sealer)

    def test_access_removed_during_result_gate_key_loading_denies_release(self):
        self.committed()
        actual = self.fetch
        def revoke_on_gate_load(**kwargs):
            response = actual(**kwargs)
            if self.secret_client.access_secret_version.call_count == 4:
                self.cloud.member.update(status='REVOKED', revision=4)
            return response
        self.secret_client.access_secret_version.side_effect = revoke_on_gate_load
        with self.assertRaises(AuthorizationError):
            self.executor.recover(self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])
        self.assertIsNone(self.runtime._sealer)

    def test_access_callback_crossing_lease_cannot_admit_dispatch(self):
        access_reader = self.executor.reader
        actual = access_reader.authorize
        calls = 0
        def delayed(*args, **kwargs):
            nonlocal calls
            result = actual(*args, **kwargs)
            calls += 1
            if calls == 2:  # First authorization is before preflight.
                self.tick += 30
            return result
        with patch.object(access_reader, 'authorize', side_effect=delayed):
            self.assert_code('SIGNING_REFRESH_EXPIRED', self.executor.submit, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])

    def test_stop_after_insert_withholds_committed_result_without_replacement(self):
        self.cloud.insert_rows = [result_row()]
        self.cloud.after_insert = lambda job: self.stop()
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.submit, self.headers, self.token)
        self.assertEqual(len(self.cloud.submissions), 1)
        job = self.cloud.jobs[self.original.job_id]
        self.assertIsNone(job.error_result)
        self.assertEqual(job.state, 'DONE')
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.recover, self.headers, self.token)
        self.assertEqual(len(self.cloud.submissions), 1)

    def test_revocation_after_insert_withholds_result_and_retains_job(self):
        self.cloud.insert_rows = [result_row()]
        self.cloud.after_insert = lambda job: self.revoke()
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', self.executor.submit, self.headers, self.token)
        self.assertIn(self.original.job_id, self.cloud.jobs)
        self.assertEqual(len(self.cloud.submissions), 1)
        self.assertIsNone(self.runtime._sealer)

    def test_expiry_during_insert_cannot_be_erased_by_release_refresh(self):
        self.cloud.insert_rows = [result_row()]
        def expire(job):
            self.tick += 30
        self.cloud.after_insert = expire
        self.assert_code('SIGNING_REFRESH_EXPIRED', self.executor.submit, self.headers, self.token)
        self.assertEqual(len(self.requests), 2)  # No new lease after expired dispatch.
        self.assertEqual(len(self.cloud.submissions), 1)
        self.cloud.after_insert = None
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.cloud.submissions), 1)

    def test_access_removal_after_insert_withholds_result(self):
        self.cloud.insert_rows = [result_row()]
        self.cloud.after_insert = lambda job: self.cloud.member.update(status='REVOKED', revision=4)
        with self.assertRaises(AuthorizationError):
            self.executor.submit(self.headers, self.token)
        self.assertEqual(len(self.cloud.submissions), 1)
        self.assertIsNone(self.runtime._sealer)

    def test_rotation_crossing_issue_boundary_withholds_then_recovers_exact_job(self):
        # Old -> new cohort transition within the same bounded operation.
        self.now = signing.TIME
        self.pubs = (publications.initial(secrets.config(policy=signing.policy('old')),
                                          published_at=self.now),)
        self.checkpoint = self.pubs[0]
        self.cloud.insert_rows = [result_row()]
        self.cloud.after_insert = lambda job: self.advance(config=secrets.config(),
                                                           published_at=signing.OLD.issue_until)
        self.assert_code('SIGNING_REFRESH_EXPIRED', self.executor.submit, self.headers, self.token)
        self.cloud.after_insert = None
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(outcome.observation.query_job_id, self.original.job_id)
        self.assertEqual(self.runtime.state.head.config.policy.active_key_id, 'new')
        self.assertEqual(len(self.cloud.submissions), 1)

    def test_lost_insert_response_recovers_original_job_and_never_mints_new_id(self):
        self.cloud.insert_rows = [result_row()]
        self.cloud.lost_response = True
        first = self.executor.submit(self.headers, self.token)
        second = self.executor.recover(self.headers, self.token)
        self.assertEqual(first, second)
        self.assertEqual(first.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.cloud.submissions), 1)

    def test_uncertain_dispatch_stays_unknown_and_recovery_never_submits(self):
        result = self.executor.submit(self.headers, self.token)
        self.assertEqual(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertIsNone(result.result)
        recovered = self.executor.recover(self.headers, self.token)
        self.assertEqual(recovered, result)
        self.assertEqual(len(self.cloud.submissions), 1)

    def test_recovery_rechecks_policy_after_original_job_lookup(self):
        self.committed()
        self.cloud.before_lookup = lambda job_id: self.revoke()
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', self.executor.recover, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])

    def test_recovery_current_policy_stop_denies_unknown_early_result(self):
        self.cloud.before_lookup = lambda job_id: self.stop()
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.recover, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])

    def test_retry_issuance_uses_verified_rollback_and_stable_envelope(self):
        self.failed()
        issued = self.executor.issue_retry(self.headers, self.token)
        self.assertEqual(issued.state, SharingRetryIssuanceState.READY)
        resolved = self.legacy.resolve_retry(issued.token, identity())
        self.assertEqual(resolved.original, self.original)
        self.assertEqual(resolved.query.command.deadline_at, self.original.query.command.deadline_at)
        self.assertEqual(self.cloud.submissions, [])

    def test_retry_issuance_unknown_branch_has_release_gate(self):
        self.cloud.after_read = lambda job: self.stop()
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.issue_retry, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])

    def test_retry_issuance_reused_branch_has_release_gate(self):
        self.cloud.snapshot = receipt_row()
        self.cloud.workspace = deepcopy(self.cloud.snapshot['workspaces'][0])
        self.cloud.conversation = deepcopy(self.cloud.snapshot['conversations'][0])
        self.cloud.after_read = lambda job: self.revoke()
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', self.executor.issue_retry, self.headers, self.token)
        self.assertEqual(self.cloud.submissions, [])

    def test_retry_dispatch_fresh_guard_after_ancestor_and_receipt_reads(self):
        self.failed()
        attempt, token = self.candidate()
        self.cloud.after_read = lambda job: self.revoke()
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', self.executor.submit_retry, self.headers, token)
        self.assertEqual(self.cloud.submissions, [])
        self.assertNotIn(attempt.job_id, self.cloud.jobs)

    def test_positive_retry_commits_and_duplicate_recovers_without_resubmission(self):
        self.failed()
        attempt, token = self.candidate()
        self.cloud.insert_rows = [result_row()]
        first = self.executor.submit_retry(self.headers, token)
        second = self.executor.submit_retry(self.headers, token)
        recovered = self.executor.recover_retry(self.headers, token)
        self.assertEqual(first.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(first.observation.query_job_id, attempt.job_id)
        self.assertEqual(first, second)
        self.assertEqual(first, recovered)
        self.assertEqual(len(self.cloud.submissions), 1)
        self.assertEqual(self.cloud.submissions[0][1]['job_id'], attempt.job_id)

    def test_post_commit_script_error_retains_positive_commit_classification(self):
        self.cloud.jobs[self.original.job_id] = native_job(self.original, error={'reason': 'invalidQuery'})
        self.cloud.children_by_job[self.original.job_id] = [child(self.original, 'BEGIN_TRANSACTION'),
                                                          child(self.original, 'COMMIT_TRANSACTION')]
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertIsNone(outcome.result)
        self.assertEqual(self.cloud.submissions, [])

    def test_known_retry_job_is_release_checked_without_resubmission(self):
        attempt, token = self.candidate()
        self.committed(attempt)
        self.cloud.before_lookup = lambda job_id: self.stop()
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.submit_retry, self.headers, token)
        self.assertEqual(self.cloud.submissions, [])

    def test_unknown_retry_without_rollback_stays_recovery_only(self):
        _, token = self.candidate()
        result = self.executor.submit_retry(self.headers, token)
        self.assertEqual(result.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.cloud.submissions, [])
        self.assertGreaterEqual(len(self.requests), 2)

    def test_unknown_retry_early_exit_denies_policy_change(self):
        _, token = self.candidate()
        self.cloud.after_read = lambda job: self.stop()
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.submit_retry, self.headers, token)
        self.assertEqual(self.cloud.submissions, [])

    def test_retry_reused_receipt_with_unknown_job_has_release_gate(self):
        _, token = self.candidate()
        self.cloud.snapshot = receipt_row()
        self.cloud.workspace = deepcopy(self.cloud.snapshot['workspaces'][0])
        self.cloud.conversation = deepcopy(self.cloud.snapshot['conversations'][0])
        self.cloud.after_read = lambda job: self.stop()
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.submit_retry, self.headers, token)
        self.assertEqual(self.cloud.submissions, [])

    def test_retry_and_receipt_recovery_never_submit_and_check_current_policy(self):
        _, token = self.candidate()
        recovered = self.executor.recover_retry(self.headers, token)
        receipt = self.executor.recover_receipt(self.headers, token)
        self.assertEqual(recovered.observation.status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(receipt.state, SharingResultState.UNKNOWN)
        self.assertEqual(self.cloud.submissions, [])
        self.cloud.after_read = lambda job: self.stop()
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.executor.recover_receipt, self.headers, token)

    def test_retry_recovery_release_gate_after_job_lookup(self):
        attempt, token = self.candidate()
        self.committed(attempt)
        self.cloud.before_lookup = lambda job_id: self.revoke()
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', self.executor.recover_retry, self.headers, token)
        self.assertEqual(self.cloud.submissions, [])

    def test_issue_intent_release_checks_access_after_sealing(self):
        original = self.runtime.seal
        def revoked(query):
            token = original(query)
            self.cloud.member.update(status='REVOKED', revision=4)
            return token
        with patch.object(self.runtime, 'seal', side_effect=revoked):
            with self.assertRaises(AuthorizationError):
                self.executor.issue_intent(self.headers, workspace_id=self.original.query.command.workspace_id,
                    conversation_id=self.original.query.command.conversation_id, visibility=Visibility.TEAM,
                    expected_membership_revision=3, expected_conversation_revision=4,
                    expected_workspace_revision=7, retry_key='new-runtime-intent')
        self.assertEqual(self.cloud.submissions, [])

    def test_typed_attempt_cannot_replace_resolved_principal_in_gate(self):
        from tests.test_membership_contract import ADMIN, ADMIN_SUBJECT
        self.assert_code('SHARING_IDENTITY_MISMATCH', self.runtime.check_attempt,
                         self.original, identity(ADMIN, ADMIN_SUBJECT))
        self.assertIsNone(self.runtime._sealer)

    def test_body_cancellation_invalidates_keys_and_preserves_original_job_identity(self):
        with self.assertRaises(KeyboardInterrupt):
            with self.runtime.attempt_gate(self.original, identity()):
                raise KeyboardInterrupt()
        self.assertIsNone(self.runtime._sealer)
        self.assertEqual(self.runtime.state.phase, SigningReplicaPhase.FAILED)
        self.assertEqual(self.cloud.submissions, [])

    def test_cancellation_after_insert_keeps_job_for_read_only_recovery(self):
        self.cloud.insert_rows = [result_row()]
        def cancel(job):
            raise KeyboardInterrupt()
        self.cloud.after_insert = cancel
        with self.assertRaises(KeyboardInterrupt):
            self.executor.submit(self.headers, self.token)
        self.assertIsNone(self.runtime._sealer)
        self.assertIn(self.original.job_id, self.cloud.jobs)
        self.cloud.after_insert = None
        outcome = self.executor.recover(self.headers, self.token)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.cloud.submissions), 1)

    def test_real_signed_iap_headers_connect_and_tampering_prevents_all_cloud_io(self):
        signer, public = signer_and_public_key()
        adapter = IapIdentityAdapter(IapSettings(AUDIENCE))
        self.executor = RuntimeProtectedSharingExecutor(client=self.client, verifier=adapter,
            reader=BigQueryAccessReader(self.client), sealer=self.runtime,
            settings=SharingExecutorSettings(execution_enabled=True))
        claims = dict(iss=ISSUER, aud=AUDIENCE, sub=SUBJECT, iat=EPOCH-1, exp=EPOCH+599)
        headers = {ASSERTION_HEADER: jwt.encode(signer, claims).decode('ascii')}
        request = Mock(return_value=SimpleNamespace(status=200,
            data=json.dumps({'fixture-key': public}).encode()))
        with patch('src.application.identity._BoundedKeyRequest', return_value=request), \
             patch('google.auth._helpers.utcnow', return_value=self.now.replace(tzinfo=None)):
            self.assertEqual(self.executor.recover(self.headers | headers, self.token).observation.status,
                             SharingCommitStatus.UNKNOWN)
            before = self.client.query.call_count
            with self.assertRaises(IdentityError):
                self.executor.recover({ASSERTION_HEADER: headers[ASSERTION_HEADER] + 'x'}, self.token)
            self.assertEqual(self.client.query.call_count, before)

    def test_concurrent_dispatch_gate_serializes_second_refresh(self):
        entered, release, queued = Event(), Event(), Event()
        self.cloud.insert_rows = [result_row()]
        def pause(job):
            entered.set()
            if not release.wait(5):
                raise RuntimeError('test synchronization timeout')
        self.cloud.after_insert = pause
        def refresh():
            queued.set()
            return self.runtime.refresh()
        with ThreadPoolExecutor(max_workers=2) as pool:
            dispatched = pool.submit(self.executor.submit, self.headers, self.token)
            self.assertTrue(entered.wait(5))
            other = pool.submit(refresh)
            self.assertTrue(queued.wait(5))
            self.assertEqual(len(self.requests), 2)
            release.set()
            outcome, state = dispatched.result(timeout=5), other.result(timeout=5)
        self.assertEqual(outcome.observation.status, SharingCommitStatus.COMMITTED)
        self.assertEqual(state.phase, SigningReplicaPhase.READY)
        self.assertEqual(len(self.cloud.submissions), 1)

    def budget(self, seconds=10):
        self.runtime._settings=replace(self.runtime._settings,operation=OperationSettings(seconds))

    def test_authenticated_recovery_caps_all_cloud_calls_without_changing_sealed_job(self):
        self.budget();job=self.committed()
        def auth(headers):self.tick+=7;return identity()
        self.verifier.authenticate.side_effect=auth
        outcome=self.executor.recover(self.headers,self.token)
        self.assertEqual(outcome.observation.status,SharingCommitStatus.COMMITTED)
        for call in self.client.query.call_args_list+self.client.get_dataset.call_args_list+\
                self.client.get_table.call_args_list+self.secret_client.access_secret_version.call_args_list:
            self.assertLessEqual(call.kwargs['timeout'],3)
            self.assertIsNone(call.kwargs['retry'])
        self.assertEqual(self.cloud.lookups[-1][1]['timeout'],3)
        self.assertEqual(job.result.call_args.kwargs['timeout'],3)
        self.assertEqual(outcome.observation.query_job_id,self.original.job_id)
        self.assertEqual(self.cloud.submissions,[])

    def test_submission_host_cap_preserves_query_fingerprint_and_original_settings(self):
        self.budget();self.cloud.insert_rows=[result_row()]
        def auth(headers):self.tick+=7;return identity()
        self.verifier.authenticate.side_effect=auth
        outcome=self.executor.submit(self.headers,self.token)
        self.assertEqual(outcome.observation.status,SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.cloud.submissions),1)
        sql,options=self.cloud.submissions[0]
        self.assertEqual(options['timeout'],3)
        self.assertEqual(options['job_id'],self.original.job_id)
        self.assertEqual(sql,self.original.query.sql)
        native_config=deepcopy(options['job_config'].to_api_repr())
        native_config['query'].pop('query',None)  # QueryJob adds SQL to its SDK config.
        self.assertEqual(native_config,self.original.query.job_config().to_api_repr())

    def test_child_job_outcome_read_uses_remaining_timeout(self):
        self.budget();self.failed()
        def auth(headers):self.tick+=7;return identity()
        self.verifier.authenticate.side_effect=auth
        outcome=self.executor.recover(self.headers,self.token)
        self.assertEqual(outcome.observation.status,SharingCommitStatus.NOT_COMMITTED)
        self.assertEqual(self.cloud.child_calls[0]['timeout'],3)
        self.assertEqual(self.cloud.submissions,[])

    def test_lazy_child_expiry_cannot_produce_rollback_proof_or_retry_token(self):
        self.budget();self.failed()
        visited=[]
        def children(**kwargs):
            def pages():
                visited.append('first');self.tick+=10
                yield child(self.original,'ROLLBACK_TRANSACTION')
                visited.append('second');yield child(self.original,'BEGIN_TRANSACTION')
            return pages()
        self.client.list_jobs=Mock(side_effect=children)
        with self.assertRaises(OperationStopped):self.executor.issue_retry(self.headers,self.token)
        self.assertEqual(visited,['first'])
        self.assertEqual(self.cloud.submissions,[])

    def test_receipt_query_and_result_timeout_share_authenticated_budget(self):
        self.budget();self.cloud.snapshot['audit_events']=[]
        def auth(headers):self.tick+=7;return identity()
        self.verifier.authenticate.side_effect=auth
        outcome=self.executor.recover_receipt(self.headers,self.token)
        self.assertEqual(outcome.state,SharingResultState.UNKNOWN)
        self.assertEqual(self.cloud.reads[-1][1]['timeout'],3)
        self.assertEqual(self.cloud.read_jobs[-1].result.call_args.kwargs['timeout'],3)
        self.assertEqual(self.cloud.submissions,[])

    def test_cancelled_top_level_calls_never_authenticate_or_read(self):
        signal=CancellationSignal();signal.cancel()
        for name in ('submit','recover','issue_retry','submit_retry','recover_retry','recover_receipt'):
            with self.subTest(name=name),self.assertRaises(OperationStopped):
                getattr(self.executor,name)(self.headers,self.token,cancellation=signal)
        with self.assertRaises(OperationStopped):
            self.executor.issue_intent(self.headers,cancellation=signal)
        self.verifier.authenticate.assert_not_called()
        self.client.query.assert_not_called()

    def test_authentication_elapsed_time_counts_before_any_metadata_read(self):
        self.budget()
        def slow(headers):self.tick+=10;return identity()
        self.verifier.authenticate.side_effect=slow
        with self.assertRaises(OperationStopped) as caught:self.executor.recover(self.headers,self.token)
        self.assertEqual(caught.exception.code,'OPERATION_DEADLINE_EXCEEDED')
        self.client.query.assert_not_called()

    def test_late_provider_error_cannot_hide_request_stop_or_leak_details(self):
        self.budget()
        def fail(headers):self.tick+=10;raise ValueError('private assertion/provider response')
        self.verifier.authenticate.side_effect=fail
        with self.assertRaises(OperationStopped) as caught:self.executor.recover(self.headers,self.token)
        self.assertEqual(str(caught.exception),'OPERATION_DEADLINE_EXCEEDED')
        self.client.query.assert_not_called()

    def test_baseexception_outside_runtime_gate_discards_idle_keys(self):
        self.runtime.refresh()
        self.verifier.authenticate.side_effect=KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):self.executor.recover(self.headers,self.token)
        self.assertIsNone(self.runtime._sealer)

    def test_separate_stage_time_accumulates_in_one_budget_before_dispatch(self):
        self.budget(5)
        fetch=self.fetch
        def slow(**kwargs):self.tick+=1;return fetch(**kwargs)
        self.secret_client.access_secret_version.side_effect=slow
        access=self.executor.reader.authorize
        def access_time(*args,**kwargs):
            value=access(*args,**kwargs);self.tick+=2;return value
        self.executor.reader.authorize=Mock(side_effect=access_time)
        with self.assertRaises(OperationStopped):self.executor.submit(self.headers,self.token)
        self.assertEqual(self.cloud.submissions,[])
        self.assertIsNone(self.runtime._sealer)

    def test_cancellation_after_preflight_stops_without_second_key_load(self):
        signal=CancellationSignal()
        self.preflight_action(signal.cancel)
        with self.assertRaises(OperationStopped):self.executor.submit(self.headers,self.token,cancellation=signal)
        self.assertEqual(self.cloud.submissions,[])
        self.assertEqual(len(self.requests),1)
        self.assertIsNone(self.runtime._sealer)

    def test_new_intent_is_withheld_when_sealing_consumes_remaining_budget(self):
        self.budget()
        seal=self.runtime.seal
        def slow(query):
            token=seal(query);self.tick+=10;return token
        with patch.object(self.runtime,'seal',side_effect=slow):
            with self.assertRaises(OperationStopped):
                self.executor.issue_intent(self.headers,
                    workspace_id=self.original.query.command.workspace_id,
                    conversation_id=self.original.query.command.conversation_id,
                    visibility=Visibility.TEAM,expected_membership_revision=3,
                    expected_conversation_revision=4,expected_workspace_revision=7,
                    retry_key='deadline-intent')
        self.assertEqual(self.cloud.submissions,[])
        self.assertIsNone(self.runtime._sealer)

    def test_deadline_after_insert_preserves_committed_original_job_for_fresh_recovery(self):
        self.budget()
        self.cloud.insert_rows=[result_row()]
        self.cloud.after_insert=lambda job:setattr(self,'tick',self.tick+10)
        with self.assertRaises(OperationStopped):self.executor.submit(self.headers,self.token)
        self.assertEqual(len(self.cloud.submissions),1)
        self.assertIn(self.original.job_id,self.cloud.jobs)
        self.assertEqual(self.cloud.lookups,[])
        self.assertIsNone(self.runtime._sealer)
        self.cloud.after_insert=None
        recovered=self.executor.recover(self.headers,self.token)
        self.assertEqual(recovered.observation.status,SharingCommitStatus.COMMITTED)
        self.assertEqual(recovered.observation.query_job_id,self.original.job_id)
        self.assertEqual(len(self.cloud.submissions),1)

    def test_cancelled_lost_response_does_not_reconcile_until_new_recovery(self):
        signal=CancellationSignal()
        self.cloud.insert_rows=[result_row()];self.cloud.lost_response=True
        self.cloud.after_insert=lambda job:signal.cancel()
        with self.assertRaises(OperationStopped):self.executor.submit(self.headers,self.token,cancellation=signal)
        self.assertEqual(self.cloud.lookups,[])
        self.cloud.after_insert=None
        self.assertEqual(self.executor.recover(self.headers,self.token).observation.status,
                         SharingCommitStatus.COMMITTED)
        self.assertEqual(len(self.cloud.submissions),1)

    def test_uncertain_dispatch_stop_never_mints_another_job(self):
        self.budget()
        query=self.query
        def timeout(sql,**kwargs):
            if kwargs.get('job_id')==self.original.job_id:
                self.tick+=10
            return query(sql,**kwargs)
        self.client.query.side_effect=timeout
        with self.assertRaises(OperationStopped):self.executor.submit(self.headers,self.token)
        self.client.query.side_effect=self.query
        recovered=self.executor.recover(self.headers,self.token)
        self.assertEqual(recovered.observation.status,SharingCommitStatus.UNKNOWN)
        self.assertEqual(recovered.observation.query_job_id,self.original.job_id)
        self.assertEqual(len(self.cloud.submissions),1)

    def test_expiry_during_original_lookup_denies_result_without_replacement(self):
        self.budget();self.committed()
        self.cloud.before_lookup=lambda job_id:setattr(self,'tick',self.tick+10)
        with self.assertRaises(OperationStopped):self.executor.recover(self.headers,self.token)
        self.assertEqual(self.cloud.submissions,[])
        self.cloud.before_lookup=None
        self.assertEqual(self.executor.recover(self.headers,self.token).observation.status,
                         SharingCommitStatus.COMMITTED)

    def test_expiry_during_result_iteration_denies_release(self):
        self.budget();job=self.committed()
        def slow(**kwargs):self.tick+=10;return [result_row()]
        job.result.side_effect=slow
        with self.assertRaises(OperationStopped):self.executor.recover(self.headers,self.token)
        self.assertEqual(self.cloud.submissions,[])

    def test_retry_and_receipt_stop_exceptions_are_not_unknown_success(self):
        self.budget();self.failed()
        self.cloud.after_read=lambda job:setattr(self,'tick',self.tick+10)
        for name in ('issue_retry','recover_receipt'):
            with self.subTest(name=name),self.assertRaises(OperationStopped):
                getattr(self.executor,name)(self.headers,self.token)
        self.assertEqual(self.cloud.submissions,[])

    def test_known_retry_expiry_stops_without_resubmission(self):
        self.budget();attempt,token=self.candidate();self.committed(attempt)
        self.cloud.before_lookup=lambda job_id:setattr(self,'tick',self.tick+10)
        with self.assertRaises(OperationStopped):self.executor.submit_retry(self.headers,token)
        self.assertEqual(self.cloud.submissions,[])

    def test_retained_retry_recovery_expiry_preserves_original_envelope(self):
        self.budget();attempt,token=self.candidate();self.committed(attempt)
        self.cloud.before_lookup=lambda job_id:setattr(self,'tick',self.tick+10)
        with self.assertRaises(OperationStopped):self.executor.recover_retry(self.headers,token)
        self.cloud.before_lookup=None
        recovered=self.executor.recover_retry(self.headers,token)
        self.assertEqual(recovered.observation.query_job_id,attempt.job_id)
        self.assertEqual(attempt.query.command.deadline_at,self.original.query.command.deadline_at)
        self.assertEqual(self.cloud.submissions,[])

    def test_outer_server_budget_cannot_be_renewed_by_executor_or_sealer(self):
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(1)):
                self.tick+=0.5
                fetch=self.fetch
                def slow(**kwargs):self.tick+=0.3;return fetch(**kwargs)
                self.secret_client.access_secret_version.side_effect=slow
                with self.assertRaises(OperationStopped):self.executor.recover(self.headers,self.token)
                self.assertEqual(self.cloud.submissions,[])

    def test_cancelled_executor_waiter_does_not_clear_active_owners_material(self):
        entered,release,queued=Event(),Event(),Event()
        signal=CancellationSignal()
        def owner():
            with self.runtime.attempt_gate(self.original,identity()):
                entered.set()
                if not release.wait(2):raise RuntimeError('test synchronization timeout')
        def auth(headers):queued.set();return identity()
        self.verifier.authenticate.side_effect=auth
        with ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(owner)
            try:
                self.assertTrue(entered.wait(1))
                material=self.runtime._sealer
                waiting=pool.submit(self.executor.recover,self.headers,self.token,cancellation=signal)
                self.assertTrue(queued.wait(1));signal.cancel()
                with self.assertRaises(OperationStopped):waiting.result(timeout=1)
                self.assertIs(self.runtime._sealer,material)
                self.assertEqual(self.runtime._state.phase,SigningReplicaPhase.READY)
            finally:release.set()
            first.result(timeout=2)
        self.assertEqual(self.cloud.submissions,[])


if __name__ == '__main__':
    unittest.main()
