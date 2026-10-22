"""Server auth/access/write/read/gate composition with offline fixtures only."""
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

from google.auth import jwt

from src.application.access_policy import AccessDecision, Action, Reason
from src.application.identity import ASSERTION_HEADER, IapIdentityAdapter, IapSettings, ISSUER, IdentityError
from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_evaluation_service import PromptEvaluationService
from src.application.prompt_evaluation_storage import EvaluationControl
from src.application.prompt_evaluation_store import EvaluationSnapshot, EvaluationWriteOutcome
from src.application.prompt_registry_reader import PromptRegistryRead, PromptRegistryReadRequest
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError
from tests.test_access_reader import IDENTITY, EPOCH, AUDIENCE, NOW as IDENTITY_NOW
from tests.test_identity import signer_and_public_key
from tests.test_prompt_contract import NOW, advance
from tests.test_prompt_evaluation_contract import evaluation, state
from tests.test_prompt_evaluation_query import result_plan, receipt
from tests.test_prompt_evaluation_storage import control


class Store:
    def __init__(self):
        self.history = []
        self.records = {}
        self.registry = state()
        self.writes = []
        self.recoveries = []
        self.status = SharingCommitStatus.COMMITTED
        self.receipt_missing = False

    def read(self, request):
        record = self.records.get(request.evaluation_id) if request.evaluation_id is not None else None
        return EvaluationSnapshot(request, self.history[-1] if self.history else None,
            tuple(self.history), record, None if record is None else self.registry,
            NOW + timedelta(seconds=20))

    def submit(self, plan):
        self.writes.append(plan)
        if self.status is SharingCommitStatus.COMMITTED:
            if plan.record is None:
                if plan.control not in self.history:
                    self.history.append(plan.control)
            else:
                self.records[plan.record.evaluation_id] = plan.record
        return EvaluationWriteOutcome(plan.job_id, self.status,
            receipt(plan) if self.status is SharingCommitStatus.COMMITTED and not self.receipt_missing else None, True)

    def recover(self, plan):
        self.recoveries.append(plan)
        return EvaluationWriteOutcome(plan.job_id, self.status,
            receipt(plan) if not self.receipt_missing else None)


class EvaluationServiceTests(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.identity = Mock()
        self.identity.authenticate.return_value = IDENTITY
        self.access = Mock()
        self.access.authorize.side_effect = lambda identity, action, **kw: self.decision(kw['workspace_id'])
        self.registry = Mock()
        value = state()
        self.registry.read_exact.return_value = PromptRegistryRead(
            PromptRegistryReadRequest.exact(value.head.snapshot.vertex_prompt_id,
                value.head.snapshot.vertex_version_id), value, NOW + timedelta(seconds=20))
        self.service = PromptEvaluationService(identity_adapter=self.identity, access_reader=self.access,
            store=self.store, registry_reader=self.registry, enabled=True)
        self.control = replace(control(), updated_by=IDENTITY.principal.principal_id)
        self.record = replace(evaluation(), recorded_by=IDENTITY.principal.principal_id)
        self.headers = {ASSERTION_HEADER: 'not-a-real-token'}
        patch('src.application.identity.time.time', return_value=EPOCH).start()
        clock = patch('src.application.prompt_evaluation_service.datetime').start()
        clock.now.return_value = NOW + timedelta(seconds=21)
        self.addCleanup(patch.stopall)

    def decision(self, workspace_id):
        return AccessDecision(True, Action.MANAGE_MODELS, Reason.ALLOWED_WORKSPACE_ACTION,
            workspace_id, IDENTITY.principal.principal_id, 3, None, None)

    def publish(self, current=None, previous=None):
        return self.service.publish_control(self.headers, self.control if current is None else current,
            previous=previous)

    def record_result(self):
        return self.service.record_evaluation(self.headers, workspace_id=self.control.workspace_id,
            record=self.record)

    def test_defaults_disabled_before_identity_reads_or_writes(self):
        self.service.enabled = False
        with self.assertRaises(ContractError):
            self.publish()
        self.identity.authenticate.assert_not_called()
        self.assertEqual(self.store.writes, [])

    def test_authentication_and_access_denials_stop_before_write(self):
        self.identity.authenticate.side_effect = IdentityError('AUTHENTICATION_REQUIRED')
        with self.assertRaises(IdentityError):
            self.publish()
        self.identity.authenticate.side_effect = None
        for changes in ({'allowed': False}, {'principal_id': 'wrong'},
                        {'workspace_id': 'wrong'}, {'membership_revision': None}):
            self.access.authorize.side_effect = None
            self.access.authorize.return_value = replace(self.decision(self.control.workspace_id), **changes)
            with self.assertRaises(ContractError):
                self.publish()
        self.assertEqual(self.store.writes, [])

    def test_control_publish_rechecks_access_and_exact_stored_history(self):
        result = self.publish()
        self.assertEqual(result.receipt_state, 'APPLIED')
        self.assertEqual(result.snapshot.control, self.control)
        self.assertEqual(len(self.store.writes), 1)
        self.assertEqual(self.access.authorize.call_count, 4)

    def test_control_cas_stale_conflict_and_immutable_version_denied(self):
        self.publish()
        next_control = replace(self.control, revision=2, parent_digest=self.control.digest,
            updated_at=self.control.updated_at + timedelta(seconds=1),
            policy=replace(self.control.policy, version='v2'))
        with self.assertRaises(ContractError):
            self.publish(next_control)
        self.publish(next_control, previous=self.control)
        original_count = len(self.store.writes)
        conflicting = replace(next_control, revision=3, parent_digest=next_control.digest,
            updated_at=next_control.updated_at + timedelta(seconds=1),
            policy=replace(next_control.policy, allowed_returned_models=('changed',)))
        with self.assertRaises(ContractError):
            self.publish(conflicting, previous=next_control)
        self.assertEqual(len(self.store.writes), original_count)

    def test_historical_control_replay_preserves_later_selection(self):
        self.publish()
        changed = replace(self.control, revision=2, parent_digest=self.control.digest,
            policy=replace(self.control.policy, version='v2'),
            updated_at=self.control.updated_at + timedelta(seconds=1))
        self.publish(changed, previous=self.control)
        self.publish()
        self.assertEqual(self.store.history[-1], changed)
        self.assertEqual(len(self.store.history), 2)

    def test_result_publication_binds_registry_control_actor_and_postread(self):
        self.publish()
        result = self.record_result()
        self.assertEqual(result.snapshot.record, self.record)
        self.assertEqual(self.store.writes[-1].record, self.record)
        self.assertEqual(self.store.writes[-1].registry_state, state())

    def test_missing_control_actor_or_registry_drift_deny_before_results(self):
        with self.assertRaises(ContractError):
            self.record_result()
        self.publish()
        for record in (evaluation(), replace(self.record,
                registry_record=replace(self.record.registry_record, changed_by='other'))):
            with self.assertRaises(ContractError):
                self.service.record_evaluation(self.headers, workspace_id=self.control.workspace_id, record=record)
        self.assertNotIn(self.record.evaluation_id, self.store.records)

    def test_unknown_or_commit_without_receipt_cannot_release_success(self):
        for status, missing in ((SharingCommitStatus.UNKNOWN, False),
                                (SharingCommitStatus.COMMITTED, True)):
            self.store.status, self.store.receipt_missing = status, missing
            with self.assertRaises(ContractError):
                self.publish()
        self.assertEqual(len(self.store.writes), 2)

    def test_access_revoked_after_write_withholds_success(self):
        allowed = self.decision(self.control.workspace_id)
        self.access.authorize.side_effect = [allowed, allowed, replace(allowed, allowed=False)]
        with self.assertRaises(ContractError):
            self.publish()
        self.assertEqual(self.store.history, [self.control])

    def test_retained_plan_recovery_uses_fresh_auth_but_never_submits(self):
        self.publish()
        self.record_result()
        plan = self.store.writes[-1]
        fresh = replace(IDENTITY, expires_at=IDENTITY.expires_at + 1)
        self.identity.authenticate.return_value = fresh
        count = len(self.store.writes)
        result = self.service.recover(self.headers, plan)
        self.assertEqual(result.snapshot.record, self.record)
        self.assertEqual(len(self.store.writes), count)
        self.assertIs(self.store.recoveries[-1], plan)

    def test_recovery_actor_or_audience_change_denied(self):
        self.publish()
        plan = self.store.writes[-1]
        self.identity.authenticate.return_value = replace(IDENTITY,
            audience='/projects/123456789/locations/asia-south1/services/other-service')
        with self.assertRaises(ContractError):
            self.service.recover(self.headers, plan)
        self.assertEqual(self.store.recoveries, [])

    def test_fresh_promotion_gate_carries_exact_current_registry_fence(self):
        self.publish()
        self.record_result()
        gate = self.service.promotion_gate(self.headers, workspace_id=self.control.workspace_id,
            prompt_key=self.control.prompt_key, evaluation_id=self.record.evaluation_id,
            target_status=PromptLifecycle.EVALUATED)
        self.assertEqual(gate.gate.current_registry_digest, state().head.digest)
        self.assertEqual(gate.gate.evaluation_digest, self.record.digest)
        self.assertEqual(gate.control_digest, self.control.digest)
        self.assertEqual(gate.control_revision, 1)
        self.assertEqual(gate.workspace_id, self.control.workspace_id)

    def test_policy_change_expiry_or_failed_result_blocks_fresh_gate(self):
        self.publish()
        self.record_result()
        changed = replace(self.control, revision=2, parent_digest=self.control.digest,
            policy=replace(self.control.policy, version='v2'),
            updated_at=self.control.updated_at + timedelta(seconds=1))
        self.publish(changed, previous=self.control)
        with self.assertRaises(ContractError):
            self.service.promotion_gate(self.headers, workspace_id=self.control.workspace_id,
                prompt_key=self.control.prompt_key, evaluation_id=self.record.evaluation_id,
                target_status=PromptLifecycle.EVALUATED)
        self.store.history = [self.control]
        with patch('src.application.prompt_evaluation_service.datetime') as clock:
            clock.now.return_value = self.record.valid_until
            with self.assertRaises(ContractError):
                self.service.promotion_gate(self.headers, workspace_id=self.control.workspace_id,
                    prompt_key=self.control.prompt_key, evaluation_id=self.record.evaluation_id,
                    target_status=PromptLifecycle.EVALUATED)

    def test_failed_and_incomplete_results_persist_but_cannot_promote(self):
        self.publish()
        original = self.record
        for i, results in enumerate(((), (replace(original.results[0], returned_model_id='unapproved'),)
                + original.results[1:])):
            self.record = replace(original, evaluation_id=f'negative-evaluation-{i}', results=results)
            stored = self.record_result()
            self.assertEqual(stored.snapshot.record, self.record)
            with self.assertRaises(ContractError):
                self.service.promotion_gate(self.headers, workspace_id=self.control.workspace_id,
                    prompt_key=self.control.prompt_key, evaluation_id=self.record.evaluation_id,
                    target_status=PromptLifecycle.EVALUATED)

    def test_real_es256_assertion_accepted_and_tampering_denied_before_reads(self):
        signer, public = signer_and_public_key()
        certs = Mock(return_value=SimpleNamespace(status=200,
            data=json.dumps({'fixture-key': public}).encode()))
        self.service.identity_adapter = IapIdentityAdapter(IapSettings(AUDIENCE))
        token = jwt.encode(signer, dict(iss=ISSUER, aud=AUDIENCE, sub=IDENTITY.subject,
            iat=EPOCH - 10, exp=EPOCH + 590)).decode('ascii')
        with patch('src.application.identity._BoundedKeyRequest', return_value=certs), \
                patch('google.auth._helpers.utcnow', return_value=IDENTITY_NOW.replace(tzinfo=None)):
            self.service.publish_control({ASSERTION_HEADER: token}, self.control)
            calls = self.access.authorize.call_count
            with self.assertRaises(IdentityError):
                self.service.publish_control({ASSERTION_HEADER: 'x' + token}, self.control)
            self.assertEqual(self.access.authorize.call_count, calls)


if __name__ == '__main__':
    unittest.main()
