"""Authorized lifecycle flows with signed offline IAP and in-memory transactions."""
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace
from unittest.mock import Mock, patch
import unittest

from google.auth import jwt

from src.application.access_policy import AccessDecision, Action, Reason
from src.application.identity import ASSERTION_HEADER, CLOCK_SKEW_SECONDS, IapIdentityAdapter, IapSettings, ISSUER, IdentityError
from src.application.prompt_contract import PromptLifecycle, initial_prompt_record
from src.application.prompt_evaluation_store import EvaluationReadRequest, EvaluationSnapshot
from src.application.prompt_lifecycle_contract import LifecycleSnapshot, LifecycleWrite
from src.application.prompt_lifecycle_service import PromptLifecycleService
from src.application.prompt_lifecycle_store import LifecycleWriteOutcome
from src.application.prompt_registry_codec import PromptRegistryState
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError
from tests.test_access_reader import IDENTITY, EPOCH, AUDIENCE, NOW as IDENTITY_NOW
from tests.test_identity import signer_and_public_key
from tests.test_prompt_contract import NOW, draft, advance
from tests.test_prompt_evaluation_contract import evaluation, state
from tests.test_prompt_evaluation_storage import control as evaluation_control
from tests.test_prompt_lifecycle_query import receipt


class Store:
    def __init__(self):
        self.states = {'4':state()}
        self.control = None
        self.history = ()
        self.evaluations = {'evaluation-1':evaluation()}
        self.evaluation_control = evaluation_control()
        self.evaluation_history = (self.evaluation_control,)
        self.jobs, self.writes, self.recoveries, self.reads = {}, [], [], []
        self.outcome = SharingCommitStatus.COMMITTED
        self.no_receipt = False
        self.omit_retirement = False

    def read(self, request):
        self.reads.append(request)
        current = self.states[request.vertex_version_id]
        active = next((s for s in self.states.values() if s.head.status is PromptLifecycle.ACTIVE), None)
        at = max(NOW+timedelta(seconds=100), current.head.changed_at,
            NOW if self.control is None else self.control.changed_at)
        ev = None
        anchor = None
        if request.evaluation_id is not None:
            ev = EvaluationSnapshot(EvaluationReadRequest(request.workspace_id, request.prompt_key,
                request.identity, request.membership_revision, request.evaluation_id),
                self.evaluation_control, self.evaluation_history, self.evaluations[request.evaluation_id], current, at)
            anchor = self.evaluation_history[0]
        return LifecycleSnapshot(request, self.control, self.history, current, active, ev, at, anchor)

    def submit(self, p):
        self.writes.append(p)
        existing = self.jobs.get(p.job_id)
        if existing is not None:
            if existing.command_digest!=p.command_digest:
                return LifecycleWriteOutcome(p.job_id, SharingCommitStatus.UNKNOWN)
            return LifecycleWriteOutcome(p.job_id, self.outcome,
                None if self.no_receipt else receipt(p, 'REUSED'))
        if self.outcome is SharingCommitStatus.COMMITTED:
            if self.control!=p.capture.control or self.history!=p.capture.control_history:
                return LifecycleWriteOutcome(p.job_id, SharingCommitStatus.NOT_COMMITTED)
            for expected in p.before_states:
                if self.states[expected.head.snapshot.vertex_version_id]!=expected:
                    return LifecycleWriteOutcome(p.job_id, SharingCommitStatus.NOT_COMMITTED)
            if p.capture.evaluation is not None and self.evaluation_control!=p.capture.evaluation.control:
                return LifecycleWriteOutcome(p.job_id, SharingCommitStatus.NOT_COMMITTED)
            # Supplied atomic fixture, not execution of the generated native SQL.
            for new in p.after_states:
                if self.omit_retirement and new.head.status is PromptLifecycle.RETIRED:
                    continue
                self.states[new.head.snapshot.vertex_version_id] = new
            self.control, self.history = p.control, self.history+(p.control,)
            self.jobs[p.job_id] = p
        return LifecycleWriteOutcome(p.job_id, self.outcome,
            None if self.no_receipt or self.outcome is not SharingCommitStatus.COMMITTED else receipt(p), True)

    def recover(self, p):
        self.recoveries.append(p)
        existing = self.jobs.get(p.job_id)
        if existing is None or existing.command_digest!=p.command_digest:
            return LifecycleWriteOutcome(p.job_id, SharingCommitStatus.UNKNOWN)
        return LifecycleWriteOutcome(p.job_id, self.outcome, None if self.no_receipt else receipt(p))


class LifecycleServiceTests(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.identity = Mock()
        self.identity.authenticate.return_value = IDENTITY
        self.access = Mock()
        self.access.authorize.side_effect = lambda identity, action, **kw: self.decision(kw['workspace_id'])
        self.service = PromptLifecycleService(identity_adapter=self.identity, access_reader=self.access,
            store=self.store, enabled=True)
        self.headers = {ASSERTION_HEADER:'offline-fixture'}
        patch('src.application.identity.time.time', return_value=EPOCH).start()
        clock = patch('src.application.prompt_lifecycle_service.datetime').start()
        clock.now.return_value = NOW+timedelta(seconds=101)
        self.addCleanup(patch.stopall)

    def decision(self, workspace):
        return AccessDecision(True, Action.MANAGE_MODELS, Reason.ALLOWED_WORKSPACE_ACTION,
            workspace, IDENTITY.principal.principal_id, 3, None, None)

    def prepare(self, status=PromptLifecycle.EVALUATED, version='4', evaluation_id='evaluation-1'):
        s = self.store.states[version].head.snapshot
        return self.service.prepare(self.headers, workspace_id='workspace-1', prompt_key=s.prompt_key,
            task=s.task, vertex_prompt_id=s.vertex_prompt_id, vertex_version_id=version,
            target_status=status, reason_code='OPERATOR_REVIEWED', evaluation_id=evaluation_id)

    def apply(self, status=PromptLifecycle.EVALUATED, version='4', evaluation_id='evaluation-1'):
        p = self.prepare(status, version, evaluation_id)
        return self.service.submit(self.headers, p), p

    def add_version(self):
        s = replace(draft().snapshot, vertex_version_id='2')
        d = initial_prompt_record(s, imported_at=NOW, imported_by=draft().changed_by)
        r = advance(d, PromptLifecycle.REGISTERED, 1)
        value = PromptRegistryState(r, (d,r))
        self.store.states['2'] = value
        self.store.evaluations['evaluation-2'] = replace(evaluation(), evaluation_id='evaluation-2', registry_record=r)

    def test_default_disabled_denies_before_identity_or_reads(self):
        self.service.enabled = False
        with self.assertRaises(ContractError):
            self.prepare()
        self.identity.authenticate.assert_not_called()
        self.assertEqual(self.store.reads, [])

    def test_invalid_auth_role_scope_or_stale_membership_never_submit(self):
        self.identity.authenticate.side_effect = IdentityError('AUTHENTICATION_REQUIRED')
        with self.assertRaises(IdentityError):
            self.prepare()
        self.identity.authenticate.side_effect = None
        p = self.prepare()
        self.access.authorize.side_effect = None
        for changes in ({'allowed':False}, {'workspace_id':'other'}, {'principal_id':'other'},
                        {'membership_revision':4}, {'resource_id':'resource'}):
            self.access.authorize.return_value = replace(self.decision('workspace-1'), **changes)
            with self.assertRaises(ContractError):
                self.service.submit(self.headers,p)
        self.assertEqual(self.store.writes, [])

    def test_registration_is_explicit_and_does_not_activate(self):
        self.store.states['4'] = PromptRegistryState(draft(), (draft(),))
        result, p = self.apply(PromptLifecycle.REGISTERED, evaluation_id=None)
        self.assertEqual(result.receipt_state, 'APPLIED')
        self.assertIs(self.store.states['4'].head.status, PromptLifecycle.REGISTERED)
        self.assertIsNone(self.store.control.active_snapshot_sha256)

    def test_evaluation_activation_and_retirement_record_actor_and_history(self):
        self.apply()
        result, activation = self.apply(PromptLifecycle.ACTIVE)
        self.assertEqual(result.control_digest, activation.control.digest)
        self.assertIs(self.store.states['4'].head.status, PromptLifecycle.ACTIVE)
        self.assertEqual(self.store.states['4'].head.changed_by, IDENTITY.principal.principal_id)
        self.apply(PromptLifecycle.RETIRED, evaluation_id=None)
        self.assertIsNone(self.store.control.active_snapshot_sha256)
        self.assertEqual(len(self.store.history),3)

    def test_atomic_replacement_then_rollback_retire_previous_active(self):
        self.apply()
        self.apply(PromptLifecycle.ACTIVE)
        self.add_version()
        self.apply(version='2',evaluation_id='evaluation-2')
        _, replacement = self.apply(PromptLifecycle.ACTIVE, version='2', evaluation_id='evaluation-2')
        self.assertEqual(len(replacement.after_states),2)
        self.assertIs(self.store.states['4'].head.status, PromptLifecycle.RETIRED)
        self.assertIs(self.store.states['2'].head.status, PromptLifecycle.ACTIVE)
        _, rollback = self.apply(PromptLifecycle.ACTIVE)
        self.assertIs(self.store.states['4'].head.status, PromptLifecycle.ACTIVE)
        self.assertIs(self.store.states['2'].head.status, PromptLifecycle.RETIRED)
        self.assertEqual(self.store.states['4'].head.evaluation_id, 'evaluation-1')

    def test_blocking_active_clears_pin_and_blocked_version_cannot_restore(self):
        self.apply()
        self.apply(PromptLifecycle.ACTIVE)
        self.apply(PromptLifecycle.BLOCKED, evaluation_id=None)
        self.assertIsNone(self.store.control.active_snapshot_sha256)
        with self.assertRaises(ContractError):
            self.prepare(PromptLifecycle.ACTIVE)

    def test_current_policy_change_before_dispatch_denies_without_mutation(self):
        p = self.prepare()
        self.store.evaluation_control = replace(self.store.evaluation_control,
            revision=2, parent_digest=self.store.evaluation_control.digest,
            policy=replace(self.store.evaluation_control.policy,version='v2'))
        with self.assertRaises(ContractError):
            self.service.submit(self.headers,p)
        self.assertEqual(self.store.history,())

    def test_unknown_or_missing_business_receipt_never_release_success(self):
        for status, missing in ((SharingCommitStatus.UNKNOWN,False),(SharingCommitStatus.COMMITTED,True)):
            self.store = Store()
            self.service.store = self.store
            self.store.outcome, self.store.no_receipt = status,missing
            p = self.prepare()
            with self.assertRaises(ContractError):
                self.service.submit(self.headers,p)

    def test_revocation_after_commit_withholds_result_without_reversing_commit(self):
        p = self.prepare()
        allowed = self.decision('workspace-1')
        self.access.authorize.side_effect = [allowed,replace(allowed,allowed=False)]
        with self.assertRaises(ContractError):
            self.service.submit(self.headers,p)
        self.assertIs(self.store.states['4'].head.status,PromptLifecycle.EVALUATED)

    def test_retained_plan_duplicate_and_recovery_preserve_later_lifecycle(self):
        _, original = self.apply()
        self.apply(PromptLifecycle.ACTIVE)
        self.apply(PromptLifecycle.BLOCKED,evaluation_id=None)
        count = len(self.store.writes)
        self.identity.authenticate.return_value = replace(IDENTITY,expires_at=IDENTITY.expires_at+1)
        result = self.service.recover(self.headers,original)
        self.assertEqual(result.applied_revision,original.control.revision)
        self.assertEqual(len(self.store.writes),count)
        self.assertIs(self.store.states['4'].head.status,PromptLifecycle.BLOCKED)
        self.assertIs(self.store.recoveries[-1],original)

    def test_recovery_wrong_actor_audience_or_unknown_original_denies(self):
        p = self.prepare()
        with self.assertRaises(ContractError):
            self.service.recover(self.headers,p)
        self.assertEqual(self.store.writes,[])
        self.identity.authenticate.return_value = replace(IDENTITY,audience=AUDIENCE+'-other')
        count = len(self.store.recoveries)
        with self.assertRaises(ContractError):
            self.service.recover(self.headers,p)
        self.assertEqual(len(self.store.recoveries),count)

    def test_stale_prepared_parent_or_competing_intent_cannot_write(self):
        original = self.prepare()
        other = replace(original,reason_code='OTHER_REVIEW')
        self.service.submit(self.headers,original)
        with self.assertRaises(ContractError):
            self.service.submit(self.headers,other)
        self.assertEqual(len(self.store.history),1)

    def test_postread_tampering_or_partial_history_denies_release(self):
        p = self.prepare()
        original = self.store.read
        def bad(request):
            observed = original(request)
            return replace(observed, request=replace(request,workspace_id='other-workspace'))
        self.store.read = bad
        with self.assertRaises(ContractError):
            self.service.submit(self.headers,p)

    def test_expired_evaluation_cannot_activate_or_rollback(self):
        self.apply()
        with patch('src.application.prompt_lifecycle_service.datetime') as clock:
            clock.now.return_value = evaluation().valid_until
            with self.assertRaises(ContractError):
                self.prepare(PromptLifecycle.ACTIVE)
        self.assertEqual(len(self.store.writes),1)

    def test_partial_replacement_is_denied_by_fresh_postreads(self):
        self.apply()
        self.apply(PromptLifecycle.ACTIVE)
        self.add_version()
        self.apply(version='2',evaluation_id='evaluation-2')
        p = self.prepare(PromptLifecycle.ACTIVE,version='2',evaluation_id='evaluation-2')
        self.store.omit_retirement = True
        with self.assertRaises(ContractError):
            self.service.submit(self.headers,p)

    def test_expired_original_assertion_denies_submit_but_fresh_recovery_is_read_only(self):
        _,p = self.apply()
        fresh_epoch = IDENTITY.expires_at+CLOCK_SKEW_SECONDS+1
        fresh = replace(IDENTITY,issued_at=fresh_epoch-10,expires_at=fresh_epoch+590)
        self.identity.authenticate.return_value = fresh
        count = len(self.store.writes)
        with patch('src.application.identity.time.time',return_value=fresh_epoch):
            with self.assertRaises(IdentityError):
                self.service.submit(self.headers,p)
            result = self.service.recover(self.headers,p)
        self.assertEqual(result.control_digest,p.control.digest)
        self.assertEqual(len(self.store.writes),count)

    def test_retry_issuance_requires_same_actor_fresh_access_and_preserves_original(self):
        p = self.prepare()
        retry = replace(p,native_slot=2,ancestors=(p,))
        self.store.issue_retry = Mock(return_value=retry)
        candidate = self.service.issue_retry(self.headers,p)
        self.assertIs(candidate,retry)
        self.assertEqual(self.store.writes,[])
        self.store.issue_retry.assert_called_once_with(p)
        self.identity.authenticate.return_value = replace(IDENTITY,audience=AUDIENCE+'-other')
        with self.assertRaises(ContractError):
            self.service.issue_retry(self.headers,p)
        self.assertEqual(self.store.issue_retry.call_count,1)

    def test_retry_unknown_or_post_proof_revocation_cannot_release_a_proposal(self):
        p = self.prepare()
        self.store.issue_retry = Mock(side_effect=ContractError('LIFECYCLE_RETRY_NONCOMMIT_UNVERIFIED'))
        with self.assertRaises(ContractError):
            self.service.issue_retry(self.headers,p)
        self.store.issue_retry.side_effect = None
        self.store.issue_retry.return_value = replace(p,native_slot=2,ancestors=(p,))
        allowed = self.decision('workspace-1')
        self.access.authorize.side_effect = [allowed,replace(allowed,allowed=False)]
        with self.assertRaises(ContractError):
            self.service.issue_retry(self.headers,p)
        self.assertEqual(self.store.writes,[])

    def test_store_cannot_rebind_retry_proposal_to_another_transition(self):
        p = self.prepare()
        other = replace(p,reason_code='OTHER_REVIEW')
        self.store.issue_retry = Mock(return_value=replace(other,native_slot=2,ancestors=(other,)))
        with self.assertRaises(ContractError):
            self.service.issue_retry(self.headers,p)
        self.assertEqual(self.store.writes,[])

    def test_real_es256_assertion_and_tampering_denial_before_reads(self):
        signer,public = signer_and_public_key()
        certs = Mock(return_value=SimpleNamespace(status=200,
            data=json.dumps({'fixture-key':public}).encode()))
        self.service.identity_adapter = IapIdentityAdapter(IapSettings(AUDIENCE))
        token = jwt.encode(signer,dict(iss=ISSUER,aud=AUDIENCE,sub=IDENTITY.subject,
            iat=EPOCH-10,exp=EPOCH+590)).decode('ascii')
        s = state().head.snapshot
        args = dict(workspace_id='workspace-1',prompt_key=s.prompt_key,task=s.task,
            vertex_prompt_id=s.vertex_prompt_id,vertex_version_id=s.vertex_version_id,
            target_status=PromptLifecycle.EVALUATED,reason_code='OPERATOR_REVIEWED',evaluation_id='evaluation-1')
        with patch('src.application.identity._BoundedKeyRequest',return_value=certs), \
                patch('google.auth._helpers.utcnow',return_value=IDENTITY_NOW.replace(tzinfo=None)):
            p = self.service.prepare({ASSERTION_HEADER:token},**args)
            self.service.submit({ASSERTION_HEADER:token},p)
            calls = len(self.store.reads)
            with self.assertRaises(IdentityError):
                self.service.prepare({ASSERTION_HEADER:'x'+token},**args)
            self.assertEqual(len(self.store.reads),calls)


if __name__ == '__main__':
    unittest.main()
