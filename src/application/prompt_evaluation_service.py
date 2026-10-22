"""Server-only IAP/MANAGE_MODELS composition; no default activation or fallback."""
from dataclasses import dataclass
from datetime import datetime, timezone

from src.application.access_policy import Action, AccessDecision, POLICY_VERSION, Reason
from src.application.identity import ensure_fresh
from src.application.prompt_evaluation_contract import PromptPromotionGate, validate_promotion_evaluation
from src.application.prompt_evaluation_query import EvaluationWriteKind, PromptEvaluationWrite, validate_write_receipt
from src.application.prompt_evaluation_storage import EvaluationControl
from src.application.prompt_evaluation_store import EvaluationReadRequest, EvaluationSnapshot, EvaluationStoreError
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import _digest, _id, _require


@dataclass(frozen=True, slots=True)
class EvaluationServiceResult:
    native_job_id: str
    receipt_state: str
    snapshot: EvaluationSnapshot


@dataclass(frozen=True, slots=True)
class EvaluationPromotionCapture:
    workspace_id: str
    prompt_key: str
    control_digest: str
    control_revision: int
    policy_sha256: str
    benchmark_sha256: str
    gate: PromptPromotionGate

    def __post_init__(self):
        _id(self.workspace_id)
        _id(self.prompt_key)
        for value in (self.control_digest, self.policy_sha256, self.benchmark_sha256):
            _digest(value)
        _require(type(self.control_revision) is int and self.control_revision >= 1
                 and isinstance(self.gate, PromptPromotionGate), 'INVALID_EVALUATION_PROMOTION_CAPTURE')


class PromptEvaluationService:
    def __init__(self, *, identity_adapter, access_reader, store, registry_reader, enabled=False):
        if type(enabled) is not bool:
            raise ValueError('Explicit service enablement required')
        self.identity_adapter = identity_adapter
        self.access_reader = access_reader
        self.store = store
        self.registry_reader = registry_reader
        self.enabled = enabled

    def _identity(self, headers, workspace_id):
        _require(self.enabled, 'EVALUATION_SERVICE_DISABLED')
        identity = self.identity_adapter.authenticate(headers)
        return identity, self._authorize(identity, workspace_id)

    def _authorize(self, identity, workspace_id):
        ensure_fresh(identity)
        decision = self.access_reader.authorize(identity, Action.MANAGE_MODELS, workspace_id=workspace_id)
        _require(isinstance(decision, AccessDecision) and decision.allowed
                 and decision.action is Action.MANAGE_MODELS
                 and decision.reason is Reason.ALLOWED_WORKSPACE_ACTION
                 and decision.workspace_id == workspace_id
                 and decision.principal_id == identity.principal.principal_id
                 and type(decision.membership_revision) is int and decision.membership_revision >= 1
                 and decision.resource_id is None and decision.resource_revision is None
                 and decision.policy_version == POLICY_VERSION, 'EVALUATION_ACCESS_INVALID')
        return decision

    def _read(self, identity, decision, workspace_id, prompt_key, evaluation_id=None):
        request = EvaluationReadRequest(workspace_id, prompt_key, identity,
            decision.membership_revision, evaluation_id)
        snapshot = self.store.read(request)
        _require(isinstance(snapshot, EvaluationSnapshot) and snapshot.request == request,
                 'EVALUATION_READ_SCOPE_MISMATCH')
        return snapshot

    def _release(self, identity, before, workspace_id):
        after = self._authorize(identity, workspace_id)
        _require(after.membership_revision >= before.membership_revision, 'EVALUATION_ACCESS_REGRESSION')
        return after

    def publish_control(self, headers, control, *, previous=None):
        _require(isinstance(control, EvaluationControl), 'INVALID_EVALUATION_CONTROL')
        identity, before = self._identity(headers, control.workspace_id)
        _require(control.updated_by == identity.principal.principal_id, 'EVALUATION_ACTOR_MISMATCH')
        captured = self._read(identity, before, control.workspace_id, control.prompt_key)
        reused = next((item for item in captured.control_history if item == control), None)
        if reused is None:
            _require(captured.control == previous, 'EVALUATION_CONTROL_STALE')
            history = captured.control_history
        else:
            # Recover the originally proposed parent chain, preserving later head changes.
            history = captured.control_history[:control.revision - 1]
            expected = None if not history else history[-1]
            _require(previous == expected, 'EVALUATION_CONTROL_PARENT_MISMATCH')
        plan = PromptEvaluationWrite(EvaluationWriteKind.CONTROL, control, identity,
            before.membership_revision, previous=previous, control_history=history)
        self._release(identity, before, control.workspace_id)
        return self._execute(identity, before, plan)

    def record_evaluation(self, headers, *, workspace_id, record):
        from src.application.prompt_evaluation_contract import PromptEvaluationRecord
        from src.application.prompt_registry_reader import PromptRegistryRead
        _require(isinstance(record, PromptEvaluationRecord), 'INVALID_PROMPT_EVALUATION')
        identity, before = self._identity(headers, workspace_id)
        _require(record.recorded_by == identity.principal.principal_id, 'EVALUATION_ACTOR_MISMATCH')
        snapshot = record.registry_record.snapshot
        captured = self._read(identity, before, workspace_id, snapshot.prompt_key)
        _require(captured.control is not None, 'EVALUATION_CONTROL_NOT_FOUND')
        registry = self.registry_reader.read_exact(snapshot.vertex_prompt_id, snapshot.vertex_version_id)
        _require(isinstance(registry, PromptRegistryRead) and registry.state.head == record.registry_record,
                 'EVALUATION_REGISTRY_STALE')
        plan = PromptEvaluationWrite(EvaluationWriteKind.EVALUATION, captured.control, identity,
            before.membership_revision, record=record, registry_state=registry.state,
            control_history=captured.control_history)
        self._release(identity, before, workspace_id)
        return self._execute(identity, before, plan)

    def _execute(self, identity, before, plan, *, recover=False):
        from src.application.prompt_evaluation_store import EvaluationWriteOutcome
        outcome = self.store.recover(plan) if recover else self.store.submit(plan)
        after = self._release(identity, before, plan.control.workspace_id)
        _require(isinstance(outcome, EvaluationWriteOutcome) and outcome.native_job_id == plan.job_id
                 and outcome.native_status is SharingCommitStatus.COMMITTED
                 and outcome.receipt is not None, 'EVALUATION_COMMIT_UNVERIFIED')
        receipt = validate_write_receipt(plan, [outcome.receipt])
        observed = self._read(identity, after, plan.control.workspace_id, plan.control.prompt_key,
            None if plan.record is None else plan.record.evaluation_id)
        self._release(identity, after, plan.control.workspace_id)
        if plan.record is None:
            _require(plan.control in observed.control_history, 'EVALUATION_CONTROL_POSTREAD_MISMATCH')
        else:
            _require(observed.record == plan.record, 'EVALUATION_POSTREAD_MISMATCH')
        return EvaluationServiceResult(plan.job_id, receipt['state'], observed)

    def recover(self, headers, plan):
        _require(isinstance(plan, PromptEvaluationWrite), 'INVALID_EVALUATION_WRITE')
        identity, before = self._identity(headers, plan.control.workspace_id)
        _require(identity.principal == plan.identity.principal and identity.issuer == plan.identity.issuer
                 and identity.subject == plan.identity.subject and identity.audience == plan.identity.audience,
                 'EVALUATION_RECOVERY_ACTOR_MISMATCH')
        # Original parameters and job ID remain untouched; no new submission allowed.
        return self._execute(identity, before, plan, recover=True)

    def promotion_gate(self, headers, *, workspace_id, prompt_key, evaluation_id, target_status):
        identity, before = self._identity(headers, workspace_id)
        captured = self._read(identity, before, workspace_id, prompt_key, evaluation_id)
        self._release(identity, before, workspace_id)
        _require(captured.control is not None and captured.record is not None,
                 'EVALUATION_NOT_FOUND')
        gate = validate_promotion_evaluation(captured.registry_state, captured.record,
            policy=captured.control.policy, benchmark=captured.control.benchmark,
            target_status=target_status, checked_at=max(captured.observed_at, datetime.now(timezone.utc)))
        return EvaluationPromotionCapture(workspace_id, prompt_key, captured.control.digest,
            captured.control.revision, captured.control.policy.sha256,
            captured.control.benchmark.sha256, gate)
