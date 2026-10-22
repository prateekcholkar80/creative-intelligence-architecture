"""Server-only authorized prompt lifecycle management; disabled by default."""
from dataclasses import dataclass
from datetime import datetime, timezone

from src.application.access_policy import Action, AccessDecision, POLICY_VERSION, Reason
from src.application.identity import ensure_fresh
from src.application.prompt_lifecycle_contract import LifecycleReadRequest, LifecycleSnapshot, LifecycleWrite
from src.application.prompt_lifecycle_query import validate_receipt
from src.application.prompt_lifecycle_store import LifecycleWriteOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import _require


@dataclass(frozen=True, slots=True)
class LifecycleServiceResult:
    native_job_id: str
    receipt_state: str
    prompt_key: str
    applied_revision: int
    control_digest: str


class PromptLifecycleService:
    def __init__(self, *, identity_adapter, access_reader, store, enabled=False):
        if type(enabled) is not bool:
            raise ValueError('Explicit service enablement required')
        self.identity_adapter, self.access_reader, self.store = identity_adapter, access_reader, store
        self.enabled = enabled

    def _authorize(self, identity, workspace_id):
        ensure_fresh(identity)
        access = self.access_reader.authorize(identity, Action.MANAGE_MODELS, workspace_id=workspace_id)
        _require(isinstance(access, AccessDecision) and access.allowed
                 and access.action is Action.MANAGE_MODELS
                 and access.reason is Reason.ALLOWED_WORKSPACE_ACTION
                 and access.workspace_id==workspace_id and access.principal_id==identity.principal.principal_id
                 and type(access.membership_revision) is int and access.membership_revision>=1
                 and access.resource_id is None and access.resource_revision is None
                 and access.policy_version==POLICY_VERSION, 'LIFECYCLE_ACCESS_INVALID')
        return access

    def _identity(self, headers, workspace_id):
        _require(self.enabled, 'LIFECYCLE_SERVICE_DISABLED')
        identity = self.identity_adapter.authenticate(headers)
        return identity, self._authorize(identity, workspace_id)

    def _release(self, identity, before, workspace_id):
        after = self._authorize(identity, workspace_id)
        _require(after.membership_revision>=before.membership_revision, 'LIFECYCLE_ACCESS_REGRESSION')
        return after

    def _read(self, request):
        capture = self.store.read(request)
        _require(isinstance(capture, LifecycleSnapshot) and capture.request==request,
                 'LIFECYCLE_READ_SCOPE_MISMATCH')
        return capture

    def prepare(self, headers, *, workspace_id, prompt_key, task, vertex_prompt_id,
                vertex_version_id, target_status, reason_code, evaluation_id=None):
        identity, access = self._identity(headers, workspace_id)
        request = LifecycleReadRequest(workspace_id, prompt_key, task, vertex_prompt_id,
            vertex_version_id, identity, access.membership_revision, evaluation_id)
        capture = self._read(request)
        proposal = LifecycleWrite(capture, target_status,
            max(capture.observed_at, datetime.now(timezone.utc)), reason_code)
        self._release(identity, access, workspace_id)
        return proposal

    def _actor(self, headers, plan):
        _require(isinstance(plan, LifecycleWrite), 'INVALID_LIFECYCLE_WRITE')
        original = plan.capture.request
        identity, access = self._identity(headers, original.workspace_id)
        expected = original.identity
        _require(identity.principal==expected.principal and identity.issuer==expected.issuer
                 and identity.subject==expected.subject and identity.audience==expected.audience,
                 'LIFECYCLE_ACTOR_MISMATCH')
        return identity, access

    def submit(self, headers, plan):
        identity, access = self._actor(headers, plan)
        ensure_fresh(plan.capture.request.identity)
        _require(access.membership_revision==plan.capture.request.membership_revision,
                 'LIFECYCLE_MEMBERSHIP_STALE')
        return self._execute(identity, access, plan)

    def recover(self, headers, plan):
        identity, access = self._actor(headers, plan)
        # Fresh identity releases results; original identity/expiry/native params
        # remain pinned. Never rebuild or submit an uncertain operation.
        return self._execute(identity, access, plan, recover=True)

    def issue_retry(self, headers, original):
        identity, access = self._actor(headers, original)
        ensure_fresh(original.capture.request.identity)
        _require(access.membership_revision==original.capture.request.membership_revision,
                 'LIFECYCLE_MEMBERSHIP_STALE')
        candidate = self.store.issue_retry(original)
        self._release(identity, access, original.capture.request.workspace_id)
        _require(isinstance(candidate,LifecycleWrite)
                 and candidate.native_slot==original.native_slot+1
                 and candidate.ancestors==original.ancestors+(original,)
                 and candidate.control==original.control and candidate.capture==original.capture,
                 'LIFECYCLE_RETRY_REBOUND')
        return candidate

    def transition(self, headers, **kwargs):
        return self.submit(headers, self.prepare(headers, **kwargs))

    def _execute(self, identity, before, plan, *, recover=False):
        request = plan.capture.request
        outcome = self.store.recover(plan) if recover else self.store.submit(plan)
        access = self._release(identity, before, request.workspace_id)
        _require(isinstance(outcome, LifecycleWriteOutcome) and outcome.native_job_id==plan.job_id
                 and outcome.native_status is SharingCommitStatus.COMMITTED and outcome.receipt is not None,
                 'LIFECYCLE_COMMIT_UNVERIFIED')
        receipt = validate_receipt(plan, [outcome.receipt])
        # Historical receipts prove this command, not today's ACTIVE selection.
        # Fresh complete reads verify every changed version while preserving any
        # later authorized retirement, blocking or replacement.
        for expected in plan.after_states:
            snapshot = expected.head.snapshot
            fresh = LifecycleReadRequest(request.workspace_id, snapshot.prompt_key, snapshot.task,
                snapshot.vertex_prompt_id, snapshot.vertex_version_id, identity, access.membership_revision)
            observed = self._read(fresh)
            _require(plan.control in observed.control_history
                     and observed.target.history[:len(expected.history)]==expected.history,
                     'LIFECYCLE_POSTREAD_MISMATCH')
            access = self._release(identity, access, request.workspace_id)
        return LifecycleServiceResult(plan.job_id, receipt['state'], plan.control.prompt_key,
            plan.control.revision, plan.control.digest)
