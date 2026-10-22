"""Protected, owned-process operator session over an explicitly configured publisher.

Browser input selects an action and confirms a digest; it cannot supply policy,
identity, authority, SQL, native jobs or approval receipts. This runtime publishes
review-operator authority only. Acceptance/clarification approval is unsupported.
"""
from dataclasses import dataclass
from threading import RLock
from src.application.campaign_operator_policy_publisher import OperatorPolicyPublisher
from src.application.campaign_review_publication_contract import ReviewOperatorPolicy
from src.application.operation_budget import require_owned_worker
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import _require


@dataclass(frozen=True, slots=True)
class OperatorReview:
    command_sha256: str
    policy_sha256: str
    proposed_record_sha256: str
    native_job_id: str


class ProtectedOperatorRuntime:
    """Server-retained exact proposal; reconnect cannot reconstruct a write intent.

    Construct inside the owned worker with trusted server configuration. Every
    operation delegates to publisher authentication and fresh source checks.
    No receipt is a campaign-policy approval or a serving eligibility grant.
    """
    def __init__(self, *, publisher, policy, reason_code, status='ACTIVE'):
        _require(type(publisher) is OperatorPolicyPublisher
            and (policy is None or type(policy) is ReviewOperatorPolicy)
            and status in {'ACTIVE', 'REVOKED'}, 'OPERATOR_RUNTIME_CONFIGURATION')
        self._publisher = publisher
        self._policy, self._reason, self._status = policy, reason_code, status
        self._intent = None
        self._lock = RLock()

    def prepare(self, headers):
        require_owned_worker()
        with self._lock:
            _require(self._policy is not None, 'OPERATOR_RUNTIME_PROPOSAL_UNCONFIGURED')
            intent = self._publisher.prepare(headers, policy=self._policy,
                reason_code=self._reason, status=self._status)
            # Repeated prepares still authenticate through publisher.prepare.
            self._intent = intent
            plan = intent.publication
            return OperatorReview(plan.command_sha256, self._policy.sha256,
                plan.proposed_record_sha256, plan.native_job_id)

    def execute(self, headers, *, confirmed_sha256, recover=False):
        require_owned_worker()
        with self._lock:
            _require(type(recover) is bool and self._intent is not None,
                'OPERATOR_RUNTIME_REVIEW_REQUIRED')
            intent = self._intent
            _require(type(confirmed_sha256) is str
                and confirmed_sha256 == intent.publication.command_sha256,
                'OPERATOR_RUNTIME_REVIEW_NOT_CONFIRMED')
            action = self._publisher.recover if recover else self._publisher.publish
            outcome = action(headers, intent)
            result = dict(native_job_id=outcome.job_id,
                native_status=outcome.native_status.value, approval_verified=False)
            if outcome.native_status is SharingCommitStatus.COMMITTED:
                # Publication proof and current eligibility are deliberately separate.
                # A historical receipt cannot survive revocation as authority.
                state = self._publisher.read_current(headers)
                _require(state.current.sha256 == intent.publication.proposed_record_sha256,
                    'OPERATOR_RUNTIME_AUTHORITY_CHANGED')
                result['current_record_sha256'] = state.current.sha256
            return result

    def current(self, headers):
        require_owned_worker()
        state = self._publisher.read_current(headers)
        return dict(policy_sha256=state.current.policy.sha256,
            record_sha256=state.current.sha256, revision=state.current.policy.revision,
            approval_verified=False)


def compose_operator_runtime(*, client, identity_adapter, manifest, policy,
                             reason_code, enabled=False, status='ACTIVE', clock=None):
    """Install dependencies in an already supervised protected operator worker.

    Configuration comes from the trusted host, never the request. No clients or
    worker scopes are created here, and no policy/control bootstrap is performed.
    """
    require_owned_worker()
    options = dict(client=client, identity_adapter=identity_adapter,
        manifest=manifest, enabled=enabled)
    if clock is not None:
        options['clock'] = clock
    publisher = OperatorPolicyPublisher(**options)
    return ProtectedOperatorRuntime(publisher=publisher, policy=policy,
        reason_code=reason_code, status=status)
