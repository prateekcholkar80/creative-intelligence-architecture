"""P6 supplied policy/observation contracts; no authentication, checking or release.

An APPROVED value is supplied metadata, not proof of human approval. Callers must
read authoritative policy/access/evidence and run actual checkers before invoking
this reducer. Its decision never grants claim permission or campaign publication.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib

from src.application.workflow_contract import (
    _require, _id, _digest, _timestamp, canonical_json_bytes,
)

CONTRACT_VERSION = 'campaign-acceptance-v1'
RULE_IDS = tuple(sorted(f'{area}{i:02}' for area, count in
                       [('A', 11), ('G', 11), ('C', 14), ('U', 11)]
                       for i in range(1, count + 1)))


class Stage(str, Enum):
    INTERPRETATION = 'INTERPRETATION'
    CLARIFICATION = 'CLARIFICATION'
    GENERATION = 'GENERATION'
    REFINEMENT = 'REFINEMENT'


class Decision(str, Enum):
    PASS = 'PASS'
    CLARIFY = 'CLARIFY'
    NEEDS_REVIEW = 'NEEDS_REVIEW'
    BLOCK = 'BLOCK'


class CheckState(str, Enum):
    PASS = 'PASS'
    FAIL = 'FAIL'
    UNAVAILABLE = 'UNAVAILABLE'
    INCONCLUSIVE = 'INCONCLUSIVE'


class PolicyStatus(str, Enum):
    DRAFT = 'DRAFT'
    APPROVED = 'APPROVED'
    REVOKED = 'REVOKED'


def _hash(payload):
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _utc(value):
    _timestamp(value)
    return value.astimezone(timezone.utc)


def _ordered(values, *, maximum=64):
    _require(type(values) is tuple and 0 < len(values) <= maximum,
             'INVALID_ACCEPTANCE_SCOPE')
    for value in values:
        _id(value)
    _require(values == tuple(sorted(set(values))), 'INVALID_ACCEPTANCE_SCOPE')


@dataclass(frozen=True, slots=True)
class AcceptanceRule:
    rule_id: str
    stages: tuple[Stage, ...]
    failure_action: Decision
    checker_version: str
    stage_exclusion_reason: str | None = None

    def __post_init__(self):
        _require(self.rule_id in RULE_IDS, 'INVALID_ACCEPTANCE_RULE')
        _require(type(self.stages) is tuple and self.stages
                 and all(isinstance(s, Stage) for s in self.stages)
                 and self.stages == tuple(sorted(set(self.stages), key=lambda s: s.value)),
                 'INVALID_ACCEPTANCE_STAGES')
        _require(isinstance(self.failure_action, Decision)
                 and self.failure_action is not Decision.PASS,
                 'INVALID_ACCEPTANCE_FAILURE_ACTION')
        _id(self.checker_version)
        if set(self.stages) != set(Stage):
            _require(self.stage_exclusion_reason is not None,
                     'ACCEPTANCE_EXCLUSION_REASON_REQUIRED')
        if self.stage_exclusion_reason is not None:
            _id(self.stage_exclusion_reason)

    def payload(self):
        return dict(rule_id=self.rule_id, stages=[s.value for s in self.stages],
                    failure_action=self.failure_action.value,
                    checker_version=self.checker_version,
                    stage_exclusion_reason=self.stage_exclusion_reason)


@dataclass(frozen=True, slots=True)
class AcceptancePolicy:
    policy_id: str
    version: str
    specification_sha256: str
    status: PolicyStatus
    approved_by: str | None
    approved_at: datetime | None
    effective_at: datetime
    valid_until: datetime
    countries: tuple[str, ...]
    channels: tuple[str, ...]
    rules: tuple[AcceptanceRule, ...]
    sha256: str = field(init=False)

    def __post_init__(self):
        _id(self.policy_id); _id(self.version); _digest(self.specification_sha256)
        _require(isinstance(self.status, PolicyStatus), 'INVALID_ACCEPTANCE_STATUS')
        for name in ('effective_at', 'valid_until'):
            object.__setattr__(self, name, _utc(getattr(self, name)))
        _require(self.effective_at < self.valid_until, 'INVALID_ACCEPTANCE_WINDOW')
        if self.approved_by is None or self.approved_at is None:
            _require(self.approved_by is None and self.approved_at is None
                     and self.status is PolicyStatus.DRAFT,
                     'ACCEPTANCE_APPROVAL_REQUIRED')
        else:
            _id(self.approved_by)
            object.__setattr__(self, 'approved_at', _utc(self.approved_at))
            _require(self.approved_at <= self.effective_at, 'INVALID_ACCEPTANCE_APPROVAL_TIME')
        _ordered(self.countries); _ordered(self.channels, maximum=16)
        _require(all(len(c) == 2 and c.isascii() and c.isalpha() and c.isupper()
                     for c in self.countries), 'INVALID_ACCEPTANCE_COUNTRY')
        _require(set(self.channels) <= {'META', 'KLAVIYO_EMAIL', 'TIKTOK', 'PINTEREST'},
                 'INVALID_ACCEPTANCE_CHANNEL')
        _require(type(self.rules) is tuple and len(self.rules) == len(RULE_IDS)
                 and all(isinstance(r, AcceptanceRule) for r in self.rules)
                 and tuple(r.rule_id for r in self.rules) == RULE_IDS,
                 'ACCEPTANCE_RULE_COVERAGE_REQUIRED')
        for stage in Stage:
            _require({r.rule_id[0] for r in self.rules if stage in r.stages}
                     == {'A', 'G', 'C', 'U'}, 'ACCEPTANCE_AREA_COVERAGE_REQUIRED')
        object.__setattr__(self, 'sha256', _hash(self.payload()))

    def payload(self):
        return dict(contract_version=CONTRACT_VERSION, policy_id=self.policy_id,
            version=self.version, specification_sha256=self.specification_sha256,
            status=self.status.value, approved_by=self.approved_by,
            approved_at=self.approved_at.isoformat() if self.approved_at else None,
            effective_at=self.effective_at.isoformat(), valid_until=self.valid_until.isoformat(),
            countries=list(self.countries), channels=list(self.channels),
            rules=[r.payload() for r in self.rules])


@dataclass(frozen=True, slots=True)
class AcceptanceContext:
    workspace_id: str
    conversation_id: str
    request_id: str
    candidate_sha256: str
    evidence_sha256: str
    prompt_sha256: str
    model_profile_sha256: str
    stage: Stage
    country: str
    channel: str

    def __post_init__(self):
        for name in ('workspace_id', 'conversation_id', 'request_id', 'country', 'channel'):
            _id(getattr(self, name))
        for name in ('candidate_sha256', 'evidence_sha256', 'prompt_sha256', 'model_profile_sha256'):
            _digest(getattr(self, name))
        _require(isinstance(self.stage, Stage), 'INVALID_ACCEPTANCE_STAGE')

    def payload(self):
        return {name: (getattr(self, name).value if name == 'stage' else getattr(self, name))
                for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class CheckObservation:
    rule_id: str
    checker_version: str
    policy_sha256: str
    context_sha256: str
    checked_at: datetime
    state: CheckState
    reason_code: str
    evidence_reference_sha256: str

    def __post_init__(self):
        _require(self.rule_id in RULE_IDS, 'INVALID_ACCEPTANCE_RULE')
        _id(self.checker_version); _id(self.reason_code)
        for value in (self.policy_sha256, self.context_sha256, self.evidence_reference_sha256):
            _digest(value)
        object.__setattr__(self, 'checked_at', _utc(self.checked_at))
        _require(isinstance(self.state, CheckState), 'INVALID_ACCEPTANCE_CHECK_STATE')

    def payload(self):
        return dict(rule_id=self.rule_id, checker_version=self.checker_version,
            policy_sha256=self.policy_sha256, context_sha256=self.context_sha256,
            checked_at=self.checked_at.isoformat(), state=self.state.value,
            reason_code=self.reason_code, evidence_reference_sha256=self.evidence_reference_sha256)


@dataclass(frozen=True, slots=True)
class AcceptanceResult:
    """Supplied-check reduction only; no claim approval or publication capability."""
    decision: Decision
    failed_rule_ids: tuple[str, ...]
    sha256: str


def evaluate_acceptance(policy: AcceptancePolicy, context: AcceptanceContext,
                        observations: tuple[CheckObservation, ...], *,
                        checked_at: datetime, not_before: datetime) -> AcceptanceResult:
    """Require exact pinned check coverage in a caller-supplied freshness window.

    Authoritative reader must supply not_before for this candidate execution.
    Self-attested timestamps, statuses and observations cannot prove real checks.
    """
    _require(isinstance(policy, AcceptancePolicy) and isinstance(context, AcceptanceContext),
             'INVALID_ACCEPTANCE_INPUT')
    now, floor = _utc(checked_at), _utc(not_before)
    _require(floor <= now and policy.status is PolicyStatus.APPROVED
             and policy.effective_at <= floor <= now < policy.valid_until,
             'ACCEPTANCE_POLICY_INELIGIBLE')
    _require(context.country in policy.countries and context.channel in policy.channels,
             'ACCEPTANCE_SCOPE_DENIED')
    rules = tuple(r for r in policy.rules if context.stage in r.stages)
    _require(type(observations) is tuple and len(observations) == len(rules)
             and all(isinstance(o, CheckObservation) for o in observations)
             and tuple(o.rule_id for o in observations) == tuple(r.rule_id for r in rules),
             'ACCEPTANCE_CHECK_COVERAGE_REQUIRED')
    context_hash = _hash(context.payload())
    actions, failed = [], []
    for rule, observation in zip(rules, observations):
        _require(observation.policy_sha256 == policy.sha256
                 and observation.context_sha256 == context_hash
                 and observation.checker_version == rule.checker_version,
                 'ACCEPTANCE_CHECK_PIN_MISMATCH')
        _require(floor <= observation.checked_at <= now, 'ACCEPTANCE_CHECK_STALE')
        if observation.state is not CheckState.PASS:
            failed.append(rule.rule_id)
            actions.append(rule.failure_action if observation.state is CheckState.FAIL
                           else Decision.NEEDS_REVIEW)
    priority = [Decision.PASS, Decision.CLARIFY, Decision.NEEDS_REVIEW, Decision.BLOCK]
    decision = max(actions, key=priority.index) if actions else Decision.PASS
    digest = _hash(dict(policy_sha256=policy.sha256, context=context.payload(),
        checked_at=now.isoformat(), not_before=floor.isoformat(),
        observations=[o.payload() for o in observations], decision=decision.value))
    return AcceptanceResult(decision, tuple(failed), digest)
