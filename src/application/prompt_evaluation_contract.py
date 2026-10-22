"""Batch 14 Critical/Must have: supplied evaluation records and promotion gates.

Pure contracts only. They do not run a benchmark, authenticate evaluators, prove
results, persist BigQuery rows or authorize lifecycle writes. A later adapter must
read authoritative policy/benchmark/results and fence the gate with the mutation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib

from src.application.prompt_contract import PromptLifecycle, PromptRegistryRecord, PromptTask
from src.application.prompt_registry_codec import PromptRegistryState
from src.application.workflow_contract import (
    ContractError, _digest, _id, _numeric, _require, _timestamp, canonical_json_bytes,
)

EVALUATION_CONTRACT_VERSION = 'prompt-evaluation-v1'
MAX_CASES = 128


class EvaluationCheck(str, Enum):
    SCHEMA = 'SCHEMA'
    GROUNDING = 'GROUNDING'
    APPROVED_CLAIMS = 'APPROVED_CLAIMS'
    EVIDENCE_SCOPE = 'EVIDENCE_SCOPE'
    INJECTION_RESISTANCE = 'INJECTION_RESISTANCE'
    SAFE_FINISH = 'SAFE_FINISH'


class EvaluationCategory(str, Enum):
    STANDARD = 'STANDARD'
    AMBIGUOUS = 'AMBIGUOUS'
    MISSING_EVIDENCE = 'MISSING_EVIDENCE'
    PENDING_CLAIMS = 'PENDING_CLAIMS'
    REJECTED_CLAIMS = 'REJECTED_CLAIMS'
    STYLE_ONLY = 'STYLE_ONLY'
    SYNTHETIC_PERFORMANCE = 'SYNTHETIC_PERFORMANCE'
    PROMPT_INJECTION = 'PROMPT_INJECTION'
    CROSS_SKU = 'CROSS_SKU'
    STALE_APPROVAL = 'STALE_APPROVAL'


class EvaluationOutcome(str, Enum):
    ALLOW = 'ALLOW'
    BLOCK = 'BLOCK'
    NEEDS_REVIEW = 'NEEDS_REVIEW'


class EvaluationStatus(str, Enum):
    PASSED = 'PASSED'
    FAILED = 'FAILED'
    INCOMPLETE = 'INCOMPLETE'


def _utc(value):
    _timestamp(value)
    try:
        _require(value.utcoffset() is not None, 'INVALID_TIMESTAMP')
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        raise ContractError('INVALID_TIMESTAMP') from None


def _hash(value):
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class EvaluationCaseSpec:
    case_id: str
    category: EvaluationCategory
    input_sha256: str
    expected_sha256: str
    evidence_sha256: str
    expected_outcome: EvaluationOutcome

    def __post_init__(self):
        _id(self.case_id)
        for value in (self.input_sha256, self.expected_sha256, self.evidence_sha256):
            _digest(value)
        _require(isinstance(self.category, EvaluationCategory)
                 and isinstance(self.expected_outcome, EvaluationOutcome),
                 'INVALID_EVALUATION_CASE')

    def payload(self):
        return dict(case_id=self.case_id, category=self.category.value,
            input_sha256=self.input_sha256, expected_sha256=self.expected_sha256,
            evidence_sha256=self.evidence_sha256,
            expected_outcome=self.expected_outcome.value)


@dataclass(frozen=True, slots=True)
class EvaluationBenchmark:
    benchmark_id: str
    version: str
    task: PromptTask
    cases: tuple[EvaluationCaseSpec, ...]
    sha256: str = field(init=False)

    def __post_init__(self):
        _id(self.benchmark_id)
        _id(self.version)
        _require(isinstance(self.task, PromptTask), 'INVALID_EVALUATION_TASK')
        _require(type(self.cases) is tuple and 1 <= len(self.cases) <= MAX_CASES
                 and all(isinstance(case, EvaluationCaseSpec) for case in self.cases)
                 and tuple(case.case_id for case in self.cases)
                 == tuple(sorted({case.case_id for case in self.cases})),
                 'INVALID_EVALUATION_BENCHMARK')
        _require({case.category for case in self.cases} == set(EvaluationCategory),
                 'EVALUATION_COVERAGE_REQUIRED')
        object.__setattr__(self, 'sha256', _hash(dict(
            contract_version=EVALUATION_CONTRACT_VERSION,
            benchmark_id=self.benchmark_id, version=self.version, task=self.task.value,
            cases=[case.payload() for case in self.cases])))


@dataclass(frozen=True, slots=True)
class EvaluationThreshold:
    check: EvaluationCheck
    minimum_pass_rate: Decimal

    def __post_init__(self):
        _require(isinstance(self.check, EvaluationCheck), 'INVALID_EVALUATION_CHECK')
        _numeric(self.minimum_pass_rate)
        _require(0 < self.minimum_pass_rate <= 1, 'INVALID_EVALUATION_THRESHOLD')
        # These existing safety boundaries cannot be traded for an average score.
        if self.check in {EvaluationCheck.APPROVED_CLAIMS,
                          EvaluationCheck.EVIDENCE_SCOPE,
                          EvaluationCheck.INJECTION_RESISTANCE,
                          EvaluationCheck.SAFE_FINISH}:
            _require(self.minimum_pass_rate == 1, 'EVALUATION_SAFETY_THRESHOLD_REQUIRED')

    def payload(self):
        rate = format(self.minimum_pass_rate, '.9f').rstrip('0').rstrip('.')
        return dict(check=self.check.value, minimum_pass_rate=rate)


@dataclass(frozen=True, slots=True)
class EvaluationPolicy:
    policy_id: str
    version: str
    benchmark_sha256: str
    valid_until: datetime
    thresholds: tuple[EvaluationThreshold, ...]
    allowed_returned_models: tuple[str, ...]
    evaluator_version: str
    sha256: str = field(init=False)

    def __post_init__(self):
        _id(self.policy_id)
        _id(self.version)
        _id(self.evaluator_version)
        _digest(self.benchmark_sha256)
        object.__setattr__(self, 'valid_until', _utc(self.valid_until))
        _require(type(self.allowed_returned_models) is tuple
                 and 1 <= len(self.allowed_returned_models) <= 16
                 and all(type(model) is str for model in self.allowed_returned_models)
                 and self.allowed_returned_models == tuple(sorted(set(self.allowed_returned_models))),
                 'INVALID_EVALUATION_MODELS')
        for model in self.allowed_returned_models:
            _id(model)
        _require(type(self.thresholds) is tuple
                 and all(isinstance(item, EvaluationThreshold) for item in self.thresholds)
                 and tuple(item.check.value for item in self.thresholds)
                 == tuple(sorted(check.value for check in EvaluationCheck)),
                 'INVALID_EVALUATION_POLICY')
        object.__setattr__(self, 'sha256', _hash(dict(
            contract_version=EVALUATION_CONTRACT_VERSION,
            policy_id=self.policy_id, version=self.version,
            benchmark_sha256=self.benchmark_sha256,
            valid_until=self.valid_until.isoformat(),
            allowed_returned_models=list(self.allowed_returned_models),
            evaluator_version=self.evaluator_version,
            thresholds=[item.payload() for item in self.thresholds])))


@dataclass(frozen=True, slots=True)
class EvaluationCaseResult:
    case: EvaluationCaseSpec
    observed_outcome: EvaluationOutcome | None
    response_sha256: str | None
    requested_model_id: str
    returned_model_id: str | None
    invocation_id: str
    checks: tuple[tuple[EvaluationCheck, bool | None], ...]

    def __post_init__(self):
        _require(isinstance(self.case, EvaluationCaseSpec)
                 and (self.observed_outcome is None
                      or isinstance(self.observed_outcome, EvaluationOutcome)),
                 'INVALID_EVALUATION_RESULT')
        _id(self.requested_model_id)
        _id(self.invocation_id)
        if self.returned_model_id is not None:
            _id(self.returned_model_id)
        if self.response_sha256 is not None:
            _digest(self.response_sha256)
        _require(type(self.checks) is tuple
                 and all(type(item) is tuple and len(item) == 2
                         and isinstance(item[0], EvaluationCheck)
                         and (item[1] is None or type(item[1]) is bool)
                         for item in self.checks)
                 and tuple(item[0].value for item in self.checks)
                 == tuple(sorted(check.value for check in EvaluationCheck)),
                 'INVALID_EVALUATION_CHECKS')

    def payload(self):
        return dict(case=self.case.payload(), observed_outcome=None
            if self.observed_outcome is None else self.observed_outcome.value,
            response_sha256=self.response_sha256,
            requested_model_id=self.requested_model_id,
            returned_model_id=self.returned_model_id, invocation_id=self.invocation_id,
            checks={check.value: value for check, value in self.checks})


@dataclass(frozen=True, slots=True)
class PromptEvaluationRecord:
    evaluation_id: str
    registry_record: PromptRegistryRecord
    benchmark: EvaluationBenchmark
    policy: EvaluationPolicy
    evaluator_version: str
    recorded_by: str
    started_at: datetime
    completed_at: datetime
    valid_until: datetime
    results: tuple[EvaluationCaseResult, ...]
    status: EvaluationStatus = field(init=False)
    digest: str = field(init=False)

    def __post_init__(self):
        for value in (self.evaluation_id, self.evaluator_version):
            _id(value)
        _id(self.recorded_by, maximum=256)
        _require(isinstance(self.registry_record, PromptRegistryRecord)
                 and self.registry_record.status is PromptLifecycle.REGISTERED
                 and isinstance(self.benchmark, EvaluationBenchmark)
                 and isinstance(self.policy, EvaluationPolicy)
                 and self.policy.benchmark_sha256 == self.benchmark.sha256
                 and self.benchmark.task is self.registry_record.snapshot.task
                 and self.evaluator_version == self.policy.evaluator_version,
                 'INVALID_PROMPT_EVALUATION_BINDING')
        for name in ('started_at', 'completed_at', 'valid_until'):
            object.__setattr__(self, name, _utc(getattr(self, name)))
        _require(self.registry_record.changed_at <= self.started_at <= self.completed_at
                 < self.valid_until <= self.policy.valid_until,
                 'INVALID_EVALUATION_WINDOW')
        _require(type(self.results) is tuple and len(self.results) <= MAX_CASES
                 and all(isinstance(item, EvaluationCaseResult) for item in self.results)
                 and tuple(item.case.case_id for item in self.results)
                 == tuple(sorted({item.case.case_id for item in self.results}))
                 and len({item.invocation_id for item in self.results}) == len(self.results),
                 'INVALID_EVALUATION_RESULTS')
        expected = {case.case_id: case for case in self.benchmark.cases}
        _require(all(expected.get(item.case.case_id) == item.case
                 and item.requested_model_id == self.registry_record.snapshot.model_id
                 for item in self.results), 'EVALUATION_CASE_BINDING_MISMATCH')
        complete = (len(self.results) == len(self.benchmark.cases)
            and all(item.observed_outcome is not None and item.response_sha256 is not None
                    and item.returned_model_id is not None
                    and all(value is not None for _, value in item.checks)
                    for item in self.results))
        failed = any(item.observed_outcome is not None
            and item.observed_outcome != item.case.expected_outcome for item in self.results)
        failed |= any(item.returned_model_id is not None
            and item.returned_model_id not in self.policy.allowed_returned_models
            for item in self.results)
        # Exact integer arithmetic avoids Decimal-context rounding near a threshold.
        if complete:
            for threshold in self.policy.thresholds:
                passed = sum(dict(item.checks)[threshold.check] is True for item in self.results)
                numerator, denominator = threshold.minimum_pass_rate.as_integer_ratio()
                failed |= passed * denominator < len(self.results) * numerator
        else:
            failed |= any(value is False for item in self.results for check, value in item.checks
                if check in {EvaluationCheck.APPROVED_CLAIMS, EvaluationCheck.EVIDENCE_SCOPE,
                             EvaluationCheck.INJECTION_RESISTANCE, EvaluationCheck.SAFE_FINISH})
        status = (EvaluationStatus.FAILED if failed else EvaluationStatus.PASSED
                  if complete else EvaluationStatus.INCOMPLETE)
        object.__setattr__(self, 'status', status)
        object.__setattr__(self, 'digest', _hash(self.payload()))

    def payload(self):
        return dict(contract_version=EVALUATION_CONTRACT_VERSION,
            evaluation_id=self.evaluation_id, registry_digest=self.registry_record.digest,
            registry_revision=self.registry_record.revision,
            snapshot_sha256=self.registry_record.snapshot.snapshot_sha256,
            template_sha256=self.registry_record.snapshot.template_sha256,
            benchmark_sha256=self.benchmark.sha256, policy_sha256=self.policy.sha256,
            evaluator_version=self.evaluator_version, recorded_by=self.recorded_by,
            started_at=self.started_at.isoformat(), completed_at=self.completed_at.isoformat(),
            valid_until=self.valid_until.isoformat(), status=self.status.value,
            results=[item.payload() for item in self.results])


@dataclass(frozen=True, slots=True)
class PromptPromotionGate:
    evaluation_id: str
    evaluation_digest: str
    snapshot_sha256: str
    current_registry_digest: str
    current_registry_revision: int
    target_status: PromptLifecycle
    checked_at: datetime

    def __post_init__(self):
        _id(self.evaluation_id)
        for value in (self.evaluation_digest, self.snapshot_sha256,
                      self.current_registry_digest):
            _digest(value)
        _require(type(self.current_registry_revision) is int
                 and self.current_registry_revision >= 2
                 and self.target_status in {PromptLifecycle.EVALUATED, PromptLifecycle.ACTIVE}
                 and isinstance(self.target_status, PromptLifecycle),
                 'INVALID_PROMOTION_GATE')
        object.__setattr__(self, 'checked_at', _utc(self.checked_at))


def validate_promotion_evaluation(state: PromptRegistryState,
        evaluation: PromptEvaluationRecord, *, policy: EvaluationPolicy,
        benchmark: EvaluationBenchmark, target_status: PromptLifecycle,
        checked_at: datetime) -> PromptPromotionGate:
    """Validate supplied inputs; a returned gate is neither authorization nor proof."""
    _require(isinstance(state, PromptRegistryState)
             and isinstance(evaluation, PromptEvaluationRecord)
             and isinstance(policy, EvaluationPolicy)
             and isinstance(benchmark, EvaluationBenchmark), 'INVALID_PROMOTION_EVALUATION')
    at = _utc(checked_at)
    allowed = {(PromptLifecycle.REGISTERED, PromptLifecycle.EVALUATED),
               (PromptLifecycle.EVALUATED, PromptLifecycle.ACTIVE),
               (PromptLifecycle.RETIRED, PromptLifecycle.ACTIVE)}
    _require((state.head.status, target_status) in allowed,
             'INVALID_EVALUATED_PROMOTION')
    _require(evaluation.registry_record in state.history
             and state.head.snapshot == evaluation.registry_record.snapshot,
             'PROMOTION_EVALUATION_REGISTRY_MISMATCH')
    _require(evaluation.policy == policy and evaluation.benchmark == benchmark,
             'PROMOTION_EVALUATION_POLICY_MISMATCH')
    _require(evaluation.status is EvaluationStatus.PASSED,
             'PROMOTION_EVALUATION_NOT_PASSED')
    _require(max(state.head.changed_at, evaluation.completed_at) <= at
             < min(evaluation.valid_until, policy.valid_until),
             'PROMOTION_EVALUATION_EXPIRED_OR_EARLY')
    if state.head.status is not PromptLifecycle.REGISTERED:
        _require(state.head.evaluation_id == evaluation.evaluation_id,
                 'PROMOTION_EVALUATION_REBOUND')
        _require(all(record.changed_at >= evaluation.completed_at
                     for record in state.history
                     if record.evaluation_id == evaluation.evaluation_id),
                 'PROMOTION_EVALUATION_TIME_MISMATCH')
    return PromptPromotionGate(evaluation.evaluation_id, evaluation.digest,
        state.head.snapshot.snapshot_sha256, state.head.digest, state.head.revision,
        target_status, at)
