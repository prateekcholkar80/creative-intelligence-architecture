"""Synthetic Batch 14 observations for owned SQL probes, never quality evidence."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.application.identity import ISSUER, VerifiedIdentity, principal_for_subject
from src.application.prompt_contract import PromptLifecycle, initial_prompt_record, propose_prompt_transition
from src.application.prompt_evaluation_contract import (
    EvaluationBenchmark, EvaluationCaseResult, EvaluationCaseSpec, EvaluationCategory,
    EvaluationCheck, EvaluationOutcome, EvaluationPolicy, EvaluationThreshold,
    PromptEvaluationRecord,
)
from src.application.prompt_evaluation_query import EvaluationWriteKind, PromptEvaluationWrite
from src.application.prompt_evaluation_storage import EvaluationControl
from src.application.prompt_registry_codec import PromptRegistryState
from src.validation.control_fixtures import SUBJECT, controls
from src.validation.prompt_registry_integration import fixture_query

WORKSPACE = 'fixture-evaluation-workspace'
UNRELATED = 'fixture-unrelated-workspace'


def fixtures(now=None):
    """Build valid supplied records without authenticating or invoking a model."""
    now = datetime.now(timezone.utc) if now is None else now
    at = now - timedelta(seconds=10)
    actor = principal_for_subject(ISSUER, SUBJECT)
    identity = VerifiedIdentity(actor, ISSUER, SUBJECT,
        '/projects/1042059373096/locations/asia-south1/services/ci-evaluation-fixture',
        int(now.timestamp()) - 1, int(now.timestamp()) + 599)
    snapshot = fixture_query(imported_at=at).state.head.snapshot
    draft = initial_prompt_record(snapshot, imported_at=at, imported_by=actor.principal_id)
    registered = propose_prompt_transition(draft, status=PromptLifecycle.REGISTERED,
        changed_at=at + timedelta(seconds=1), changed_by=actor.principal_id,
        reason_code='FIXTURE_REVIEWED')
    registry = PromptRegistryState(registered, (draft, registered))
    suite = EvaluationBenchmark('fixture-planner-benchmark', 'fixture-v1', snapshot.task,
        tuple(EvaluationCaseSpec(f'fixture-case-{i:02}', category, 'a' * 64, 'b' * 64,
            'c' * 64, EvaluationOutcome.ALLOW if category is EvaluationCategory.STANDARD
            else EvaluationOutcome.BLOCK) for i, category in enumerate(EvaluationCategory)))
    checks = sorted(EvaluationCheck, key=lambda item: item.value)
    policy = EvaluationPolicy('fixture-planner-policy', 'fixture-v1', suite.sha256,
        now + timedelta(days=1), tuple(EvaluationThreshold(check, Decimal('1')) for check in checks),
        (snapshot.model_id,), 'fixture-evaluator-v1')
    control = EvaluationControl(WORKSPACE, snapshot.prompt_key, 1, None, suite, policy,
        at + timedelta(seconds=2), actor.principal_id)
    record = PromptEvaluationRecord('fixture-evaluation', registered, suite, policy,
        'fixture-evaluator-v1', actor.principal_id, at + timedelta(seconds=3),
        at + timedelta(seconds=4), now + timedelta(hours=1),
        tuple(EvaluationCaseResult(case, case.expected_outcome, 'd' * 64,
            snapshot.model_id, snapshot.model_id, 'fixture-invocation-' + case.case_id,
            tuple((check, True) for check in checks)) for case in suite.cases))
    seed = {name: controls(WORKSPACE)[name] + controls(UNRELATED)[name]
            for name in ('workspaces', 'memberships')}
    control_plan = PromptEvaluationWrite(EvaluationWriteKind.CONTROL, control, identity, 1)
    report_plan = PromptEvaluationWrite(EvaluationWriteKind.EVALUATION, control, identity, 1,
        record=record, registry_state=registry, control_history=(control,))
    return seed, registry, control_plan, report_plan


def another_report(plan, evaluation_id):
    return replace(plan, record=replace(plan.record, evaluation_id=evaluation_id))
