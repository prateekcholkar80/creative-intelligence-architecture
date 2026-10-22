"""Supplied evaluation contracts/gates, not measured RAG quality or cloud tests."""
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta, timezone, tzinfo
from decimal import Decimal, localcontext
import unittest

from src.application.prompt_contract import PromptLifecycle, PromptTask
from src.application.prompt_registry_codec import PromptRegistryState
from src.application.prompt_evaluation_contract import (
    EvaluationBenchmark, EvaluationCaseResult, EvaluationCaseSpec,
    EvaluationCategory, EvaluationCheck, EvaluationOutcome, EvaluationPolicy,
    EvaluationStatus, EvaluationThreshold, PromptEvaluationRecord,
    validate_promotion_evaluation,
)
from src.application.workflow_contract import ContractError
from tests.test_prompt_contract import ACTOR, NOW, advance, draft


def benchmark():
    cases = tuple(EvaluationCaseSpec(
        f'case-{i:02}', category, 'a' * 64, 'b' * 64, 'c' * 64,
        EvaluationOutcome.ALLOW if category is EvaluationCategory.STANDARD
        else EvaluationOutcome.BLOCK) for i, category in enumerate(EvaluationCategory))
    return EvaluationBenchmark('planner-benchmark', 'v1', PromptTask.PLANNER, cases)


def policy(suite):
    return EvaluationPolicy('planner-policy', 'v1', suite.sha256,
        NOW + timedelta(days=3), tuple(EvaluationThreshold(check, Decimal('1'))
        for check in sorted(EvaluationCheck, key=lambda item: item.value)),
        ('gemini-3.5-flash-001',), 'evaluator-v1')


def state():
    initial = draft()
    registered = advance(initial, PromptLifecycle.REGISTERED, 1)
    return PromptRegistryState(registered, (initial, registered))


def evaluation(**changes):
    current = state()
    suite = benchmark()
    results = tuple(EvaluationCaseResult(case, case.expected_outcome, 'd' * 64,
        current.head.snapshot.model_id, current.head.snapshot.model_id,
        'invocation-' + case.case_id,
        tuple((check, True) for check in sorted(EvaluationCheck, key=lambda item: item.value)))
        for case in suite.cases)
    values = dict(evaluation_id='evaluation-1', registry_record=current.head,
        benchmark=suite, policy=policy(suite), evaluator_version='evaluator-v1',
        recorded_by=ACTOR, started_at=NOW + timedelta(seconds=2),
        completed_at=NOW + timedelta(seconds=3), valid_until=NOW + timedelta(days=1),
        results=results)
    return PromptEvaluationRecord(**(values | changes))


def change_check(result, check, value):
    return replace(result, checks=tuple((key, value if key is check else old)
        for key, old in result.checks))


class PromptEvaluationTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def gate(self, record=None, current=None, **changes):
        record = evaluation() if record is None else record
        current = state() if current is None else current
        values = dict(policy=record.policy, benchmark=record.benchmark,
            target_status=PromptLifecycle.EVALUATED, checked_at=NOW + timedelta(seconds=4))
        return validate_promotion_evaluation(current, record, **(values | changes))

    def test_pass_is_derived_deterministic_and_immutable(self):
        record = evaluation()
        self.assertIs(record.status, EvaluationStatus.PASSED)
        self.assertEqual(record.digest, evaluation().digest)
        self.assertEqual(len(record.digest), 64)
        with self.assertRaises(FrozenInstanceError):
            record.status = EvaluationStatus.PASSED

    def test_benchmark_requires_negative_coverage_unique_order_and_bounds(self):
        suite = benchmark()
        for cases in (suite.cases[:-1], suite.cases[::-1], suite.cases + suite.cases,
                      (), (suite.cases[0],) * 129):
            with self.subTest(count=len(cases)), self.assertRaises(ContractError):
                replace(suite, cases=cases)

    def test_policy_requires_all_checks_explicit_decimal_and_model_pins(self):
        selected = policy(benchmark())
        for thresholds in (selected.thresholds[:-1], selected.thresholds[::-1], ()):
            with self.assertRaises(ContractError):
                replace(selected, thresholds=thresholds)
        for rate in (0.9, True, Decimal('NaN'), Decimal('0'), Decimal('1.01'),
                     Decimal('0.9999999999')):
            with self.assertRaises(ContractError):
                EvaluationThreshold(EvaluationCheck.SCHEMA, rate)
        for models in ((), ('x', 'x'), ('z', 'a'), ['model']):
            with self.assertRaises(ContractError):
                replace(selected, allowed_returned_models=models)

    def test_safety_boundaries_cannot_be_weakened_by_average_thresholds(self):
        for check in (EvaluationCheck.APPROVED_CLAIMS, EvaluationCheck.EVIDENCE_SCOPE,
                      EvaluationCheck.INJECTION_RESISTANCE, EvaluationCheck.SAFE_FINISH):
            self.assert_code('EVALUATION_SAFETY_THRESHOLD_REQUIRED',
                EvaluationThreshold, check, Decimal('0.9'))

    def test_missing_cases_and_unknown_fields_never_pass(self):
        base = evaluation()
        for results in ((), base.results[:-1],
                (replace(base.results[0], response_sha256=None),) + base.results[1:],
                (replace(base.results[0], returned_model_id=None),) + base.results[1:],
                (replace(base.results[0], observed_outcome=None),) + base.results[1:],
                (change_check(base.results[0], EvaluationCheck.SCHEMA, None),) + base.results[1:]):
            with self.subTest(results=results):
                record = replace(base, results=results)
                self.assertIs(record.status, EvaluationStatus.INCOMPLETE)
                self.assert_code('PROMOTION_EVALUATION_NOT_PASSED', self.gate, record)

    def test_actual_failure_takes_precedence_over_missing_data(self):
        base = evaluation()
        changed = change_check(base.results[0], EvaluationCheck.APPROVED_CLAIMS, False)
        self.assertIs(replace(base, results=(changed,)).status, EvaluationStatus.FAILED)
        changed = replace(base.results[0], observed_outcome=EvaluationOutcome.BLOCK)
        self.assertIs(replace(base, results=(changed,)).status, EvaluationStatus.FAILED)

    def test_known_wrong_model_and_wrong_expected_outcome_fail(self):
        base = evaluation()
        for changed in (replace(base.results[0], returned_model_id='unapproved-model'),
                        replace(base.results[0], observed_outcome=EvaluationOutcome.NEEDS_REVIEW)):
            record = replace(base, results=(changed,) + base.results[1:])
            self.assertIs(record.status, EvaluationStatus.FAILED)

    def test_boolean_check_values_are_strict_and_complete(self):
        result = evaluation().results[0]
        for value in (1, 'true', Decimal('1')):
            with self.assertRaises(ContractError):
                change_check(result, EvaluationCheck.SCHEMA, value)
        for checks in (result.checks[:-1], result.checks[::-1], result.checks + result.checks):
            with self.assertRaises(ContractError):
                replace(result, checks=checks)

    def test_results_cannot_rebind_case_inputs_evidence_or_requested_model(self):
        base = evaluation()
        for changed in (replace(base.results[0], case=replace(base.results[0].case,
                            input_sha256='e' * 64)),
                        replace(base.results[0], case=replace(base.results[0].case,
                            evidence_sha256='e' * 64)),
                        replace(base.results[0], requested_model_id='wrong-model')):
            self.assert_code('EVALUATION_CASE_BINDING_MISMATCH', replace,
                base, results=(changed,) + base.results[1:])

    def test_duplicate_case_or_invocation_and_changed_policy_binding_deny(self):
        base = evaluation()
        for results in (base.results[::-1], base.results + base.results,
                        (replace(base.results[0], invocation_id=base.results[1].invocation_id),)
                        + base.results[1:]):
            with self.assertRaises(ContractError):
                replace(base, results=results)
        self.assert_code('INVALID_PROMPT_EVALUATION_BINDING', replace, base,
            policy=replace(base.policy, benchmark_sha256='f' * 64))

    def test_evaluation_requires_registered_anchor_and_ordered_bounded_window(self):
        base = evaluation()
        class MissingOffset(tzinfo):
            def utcoffset(self, dt):
                return None
        for changes in ({'registry_record': draft()},
                        {'started_at': NOW}, {'completed_at': NOW},
                        {'valid_until': base.completed_at},
                        {'valid_until': base.policy.valid_until + timedelta(seconds=1)},
                        {'completed_at': NOW.replace(tzinfo=None)},
                        {'completed_at': NOW.replace(tzinfo=MissingOffset())}):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                replace(base, **changes)

    def test_evaluator_version_and_benchmark_task_cannot_be_rebound(self):
        base = evaluation()
        self.assert_code('INVALID_PROMPT_EVALUATION_BINDING', replace, base,
            evaluator_version='unapproved-evaluator')
        suite = replace(base.benchmark, task=PromptTask.INTERPRETER)
        self.assert_code('INVALID_PROMPT_EVALUATION_BINDING', replace, base,
            benchmark=suite, policy=replace(base.policy, benchmark_sha256=suite.sha256))

    def test_activation_rejects_evaluated_history_before_result_completion(self):
        base = evaluation()
        current = state()
        early = advance(current.head, PromptLifecycle.EVALUATED, 2,
            evaluation_id=base.evaluation_id)
        current = PromptRegistryState(early, current.history + (early,))
        self.assert_code('PROMOTION_EVALUATION_TIME_MISMATCH', self.gate,
            base, current, target_status=PromptLifecycle.ACTIVE)

    def test_threshold_comparison_is_exact_under_low_decimal_precision(self):
        base = evaluation()
        thresholds = tuple(EvaluationThreshold(item.check,
            Decimal('0.9') if item.check is EvaluationCheck.SCHEMA else Decimal('1'))
            for item in base.policy.thresholds)
        selected = replace(base.policy, thresholds=thresholds)
        results = (change_check(base.results[0], EvaluationCheck.SCHEMA, False),) + base.results[1:]
        with localcontext() as context:
            context.prec = 1
            self.assertIs(replace(base, results=results, policy=selected).status, EvaluationStatus.PASSED)
            stricter = replace(selected, thresholds=tuple(EvaluationThreshold(item.check,
                Decimal('0.900000001') if item.check is EvaluationCheck.SCHEMA else Decimal('1'))
                for item in selected.thresholds))
            self.assertIs(replace(base, results=results, policy=stricter).status, EvaluationStatus.FAILED)

    def test_equivalent_decimal_policy_and_timezone_have_identical_hashes(self):
        base = evaluation()
        selected = replace(base.policy, thresholds=tuple(
            replace(item, minimum_pass_rate=Decimal('1.000000000')) for item in base.policy.thresholds))
        self.assertEqual(selected.sha256, base.policy.sha256)
        other = replace(base, started_at=base.started_at.astimezone(timezone(timedelta(hours=5))))
        self.assertEqual(other.digest, base.digest)

    def test_gate_binds_exact_registry_revision_and_evaluation(self):
        record = evaluation()
        gate = self.gate(record)
        self.assertEqual(gate.current_registry_digest, state().head.digest)
        self.assertEqual(gate.current_registry_revision, 2)
        self.assertEqual(gate.evaluation_digest, record.digest)
        self.assertEqual(gate.snapshot_sha256, record.registry_record.snapshot.snapshot_sha256)

    def test_changed_prompt_or_historical_anchor_denies_promotion(self):
        current = state()
        changed = replace(current.head.snapshot, vertex_version_id='99')
        initial = replace(current.history[0], snapshot=changed)
        head = advance(initial, PromptLifecycle.REGISTERED, 1)
        self.assert_code('PROMOTION_EVALUATION_REGISTRY_MISMATCH', self.gate,
            current=PromptRegistryState(head, (initial, head)))
        head = replace(current.head, changed_by='other-actor')
        self.assert_code('PROMOTION_EVALUATION_REGISTRY_MISMATCH', self.gate,
            current=PromptRegistryState(head, (current.history[0], head)))

    def test_current_policy_benchmark_and_expiry_are_required(self):
        base = evaluation()
        self.assert_code('PROMOTION_EVALUATION_POLICY_MISMATCH', self.gate, base,
            policy=replace(base.policy, version='v2'))
        self.assert_code('PROMOTION_EVALUATION_POLICY_MISMATCH', self.gate, base,
            benchmark=replace(base.benchmark, version='v2'))
        for at in (base.completed_at - timedelta(seconds=1), base.valid_until,
                   base.policy.valid_until):
            self.assert_code('PROMOTION_EVALUATION_EXPIRED_OR_EARLY', self.gate,
                base, checked_at=at)

    def test_activation_and_rollback_preserve_evaluation_id_and_history(self):
        base = evaluation()
        current = state()
        evaluated = advance(current.head, PromptLifecycle.EVALUATED, 4,
            evaluation_id=base.evaluation_id)
        current = PromptRegistryState(evaluated, current.history + (evaluated,))
        self.gate(base, current, target_status=PromptLifecycle.ACTIVE,
            checked_at=NOW + timedelta(seconds=5))
        active = advance(evaluated, PromptLifecycle.ACTIVE, 6)
        retired = advance(active, PromptLifecycle.RETIRED, 7)
        current = PromptRegistryState(retired, current.history + (active, retired))
        gate = self.gate(base, current, target_status=PromptLifecycle.ACTIVE,
            checked_at=NOW + timedelta(seconds=8))
        self.assertEqual(gate.current_registry_revision, 5)
        self.assert_code('PROMOTION_EVALUATION_REBOUND', self.gate,
            replace(base, evaluation_id='other-evaluation'), current,
            target_status=PromptLifecycle.ACTIVE, checked_at=NOW + timedelta(seconds=8))

    def test_invalid_lifecycle_does_not_become_activation_authorization(self):
        self.assert_code('INVALID_EVALUATED_PROMOTION', self.gate,
            target_status=PromptLifecycle.ACTIVE)
        initial = draft()
        current = PromptRegistryState(initial, (initial,))
        self.assert_code('INVALID_EVALUATED_PROMOTION', self.gate, current=current)


if __name__ == '__main__':
    unittest.main()
