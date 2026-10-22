"""Offline model-call/accounting contracts; no prices, users or calls are published."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal, localcontext
import unittest
from unittest.mock import patch

from src.application.workflow_contract import (
    NUMERIC_MAX, TOKEN_FIELDS, ContractError, request_fingerprint,
    validate_model_invocation, validate_model_invocation_change,
)
from src.application.workflow_schema import TABLES
from tests.test_execution_contract import execution
from tests.test_workflow_contract import CONVERSATION, TIME, digest, request


def invocation(state='PLANNED', stage='PLANNER', *, dispatched=True):
    if stage == 'PLANNER':
        attempt, req, job = execution('RECOVERY_REQUIRED' if state == 'UNKNOWN' else 'RUNNING')
    else:
        req, job, attempt = request('INTERPRET'), None, None
    sent = state not in {'PLANNED'} and dispatched
    if attempt is not None:
        attempt['dispatch_receipt_id'] = 'test-receipt' if sent else None
    row = dict.fromkeys(f.name for f in TABLES['model_invocations'].schema)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'request_id', 'trace_id'):
        row[key] = req[key]
    row.update(invocation_id='test-invocation', stage=stage, call_number=1,
        job_id=job['job_id'] if job is not None else None,
        attempt_id=attempt['attempt_id'] if attempt is not None else None,
        state=state, revision=1 if state == 'PLANNED' else 2 if state == 'DISPATCHED' else 3,
        dispatch_receipt_id='test-receipt' if sent else None,
        worker_id=('test-worker' if stage == 'PLANNER' else 'test-interpreter-worker') if sent else None,
        profile=deepcopy(next(p for p in req['model_profiles'] if p['stage'] == stage)),
        usage_status='NOT_REPORTED', cost_status='NOT_ESTIMATED',
        started_at=TIME+timedelta(seconds=2 if stage == 'PLANNER' else 1),
        completed_at=TIME+timedelta(seconds=3) if state in {'SUCCEEDED', 'FAILED', 'BLOCKED'} else None,
        safe_error_code='CALL_OUTCOME_UNKNOWN' if state == 'UNKNOWN'
                        else 'RESPONSE_INVALID' if state == 'FAILED' else 'MODEL_BLOCKED' if state == 'BLOCKED' else None)
    if state == 'SUCCEEDED':
        row.update(returned_model_id='gemini-3.5-flash-001', response_sha256=digest('observed response'),
                   finish_reason='STOP', safety_outcome='PASSED')
    return row, dict(request=req, job=job, attempt=attempt, conversation=CONVERSATION)


def reported(row):
    return row | dict(usage_status='REPORTED', input_tokens=100, output_tokens=10, cached_tokens=0, thinking_tokens=1)


def estimated(row, value=Decimal('0.000123456')):
    return reported(row) | dict(cost_status='ESTIMATED', estimated_cost=value, currency='USD', pricing_version='test-pricing-v1')


class InvocationContractTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_all_states_in_both_stage_scopes(self):
        for stage in ('INTERPRETER', 'PLANNER'):
            for state in ('PLANNED', 'DISPATCHED', 'UNKNOWN', 'SUCCEEDED', 'FAILED', 'BLOCKED'):
                row, context = invocation(state, stage)
                with self.subTest(stage=stage, state=state):
                    validate_model_invocation(row, **context)

    def test_exact_schema_with_explicit_nullable_fields_no_sensitive_extras(self):
        original, context = invocation()
        for mutate in (lambda r: r.pop('input_tokens'), lambda r: r.update(raw_response='secret'),
                       lambda r: r.update(email='private'), lambda r: r.update(profile=[])):
            row = deepcopy(original)
            mutate(row)
            with self.assertRaises(ContractError):
                validate_model_invocation(row, **context)

    def test_scope_request_trace_and_schema_must_match(self):
        row, context = invocation()
        for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'request_id', 'trace_id'):
            with self.subTest(key=key), self.assertRaises(ContractError):
                validate_model_invocation(row | {key: 'other'}, **context)

    def test_parent_integrity_and_canonical_sku_inherited_for_all_twenty(self):
        for i in range(1, 21):
            row, context = invocation()
            req, job = context['request'], context['job']
            req['canonical_sku'] = req['submitted_preferences']['preferences']['canonical_sku'] = f'CI-SKU-{i:04d}'
            req['request_sha256'] = request_fingerprint(req)
            job.update(canonical_sku=req['canonical_sku'], request_sha256=req['request_sha256'])
            validate_model_invocation(row, **context)
        context['request']['request_sha256'] = '0'*64
        self.assert_code('REQUEST_HASH_MISMATCH', validate_model_invocation, row, **context)

    def test_interpreter_has_no_job_attempt_and_cannot_reuse_generate_request(self):
        row, context = invocation(stage='INTERPRETER')
        for changes in (dict(job_id='other'), dict(attempt_id='other')):
            self.assert_code('INVALID_INTERPRETER_SCOPE', validate_model_invocation, row | changes, **context)
        _, generate_context = invocation()
        self.assert_code('INVALID_INTERPRETER_SCOPE', validate_model_invocation, row, **generate_context)

    def test_closed_interpretation_cannot_be_planned_or_dispatched_again(self):
        for state in ('PLANNED', 'DISPATCHED'):
            row, context = invocation(state, 'INTERPRETER')
            context['request'].update(request_status='FAILED', updated_at=TIME+timedelta(seconds=4))
            self.assert_code('INTERPRETATION_ALREADY_CLOSED', validate_model_invocation, row, **context)

    def test_successful_interpretation_can_precede_or_follow_valid_parent_outcome(self):
        row, context = invocation('SUCCEEDED', 'INTERPRETER')
        validate_model_invocation(row, **context)
        context['request'].update(request_status='NEEDS_CLARIFICATION', updated_at=TIME+timedelta(seconds=4))
        validate_model_invocation(row, **context)
        context['request']['updated_at'] = TIME+timedelta(seconds=2)
        self.assert_code('INVALID_TIME_ORDER', validate_model_invocation, row, **context)

    def test_planner_requires_full_job_and_attempt_and_correct_links(self):
        row, context = invocation()
        for missing in ('job', 'attempt'):
            self.assert_code('INVALID_PLANNER_SCOPE', validate_model_invocation, row, **(context | {missing: None}))
        for changes in (dict(job_id=None), dict(attempt_id=None), dict(job_id='other'), dict(attempt_id='other')):
            self.assert_code('INVOCATION_PARENT_MISMATCH', validate_model_invocation, row | changes, **context)

    def test_profile_stage_and_every_pinned_setting_must_match(self):
        row, context = invocation()
        for key, value in (('thinking_level', 'HIGH'), ('model_id', 'unpublished-model'), ('prompt_version', 'other'),
                           ('catalog_version', 'other'), ('settings_sha256', digest('other')), ('max_output_tokens', 100)):
            changed = deepcopy(row)
            changed['profile'][key] = value
            self.assert_code('INVOCATION_PROFILE_MISMATCH', validate_model_invocation, changed, **context)
        self.assert_code('INVOCATION_PROFILE_MISMATCH', validate_model_invocation, row | dict(stage='OTHER'), **context)

    def test_validated_budget_and_none_thinking_snapshots_also_supported(self):
        for control in ('BUDGET', 'NONE'):
            row, context = invocation()
            row['profile'].update(thinking_control=control, thinking_level=None, thinking_budget=0 if control == 'BUDGET' else None)
            context['request']['model_profiles'][1] = deepcopy(row['profile'])
            context['request']['request_sha256'] = request_fingerprint(context['request'])
            context['job']['request_sha256'] = context['request']['request_sha256']
            validate_model_invocation(row, **context)

    def test_ids_revision_call_numbers_and_unknown_state_are_rejected(self):
        row, context = invocation()
        for changes in (dict(invocation_id='bad id'), dict(invocation_id='x'*129), dict(revision=True), dict(revision=0),
                        dict(revision=2**63), dict(call_number=0), dict(call_number=True), dict(call_number=2), dict(state='RUNNING')):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                validate_model_invocation(row | changes, **context)

    def test_planned_and_dispatched_states_cannot_claim_response_usage_or_cost(self):
        for state in ('PLANNED', 'DISPATCHED'):
            row, context = invocation(state)
            for changes in (dict(returned_model_id='returned'), dict(finish_reason='STOP'),
                            dict(safety_outcome='PASSED'), dict(response_sha256=digest('response')),
                            dict(usage_status='PARTIAL', input_tokens=1)):
                with self.subTest(state=state), self.assertRaises(ContractError):
                    validate_model_invocation(row | changes, **context)

    def test_receipt_worker_binding_to_attempt_and_safe_response_identifiers(self):
        row, context = invocation('DISPATCHED')
        for changes in (dict(dispatch_receipt_id=None), dict(worker_id=None), dict(worker_id='other'),
                        dict(dispatch_receipt_id='other'), dict(worker_id='bad worker'), dict(dispatch_receipt_id='bad receipt')):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                validate_model_invocation(row | changes, **context)
        row, context = invocation('SUCCEEDED')
        for changes in (dict(returned_model_id='private model name'), dict(response_sha256='bad'), dict(finish_reason='private exception')):
            with self.assertRaises(ContractError):
                validate_model_invocation(row | changes, **context)

    def test_success_needs_complete_safe_response_not_returned_model_fabrication(self):
        row, context = invocation('SUCCEEDED')
        validate_model_invocation(row | dict(returned_model_id=None), **context)
        for changes in (dict(response_sha256=None), dict(finish_reason='MAX_TOKENS'), dict(finish_reason=None),
                        dict(safety_outcome='BLOCKED'), dict(safety_outcome='NOT_EVALUATED'), dict(safety_outcome=None),
                        dict(safe_error_code='FAILED')):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                validate_model_invocation(row | changes, **context)

    def test_pre_dispatch_failure_or_block_has_no_worker_results_or_usage(self):
        for state in ('FAILED', 'BLOCKED'):
            row, context = invocation(state, dispatched=False)
            validate_model_invocation(row, **context)
            for changes in (dict(worker_id='test-worker'), dict(returned_model_id='returned'),
                            dict(usage_status='PARTIAL', input_tokens=1)):
                self.assert_code('UNDISPATCHED_INVOCATION_HAS_RESULTS', validate_model_invocation, row | changes, **context)

    def test_failed_blocked_calls_keep_observed_billable_work(self):
        for state, finish, safety in (('FAILED', 'MAX_TOKENS', 'PASSED'), ('BLOCKED', 'SAFETY', 'BLOCKED')):
            row, context = invocation(state)
            row.update(response_sha256=digest('partial response'), finish_reason=finish, safety_outcome=safety)
            validate_model_invocation(estimated(row), **context)

    def test_unknown_is_dispatched_uncertainty_without_fake_completion(self):
        row, context = invocation('UNKNOWN')
        validate_model_invocation(row | dict(usage_status='PARTIAL', input_tokens=100), **context)
        for changes in (dict(completed_at=TIME+timedelta(seconds=3)), dict(dispatch_receipt_id=None), dict(safe_error_code=None)):
            with self.assertRaises(ContractError):
                validate_model_invocation(row | changes, **context)
        _, running = invocation('DISPATCHED')
        self.assert_code('UNKNOWN_INVOCATION_REQUIRES_RECOVERY', validate_model_invocation, row, **running)

    def test_pending_or_failed_call_cannot_exist_inside_successful_attempt(self):
        for state in ('PLANNED', 'DISPATCHED', 'FAILED', 'BLOCKED'):
            row, context = invocation(state)
            attempt, req, job = execution('SUCCEEDED')
            context.update(attempt=attempt, request=req, job=job)
            with self.assertRaises(ContractError):
                validate_model_invocation(row, **context)

    def test_successful_call_can_be_followed_by_render_failure_or_reconciled_success(self):
        row, context = invocation('SUCCEEDED')
        for state in ('SUCCEEDED', 'FAILED', 'BLOCKED', 'RECOVERY_REQUIRED'):
            attempt, req, job = execution(state)
            attempt['dispatch_receipt_id'] = row['dispatch_receipt_id']
            validate_model_invocation(row, **(context | dict(attempt=attempt, request=req, job=job)))

    def test_time_bounds_and_aware_timestamps(self):
        row, context = invocation('SUCCEEDED')
        for changes in (dict(started_at=TIME), dict(started_at=context['job']['deadline_at']),
                        dict(completed_at=TIME), dict(completed_at=None), dict(started_at=TIME.replace(tzinfo=None))):
            with self.assertRaises(ContractError):
                validate_model_invocation(row | changes, **context)
        attempt, req, job = execution('SUCCEEDED')
        self.assert_code('INVALID_TIME_ORDER', validate_model_invocation, row | dict(completed_at=TIME+timedelta(seconds=21)),
            **(context | dict(attempt=attempt, request=req, job=job)))

    def test_usage_null_partial_and_fully_reported_are_distinct(self):
        row, context = invocation('SUCCEEDED')
        validate_model_invocation(row, **context)
        for key in TOKEN_FIELDS:
            validate_model_invocation(row | dict(usage_status='PARTIAL') | {key: 0}, **context)
        validate_model_invocation(reported(row), **context)
        for changes in (dict(usage_status='REPORTED'), dict(usage_status='PARTIAL'), dict(usage_status='OTHER'),
                        dict(input_tokens=0), dict(usage_status='NOT_REPORTED', input_tokens=1)):
            self.assert_code('INVALID_USAGE_STATUS', validate_model_invocation, row | changes, **context)

    def test_tokens_strict_nonnegative_int64_and_cache_is_prompt_subset(self):
        row, context = invocation('SUCCEEDED')
        for key in TOKEN_FIELDS:
            for value in (True, 1.0, '1', -1, 2**63):
                with self.subTest(key=key, value=value), self.assertRaises(ContractError):
                    validate_model_invocation(reported(row) | {key: value}, **context)
        self.assert_code('INVALID_CACHED_TOKENS', validate_model_invocation, reported(row) | dict(cached_tokens=101), **context)

    def test_actual_usage_is_not_clamped_to_configuration_or_thinking_output_relation(self):
        row, context = invocation('FAILED')
        validate_model_invocation(reported(row) | dict(output_tokens=10000, thinking_tokens=90000), **context)

    def test_missing_usage_is_never_implicitly_zero(self):
        row, context = invocation('SUCCEEDED')
        snapshot = deepcopy(row)
        validate_model_invocation(row, **context)
        self.assertEqual(row, snapshot)
        self.assertTrue(all(row[key] is None for key in TOKEN_FIELDS))

    def test_cost_not_estimated_has_no_amount_currency_or_pricing(self):
        row, context = invocation('SUCCEEDED')
        for changes in (dict(estimated_cost=Decimal('0')), dict(currency='USD'), dict(pricing_version='pricing')):
            self.assert_code('INVALID_COST_STATUS', validate_model_invocation, row | changes, **context)

    def test_partial_cost_accepts_partial_usage_or_partial_pricing_coverage(self):
        row, context = invocation('SUCCEEDED')
        cost = dict(cost_status='PARTIAL', estimated_cost=Decimal('0.01'), currency='USD', pricing_version='partial-pricing')
        validate_model_invocation(row | dict(usage_status='PARTIAL', input_tokens=100) | cost, **context)
        validate_model_invocation(reported(row) | cost, **context)
        self.assert_code('INVALID_COST_STATUS', validate_model_invocation, row | cost, **context)
        self.assert_code('INCOMPLETE_ESTIMATED_USAGE', validate_model_invocation,
            row | dict(usage_status='PARTIAL', input_tokens=100) | cost | dict(cost_status='ESTIMATED'), **context)

    def test_cost_requires_exact_decimal_nonnegative_amount_and_pricing_identity(self):
        row, context = invocation('SUCCEEDED')
        original = estimated(row)
        for changes in (dict(estimated_cost=None), dict(estimated_cost=-Decimal('0.1')), dict(estimated_cost=0.1),
                        dict(estimated_cost=1), dict(estimated_cost=True), dict(estimated_cost='0.1'), dict(currency=None),
                        dict(currency='usd'), dict(currency='US Dollars'), dict(pricing_version=None), dict(pricing_version='bad pricing')):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                validate_model_invocation(original | changes, **context)

    def test_numeric_precision_scale_and_nonfinite_values_fail_without_rounding(self):
        row, context = invocation('SUCCEEDED')
        for value in (Decimal('NaN'), Decimal('sNaN'), Decimal('Infinity'), Decimal('-Infinity'),
                      Decimal('1e29'), Decimal('0.0000000001'), Decimal('1.1234567891')):
            self.assert_code('INVALID_NUMERIC', validate_model_invocation, estimated(row, value), **context)
        for value in (Decimal('0'), Decimal('1e-9'), Decimal('1.0000000000'), NUMERIC_MAX):
            validate_model_invocation(estimated(row, value), **context)

    def test_numeric_validation_independent_of_process_decimal_precision(self):
        row, context = invocation('SUCCEEDED')
        with localcontext() as decimal_context:
            decimal_context.prec = 2
            validate_model_invocation(estimated(row, NUMERIC_MAX), **context)
            self.assert_code('INVALID_NUMERIC', validate_model_invocation, estimated(row, Decimal('1.1234567891')), **context)

    def test_invalid_safe_codes_and_safety_outcomes_do_not_leak_provider_messages(self):
        row, context = invocation('FAILED')
        for changes in (dict(safe_error_code=None), dict(safe_error_code='private provider error'),
                        dict(finish_reason='lowercase'), dict(safety_outcome='APPROVED')):
            with self.assertRaises(ContractError) as caught:
                validate_model_invocation(row | changes, **context)
            self.assertNotIn('private', str(caught.exception))

    def change(self, previous, candidate, context, **kwargs):
        return validate_model_invocation_change(previous, candidate,
            expected_revision=kwargs.pop('expected_revision', previous['revision']), **context, **kwargs)

    def test_planned_to_dispatched_requires_explicit_prior_parent_receipt_snapshot(self):
        previous, before = invocation()
        candidate, after = invocation('DISPATCHED')
        self.change(previous, candidate, after, previous_request=before['request'], previous_job=before['job'],
                    previous_attempt=before['attempt'])
        self.assert_code('INVOCATION_DISPATCH_MISMATCH', self.change, previous, candidate, after)

    def test_dispatched_can_complete_or_become_unknown_with_explicit_prior_parents(self):
        previous, before = invocation('DISPATCHED')
        for state in ('SUCCEEDED', 'FAILED', 'BLOCKED', 'UNKNOWN'):
            candidate, after = invocation(state)
            self.change(previous, candidate, after, previous_request=before['request'], previous_job=before['job'],
                        previous_attempt=before['attempt'])

    def test_interpreter_completion_uses_explicit_previous_accepted_request(self):
        previous, before = invocation('DISPATCHED', 'INTERPRETER')
        candidate, after = invocation('SUCCEEDED', 'INTERPRETER')
        after['request'].update(request_status='NEEDS_CLARIFICATION', updated_at=TIME+timedelta(seconds=4))
        self.change(previous, candidate, after, previous_request=before['request'])
        self.assert_code('INTERPRETATION_ALREADY_CLOSED', self.change, previous, candidate, after)

    def test_unknown_reconciles_but_never_replans_or_redispatches(self):
        previous, before = invocation('UNKNOWN')
        for state in ('SUCCEEDED', 'FAILED', 'BLOCKED'):
            candidate, after = invocation(state)
            candidate['revision'] = 4
            self.change(previous, candidate, after, previous_job=before['job'], previous_attempt=before['attempt'])
        for state in ('PLANNED', 'DISPATCHED'):
            candidate, after = invocation(state)
            candidate['revision'] = 4
            with self.assertRaises(ContractError):
                self.change(previous, candidate, after, previous_job=before['job'], previous_attempt=before['attempt'])

    def test_dispatched_cannot_replan_even_without_outcome(self):
        previous, before = invocation('DISPATCHED')
        candidate, after = invocation()
        candidate['revision'] = 3
        self.assert_code('INVOCATION_DISPATCH_CHANGED', self.change, previous, candidate, after,
                         previous_job=before['job'], previous_attempt=before['attempt'])

    def test_stale_revisions_and_skipped_revision_updates_rejected(self):
        previous, context = invocation('DISPATCHED')
        candidate, _ = invocation('SUCCEEDED')
        for revision in (0, True, 1, 3):
            with self.assertRaises(ContractError):
                self.change(previous, candidate, context, expected_revision=revision)
        self.assert_code('REVISION_CONFLICT', self.change, previous, candidate | dict(revision=4), context)

    def test_invocation_identity_start_time_and_claim_cannot_change(self):
        previous, context = invocation('DISPATCHED', 'INTERPRETER')
        candidate, _ = invocation('SUCCEEDED', 'INTERPRETER')
        for changes in (dict(invocation_id='other'), dict(started_at=TIME+timedelta(seconds=2)),
                        dict(dispatch_receipt_id='other'), dict(worker_id='other')):
            with self.assertRaises(ContractError):
                self.change(previous, candidate | changes, context)

    def test_parent_input_change_is_rejected_even_when_call_profile_matches(self):
        previous, before = invocation('DISPATCHED')
        candidate, after = invocation('SUCCEEDED')
        after['request']['audience'] = after['request']['submitted_preferences']['preferences']['audience'] = 'Changed audience'
        after['request']['request_sha256'] = request_fingerprint(after['request'])
        after['job']['request_sha256'] = after['request']['request_sha256']
        self.assert_code('INVOCATION_REQUEST_CHANGED', self.change, previous, candidate, after,
            previous_request=before['request'], previous_job=before['job'], previous_attempt=before['attempt'])

    def test_terminal_accounting_updates_fill_unknown_categories_preserve_observed_counts(self):
        previous, context = invocation('SUCCEEDED')
        previous.update(usage_status='PARTIAL', input_tokens=100)
        candidate = estimated(previous) | dict(revision=4)
        self.change(previous, candidate, context)
        self.assert_code('OBSERVED_USAGE_CHANGED', self.change, previous, candidate | dict(input_tokens=101), context)
        self.assert_code('OBSERVED_USAGE_CHANGED', self.change, previous,
                         candidate | dict(input_tokens=None, usage_status='PARTIAL', cost_status='PARTIAL'), context)

    def test_terminal_response_or_completion_cannot_be_rewritten(self):
        previous, context = invocation('SUCCEEDED')
        for changes in (dict(response_sha256=digest('different response')), dict(returned_model_id='other'),
                        dict(completed_at=TIME+timedelta(seconds=4))):
            with self.assertRaises(ContractError):
                self.change(previous, previous | dict(revision=4) | changes, context)
        failed, _ = invocation('FAILED')
        failed['revision'] = 4
        self.assert_code('UNSAFE_INVOCATION_TRANSITION', self.change, previous, failed, context)

    def test_repricing_with_versioned_estimate_does_not_change_token_counts(self):
        previous, context = invocation('SUCCEEDED')
        previous = estimated(previous)
        candidate = previous | dict(revision=4, pricing_version='test-pricing-v2', estimated_cost=Decimal('0.02'))
        self.change(previous, candidate, context)

    def test_planned_can_abort_without_dispatch_but_cannot_skip_dispatch_claim(self):
        previous, before = invocation()
        for state in ('FAILED', 'BLOCKED'):
            candidate, after = invocation(state, dispatched=False)
            candidate['revision'] = 2
            self.change(previous, candidate, after)
            candidate, after = invocation(state)
            candidate['revision'] = 2
            self.assert_code('DISPATCH_CLAIM_SKIPPED', self.change, previous, candidate, after,
                             previous_job=before['job'], previous_attempt=before['attempt'])

    def test_reconciliation_preserves_observed_response_identity(self):
        previous, before = invocation('UNKNOWN')
        previous.update(returned_model_id='gemini-3.5-flash-001', response_sha256=digest('observed response'),
                        finish_reason='STOP', safety_outcome='NOT_EVALUATED')
        candidate, after = invocation('SUCCEEDED')
        candidate['revision'] = 4
        self.change(previous, candidate, after, previous_job=before['job'], previous_attempt=before['attempt'])
        for changes in (dict(response_sha256=digest('different response')), dict(returned_model_id='other')):
            self.assert_code('OBSERVED_RESPONSE_CHANGED', self.change, previous, candidate | changes, after,
                             previous_job=before['job'], previous_attempt=before['attempt'])

    def test_known_estimates_cannot_disappear_or_change_without_updated_usage_or_price_version(self):
        previous, context = invocation('SUCCEEDED')
        previous = estimated(previous)
        self.assert_code('COST_ESTIMATE_REMOVED', self.change, previous,
            previous | dict(revision=4, cost_status='NOT_ESTIMATED', estimated_cost=None, currency=None, pricing_version=None), context)
        self.assert_code('UNVERSIONED_COST_CHANGE', self.change, previous,
            previous | dict(revision=4, estimated_cost=Decimal('0.02')), context)
        previous.update(usage_status='PARTIAL', output_tokens=None, thinking_tokens=None, cost_status='PARTIAL')
        candidate = estimated(previous, Decimal('0.02')) | dict(revision=4)
        self.change(previous, candidate, context)

    def test_no_cloud_calls_mutation_or_secret_capture(self):
        row, context = invocation('SUCCEEDED')
        original = deepcopy((row, context))
        with patch('google.cloud.bigquery.Client') as bq, patch('google.cloud.storage.Client') as gcs:
            validate_model_invocation(row, **context)
            self.change(row, reported(row) | dict(revision=4), context)
        bq.assert_not_called()
        gcs.assert_not_called()
        self.assertEqual((row, context), original)


if __name__ == '__main__':
    unittest.main()
