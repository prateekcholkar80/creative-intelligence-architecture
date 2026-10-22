"""Meaningful contract/retry/state cases with local fixtures, never published as users."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import unittest
from unittest.mock import patch

from src.application.access_policy import OwnedResource
from src.application.workflow_contract import (
    ContractError, DispatchStatus, JobState, RequestState, RetryDecision,
    request_fingerprint, resolve_retry, retry_key_hash, transition_job, transition_request,
    validate_job, validate_profile, validate_request,
)
from src.application.workflow_schema import SCHEMA_VERSION, TABLES

TIME = datetime(2026, 10, 10, 0, 0, tzinfo=timezone.utc)
CONVERSATION = OwnedResource('test-conversation', 'test-workspace', 'test-principal')


def digest(text):
    return hashlib.sha256(text.encode()).hexdigest()


def preferences():
    return dict(canonical_sku='CI-SKU-0001', objective='CONVERSION', audience='Skincare shoppers',
                channel='PAID_SOCIAL', visual_reference='NONE')


def profile(stage='PLANNER'):
    return dict(stage=stage, profile_id=f'test-{stage.lower()}', profile_version='test-v1',
                catalog_version='test-v1', model_id='gemini-3.5-flash', location='asia-south1',
                thinking_control='LEVEL', thinking_level='LOW' if stage == 'PLANNER' else 'MINIMAL',
                thinking_budget=None, max_output_tokens=4096, timeout_ms=120000,
                seed=42, temperature=None, prompt_id=f'test-{stage.lower()}', prompt_version='test-v1',
                output_schema_version='test-v1', safety_policy_version='test-v1',
                settings_sha256=digest(f'settings-{stage}'))


def request(operation='GENERATE'):
    row = dict.fromkeys(f.name for f in TABLES['requests'].schema)
    row.update(schema_version=SCHEMA_VERSION, workspace_id=CONVERSATION.workspace_id,
        conversation_id=CONVERSATION.resource_id, owner_id=CONVERSATION.owner_id,
        principal_id=CONVERSATION.owner_id, request_id='test-request', session_id='test-session',
        trace_id='test-trace', operation=operation,
        request_status='READY' if operation == 'GENERATE' else 'ACCEPTED', revision=1,
        submitted_preferences=dict(preferences=preferences() if operation == 'GENERATE'
                                   else dict.fromkeys(preferences()),
            renderer=dict(version='test-renderer-v1', settings_sha256=digest('renderer'))
                     if operation == 'GENERATE' else None),
        unsupported_features=[], message_sha256=digest('message') if operation == 'INTERPRET' else None,
        generation_intent_id='test-intent' if operation == 'GENERATE' else None,
        model_profiles=[profile('INTERPRETER'), profile()] if operation == 'GENERATE'
                       else [profile('INTERPRETER')],
        policy_version='marketing-access-v1', created_at=TIME, updated_at=TIME,
        request_sha256='0'*64,
        idempotency_key_sha256=retry_key_hash(workspace_id=CONVERSATION.workspace_id,
            principal_id=CONVERSATION.owner_id, conversation_id=CONVERSATION.resource_id,
            operation=operation, retry_key='test-retry-key'))
    if operation == 'GENERATE':
        row.update(preferences())
    row['request_sha256'] = request_fingerprint(row)
    return row


def job(req=None):
    req = request() if req is None else req
    row = dict.fromkeys(f.name for f in TABLES['jobs'].schema)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'request_id',
                'principal_id', 'trace_id', 'canonical_sku', 'idempotency_key_sha256', 'request_sha256',
                'generation_intent_id', 'policy_version'):
        row[key] = req[key]
    row.update(job_id='test-job', state='QUEUED', revision=1, attempt_count=0, created_at=TIME,
               updated_at=TIME, deadline_at=TIME + timedelta(minutes=5))
    return row


class WorkflowContractTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def validate(self, row):
        validate_request(row, conversation=CONVERSATION)

    def move(self, row, target, **kwargs):
        return transition_job(row, request=request(), conversation=CONVERSATION, target=target,
                              expected_revision=kwargs.pop('expected_revision', row['revision']),
                              now=kwargs.pop('now', row['updated_at'] + timedelta(seconds=1)), **kwargs)

    def running(self):
        return self.move(job(), JobState.RUNNING, attempt_id='test-attempt')

    def recovery(self):
        return self.move(self.running(), JobState.RECOVERY_REQUIRED, attempt_id='test-attempt',
                         safe_error_code='CALL_OUTCOME_UNKNOWN')

    def test_generation_and_accepted_interpretation_match_existing_schemas(self):
        for operation in ('GENERATE', 'INTERPRET'):
            self.validate(request(operation))
        validate_job(job(), request=request(), conversation=CONVERSATION)

    def test_all_canonical_skus_are_accepted_but_unrelated_id_is_rejected(self):
        for i in range(1, 21):
            row = request()
            row['canonical_sku'] = row['submitted_preferences']['preferences']['canonical_sku'] = f'CI-SKU-{i:04d}'
            row['request_sha256'] = request_fingerprint(row)
            self.validate(row)
        row['submitted_preferences']['preferences']['canonical_sku'] = 'SKU-1'
        self.assert_code('INVALID_PREFERENCES', request_fingerprint, row)

    def test_missing_extra_or_untyped_row_fields_fail_closed(self):
        for mutate in (lambda r: r.pop('session_id'), lambda r: r.update(admin=True),
                       lambda r: r.update(revision=True), lambda r: r.update(revision=2**63),
                       lambda r: r.update(created_at=TIME.replace(tzinfo=None)),
                       lambda r: r.update(created_at=TIME.isoformat()),
                       lambda r: r.update(unsupported_features=None)):
            row = request()
            mutate(row)
            with self.subTest(mutate=mutate), self.assertRaises(ContractError):
                self.validate(row)

    def test_unverified_scope_and_borrowed_owner_are_rejected(self):
        for key in ('workspace_id', 'conversation_id', 'owner_id', 'principal_id'):
            row = request()
            row[key] = 'other'
            with self.subTest(key=key), self.assertRaises(ContractError):
                self.validate(row)

    def test_complete_preferences_and_normalized_audience_are_required(self):
        for value in (None, '', '  shoppers', 'x'*161, 'shoppers\n'):
            row = request()
            row['submitted_preferences']['preferences']['audience'] = value
            with self.subTest(value=value), self.assertRaises(ContractError):
                request_fingerprint(row)

    def test_unsupported_features_and_unknown_claim_fields_cannot_generate(self):
        row = request()
        row['unsupported_features'] = ['BENEFIT_CLAIMS']
        self.assert_code('INVALID_GENERATION_REQUEST', self.validate, row)
        row = request()
        row['submitted_preferences']['preferences']['benefit_claim'] = 'Cures acne'
        self.assert_code('INVALID_PREFERENCES', request_fingerprint, row)

    def test_refinement_is_reserved_and_cannot_be_enabled_by_a_row(self):
        row = request()
        row['operation'] = 'REFINE'
        self.assert_code('FEATURE_NOT_ENABLED', request_fingerprint, row)
        row = request()
        row['parent_output_id'] = 'another-output'
        self.assert_code('FEATURE_NOT_ENABLED', request_fingerprint, row)

    def test_interpretation_input_does_not_create_generation_intent(self):
        row = request('INTERPRET')
        row['generation_intent_id'] = 'test-intent'
        self.assert_code('INVALID_INTERPRETATION_INPUT', request_fingerprint, row)

    def test_interpretation_finalizes_without_mutating_input_hash(self):
        original = request('INTERPRET')
        result = transition_request(original, conversation=CONVERSATION, target=RequestState.READY,
            expected_revision=1, now=TIME + timedelta(seconds=1), resolved_preferences=preferences())
        self.assertEqual(result['request_sha256'], original['request_sha256'])
        self.assertEqual(result['revision'], 2)
        self.assertEqual(original['request_status'], 'ACCEPTED')
        self.assertEqual(result['submitted_preferences'], original['submitted_preferences'])
        self.assert_code('INVALID_TRANSITION', transition_request, result, conversation=CONVERSATION,
            target=RequestState.READY, expected_revision=2, now=TIME + timedelta(seconds=2),
            resolved_preferences=preferences())

    def test_interpretation_clarification_block_and_failure_outcomes(self):
        partial = preferences()
        partial['channel'] = None
        cases = ((RequestState.NEEDS_CLARIFICATION, partial, None),
                 (RequestState.BLOCKED, preferences(), ['BENEFIT_CLAIMS']),
                 (RequestState.FAILED, None, None))
        for target, resolved, features in cases:
            result = transition_request(request('INTERPRET'), conversation=CONVERSATION,
                target=target, expected_revision=1, now=TIME + timedelta(seconds=1),
                resolved_preferences=resolved, unsupported_features=features)
            self.assertEqual(result['request_status'], target.value)
            self.validate(result)

    def test_interpretation_rejects_inconsistent_outcome_or_stale_revision(self):
        for target, resolved, features, revision in (
            (RequestState.READY, preferences(), ['BENEFIT_CLAIMS'], 1),
            (RequestState.NEEDS_CLARIFICATION, preferences(), None, 1),
            (RequestState.BLOCKED, preferences(), [], 1),
            (RequestState.FAILED, preferences(), None, 1),
            (RequestState.READY, preferences(), None, 2),
        ):
            with self.subTest(target=target), self.assertRaises(ContractError):
                transition_request(request('INTERPRET'), conversation=CONVERSATION, target=target,
                    expected_revision=revision, now=TIME + timedelta(seconds=1),
                    resolved_preferences=resolved, unsupported_features=features)

    def test_model_thinking_control_and_stage_snapshots_are_checked(self):
        for control, level, budget in (('LEVEL', 'LOW', None), ('BUDGET', None, 0), ('NONE', None, None)):
            value = profile()
            value.update(thinking_control=control, thinking_level=level, thinking_budget=budget)
            validate_profile(value)
        for updates in (dict(thinking_budget=1), dict(thinking_control='BUDGET', thinking_level=None,
                                                     thinking_budget=-1),
                        dict(thinking_control='NONE'), dict(location='us-central1'),
                        dict(max_output_tokens=0), dict(seed=True), dict(temperature=float('nan')),
                        dict(temperature=10**400), dict(settings_sha256='invalid')):
            value = dict(profile(), **updates)
            with self.subTest(updates=updates), self.assertRaises(ContractError):
                validate_profile(value)

    def test_duplicate_missing_or_unknown_stage_profiles_are_rejected(self):
        for profiles in ([profile()], [profile(), profile()], [dict(profile(), stage='USER')]):
            row = request()
            row['model_profiles'] = profiles
            with self.subTest(profiles=profiles), self.assertRaises(ContractError):
                request_fingerprint(row)

    def test_fingerprint_is_independent_of_dict_order_profile_order_and_runtime_ids(self):
        original = request()
        reordered = {k: original[k] for k in reversed(original)}
        reordered = deepcopy(reordered)
        reordered['model_profiles'].reverse()
        reordered.update(request_id='new-request', trace_id='new-trace', session_id=None, revision=9,
                         created_at=TIME + timedelta(days=1), updated_at=TIME + timedelta(days=1))
        self.assertEqual(request_fingerprint(original), request_fingerprint(reordered))

    def test_every_generation_affecting_input_changes_fingerprint(self):
        mutations = (
            lambda r: r['submitted_preferences']['preferences'].update(channel='EMAIL'),
            lambda r: r['submitted_preferences']['renderer'].update(version='renderer-v2'),
            lambda r: r['submitted_preferences']['renderer'].update(settings_sha256=digest('new-renderer')),
            lambda r: r['model_profiles'][1].update(model_id='another-model'),
            lambda r: r['model_profiles'][1].update(thinking_level='HIGH'),
            lambda r: r['model_profiles'][1].update(max_output_tokens=8192),
            lambda r: r['model_profiles'][0].update(prompt_version='prompt-v2'),
            lambda r: r.update(policy_version='policy-v2'),
            lambda r: r.update(generation_intent_id='new-intent'),
        )
        baseline = request_fingerprint(request())
        for mutate in mutations:
            row = request()
            mutate(row)
            with self.subTest(mutate=mutate):
                self.assertNotEqual(baseline, request_fingerprint(row))

    def test_input_hash_uses_canonical_json_with_nulls_and_utf8(self):
        # Independent known canonical retry payload guards accidental serializer changes.
        payload = dict(schema_version=SCHEMA_VERSION, workspace_id='team', principal_id='subject',
                       conversation_id='chat', operation='GENERATE', retry_key='retry-é')
        expected = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                                  separators=(',', ':')).encode('utf-8')).hexdigest()
        self.assertEqual(retry_key_hash(**{k: v for k, v in payload.items() if k != 'schema_version'}), expected)

    def test_retry_hash_is_scoped_to_principal_workspace_conversation_and_operation(self):
        base = dict(workspace_id='team', principal_id='subject', conversation_id='chat',
                    operation='GENERATE', retry_key='key')
        expected = retry_key_hash(**base)
        for key, value in (('workspace_id', 'other'), ('principal_id', 'other'),
                           ('conversation_id', 'other'), ('operation', 'INTERPRET'), ('retry_key', 'other')):
            self.assertNotEqual(expected, retry_key_hash(**dict(base, **{key: value})))
        self.assert_code('INVALID_IDENTIFIER', retry_key_hash, **dict(base, retry_key=' key'))

    def test_forged_input_hash_and_bad_json_are_rejected(self):
        row = request()
        row['request_sha256'] = '0'*64
        self.assert_code('REQUEST_HASH_MISMATCH', self.validate, row)
        for invalid in (float('nan'), float('inf'), {1: 'non-string-key'}, object(), '\ud800'):
            row = request()
            row['submitted_preferences']['extra'] = invalid
            with self.subTest(invalid=repr(invalid)), self.assertRaises(ContractError):
                request_fingerprint(row)

    def test_new_retry_and_exact_duplicate(self):
        candidate = request()
        self.assertEqual(resolve_retry(candidate, None, conversation=CONVERSATION, now=TIME), RetryDecision.NEW)
        self.assertEqual(resolve_retry(candidate, request(), conversation=CONVERSATION, now=TIME), RetryDecision.REUSE)

    def test_retry_window_boundary_and_missing_old_request(self):
        self.assertEqual(resolve_retry(request(), request(), conversation=CONVERSATION,
                                      now=TIME + timedelta(days=7)), RetryDecision.REUSE)
        self.assert_code('RETRY_WINDOW_EXPIRED', resolve_retry, request(), None,
                         conversation=CONVERSATION, now=TIME + timedelta(days=7, microseconds=1))
        self.assert_code('INVALID_RETRY_WINDOW', resolve_retry, request(), None,
                         conversation=CONVERSATION, now=TIME, retry_window=timedelta(0))

    def test_new_transport_submission_cannot_renew_an_expired_original_intent(self):
        candidate = request()
        candidate['created_at'] = candidate['updated_at'] = TIME + timedelta(days=8)
        self.assert_code('RETRY_WINDOW_EXPIRED', resolve_retry, candidate, request(),
                         conversation=CONVERSATION, now=TIME + timedelta(days=8))

    def test_same_retry_key_changed_input_conflicts(self):
        candidate = request()
        candidate['submitted_preferences']['renderer']['version'] = 'renderer-v2'
        candidate['request_sha256'] = request_fingerprint(candidate)
        self.assert_code('IDEMPOTENCY_CONFLICT', resolve_retry, candidate, request(),
                         conversation=CONVERSATION, now=TIME)

    def test_same_intent_different_retry_key_reuses_original_job(self):
        candidate = request()
        candidate['idempotency_key_sha256'] = digest('another-scoped-key')
        self.assertEqual(resolve_retry(candidate, request(), conversation=CONVERSATION, now=TIME), RetryDecision.REUSE)
        candidate['submitted_preferences']['renderer']['version'] = 'new-renderer'
        candidate['request_sha256'] = request_fingerprint(candidate)
        self.assert_code('IDEMPOTENCY_CONFLICT', resolve_retry, candidate, request(),
                         conversation=CONVERSATION, now=TIME)

    def test_unrelated_lookup_expired_retry_and_future_request_are_rejected(self):
        candidate = request()
        candidate['generation_intent_id'] = 'new-intent'
        candidate['idempotency_key_sha256'] = digest('new-key')
        candidate['request_sha256'] = request_fingerprint(candidate)
        self.assert_code('EXISTING_REQUEST_MISMATCH', resolve_retry, candidate, request(),
                         conversation=CONVERSATION, now=TIME)
        self.assert_code('RETRY_WINDOW_EXPIRED', resolve_retry, request(), request(),
                         conversation=CONVERSATION, now=TIME + timedelta(days=7, seconds=1))
        self.assert_code('INVALID_TIME_ORDER', resolve_retry, request(), None,
                         conversation=CONVERSATION, now=TIME - timedelta(seconds=1))

    def test_completed_interpretation_is_reused_for_accepted_transport_retry(self):
        completed = transition_request(request('INTERPRET'), conversation=CONVERSATION,
            target=RequestState.READY, expected_revision=1, now=TIME + timedelta(seconds=1),
            resolved_preferences=preferences())
        self.assertEqual(resolve_retry(request('INTERPRET'), completed, conversation=CONVERSATION,
                                      now=TIME + timedelta(seconds=2)), RetryDecision.REUSE)

    def test_claim_then_success_preserves_immutable_input(self):
        original = job()
        running = self.running()
        successful = self.move(running, JobState.SUCCEEDED, attempt_id='test-attempt', output_id='test-output')
        self.assertEqual(original['state'], 'QUEUED')
        self.assertEqual(running['attempt_count'], 1)
        self.assertEqual(successful['revision'], 3)
        self.assertEqual(successful['completed_at'], successful['updated_at'])
        for key in ('owner_id', 'workspace_id', 'request_id', 'request_sha256', 'generation_intent_id'):
            self.assertEqual(successful[key], original[key])

    def test_stale_revision_wrong_attempt_and_invalid_jump_cannot_change_state(self):
        self.assert_code('REVISION_CONFLICT', self.move, self.running(), JobState.FAILED,
                         expected_revision=1, attempt_id='test-attempt', safe_error_code='CALL_FAILED')
        self.assert_code('ATTEMPT_CONFLICT', self.move, self.running(), JobState.SUCCEEDED,
                         attempt_id='other-attempt', output_id='test-output')
        self.assert_code('INVALID_TRANSITION', self.move, job(), JobState.SUCCEEDED, output_id='test-output')

    def test_claim_requires_attempt_and_live_deadline(self):
        self.assert_code('INVALID_IDENTIFIER', self.move, job(), JobState.RUNNING)
        self.assert_code('DEADLINE_EXCEEDED', self.move, job(), JobState.RUNNING,
                         now=TIME + timedelta(minutes=5), attempt_id='test-attempt')

    def test_success_requires_output_and_failure_requires_safe_error(self):
        for target, kwargs in ((JobState.SUCCEEDED, {}), (JobState.FAILED, {}),
                               (JobState.BLOCKED, dict(safe_error_code='raw error with private data')),
                               (JobState.SUCCEEDED, dict(output_id='test-output', safe_error_code='FAILED'))):
            with self.subTest(target=target), self.assertRaises(ContractError):
                self.move(self.running(), target, attempt_id='test-attempt', **kwargs)

    def test_job_must_match_request_and_conversation(self):
        for key in ('owner_id', 'request_id', 'trace_id', 'canonical_sku', 'request_sha256', 'generation_intent_id'):
            row = job()
            row[key] = 'another'
            with self.subTest(key=key), self.assertRaises(ContractError):
                validate_job(row, request=request(), conversation=CONVERSATION)

    def test_malformed_job_states_counters_and_timestamps_are_rejected(self):
        for updates in (dict(state='LAUNCHED'), dict(revision=0), dict(attempt_count=-1),
                        dict(created_at=TIME - timedelta(seconds=1)), dict(updated_at=TIME.replace(tzinfo=None)),
                        dict(deadline_at=TIME), dict(active_attempt_id='unexpected'),
                        dict(output_id='unexpected'), dict(completed_at=TIME),
                        dict(safe_error_code='UNEXPECTED')):
            row = dict(job(), **updates)
            with self.subTest(updates=updates), self.assertRaises(ContractError):
                validate_job(row, request=request(), conversation=CONVERSATION)

    def test_transition_rejects_backwards_time_untyped_target_and_unexpected_dispatch_flags(self):
        self.assert_code('INVALID_TIME_ORDER', self.move, self.running(), JobState.FAILED,
                         now=TIME, attempt_id='test-attempt', safe_error_code='FAILED')
        self.assert_code('INVALID_JOB_STATE', self.move, job(), 'RUNNING', attempt_id='test-attempt')
        self.assert_code('UNEXPECTED_DISPATCH_STATUS', self.move, job(), JobState.RUNNING,
                         attempt_id='test-attempt', dispatch_status=DispatchStatus.NOT_DISPATCHED)

    def test_terminal_jobs_cannot_be_reopened(self):
        for target in (JobState.SUCCEEDED, JobState.BLOCKED, JobState.FAILED):
            row = self.move(self.running(), target, attempt_id='test-attempt',
                            output_id='test-output' if target == JobState.SUCCEEDED else None,
                            safe_error_code=None if target == JobState.SUCCEEDED else 'FAILED')
            self.assert_code('INVALID_TRANSITION', self.move, row, JobState.RUNNING, attempt_id='test-attempt')

    def test_uncertain_or_dispatched_recovery_cannot_requeue(self):
        row = self.recovery()
        for status in (None, DispatchStatus.UNKNOWN, DispatchStatus.DISPATCHED):
            self.assert_code('UNSAFE_DISPATCH_REPLAY', self.move, row, JobState.QUEUED,
                             attempt_id='test-attempt', dispatch_status=status)

    def test_confirmed_no_dispatch_recovery_can_requeue_then_claim_new_attempt(self):
        queued = self.move(self.recovery(), JobState.QUEUED, attempt_id='test-attempt',
                           dispatch_status=DispatchStatus.NOT_DISPATCHED)
        self.assertIsNone(queued['active_attempt_id'])
        running = self.move(queued, JobState.RUNNING, attempt_id='new-attempt')
        self.assertEqual(running['attempt_count'], 2)
        failed = self.move(queued, JobState.FAILED, safe_error_code='ADMISSION_DENIED')
        self.assertEqual(failed['state'], 'FAILED')

    def test_reconciliation_can_commit_existing_output_without_new_dispatch(self):
        row = self.move(self.recovery(), JobState.SUCCEEDED, attempt_id='test-attempt', output_id='existing-output')
        self.assertEqual(row['attempt_count'], 1)
        self.assertEqual(row['output_id'], 'existing-output')

    def test_validation_errors_are_safe_and_no_cloud_clients_are_constructed(self):
        with patch('google.cloud.bigquery.Client', side_effect=AssertionError('No cloud calls')):
            self.validate(request())
            self.running()
        row = request()
        row['submitted_preferences']['preferences']['audience'] = 'PRIVATE\nSECRET'
        with self.assertRaises(ContractError) as caught:
            request_fingerprint(row)
        self.assertNotIn('PRIVATE', str(caught.exception))
        self.assertNotIn('SECRET', str(caught.exception))
