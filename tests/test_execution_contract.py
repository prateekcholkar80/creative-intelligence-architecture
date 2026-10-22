"""Pure attempt/event contracts: unpublished fixtures, no clients or writer proofs."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.workflow_contract import (
    CAMPAIGN_STAGES, ContractError, DispatchStatus, JobState, append_stage_event,
    request_fingerprint, transition_job, validate_attempt, validate_stage_event, validate_stage_events,
)
from src.application.workflow_schema import TABLES
from tests.test_workflow_contract import CONVERSATION, TIME, job, request


def execution(state='RUNNING', sku='CI-SKU-0001'):
    req = request()
    req['canonical_sku'] = req['submitted_preferences']['preferences']['canonical_sku'] = sku
    req['request_sha256'] = request_fingerprint(req)
    parent = transition_job(job(req), request=req, conversation=CONVERSATION,
        target=JobState.RUNNING, expected_revision=1, attempt_id='test-attempt', now=TIME+timedelta(seconds=1))
    if state != 'RUNNING':
        parent = transition_job(parent, request=req, conversation=CONVERSATION,
            target=JobState(state), expected_revision=2, attempt_id='test-attempt', now=TIME+timedelta(seconds=20),
            output_id='test-output' if state == 'SUCCEEDED' else None,
            safe_error_code=None if state == 'SUCCEEDED' else 'GROUNDING_CHECK_FAILED')
    attempt = dict.fromkeys(f.name for f in TABLES['job_attempts'].schema)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id', 'trace_id'):
        attempt[key] = parent[key]
    attempt.update(attempt_id='test-attempt', attempt_number=1, state=state, revision=1 if state == 'RUNNING' else 2,
        worker_id='test-worker', dispatch_receipt_id='test-receipt' if state == 'SUCCEEDED' else None,
        safe_error_code=parent['safe_error_code'], started_at=TIME+timedelta(seconds=1),
        updated_at=parent['updated_at'], completed_at=parent['completed_at'])
    return attempt, req, parent


def event(attempt, parent, sequence=1, *, stage='RETRIEVING_EVIDENCE', outcome='STARTED', at=None):
    row = {key: attempt[key] for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id',
                                        'job_id', 'request_id', 'attempt_id', 'trace_id')}
    row.update(event_id=f'test-event-{sequence}', sequence=sequence, stage=stage, outcome=outcome,
        duration_ms=None if outcome == 'STARTED' else 1000,
        safe_error_code='GROUNDING_CHECK_FAILED' if outcome in {'FAILED', 'BLOCKED'} else None,
        policy_version=parent['policy_version'], occurred_at=at or TIME+timedelta(seconds=sequence+1))
    return row


def successful_events(attempt, parent):
    return [event(attempt, parent, 2*index+offset, stage=stage, outcome=outcome)
            for index, stage in enumerate(CAMPAIGN_STAGES)
            for offset, outcome in ((1, 'STARTED'), (2, 'SUCCEEDED'))]


class ExecutionContractTests(unittest.TestCase):
    def setUp(self):
        self.attempt, self.request, self.job = execution()

    def attempt_validate(self, row=None, parent=None):
        validate_attempt(self.attempt if row is None else row, request=self.request,
                         job=self.job if parent is None else parent, conversation=CONVERSATION)

    def event_validate(self, row):
        validate_stage_event(row, attempt=self.attempt, request=self.request, job=self.job, conversation=CONVERSATION)

    def history_validate(self, rows):
        validate_stage_events(rows, attempt=self.attempt, request=self.request, job=self.job, conversation=CONVERSATION)

    def append(self, rows, candidate):
        return append_stage_event(rows, candidate, attempt=self.attempt, request=self.request,
                                  job=self.job, conversation=CONVERSATION)

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_each_attempt_state_matches_current_parent(self):
        for state in ('RUNNING', 'RECOVERY_REQUIRED', 'SUCCEEDED', 'BLOCKED', 'FAILED'):
            attempt, request, parent = execution(state)
            with self.subTest(state=state):
                validate_attempt(attempt, request=request, job=parent, conversation=CONVERSATION)

    def test_all_twenty_skus_inherit_canonical_identity_from_request_and_job(self):
        for index in range(1, 21):
            attempt, req, parent = execution(sku=f'CI-SKU-{index:04d}')
            validate_attempt(attempt, request=req, job=parent, conversation=CONVERSATION)
            validate_stage_event(event(attempt, parent), attempt=attempt, request=req,
                                 job=parent, conversation=CONVERSATION)

    def test_exact_attempt_schema_required_and_no_inline_sensitive_content(self):
        for mutate in (lambda r: r.pop('dispatch_receipt_id'), lambda r: r.update(raw_prompt='secret'),
                       lambda r: r.update(canonical_sku='CI-SKU-0001'), lambda r: r.update(state=None)):
            row = deepcopy(self.attempt)
            mutate(row)
            with self.subTest(mutate=mutate), self.assertRaises(ContractError):
                self.attempt_validate(row)

    def test_attempt_rejects_wrong_scope_parent_trace_and_schema(self):
        for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id', 'trace_id'):
            with self.subTest(key=key), self.assertRaises(ContractError):
                self.attempt_validate(self.attempt | {key: 'other'})

    def test_parent_request_must_be_valid(self):
        self.request['request_sha256'] = '0'*64
        self.assert_code('REQUEST_HASH_MISMATCH', self.attempt_validate)

    def test_untyped_missing_or_unsafe_worker_and_receipt_fail(self):
        for key in ('worker_id', 'attempt_id', 'dispatch_receipt_id'):
            values = ('bad\nworker', 'bad worker', 'x'*129, 1, '')
            if key != 'dispatch_receipt_id':
                values += (None,)
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaises(ContractError):
                    self.attempt_validate(self.attempt | {key: value})

    def test_positive_strict_int_counters_and_int64_bound(self):
        for key in ('revision', 'attempt_number'):
            for value in (0, -1, True, 1.0, '1', 2**63):
                with self.subTest(key=key, value=value), self.assertRaises(ContractError):
                    self.attempt_validate(self.attempt | {key: value})
        self.assert_code('INVALID_ATTEMPT_COUNT', self.attempt_validate, self.attempt | dict(attempt_number=2))

    def test_unknown_or_queued_attempt_state_denied(self):
        for state in ('QUEUED', 'CANCELLED', 'UNKNOWN'):
            self.assert_code('INVALID_ATTEMPT_STATE', self.attempt_validate, self.attempt | dict(state=state))

    def test_current_attempt_id_number_and_state_must_match_parent(self):
        self.assert_code('INVALID_HISTORICAL_ATTEMPT', self.attempt_validate, self.attempt | dict(attempt_id='other'))
        parent = self.job | dict(attempt_count=2)
        self.assert_code('INVALID_ATTEMPT_COUNT', self.attempt_validate, parent=parent)
        changed = self.attempt | dict(state='RECOVERY_REQUIRED', safe_error_code='OUTCOME_UNKNOWN')
        self.assert_code('ACTIVE_ATTEMPT_MISMATCH', self.attempt_validate, changed)

    def test_running_attempt_has_no_error_or_completion(self):
        for changes in (dict(safe_error_code='FAILED'), dict(completed_at=TIME+timedelta(seconds=2))):
            self.assert_code('INVALID_ACTIVE_ATTEMPT', self.attempt_validate, self.attempt | changes)

    def test_recovery_attempt_requires_error_but_receipt_can_be_unknown(self):
        self.attempt, self.request, self.job = execution('RECOVERY_REQUIRED')
        self.assert_code('INVALID_ACTIVE_ATTEMPT', self.attempt_validate, self.attempt | dict(safe_error_code=None))
        self.attempt_validate()
        self.attempt_validate(self.attempt | dict(dispatch_receipt_id='test-dispatched-receipt'))

    def test_success_requires_receipt_and_same_parent_completion(self):
        self.attempt, self.request, self.job = execution('SUCCEEDED')
        self.assert_code('MISSING_DISPATCH_RECEIPT', self.attempt_validate, self.attempt | dict(dispatch_receipt_id=None))
        row = self.attempt | dict(updated_at=TIME+timedelta(seconds=19), completed_at=TIME+timedelta(seconds=19))
        self.assert_code('ACTIVE_ATTEMPT_MISMATCH', self.attempt_validate, row)
        self.assert_code('INVALID_TERMINAL_ATTEMPT', self.attempt_validate, self.attempt | dict(safe_error_code='FAILED'))

    def test_blocked_failed_attempts_require_safe_matching_parent_error(self):
        for state in ('BLOCKED', 'FAILED'):
            self.attempt, self.request, self.job = execution(state)
            self.assert_code('INVALID_TERMINAL_ATTEMPT', self.attempt_validate, self.attempt | dict(safe_error_code=None))
            self.assert_code('ACTIVE_ATTEMPT_MISMATCH', self.attempt_validate, self.attempt | dict(safe_error_code='OTHER_ERROR'))
        for code in ('private exception', 'bad-code', 'a'*65, 'lowercase'):
            self.assert_code('INVALID_SAFE_ERROR', self.attempt_validate, self.attempt | dict(safe_error_code=code))

    def test_attempt_timestamps_are_aware_ordered_and_claim_before_deadline(self):
        for changes in (dict(started_at=TIME-timedelta(seconds=1)), dict(updated_at=TIME),
                        dict(started_at=self.job['deadline_at'], updated_at=self.job['deadline_at']),
                        dict(started_at=TIME.replace(tzinfo=None)), dict(updated_at=TIME.isoformat())):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                self.attempt_validate(self.attempt | changes)

    def test_closed_attempt_completion_must_equal_its_update_time(self):
        self.attempt, self.request, self.job = execution('FAILED')
        for value in (None, TIME+timedelta(seconds=19)):
            self.assert_code('INVALID_COMPLETED_TIME', self.attempt_validate, self.attempt | dict(completed_at=value))

    def requeued(self):
        old, req, recovering = execution('RECOVERY_REQUIRED')
        queued = transition_job(recovering, request=req, conversation=CONVERSATION,
            target=JobState.QUEUED, expected_revision=3, attempt_id='test-attempt',
            dispatch_status=DispatchStatus.NOT_DISPATCHED, now=TIME+timedelta(seconds=21))
        old.update(state='FAILED', safe_error_code='DISPATCH_NOT_STARTED', completed_at=old['updated_at'])
        return old, req, queued

    def test_closed_undispatched_attempt_survives_requeue_and_new_claim(self):
        old, req, queued = self.requeued()
        validate_attempt(old, request=req, job=queued, conversation=CONVERSATION)
        claimed = transition_job(queued, request=req, conversation=CONVERSATION,
            target=JobState.RUNNING, expected_revision=4, attempt_id='second-attempt', now=TIME+timedelta(seconds=22))
        validate_attempt(old, request=req, job=claimed, conversation=CONVERSATION)
        current = old | dict(attempt_id='second-attempt', attempt_number=2, state='RUNNING', revision=1,
            completed_at=None, safe_error_code=None, started_at=claimed['updated_at'], updated_at=claimed['updated_at'])
        validate_attempt(current, request=req, job=claimed, conversation=CONVERSATION)

    def test_old_active_unknown_dispatched_or_successful_attempt_cannot_be_requeued(self):
        old, req, queued = self.requeued()
        for changes in (dict(state='RECOVERY_REQUIRED', completed_at=None), dict(dispatch_receipt_id='already-sent'),
                        dict(state='SUCCEEDED', safe_error_code=None, dispatch_receipt_id='already-sent')):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                validate_attempt(old | changes, request=req, job=queued, conversation=CONVERSATION)

    def test_historical_attempt_cannot_use_current_number_or_finish_after_parent(self):
        old, req, queued = self.requeued()
        claimed = transition_job(queued, request=req, conversation=CONVERSATION,
            target=JobState.RUNNING, expected_revision=4, attempt_id='second-attempt', now=TIME+timedelta(seconds=22))
        self.assert_code('INVALID_ATTEMPT_COUNT', validate_attempt, old | dict(attempt_number=2),
            request=req, job=claimed, conversation=CONVERSATION)
        self.assert_code('INVALID_TIME_ORDER', validate_attempt,
            old | dict(updated_at=TIME+timedelta(seconds=23), completed_at=TIME+timedelta(seconds=23)),
            request=req, job=claimed, conversation=CONVERSATION)

    def test_stage_vocabulary_matches_existing_local_flow(self):
        from src.application.campaign_chat import STAGES
        self.assertIs(STAGES, CAMPAIGN_STAGES)
        self.assertEqual(STAGES, ('RETRIEVING_EVIDENCE', 'PLANNING', 'RENDERING', 'VERIFYING'))

    def test_event_exact_schema_and_parent_policy_links(self):
        original = event(self.attempt, self.job)
        for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id',
                    'request_id', 'attempt_id', 'trace_id', 'policy_version'):
            with self.subTest(key=key), self.assertRaises(ContractError):
                self.event_validate(original | {key: 'other'})
        for mutate in (lambda r: r.pop('duration_ms'), lambda r: r.update(raw_response='secret')):
            row = deepcopy(original)
            mutate(row)
            self.assert_code('INVALID_ROW_SHAPE', self.event_validate, row)

    def test_event_id_sequence_and_vocabulary_are_bounded(self):
        original = event(self.attempt, self.job)
        for key, value in (('event_id', 'a'*129), ('event_id', 'bad id'), ('sequence', 0), ('sequence', True),
                           ('sequence', 2**63), ('stage', 'INTERPRETING'), ('outcome', 'APPROVED')):
            with self.subTest(key=key, value=value), self.assertRaises(ContractError):
                self.event_validate(original | {key: value})

    def test_started_event_omits_duration_and_error(self):
        for changes in (dict(duration_ms=0), dict(safe_error_code='FAILED')):
            self.assert_code('INVALID_STARTED_EVENT', self.event_validate, event(self.attempt, self.job) | changes)

    def test_terminal_event_duration_error_and_time_are_validated(self):
        original = event(self.attempt, self.job, 2, outcome='SUCCEEDED')
        for value in (None, -1, True, 1.0, 2**63):
            with self.subTest(value=value), self.assertRaises(ContractError):
                self.event_validate(original | dict(duration_ms=value))
        self.assert_code('INVALID_SUCCESSFUL_EVENT', self.event_validate, original | dict(safe_error_code='FAILED'))
        for outcome in ('FAILED', 'BLOCKED'):
            row = event(self.attempt, self.job, 2, outcome=outcome)
            self.event_validate(row)
            self.assert_code('MISSING_EVENT_ERROR', self.event_validate, row | dict(safe_error_code=None))
            self.assert_code('INVALID_SAFE_ERROR', self.event_validate, row | dict(safe_error_code='private cloud error'))

    def test_event_timestamps_within_attempt_bounds(self):
        self.assert_code('INVALID_TIME_ORDER', self.event_validate, event(self.attempt, self.job, at=TIME))
        self.attempt, self.request, self.job = execution('FAILED')
        self.assert_code('INVALID_TIME_ORDER', self.event_validate,
                         event(self.attempt, self.job, at=TIME+timedelta(seconds=21)))
        with self.assertRaises(ContractError):
            self.event_validate(event(self.attempt, self.job, at=TIME.replace(tzinfo=None)))

    def test_each_inflight_prefix_and_complete_success_are_valid(self):
        rows = successful_events(self.attempt, self.job)
        for length in range(9):
            self.history_validate(rows[:length])
        self.attempt, self.request, self.job = execution('SUCCEEDED')
        self.history_validate(successful_events(self.attempt, self.job))

    def test_success_requires_all_four_completed_stages(self):
        self.attempt, self.request, self.job = execution('SUCCEEDED')
        rows = successful_events(self.attempt, self.job)
        for length in range(8):
            self.assert_code('INCOMPLETE_SUCCESSFUL_STAGES', self.history_validate, rows[:length])

    def test_history_is_complete_ordered_prefix_not_paginated_fragment(self):
        rows = successful_events(self.attempt, self.job)
        for changes in (rows[2:], [rows[1]], [rows[0], rows[0] | dict(event_id='different', sequence=2)],
                        [rows[0], rows[1] | dict(sequence=3)], [rows[0] | dict(stage='PLANNING')]):
            with self.subTest(length=len(changes)), self.assertRaises(ContractError):
                self.history_validate(changes)

    def test_duplicate_event_ids_rejected_even_if_rows_identical(self):
        row = event(self.attempt, self.job)
        self.assert_code('DUPLICATE_EVENT_ID', self.history_validate, [row, deepcopy(row)])

    def test_history_time_must_not_go_backwards(self):
        rows = successful_events(self.attempt, self.job)[:3]
        rows[2]['occurred_at'] = rows[0]['occurred_at']
        self.assert_code('INVALID_TIME_ORDER', self.history_validate, rows)

    def test_duration_matches_start_finish_using_integer_millisecond_floor(self):
        start = event(self.attempt, self.job)
        finish = event(self.attempt, self.job, 2, outcome='SUCCEEDED',
                       at=start['occurred_at']+timedelta(microseconds=1999)) | dict(duration_ms=1)
        self.history_validate([start, finish])
        self.assert_code('EVENT_DURATION_MISMATCH', self.history_validate, [start, finish | dict(duration_ms=2)])
        finish.update(occurred_at=start['occurred_at'], duration_ms=0)
        self.history_validate([start, finish])

    def test_failed_stage_stops_sequence_and_cannot_support_success(self):
        rows = [event(self.attempt, self.job), event(self.attempt, self.job, 2, outcome='FAILED')]
        self.history_validate(rows)
        self.assert_code('EVENT_AFTER_TERMINAL_STAGE', self.history_validate,
                         rows+[event(self.attempt, self.job, 3, stage='PLANNING')])
        self.attempt, self.request, self.job = execution('SUCCEEDED')
        self.assert_code('INCOMPLETE_SUCCESSFUL_STAGES', self.history_validate, rows)

    def test_failed_blocked_stage_matches_closed_attempt_outcome_and_error(self):
        for state in ('FAILED', 'BLOCKED'):
            self.attempt, self.request, self.job = execution(state)
            rows = [event(self.attempt, self.job), event(self.attempt, self.job, 2, outcome=state)]
            self.history_validate(rows)
            rows[1]['safe_error_code'] = 'OTHER_ERROR'
            self.assert_code('EVENT_ATTEMPT_OUTCOME_MISMATCH', self.history_validate, rows)

    def test_interrupted_attempt_can_retain_unfinished_stage_without_fabricated_outcome(self):
        for state in ('RECOVERY_REQUIRED', 'FAILED', 'BLOCKED'):
            self.attempt, self.request, self.job = execution(state)
            self.history_validate([])
            self.history_validate([event(self.attempt, self.job)])

    def test_append_and_identical_retry_return_copies_without_mutating_inputs(self):
        candidate = event(self.attempt, self.job)
        original = deepcopy(candidate)
        initial = []
        result = self.append(initial, candidate)
        replay = self.append(result, candidate)
        self.assertEqual(replay, result)
        self.assertEqual(initial, [])
        self.assertEqual(candidate, original)
        self.assertIsNot(replay, result)
        replay[0]['event_id'] = 'changed'
        self.assertEqual(result[0]['event_id'], candidate['event_id'])

    def test_reused_id_with_changed_content_or_sequence_conflicts(self):
        candidate = event(self.attempt, self.job)
        for changes in (dict(sequence=2), dict(occurred_at=candidate['occurred_at']+timedelta(seconds=1))):
            self.assert_code('EVENT_IDEMPOTENCY_CONFLICT', self.append, [candidate], candidate | changes)
        self.assert_code('EVENT_SEQUENCE_CONFLICT', self.append, [candidate], candidate | dict(event_id='new'))

    def test_replay_still_validates_whole_history_and_parent(self):
        rows = successful_events(self.attempt, self.job)[:2]
        self.assert_code('DUPLICATE_EVENT_ID', self.append, rows+[rows[0]], rows[0])
        rows[1]['duration_ms'] = 0
        self.assert_code('EVENT_DURATION_MISMATCH', self.append, rows, rows[0])
        self.request['request_sha256'] = '0'*64
        self.assert_code('REQUEST_HASH_MISMATCH', self.append, rows, rows[0])

    def test_final_event_can_be_appended_against_proposed_success(self):
        self.attempt, self.request, self.job = execution('SUCCEEDED')
        rows = successful_events(self.attempt, self.job)
        self.assertEqual(self.append(rows[:-1], rows[-1]), rows)
        self.assertEqual(self.append(rows, rows[-1]), rows)

    def test_history_type_and_size_are_bounded_before_append(self):
        row = event(self.attempt, self.job)
        for rows in (None, {}, (), [row]*9):
            self.assert_code('INVALID_EVENT_HISTORY', self.history_validate, rows)
            self.assert_code('INVALID_EVENT_HISTORY', self.append, rows, row)

    def test_validators_do_not_mutate_rows_create_clients_or_dispatch(self):
        records = deepcopy((self.attempt, self.request, self.job))
        with patch('google.cloud.bigquery.Client') as bq, patch('google.cloud.storage.Client') as gcs:
            self.attempt_validate()
            self.history_validate(successful_events(self.attempt, self.job))
        bq.assert_not_called()
        gcs.assert_not_called()
        self.assertEqual((self.attempt, self.request, self.job), records)

    def test_errors_do_not_disclose_worker_receipts_or_row_content(self):
        with self.assertRaises(ContractError) as caught:
            self.attempt_validate(self.attempt | dict(worker_id='private worker value'))
        self.assertNotIn('private', str(caught.exception))
        self.assertNotIn('worker', str(caught.exception))


if __name__ == '__main__':
    unittest.main()
