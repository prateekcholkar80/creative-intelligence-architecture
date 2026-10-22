"""Offline guards for Batch 14's planned native concurrency and corrupt-state cases."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock

from google.cloud import bigquery

from src.application.prompt_evaluation_storage import decode_control_history, decode_evaluation
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.validation.prompt_evaluation_remaining import (
    PARTIAL_CASES, RemainingProbe, assert_scope_preserved, remaining_fixtures,
    transaction_interval, validate_race,
)
from src.validation.prompt_evaluation_integration import TABLES


class EvaluationRemainingTests(unittest.TestCase):
    def setUp(self):
        self.rows, self.contexts = remaining_fixtures()
        self.at = datetime.now(timezone.utc)

    def test_all_fixture_rows_have_exact_destination_fields(self):
        self.assertEqual(set(self.rows), set(TABLES))
        for name, rows in self.rows.items():
            for row in rows:
                self.assertEqual(set(row), {field.name for field in TABLES[name].schema})
        self.assertEqual(len(self.contexts), 8)

    def test_control_corruption_is_intentional_and_decoders_deny(self):
        for case in ('head_without_history', 'history_gap'):
            control, _ = self.contexts[case]
            history = [row for row in self.rows['prompt_evaluation_control_history']
                       if row['workspace_id'] == control.control.workspace_id]
            with self.assertRaises(ContractError) as error:
                decode_control_history(control.control, history)
            self.assertEqual(error.exception.code, PARTIAL_CASES[case][0])
        control, _ = self.contexts['history_without_head']
        self.assertFalse(any(row['workspace_id'] == control.control.workspace_id
                             for row in self.rows['prompt_evaluation_controls']))

    def test_report_missing_cases_and_orphans_are_scoped(self):
        for case, count in (('report_without_cases', 0), ('report_missing_case', 9), ('orphan_case', 1)):
            control, report = self.contexts[case]
            cases = [row for row in self.rows['prompt_evaluation_cases']
                     if row['workspace_id'] == control.control.workspace_id]
            self.assertEqual(len(cases), count)
            summaries = [row for row in self.rows['prompt_evaluations']
                         if row['workspace_id'] == control.control.workspace_id]
            if case == 'orphan_case':
                self.assertEqual(summaries, [])
            else:
                with self.assertRaises(ContractError) as error:
                    decode_evaluation(control.control, summaries[0], cases, report.registry_state)
                self.assertEqual(error.exception.code, 'INVALID_EVALUATION_CASE_COUNT')

    def child(self, statement, at, parent='parent'):
        job = bigquery.QueryJob(bigquery.job._JobReference('child-' + statement, PROJECT, LOCATION),
            'SELECT 1', Mock(project=PROJECT))
        job._properties.update(jobReference=dict(projectId=PROJECT, location=LOCATION,
            jobId='child-' + statement), status={'state': 'DONE'}, statistics={
            'parentJobId': parent, 'startTime': int(at.timestamp() * 1000),
            'endTime': int((at + timedelta(seconds=1)).timestamp() * 1000),
            'query': {'statementType': statement}})
        return job

    def test_transaction_evidence_requires_native_parent_scope_and_terminal_children(self):
        children = [self.child('BEGIN_TRANSACTION', self.at),
                    self.child('ROLLBACK_TRANSACTION', self.at + timedelta(seconds=3))]
        begin, end = transaction_interval(children, 'parent')
        self.assertLess(begin, end)
        for invalid in ([], children[:1], children + children,
                        [self.child('BEGIN_TRANSACTION', self.at, 'foreign'), children[1]]):
            with self.assertRaises(RuntimeError):
                transaction_interval(invalid, 'parent')

    def race_jobs(self):
        return [dict(status='COMMITTED', receipt_state='APPLIED', error=None),
                dict(status='NOT_COMMITTED', receipt_state=None, error={
                    'reason': 'invalidQuery', 'message': 'Transaction is aborted due to concurrent update'})]

    def test_race_requires_actual_overlap_not_just_concurrent_submission(self):
        jobs = self.race_jobs()
        intervals = [(self.at, self.at + timedelta(seconds=3)),
                     (self.at + timedelta(seconds=1), self.at + timedelta(seconds=4))]
        self.assertEqual(validate_race(jobs, intervals, ()), 0)
        with self.assertRaises(RuntimeError):
            validate_race(jobs, [intervals[0], (intervals[0][1], intervals[1][1])], ())

    def test_race_unknown_double_winners_and_incidental_errors_cannot_pass(self):
        intervals = [(self.at, self.at + timedelta(seconds=3))] * 2
        for change in ({'status': 'UNKNOWN'}, {'status': 'COMMITTED', 'receipt_state': 'APPLIED', 'error': None},
                       {'error': {'reason': 'invalidQuery', 'message': 'Syntax error'}}):
            jobs = self.race_jobs()
            jobs[1].update(change)
            with self.assertRaises(RuntimeError):
                validate_race(jobs, intervals, ())

    def test_fresh_fixture_context_does_not_change_records_or_actor(self):
        plan = self.contexts['report_race'][1]
        fresh = RemainingProbe.fresh(plan)
        self.assertEqual(fresh.record, plan.record)
        self.assertEqual(fresh.control, plan.control)
        self.assertEqual(fresh.identity.principal, plan.identity.principal)
        self.assertEqual(fresh.identity.expires_at - fresh.identity.issued_at, 600)

    def test_preservation_rejects_unrelated_rows_or_more_than_one_fence(self):
        before = {name: [] for name in TABLES}
        row = {'workspace_id': 'target', 'revision': 1, 'updated_at': self.at.isoformat(), 'status': 'ACTIVE'}
        before['workspaces'] = [json.dumps(row)]
        after = deepcopy(before)
        after['workspaces'] = [json.dumps(row | {'revision': 2})]
        assert_scope_preserved(before, after, 'target', {'prompt_evaluations'})
        bad = deepcopy(after)
        bad['memberships'] = [json.dumps({'workspace_id': 'other'})]
        with self.assertRaises(RuntimeError):
            assert_scope_preserved(before, bad, 'target', {'prompt_evaluations'})
        after['workspaces'] = [json.dumps(row | {'revision': 3})]
        with self.assertRaises(RuntimeError):
            assert_scope_preserved(before, after, 'target', {'prompt_evaluations'})

    def test_partial_reader_does_not_accept_wrong_error_or_success(self):
        probe = RemainingProbe(Mock(project=PROJECT, location=LOCATION, default_query_job_config=None))
        plan = self.contexts['report_race'][1]
        probe.read = Mock(side_effect=ContractError('OTHER_ERROR'))
        with self.assertRaises(RuntimeError):
            probe.deny_read(plan, 'ORPHAN_EVALUATION_CASES')
        probe.read = Mock(return_value=object())
        with self.assertRaises(RuntimeError):
            probe.deny_read(plan, 'ORPHAN_EVALUATION_CASES')


if __name__ == '__main__':
    unittest.main()
