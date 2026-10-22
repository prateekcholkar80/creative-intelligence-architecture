"""SDK-shaped offline reads, job correlation and original-job-only recovery."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock
import re
import threading
import unittest

from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.cloud import bigquery

from src.application.prompt_evaluation_storage import TABLES as EVALUATION_TABLES, encode_control as encode_ev_control, encode_evaluation
from src.application.prompt_evaluation_store import EvaluationStoreSettings
from src.application.prompt_lifecycle_contract import TABLES, encode_control
from src.application.prompt_lifecycle_query import config, sql, parameters
from src.application.prompt_lifecycle_store import (
    BigQueryPromptLifecycleStore, LifecycleStoreError, decode_snapshot, read_sql, read_config,
)
from src.application.prompt_registry_codec import encode_registry_state
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError
from tests.test_prompt_lifecycle_contract import plan, snapshot
from tests.test_prompt_lifecycle_query import receipt
from tests.test_prompt_evaluation_contract import evaluation
from tests.test_prompt_evaluation_store import Client as EvaluationClient
from tests.test_prompt_registry_executor import child


class Client(EvaluationClient):
    def __init__(self):
        super().__init__(None)
        self.query_calls = []

    def get_table(self, ref, **kwargs):
        if ref.table_id not in TABLES:
            return super().get_table(ref, **kwargs)
        table = TABLES[ref.table_id].table(self.project)
        table._properties.update(type='TABLE', location=self.location)
        return table

    def list_jobs(self, **kwargs):
        return iter(c for c in self.children if c.parent_job_id==kwargs['parent_job'])


def native_job(job_id, script, cfg):
    job = bigquery.QueryJob(job_id, script, client=Mock(project=Client.project), job_config=cfg)
    job._properties['jobReference'].update(projectId=Client.project, location=Client.location)
    job._properties['status'] = {'state':'DONE'}
    job._properties['statistics'] = {'query':{'statementType':'SCRIPT'}}
    return job


def snapshot_rows(capture):
    row = dict(observed_at=capture.observed_at, controls=[] if capture.control is None else [encode_control(capture.control)],
        control_history=[encode_control(item) for item in capture.control_history])
    target, history = encode_registry_state(capture.target)
    row.update(target_heads=[target], target_history=list(history), active_heads=[], active_history=[])
    if capture.active is not None:
        active, chain = encode_registry_state(capture.active)
        row.update(active_heads=[active], active_history=list(chain))
    row.update({name:[] for name in EVALUATION_TABLES})
    if capture.evaluation is not None:
        ev = capture.evaluation
        report, cases = encode_evaluation(capture.evaluation_control_anchor, ev.record)
        row.update(prompt_evaluation_controls=[encode_ev_control(ev.control)],
            prompt_evaluation_control_history=[encode_ev_control(c) for c in ev.control_history],
            prompt_evaluations=[report], prompt_evaluation_cases=list(cases))
    row.update({name+'_count':len(values) for name, values in list(row.items()) if name!='observed_at'})
    return [row]


class LifecycleStoreTests(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        self.store = BigQueryPromptLifecycleStore(self.client, EvaluationStoreSettings(True))
        self.plan = plan()

    def install_job(self, p=None, *, rows=None, failed=False):
        p = self.plan if p is None else p
        job = native_job(p.job_id, sql(p), config(p))
        job.result = Mock(return_value=[receipt(p)] if rows is None else rows)
        if failed:
            job._properties['status']['errorResult'] = {'reason':'invalidQuery'}
        self.client.jobs[p.job_id] = job
        return job

    def test_default_disabled_and_incompatible_schema_deny_before_submit(self):
        with self.assertRaises(Exception):
            BigQueryPromptLifecycleStore(self.client).submit(self.plan)
        self.client.get_table = Mock(side_effect=NotFound('missing'))
        with self.assertRaises(LifecycleStoreError):
            self.store.submit(self.plan)
        self.assertEqual(self.client.query_calls, [])

    def test_duplicate_exact_job_lookup_never_resubmits(self):
        self.install_job()
        for _ in range(2):
            outcome = self.store.submit(self.plan)
            self.assertIs(outcome.native_status, SharingCommitStatus.COMMITTED)
            self.assertFalse(outcome.submitted)
        self.assertEqual(self.client.query_calls, [])

    def test_first_submission_then_exact_duplicate_inserts_once(self):
        def submit(script, **kwargs):
            self.client.query_calls.append(kwargs)
            return self.install_job()
        self.client.query = submit
        first = self.store.submit(self.plan)
        second = self.store.submit(self.plan)
        self.assertIs(first.native_status, SharingCommitStatus.COMMITTED)
        self.assertTrue(first.submitted)
        self.assertFalse(second.submitted)
        self.assertEqual(len(self.client.query_calls),1)
        self.assertIsNone(self.client.query_calls[0]['job_retry'])

    def test_lost_submission_response_recovers_original_once(self):
        def submit(script, **kwargs):
            self.client.query_calls.append(kwargs)
            self.install_job()
            raise ServiceUnavailable('discarded response')
        self.client.query = submit
        outcome = self.store.submit(self.plan)
        self.assertIs(outcome.native_status, SharingCommitStatus.COMMITTED)
        self.assertTrue(outcome.submitted)
        self.assertEqual(len(self.client.query_calls), 1)
        self.assertEqual(self.client.query_calls[0]['job_id'], self.plan.job_id)

    def test_missing_recovery_and_unavailable_lookup_do_not_insert(self):
        self.assertIs(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        self.client.get_job = Mock(side_effect=ServiceUnavailable('unknown'))
        self.assertIs(self.store.submit(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.query_calls, [])

    def test_competing_workspace_or_payload_same_slot_returns_unknown_without_write(self):
        self.install_job()
        for other in (plan(snapshot(record=evaluation(), workspace='other-workspace')),
                      replace(self.plan, reason_code='DIFFERENT_REVIEW')):
            self.assertEqual(other.job_id, self.plan.job_id)
            self.assertIs(self.store.submit(other).native_status, SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.query_calls, [])

    def test_tampered_job_or_receipt_never_proves_commit(self):
        for changes in ({'command_digest':'a'*64}, {'state':'UNKNOWN'}, {'revision':True}):
            self.install_job(rows=[receipt(self.plan)|changes])
            self.assertIs(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)
        job = self.install_job()
        job._properties['configuration']['query']['query'] += '\n-- changed'
        self.assertIs(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)

    def test_native_job_changed_during_result_consumption_denies_commit(self):
        job = self.install_job()
        def result(**kwargs):
            job._properties['configuration']['query']['useQueryCache'] = True
            return [receipt(self.plan)]
        job.result.side_effect = result
        self.assertIs(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)

    def test_same_policy_second_control_revision_keeps_exact_stored_report_anchor(self):
        capture = self.plan.capture
        first = capture.evaluation.control
        second = replace(first,revision=2,parent_digest=first.digest,
            updated_at=first.updated_at+timedelta(seconds=1))
        current = replace(capture,evaluation=replace(capture.evaluation,
            control=second,control_history=(first,second)),evaluation_control_anchor=second)
        decoded = decode_snapshot(current.request,snapshot_rows(current))
        self.assertEqual(decoded.evaluation_control_anchor,second)
        self.assertEqual(decoded,current)

    def test_correlated_failed_children_do_not_invent_business_receipt(self):
        self.install_job(failed=True)
        self.client.children = [child(self.plan, 'COMMIT_TRANSACTION')]
        outcome = self.store.recover(self.plan)
        self.assertIs(outcome.native_status, SharingCommitStatus.COMMITTED)
        self.assertIsNone(outcome.receipt)
        self.client.children = [child(self.plan, 'ROLLBACK_TRANSACTION')]
        self.assertIs(self.store.recover(self.plan).native_status, SharingCommitStatus.NOT_COMMITTED)
        self.client.children = []
        self.assertIs(self.store.recover(self.plan).native_status, SharingCommitStatus.UNKNOWN)

    def test_single_snapshot_roundtrip_and_no_export_fallback(self):
        capture = self.plan.capture
        self.assertEqual(decode_snapshot(capture.request, snapshot_rows(capture)), capture)
        def query(script, **kwargs):
            self.client.query_calls.append(kwargs)
            job = native_job(kwargs['job_id'], script, kwargs['job_config'])
            job.result = Mock(return_value=snapshot_rows(capture))
            return job
        self.client.query = query
        self.assertEqual(self.store.read(capture.request), capture)
        self.assertEqual(len(self.client.query_calls), 1)
        self.assertIsNone(self.client.query_calls[0]['retry'])

    def test_duplicate_partial_or_overflow_reads_rejected(self):
        for name in ('controls', 'target_heads', 'target_history', 'prompt_evaluation_cases'):
            rows = snapshot_rows(self.plan.capture)
            row = rows[0]
            if name=='controls':
                row[name] = [encode_control(self.plan.control)]*2
            else:
                row[name] += row[name]
            row[name+'_count'] = len(row[name])
            with self.assertRaises(Exception):
                decode_snapshot(self.plan.capture.request, rows)
        rows = snapshot_rows(self.plan.capture)
        rows[0]['target_history_count'] += 1
        with self.assertRaises(ContractError):
            decode_snapshot(self.plan.capture.request, rows)

    def test_read_job_scope_mismatch_denies_before_result(self):
        job = native_job('wrong', read_sql(), read_config(self.plan.capture.request))
        job.result = Mock(return_value=snapshot_rows(self.plan.capture))
        self.client.query = Mock(return_value=job)
        with self.assertRaises(LifecycleStoreError):
            self.store.read(self.plan.capture.request)
        job.result.assert_not_called()

    def test_read_sql_is_one_transaction_with_qualified_history_and_all_current_access(self):
        script = read_sql()
        self.assertEqual(script.count('BEGIN TRANSACTION'), 1)
        self.assertIn('t.snapshot_sha256=r.snapshot_sha256', script)
        self.assertIn('changed_at BETWEEN', script)
        self.assertIn('revision=@membership_revision', script)
        self.assertIn('ASSERTION_EXPIRED', script)
        self.assertNotIn('UPDATE ', script)
        self.assertNotIn('INSERT INTO', script)
        self.assertEqual(set(re.findall(r'(?<!@)@([a-z_]+)', script)),
            {p.name for p in read_config(self.plan.capture.request).query_parameters})

    def retry_parent(self,p):
        self.install_job(p,failed=True)
        prior = list(self.client.children)
        self.client.children = prior+[child(p,'ROLLBACK_TRANSACTION')]

    def test_positive_rollback_retry_preserves_business_pins_and_submits_once(self):
        self.retry_parent(self.plan)
        retry = self.store.issue_retry(self.plan)
        self.assertEqual(retry.control,self.plan.control)
        self.assertEqual(retry.capture,self.plan.capture)
        self.assertNotEqual(retry.job_id,self.plan.job_id)
        self.assertEqual(config(retry).to_api_repr(),config(self.plan).to_api_repr())
        def query(script,**kwargs):
            self.client.query_calls.append(kwargs)
            return self.install_job(retry)
        self.client.query = query
        outcome = self.store.submit(retry)
        self.assertIs(outcome.native_status,SharingCommitStatus.COMMITTED)
        self.store.submit(retry)
        self.assertEqual(len(self.client.query_calls),1)

    def test_unknown_committed_mismatched_or_missing_predecessor_cannot_issue_retry(self):
        with self.assertRaises(Exception):
            self.store.issue_retry(self.plan)
        self.install_job()
        with self.assertRaises(ContractError):
            self.store.issue_retry(self.plan)
        self.install_job(failed=True)
        for children in ([],[child(self.plan,'COMMIT_TRANSACTION')]):
            self.client.children = children
            with self.assertRaises(ContractError):
                self.store.issue_retry(self.plan)
        self.retry_parent(self.plan)
        job = self.client.jobs[self.plan.job_id]
        job._properties['configuration']['query']['query']+='-- altered'
        with self.assertRaises(ContractError):
            self.store.issue_retry(self.plan)
        self.assertEqual(self.client.query_calls,[])

    def test_retry_rechecks_rollback_before_absent_job_insert_and_known_job_recovers(self):
        self.retry_parent(self.plan)
        retry = self.store.issue_retry(self.plan)
        self.client.children=[]
        self.assertIs(self.store.submit(retry).native_status,SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.query_calls,[])
        self.install_job(retry)
        self.assertIs(self.store.submit(retry).native_status,SharingCommitStatus.COMMITTED)
        self.assertIs(self.store.recover(retry).native_status,SharingCommitStatus.COMMITTED)
        self.assertEqual(self.client.query_calls,[])

    def test_retry_slots_are_bounded_and_cannot_rebind_any_original_command_pin(self):
        self.retry_parent(self.plan)
        second = self.store.issue_retry(self.plan)
        self.retry_parent(second)
        third = self.store.issue_retry(second)
        self.assertEqual(third.native_slot,3)
        with self.assertRaises(ContractError):
            self.store.issue_retry(third)
        for changes in ({'ancestors':()}, {'native_slot':True}, {'reason_code':'OTHER_REVIEW'},
                        {'changed_at':second.changed_at+timedelta(seconds=1)}):
            with self.assertRaises(ContractError):
                replace(second,**changes)

    def test_competing_global_slots_with_discarded_responses_submit_one_mock_job(self):
        # A SDK-shaped, locked native-slot fixture, not native SQL race evidence.
        lock, barrier = threading.Lock(), threading.Barrier(2)
        insertions = []
        def get_job(job_id, **kwargs):
            with lock:
                existing = self.client.jobs.get(job_id)
            if existing is not None:
                return existing
            barrier.wait(timeout=3)
            raise NotFound('not found')
        def query(script, **kwargs):
            with lock:
                if kwargs['job_id'] not in self.client.jobs:
                    p = self.plan if kwargs['job_config'].to_api_repr()==config(self.plan).to_api_repr() else competitor
                    job = native_job(p.job_id, script, kwargs['job_config'])
                    job.result = Mock(return_value=[receipt(p)])
                    self.client.jobs[p.job_id] = job
                    insertions.append(p.command_digest)
            raise ServiceUnavailable('lost/conflict response')
        self.client.get_job, self.client.query = get_job, query
        competitor = plan(snapshot(record=evaluation(), workspace='other-workspace'))
        outcomes = []
        threads = [threading.Thread(target=lambda p=p: outcomes.append(self.store.submit(p)))
            for p in (self.plan, competitor)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(4)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(insertions), 1)
        self.assertEqual(sorted(o.native_status.value for o in outcomes), ['COMMITTED', 'UNKNOWN'])


if __name__ == '__main__':
    unittest.main()
