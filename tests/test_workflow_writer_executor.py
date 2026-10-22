"""Offline single-submit workflow-writer executor tests with real SDK jobs."""
from copy import deepcopy
from unittest.mock import Mock
import unittest

from google.api_core.exceptions import Forbidden,NotFound,ServiceUnavailable
from google.cloud import bigquery

from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION,TABLES
from src.application.workflow_writer_executor import (
    WorkflowWriterError,WorkflowWriterExecutor,WorkflowWriterSettings,_normalize,
)
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_setup_workflow_tables import dataset,table
from tests import (
    test_workflow_attempt_query as attempt_fixtures,
    test_workflow_evidence_query as evidence_fixtures,
    test_workflow_failure_query as failure_fixtures,
    test_workflow_invocation_query as invocation_fixtures,
    test_workflow_invocation_result_query as result_fixtures,
    test_workflow_output_query as output_fixtures,
    test_workflow_start_query as start_fixtures,
)


def plan_job_id(plan):
    return getattr(plan,'native_job_id',getattr(plan,'job_id',None))


def native_job(plan,rows,*,state='DONE',error=None,sql=None):
    job_id=plan_job_id(plan)
    job=bigquery.QueryJob(job_id,plan.sql if sql is None else sql,client=Mock(project=PROJECT),job_config=plan.config())
    job._properties['jobReference'].update(projectId=PROJECT,location=LOCATION)
    job._properties['status']=dict(state=state)
    if error is not None:job._properties['status']['errorResult']=error
    job._properties['statistics']=dict(query=dict(statementType='SCRIPT'))
    job.result=Mock(return_value=rows)
    return job


def child(plan,statement,*,error=None,state='DONE',location=LOCATION):
    job=bigquery.QueryJob('child_'+statement,statement,client=Mock(project=PROJECT))
    job._properties['jobReference'].update(projectId=PROJECT,location=location)
    job._properties['statistics']=dict(parentJobId=plan_job_id(plan),query=dict(statementType=statement))
    job._properties['status']=dict(state=state)
    if error is not None:job._properties['status']['errorResult']=error
    return job


class ClientFixture:
    project=PROJECT
    location=LOCATION
    default_query_job_config=None

    def __init__(self,plan,row):
        self.plan,self.row=plan,row;self.jobs={};self.children=[]
        self.lookups=[];self.submissions=[];self.schemas=[];self.child_calls=[]
        self.lookup_error=None;self.query_error=None

    def get_dataset(self,name,**kwargs):
        self.schemas.append(('dataset',name,kwargs));return dataset()

    def get_table(self,reference,**kwargs):
        self.schemas.append(('table',reference.table_id,kwargs));return table(reference.table_id)

    def get_job(self,job_id,**kwargs):
        self.lookups.append((job_id,kwargs))
        if self.lookup_error is not None:raise self.lookup_error
        if job_id not in self.jobs:raise NotFound('missing')
        return self.jobs[job_id]

    def query(self,sql,**kwargs):
        self.submissions.append((sql,kwargs))
        job=native_job(self.plan,[deepcopy(self.row)]);self.jobs[kwargs['job_id']]=job
        if self.query_error is not None:raise self.query_error
        return job

    def list_jobs(self,**kwargs):
        self.child_calls.append(kwargs);return iter(self.children)


class WorkflowWriterExecutorTests(unittest.TestCase):
    def setUp(self):
        fixture=failure_fixtures.WorkflowFailureQueryTests(
            'test_plan_is_deterministic_bounded_parameterized_and_output_free')
        fixture.setUp();self.plan=fixture.prepare();self.row=fixture.result(self.plan)
        self.client=ClientFixture(self.plan,self.row)
        self.executor=WorkflowWriterExecutor(self.client,WorkflowWriterSettings(execution_enabled=True))

    def assert_code(self,code,function,*args,**kwargs):
        with self.assertRaises((ContractError,WorkflowWriterError)) as caught:function(*args,**kwargs)
        self.assertEqual(caught.exception.code,code)

    def test_default_disabled_before_schema_lookup_or_submission(self):
        executor=WorkflowWriterExecutor(self.client)
        self.assert_code('WORKFLOW_WRITES_DISABLED',executor.submit,self.plan)
        self.assert_code('WORKFLOW_WRITES_DISABLED',executor.recover,self.plan)
        self.assertEqual(self.client.schemas+self.client.lookups+self.client.submissions,[])

    def test_submit_preflights_all_tables_and_inserts_once_with_exact_job_id(self):
        outcome=self.executor.submit(self.plan)
        self.assertEqual((outcome.native_status,outcome.result_state,outcome.submitted),
            (SharingCommitStatus.COMMITTED,'APPLIED',True))
        self.assertEqual(len([row for row in self.client.schemas if row[0]=='table']),len(TABLES))
        self.assertEqual(len(self.client.submissions),1)
        sql,kwargs=self.client.submissions[0]
        self.assertEqual((sql,kwargs['job_id'],kwargs['project'],kwargs['location']),
            (self.plan.sql,self.plan.native_job_id,PROJECT,LOCATION))
        self.assertIsNone(kwargs['retry']);self.assertIsNone(kwargs['job_retry'])
        self.assertEqual(kwargs['api_method'],bigquery.enums.QueryApiMethod.INSERT)

    def test_repeated_submit_recovers_existing_job_without_second_insert(self):
        first=self.executor.submit(self.plan);second=self.executor.submit(self.plan)
        self.assertEqual((first.native_status,second.native_status),
            (SharingCommitStatus.COMMITTED,SharingCommitStatus.COMMITTED))
        self.assertTrue(first.submitted);self.assertFalse(second.submitted)
        self.assertEqual(len(self.client.submissions),1)

    def test_lost_submission_response_recovers_same_original_job(self):
        self.client.query_error=ServiceUnavailable('lost response with private content')
        outcome=self.executor.submit(self.plan)
        self.assertEqual((outcome.native_status,outcome.result_state,outcome.submitted),
            (SharingCommitStatus.COMMITTED,'APPLIED',True))
        self.assertEqual(len(self.client.submissions),1)
        self.assertEqual({item[0] for item in self.client.lookups},{self.plan.native_job_id})

    def test_unavailable_lookup_or_missing_recovery_is_unknown_and_never_submits(self):
        self.client.lookup_error=Forbidden('unavailable')
        outcome=self.executor.submit(self.plan)
        self.assertEqual(outcome.native_status,SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.submissions,[])
        self.client.lookup_error=NotFound('missing')
        recovered=self.executor.recover(self.plan)
        self.assertEqual(recovered.native_status,SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.submissions,[])

    def test_pending_job_waits_once_and_malformed_or_mismatched_result_is_unknown(self):
        pending=native_job(self.plan,[deepcopy(self.row)],state='RUNNING')
        def finish(**_kwargs):
            pending._properties['status']={'state':'DONE'}
            return [deepcopy(self.row)]
        pending.result.side_effect=finish;self.client.jobs[self.plan.native_job_id]=pending
        self.assertEqual(self.executor.recover(self.plan).native_status,SharingCommitStatus.COMMITTED)
        for job in (native_job(self.plan,[]),native_job(self.plan,[self.row,self.row]),
                    native_job(self.plan,[{'state':'APPLIED'}]),native_job(self.plan,[self.row],sql='SELECT 1')):
            self.client.jobs[self.plan.native_job_id]=job
            outcome=self.executor.recover(self.plan)
            self.assertEqual(outcome.native_status,SharingCommitStatus.UNKNOWN)

    def test_failed_parent_requires_correlated_commit_or_rollback_children(self):
        failed=native_job(self.plan,[],error={'reason':'backendError'})
        self.client.jobs[self.plan.native_job_id]=failed
        self.client.children=[child(self.plan,'ROLLBACK_TRANSACTION')]
        self.assertEqual(self.executor.recover(self.plan).native_status,SharingCommitStatus.NOT_COMMITTED)
        self.client.children=[child(self.plan,'COMMIT_TRANSACTION')]
        self.assertEqual(self.executor.recover(self.plan).native_status,SharingCommitStatus.COMMITTED)
        self.client.children=[]
        self.assertEqual(self.executor.recover(self.plan).native_status,SharingCommitStatus.UNKNOWN)

    def test_reused_result_and_invalid_plan_settings_or_client_fail_closed(self):
        self.row['state']='REUSED'
        self.assertEqual(self.executor.submit(self.plan).result_state,'REUSED')
        self.assert_code('INVALID_WORKFLOW_WRITE_PLAN',self.executor.submit,{})
        for values in (dict(execution_enabled=1),dict(rpc_timeout_seconds=0),
                       dict(result_timeout_seconds=61),dict(result_timeout_seconds=True)):
            self.assert_code('INVALID_WORKFLOW_WRITER_SETTINGS',WorkflowWriterSettings,**values)
        self.client.default_query_job_config=bigquery.QueryJobConfig()
        with self.assertRaises(ValueError):WorkflowWriterExecutor(self.client)

    def test_all_seven_typed_plan_results_use_their_exact_validator(self):
        fixtures=((start_fixtures.WorkflowStartQueryTests,'test_builds_deterministic_bounded_parameterized_transaction'),
            (attempt_fixtures.WorkflowAttemptQueryTests,'test_builds_running_job_attempt_first_event_and_distinct_audits'),
            (evidence_fixtures.WorkflowEvidenceQueryTests,'test_builds_complete_sorted_bundle_and_append_audit'),
            (invocation_fixtures.WorkflowInvocationQueryTests,'test_builds_planned_invocation_from_pinned_profile_and_audit'),
            (result_fixtures.WorkflowInvocationResultQueryTests,
                'test_success_records_response_usage_exact_cost_and_separate_audits'),
            (output_fixtures.WorkflowOutputQueryTests,'test_builds_terminal_success_output_events_and_audits'),
            (failure_fixtures.WorkflowFailureQueryTests,
                'test_plan_is_deterministic_bounded_parameterized_and_output_free'))
        for fixture_type,method in fixtures:
            with self.subTest(plan=fixture_type.__name__):
                fixture=fixture_type(method);fixture.setUp();plan=fixture.prepare();row=fixture.result(plan)
                client=ClientFixture(plan,row)
                outcome=WorkflowWriterExecutor(client,WorkflowWriterSettings(execution_enabled=True)).submit(plan)
                self.assertEqual((outcome.native_status,outcome.result_state),
                    (SharingCommitStatus.COMMITTED,'APPLIED'))
                self.assertEqual(client.submissions[0][1]['job_id'],plan_job_id(plan))

    def test_normalization_is_bounded_and_errors_do_not_expose_provider_content(self):
        for value in ({str(index):index for index in range(33)},list(range(17)),[[[[[1]]]]]):
            self.assert_code('INVALID_WORKFLOW_WRITE_RESULT',_normalize,value)
        self.client.query_error=ServiceUnavailable('private provider details')
        self.client.jobs.clear()
        self.client.get_job=lambda *_args,**_kwargs: (_ for _ in ()).throw(ServiceUnavailable('private lookup'))
        outcome=self.executor.submit(self.plan)
        self.assertEqual(outcome.native_status,SharingCommitStatus.UNKNOWN)
        self.assertNotIn('private',repr(outcome))


if __name__=='__main__':unittest.main()
