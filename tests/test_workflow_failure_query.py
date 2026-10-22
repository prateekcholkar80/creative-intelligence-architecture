"""Offline FAILED/BLOCKED workflow-finalization tests; no cloud submission."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.identity import ISSUER,principal_for_subject
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import ContractError,JobState,request_fingerprint,transition_job
from src.application.workflow_failure_query import (
    prepare_workflow_failure,validate_workflow_failure_result,
)
from src.application.workflow_schema import SCHEMA_VERSION,TABLES
from tests.test_workflow_contract import CONVERSATION,TIME,job as job_fixture,request as request_fixture


class WorkflowFailureQueryTests(unittest.TestCase):
    def setUp(self):
        subject='accounts.google.com:workflow-failure-user';principal=principal_for_subject(ISSUER,subject)
        self.request=request_fixture();self.request.update(owner_id=principal.principal_id,
            principal_id=principal.principal_id,trace_id='workflow-failure-trace')
        self.request['request_sha256']=request_fingerprint(self.request)
        self.resource=type(CONVERSATION)(self.request['conversation_id'],self.request['workspace_id'],principal.principal_id)
        self.job=transition_job(job_fixture(self.request),request=self.request,conversation=self.resource,
            target=JobState.RUNNING,expected_revision=1,attempt_id='test-attempt',now=TIME+timedelta(seconds=1))
        self.attempt=dict.fromkeys(field.name for field in TABLES['job_attempts'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','job_id','request_id','trace_id'):
            self.attempt[key]=self.job[key]
        self.attempt.update(attempt_id='test-attempt',attempt_number=1,state='RUNNING',revision=2,
            dispatch_receipt_id='test-receipt',worker_id='test-worker',safe_error_code=None,
            started_at=TIME+timedelta(seconds=1),updated_at=TIME+timedelta(seconds=1),completed_at=None)
        self.workspace=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',status='ACTIVE',revision=13,
            created_at=TIME-timedelta(days=1),updated_at=TIME+timedelta(seconds=5))
        self.membership=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',principal_id=principal.principal_id,
            identity_issuer=ISSUER,identity_subject=subject,roles=['CREATOR'],status='ACTIVE',revision=4,
            assignment_origin='ADMIN',updated_by=principal.principal_id,
            created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.conversation=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            conversation_id='test-conversation',owner_id=principal.principal_id,visibility='PRIVATE',status='ACTIVE',
            access_revision=5,created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.invocation=dict.fromkeys(field.name for field in TABLES['model_invocations'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','request_id','trace_id'):
            self.invocation[key]=self.request[key]
        self.invocation.update(invocation_id='test-invocation',job_id=self.job['job_id'],
            attempt_id=self.attempt['attempt_id'],stage='PLANNER',call_number=1,state='FAILED',revision=3,
            dispatch_receipt_id='test-receipt',worker_id='test-worker',
            profile=deepcopy(next(p for p in self.request['model_profiles'] if p['stage']=='PLANNER')),
            returned_model_id=None,response_sha256=None,finish_reason=None,safety_outcome='NOT_EVALUATED',
            safe_error_code='RESPONSE_INVALID',usage_status='NOT_REPORTED',cost_status='NOT_ESTIMATED',
            started_at=TIME+timedelta(seconds=3),completed_at=TIME+timedelta(seconds=4))
        self.finalized=TIME+timedelta(seconds=6)
        self.events=self._events('FAILED','RESPONSE_INVALID',self.finalized)

    def _events(self,state,error,finalized):
        definitions=(('RETRIEVING_EVIDENCE','STARTED',TIME+timedelta(seconds=1),None),
            ('RETRIEVING_EVIDENCE','SUCCEEDED',TIME+timedelta(seconds=2),1000),
            ('PLANNING','STARTED',TIME+timedelta(seconds=2),None),
            ('PLANNING',state,finalized,int((finalized-(TIME+timedelta(seconds=2))).total_seconds()*1000)))
        rows=[]
        for sequence,(stage,outcome,occurred,duration) in enumerate(definitions,1):
            row={key:self.attempt[key] for key in ('schema_version','workspace_id','conversation_id','owner_id',
                'job_id','request_id','attempt_id','trace_id')}
            row.update(event_id=f'test-event-{sequence}',sequence=sequence,stage=stage,outcome=outcome,
                duration_ms=duration,safe_error_code=error if outcome in {'FAILED','BLOCKED'} else None,
                policy_version=self.job['policy_version'],occurred_at=occurred)
            rows.append(row)
        return tuple(rows)

    def prepare(self,**changes):
        values=dict(workspace=self.workspace,membership=self.membership,conversation=self.conversation,
            request=self.request,job=self.job,attempt=self.attempt,invocation=self.invocation,
            stage_events=self.events,finalized_at=self.finalized)
        return prepare_workflow_failure(**(values|changes))

    @staticmethod
    def result(plan,state='APPLIED'):
        return dict(state=state,job_row=plan.job_json,attempt_row=plan.attempt_json,
            event_rows=list(plan.event_rows_json),audit_rows=list(plan.audit_rows_json))

    def test_failed_and_blocked_close_job_attempt_and_terminal_event(self):
        cases=(('FAILED','RESPONSE_INVALID'),('BLOCKED','MODEL_BLOCKED'))
        for state,error in cases:
            with self.subTest(state=state):
                invocation=self.invocation|{'state':state,'safe_error_code':error,
                    'safety_outcome':'BLOCKED' if state=='BLOCKED' else 'NOT_EVALUATED',
                    'finish_reason':'SAFETY' if state=='BLOCKED' else None}
                events=self._events(state,error,self.finalized)
                plan=self.prepare(invocation=invocation,stage_events=events)
                job=decode_row(plan.job_json,'jobs');attempt=decode_row(plan.attempt_json,'job_attempts')
                self.assertEqual((job['state'],job['safe_error_code'],job['output_id']),(state,error,None))
                self.assertEqual((attempt['state'],attempt['safe_error_code']),(state,error))
                actions=[decode_row(value,'audit_events')['action'] for value in plan.audit_rows_json]
                self.assertEqual(actions[:2],['UPDATE_JOB','UPDATE_ATTEMPT'])
                self.assertEqual(actions[2:],['APPEND_STAGE_EVENT']*3)

    def test_late_recorded_failure_can_close_without_retrying_model_call(self):
        finalized=self.job['deadline_at']+timedelta(seconds=1)
        plan=self.prepare(finalized_at=finalized,
            stage_events=self._events('FAILED','RESPONSE_INVALID',finalized))
        self.assertEqual(decode_row(plan.job_json,'jobs')['completed_at'],finalized)

    def test_plan_is_deterministic_bounded_parameterized_and_output_free(self):
        first=self.prepare();second=self.prepare();self.assertEqual(first,second)
        self.assertTrue(first.native_job_id.startswith('workflow_failure_v1_'))
        config=first.config();params={p.name:(p.value if hasattr(p,'value') else p.values) for p in config.query_parameters}
        self.assertFalse(config.use_query_cache);self.assertFalse(config.create_session)
        self.assertEqual((params['terminal_state'],params['safe_error_code']),('FAILED','RESPONSE_INVALID'))
        self.assertEqual((params['new_event_row_count'],params['audit_row_count']),(3,5))
        for value in ('WORKFLOW_FAILURE_IDEMPOTENCY_CONFLICT','WORKFLOW_FAILURE_OUTPUT_CONFLICT',
                      'WORKFLOW_FAILURE_FENCE_CONFLICT','ROLLBACK TRANSACTION'):
            self.assertIn(value,first.sql)
        self.assertNotIn('__',first.sql);self.assertNotIn('DELETE ',first.sql)

    def test_exact_applied_and_reused_results_only(self):
        plan=self.prepare()
        self.assertEqual(validate_workflow_failure_result(plan,self.result(plan)),'APPLIED')
        self.assertEqual(validate_workflow_failure_result(plan,self.result(plan,'REUSED')),'REUSED')
        for change in (dict(state='UNKNOWN'),dict(event_rows=[]),dict(audit_rows=[]),dict(extra='x')):
            with self.subTest(change=change),self.assertRaises(ContractError):
                validate_workflow_failure_result(plan,self.result(plan)|change)

    def test_access_parent_invocation_and_terminal_event_conflicts_fail_closed(self):
        mismatched=list(deepcopy(self.events));mismatched[-1]['safe_error_code']='OTHER_ERROR'
        cases=(dict(membership=self.membership|{'status':'REVOKED'}),
            dict(job=self.job|{'active_attempt_id':'other'}),
            dict(invocation=self.invocation|{'state':'UNKNOWN','completed_at':None,
                'safe_error_code':'CALL_OUTCOME_UNKNOWN'}),dict(stage_events=tuple(mismatched)),
            dict(stage_events=self.events[:-1]))
        for values in cases:
            with self.subTest(values=values),self.assertRaises((ContractError,PermissionError)):
                self.prepare(**values)

    def test_inputs_are_immutable_and_no_cloud_or_model_clients_are_created(self):
        before=deepcopy((self.workspace,self.membership,self.conversation,self.request,self.job,
            self.attempt,self.invocation,self.events))
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('no bq client')), \
             patch('google.cloud.storage.Client',side_effect=AssertionError('no gcs client')), \
             patch('google.genai.Client',side_effect=AssertionError('no model client')):
            self.prepare()
        self.assertEqual(before,(self.workspace,self.membership,self.conversation,self.request,self.job,
            self.attempt,self.invocation,self.events))


if __name__=='__main__':unittest.main()
