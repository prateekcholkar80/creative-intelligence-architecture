"""Offline post-dispatch result/accounting transaction tests."""
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
import unittest
from unittest.mock import patch

from src.application.identity import ISSUER,principal_for_subject
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import (
    ContractError,JobState,request_fingerprint,transition_job,validate_model_invocation_change,
)
from src.application.workflow_invocation_result_query import (
    ModelResultObservation,prepare_workflow_invocation_result,validate_workflow_invocation_result_write,
)
from src.application.workflow_schema import SCHEMA_VERSION,TABLES
from tests.test_workflow_contract import CONVERSATION,TIME,job as job_fixture,request as request_fixture,digest


class WorkflowInvocationResultQueryTests(unittest.TestCase):
    def setUp(self):
        subject='accounts.google.com:workflow-result-user';principal=principal_for_subject(ISSUER,subject)
        self.request=request_fixture();self.request.update(owner_id=principal.principal_id,
            principal_id=principal.principal_id,trace_id='workflow-result-trace')
        self.request['request_sha256']=request_fingerprint(self.request)
        self.resource=type(CONVERSATION)(self.request['conversation_id'],self.request['workspace_id'],principal.principal_id)
        self.job=transition_job(job_fixture(self.request),request=self.request,conversation=self.resource,
            target=JobState.RUNNING,expected_revision=1,attempt_id='test-attempt',now=TIME+timedelta(seconds=1))
        self.attempt=dict.fromkeys(field.name for field in TABLES['job_attempts'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','job_id','request_id','trace_id'):
            self.attempt[key]=self.job[key]
        self.attempt.update(attempt_id='test-attempt',attempt_number=1,state='RUNNING',revision=1,
            dispatch_receipt_id=None,worker_id='test-worker',safe_error_code=None,
            started_at=TIME+timedelta(seconds=1),updated_at=TIME+timedelta(seconds=1),completed_at=None)
        planned=dict.fromkeys(field.name for field in TABLES['model_invocations'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','request_id','trace_id'):
            planned[key]=self.request[key]
        planned.update(invocation_id='test-invocation',job_id=self.job['job_id'],attempt_id=self.attempt['attempt_id'],
            stage='PLANNER',call_number=1,state='PLANNED',revision=1,
            profile=deepcopy(next(p for p in self.request['model_profiles'] if p['stage']=='PLANNER')),
            usage_status='NOT_REPORTED',cost_status='NOT_ESTIMATED',started_at=TIME+timedelta(seconds=3))
        receipt='dispatch-receipt';self.attempt=self.attempt|dict(dispatch_receipt_id=receipt,revision=2)
        self.invocation=planned|dict(state='DISPATCHED',revision=2,dispatch_receipt_id=receipt,worker_id='test-worker')
        validate_model_invocation_change(planned,self.invocation,expected_revision=1,request=self.request,
            conversation=self.resource,job=self.job,attempt=self.attempt,
            previous_job=self.job,previous_attempt=self.attempt|dict(dispatch_receipt_id=None,revision=1))
        self.workspace=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',status='ACTIVE',revision=12,
            created_at=TIME-timedelta(days=1),updated_at=TIME+timedelta(seconds=3))
        self.membership=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',principal_id=principal.principal_id,
            identity_issuer=ISSUER,identity_subject=subject,roles=['CREATOR'],status='ACTIVE',revision=4,
            assignment_origin='ADMIN',updated_by=principal.principal_id,
            created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.conversation=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            conversation_id='test-conversation',owner_id=principal.principal_id,visibility='PRIVATE',status='ACTIVE',
            access_revision=5,created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.observed=TIME+timedelta(seconds=4)
        self.success=ModelResultObservation('SUCCEEDED','gemini-3.5-flash-001',digest('response'),'STOP','PASSED',
            input_tokens=100,output_tokens=20,cached_tokens=10,thinking_tokens=5,
            estimated_cost=Decimal('0.001234567'),currency='USD',pricing_version='pricing-v1')

    def prepare(self,observation=None,**changes):
        values=dict(workspace=self.workspace,membership=self.membership,conversation=self.conversation,
            request=self.request,job=self.job,attempt=self.attempt,invocation=self.invocation,
            observation=observation or self.success,observed_at=self.observed)
        return prepare_workflow_invocation_result(**(values|changes))

    @staticmethod
    def result(plan,state='APPLIED'):
        return dict(state=state,invocation_row=plan.invocation_json,job_row=plan.job_json,
            attempt_row=plan.attempt_json,result_audit=plan.result_audit_json,
            accounting_audit=plan.accounting_audit_json,recovery_job_audit=plan.recovery_job_audit_json,
            recovery_attempt_audit=plan.recovery_attempt_audit_json)

    def test_success_records_response_usage_exact_cost_and_separate_audits(self):
        plan=self.prepare();row=decode_row(plan.invocation_json,'model_invocations')
        self.assertEqual((row['state'],row['revision'],row['usage_status'],row['cost_status']),
            ('SUCCEEDED',3,'REPORTED','ESTIMATED'))
        self.assertEqual(row['estimated_cost'],Decimal('0.001234567'))
        audits=[decode_row(value,'audit_events') for value in (plan.result_audit_json,plan.accounting_audit_json)]
        self.assertEqual([audit['action'] for audit in audits],['RECORD_MODEL_RESULT','RECORD_MODEL_ACCOUNTING'])
        self.assertIsNone(plan.recovery_job_audit_json);self.assertNotIn('recovery_job_audit_id',plan.sql)

    def test_failed_blocked_and_unknown_are_explicit_and_unknown_fences_parents(self):
        observations=(ModelResultObservation('FAILED',safe_error_code='RESPONSE_INVALID',safety_outcome='NOT_EVALUATED'),
            ModelResultObservation('BLOCKED',returned_model_id='gemini-3.5-flash-001',finish_reason='SAFETY',
                safety_outcome='BLOCKED',safe_error_code='MODEL_BLOCKED'),
            ModelResultObservation('UNKNOWN',safe_error_code='CALL_OUTCOME_UNKNOWN',safety_outcome='NOT_EVALUATED'))
        for observation in observations:
            with self.subTest(state=observation.state):
                plan=self.prepare(observation);row=decode_row(plan.invocation_json,'model_invocations')
                self.assertEqual(row['state'],observation.state)
                if observation.state=='UNKNOWN':
                    self.assertEqual(decode_row(plan.job_json,'jobs')['state'],'RECOVERY_REQUIRED')
                    self.assertEqual(decode_row(plan.attempt_json,'job_attempts')['state'],'RECOVERY_REQUIRED')
                    self.assertIsNotNone(plan.recovery_job_audit_json);self.assertIn('recovery_job_audit_id',plan.sql)
                else:self.assertEqual(decode_row(plan.job_json,'jobs')['state'],'RUNNING')

    def test_plan_is_deterministic_bounded_and_has_no_provider_call(self):
        first=self.prepare();second=self.prepare();self.assertEqual(first,second)
        self.assertTrue(first.native_job_id.startswith('workflow_result_v1_'))
        config=first.config();self.assertFalse(config.use_query_cache);self.assertFalse(config.create_session)
        self.assertIn('WORKFLOW_RESULT_IDEMPOTENCY_CONFLICT',first.sql)
        self.assertIn('dispatch_receipt_id=@dispatch_receipt_id',first.sql)
        self.assertNotIn('__',first.sql);self.assertNotIn('DELETE ',first.sql)

    def test_late_provider_result_is_still_recorded_for_accounting(self):
        plan=self.prepare(observed_at=self.job['deadline_at']+timedelta(seconds=1))
        self.assertEqual(decode_row(plan.invocation_json,'model_invocations')['state'],'SUCCEEDED')

    def test_exact_applied_and_reused_results_only(self):
        for plan in (self.prepare(),self.prepare(ModelResultObservation('UNKNOWN',safe_error_code='CALL_OUTCOME_UNKNOWN'))):
            self.assertEqual(validate_workflow_invocation_result_write(plan,self.result(plan)),'APPLIED')
            self.assertEqual(validate_workflow_invocation_result_write(plan,self.result(plan,'REUSED')),'REUSED')
            with self.assertRaises(ContractError):
                validate_workflow_invocation_result_write(plan,self.result(plan)|{'state':'UNKNOWN'})

    def test_revocation_receipt_mismatch_stale_time_and_inexact_cost_fail_closed(self):
        cases=(dict(membership=self.membership|{'status':'REVOKED'}),
            dict(invocation=self.invocation|{'dispatch_receipt_id':'other'}),
            dict(observed_at=self.invocation['started_at']-timedelta(seconds=1)),
            dict(observation=ModelResultObservation('SUCCEEDED','gemini-3.5-flash-001',digest('response'),'STOP','PASSED',
                input_tokens=1,output_tokens=1,cached_tokens=0,thinking_tokens=0,
                estimated_cost=0.1,currency='USD',pricing_version='pricing-v1')))
        for values in cases:
            with self.subTest(values=values),self.assertRaises((ContractError,PermissionError)):
                self.prepare(**values)

    def test_inputs_are_immutable_and_no_cloud_or_model_clients_are_created(self):
        before=deepcopy((self.workspace,self.membership,self.conversation,self.request,self.job,
            self.attempt,self.invocation,self.success))
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('no bq client')), \
             patch('google.genai.Client',side_effect=AssertionError('no model client')):
            self.prepare()
        self.assertEqual((self.workspace,self.membership,self.conversation,self.request,self.job,
            self.attempt,self.invocation,self.success),before)


if __name__=='__main__':unittest.main()
