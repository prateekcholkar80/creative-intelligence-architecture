"""Offline successful output-publication transaction tests; no cloud calls."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.evidence_bundle import PlanningEvidenceBundle
from src.application.identity import ISSUER,principal_for_subject
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import ContractError,JobState,request_fingerprint,transition_job
from src.application.workflow_evidence_query import prepare_workflow_evidence
from src.application.workflow_output_query import (
    ArtifactVerification,prepare_workflow_output,validate_workflow_output_result,
)
from src.application.workflow_schema import SCHEMA_VERSION,TABLES
from tests.test_evidence_contract import SKU,SNAPSHOT,complete_bundle
from tests.test_output_contract import context as output_context
from tests.test_workflow_contract import CONVERSATION,TIME,digest,job as job_fixture,request as request_fixture


class WorkflowOutputQueryTests(unittest.TestCase):
    def setUp(self):
        subject='accounts.google.com:workflow-output-user';principal=principal_for_subject(ISSUER,subject)
        self.request=request_fixture();self.request.update(owner_id=principal.principal_id,
            principal_id=principal.principal_id,trace_id='workflow-output-trace')
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
        rows=complete_bundle()
        for row in rows:row['owner_id']=principal.principal_id
        bundle=PlanningEvidenceBundle('test-evidence-snapshot',SNAPSHOT,SKU,tuple(rows))
        base_workspace=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',status='ACTIVE',revision=9,
            created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.membership=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',principal_id=principal.principal_id,
            identity_issuer=ISSUER,identity_subject=subject,roles=['CREATOR'],status='ACTIVE',revision=4,
            assignment_origin='ADMIN',updated_by=principal.principal_id,
            created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.conversation=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            conversation_id='test-conversation',owner_id=principal.principal_id,visibility='PRIVATE',status='ACTIVE',
            access_revision=5,created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.evidence_plan=prepare_workflow_evidence(workspace=base_workspace,membership=self.membership,
            conversation=self.conversation,request=self.request,job=self.job,attempt=self.attempt,bundle=bundle,
            worker_id='test-worker',observed_at=TIME+timedelta(seconds=2))
        self.workspace=base_workspace|{'revision':12,'updated_at':TIME+timedelta(seconds=4)}
        self.invocation=dict.fromkeys(field.name for field in TABLES['model_invocations'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','request_id','trace_id'):
            self.invocation[key]=self.request[key]
        self.invocation.update(invocation_id='test-invocation',job_id=self.job['job_id'],
            attempt_id=self.attempt['attempt_id'],stage='PLANNER',call_number=1,state='SUCCEEDED',revision=3,
            dispatch_receipt_id='test-receipt',worker_id='test-worker',
            profile=deepcopy(next(p for p in self.request['model_profiles'] if p['stage']=='PLANNER')),
            returned_model_id='gemini-3.5-flash-001',response_sha256=digest('response'),finish_reason='STOP',
            safety_outcome='PASSED',safe_error_code=None,usage_status='NOT_REPORTED',cost_status='NOT_ESTIMATED',
            started_at=TIME+timedelta(seconds=3),completed_at=TIME+timedelta(seconds=4))
        self.observed=TIME+timedelta(seconds=8)
        self.output,_,_=output_context()
        for key in ('schema_version','workspace_id','conversation_id','owner_id','job_id','request_id',
                    'trace_id','canonical_sku','policy_version'):
            self.output[key]=self.job[key]
        self.output.update(evidence_snapshot_id=self.evidence_plan.evidence_snapshot_id,
            model_profiles=deepcopy(self.request['model_profiles']),created_at=TIME+timedelta(seconds=7))
        self.events=self._events()
        self.verifications=tuple(ArtifactVerification(**{key:artifact[key] for key in
            ('artifact_id','gcs_uri','gcs_generation','sha256','content_type','size_bytes')},verified_at=self.observed)
            for artifact in self.output['artifacts'])

    def _events(self):
        times=(1,2,2,3,3,4,4,8);stages=('RETRIEVING_EVIDENCE','PLANNING','RENDERING','VERIFYING')
        rows=[]
        for index,stage in enumerate(stages):
            start=TIME+timedelta(seconds=times[index*2]);finish=TIME+timedelta(seconds=times[index*2+1])
            for offset,(outcome,stamp) in enumerate((('STARTED',start),('SUCCEEDED',finish)),1):
                sequence=index*2+offset
                row={key:self.attempt[key] for key in ('schema_version','workspace_id','conversation_id','owner_id',
                    'job_id','request_id','attempt_id','trace_id')}
                row.update(event_id=f'test-event-{sequence}',sequence=sequence,stage=stage,outcome=outcome,
                    duration_ms=None if outcome=='STARTED' else int((finish-start).total_seconds()*1000),
                    safe_error_code=None,policy_version=self.job['policy_version'],occurred_at=stamp)
                rows.append(row)
        return tuple(rows)

    def prepare(self,**changes):
        values=dict(workspace=self.workspace,membership=self.membership,conversation=self.conversation,
            request=self.request,job=self.job,attempt=self.attempt,invocation=self.invocation,
            evidence_plan=self.evidence_plan,stage_events=self.events,output=self.output,
            artifact_verifications=self.verifications,observed_at=self.observed)
        return prepare_workflow_output(**(values|changes))

    @staticmethod
    def result(plan,state='APPLIED'):
        return dict(state=state,output_row=plan.output_json,job_row=plan.job_json,
            attempt_row=plan.attempt_json,event_rows=list(plan.event_rows_json),
            audit_rows=list(plan.audit_rows_json),artifact_verification_sha256=plan.artifact_verification_sha256)

    def test_builds_terminal_success_output_events_and_audits(self):
        plan=self.prepare();job=decode_row(plan.job_json,'jobs');attempt=decode_row(plan.attempt_json,'job_attempts')
        self.assertEqual((job['state'],job['output_id'],job['revision']),('SUCCEEDED','test-output',3))
        self.assertEqual((attempt['state'],attempt['revision']),('SUCCEEDED',3))
        self.assertEqual(len(plan.event_rows_json),7);self.assertEqual(len(plan.audit_rows_json),10)
        actions=[decode_row(value,'audit_events')['action'] for value in plan.audit_rows_json]
        self.assertEqual(actions[:3],['APPEND_OUTPUT','UPDATE_JOB','UPDATE_ATTEMPT'])
        self.assertEqual(actions[3:],['APPEND_STAGE_EVENT']*7)

    def test_plan_is_deterministic_bounded_parameterized_and_delete_free(self):
        first=self.prepare();second=self.prepare();self.assertEqual(first,second)
        self.assertTrue(first.native_job_id.startswith('workflow_output_v1_'))
        config=first.config();params={p.name:(p.value if hasattr(p,'value') else p.values) for p in config.query_parameters}
        self.assertFalse(config.use_query_cache);self.assertFalse(config.create_session)
        self.assertEqual(params['artifact_verification_sha256'],first.artifact_verification_sha256)
        self.assertEqual(params['new_event_row_count'],7);self.assertEqual(params['audit_row_count'],10)
        for value in ('WORKFLOW_OUTPUT_IDEMPOTENCY_CONFLICT','WORKFLOW_OUTPUT_EVIDENCE_CONFLICT',
                      'WORKFLOW_OUTPUT_FENCE_CONFLICT','ROLLBACK TRANSACTION'):
            self.assertIn(value,first.sql)
        self.assertNotIn('__',first.sql);self.assertNotIn('DELETE ',first.sql)

    def test_exact_applied_and_reused_results_only(self):
        plan=self.prepare()
        self.assertEqual(validate_workflow_output_result(plan,self.result(plan)),'APPLIED')
        self.assertEqual(validate_workflow_output_result(plan,self.result(plan,'REUSED')),'REUSED')
        for change in (dict(state='UNKNOWN'),dict(artifact_verification_sha256='0'*64),
                       dict(event_rows=[]),dict(extra='x')):
            with self.subTest(change=change),self.assertRaises(ContractError):
                validate_workflow_output_result(plan,self.result(plan)|change)

    def test_missing_stale_or_mismatched_artifact_verification_fails_closed(self):
        artifact=self.output['artifacts'][0];values={key:artifact[key] for key in
            ('artifact_id','gcs_uri','gcs_generation','sha256','content_type','size_bytes')}
        cases=((),(ArtifactVerification(**(values|{'gcs_generation':'999'}),verified_at=self.observed),),
            (ArtifactVerification(**values,verified_at=self.observed-timedelta(seconds=31)),))
        for verifications in cases:
            with self.subTest(verifications=verifications),self.assertRaises(ContractError):
                self.prepare(artifact_verifications=verifications)

    def test_access_invocation_evidence_and_event_conflicts_fail_closed(self):
        bad_output=deepcopy(self.output);bad_output['brief']['model']['response_sha256']=digest('other')
        cases=(dict(membership=self.membership|{'status':'REVOKED'}),
            dict(invocation=self.invocation|{'state':'FAILED','safe_error_code':'RESPONSE_INVALID'}),
            dict(output=bad_output),dict(stage_events=self.events[:-1]),
            dict(job=self.job|{'active_attempt_id':'other'}))
        for values in cases:
            with self.subTest(values=values),self.assertRaises((ContractError,PermissionError)):
                self.prepare(**values)

    def test_inputs_are_immutable_and_no_cloud_or_model_clients_are_created(self):
        before=deepcopy((self.workspace,self.membership,self.conversation,self.request,self.job,self.attempt,
            self.invocation,self.evidence_plan,self.events,self.output,self.verifications))
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('no bq client')), \
             patch('google.cloud.storage.Client',side_effect=AssertionError('no gcs client')), \
             patch('google.genai.Client',side_effect=AssertionError('no model client')):
            self.prepare()
        self.assertEqual(before,(self.workspace,self.membership,self.conversation,self.request,self.job,self.attempt,
            self.invocation,self.evidence_plan,self.events,self.output,self.verifications))


if __name__=='__main__':unittest.main()
