"""Offline PLANNED invocation creation tests; no model or cloud call."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.evidence_bundle import PlanningEvidenceBundle
from src.application.identity import ISSUER,principal_for_subject
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import ContractError,JobState,request_fingerprint,transition_job
from src.application.workflow_evidence_query import prepare_workflow_evidence
from src.application.workflow_invocation_query import prepare_workflow_invocation,validate_workflow_invocation_result
from src.application.workflow_schema import SCHEMA_VERSION,TABLES
from tests.test_evidence_contract import SKU,SNAPSHOT,complete_bundle
from tests.test_workflow_contract import CONVERSATION,TIME,job as job_fixture,request as request_fixture


class WorkflowInvocationQueryTests(unittest.TestCase):
    def setUp(self):
        subject='accounts.google.com:workflow-invocation-user';principal=principal_for_subject(ISSUER,subject)
        self.request=request_fixture();self.request.update(owner_id=principal.principal_id,
            principal_id=principal.principal_id,trace_id='workflow-invocation-trace')
        self.request['request_sha256']=request_fingerprint(self.request)
        resource=type(CONVERSATION)(self.request['conversation_id'],self.request['workspace_id'],principal.principal_id)
        self.job=transition_job(job_fixture(self.request),request=self.request,conversation=resource,
            target=JobState.RUNNING,expected_revision=1,attempt_id='test-attempt',now=TIME+timedelta(seconds=1))
        self.attempt=dict.fromkeys(field.name for field in TABLES['job_attempts'].schema)
        for key in ('schema_version','workspace_id','conversation_id','owner_id','job_id','request_id','trace_id'):
            self.attempt[key]=self.job[key]
        self.attempt.update(attempt_id='test-attempt',attempt_number=1,state='RUNNING',revision=1,
            dispatch_receipt_id=None,worker_id='test-worker',safe_error_code=None,
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
        self.workspace=base_workspace|{'revision':10,'updated_at':TIME+timedelta(seconds=2)}
        self.observed=TIME+timedelta(seconds=3)

    def prepare(self,**changes):
        values=dict(workspace=self.workspace,membership=self.membership,conversation=self.conversation,
            request=self.request,job=self.job,attempt=self.attempt,evidence_plan=self.evidence_plan,
            invocation_id='test-invocation',worker_id='test-worker',observed_at=self.observed)
        return prepare_workflow_invocation(**(values|changes))

    @staticmethod
    def result(plan,state='APPLIED'):
        return dict(state=state,invocation_row=plan.invocation_json,audit_row=plan.audit_json)

    def test_builds_planned_invocation_from_pinned_profile_and_audit(self):
        plan=self.prepare();row=decode_row(plan.invocation_json,'model_invocations')
        self.assertEqual((row['stage'],row['state'],row['revision'],row['call_number']),('PLANNER','PLANNED',1,1))
        self.assertEqual(row['profile'],next(profile for profile in self.request['model_profiles'] if profile['stage']=='PLANNER'))
        self.assertIsNone(row['dispatch_receipt_id']);self.assertEqual(row['usage_status'],'NOT_REPORTED')
        audit=decode_row(plan.audit_json,'audit_events')
        self.assertEqual((audit['action'],audit['mutation_outcome'],audit['new_revision']),
            ('CREATE_MODEL_INVOCATION','APPLIED',1))

    def test_plan_is_deterministic_bounded_and_contains_no_provider_call(self):
        first=self.prepare();second=self.prepare();self.assertEqual(first,second)
        self.assertTrue(first.native_job_id.startswith('workflow_invocation_v1_'))
        config=first.config();params={p.name:(p.value if hasattr(p,'value') else p.values) for p in config.query_parameters}
        self.assertFalse(config.use_query_cache);self.assertFalse(config.create_session)
        self.assertEqual(params['invocation_row'],first.invocation_json)
        self.assertEqual(params['evidence_snapshot_id'],self.evidence_plan.evidence_snapshot_id)
        self.assertNotIn(self.request['principal_id'],first.sql)
        for text in ('WORKFLOW_INVOCATION_EVIDENCE_CHANGED','WORKFLOW_INVOCATION_STAGE_DUPLICATE',
            'WORKFLOW_INVOCATION_IDEMPOTENCY_CONFLICT','ROLLBACK TRANSACTION','model_invocations'):
            self.assertIn(text,first.sql)
        self.assertNotIn('__',first.sql);self.assertNotIn('DELETE ',first.sql)

    def test_exact_applied_and_reused_results_only(self):
        plan=self.prepare()
        self.assertEqual(validate_workflow_invocation_result(plan,self.result(plan)),'APPLIED')
        self.assertEqual(validate_workflow_invocation_result(plan,self.result(plan,'REUSED')),'REUSED')
        for change in (dict(state='UNKNOWN'),dict(invocation_row=plan.audit_json),dict(extra='x')):
            with self.subTest(change=change),self.assertRaises(ContractError):
                validate_workflow_invocation_result(plan,self.result(plan)|change)

    def test_revocation_stale_evidence_wrong_worker_parent_and_profile_fail_closed(self):
        changed_request=deepcopy(self.request);changed_request['model_profiles'][1]['model_id']='other'
        changed_request['request_sha256']=request_fingerprint(changed_request)
        cases=(dict(membership=self.membership|{'status':'REVOKED'}),dict(worker_id='other-worker'),
            dict(job=self.job|{'active_attempt_id':'other'}),
            dict(observed_at=self.observed+timedelta(seconds=31)),dict(request=changed_request))
        for values in cases:
            with self.subTest(values=values),self.assertRaises((ContractError,PermissionError)):
                self.prepare(**values)

    def test_inputs_are_immutable_and_no_cloud_or_model_clients_are_created(self):
        before=deepcopy((self.workspace,self.membership,self.conversation,self.request,self.job,
            self.attempt,self.evidence_plan))
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('no bq client')), \
             patch('google.genai.Client',side_effect=AssertionError('no model client')):
            self.prepare()
        self.assertEqual((self.workspace,self.membership,self.conversation,self.request,self.job,
            self.attempt,self.evidence_plan),before)


if __name__=='__main__':unittest.main()
