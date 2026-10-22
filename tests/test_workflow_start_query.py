"""Offline atomic workflow-start query contract tests; no BigQuery submission."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.access_capture import capture_access
from src.application.access_policy import Action,Principal,authorize
from src.application.audit_contract import (
    AuditTarget,MutationKind,MutationObservation,MutationOutcome,ResourceType,
    WorkflowAction,WorkflowObservation,
)
from src.application.audit_producer import produce_access_audit,produce_service_audit
from src.application.identity import ISSUER,principal_for_subject
from src.application.membership_contract import validate_membership
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_contract import ContractError,request_fingerprint,retry_key_hash
from src.application.workflow_start_query import prepare_workflow_start,validate_workflow_start_result
from src.application.workflow_schema import SCHEMA_VERSION
from tests.test_workflow_contract import TIME,job as job_fixture,request as request_fixture


class WorkflowStartQueryTests(unittest.TestCase):
    def setUp(self):
        self.subject='accounts.google.com:workflow-start-user'
        self.principal=principal_for_subject(ISSUER,self.subject);principal_id=self.principal.principal_id
        self.workspace=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',status='ACTIVE',
            revision=7,created_at=TIME-timedelta(days=1),updated_at=TIME-timedelta(seconds=1))
        self.membership=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            principal_id=principal_id,identity_issuer=ISSUER,identity_subject=self.subject,roles=['CREATOR'],
            status='ACTIVE',revision=3,assignment_origin='ADMIN',updated_by=principal_id,
            created_at=TIME-timedelta(days=1),updated_at=TIME-timedelta(seconds=1))
        self.conversation=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            conversation_id='test-conversation',owner_id=principal_id,visibility='PRIVATE',status='ACTIVE',
            access_revision=4,created_at=TIME-timedelta(days=1),updated_at=TIME-timedelta(seconds=1))
        self.request=request_fixture();self.request.update(owner_id=principal_id,principal_id=principal_id,
            created_at=TIME,updated_at=TIME,trace_id='workflow-start-trace')
        self.request['idempotency_key_sha256']=retry_key_hash(workspace_id='test-workspace',
            principal_id=principal_id,conversation_id='test-conversation',operation='GENERATE',retry_key='retry-1')
        self.request['request_sha256']=request_fingerprint(self.request)
        self.job=job_fixture(self.request);self.job.update(created_at=TIME,updated_at=TIME)
        member=validate_membership(self.membership,workspace_id='test-workspace',principal_id=principal_id)
        decision=authorize(self.principal,member,Action.GENERATE_DRAFT,workspace_id='test-workspace')
        request_target=AuditTarget(ResourceType.REQUEST,self.request['request_id'],'test-workspace',
            conversation_id='test-conversation',owner_id=principal_id,request_id=self.request['request_id'])
        access=capture_access(decision,target=request_target,trace_id=self.request['trace_id'],observed_at=TIME)
        self.access_audit=produce_access_audit(access,recorded_at=TIME)
        self.request_audit=produce_service_audit(WorkflowObservation('workflow-worker','test-workspace',
            WorkflowAction.CREATE_REQUEST),target=request_target,
            mutation=MutationObservation(MutationKind.CREATE,MutationOutcome.APPLIED,1),
            trace_id=self.request['trace_id'],observed_at=TIME,recorded_at=TIME)
        job_target=AuditTarget(ResourceType.JOB,self.job['job_id'],'test-workspace',
            conversation_id='test-conversation',owner_id=principal_id,request_id=self.request['request_id'],
            job_id=self.job['job_id'])
        self.job_audit=produce_service_audit(WorkflowObservation('workflow-worker','test-workspace',
            WorkflowAction.CREATE_JOB),target=job_target,
            mutation=MutationObservation(MutationKind.CREATE,MutationOutcome.APPLIED,1),
            trace_id=self.request['trace_id'],observed_at=TIME,recorded_at=TIME)

    def prepare(self,**changes):
        values=dict(workspace=self.workspace,membership=self.membership,conversation=self.conversation,
            request=self.request,job=self.job,request_audit=self.request_audit,
            job_audit=self.job_audit,access_audit=self.access_audit,observed_at=TIME)
        return prepare_workflow_start(**(values|changes))

    def result(self,plan,state='APPLIED'):
        return dict(state=state,request_row=plan.request_json,job_row=plan.workflow_job_json,
            access_audit=plan.access_audit_json,request_audit=plan.request_audit_json,
            job_audit=plan.job_audit_json)

    def test_builds_deterministic_bounded_parameterized_transaction(self):
        first=self.prepare();second=self.prepare()
        self.assertEqual(first,second)
        self.assertTrue(first.job_id.startswith('workflow_start_v1_'))
        config=first.config();params={parameter.name:parameter.value for parameter in config.query_parameters}
        self.assertFalse(config.use_query_cache);self.assertFalse(config.create_session)
        self.assertEqual(params['workspace_revision'],7);self.assertEqual(params['membership_revision'],3)
        self.assertEqual(params['conversation_revision'],4)
        self.assertEqual(params['candidate'],first.request_json)
        self.assertEqual(params['attempt'],first.workflow_job_json)
        self.assertNotIn(self.principal.principal_id,first.sql)
        for text in ('BEGIN TRANSACTION','ROLLBACK TRANSACTION','WORKFLOW_START_IDEMPOTENCY_CONFLICT',
                     'WORKFLOW_START_INTENT_CONFLICT',
                     'UPDATE `project-661d501a-e709-4276-a26.ci_application.workspaces`',
                     'INSERT INTO `project-661d501a-e709-4276-a26.ci_application.requests`',
                     'INSERT INTO `project-661d501a-e709-4276-a26.ci_application.jobs`'):
            self.assertIn(text,first.sql)
        self.assertNotIn('__',first.sql)
        self.assertNotIn('model_invocations',first.sql)

    def test_applied_and_reused_results_require_exact_rows(self):
        plan=self.prepare()
        self.assertEqual(validate_workflow_start_result(plan,self.result(plan)),'APPLIED')
        self.assertEqual(validate_workflow_start_result(plan,self.result(plan,'REUSED')),'REUSED')
        for change in (dict(state='UNKNOWN'),dict(request_row=self.result(plan)['job_row']),dict(extra='x')):
            with self.subTest(change=change),self.assertRaises(ContractError):
                validate_workflow_start_result(plan,self.result(plan)|change)

    def test_access_parent_and_audit_mismatches_fail_closed(self):
        cases=(dict(membership=self.membership|{'status':'REVOKED'}),
            dict(conversation=self.conversation|{'owner_id':'other'}),
            dict(job=self.job|{'state':'RUNNING'}),dict(observed_at=TIME+timedelta(seconds=1)))
        for values in cases:
            with self.subTest(values=values),self.assertRaises((ContractError,PermissionError)):
                self.prepare(**values)
        with self.assertRaises(ContractError):self.prepare(request_audit=self.job_audit)

    def test_config_limits_and_candidate_inputs_are_not_mutated(self):
        before=deepcopy((self.workspace,self.membership,self.conversation,self.request,self.job))
        plan=self.prepare(settings=SharingQuerySettings(maximum_bytes_billed=64*1024*1024,
            job_timeout_ms=20000,statement_timeout_ms=10000))
        config=plan.config()
        self.assertEqual(config.maximum_bytes_billed,64*1024*1024)
        self.assertEqual(int(config.job_timeout_ms),20000)
        self.assertEqual((self.workspace,self.membership,self.conversation,self.request,self.job),before)

    def test_contract_never_creates_client_or_performs_cloud_io(self):
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('no BigQuery client')):
            self.prepare()


if __name__=='__main__':
    unittest.main()
