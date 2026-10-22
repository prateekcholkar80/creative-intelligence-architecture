"""Offline worker-attempt transaction contract tests; no cloud submission."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.identity import ISSUER,principal_for_subject
from src.application.workflow_attempt_query import prepare_workflow_attempt,validate_workflow_attempt_result
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import ContractError,request_fingerprint,retry_key_hash
from src.application.workflow_schema import SCHEMA_VERSION
from tests.test_workflow_contract import TIME,job as job_fixture,request as request_fixture


class WorkflowAttemptQueryTests(unittest.TestCase):
    def setUp(self):
        subject='accounts.google.com:workflow-attempt-user';principal=principal_for_subject(ISSUER,subject)
        self.workspace=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',status='ACTIVE',
            revision=8,created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.membership=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            principal_id=principal.principal_id,identity_issuer=ISSUER,identity_subject=subject,
            roles=['CREATOR'],status='ACTIVE',revision=3,assignment_origin='ADMIN',
            updated_by=principal.principal_id,created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.conversation=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            conversation_id='test-conversation',owner_id=principal.principal_id,visibility='PRIVATE',
            status='ACTIVE',access_revision=4,created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.request=request_fixture();self.request.update(owner_id=principal.principal_id,
            principal_id=principal.principal_id,trace_id='workflow-attempt-trace')
        self.request['idempotency_key_sha256']=retry_key_hash(workspace_id='test-workspace',
            principal_id=principal.principal_id,conversation_id='test-conversation',operation='GENERATE',
            retry_key='attempt-retry')
        self.request['request_sha256']=request_fingerprint(self.request)
        self.job=job_fixture(self.request)
        self.observed=TIME+timedelta(seconds=1)

    def prepare(self,**changes):
        values=dict(workspace=self.workspace,membership=self.membership,conversation=self.conversation,
            request=self.request,job=self.job,attempt_id='attempt-1',event_id='event-1',
            worker_id='worker-1',observed_at=self.observed)
        return prepare_workflow_attempt(**(values|changes))

    def result(self,plan,state='APPLIED'):
        return dict(state=state,job_row=plan.job_json,attempt_row=plan.attempt_json,
            event_row=plan.event_json,job_audit=plan.job_audit_json,
            attempt_audit=plan.attempt_audit_json,event_audit=plan.event_audit_json)

    def test_builds_running_job_attempt_first_event_and_distinct_audits(self):
        plan=self.prepare();job=decode_row(plan.job_json,'jobs');attempt=decode_row(plan.attempt_json,'job_attempts')
        event=decode_row(plan.event_json,'stage_events')
        self.assertEqual((job['state'],job['revision'],job['attempt_count']),('RUNNING',2,1))
        self.assertEqual((job['active_attempt_id'],attempt['attempt_id'],attempt['worker_id']),
            ('attempt-1','attempt-1','worker-1'))
        self.assertEqual((event['sequence'],event['stage'],event['outcome']),(1,'RETRIEVING_EVIDENCE','STARTED'))
        audits=[decode_row(value,'audit_events') for value in
            (plan.job_audit_json,plan.attempt_audit_json,plan.event_audit_json)]
        self.assertEqual([row['action'] for row in audits],['UPDATE_JOB','CLAIM_ATTEMPT','APPEND_STAGE_EVENT'])
        self.assertEqual(len({row['audit_id'] for row in audits}),3)

    def test_plan_is_deterministic_bounded_and_parameterized(self):
        first=self.prepare();second=self.prepare();self.assertEqual(first,second)
        self.assertTrue(first.native_job_id.startswith('workflow_attempt_v1_'))
        config=first.config();params={parameter.name:parameter.value for parameter in config.query_parameters}
        self.assertFalse(config.use_query_cache);self.assertFalse(config.create_session)
        self.assertEqual(params['workspace_revision'],8);self.assertEqual(params['job_revision'],1)
        self.assertEqual(params['job_candidate'],first.job_json)
        self.assertNotIn(self.membership['principal_id'],first.sql)
        for text in ('BEGIN TRANSACTION','ROLLBACK TRANSACTION','WORKFLOW_CLAIM_IDEMPOTENCY_CONFLICT',
            'WORKFLOW_CLAIM_FENCE_CONFLICT','ci_application.job_attempts','ci_application.stage_events'):
            self.assertIn(text,first.sql)
        self.assertNotIn('__',first.sql);self.assertNotIn('model_invocations',first.sql)

    def test_exact_applied_and_reused_results_only(self):
        plan=self.prepare()
        self.assertEqual(validate_workflow_attempt_result(plan,self.result(plan)),'APPLIED')
        self.assertEqual(validate_workflow_attempt_result(plan,self.result(plan,'REUSED')),'REUSED')
        for change in (dict(state='UNKNOWN'),dict(attempt_row=plan.event_json),dict(extra='x')):
            with self.subTest(change=change),self.assertRaises(ContractError):
                validate_workflow_attempt_result(plan,self.result(plan)|change)

    def test_revocation_parent_changes_deadline_and_bad_worker_fail_closed(self):
        cases=(dict(membership=self.membership|{'status':'REVOKED'}),
            dict(conversation=self.conversation|{'owner_id':'other'}),
            dict(job=self.job|{'state':'RUNNING'}),
            dict(observed_at=self.job['deadline_at']),dict(worker_id='bad worker'))
        for values in cases:
            with self.subTest(values=values),self.assertRaises((ContractError,PermissionError)):
                self.prepare(**values)

    def test_inputs_are_immutable_and_no_cloud_clients_are_created(self):
        before=deepcopy((self.workspace,self.membership,self.conversation,self.request,self.job))
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('no client')):
            self.prepare()
        self.assertEqual((self.workspace,self.membership,self.conversation,self.request,self.job),before)


if __name__=='__main__':unittest.main()
