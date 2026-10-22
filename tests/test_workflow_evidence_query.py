"""Offline evidence-publication transaction tests; no cloud submission."""
from copy import deepcopy
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

from src.application.evidence_bundle import PlanningEvidenceBundle
from src.application.identity import ISSUER,principal_for_subject
from src.application.workflow_contract import ContractError,JobState,request_fingerprint,transition_job
from src.application.workflow_evidence_query import prepare_workflow_evidence,validate_workflow_evidence_result
from src.application.workflow_schema import SCHEMA_VERSION,TABLES
from tests.test_evidence_contract import SKU,SNAPSHOT,complete_bundle
from tests.test_workflow_contract import CONVERSATION,TIME,job as job_fixture,request as request_fixture


class WorkflowEvidenceQueryTests(unittest.TestCase):
    def setUp(self):
        subject='accounts.google.com:workflow-evidence-user';principal=principal_for_subject(ISSUER,subject)
        self.request=request_fixture();self.request.update(owner_id=principal.principal_id,
            principal_id=principal.principal_id,trace_id='workflow-evidence-trace')
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
        self.bundle=PlanningEvidenceBundle('test-evidence-snapshot',SNAPSHOT,SKU,tuple(rows))
        self.workspace=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',status='ACTIVE',revision=9,
            created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.membership=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',principal_id=principal.principal_id,
            identity_issuer=ISSUER,identity_subject=subject,roles=['CREATOR'],status='ACTIVE',revision=4,
            assignment_origin='ADMIN',updated_by=principal.principal_id,
            created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.conversation=dict(schema_version=SCHEMA_VERSION,workspace_id='test-workspace',
            conversation_id='test-conversation',owner_id=principal.principal_id,visibility='PRIVATE',status='ACTIVE',
            access_revision=5,created_at=TIME-timedelta(days=1),updated_at=TIME)
        self.observed=TIME+timedelta(seconds=2)

    def prepare(self,**changes):
        values=dict(workspace=self.workspace,membership=self.membership,conversation=self.conversation,
            request=self.request,job=self.job,attempt=self.attempt,bundle=self.bundle,
            worker_id='test-worker',observed_at=self.observed)
        return prepare_workflow_evidence(**(values|changes))

    @staticmethod
    def result(plan,state='APPLIED'):
        return dict(state=state,evidence_snapshot_id=plan.evidence_snapshot_id,
            evidence_row_count=len(plan.evidence_rows_json),bundle_sha256=plan.bundle_sha256,
            audit_row=plan.audit_json)

    def test_builds_complete_sorted_bundle_and_append_audit(self):
        plan=self.prepare()
        self.assertEqual(len(plan.evidence_rows_json),len(self.bundle.rows))
        refs=[json.loads(row)['evidence_ref_id'] for row in plan.evidence_rows_json]
        self.assertEqual(refs,sorted(refs))
        audit=json.loads(plan.audit_json)
        self.assertEqual((audit['action'],audit['resource_id'],audit['mutation_outcome']),
            ('APPEND_EVIDENCE','test-evidence-snapshot','APPLIED'))

    def test_plan_is_deterministic_bounded_and_parameterized(self):
        first=self.prepare();second=self.prepare();self.assertEqual(first,second)
        self.assertTrue(first.native_job_id.startswith('workflow_evidence_v1_'))
        config=first.config();params={parameter.name:getattr(parameter,'value',None)
            if hasattr(parameter,'value') else parameter.values for parameter in config.query_parameters}
        self.assertFalse(config.use_query_cache);self.assertFalse(config.create_session)
        self.assertEqual(params['evidence_row_count'],len(self.bundle.rows))
        self.assertEqual(params['evidence_rows'],list(first.evidence_rows_json))
        self.assertNotIn(self.request['principal_id'],first.sql)
        for text in ('BEGIN TRANSACTION','ROLLBACK TRANSACTION','WORKFLOW_EVIDENCE_IDEMPOTENCY_CONFLICT',
            'WORKFLOW_EVIDENCE_FENCE_CONFLICT','candidate_evidence','ci_application.evidence_snapshots'):
            self.assertIn(text,first.sql)
        self.assertNotIn('__',first.sql);self.assertNotIn('DELETE ',first.sql);self.assertNotIn('model_invocations',first.sql)

    def test_exact_applied_and_reused_results_only(self):
        plan=self.prepare()
        self.assertEqual(validate_workflow_evidence_result(plan,self.result(plan)),'APPLIED')
        self.assertEqual(validate_workflow_evidence_result(plan,self.result(plan,'REUSED')),'REUSED')
        for change in (dict(state='UNKNOWN'),dict(evidence_row_count=1),dict(bundle_sha256='0'*64),dict(extra='x')):
            with self.subTest(change=change),self.assertRaises(ContractError):
                validate_workflow_evidence_result(plan,self.result(plan)|change)

    def test_partial_bundle_stale_access_wrong_parent_and_worker_fail_closed(self):
        partial=PlanningEvidenceBundle(self.bundle.evidence_snapshot_id,self.bundle.knowledge_snapshot_id,
            self.bundle.canonical_sku,self.bundle.rows[:-1])
        cases=(dict(bundle=partial),dict(membership=self.membership|{'status':'REVOKED'}),
            dict(conversation=self.conversation|{'owner_id':'other'}),
            dict(job=self.job|{'active_attempt_id':'other'}),dict(worker_id='other-worker'),
            dict(observed_at=self.observed+timedelta(seconds=31)),
            dict(observed_at=self.observed.replace(tzinfo=None)))
        for values in cases:
            with self.subTest(values=values),self.assertRaises((ContractError,PermissionError)):
                self.prepare(**values)

    def test_inputs_are_immutable_and_no_cloud_clients_are_created(self):
        before=deepcopy((self.workspace,self.membership,self.conversation,self.request,
            self.job,self.attempt,self.bundle.rows))
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('no client')):
            self.prepare()
        self.assertEqual((self.workspace,self.membership,self.conversation,self.request,
            self.job,self.attempt,self.bundle.rows),before)


if __name__=='__main__':unittest.main()
