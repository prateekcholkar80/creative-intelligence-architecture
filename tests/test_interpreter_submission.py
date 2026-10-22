from copy import deepcopy
from datetime import datetime,timezone
import hashlib,json
from google.cloud import bigquery
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.interpreter_submission import prepare_interpreter_submission,ProtectedInterpreterSubmissionService
from src.application.conversation_contract import validate_conversation
from src.application.workflow_codec import decode_row
from src.application.campaign_transcript import SPEC,RETENTION_DAYS
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from tests.test_generation_admission import records
from tests.test_management_service import client,service

class InterpreterSubmissionTests(TestCase):
    def setUp(self):
        self.rows=records('INTERPRETER');r=self.rows
        self.resource=validate_conversation(r['conversations'],workspace_id=r['workspaces']['workspace_id'],
            conversation_id=r['conversations']['conversation_id'])
        self.inputs=dict(workspace=r['workspaces'],membership=r['memberships'],conversation=r['conversations'],
            resource=self.resource,principal_id=r['requests']['principal_id'],message='Create a campaign',retry_key='turn-1',
            profile=r['model_invocations']['profile'],observed_at=datetime.now(timezone.utc),worker_id='interpreter-submit')
    def plan(self,**changes):return prepare_interpreter_submission(**(self.inputs|changes))
    def test_plan_atomically_links_message_request_invocation_and_retained_body(self):
        p=self.plan();values={x.name:x.value for x in p.config().query_parameters}
        req=decode_row(values['request_candidate'],'requests');inv=decode_row(values['invocation_candidate'],'model_invocations')
        transcript=json.loads(values['transcript_candidate']);body=json.loads(transcript['batch_canonical_json'])
        self.assertEqual(req['request_status'],'ACCEPTED');self.assertEqual(inv['state'],'PLANNED')
        self.assertEqual(inv['request_id'],req['request_id']);self.assertIsNone(inv['job_id'])
        self.assertEqual(body[0]['text'],self.inputs['message'])
        self.assertEqual(body[0]['turn']['content_sha256'],hashlib.sha256(self.inputs['message'].encode()).hexdigest())
        self.assertEqual(RETENTION_DAYS,30);self.assertIn('BEGIN TRANSACTION',p.sql)
        self.assertNotIn('DELETE',p.sql);self.assertNotIn('.jobs`',p.sql)
    def test_same_key_conflicting_messages_share_slot_but_have_distinct_fingerprints(self):
        a=self.plan();b=self.plan(message='Different request')
        self.assertEqual(a.job_id,b.job_id)
        self.assertNotEqual(decode_row(a.candidate_json,'requests')['request_sha256'],decode_row(b.candidate_json,'requests')['request_sha256'])
    def test_scope_role_message_and_stage_fail_closed(self):
        for changes in (dict(message=''),dict(message='x'*2001),dict(principal_id='foreign'),
                dict(profile=dict(self.inputs['profile'],stage='PLANNER'))):
            with self.subTest(changes=changes),self.assertRaises(ValueError):self.plan(**changes)
        bad=deepcopy(self.inputs['membership']);bad['roles']=['VIEWER']
        with self.assertRaises(Exception):self.plan(membership=bad)
    def instance(self):
        c=client();self.addCleanup(c.close)
        instance=ProtectedInterpreterSubmissionService(client=c,verifier=service().verifier,profile=self.inputs['profile'])
        r=self.rows
        instance.reader._snapshot=Mock(return_value=(self.inputs['observed_at'],r['workspaces'],r['memberships'],r['conversations'],self.resource,None))
        instance.io.preflight=Mock()
        metadata=SPEC.table(c.project).to_api_repr()
        metadata.update(location="asia-south1",type="TABLE")
        c.get_table=Mock(return_value=bigquery.Table.from_api_repr(metadata))
        return instance
    def args(self):return dict(workspace_id=self.rows['workspaces']['workspace_id'],conversation_id=self.resource.resource_id,
        message=self.inputs['message'],retry_key='turn-1')
    def test_disabled_default_and_conflicting_retry_do_not_write(self):
        instance=self.instance()
        with self.assertRaises(ValueError):instance.submit({},**self.args())
        instance.enabled=True
        instance.io.submit_once=Mock(side_effect=lambda p:ControlOutcome(p.job_id,Status.UNKNOWN))
        result=instance.submit({},**self.args());self.assertFalse(result.release_allowed)
        with self.assertRaises(ValueError):instance.submit({},**(self.args()|dict(message='changed')))
        instance.io.submit_once.assert_called_once()
    def test_same_process_retry_retains_original_plan(self):
        instance=self.instance();instance.enabled=True;seen=[]
        def write(p):
            seen.append(p);values={x.name:x.value for x in p.config().query_parameters}
            return ControlOutcome(p.job_id,Status.COMMITTED,(dict(state='APPLIED',request_row=values['request_candidate'],
                invocation_row=values['invocation_candidate'],transcript_row=values['transcript_candidate']),),len(seen)==1)
        instance.io.submit_once=Mock(side_effect=write)
        first=instance.submit({},**self.args());second=instance.submit({},**self.args())
        self.assertEqual(first,second);self.assertIs(seen[0],seen[1])
        self.assertEqual(len(instance._plans),1)
    def test_corrupt_transcript_scope_withholds_receipt(self):
        instance=self.instance();instance.enabled=True
        def write(p):
            values={x.name:x.value for x in p.config().query_parameters}
            transcript=json.loads(values['transcript_candidate']);transcript['conversation_id']='foreign'
            return ControlOutcome(p.job_id,Status.COMMITTED,(dict(state='APPLIED',request_row=values['request_candidate'],
                invocation_row=values['invocation_candidate'],transcript_row=json.dumps(transcript)),),True)
        instance.io.submit_once=Mock(side_effect=write)
        with self.assertRaisesRegex(ValueError,'INTERPRETER_SUBMISSION_UNVERIFIED'):instance.submit({},**self.args())
    def test_postcommit_access_revocation_withholds_receipt(self):
        instance=self.instance();instance.enabled=True
        initial=instance.reader._snapshot.return_value
        instance.reader._snapshot.side_effect=[initial,ValueError('ACCESS_REVOKED')]
        instance.io.submit_once=Mock(side_effect=lambda p:ControlOutcome(p.job_id,Status.UNKNOWN))
        with self.assertRaisesRegex(ValueError,'ACCESS_REVOKED'):instance.submit({},**self.args())
        instance.io.submit_once.assert_called_once()
