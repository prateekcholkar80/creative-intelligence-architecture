"""Fresh authorization, original-operation recovery and private receipt checks."""
from datetime import datetime,timezone,timedelta
from dataclasses import replace
from types import SimpleNamespace
import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.application.campaign_question_writer import ProtectedQuestionWriter
from src.application.identity import IapIdentityAdapter,IapSettings,VerifiedIdentity,ISSUER,principal_for_subject
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.conversation_contract import validate_conversation
from src.validation.control_fixtures import SUBJECT,ACTOR
from src.application.campaign_question_context import NAMES,QuestionContextRead,canonical_records
from src.application.campaign_evidence_conversation import BoundGapRead
from src.application.workflow_codec import encode_row
from src.validation.question_child_fixtures import child_plan
from tests import test_question_child_integration as fixtures
from tests.test_management_service import client
from tests.test_identity import AUDIENCE


class QuestionWriterTests(TestCase):
    def setUp(self):
        f=fixtures.QuestionChildProbeTests();rows,extra,history=f.capture()
        self.rows,self.extra,self.history=rows,extra,history
        self.parent=rows['requests'][0]['request_id']
        retry='question-parent-v1-'+hashlib.sha256(self.parent.encode()).hexdigest()
        self.plan=child_plan(rows,extra,history,retry)
        self.args=dict(workspace_id='question-offline',conversation_id=rows['conversations'][0]['conversation_id'],parent_request_id=self.parent)
        c=client();self.addCleanup(c.close)
        verifier=IapIdentityAdapter(IapSettings(AUDIENCE));epoch=int(time.time())
        verifier.authenticate=Mock(return_value=VerifiedIdentity(principal_for_subject(ISSUER,SUBJECT),ISSUER,SUBJECT,AUDIENCE,epoch-1,epoch+300))
        profile=json.loads({p.name:p.value for p in self.plan.config().query_parameters}['invocation_candidate'])['profile']
        self.writer=ProtectedQuestionWriter(client=c,verifier=verifier,access_reader=Mock(),policy_request=Mock(),
            profile=profile,worker_id='fixture-worker',enabled=True)
        conv=rows['conversations'][0]
        self.snapshot=(datetime.now(timezone.utc),rows['workspaces'][0],rows['memberships'][0],conv,
            validate_conversation(conv,workspace_id='question-offline',conversation_id=conv['conversation_id']),None)
        self.writer.reader._snapshot=Mock(return_value=self.snapshot)
        self.writer._prepare=Mock(return_value=self.plan)
        self.writer.io=Mock();self.writer.io.original_job.side_effect=NotFound('absent')
        self.writer.io.submit_once.return_value=self.result()
        self.writer.io.recover.return_value=self.result()
        native={name:[] for name in NAMES}
        for name,values in rows.items():native[name]=[encode_row(v,name) for v in values]
        native.update({name:[json.dumps(v) for v in values] for name,values in extra.items()})
        after={name:list(values) for name,values in native.items()}
        p={p.name:p.value for p in self.plan.config().query_parameters};req=json.loads(p['request_candidate'])
        after['workspaces']=[p['workspace_candidate']]
        for name,key in [('requests','request_candidate'),('model_invocations','invocation_candidate'),
                ('campaign_transcript_bodies','transcript_candidate'),('audit_events','request_audit'),('audit_events','invocation_audit')]:
            after[name].append(p[key])
        after['campaign_request_authorities'].append(json.dumps(dict(workspace_id=req['workspace_id'],
            conversation_id=req['conversation_id'],request_id=req['request_id'],principal_id=req['principal_id'],
            payload_canonical_json=p['authority_payload'],payload_sha256=p['authority_digest'],recorded_at=req['created_at'])))
        now=datetime.now(timezone.utc)
        def context(records,read_id):return QuestionContextRead(req['workspace_id'],req['conversation_id'],ACTOR,
            canonical_records(records),read_id,now,now+timedelta(seconds=60))
        gaps=BoundGapRead(SimpleNamespace(sha256='a'*64),'b'*64,ACTOR,now+timedelta(seconds=60),False,'c'*64)
        self.writer._prepared[self.plan.job_id]=(context(native,'before'),gaps)
        self.after_context=context(after,'after')
        self.context_reader=patch('src.application.campaign_question_writer.read_question_context',return_value=self.after_context)
        self.context_reader.start();self.addCleanup(self.context_reader.stop)
        self.owned=patch('src.application.campaign_question_writer.require_owned_worker');self.owned.start();self.addCleanup(self.owned.stop)

    def result(self,status=Status.COMMITTED):
        p={p.name:p.value for p in self.plan.config().query_parameters}
        row=dict(state='APPLIED',request_row=p['request_candidate'],invocation_row=p['invocation_candidate'],
            transcript_row=p['transcript_candidate'],authority_sha256=p['authority_digest'])
        return ControlOutcome(self.plan.job_id,status,(row,) if status is Status.COMMITTED else ())

    def test_committed_retry_recovers_original_plan_without_resubmission(self):
        first=self.writer.submit({},**self.args);second=self.writer.submit({},**self.args)
        self.assertEqual(first,second);self.assertFalse(first.release_allowed)
        self.writer._prepare.assert_called_once();self.writer.io.submit_once.assert_called_once_with(self.plan)
        self.writer.io.recover.assert_called_once_with(self.plan)

    def test_unknown_outcome_never_mints_a_second_child(self):
        self.writer.io.submit_once.return_value=self.result(Status.UNKNOWN)
        self.assertEqual(self.writer.submit({},**self.args).native_status,'UNKNOWN')
        self.writer.submit({},**self.args)
        self.writer._prepare.assert_called_once();self.writer.io.submit_once.assert_called_once()

    def test_cold_existing_operation_requires_original_plan_and_never_writes(self):
        self.writer.io.original_job.side_effect=None
        with self.assertRaisesRegex(ValueError,'QUESTION_ORIGINAL_PLAN_RECOVERY_REQUIRED'):self.writer.submit({},**self.args)
        self.writer._prepare.assert_not_called();self.writer.io.submit_once.assert_not_called()

    def test_revocation_before_or_after_write_withholds_receipt(self):
        for sequence in ([ValueError('revoked')],[self.snapshot,ValueError('revoked')]):
            self.writer.reader._snapshot.side_effect=sequence
            with self.assertRaisesRegex(ValueError,'revoked'):self.writer.submit({},**self.args)

    def test_corrupt_receipt_withholds_result(self):
        result=self.result();result.rows[0]['authority_sha256']='f'*64
        self.writer.io.submit_once.return_value=result
        with self.assertRaisesRegex(ValueError,'QUESTION_WRITER_RECEIPT_UNVERIFIED'):self.writer.submit({},**self.args)

    def test_disabled_writer_does_not_authenticate_or_write(self):
        self.writer.enabled=False
        with self.assertRaisesRegex(ValueError,'QUESTION_WRITER_DISABLED'):self.writer.submit({},**self.args)
        self.writer.verifier.authenticate.assert_not_called();self.writer.io.submit_once.assert_not_called()

    def test_preparation_rechecks_parent_native_capture_and_access(self):
        from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
        from src.application.campaign_acceptance_contract import _hash
        from src.application.campaign_evidence_gaps import canonical
        now=datetime.now(timezone.utc)
        p={p.name:p.value for p in self.plan.config().query_parameters}
        content=dict(authority=json.loads(p['question_parent_authority']))
        first=ExecutionAuthorityRead(canonical(content),_hash(content),'first-read',now+timedelta(seconds=1),now+timedelta(seconds=60))
        final=replace(first,native_job_id='second-read',observed_at=now+timedelta(seconds=4))
        history=replace(self.history,observed_at=now+timedelta(seconds=2))
        before=(now,)+self.snapshot[1:];after=(now+timedelta(seconds=5),)+self.snapshot[1:]
        self.writer.clock=lambda:now+timedelta(seconds=5)
        actor=self.writer.verifier.authenticate({})
        self.writer.reader._snapshot.side_effect=[before,after]
        with patch('src.application.campaign_question_writer.read_execution_authority',side_effect=[first,final]), \
            patch('src.application.campaign_question_writer.read_history',return_value=history), \
            patch('src.application.campaign_question_writer.read_bound_gaps',return_value=SimpleNamespace(valid_until=now+timedelta(seconds=60))), \
            patch('src.application.campaign_question_writer.prepare_question_submission',return_value=self.plan) as build:
            actual=ProtectedQuestionWriter._prepare(self.writer,actor,self.args['workspace_id'],self.args['conversation_id'],self.parent)
            self.assertIs(actual,self.plan)
            self.assertIs(build.call_args.kwargs['history'],history)
        self.writer.reader._snapshot.side_effect=[before,after]
        with patch('src.application.campaign_question_writer.read_execution_authority',side_effect=[first,replace(final,content_sha256='e'*64)]), \
            patch('src.application.campaign_question_writer.read_history',return_value=history), \
            patch('src.application.campaign_question_writer.read_bound_gaps',return_value=SimpleNamespace(valid_until=now+timedelta(seconds=60))), \
            patch('src.application.campaign_question_writer.prepare_question_submission') as build:
            with self.assertRaisesRegex(ValueError,'QUESTION_WRITER_CONTEXT_CHANGED'):
                ProtectedQuestionWriter._prepare(self.writer,actor,self.args['workspace_id'],self.args['conversation_id'],self.parent)
            build.assert_not_called()

    def test_handoff_is_bound_to_receipt_access_and_original_context(self):
        receipt=self.writer.submit({},**self.args)
        value=self.writer.admission_context({},receipt)
        self.assertIs(value.admitted,self.after_context)
        self.assertFalse(value.release_allowed)
        with self.assertRaisesRegex(ValueError,'QUESTION_ADMISSION_CONTEXT_CHANGED_OR_EXPIRED'):
            self.writer.admission_context({},replace(receipt,request_id='different-request'))
        self.writer.reader._snapshot.side_effect=ValueError('revoked')
        with self.assertRaisesRegex(ValueError,'revoked'):self.writer.admission_context({},receipt)

    def test_context_change_after_commit_withholds_handoff_without_resubmission(self):
        with patch('src.application.campaign_question_writer.verify_transition',side_effect=ValueError('context-changed')):
            with self.assertRaisesRegex(ValueError,'context-changed'):self.writer.submit({},**self.args)
        self.assertFalse(self.writer._contexts)
        self.writer.io.submit_once.assert_called_once()
