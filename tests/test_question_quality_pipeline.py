from datetime import datetime,timedelta,timezone
import json
from types import SimpleNamespace as NS
from unittest import TestCase
from unittest.mock import Mock,patch
from contextlib import ExitStack
from src.application.question_quality_pipeline import QuestionQualityPublisher
from src.application.campaign_quality_execution import QualityRequest

MODULE='src.application.question_quality_pipeline.'


class QuestionQualityPipelineTests(TestCase):
    def setUp(self):
        self.now=datetime.now(timezone.utc)
        self.publisher=object.__new__(QuestionQualityPublisher)
        for key in ('client','verifier','access_reader','runtime_reader','settings','evidence'):
            setattr(self.publisher,key,Mock())
        self.publisher.clock=lambda:self.now
        self.publisher.evidence._identity.return_value=(Mock(),Mock())
        self.receipt=NS(workspace_id='w',conversation_id='c',request_id='child',invocation_id='call')
        self.context=NS(initial_gaps=NS(plan=object(),valid_until=self.now+timedelta(seconds=25)))
        self.result=NS(outcome=NS(native_status=NS(value='COMMITTED')),
            draft=NS(response_text='observed raw JSON',binding_sha256='a'*64))
        self.content=dict(authority=dict(sku='CI-SKU-0021',country='US',channel='META'),
            request_record=dict(message_sha256='b'*64),transcript_sha256='c'*64,
            calls=[dict(invocation_id='call',profile={})])

    def patches(self,stack):
        stack.enter_context(patch(MODULE+'read_execution_authority',return_value=NS(content_canonical_json=json.dumps(self.content))))
        projector=stack.enter_context(patch(MODULE+'bind_question_output',return_value=(NS(text='Which terms apply?'),'pin')))
        quality=stack.enter_context(patch(MODULE+'run_authoritative_quality'))
        return projector,quality

    def test_observed_projection_and_current_sources_reach_private_staging(self):
        with ExitStack() as stack:
            _,quality=self.patches(stack)
            fact=object()
            def check(request,builder,**kwargs):
                self.assertIsInstance(request,QualityRequest)
                candidate,raw=builder(fact)
                self.assertEqual(candidate.text,'Which terms apply?')
                self.assertEqual(raw,self.result.draft.response_text)
                self.assertIs(kwargs['question_plan'],self.context.initial_gaps.plan)
                return NS(report=NS(checked_at=self.now))
            quality.side_effect=check
            self.publisher({},self.receipt,self.context,self.result)
        turn=self.publisher.evidence.stage_questions.call_args.args[0]
        self.assertEqual(turn.sources,(fact,))
        self.assertEqual(turn.expires_at,self.context.initial_gaps.valid_until)

    def test_failed_check_cannot_stage_turn(self):
        with ExitStack() as stack:
            _,quality=self.patches(stack);quality.side_effect=ValueError('changed context')
            with self.assertRaises(ValueError):self.publisher({},self.receipt,self.context,self.result)
        self.publisher.evidence.stage_questions.assert_not_called()

    def test_original_catalog_now_reaches_authoritative_check_without_claim_approval(self):
        self.content['authority']['sku']='CI-SKU-0001'
        with ExitStack() as stack:
            _,quality=self.patches(stack)
            quality.side_effect=ValueError('quality gate holds')
            with self.assertRaisesRegex(ValueError,'quality gate holds'):
                self.publisher({},self.receipt,self.context,self.result)
        self.assertEqual(quality.call_args.args[0].sku,'CI-SKU-0001')
        self.publisher.evidence.stage_questions.assert_not_called()

    def test_unsupported_catalog_remains_explicit_hold(self):
        self.content['authority']['sku']='CI-SKU-0024'
        with ExitStack() as stack:
            _,quality=self.patches(stack)
            with self.assertRaisesRegex(ValueError,'INVALID_QUALITY_REQUEST'):
                self.publisher({},self.receipt,self.context,self.result)
        quality.assert_not_called();self.publisher.evidence.stage_questions.assert_not_called()

    def test_unknown_commit_or_access_denial_cannot_read_and_stage(self):
        self.result.outcome.native_status.value='UNKNOWN'
        with patch(MODULE+'read_execution_authority') as read:
            with self.assertRaises(ValueError):self.publisher({},self.receipt,self.context,self.result)
        read.assert_not_called();self.publisher.evidence.stage_questions.assert_not_called()
        self.result.outcome.native_status.value='COMMITTED'
        self.publisher.evidence._identity.side_effect=ValueError('revoked')
        with patch(MODULE+'read_execution_authority') as read:
            with self.assertRaises(ValueError):self.publisher({},self.receipt,self.context,self.result)
        read.assert_not_called()

    def test_installed_approval_guard_binds_before_staging_and_is_discarded(self):
        guard=Mock();self.publisher.approval_guard=guard
        bound=NS(report=NS(checked_at=self.now));guard.bind.return_value=bound
        with ExitStack() as stack:
            _,quality=self.patches(stack);quality.return_value=NS(report=NS(checked_at=self.now))
            self.publisher({},self.receipt,self.context,self.result)
        guard.capture.assert_called_once_with({})
        guard.bind.assert_called_once_with({},guard.capture.return_value,quality.return_value)
        guard.discard.assert_called_once_with(guard.capture.return_value)
        self.assertIs(self.publisher.evidence.stage_questions.call_args.args[0].quality,bound)

    def test_approval_revocation_prevents_staging_and_cleans_checkpoint(self):
        guard=Mock();self.publisher.approval_guard=guard;guard.bind.side_effect=ValueError('revoked approval')
        with ExitStack() as stack:
            _,quality=self.patches(stack)
            with self.assertRaisesRegex(ValueError,'revoked approval'):
                self.publisher({},self.receipt,self.context,self.result)
        self.publisher.evidence.stage_questions.assert_not_called()
        guard.discard.assert_called_once_with(guard.capture.return_value)
