from dataclasses import replace
from datetime import timedelta
import hashlib,json
from unittest import TestCase
from unittest.mock import MagicMock,patch,PropertyMock
from src.application.campaign_question_view import QuestionTurn
from src.application.campaign_evidence_dispatch import EvidenceInvocationDraft
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.campaign_quality_checks import QualityCandidate,check_candidate
from src.application.campaign_quality_execution import QualityExecutionResult
from src.application.campaign_acceptance_contract import Stage,Decision,_hash
from src.application.campaign_evidence_gaps import canonical
from src.ui.question_chat import render_questions
from tests.test_prompt_contract import NOW

class QuestionViewTests(TestCase):
    def turn(self):
        text='Which shipping terms apply?'
        raw=canonical(dict(plan_sha256='a'*64,questions=[dict(requirement_id='shipping',fields=['terms'],question=text)]))
        draft=EvidenceInvocationDraft(raw,raw,'c'*64,'b'*64,ModelResultObservation('SUCCEEDED',
            response_sha256=hashlib.sha256(raw.encode()).hexdigest()))
        candidate=QualityCandidate('request',Stage.CLARIFICATION,'CI-SKU-0001','US','META',text,())
        report=check_candidate(candidate,(),checked_at=NOW,not_before=NOW)
        context=dict(request=dict(workspace_id='w',conversation_id='c',request_id='request'),
            invocation_binding_sha256='b'*64,candidate_sha256=candidate.sha256,report_sha256=report.sha256)
        quality=QualityExecutionResult(report,canonical(context),_hash(context))
        return QuestionTurn(draft,candidate,quality,NOW+timedelta(seconds=30))
    def test_real_quality_holds_never_display_questions(self):
        st=MagicMock();turn=self.turn()
        self.assertFalse(render_questions(st,turn,workspace_id='w',conversation_id='c',now=NOW))
        st.text.assert_not_called();st.chat_message.assert_not_called()
    def test_expired_cross_scope_or_changed_candidate_hides_text(self):
        turn=self.turn()
        cases=[(turn,'foreign',NOW),(turn,'w',turn.expires_at),
            (replace(turn,candidate=replace(turn.candidate,text='unreviewed text')),'w',NOW)]
        for value,workspace,now in cases:
            st=MagicMock()
            self.assertFalse(render_questions(st,value,workspace_id=workspace,conversation_id='c',now=now))
            st.text.assert_not_called()
    def test_synthetic_allowed_layout_uses_plain_text_and_unverified_notice(self):
        # Layout fixture only: does not establish actual policy/evaluation approval.
        turn=self.turn();report=replace(turn.quality.report,decision=Decision.PASS)
        turn=replace(turn,quality=replace(turn.quality,report=report))
        st=MagicMock()
        with patch.object(QualityExecutionResult,'release_allowed',new_callable=PropertyMock,return_value=True),\
                patch('src.ui.question_chat.require_release',return_value=report) as release:
            self.assertTrue(render_questions(st,turn,workspace_id='w',conversation_id='c',now=NOW))
        st.text.assert_called_once_with(turn.candidate.text)
        self.assertIn('unverified',st.caption.call_args.args[0]);release.assert_called_once()

    def test_projected_question_metadata_is_bound_even_when_display_text_is_unchanged(self):
        from dataclasses import asdict
        from src.application.campaign_question_projection import QuestionProjection,VERSION
        turn=self.turn();value=json.loads(turn.draft.output_canonical_json)
        projection=QuestionProjection(turn.candidate.request_id,'fixture-invocation',turn.draft.binding_sha256,
            value['plan_sha256'],turn.draft.observation.response_sha256,turn.draft.output_canonical_json,turn.candidate.text)
        context=json.loads(turn.quality.context_canonical_json)
        context.update(projection_sha256=projection.sha256,question_projection=dict(version=VERSION,**asdict(projection)))
        quality=replace(turn.quality,context_canonical_json=canonical(context),context_sha256=_hash(context))
        turn=replace(turn,quality=quality)
        turn.validate('w','c',NOW)
        value['questions'][0]['requirement_id']='different-requirement'
        changed=replace(turn,draft=replace(turn.draft,output_canonical_json=canonical(value)))
        with self.assertRaisesRegex(ValueError,'QUESTION_TURN_PROJECTION_CHANGED'):changed.validate('w','c',NOW)
        st=MagicMock()
        self.assertFalse(render_questions(st,changed,workspace_id='w',conversation_id='c',now=NOW))
        st.text.assert_not_called()
