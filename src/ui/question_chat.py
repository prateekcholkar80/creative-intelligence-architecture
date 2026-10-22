"""Render server-held question turns only after the existing quality gate allows."""
from datetime import datetime,timezone
from src.application.campaign_question_view import QuestionTurn
from src.application.campaign_acceptance_contract import Decision
from src.application.workflow_contract import _require
from src.application.campaign_quality_checks import require_release

def render_questions(st,turn,*,workspace_id,conversation_id,now=None):
    if turn is None:return False
    try:
        _require(type(turn) is QuestionTurn,'QUESTION_TURN_UNAVAILABLE')
        value=turn.validate(workspace_id,conversation_id,now or datetime.now(timezone.utc))
        if not turn.quality.release_allowed or turn.quality.report.decision is not Decision.PASS:
            st.info('Evidence questions are awaiting validation and review.')
            return False
        require_release(turn.candidate, turn.sources, turn.quality.report,
            checked_at=now or datetime.now(timezone.utc),
            not_before=turn.quality.report.not_before)
    except Exception:
        st.info('Evidence questions are unavailable. Refresh after service verification.')
        return False
    with st.chat_message('assistant'):
        for question in value['questions']:
            st.text(question['question'])
        st.caption('Information you provide remains unverified until checked and reviewed.')
    return True
