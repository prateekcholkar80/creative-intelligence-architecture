"""Bound server-held question-view contract; no approval or release grant."""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
from src.application.campaign_evidence_dispatch import EvidenceInvocationDraft
from src.application.campaign_quality_checks import QualityCandidate
from src.application.campaign_quality_execution import QualityExecutionResult
from src.application.campaign_acceptance_contract import Stage,Decision,_hash
from src.application.workflow_contract import _require


@dataclass(frozen=True,slots=True)
class QuestionTurn:
    draft: EvidenceInvocationDraft
    candidate: QualityCandidate
    quality: QualityExecutionResult
    expires_at: datetime
    sources: tuple = ()

    def validate(self,workspace_id,conversation_id,now):
        _require(type(self.draft) is EvidenceInvocationDraft and type(self.candidate) is QualityCandidate
            and type(self.quality) is QualityExecutionResult and now < self.expires_at,
            'QUESTION_TURN_UNAVAILABLE')
        context=json.loads(self.quality.context_canonical_json)
        _require(_hash(context)==self.quality.context_sha256
            and context['request']['workspace_id']==workspace_id
            and context['request']['conversation_id']==conversation_id
            and context['request']['request_id']==self.candidate.request_id
            and self.candidate.stage is Stage.CLARIFICATION
            and context['invocation_binding_sha256']==self.draft.binding_sha256
            and context['candidate_sha256']==self.candidate.sha256
            and self.quality.report.candidate_sha256==self.candidate.sha256
            and context['report_sha256']==self.quality.report.sha256
            and hashlib.sha256(self.draft.response_text.encode()).hexdigest()==self.draft.observation.response_sha256,
            'QUESTION_TURN_BINDING_CHANGED')
        value=json.loads(self.draft.output_canonical_json)
        _require(type(value) is dict and set(value)=={'plan_sha256','questions'}
            and type(value['questions']) is list and 0<len(value['questions'])<=3
            and all(type(q) is dict and set(q)=={'requirement_id','fields','question'}
                and type(q['question']) is str and 0<len(q['question'])<=2048 for q in value['questions'])
            and self.candidate.text=='\n'.join(q['question'] for q in value['questions']),
            'QUESTION_TURN_OUTPUT_CHANGED')
        if context.get('projection_sha256') is not None:
            projection=context.get('question_projection')
            _require(type(projection) is dict and set(projection)=={'version','request_id','invocation_id',
                'binding_sha256','plan_sha256','response_sha256','questions_canonical_json','text'}
                and projection['version']=='campaign-question-projection-v1'
                and _hash(projection)==context['projection_sha256']
                and projection['request_id']==self.candidate.request_id
                and projection['binding_sha256']==self.draft.binding_sha256
                and projection['response_sha256']==self.draft.observation.response_sha256
                and projection['plan_sha256']==value['plan_sha256']
                and projection['questions_canonical_json']==self.draft.output_canonical_json
                and projection['text']==self.candidate.text,'QUESTION_TURN_PROJECTION_CHANGED')
        return value

