"""Versioned exact question JSON-to-copy projection; no approval/release grant."""
from dataclasses import dataclass,asdict
import hashlib,json
from src.application.campaign_evidence_gaps import GapPlan,validate_questions
from src.application.campaign_planner_projection import parse_json
from src.application.campaign_quality_authority import validate_payload,bind_provider
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import _require,_digest

VERSION='campaign-question-projection-v1'


@dataclass(frozen=True,slots=True)
class QuestionProjection:
    request_id:str
    invocation_id:str
    binding_sha256:str
    plan_sha256:str
    response_sha256:str
    questions_canonical_json:str
    text:str
    @property
    def sha256(self):return _hash(dict(version=VERSION,**asdict(self)))
    @property
    def release_allowed(self):return False


def bind_question_output(plan,response_text,*,authority,request,invocation,profile,binding_sha256):
    _require(type(plan) is GapPlan,'QUESTION_PROJECTION_PLAN_REQUIRED');_digest(binding_sha256)
    validate_payload(authority)
    scope=json.loads(plan.canonical_json)['scope'];parent=authority.get('question_parent',{})
    _require(authority['version']=='campaign-question-authority-v1' and authority['stage']=='CLARIFICATION'
        and authority['request_id']==request['request_id']==invocation['request_id']
        and authority['request_sha256']==request['request_sha256']
        and all(authority[k]==request[k] for k in ('workspace_id','conversation_id','principal_id'))
        and all(authority[k]==scope[k] for k in ('workspace_id','conversation_id','sku','country','channel'))
        and parent.get('request_id')==scope['request_id']
        and parent.get('request_sha256')==scope['request_sha256']
        and parent.get('response_sha256')==scope['intent_sha256']
        and parent.get('gap_plan_sha256')==plan.sha256
        and parent.get('question_profile_sha256')==_hash(profile)
        and invocation['stage']==profile['stage']=='INTERPRETER'
        and profile['output_schema_version']=='campaign-evidence-questions-v1',
        'QUESTION_PROJECTION_BINDING_CHANGED')
    provider_sha=bind_provider(invocation,request=request,profile=profile,
        response_text=response_text,candidate_text=response_text)
    canonical=validate_questions(plan,parse_json(response_text))
    questions=json.loads(canonical)['questions']
    _require(0<len(questions)<=3,'QUESTION_PROJECTION_EMPTY')
    text='\n'.join(q['question'] for q in questions)
    projection=QuestionProjection(request['request_id'],invocation['invocation_id'],binding_sha256,
        plan.sha256,hashlib.sha256(response_text.encode()).hexdigest(),canonical,text)
    return projection,provider_sha
