"""Pure evidence-gap/question contracts. Supplied captures are not BigQuery proof.

No keyword inference, local policy loading, model call, approval or response release.
The installed interpreter/readers must provide scoped features and fresh coverage.
"""
from dataclasses import dataclass
from datetime import datetime,timedelta
import json
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.workflow_contract import _require,_id,_digest,canonical_json_bytes

VERSION='campaign-evidence-gap-plan-v1'
STATES=frozenset({'CURRENT_APPROVED','ABSENT','PENDING_REVIEW','REJECTED','UNAVAILABLE','STALE','CONFLICT'})


def canonical(value):return canonical_json_bytes(value).decode()


def _unique(values,maximum):
    _require(type(values) is list and 0<len(values)<=maximum and len(set(values))==len(values),'GAP_POLICY_VALUES')
    for v in values:_id(v)


def validate_policy(policy_json):
    _require(type(policy_json) is str and len(policy_json.encode())<=65536,'GAP_POLICY_SIZE')
    policy=json.loads(policy_json)
    _require(type(policy) is dict and set(policy)=={'schema_version','policy_id','version','status','max_questions',
        'question_guidance','requirements'} and canonical(policy)==policy_json,'GAP_POLICY_SHAPE')
    _require(policy['schema_version']=='campaign-evidence-requirements-v1' and policy['status'] in {'DRAFT','APPROVED'}
        and type(policy['max_questions']) is int and 1<=policy['max_questions']<=3
        and type(policy['question_guidance']) is str and 0<len(policy['question_guidance'])<=4096,'GAP_POLICY_VALUES')
    _id(policy['policy_id']);_id(policy['version'])
    rules=policy['requirements'];_require(type(rules) is list and 1<=len(rules)<=32,'GAP_POLICY_RULES')
    for r in rules:
        _require(type(r) is dict and set(r)=={'requirement_id','when_any','evidence_kind','owner','required_fields','alternative'},'GAP_POLICY_RULE')
        for k in ('requirement_id','evidence_kind','owner','alternative'):_id(r[k])
        _unique(r['when_any'],32);_unique(r['required_fields'],32)
    _require(len({r['requirement_id'] for r in rules})==len(rules),'GAP_POLICY_DUPLICATE')
    return policy


@dataclass(frozen=True,slots=True)
class EvidenceCoverage:
    requirement_id: str
    scope_sha256: str
    state: str
    available_fields: tuple[str,...]
    observed_at: datetime
    valid_until: datetime
    native_job_id: str


@dataclass(frozen=True,slots=True)
class GapPlan:
    canonical_json: str

    @property
    def sha256(self):return _hash(json.loads(self.canonical_json))

    @property
    def generation_authorized(self):return False

    def question_request(self):
        plan=json.loads(self.canonical_json)
        _require(plan['decision']=='CLARIFY' and bool(plan['questions']),'GAP_QUESTIONS_NOT_NEEDED')
        return dict(plan_sha256=self.sha256,policy_sha256=plan['policy_sha256'],
            instructions=plan['question_guidance'],gaps=plan['questions'],
            response_schema=dict(type='object',required=['plan_sha256','questions'],additionalProperties=False,
                properties=dict(plan_sha256=dict(type='string',enum=[self.sha256]),questions=dict(type='array',
                    minItems=len(plan['questions']),maxItems=len(plan['questions']),items=dict(type='object',
                        additionalProperties=False,required=['requirement_id','fields','question'],properties=dict(
                            requirement_id=dict(type='string',enum=[g['requirement_id'] for g in plan['questions']]),
                            fields=dict(type='array',items=dict(type='string')),question=dict(type='string',maxLength=1200)))))))


def plan_gaps(policy_json,*,scope,features,captures,checked_at):
    policy=validate_policy(policy_json);now=_utc(checked_at)
    _require(type(scope) is dict and set(scope)=={'workspace_id','conversation_id','request_id','sku','country','channel','request_sha256','intent_sha256'}
        and all(type(v) is str and v for v in scope.values()),'GAP_SCOPE_REQUIRED')
    for v in scope.values():_id(v)
    for key in ('request_sha256','intent_sha256'):_digest(scope[key])
    allowed={f for r in policy['requirements'] for f in r['when_any']}
    _require(type(features) is tuple and len(set(features))==len(features)
        and set(features)<=allowed,'GAP_FEATURES_UNRESOLVED')
    active=[r for r in policy['requirements'] if set(features)&set(r['when_any'])]
    _require(type(captures) is tuple and all(type(c) is EvidenceCoverage for c in captures)
        and len(captures)==len(active) and {c.requirement_id for c in captures}=={r['requirement_id'] for r in active},
        'GAP_RETRIEVAL_COVERAGE_REQUIRED')
    by_id={c.requirement_id:c for c in captures};outcomes=[];questions=[];pins=[]
    for rule in active:
        c=by_id[rule['requirement_id']]
        _require(c.scope_sha256==_hash(scope) and c.state in STATES
            and type(c.available_fields) is tuple and len(set(c.available_fields))==len(c.available_fields)
            and set(c.available_fields)<=set(rule['required_fields']),'GAP_CAPTURE_SCOPE')
        _id(c.native_job_id)
        _require(now-timedelta(seconds=60)<=_utc(c.observed_at)<=now<_utc(c.valid_until)
            <=c.observed_at+timedelta(seconds=60),'GAP_CAPTURE_EXPIRED')
        missing=[f for f in rule['required_fields'] if f not in c.available_fields]
        if c.state=='ABSENT':_require(not c.available_fields,'GAP_ABSENCE_CONFLICT')
        if c.state=='CURRENT_APPROVED' and not missing:action='COVERED'
        elif c.state=='UNAVAILABLE':action='RETRY_RETRIEVAL'
        elif c.state in {'PENDING_REVIEW','REJECTED','STALE','CONFLICT'}:action='NEEDS_REVIEW'
        else:
            action='CLARIFY'
            questions.append(dict(requirement_id=rule['requirement_id'],evidence_kind=rule['evidence_kind'],
                owner=rule['owner'],fields=missing,alternative=rule['alternative'],submission_status='UNVERIFIED'))
        outcomes.append(dict(requirement_id=rule['requirement_id'],action=action))
        pins.append(dict(requirement_id=c.requirement_id,state=c.state,available_fields=list(c.available_fields),
            native_job_id=c.native_job_id,observed_at=c.observed_at.isoformat(),valid_until=c.valid_until.isoformat()))
    priority=('RETRY_RETRIEVAL','NEEDS_REVIEW','CLARIFY')
    decision=next((a for a in priority if any(o['action']==a for o in outcomes)),'NO_EVIDENCE_GAPS')
    # Do not ask users to work around an outage/conflicting or unreviewed source.
    selected=questions[:policy['max_questions']] if decision=='CLARIFY' else []
    return GapPlan(canonical(dict(version=VERSION,scope=scope,features=list(features),policy_sha256=_hash(policy),
        policy_status=policy['status'],question_guidance=policy['question_guidance'],outcomes=outcomes,
        decision=decision,questions=selected,deferred_questions=max(0,len(questions)-len(selected)),
        capture_sha256=_hash(pins),checked_at=now.isoformat(),generation_authorized=False)))


def validate_questions(plan,response):
    """Integrity/coverage checks only; semantic safety still needs quality gates."""
    _require(type(plan) is GapPlan and type(response) is dict and set(response)=={'plan_sha256','questions'}
        and response['plan_sha256']==plan.sha256,'GAP_QUESTION_PLAN_CHANGED')
    expected=json.loads(plan.canonical_json)['questions'];questions=response['questions']
    _require(type(questions) is list and len(questions)==len(expected),'GAP_QUESTION_COVERAGE')
    for value,gap in zip(questions,expected):
        _require(type(value) is dict and set(value)=={'requirement_id','fields','question'}
            and value['requirement_id']==gap['requirement_id'] and value['fields']==gap['fields']
            and type(value['question']) is str and 0<len(value['question'].strip())<=1200,'GAP_QUESTION_CHANGED')
    return canonical(response)
