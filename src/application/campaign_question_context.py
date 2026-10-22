"""Exact child-induced context projections; no approval or public release grant."""
from dataclasses import dataclass
from datetime import datetime
import json,hashlib
from src.application.workflow_schema import TABLES as WORKFLOW
from src.application.campaign_transcript import SPEC as BODY
from src.application.campaign_response_transcript import SPEC as RESPONSE
from src.application.campaign_quality_authority import table_metadata,validate_payload
from src.application.prompt_runtime_contract import TABLES as PROMPTS
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.workflow_contract import _require
from src.application.control_io import ControlPlan

SPECS=dict(WORKFLOW)|{BODY.name:BODY,RESPONSE.name:RESPONSE,
    'invocation_prompt_bindings':PROMPTS['invocation_prompt_bindings']}
NAMES=frozenset({'workspaces','memberships','conversations','requests','model_invocations',
    'jobs','job_attempts','audit_events',BODY.name,RESPONSE.name,
    'invocation_prompt_bindings','campaign_request_authorities'})


def _bounded_json(value,maximum):
    try:text=json.dumps(value,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))
    except (ValueError,TypeError,UnicodeError):
        _require(False,'QUESTION_CONTEXT_JSON_INVALID')
    _require(len(text.encode())<=maximum,'QUESTION_CONTEXT_TOO_LARGE')
    return text


def normalize(name,row):
    value=json.loads(row) if type(row) is str else dict(row)
    schema=table_metadata().schema if name=='campaign_request_authorities' else SPECS[name].schema
    _require(set(value)=={f.name for f in schema},'QUESTION_CONTEXT_ROW_SHAPE')
    for field in schema:
        if field.field_type=='TIMESTAMP' and value[field.name] is not None:
            at=value[field.name]
            if type(at) is str:at=datetime.fromisoformat(at.replace('Z','+00:00'))
            value[field.name]=_utc(at).isoformat()
    return _bounded_json(value,131072)


def canonical_records(records):
    _require(type(records) is dict and set(records)==NAMES,'QUESTION_CONTEXT_TABLE_COVERAGE')
    result={}
    for name,rows in records.items():
        _require(type(rows) is list and len(rows)<=256,'QUESTION_CONTEXT_OVERFLOW')
        result[name]=sorted(normalize(name,row) for row in rows)
        _require(len(result[name])==len(set(result[name])),'QUESTION_CONTEXT_DUPLICATE')
        keys=('workspace_id','request_id') if name=='campaign_request_authorities' else SPECS[name].logical_key
        logical=[tuple(json.loads(row)[key] for key in keys) for row in result[name]]
        _require(len(logical)==len(set(logical)),'QUESTION_CONTEXT_DUPLICATE_KEY')
    text=_bounded_json(result,4*1024*1024)
    _require(len(text.encode())<=4*1024*1024,'QUESTION_CONTEXT_TOO_LARGE')
    return text


@dataclass(frozen=True,slots=True)
class QuestionContextRead:
    workspace_id:str
    conversation_id:str
    principal_id:str
    records_canonical_json:str
    read_id:str
    observed_at:datetime
    valid_until:datetime

    @property
    def sha256(self):return hashlib.sha256(self.records_canonical_json.encode()).hexdigest()

    def records(self):
        value=json.loads(self.records_canonical_json)
        _require(canonical_records(value)==self.records_canonical_json,'QUESTION_CONTEXT_HASH_CHANGED')
        for name,rows in value.items():
            for text in rows:
                row=json.loads(text)
                _require(row['workspace_id']==self.workspace_id,'QUESTION_CONTEXT_SCOPE')
                if name not in {'workspaces','memberships'}:
                    _require(row.get('conversation_id')==self.conversation_id,'QUESTION_CONTEXT_SCOPE')
                if 'owner_id' in row:
                    _require(row['owner_id']==self.principal_id,'QUESTION_CONTEXT_SCOPE')
                if 'principal_id' in row:
                    _require(row['principal_id']==self.principal_id,'QUESTION_CONTEXT_SCOPE')
        return value


@dataclass(frozen=True,slots=True)
class QuestionContextTransition:
    phase:str
    before_sha256:str
    after_sha256:str
    plan_sha256:str
    @property
    def release_allowed(self):return False


def verify_transition(before,after,plan,*,phase,checked_at):
    _require(type(before) is QuestionContextRead and type(after) is QuestionContextRead,
        'QUESTION_CONTEXT_READ_REQUIRED')
    _require(type(plan) is ControlPlan,'QUESTION_CONTEXT_PLAN_REQUIRED')
    now=_utc(checked_at)
    _require(before.observed_at<=after.observed_at<=now<min(before.valid_until,after.valid_until)
        and before.read_id!=after.read_id,'QUESTION_CONTEXT_EXPIRED_OR_REUSED')
    _require((before.workspace_id,before.conversation_id,before.principal_id)==
        (after.workspace_id,after.conversation_id,after.principal_id)
        and plan.workspace_id==before.workspace_id and plan.actor_id==before.principal_id,
        'QUESTION_CONTEXT_SCOPE')
    expected=before.records();actual=after.records()
    p={v.name:v.value for v in plan.config().query_parameters}
    for name,parameter in (('memberships','membership_pin' if phase=='ADMITTED' else 'memberships_pin'),
            ('conversations','conversation_pin' if phase=='ADMITTED' else 'conversations_pin')):
        _require(parameter in p and normalize(name,p[parameter]) in expected[name],'QUESTION_CONTEXT_PIN_CHANGED')
    def row(parameter,name):return json.loads(normalize(name,p[parameter]))
    def add(name,value):
        encoded=normalize(name,value)
        _require(encoded not in expected[name],'QUESTION_CONTEXT_DUPLICATE')
        expected[name].append(encoded);expected[name].sort()
    def update(name,pin,candidate):
        old=normalize(name,pin);new=normalize(name,candidate)
        _require(expected[name].count(old)==1,'QUESTION_CONTEXT_PIN_CHANGED')
        expected[name].remove(old);expected[name].append(new);expected[name].sort()
    if phase=='ADMITTED':
        _require(plan.kind=='QUESTION_INTERPRETER_SUBMISSION','QUESTION_CONTEXT_PLAN_KIND')
        request=row('request_candidate','requests');call=row('invocation_candidate','model_invocations')
        authority=validate_payload(json.loads(p['authority_payload']))
        _require(authority['version']=='campaign-question-authority-v1'
            and _hash(authority)==p['authority_digest']
            and authority['request_id']==request['request_id']==call['request_id']
            and authority['question_parent']['question_profile_sha256']==_hash(call['profile']),
            'QUESTION_CONTEXT_CHILD_BINDING')
        update('workspaces',p['workspace_pin'],p['workspace_candidate'])
        for name,param in (('requests','request_candidate'),('model_invocations','invocation_candidate'),
                (BODY.name,'transcript_candidate'),('audit_events','request_audit'),('audit_events','invocation_audit')):
            add(name,p[param])
        add('campaign_request_authorities',dict(workspace_id=request['workspace_id'],
            conversation_id=request['conversation_id'],request_id=request['request_id'],principal_id=request['principal_id'],
            payload_canonical_json=p['authority_payload'],payload_sha256=p['authority_digest'],recorded_at=request['created_at']))
    elif phase=='DISPATCHED':
        _require(plan.kind=='DISPATCH' and 'runtime_binding' in p and 'attempt' not in p,
            'QUESTION_CONTEXT_PLAN_KIND')
        request=row('requests_pin','requests');old=row('model_invocations_pin','model_invocations');new=row('candidate','model_invocations')
        authority=[json.loads(t) for t in expected['campaign_request_authorities']
            if json.loads(t)['request_id']==request['request_id']]
        _require(len(authority)==1,'QUESTION_CONTEXT_CHILD_AUTHORITY')
        payload=validate_payload(json.loads(authority[0]['payload_canonical_json']))
        binding=row('runtime_binding','invocation_prompt_bindings')
        _require(payload['version']=='campaign-question-authority-v1' and old['state']=='PLANNED'
            and new['state']=='DISPATCHED' and new['revision']==old['revision']+1
            and old['request_id']==new['request_id']==request['request_id']==binding['request_id']
            and old['invocation_id']==new['invocation_id']==binding['invocation_id']
            and payload['question_parent']['question_profile_sha256']==_hash(old['profile'])
            and old['profile']==new['profile'] and binding['prompt_key']=='campaign-evidence-questions',
            'QUESTION_CONTEXT_CHILD_BINDING')
        _require(normalize('requests',request) in expected['requests'],'QUESTION_CONTEXT_PIN_CHANGED')
        update('workspaces',p['workspaces_pin'],p['workspace'])
        update('model_invocations',old,new)
        add('audit_events',p['audit']);add('invocation_prompt_bindings',binding)
    else:_require(False,'QUESTION_CONTEXT_PHASE')
    _require(canonical_records(expected)==canonical_records(actual),'QUESTION_CONTEXT_UNEXPECTED_CHANGE')
    return QuestionContextTransition(phase,before.sha256,after.sha256,
        _hash(dict(sql_sha256=hashlib.sha256(plan.sql.encode()).hexdigest(),
            config_sha256=hashlib.sha256(plan.config_json.encode()).hexdigest(),job_id=plan.job_id)))
