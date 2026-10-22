"""Versioned feature projection from a successful durable interpreter response."""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
from src.application.campaign_evidence_gaps import validate_policy,canonical
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_quality_authority import bind_provider
from src.application.campaign_planner_projection import parse_json
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.workflow_contract import _require,_id

VERSION='campaign-evidence-intent-v1'
INTENT_KEY='campaign-evidence-intent'

@dataclass(frozen=True,slots=True)
class EvidenceIntent:
    scope_json: str
    features: tuple[str,...]
    formula_version: str | None
    audience: str
    authority_sha256: str
    response_sha256: str


def bind_evidence_intent(capture,response_text,policy_json,*,checked_at=None):
    now=_utc(checked_at or datetime.now(timezone.utc));policy=validate_policy(policy_json)
    _require(type(capture) is ExecutionAuthorityRead and capture.observed_at<=now<capture.valid_until,
        'GAP_INTENT_AUTHORITY_EXPIRED')
    content=json.loads(capture.content_canonical_json)
    _require(canonical(content)==capture.content_canonical_json and _hash(content)==capture.content_sha256,
        'GAP_INTENT_AUTHORITY_CHANGED')
    a=content['authority'];request=content['request_record']
    _require(request['operation']=='INTERPRET' and request['request_status'] in {'READY','NEEDS_CLARIFICATION'}
        and all(a[k]==request[k] for k in ('workspace_id','conversation_id','request_id','principal_id')),
        'GAP_INTENT_REQUEST_REQUIRED')
    calls=[r for r in content['calls'] if r['request_id']==request['request_id'] and r['stage']=='INTERPRETER']
    _require(len(calls)==1 and calls[0]['profile']['output_schema_version']==VERSION,'GAP_INTENT_SCHEMA_UNSUPPORTED')
    call=calls[0]
    bindings=[b for b in content.get('bindings',[]) if b['invocation_id']==call['invocation_id']]
    _require(len(bindings)==1,'GAP_INTENT_BINDING_REQUIRED')
    binding=bindings[0];payload=json.loads(binding['binding_canonical_json'])
    _require(binding['prompt_key']==INTENT_KEY and binding['stage']=='INTERPRETER'
        and all(binding[k]==request[k] for k in ('workspace_id','conversation_id','request_id','principal_id'))
        and payload['profile']==call['profile'] and payload['snapshot']['prompt_key']==INTENT_KEY
        and payload['snapshot']['output_schema_version']==VERSION,'GAP_INTENT_BINDING_CHANGED')
    bind_provider(call,request=request,profile=call['profile'],response_text=response_text,candidate_text=response_text)
    value=parse_json(response_text)
    _require(type(value) is dict and set(value)=={'schema_version','features','formula_version','audience'}
        and value['schema_version']==VERSION,'GAP_INTENT_SHAPE')
    allowed={f for rule in policy['requirements'] for f in rule['when_any']}
    _require(type(value['features']) is list and len(value['features'])<=32
        and all(type(f) is str for f in value['features']) and len(set(value['features']))==len(value['features'])
        and set(value['features'])<=allowed,'GAP_INTENT_FEATURES')
    _require(value['audience'] in {'NEW','RETURNING'} and value['audience']==a['audience']
        and value['formula_version']==a['formula_version'],'GAP_INTENT_SELECTION_CHANGED')
    if value['formula_version'] is not None:_id(value['formula_version'])
    response_sha=hashlib.sha256(response_text.encode()).hexdigest()
    scope={k:a[k] for k in ('workspace_id','conversation_id','request_id','sku','country','channel')}
    scope.update(request_sha256=request['request_sha256'],intent_sha256=response_sha)
    return EvidenceIntent(canonical(scope),tuple(sorted(value['features'])),value['formula_version'],value['audience'],
        capture.content_sha256,response_sha)
