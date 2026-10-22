"""Bounded stage/output classification; not model quality, source approval or release."""
import hashlib,json
from src.application.campaign_acceptance_contract import Stage,_hash
from src.application.campaign_planner_projection import parse_json,VERSION as PLANNER_CONTRACT
from src.application.workflow_contract import _require,_id

VERSION='campaign-stage-applicability-v1'
INTENT_CONTRACT='campaign-evidence-intent-v1'


def stage_applicability(stage,response_text,output_contract):
    _require(stage in {s.value for s in Stage} and type(response_text) is str and 0<len(response_text)<=8192,
        'STAGE_APPLICABILITY_INPUT')
    _require(output_contract is None or type(output_contract) is str,'STAGE_APPLICABILITY_CONTRACT')
    kind='UNRESOLVED';u02='UNRESOLVED';reason='OUTPUT_CONTRACT_NOT_ESTABLISHED'
    if output_contract==INTENT_CONTRACT:
        _require(stage==Stage.INTERPRETATION.value,'STAGE_APPLICABILITY_CONTRACT_STAGE')
        value=parse_json(response_text)
        _require(type(value) is dict and set(value)=={'schema_version','features','formula_version','audience'}
            and value['schema_version']==INTENT_CONTRACT and type(value['features']) is list and len(value['features'])<=32
            and all(type(f) is str and f for f in value['features']) and len(value['features'])==len(set(value['features']))
            and value['audience'] in {'NEW','RETURNING'},'STAGE_APPLICABILITY_INTENT_SHAPE')
        if value['formula_version'] is not None:_id(value['formula_version'])
        kind='EVIDENCE_INTENT_METADATA';u02='NOT_APPLICABLE';reason='NO_CAMPAIGN_CONCEPT_DELIVERABLE_IN_TYPED_INTENT'
    elif output_contract==PLANNER_CONTRACT:
        # This contract is supplied by the server's verified planner projection,
        # never guessed from words or a model's self-declared output label.
        kind='CAMPAIGN_CONCEPT_DELIVERABLE';u02='APPLICABLE';reason='CAMPAIGN_CONCEPT_DELIVERABLE_CONTRACT'
    value=dict(version=VERSION,stage=stage,output_contract=output_contract,
        response_sha256=hashlib.sha256(response_text.encode()).hexdigest(),output_kind=kind,
        u02=dict(applicability=u02,reason_code=reason),release_allowed=False)
    return dict(value,sha256=_hash(value))


def verify_stage_context(encoded,*,stage,response_text):
    _require(type(encoded) is str and len(encoded.encode())<=8192,'STAGE_APPLICABILITY_CONTEXT')
    value=json.loads(encoded)
    _require(type(value) is dict and value==stage_applicability(stage,response_text,value.get('output_contract')),
        'STAGE_APPLICABILITY_CONTEXT_CHANGED')
    return value
