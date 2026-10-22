"""Interpretation-stage source-use checks, distinct from downstream evidence approval.

Consumes native-reader captures; this pure function does not authenticate or fetch.
Never substitutes benchmark observations/uploads for current permissions or bytes.
"""
import json
from src.application.campaign_stage_applicability import stage_applicability,INTENT_CONTRACT
from src.application.campaign_quality_product_reader import ProductFactRead,OFFICIAL_SKUS
from src.application.campaign_clarification_policy import ClarificationPolicyRead
from src.application.campaign_evidence_gaps import validate_policy
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.workflow_contract import _require,_id,_digest

VERSION='campaign-evaluation-source-applicability-v1'
USES=('PRODUCT_IDENTITY','CLARIFICATION_REQUIREMENTS')


def interpretation_source_applicability(*,response_text,scope,product,policy,checked_at,source_uses=USES):
    now=_utc(checked_at)
    _require(type(scope) is dict and set(scope)=={'workspace_id','principal_id','sku','country','channel','audience','formula_version'},
        'EVALUATION_SOURCE_SCOPE')
    for key in ('workspace_id','principal_id','sku'):_id(scope[key])
    _require(scope['country'] in {'US','CA','GB','AU'} and scope['channel'] in {'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'}
        and scope['audience'] in {'NEW','RETURNING'},'EVALUATION_SOURCE_SCOPE')
    if scope['formula_version'] is not None:_id(scope['formula_version'])
    _require(type(source_uses) is tuple and source_uses==USES,'EVALUATION_SOURCE_USE_UNSUPPORTED')
    stage=stage_applicability('INTERPRETATION',response_text,INTENT_CONTRACT);intent=json.loads(response_text)
    _require(intent['audience']==scope['audience'] and intent['formula_version']==scope['formula_version'],
        'EVALUATION_SOURCE_INTENT_SCOPE_CHANGED')
    _require(type(product) is ProductFactRead and product.fact.sku==scope['sku'] and product.fact.access_allowed
        and product.fact.source_id==product.native_job_id and product.fact.observed_at<=now<product.fact.valid_until,
        'EVALUATION_SOURCE_PRODUCT_INELIGIBLE')
    _id(product.native_job_id)
    fields=json.loads(product.fact.fields_json)
    _require('product_name' in fields and set(fields)<={'product_name','full_size_ml'},'EVALUATION_SOURCE_PRODUCT_FIELDS')
    _require(type(policy) is ClarificationPolicyRead and policy.observed_at<=now<policy.valid_until,
        'EVALUATION_SOURCE_POLICY_INELIGIBLE')
    _id(policy.native_job_id);_digest(policy.record_sha256)
    requirements=validate_policy(policy.policy_canonical_json)
    _require(_hash(requirements)==policy.request.policy_sha256 and requirements['policy_id']==policy.request.policy_id
        and requirements['version']==policy.request.version,'EVALUATION_SOURCE_POLICY_CHANGED')
    allowed={feature for r in requirements['requirements'] for feature in r['when_any']}
    features=intent['features'];_require(set(features)<=allowed,'EVALUATION_SOURCE_FEATURE_UNSUPPORTED')
    downstream=[]
    for rule in requirements['requirements']:
        if not set(features)&set(rule['when_any']):continue
        unsupported=scope['sku'] in OFFICIAL_SKUS and rule['evidence_kind'] in {'SCIENTIFIC_CLAIM_PERMISSION','CURRENT_OFFER'}
        downstream.append(dict(requirement_id=rule['requirement_id'],evidence_kind=rule['evidence_kind'],
            required_fields=rule['required_fields'],state='UNAVAILABLE_LEGACY_PERMISSION_SCOPE' if unsupported else 'CURRENT_NATIVE_COVERAGE_REQUIRED',
            stage_use='REQUIRED_BEFORE_DOWNSTREAM_ASSERTION',approved=False))
    value=dict(version=VERSION,stage='INTERPRETATION',stage_context_sha256=stage['sha256'],scope_sha256=_hash(scope),
        response_sha256=stage['response_sha256'],features=sorted(features),
        current_stage_sources=[dict(use='PRODUCT_IDENTITY',native_job_id=product.native_job_id,
            fields_sha256=_hash(fields),capture_sha256=product.fact.sha256,observed_at=product.fact.observed_at.isoformat(),valid_until=product.fact.valid_until.isoformat()),
            dict(use='CLARIFICATION_REQUIREMENTS',native_job_id=policy.native_job_id,record_sha256=policy.record_sha256,
                policy_sha256=policy.request.policy_sha256,observed_at=policy.observed_at.isoformat(),valid_until=policy.valid_until.isoformat())],
        current_stage_source_checks='VERIFIED_WITHIN_NATIVE_READER_CAPTURE_SCOPE',
        downstream_requirements=downstream,downstream_policy_review_required=not policy.approval_verified,
        downstream_source_authority_verified=False,qualification_ready=False,generation_authorized=False,release_allowed=False)
    return dict(value,sha256=_hash(value))
