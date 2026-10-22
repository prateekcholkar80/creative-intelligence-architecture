"""Explicit use limits for the identity-only private evaluation, never approval.

Pure checks over caller-verified native captures. A source fingerprint or a visual
finding alone is not current access, review authority or permission to dispatch.
"""
import json
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_quality_product_reader import ProductFactRead
from src.application.campaign_acceptance_contract import _utc
from src.application.workflow_contract import _require,_digest
from src.retrieval.upload_corpus import UploadCorpusCapture,VERSION as CORPUS_VERSION

VERSION='private-identity-source-applicability-v1'
FEATURES=('PRODUCT_IDENTITY','NEUTRAL_CTA')


def identity_source_plan(*,capture,product,copy,features,checked_at,image_findings=None):
    now=_utc(checked_at)
    _require(type(capture) is UploadCorpusCapture and type(product) is ProductFactRead,'PRIVATE_SOURCE_CAPTURE')
    value=json.loads(capture.manifest_json)
    _require(canonical(value)==capture.manifest_json and value['version']==CORPUS_VERSION
        and value['use']=='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY'
        and all(value[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed')),
        'PRIVATE_SOURCE_CAPTURE')
    _require(type(features) is tuple and features==FEATURES,'PRIVATE_SOURCE_DECLARED_FEATURES')
    fact=product.fact;fields=json.loads(fact.fields_json)
    _require(fact.sku==value['scope']['sku'] and fact.access_allowed is True
        and fact.source_id==product.native_job_id and fact.observed_at<=now<fact.valid_until,
        'PRIVATE_SOURCE_IDENTITY_UNAVAILABLE')
    _require(type(copy) is dict and set(copy)=={'headline','body','cta'}
        and copy==dict(headline=fields['product_name'],body='',cta='View product'),
        'PRIVATE_SOURCE_COPY_OUTSIDE_IDENTITY_SCOPE')
    images=value['image_pins'];_require(type(images) is list and 0<len(images)<=16,'PRIVATE_SOURCE_IMAGE_PINS')
    expected={p['sha256'] for p in images}
    for digest in expected:_digest(digest)
    checks=[]
    if image_findings is None:
        checks=[dict(image_sha256=d,decision='NEEDS_REVIEW',reasons=['SCOPED_IMAGE_ASSESSMENT_NOT_EXECUTED']) for d in sorted(expected)]
    else:
        _require(type(image_findings) is tuple and len(image_findings)==len(expected)
            and {v['image_sha256'] for v in image_findings}==expected,'PRIVATE_SOURCE_IMAGE_COVERAGE')
        for image in image_findings:
            _require(set(image)=={'image_sha256','assessment_record_id','assessment_outcome_sha256',
                'product_identity','product_visible','contains_people','contains_before_after',
                'contains_clinical_badge','embedded_claims'},'PRIVATE_SOURCE_IMAGE_FINDINGS')
            for k in ('assessment_record_id','assessment_outcome_sha256'):_digest(image[k])
            _require(image['product_identity'] in {'MATCH','MISMATCH','UNAVAILABLE'}
                and all(type(image[k]) is bool for k in ('product_visible','contains_people','contains_before_after','contains_clinical_badge'))
                and type(image['embedded_claims']) is list and len(image['embedded_claims'])<=24
                and all(type(c) is str and 0<len(c)<=512 for c in image['embedded_claims']),'PRIVATE_SOURCE_IMAGE_FINDINGS')
            reasons=[];decision='APPLICABLE_AS_UNVERIFIED_PRIVATE_REFERENCE'
            if image['contains_people'] or image['contains_before_after']:
                decision='BLOCK';reasons.append('PROHIBITED_PEOPLE_OR_BEFORE_AFTER_REFERENCE')
            elif image['product_identity']=='MISMATCH' or image['product_visible'] is False:
                decision='BLOCK';reasons.append('NOT_THE_REQUIRED_PRODUCT_IMAGE')
            elif image['product_identity']=='UNAVAILABLE':
                decision='NEEDS_REVIEW';reasons.append('PRODUCT_VISUAL_IDENTITY_UNVERIFIED')
            if image['contains_clinical_badge'] or image['embedded_claims']:
                reasons.append('EMBEDDED_CLAIMS_NOT_APPROVED')
                if decision!='BLOCK':decision='NEEDS_REVIEW'
            checks.append(dict(image_sha256=image['image_sha256'],decision=decision,reasons=reasons,
                assessment_record_id=image['assessment_record_id'],assessment_outcome_sha256=image['assessment_outcome_sha256']))
    result=dict(version=VERSION,scope=value['scope'],principal_id=value['principal_id'],corpus_sha256=capture.sha256,
        declared_features=list(features),copy_sha256=sha(copy),
        identity=dict(native_job_id=product.native_job_id,capture_sha256=fact.sha256,
            fields_sha256=sha(fields),observed_at=fact.observed_at.isoformat(),valid_until=fact.valid_until.isoformat()),
        text_uses=[dict(chunk_id=c['chunk_id'],text_sha256=c['provenance']['text_sha256'],
            use='RETRIEVAL_RELEVANCE_ONLY',trust='UNTRUSTED_USER_EVIDENCE',approved_copy_source=False) for c in value['text_chunks']],
        claim_applicability='NOT_ASSERTED_BY_EXPLICIT_IDENTITY_ONLY_COPY',image_checks=checks,
        downstream_holds=['BRAND_MARKET_CHANNEL_REVIEW_REQUIRED','EXACT_RENDER_PROMPT_PROFILE_REQUIRED','RENDERED_ARTIFACT_CHECKS_REQUIRED'],
        input_use_verified_by_this_pure_function=False,claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
    return dict(result,sha256=sha(result))
