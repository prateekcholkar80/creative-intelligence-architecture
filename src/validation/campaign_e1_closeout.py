"""Check the complete E1 review packet. Never records human approval or release."""
import argparse,hashlib,json
from pathlib import Path
from src.validation.campaign_e1_benchmark import ROOT,MAX_BYTES,_encoded,_exact,_text,_pairs,_reject_constant,load_package
from src.validation.campaign_e1_brand_cases import load as load_brand
from src.application.campaign_acceptance_contract import RULE_IDS,Stage,Decision
from src.application.workflow_contract import _require
from src.ingestion.configure_campaign_acceptance import manifest,SOURCES

DEFAULT=ROOT/'config/evaluation/campaign_e1_closeout_v1.json'
REVIEW=ROOT/'config/evaluation/campaign_e1_review_acceptance_v1.json'
CHANNELS={'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'}
COUNTRIES={'AU','CA','GB','US'}


def validate(package):
    _exact(package,'schema_version status policy_version policy_specification_sha256 approval serving_enabled e1_complete '
        'criteria stage_rubrics channel_deliverables source_precedence market_dispositions visual_rubric visual_specimens '
        'case_artifacts case_index supplemental_cases review_requirements scope_limits')
    raw=_encoded(package)
    _require(package['schema_version']=='campaign-e1-closeout-v1' and package['status']=='READY_FOR_HUMAN_REVIEW'
        and package['approval'] is None and package['serving_enabled'] is False and package['e1_complete'] is False,
        'E1_CLOSEOUT_REVIEW_REQUIRED')
    source=manifest(SOURCES['draft-v0.2'],version='draft-v0.2');rules={r['rule_id']:json.loads(r['payload_canonical_json']) for r in source[1:]}
    _require(package['policy_version']=='draft-v0.2' and package['policy_specification_sha256']==source[0]['specification_sha256'],
        'E1_CLOSEOUT_POLICY_CONFLICT')
    base,_=load_package();load_brand()
    _require(type(package['criteria']) is list and len(package['criteria'])==47,'E1_CLOSEOUT_RULE_COVERAGE')
    for criterion,rid in zip(package['criteria'],RULE_IDS):
        _exact(criterion,'rule_id area acceptance_rule failure_handling stage_applicability required_evidence review_status')
        _require(criterion['rule_id']==rid and criterion['review_status']=='PENDING_REVIEW'
            and all(criterion[k]==rules[rid][k] for k in ('area','acceptance_rule','failure_handling')),
            'E1_CLOSEOUT_CRITERIA_CONFLICT')
        _require(set(criterion['stage_applicability'])=={s.value for s in Stage},'E1_CLOSEOUT_STAGE_COVERAGE')
        for applicability in criterion['stage_applicability'].values():
            _exact(applicability,'mode trigger not_applicable_requires')
            _require(applicability['mode']=='APPLICABLE_IF_TRIGGERED','E1_CLOSEOUT_STAGE_EXCLUSION')
            _text(applicability['trigger']);_text(applicability['not_applicable_requires'])
            if rid=='C07':_require('absence of efficacy claims do not exempt AI people' in applicability['trigger'],
                'E1_CLOSEOUT_AI_PEOPLE_EXEMPTION')
    _require(set(package['stage_rubrics'])=={s.value for s in Stage} and set(package['channel_deliverables'])==CHANNELS,
        'E1_CLOSEOUT_RUBRIC_COVERAGE')
    for values in list(package['stage_rubrics'].values())+list(package['channel_deliverables'].values()):
        _require(type(values) is list and len(values)>=3,'E1_CLOSEOUT_RUBRIC_COVERAGE')
        for value in values:_text(value)
    _require({r['domain'] for r in package['source_precedence']}=={'PRODUCT_FACTS','BENEFIT_CLAIMS','SCIENTIFIC_WORDING',
        'COMMERCIAL_FACTS','CAMPAIGN_CONSTRAINTS','ANALYTICS','CONFLICTS'} and len(package['source_precedence'])==7,
        'E1_CLOSEOUT_SOURCE_PRECEDENCE')
    for row in package['source_precedence']:
        _exact(row,'domain rule uncertainty');_text(row['rule']);_text(row['uncertainty'])
    markets=package['market_dispositions'];_require(type(markets) is list and len(markets)==16,'E1_CLOSEOUT_MARKET_COVERAGE')
    for row in markets:
        _exact(row,'country channel scope overlay_status campaign_compliance_pass_allowed missing_overlay_decision')
        _require(row['scope']=='INTERNAL_DRAFT_BENCHMARK_ONLY' and row['overlay_status']=='UNREVIEWED'
            and row['campaign_compliance_pass_allowed'] is False and row['missing_overlay_decision']=='NEEDS_REVIEW',
            'E1_CLOSEOUT_MARKET_CLEARANCE_FORGED')
    _require({(r['country'],r['channel']) for r in markets}=={(c,ch) for c in COUNTRIES for ch in CHANNELS},
        'E1_CLOSEOUT_MARKET_COVERAGE')
    _require([r['rubric_id'] for r in package['visual_rubric']]==[f'V{i:02}' for i in range(1,9)],'E1_CLOSEOUT_VISUAL_COVERAGE')
    for row in package['visual_rubric']:
        _exact(row,'rubric_id criterion acceptance failure_decision anchor')
        for key in ('criterion','acceptance','anchor'):_text(row[key])
        _require(row['failure_decision'] in {'BLOCK','NEEDS_REVIEW'},'E1_CLOSEOUT_VISUAL_FAILURE')
    receipt=json.loads((ROOT/'data/generated/campaign_e1_diagnostics/catalog_import_verification_20261015.json').read_text())
    _require(package['visual_specimens']==receipt['gcs_pins'],'E1_CLOSEOUT_VISUAL_PIN_CONFLICT')
    expected_index=[]
    _require(type(package['case_artifacts']) is list and len(package['case_artifacts'])==2,'E1_CLOSEOUT_CASE_ARTIFACTS')
    allowed={'config/evaluation/campaign_e1_draft_v02.json','config/evaluation/campaign_e1_brand_cases_v01.json'}
    _require({a['path'] for a in package['case_artifacts']}==allowed,'E1_CLOSEOUT_CASE_ARTIFACTS')
    for artifact in package['case_artifacts']:
        _exact(artifact,'path sha256');path=ROOT/artifact['path'];data=path.read_bytes()
        _require(hashlib.sha256(data).hexdigest()==artifact['sha256'],'E1_CLOSEOUT_CASE_HASH_CONFLICT')
        for case in json.loads(data)['cases']:
            expected_index.append(dict(case_id=case['case_id'],artifact=artifact['path'],
                label_scope='ISOLATED_RULE_HANDLING' if case.get('case_type') in ('CONFORMING_HANDLING','VIOLATION') else 'SCENARIO_HANDLING',
                expected_decision=case['expected_decision'],rule_ids=case['rule_ids']))
    _require(package['case_index']==sorted(expected_index,key=lambda c:c['case_id']),'E1_CLOSEOUT_CASE_INDEX_CONFLICT')
    extras=package['supplemental_cases'];_require(type(extras) is list and len(extras)==16,'E1_CLOSEOUT_SUPPLEMENTAL_COVERAGE')
    ids=set();multi=0
    for case in extras:
        _exact(case,'case_id canonical_sku country channel stage objective audience rule_ids turns evidence_state '
            'expected_handling expected_decision decision_target evidence_references label_status provenance')
        _require(type(case['case_id']) is str and case['case_id'] not in ids and case['label_status']=='PENDING_REVIEW'
            and case['canonical_sku'] in {'CI-SKU-0021','CI-SKU-0022','CI-SKU-0023'}
            and case['stage'] in {s.value for s in Stage} and case['audience'] in {'NEW_35_PLUS','RETURNING_35_PLUS'}
            and case['expected_decision'] in {d.value for d in Decision},'E1_CLOSEOUT_SUPPLEMENTAL_PINS')
        ids.add(case['case_id']);_text(case['evidence_state']);_text(case['expected_handling'])
        _require(set(case['rule_ids'])<=set(RULE_IDS) and bool(case['rule_ids'])
            and bool(case['evidence_references']),'E1_CLOSEOUT_SUPPLEMENTAL_PINS')
        turns=case['turns'];_require(type(turns) is list and len(turns)%2==1,'E1_CLOSEOUT_TURN_ORDER')
        for i,turn in enumerate(turns):
            _exact(turn,'role text');_text(turn['text']);_require(turn['role']==('USER' if i%2==0 else 'ASSISTANT'),'E1_CLOSEOUT_TURN_ORDER')
        multi+=len(turns)>1
        _require(case['decision_target'] in {'REQUESTED_CONTENT_OR_ARTIFACT','IDENTITY_RESPONSE_ONLY'}
            and (case['expected_decision']!='PASS' or case['decision_target']=='IDENTITY_RESPONSE_ONLY'),
            'E1_CLOSEOUT_PASS_SCOPE')
    _require(multi>=3 and {(r['country'],r['channel']) for r in extras}=={(c,ch) for c in COUNTRIES for ch in CHANNELS},
        'E1_CLOSEOUT_SUPPLEMENTAL_COVERAGE')
    for key in ('review_requirements','scope_limits'):
        _require(type(package[key]) is list and len(package[key])>=3,'E1_CLOSEOUT_REVIEW_LIMITS')
        for value in package[key]:_text(value)
    return dict(status='READY_FOR_HUMAN_REVIEW',criteria_count=47,proposed_label_count=len(expected_index)+len(extras),
        market_channel_dispositions=16,visual_rubric_items=8,supplemental_multi_turn_cases=multi,
        review_sha256=hashlib.sha256(raw).hexdigest(),human_review_recorded=False,e1_complete=False,
        serving_enabled=False,model_quality_measured=False)


def load(path=DEFAULT):
    with Path(path).open('rb') as source:raw=source.read(MAX_BYTES+1)
    _require(len(raw)<=MAX_BYTES,'E1_CLOSEOUT_TOO_LARGE')
    return validate(json.loads(raw,object_pairs_hook=_pairs,parse_constant=_reject_constant))


def validate_review(package,review):
    """Bind recorded conversation signoff; not authentication or runtime authority."""
    result=validate(package)
    _exact(review,'schema_version reviewed_on review_source reviewer_identity response review_sha256 criteria_count '
        'label_count scope scientific_claim_approval unreviewed_market_compliance claim_use_authorized '
        'final_campaign_approved legal_clearance serving_enabled')
    expected=dict(schema_version='campaign-e1-scoped-review-v1',reviewed_on='2026-10-15',
        review_source='EXPLICIT_CHAT_USER_RESPONSE',reviewer_identity='CONVERSATION_USER_NOT_A_RUNTIME_ROLE_ASSIGNMENT',
        response='Approve the E1 criteria and benchmark labels within the stated scope',
        review_sha256=result['review_sha256'],criteria_count=47,label_count=159,
        scope='E1_CRITERIA_AND_BENCHMARK_LABELS_ONLY',scientific_claim_approval='HELD',unreviewed_market_compliance='HELD',
        claim_use_authorized=False,final_campaign_approved=False,legal_clearance=False,serving_enabled=False)
    _require(review==expected and type(review['criteria_count']) is int and type(review['label_count']) is int
        and all(review[key] is False for key in ('claim_use_authorized','final_campaign_approved','legal_clearance','serving_enabled')),
        'E1_REVIEW_SCOPE_OR_DIGEST_CONFLICT')
    return result|dict(status='SCOPED_E1_COMPLETE',human_review_recorded=True,e1_complete=True,
        review_record_sha256=hashlib.sha256(_encoded(review)).hexdigest(),runtime_authority_granted=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--path',type=Path,default=DEFAULT)
    args=parser.parse_args()
    with args.path.open('rb') as source:raw=source.read(MAX_BYTES+1)
    _require(len(raw)<=MAX_BYTES,'E1_CLOSEOUT_TOO_LARGE')
    package=json.loads(raw,object_pairs_hook=_pairs,parse_constant=_reject_constant)
    review=json.loads(REVIEW.read_text(),object_pairs_hook=_pairs,parse_constant=_reject_constant)
    print(json.dumps(validate_review(package,review),indent=2))


if __name__=='__main__':main()
