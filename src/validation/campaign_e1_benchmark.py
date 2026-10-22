"""Validate the review-only E1 draft package, without evaluating model quality.

This artifact is not an approved policy, a serving fallback or permission to use
synthetic facts. No client creation, model invocation or BigQuery mutation occurs.
"""
import argparse
import hashlib
import json
from pathlib import Path
from src.application.campaign_acceptance_contract import RULE_IDS,Stage,Decision
from src.application.workflow_contract import _require,ContractError
from src.ingestion.configure_campaign_acceptance import manifest

ROOT=Path(__file__).resolve().parents[2]
DEFAULT=ROOT/'config/evaluation/campaign_e1_draft_v02.json'
MAX_BYTES=2*1024*1024
STAGES=[stage.value for stage in Stage]
CHANNELS={'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'}
COUNTRIES=['AU','CA','GB','US']
DIRECTIVES=dict(version='user-direction-2026-10-15',ai_people='PROHIBITED',
    before_after_imagery='PROHIBITED',efficacy_changing_retouching='PROHIBITED',
    authority='EXPLICIT_USER_DIRECTION',serving_configuration_updated=False)


def _encoded(package):
    # A review package contains many cases. Keep its separate bound rather than
    # relaxing the 64-KiB runtime record serializer.
    try:encoded=json.dumps(package,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
    except (TypeError,ValueError,UnicodeError,RecursionError):raise ContractError('INVALID_E1_JSON') from None
    _require(len(encoded)<=MAX_BYTES,'E1_PACKAGE_TOO_LARGE')
    return encoded


def _text(value):
    _require(type(value) is str and bool(value.strip()) and len(value.encode())<=8000,'INVALID_E1_TEXT')


def _unique(values):
    _require(type(values) is list and bool(values) and all(type(v) is str for v in values)
        and len(values)==len(set(values)),'INVALID_E1_LIST')


def _exact(value,keys):
    _require(type(value) is dict and set(value)==set(keys.split()),'INVALID_E1_SHAPE')


def validate_package(package):
    _exact(package,'schema_version package_id version status approved_by approved_at serving_enabled '
        'specification_sha256 source_policy_version label_semantics geography_status product_slots pending_decisions '
        'criteria cases benchmark_countries deferred_regions language brand_directives source_documents')
    encoded=_encoded(package)
    _require(package['schema_version']=='campaign-e1-draft-v1' and package['package_id']=='naked-and-thriving-e1'
        and package['version']=='draft-v0.2' and package['source_policy_version']=='draft-v0.1'
        and package['status']=='DRAFT' and package['approved_by'] is None and package['approved_at'] is None
        and package['serving_enabled'] is False,'E1_DRAFT_ONLY')
    _require(package['brand_directives']==DIRECTIVES and package['brand_directives']['serving_configuration_updated'] is False
        and package['benchmark_countries']==COUNTRIES
        and package['deferred_regions']==['EUROPE'] and package['language']=='ENGLISH'
        and package['geography_status']=='USER_SELECTED_BENCHMARK_COUNTRIES_OVERLAYS_PENDING','E1_USER_SCOPE_CHANGED')
    source=manifest();definitions={r['rule_id']:json.loads(r['payload_canonical_json']) for r in source[1:]}
    _require(package['specification_sha256']==source[0]['specification_sha256'],'E1_SPECIFICATION_CONFLICT')
    _text(package['label_semantics']);_unique(package['pending_decisions'])
    _require(type(package['source_documents']) is list and len(package['source_documents'])==1,'INVALID_E1_SOURCE')
    document=package['source_documents'][0]
    _exact(document,'source_id filename sha256 slide_count usage')
    _require(document==dict(source_id='holiday-2025-playbook',filename='Holidays 2025 - Campaign Playbook.pptx',
        sha256='dc656614f6c28fddad6ec7bea457e09ad480e4e13befea15355f9052c56f0f3e',slide_count=135,
        usage='PRODUCT_NAME_SELECTION_ONLY_NOT_CURRENT_CLAIM_OR_OFFER_APPROVAL'),'E1_PLAYBOOK_SOURCE_CONFLICT')
    names=['Renew Resurfacing Night Serum','Rejuvenate Restorative Face Oil','Prevent Anti-Aging Face Oil']
    slots=package['product_slots']
    _require(type(slots) is list and len(slots)==3,'INVALID_E1_PRODUCT_SLOTS')
    for i,(slot,name) in enumerate(zip(slots,names),1):
        _exact(slot,'slot_id brand_product_name canonical_sku formula_version evidence_references mapping_status')
        _require(slot['slot_id']==f'product-{i}' and slot['brand_product_name']==name and slot['canonical_sku'] is None
            and slot['formula_version'] is None and slot['mapping_status']=='NAME_SELECTED_FROM_PLAYBOOK_MAPPING_PENDING'
            and slot['evidence_references']==[dict(source_id=document['source_id'],sha256=document['sha256'],slide=98,exact_text=name)],
            'E1_UNVERIFIED_PRODUCT_MAPPING')
    criteria=package['criteria']
    _require(type(criteria) is list and len(criteria)==47,'E1_RULE_COVERAGE_REQUIRED')
    ids=[]
    for criterion in criteria:
        _exact(criterion,'rule_id area acceptance_rule failure_handling stages applicability_condition required_evidence '
            'pass_assertion negative_assertion proposed_negative_decision review_status checker_status brand_override')
        rid=criterion['rule_id'];_require(type(rid) is str and rid in definitions,'INVALID_E1_RULE')
        original=definitions[rid];ids.append(rid)
        _require(all(criterion[k]==original[k] for k in ('area','acceptance_rule','failure_handling'))
            and criterion['stages']==STAGES and criterion['review_status']=='PENDING_REVIEW'
            and criterion['checker_status']=='NOT_IMPLEMENTED','E1_CRITERIA_SOURCE_OR_STATUS_CONFLICT')
        for key in ('applicability_condition','pass_assertion','negative_assertion'):_text(criterion[key])
        _unique(criterion['required_evidence'])
        _require(type(criterion['proposed_negative_decision']) is str and criterion['proposed_negative_decision'] in {'CLARIFY','BLOCK','NEEDS_REVIEW'},'INVALID_E1_NEGATIVE_DECISION')
        if rid=='C07':_text(criterion['brand_override'])
        else:_require(criterion['brand_override'] is None,'INVALID_E1_BRAND_OVERRIDE')
    _require(tuple(ids)==RULE_IDS,'E1_RULE_COVERAGE_REQUIRED')
    cases=package['cases'];_require(type(cases) is list and 1<=len(cases)<=512,'INVALID_E1_CASE_COUNT')
    case_ids=[];coverage={rid:set() for rid in RULE_IDS};stages=set();channels=set();countries=set();decisions=set();products=set();multi=0
    for case in cases:
        _exact(case,'case_id case_type rule_ids stage channel country product_slot objective audience provenance label_status '
            'evidence turns candidate expected_decision expected_evidence_ids expected_reason modality rendered_artifact_verified')
        _text(case['case_id']);case_ids.append(case['case_id']);_unique(case['rule_ids'])
        _require(case['rule_ids']==sorted(case['rule_ids']) and set(case['rule_ids'])<=set(RULE_IDS),'INVALID_E1_CASE_RULES')
        _require(all(type(case[key]) is str for key in ('case_type','stage','channel','country','product_slot','expected_decision'))
            and case['case_type'] in {'CONFORMING_HANDLING','VIOLATION','MULTI_TURN','PLAYBOOK_NAME_ONLY','USER_BRAND_PROHIBITION'}
            and case['stage'] in STAGES and case['channel'] in CHANNELS and case['country'] in COUNTRIES
            and case['product_slot'] in {s['slot_id'] for s in slots} and case['label_status']=='PROPOSED'
            and case['expected_decision'] in {d.value for d in Decision}
            and case['modality']=='TEXT_DESCRIPTION_ONLY' and case['rendered_artifact_verified'] is False
            and case['audience']=='NEW_AND_RETURNING_35_PLUS','INVALID_E1_CASE_SCOPE')
        _text(case['objective']);_text(case['candidate']);_text(case['expected_reason'])
        provenance=case['provenance'];_require(type(provenance) is str and provenance in {'SYNTHETIC_E1_FIXTURE','USER_PLAYBOOK_TEXT'},'E1_PROVENANCE_REQUIRED')
        _require((provenance=='USER_PLAYBOOK_TEXT')==(case['case_type']=='PLAYBOOK_NAME_ONLY'),'E1_PROVENANCE_REQUIRED')
        evidence=case['evidence'];_require(type(evidence) is list and 1<=len(evidence)<=32,'INVALID_E1_EVIDENCE')
        evidence_ids=[]
        for record in evidence:
            extra=' source_id source_sha256 slide exact_text' if provenance=='USER_PLAYBOOK_TEXT' else ''
            _exact(record,'evidence_id kinds statement provenance'+extra)
            _text(record['evidence_id']);evidence_ids.append(record['evidence_id']);_unique(record['kinds']);_text(record['statement'])
            _require(record['provenance']==provenance,'E1_PROVENANCE_REQUIRED')
            if provenance=='USER_PLAYBOOK_TEXT':
                name=names[int(case['product_slot'][-1])-1]
                _require(record['source_id']==document['source_id'] and record['source_sha256']==document['sha256']
                    and type(record['slide']) is int and record['slide']==98 and record['exact_text']==name
                    and name in record['statement'],'E1_PLAYBOOK_EVIDENCE_CONFLICT')
        _unique(evidence_ids);_unique(case['expected_evidence_ids'])
        _require(set(case['expected_evidence_ids'])<=set(evidence_ids),'E1_EXPECTED_EVIDENCE_MISSING')
        turns=case['turns'];_require(type(turns) is list and 1<=len(turns)<=16,'INVALID_E1_TURNS')
        for i,turn in enumerate(turns):
            _exact(turn,'role text');_text(turn['text'])
            _require(turn['role']==('USER' if i%2==0 else 'ASSISTANT'),'INVALID_E1_TURN_ORDER')
        _require(len(turns)%2==1,'INVALID_E1_TURN_ORDER');multi+=len(turns)>1
        for rid in case['rule_ids']:coverage[rid].add(case['case_type'])
        if case['case_type']=='USER_BRAND_PROHIBITION':
            _require(case['rule_ids']==['C07'] and case['expected_decision']=='BLOCK','E1_BRAND_PROHIBITION_NOT_BLOCKED')
        stages.add(case['stage']);channels.add(case['channel']);countries.add(case['country']);decisions.add(case['expected_decision']);products.add(case['product_slot'])
    _require(case_ids==sorted(set(case_ids)),'E1_DUPLICATE_OR_UNORDERED_CASES')
    _require(all({'CONFORMING_HANDLING','VIOLATION'}<=types for types in coverage.values())
        and stages==set(STAGES) and channels==CHANNELS and countries==set(COUNTRIES)
        and decisions=={d.value for d in Decision} and len(products)==3 and multi>=1,'E1_BENCHMARK_COVERAGE_REQUIRED')
    return dict(status='VALID_DRAFT_NOT_READY',milestone='E1',criteria_count=len(criteria),case_count=len(cases),
        multi_turn_case_count=multi,synthetic_case_count=sum(c['provenance']=='SYNTHETIC_E1_FIXTURE' for c in cases),
        playbook_name_case_count=sum(c['provenance']=='USER_PLAYBOOK_TEXT' for c in cases),
        selected_products=names,benchmark_countries=COUNTRIES,specification_sha256=package['specification_sha256'],
        package_sha256=hashlib.sha256(encoded).hexdigest(),
        pending_decisions=package['pending_decisions'],e1_complete=False,serving_enabled=False,
        labels_reviewed=False,model_quality_measured=False,bigquery_updated=False)


def _pairs(pairs):
    result={}
    for key,value in pairs:
        _require(key not in result,'E1_DUPLICATE_JSON_KEY');result[key]=value
    return result


def _reject_constant(_):raise ContractError('INVALID_E1_JSON')


def load_package(path=DEFAULT):
    with Path(path).open('rb') as source:raw=source.read(MAX_BYTES+1)
    _require(len(raw)<=MAX_BYTES,'E1_PACKAGE_TOO_LARGE')
    try:
        data=json.loads(raw,object_pairs_hook=_pairs,parse_constant=_reject_constant)
    except (ValueError,UnicodeError,RecursionError):raise ContractError('INVALID_E1_JSON') from None
    return data,validate_package(data)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--path',type=Path,default=DEFAULT)
    args=parser.parse_args();_,report=load_package(args.path);print(json.dumps(report,indent=2))


if __name__=='__main__':main()
