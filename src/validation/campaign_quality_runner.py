"""Repeatable partial E2 deterministic replay, never a model/RAG quality claim.

Gold labels are used only after checks execute. All unexecuted E1 coverage and
unimplemented checks are reported explicitly. No cloud clients or model calls.
"""
import argparse,json,hashlib
from pathlib import Path
from datetime import datetime
from src.application.campaign_quality_checks import SourceFact,FactSpan,QualityCandidate,check_candidate,VERSION,IMPLEMENTED
from src.application.campaign_acceptance_contract import Stage,RULE_IDS
from src.application.workflow_contract import _require
from src.validation.campaign_e1_benchmark import ROOT,MAX_BYTES,_encoded,_pairs,_reject_constant
from src.validation.campaign_e1_closeout import DEFAULT as CLOSEOUT,REVIEW,validate_review

DEFAULT=ROOT/'config/evaluation/campaign_e2_executable_v1.json'


def retrieval_metrics(expected_ids,returned_ids,*,top_k,mode):
    """Exact SKU and semantic ranking have separate metric identities. No ranking."""
    _require(mode in {'EXACT_SKU','SEMANTIC'} and type(top_k) is int and 1<=top_k<=100,'INVALID_E2_RETRIEVAL_PROFILE')
    for values in (expected_ids,returned_ids):
        _require(type(values) is tuple and len(values)<=1000 and all(type(v) is str and v for v in values)
            and len(set(values))==len(values),'INVALID_E2_RETRIEVAL_IDS')
    selected=returned_ids[:top_k];hits=len(set(selected)&set(expected_ids))
    return dict(mode=mode,top_k=top_k,relevant_count=len(expected_ids),returned_at_k=len(selected),hits=hits,
        precision_at_k=hits/len(selected) if selected else None,recall_at_k=hits/len(expected_ids) if expected_ids else None,
        irrelevant_at_k=len(selected)-hits,no_match_expected=not expected_ids,no_match_observed=not selected)


def run(package):
    closeout=json.loads(CLOSEOUT.read_text());review=json.loads(REVIEW.read_text());accepted=validate_review(closeout,review)
    _require(package['version']=='campaign-e2-executable-fixtures-v1' and package['e1_review_sha256']==accepted['review_sha256']
        and package['provenance']=='FROZEN_OFFICIAL_PRODUCT_OBSERVATIONS_WITH_SIMULATED_ACCESS_AND_FRESHNESS'
        and package['label_scope']=='A01_PROJECTION_ONLY_NOT_FULL_E1_CASE','E2_FIXTURE_PROFILE_CONFLICT')
    brand=json.loads((ROOT/'config/evaluation/campaign_e1_brand_cases_v01.json').read_text())
    originals={c['case_id']:c for c in brand['cases']};sources={s['source_id']:s for s in brand['sources']}
    _require(type(package['cases']) is list and len(package['cases'])==6
        and {c['e1_case_id'] for c in package['cases']}=={f'e1-brand-{i}-{suffix}' for i in range(1,4) for suffix in ('identity','size-mismatch')},
        'E2_FIXTURE_COVERAGE')
    now=datetime.fromisoformat(package['clock']);results=[]
    for case in package['cases']:
        payload=dict(case['source']);source_id=payload['source_id'];original=originals[case['e1_case_id']]
        _require(case['source_projection_sha256']==sources[source_id]['projection_sha256']
            and case['candidate']['text']==original['candidate'] and case['candidate']['sku']==original['canonical_sku']
            and case['expected_e1_decision']==original['expected_decision'] and case['projected_rule']=='A01',
            'E2_FIXTURE_E1_PIN_CONFLICT')
        facts=sources[source_id]['facts']
        _require(json.loads(payload['fields_json'])==dict(product_name=facts['product_name'],full_size_ml=str(facts['full_size_ml'])),
            'E2_FIXTURE_SOURCE_FACT_CONFLICT')
        payload['observed_at']=datetime.fromisoformat(payload['observed_at']);payload['valid_until']=datetime.fromisoformat(payload['valid_until'])
        source=SourceFact(**payload);data=dict(case['candidate']);data['stage']=Stage(data['stage'])
        data['spans']=tuple(FactSpan(**span) for span in data['spans']);candidate=QualityCandidate(**data)
        # No case ID or expected label is supplied to the implementation.
        report=check_candidate(candidate,(source,),checked_at=now,not_before=now)
        observed=next(c for c in report.checks if c.rule_id==case['projected_rule'])
        results.append(dict(e1_case_id=case['e1_case_id'],rule_id=observed.rule_id,observed_state=observed.state.value,
            expected_projected_state=case['projected_expected_state'],projection_matches=observed.state.value==case['projected_expected_state'],
            global_decision=report.decision.value,expected_e1_decision=case['expected_e1_decision'],
            global_matches_e1=report.decision.value==case['expected_e1_decision'],report_sha256=report.sha256,
            unimplemented_rule_count=sum(c.reason_code=='CHECKER_NOT_IMPLEMENTED' for c in report.checks)))
    return dict(status='PARTIAL_E2_BASELINE',checker_version=VERSION,fixture_sha256=hashlib.sha256(_encoded(package)).hexdigest(),
        e1_review_sha256=accepted['review_sha256'],results=results,rule_projection_cases=6,
        rule_projection_matches=sum(r['projection_matches'] for r in results),global_e1_matches=sum(r['global_matches_e1'] for r in results),
        full_e1_cases_executed=0,reviewed_e1_case_count=159,implemented_component_rule_ids=list(IMPLEMENTED),
        component_scope_only=True,
        unimplemented_rule_ids=[r for r in RULE_IDS if r not in IMPLEMENTED],retrieval_execution='NOT_RUN',retrieval_quality_metrics=None,
        model_quality_measured=False,live_source_eligibility_verified=False,release_allowed=False,e2_complete=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path);args=parser.parse_args()
    with DEFAULT.open('rb') as source:raw=source.read(MAX_BYTES+1)
    _require(len(raw)<=MAX_BYTES,'E2_FIXTURE_TOO_LARGE')
    report=run(json.loads(raw,object_pairs_hook=_pairs,parse_constant=_reject_constant))
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k!='results'},indent=2))


if __name__=='__main__':main()
