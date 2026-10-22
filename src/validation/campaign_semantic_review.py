"""Technical review of calibration observations; never human/policy approval."""
import argparse,json,hashlib
from collections import Counter
from pathlib import Path
from src.validation.campaign_semantic_benchmark import load_cases
from src.application.campaign_semantic_checks import verify_result,SEMANTIC_RULE_IDS,VERSION
from src.application.workflow_contract import _require,canonical_json_bytes


MAX_REPORT_BYTES=8*1024*1024


def report_digest(report):
    # Workflow's 64 KiB JSON cap is intentionally not relaxed for a diagnostic
    # report containing up to 159 separate bounded observations.
    raw=json.dumps(report,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
    _require(len(raw)<=MAX_REPORT_BYTES,'SEMANTIC_REVIEW_REPORT_SIZE')
    return hashlib.sha256(raw).hexdigest()


def review(report):
    catalog,cases=load_cases();expected={c['case_id']:c for c in cases}
    _require(report['version']==VERSION and report['evaluator_sha256']==catalog.sha256
        and report['review_sha256']==catalog.review_sha256 and report['qualifying'] is False
        and report['release_allowed'] is False and report['full_e1_end_to_end_cases_executed']==0,'SEMANTIC_REVIEW_SCOPE')
    rows=report['cases'];_require(type(rows) is list and len(rows)<=159
        and len({r['case_id'] for r in rows})==len(rows),'SEMANTIC_REVIEW_CASES')
    groups={};rules={};mismatches=[];errors=[];seen=set();assessed=set();handling=[]
    base=json.loads(Path('config/evaluation/campaign_e1_draft_v02.json').read_text())
    authored_types={c['case_id']:c.get('case_type') for c in base['cases']}
    for row in rows:
        entry=expected.get(row['case_id']);_require(entry is not None and row['case_sha256']==entry['case_sha256']
            and row['input_sha256']==entry['case'].sha256,'SEMANTIC_REVIEW_INPUT_CHANGED')
        seen.add(row['case_id'])
        if row['state']!='OBSERVED':
            errors.append(dict(case_id=row['case_id'],state=row['state'],safe_error_type=row.get('safe_error_type'),safe_error=row.get('safe_error')));continue
        result=row['result']
        checked=verify_result(catalog,entry['case'],{k:result[k] for k in ('requested_content_decision','decision_reason','checks')})
        _require(checked==result,'SEMANTIC_REVIEW_RESULT_CHANGED')
        assessed.add(row['case_id']);matched=result['requested_content_decision']==entry['expected_decision']
        key=entry['case'].label_scope
        group=groups.setdefault(key,dict(observed=0,matches=0,confusion={}))
        group['observed']+=1;group['matches']+=int(matched)
        pair=entry['expected_decision']+'->'+result['requested_content_decision'];group['confusion'][pair]=group['confusion'].get(pair,0)+1
        for check in result['checks']:
            counts=rules.setdefault(check['rule_id'],dict(observations=0,decision_matches=0,handling_states={}))
            counts['observations']+=1;counts['decision_matches']+=int(matched)
            counts['handling_states'][check['handling_state']]=counts['handling_states'].get(check['handling_state'],0)+1
        if not matched:
            mismatches.append(dict(case_id=row['case_id'],label_scope=key,rule_ids=list(entry['case'].rule_ids),
                expected=entry['expected_decision'],observed=result['requested_content_decision'],
                candidate=entry['case'].candidate,reason=result['decision_reason'],checks=result['checks']))
        authored=authored_types.get(row['case_id'])
        if authored in {'CONFORMING_HANDLING','VIOLATION'}:
            expected_state='PASS' if authored=='CONFORMING_HANDLING' else 'FAIL'
            handling.append(dict(case_id=row['case_id'],authored_type=authored,expected_handling=expected_state,
                matches=all(c['handling_state']==expected_state for c in result['checks'])))
    missing=sorted(set(expected)-seen);unobserved=sorted(set(expected)-assessed)
    return dict(status='TECHNICAL_CALIBRATION_REVIEW_NOT_APPROVAL',evaluator_sha256=catalog.sha256,
        input_report_sha256=report_digest(report),review_sha256=catalog.review_sha256,
        reviewed_case_count=159,attempted_cases=len(rows),valid_assessments=len(assessed),
        unattempted_cases=missing,unobserved_cases=unobserved,source_report_status=report['status'],
        labels_by_scope=groups,per_rule=rules,mismatches=mismatches,provider_or_validation_errors=errors,
        authored_candidate_type_agreement=dict(count=len(handling),matches=sum(r['matches'] for r in handling),
            scope='AUTHORED_CONFORMING_VIOLATION_TYPES_NOT_A_NEW_GLOBAL_RELEASE_LABEL',disagreements=[r for r in handling if not r['matches']]),
        semantic_components_exercised=sorted(set(rules)&set(SEMANTIC_RULE_IDS)),
        false_pass_dispositions=[m for m in mismatches if m['observed']=='PASS' and m['expected']!='PASS'],
        qualifying=False,approval_request_ready=False,full_end_to_end_execution='0/159',
        limitations=['143 frozen candidate fixtures; 16 request-only scenarios have no observed assistant response.',
            'Duplicated expected-handling text is excluded from supplemental inputs.',
            'Only requested rule IDs are assessed per fixture; global all-47 checks are not inferred.',
            'No rendered-image bytes, live retrieval/reranking execution or current product/claim authority was assessed.',
            'Judge prompt is versioned local diagnostic code, not an ACTIVE registered runtime prompt.',
            'Existing six-component serving checker path and all release holds remain unchanged.'],
        release_allowed=False,human_approval_recorded=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--report',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    _require(not args.output.exists(),'SEMANTIC_REVIEW_EXISTS');raw=args.report.open('rb').read(MAX_REPORT_BYTES+1)
    _require(len(raw)<=MAX_REPORT_BYTES,'SEMANTIC_REVIEW_REPORT_SIZE')
    result=review(json.loads(raw))
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:result[k] for k in ('status','valid_assessments','attempted_cases','approval_request_ready')}))


if __name__=='__main__':main()
