"""Measured semantic calibration of reviewed E1 fixtures, never serving qualification.

Gold decisions are held outside model input. No candidate is manufactured from
expected_handling; request-only supplemental cases remain explicitly unobserved.
No local fixture is substituted for current BigQuery product/claim authority.
"""
import argparse,hashlib,json,time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from pathlib import Path
from google import genai
from google.genai import types
from google.cloud import bigquery
from src.application.campaign_semantic_checks import SemanticCatalog,SemanticInput,prompt,output_schema,verify_result,VERSION,SEMANTIC_RULE_IDS
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import _require,canonical_json_bytes
from src.application.campaign_planner_projection import parse_json
from src.application.campaign_evidence_dispatch import provider_observation
from src.application.operation_budget import worker_operation_scope,operation_timeout
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.campaign_policy_reader import read_policy
from src.common.gemini_generation import complete_response_text
from src.validation.campaign_e1_closeout import DEFAULT,REVIEW,validate_review
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

POLICY=dict(policy_id='naked-and-thriving-campaign-acceptance',version='draft-v0.2',
    specification_sha256='b9734cee1d1b1d6cfcac6ec18b5680363ad6d2acd18adfac8f06180833dd08c3',
    header_sha256='483fe36e3e408fd2febaf464033c1aa73e92b15e0e40ab0882d4212b0ce72ac4')


def load_cases():
    package=json.loads(DEFAULT.read_text());review=json.loads(REVIEW.read_text());validate_review(package,review)
    keys=('rule_id','acceptance_rule','failure_handling','stage_applicability','required_evidence')
    catalog=SemanticCatalog(review['review_sha256'],canonical_json_bytes([{k:c[k] for k in keys} for c in package['criteria']]).decode())
    base=json.loads(Path('config/evaluation/campaign_e1_draft_v02.json').read_text())
    brand=json.loads(Path('config/evaluation/campaign_e1_brand_cases_v01.json').read_text())
    labels={c['case_id']:c for c in package['case_index']}
    sources={s['source_id']:s for s in brand['sources']};products={p['canonical_sku']:p for p in brand['products']}
    cases=[]
    for raw in base['cases']+brand['cases']+package['supplemental_cases']:
        case_id=raw['case_id'];is_supp=case_id not in labels
        evidence=raw.get('evidence')
        if evidence is None:
            evidence=[]
            product=products.get(raw.get('canonical_sku'))
            for source_id in product['source_ids'] if product else ():
                s=sources[source_id]
                evidence.append(dict(evidence_id=source_id,kinds=['WEBSITE_SUMMARY'] if 'SUMMARY' in s['source_kind'] else ['PRODUCT_FACT','SOURCE_VERSION'],
                    statement=canonical_json_bytes(dict(facts=s['facts'],source_kind=s['source_kind'],formula_verified=s['formula_verified'],
                        claim_approved=s['claim_approved'],projection_sha256=s['projection_sha256'],html_sha256=s.get('html_sha256'),fetched_at=s.get('fetched_at'))).decode(),provenance='FROZEN_WEBSITE_OBSERVATION_NOT_CURRENT_SOURCE_AUTHORITY'))
            if not is_supp and raw.get('evidence_state'):
                evidence.append(dict(evidence_id='authored-scenario-state',kinds=['SCENARIO_DESCRIPTION'],
                    statement=raw['evidence_state'],provenance='AUTHORED_SCENARIO_NOT_RETRIEVAL'))
        normalized=[{k:e[k] for k in ('evidence_id','kinds','statement','provenance')} for e in evidence]
        scope='REQUEST_ONLY_SCENARIO' if is_supp else labels[case_id]['label_scope']
        candidate=None if is_supp else raw.get('candidate')
        selection={k:raw[k] for k in ('canonical_sku','objective','audience','product_slot') if k in raw}
        value=SemanticInput(raw['stage'],raw['country'],raw['channel'],candidate,
            canonical_json_bytes(raw.get('turns',[])).decode(),canonical_json_bytes(normalized).decode(),
            tuple(sorted(raw['rule_ids'])),scope,canonical_json_bytes(selection).decode())
        # Validate every actual model payload before spending on any call.
        prompt(catalog,value)
        cases.append(dict(case_id=case_id,case=value,expected_decision=raw['expected_decision'],
            decision_target=raw.get('decision_target','REVIEWED_CASE_TARGET'),case_sha256=_hash(raw)))
    _require(len(cases)==159 and len({c['case_id'] for c in cases})==159,'SEMANTIC_BENCHMARK_COVERAGE')
    return catalog,sorted(cases,key=lambda c:c['case_id'])


def observe(case,catalog,snapshot):
    system,content=prompt(catalog,case);generation=json.loads(snapshot.generation_config_json)
    _require(generation.pop('automatic_function_calling_disabled') is True,'SEMANTIC_TOOLS_DISABLED')
    level=generation.pop('thinking_level')
    config=types.GenerateContentConfig(**generation,system_instruction=system,response_schema=output_schema(case.rule_ids),
        thinking_config=None if level is None else types.ThinkingConfig(thinking_level=level),
        safety_settings=[types.SafetySetting(**s) for s in json.loads(snapshot.safety_settings_json)],
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        tool_config=types.ToolConfig(function_calling_config=types.FunctionCallingConfig(mode='NONE')))
    with worker_operation_scope(time.monotonic()+100):
        with genai.Client(vertexai=True,project=PROJECT,location=LOCATION,
            http_options=types.HttpOptions(timeout=int(operation_timeout(75)*1000),retry_options=types.HttpRetryOptions(attempts=1))) as client:
            response=client.models.generate_content(model=snapshot.model_id,contents=content,config=config)
        raw=complete_response_text(response);_require(len(raw.encode())<=65536,'SEMANTIC_OUTPUT_SIZE')
        provider=provider_observation(response,raw)
        common=dict(response_sha256=provider.response_sha256,returned_model_id=provider.returned_model_id,
            usage={k:getattr(provider,k) for k in ('input_tokens','output_tokens','cached_tokens','thinking_tokens')})
        try:measured=verify_result(catalog,case,parse_json(raw))
        except Exception as error:
            return dict(state='RESPONSE_REJECTED',safe_error_type=type(error).__name__,safe_error=str(error)[:400],
                rejected_response_text=raw,**common)
        return dict(state='OBSERVED',result=measured,**common)


def run(output,*,limit=159,workers=1,min_interval_seconds=5,prior_observations=None,case_ids=None):
    _require(not output.exists(),'SEMANTIC_REPORT_EXISTS')
    _require(type(limit) is int and 1<=limit<=159 and type(workers) is int and 1<=workers<=2
        and type(min_interval_seconds) is int and 0<=min_interval_seconds<=60,'SEMANTIC_RUN_LIMIT')
    catalog,cases=load_cases()
    if case_ids is not None:
        _require(type(case_ids) is tuple and 0<len(case_ids)<=159 and len(set(case_ids))==len(case_ids)
            and set(case_ids)<={c['case_id'] for c in cases},'SEMANTIC_CASE_SELECTION')
        cases=[c for c in cases if c['case_id'] in case_ids]
    selected=cases[:limit];limit=len(selected)
    report=dict(status='RUNNING',version=VERSION,evaluator_sha256=catalog.sha256,review_sha256=catalog.review_sha256,
        input_provenance='REVIEWED_FIXTURE_CALIBRATION_NOT_LIVE_RAG',prompt_registration='LOCAL_VERSIONED_DIAGNOSTIC_NOT_REGISTERED',
        reviewed_case_count=159,selected_cases=limit,cases=[],started_at=datetime.now(timezone.utc).isoformat(),
        full_e1_end_to_end_cases_executed=0,qualifying=False,source_eligibility_verified=False,release_allowed=False,serving_writes=0,min_interval_seconds=min_interval_seconds,consecutive_capacity_failure_limit=3,
        implementation_sha256={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in
            (Path(__file__),Path('src/application/campaign_semantic_checks.py'))})
    def save():output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    try:
        with bigquery.Client(project=PROJECT,location=LOCATION) as client:
            policy=read_policy(client,**POLICY);registry=BigQueryPromptRegistryReader(client)
            snapshot=registry.read_exact('8077808464061005824','1').state.head.snapshot
            _require(snapshot.model_id=='gemini-3.5-flash','SEMANTIC_MODEL_PROFILE_CHANGED')
            stored={json.loads(text)['rule_id']:json.loads(text) for text in policy.rule_canonical_json}
            _require(all(stored[c['rule_id']]['acceptance_rule']==c['acceptance_rule']
                and stored[c['rule_id']]['failure_handling']==c['failure_handling'] for c in json.loads(catalog.criteria_json)),
                'SEMANTIC_REVIEWED_POLICY_CHANGED')
            report.update(policy_snapshot_sha256=policy.snapshot_sha256,policy_native_job_before=policy.native_job_id,
                requested_model_id=snapshot.model_id,generation_profile_json=snapshot.generation_config_json,
                model_configuration_source_snapshot=snapshot.snapshot_sha256,
                source_snapshot_is_judge_prompt=False)
            if prior_observations is not None:
                raw=prior_observations.read_bytes();_require(len(raw)<=8*1024*1024,'SEMANTIC_PRIOR_SIZE')
                prior=json.loads(raw)
                _require(prior['evaluator_sha256']==catalog.sha256 and prior['review_sha256']==catalog.review_sha256
                    and prior['model_configuration_source_snapshot']==snapshot.snapshot_sha256
                    and prior['generation_profile_json']==snapshot.generation_config_json
                    and prior['policy_snapshot_sha256']==policy.snapshot_sha256
                    and prior['qualifying'] is False and prior['release_allowed'] is False
                    and all(c['state']=='OBSERVED' for c in prior['cases']),'SEMANTIC_PRIOR_NOT_RECOVERABLE')
                inputs={c['case_id']:c for c in selected};reused=set()
                for old in prior['cases']:
                    entry=inputs.get(old['case_id'])
                    _require(entry is not None and old['case_id'] not in reused and old['case_sha256']==entry['case_sha256']
                        and old['input_sha256']==entry['case'].sha256,'SEMANTIC_PRIOR_INPUT_CHANGED')
                    checked=verify_result(catalog,entry['case'],{k:old['result'][k] for k in
                        ('requested_content_decision','decision_reason','checks')})
                    _require(checked==old['result'],'SEMANTIC_PRIOR_RESULT_CHANGED')
                    report['cases'].append(old|dict(observation_origin='PRIOR_COMPLETED_DIAGNOSTIC',
                        prior_report_sha256=hashlib.sha256(raw).hexdigest()))
                    reused.add(old['case_id'])
                report['reused_completed_observations']=len(reused)
                report['prior_report_sha256']=hashlib.sha256(raw).hexdigest()
                selected=[entry for entry in selected if entry['case_id'] not in reused];save()
            next_dispatch=0;capacity_failures=0;capacity_stopped=False
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for offset in range(0,len(selected),workers):
                    if capacity_stopped:break
                    while time.monotonic()<next_dispatch:time.sleep(min(1,next_dispatch-time.monotonic()))
                    chunk=selected[offset:offset+workers];pending=[]
                    for entry in chunk:
                        if pending and min_interval_seconds:time.sleep(min_interval_seconds)
                        row=dict(case_id=entry['case_id'],case_sha256=entry['case_sha256'],input_sha256=entry['case'].sha256,
                            submitted_at=datetime.now(timezone.utc).isoformat(),label_scope=entry['case'].label_scope,candidate_present=entry['case'].candidate is not None,
                            requested_rule_ids=list(entry['case'].rule_ids),state='IN_FLIGHT_NO_AUTOMATIC_RETRY')
                        report['cases'].append(row);save()
                        pending.append((entry,row,pool.submit(observe,entry['case'],catalog,snapshot)))
                    for entry,row,future in pending:
                        try:
                            row.update(future.result())
                            row['expected_decision']=entry['expected_decision']
                            if row['state']=='OBSERVED':row['requested_content_decision_matches']=row['result']['requested_content_decision']==entry['expected_decision']
                            capacity_failures=0
                        except Exception as error:
                            row.update(state='ERROR_OR_UNKNOWN_NO_RETRY',safe_error_type=type(error).__name__,safe_error=str(error)[:400])
                            if getattr(error,'code',None)==429:
                                capacity_failures+=1;next_dispatch=max(next_dispatch,time.monotonic()+30)
                                capacity_stopped=capacity_failures>=3
                            else:capacity_failures=0
                        row['completed_at']=datetime.now(timezone.utc).isoformat()
                        next_dispatch=max(next_dispatch,time.monotonic()+min_interval_seconds)
                        save()
            final=read_policy(client,**POLICY);last=registry.read_exact('8077808464061005824','1').state.head.snapshot
            _require(final.snapshot_sha256==policy.snapshot_sha256 and last.snapshot_sha256==snapshot.snapshot_sha256,'SEMANTIC_SOURCE_CHANGED')
            report['policy_native_job_after']=final.native_job_id
        report['status']='INCOMPLETE_CAPACITY' if capacity_stopped else 'MEASURED_CALIBRATION_NOT_QUALIFYING'
    except BaseException as error:
        report.update(status='INCOMPLETE',safe_error_type=type(error).__name__);raise
    finally:
        observed=[c for c in report['cases'] if c['state']=='OBSERVED']
        report.update(attempted_cases=len(report['cases']),unattempted_cases=limit-len(report['cases']),
            dispatched_in_this_run=sum(c.get('observation_origin')!='PRIOR_COMPLETED_DIAGNOSTIC' for c in report['cases']),
            completed_provider_responses=sum('response_sha256' in c for c in report['cases']),
            observed_cases=len(observed),error_or_unknown_cases=sum(c['state']!='OBSERVED' for c in report['cases']),
            requested_content_decision_matches=sum(c.get('requested_content_decision_matches') is True for c in observed),
            candidate_cases_observed=sum(c['candidate_present'] for c in observed),
            request_only_cases_observed=sum(not c['candidate_present'] for c in observed),
            semantic_components_exercised=sorted({v['rule_id'] for c in observed for v in c['result']['checks']} & set(SEMANTIC_RULE_IDS)),
            finished_at=datetime.now(timezone.utc).isoformat())
        report['usage']={k:dict(known_total=sum(c['usage'][k] for c in report['cases'] if c.get('usage',{}).get(k) is not None),
            unknown_cases=sum(c.get('usage',{}).get(k) is None for c in report['cases'])) for k in ('input_tokens','output_tokens','cached_tokens','thinking_tokens')}
        save()
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true');parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--case-ids',nargs='+');parser.add_argument('--prior-observations',type=Path);parser.add_argument('--limit',type=int,default=159);parser.add_argument('--workers',type=int,default=1);parser.add_argument('--min-interval-seconds',type=int,default=5);args=parser.parse_args()
    _require(args.run,'SEMANTIC_EXPLICIT_RUN');result=run(args.output,limit=args.limit,workers=args.workers,min_interval_seconds=args.min_interval_seconds,prior_observations=args.prior_observations,case_ids=tuple(args.case_ids) if args.case_ids else None)
    print(json.dumps({k:result[k] for k in ('status','observed_cases','error_or_unknown_cases','requested_content_decision_matches')}))


if __name__=='__main__':main()
