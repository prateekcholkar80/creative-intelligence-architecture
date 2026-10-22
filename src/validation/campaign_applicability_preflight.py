"""Read-only native applicability audit. ADC inspection is not serving authorization."""
import argparse,json
from pathlib import Path
from datetime import datetime,timezone
from dataclasses import replace
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_evaluator_store import EvaluatorStore,canonical
from src.application.campaign_evaluation_source_applicability import interpretation_source_applicability
from src.application.campaign_planner_projection import parse_json
from src.application.campaign_stage_applicability import stage_applicability,INTENT_CONTRACT
from src.application.campaign_evidence_coverage import SQL as COVERAGE_SQL,decode_catalog,project_coverage
from src.application.campaign_structured_evidence import SQL as STRUCTURED_SQL,FIELDS,decode_rows
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,read_policy
from src.application.campaign_quality_product_reader import read_product_fact
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.campaign_clarification_prompts import definitions
from src.application.campaign_semantic_checks import APPLICABILITY_VERSION
from src.application.campaign_full_quality_checks import FullQualityEvaluator
from src.validation.campaign_semantic_benchmark import load_cases
from src.application.sharing_jobs import matches_sharing_job
from src.application.sharing_publication_codec import normalize
from src.application.workflow_contract import _require
from src.application.campaign_acceptance_contract import _hash
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

STAGE='aeae7b910d0cc250a649186e44949f09cf35d95a4023b20ad2199acbaf943d57'


def native_read(client,sql,parameters):
    cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
        maximum_bytes_billed=512*1024**2,job_timeout_ms=60000,query_parameters=parameters)
    job_id='evaluation_applicability_read_'+uuid4().hex
    job=client.query(sql,job_id=job_id,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
    rows=tuple(normalize(r) for r in job.result(timeout=60,max_results=2,retry=None,job_retry=None))
    _require(len(rows)==1 and matches_sharing_job(job,job_id=job_id,sql=sql,config=cfg)
        and job.state=='DONE' and job.error_result is None,'APPLICABILITY_NATIVE_READ_UNVERIFIED')
    return rows[0],job_id


def policy_request_from_stage(intent):
    prefix,separator,raw=intent['content'].partition('\n')
    _require(separator and prefix=='Evidence-intent context (data):','APPLICABILITY_ORIGINAL_TEMPLATE')
    value=parse_json(raw)
    return ClarificationPolicyRequest('campaign-evidence-clarification','draft-v1',value['policy_sha256'])


def audit(client,stage_record_id=STAGE):
    store=EvaluatorStore(client);stored=store._read('intents',stage_record_id)
    _require(stored is not None,'APPLICABILITY_ORIGINAL_INTENT_MISSING')
    intent=dict(stored);intent.pop('reservation_nonce',None);outcome=store.recover(intent)
    _require(type(outcome) is dict and outcome.get('state')=='SUCCEEDED','APPLICABILITY_ORIGINAL_OUTCOME_UNAVAILABLE')
    scope=intent['source']['scope'];source=intent['source'];profile=json.loads(intent['profile']['settings_json'])
    catalog,_=load_cases();u02=next(c for c in json.loads(catalog.criteria_json) if c['rule_id']=='U02')
    _require(u02['stage_applicability']['INTERPRETATION']['trigger']=='Whenever producing a campaign concept deliverable.'
        and u02['failure_handling']=='Revise incomplete draft.','APPLICABILITY_REVIEWED_TRIGGER_CHANGED')
    stage=stage_applicability('INTERPRETATION',outcome['response_text'],INTENT_CONTRACT)
    _require(stage['u02']['applicability']=='NOT_APPLICABLE','APPLICABILITY_OUTPUT_KIND_UNRESOLVED')
    row,coverage_job=native_read(client,COVERAGE_SQL,[bigquery.ScalarQueryParameter('scope_sha256','STRING',_hash(scope))])
    states,coverage_sha=decode_catalog(row)
    structured={}
    for kind in FIELDS:
        raw,job_id=native_read(client,STRUCTURED_SQL,[bigquery.ScalarQueryParameter('sku','STRING',scope['sku']),bigquery.ScalarQueryParameter('kind','STRING',kind)])
        records,revocations,sha=decode_rows(raw,sku=scope['sku'],kind=kind,now=raw['observed_at'])
        structured[kind]=dict(record_count=len(records),revocation_count=len(revocations),source_sha256=sha,native_job_id=job_id,
            disposition='ABSENT' if not records else 'REQUIRES_INDEPENDENT_CURRENT_REVIEW',use_authorized=False)
    # Fresh native captures after inventory, not re-dated historical observations.
    policy=read_policy(client,policy_request_from_stage(intent))
    product=read_product_fact(client,scope['sku'])
    registered=BigQueryPromptRegistryReader(client).read_exact('5933813566455939072','1');snapshot=registered.state.head.snapshot
    definitions(policy,checked_at=datetime.now(timezone.utc))[0].verify(snapshot)
    _require(snapshot.snapshot_sha256==source['prompt_snapshot_sha256']==profile['source_snapshot_sha256']
        and product.fact.fields_json==source['product_fields_json'] and policy.record_sha256==source['policy_record_sha256'],
        'APPLICABILITY_ORIGINAL_SOURCE_CHANGED')
    checks=interpretation_source_applicability(response_text=outcome['response_text'],scope=scope,
        product=product,policy=policy,checked_at=datetime.now(timezone.utc))
    inventory=[]
    for rule in json.loads(policy.policy_canonical_json)['requirements']:
        if rule['evidence_kind'] in structured:
            disposition=structured[rule['evidence_kind']]['disposition'];fields=[]
        else:
            disposition,fields=project_coverage(states,rule,scope,formula_version=scope['formula_version'],audience=scope['audience'],now=row['observed_at'])
        inventory.append(dict(requirement_id=rule['requirement_id'],evidence_kind=rule['evidence_kind'],
            trigger_features=rule['when_any'],required_fields=rule['required_fields'],native_disposition=disposition,
            available_fields=list(fields),applies_to_actual_intent=bool(set(checks['features'])&set(rule['when_any'])),use_authorized=False))
    v5=replace(catalog,contract_version=APPLICABILITY_VERSION)
    return dict(status='NATIVE_STAGE_APPLICABILITY_CHECKS_VERIFIED_DOWNSTREAM_HOLDS_RETAINED',
        original_stage_record_id=stage_record_id,original_response_sha256=outcome['response_sha256'],
        stage_applicability=stage,source_applicability=checks,reviewed_u02_trigger=u02['stage_applicability']['INTERPRETATION'],
        all_configured_source_requirements=inventory,permission_catalog_native_job_id=coverage_job,permission_catalog_sha256=coverage_sha,
        structured_inventory=structured,v5_evaluator_sha256=FullQualityEvaluator(v5,v5.sha256,lambda *a:None).evaluator_sha256,
        current_prompt_status=registered.state.head.status.value,original_result_modified=False,model_calls=0,
        qualifying_pipeline_run_allowed=False,qualifying_full_pipeline_benchmark='0/159',release_allowed=False,
        limits=['Read-only ADC audit is not IAP/serving authority.','Independent source-use permissions and report/image original bytes remain required for applicable downstream assertions.','No model rescore or qualifying pipeline execution performed.'])


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    _require(not args.output.exists(),'APPLICABILITY_REPORT_EXISTS')
    report=dict(status='RUNNING',model_calls=0,release_allowed=False)
    try:
        with bigquery.Client(project=PROJECT,location=LOCATION) as client:report=audit(client)
    except BaseException as error:
        report.update(status='FAILED',safe_error_type=type(error).__name__);raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':main()
