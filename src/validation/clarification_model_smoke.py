"""Bounded DRAFT-prompt evaluation only; no serving, approval or promotion.

Actual model observations on proposed synthetic labels are diagnostic. They are
not the reviewed E1 benchmark, scientific evidence or a policy approval grant.
An existing output cannot be resumed/replayed: an uncertain call is not retried.
"""
import argparse,hashlib,json
from datetime import datetime,timezone
from pathlib import Path
from google.cloud import bigquery
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,read_policy
from src.application.campaign_clarification_prompts import definitions
from src.application.campaign_evidence_gaps import GapPlan,canonical,validate_questions
from src.application.campaign_evidence_dispatch import provider_observation
from src.application.campaign_planner_projection import parse_json
from src.application.interpreter_serving_composition import provider_call
from src.application.operation_budget import worker_operation_scope
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.prompt_runtime_contract import render_prompt
from src.application.workflow_contract import _require
from src.common.gemini_generation import complete_response_text
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION
import time

FIXTURE=Path('config/evaluation/clarification_model_smoke_v1.json')
TARGETS=(('5933813566455939072','1'),('7384254121445949440','1'))
POLICY=ClarificationPolicyRequest('campaign-evidence-clarification','draft-v1',
    '97497414d6aed432b013ed5565ef717b3705198334d2c2e68c3c5c92012620df')


def inputs(case,definition,policy):
    value=json.loads(policy.policy_canonical_json)
    if 'text' in case:
        context=dict(user_text=case['text'],selection=dict(sku='CI-SKU-0001',country='US',channel='META',
            audience='NEW',formula_version=None),requirements=value['requirements'],policy_sha256=definition.policy_sha256,
            provenance='SYNTHETIC_EVALUATION_FIXTURE_NOT_PERSISTED_USER_AUTHORITY')
        return {'evidence_intent_context_json':canonical(context)},None
    active=[r for r in value['requirements'] if set(r['when_any'])&set(case['features'])]
    _require(0<len(active)<=value['max_questions'],'EVAL_QUESTION_COVERAGE')
    questions=[dict(requirement_id=r['requirement_id'],fields=r['required_fields'],owner=r['owner'],
        alternative=r['alternative']) for r in active]
    # An explicit synthetic gap plan; never passed to serving or an authority reader.
    plan=GapPlan(canonical(dict(decision='CLARIFY',policy_sha256=definition.policy_sha256,
        question_guidance=value['question_guidance'],questions=questions,
        provenance='SIMULATED_ABSENT_EVIDENCE_NOT_A_RETRIEVAL_OBSERVATION')))
    return {'evidence_gap_plan_json':canonical(plan.question_request())},plan


def grade(case,raw,plan,*,allowed_features=()):
    value=parse_json(raw)
    if plan is not None:
        validate_questions(plan,value)
        return dict(structural_pass=True,feature_match=None,semantic_review='REQUIRED')
    valid=(type(value) is dict and set(value)=={'schema_version','features','formula_version','audience'}
        and value['schema_version']=='campaign-evidence-intent-v1' and value['audience']=='NEW'
        and value['formula_version'] is None and type(value['features']) is list
        and all(type(v) is str and v in allowed_features for v in value['features'])
        and len(set(value['features']))==len(value['features']))
    return dict(structural_pass=valid,feature_match=valid and set(value['features'])==set(case['features']),
        semantic_review='PROPOSED_LABEL_NOT_AUTHORIZED_BENCHMARK')


def run(output):
    _require(not output.exists(),'EVAL_ORIGINAL_REPORT_ALREADY_EXISTS')
    fixture=json.loads(FIXTURE.read_text())
    _require(fixture['version']=='clarification-model-smoke-v1' and fixture['label_status']=='PROPOSED_NOT_HUMAN_REVIEWED'
        and len(fixture['intent_cases'])==6 and len(fixture['question_cases'])==3,'EVAL_FIXTURE_SCOPE')
    report=dict(status='RUNNING',checked_at=datetime.now(timezone.utc).isoformat(),fixture_sha256=_hash(fixture),
        label_status=fixture['label_status'],provenance=fixture['provenance'],cases=[],release_allowed=False,
        serving_writes=0,promotions=0,full_e1_cases_executed=0,approval_granted=False)
    def save():output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
    save()
    try:
        with bigquery.Client(project=PROJECT,location=LOCATION) as client:
            registry=BigQueryPromptRegistryReader(client)
            for index,cases in enumerate((fixture['intent_cases'],fixture['question_cases'])):
                for case in cases:
                    with worker_operation_scope(time.monotonic()+120):
                        policy=read_policy(client,POLICY)
                        definition=definitions(policy,checked_at=datetime.now(timezone.utc))[index]
                        snapshot=registry.read_exact(*TARGETS[index]).state.head.snapshot
                        definition.verify(snapshot)
                        variables,plan=inputs(case,definition,policy)
                        system,prompt=render_prompt(snapshot,variables)
                        record=dict(case_id=case['id'],snapshot_sha256=snapshot.snapshot_sha256,
                            policy_record_sha256=policy.record_sha256,input_sha256=_hash(variables),
                            state='IN_FLIGHT_NO_AUTOMATIC_RETRY')
                        report['cases'].append(record);save()
                        # One request, exact registered generation settings and no automatic retry/tools.
                        response=provider_call(dict(timeout_ms=90000,model_id=snapshot.model_id),system,prompt,
                            json.loads(definition.output_schema_json),snapshot)
                        raw=complete_response_text(response)
                        _require(len(raw.encode())<=65536,'EVAL_OUTPUT_TOO_LARGE')
                        observation=provider_observation(response,raw)
                        record.update(state='OBSERVED',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                            output=parse_json(raw),usage={k:getattr(observation,k) for k in
                                ('input_tokens','output_tokens','cached_tokens','thinking_tokens')},
                            returned_model_id=observation.returned_model_id)
                        save()
                        schema=json.loads(definition.output_schema_json)
                        allowed=() if plan is not None else schema['properties']['features']['items']['enum']
                        record.update(grade(case,raw,plan,allowed_features=allowed))
                        final=registry.read_exact(*TARGETS[index]).state.head.snapshot
                        final_policy=read_policy(client,POLICY)
                        _require(final.snapshot_sha256==snapshot.snapshot_sha256
                            and final_policy.record_sha256==policy.record_sha256,'EVAL_SOURCE_CHANGED')
                        save()
        report['status']='MEASURED_DIAGNOSTIC_NOT_RELEASE_EVALUATION'
    except Exception as error:
        report['status']='INCOMPLETE';report['safe_error_type']=type(error).__name__
        raise
    finally:
        report['observed_cases']=sum(c['state']=='OBSERVED' for c in report['cases'])
        report['structural_passes']=sum(c.get('structural_pass') is True for c in report['cases'])
        report['intent_feature_matches']=sum(c.get('feature_match') is True for c in report['cases'])
        report['question_semantics_measured']=False
        save()
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();report=run(args.output)
    print(json.dumps({k:v for k,v in report.items() if k!='cases'},indent=2))

if __name__=='__main__':main()
