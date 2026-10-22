"""Reviewed full-pipeline execution inventory; no fabricated observations or release.

Expected outcomes stay outside execution inputs. Frozen candidate tests retain
scope and cannot be counted as observed stage outputs. This module does not call
providers, manufacture source authority, or publish a qualifying measurement.
"""
import argparse,hashlib,json
from collections import Counter
from pathlib import Path
from src.validation.campaign_semantic_benchmark import load_cases
from src.validation.campaign_e1_closeout import DEFAULT,REVIEW,validate_review
from src.application.campaign_acceptance_contract import RULE_IDS
from src.application.workflow_contract import _require

VERSION='campaign-pipeline-inventory-v1'


def digest(value):
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def compile_inventory():
    package=json.loads(DEFAULT.read_text());review=json.loads(REVIEW.read_text())
    validate_review(package,review)
    catalog,cases=load_cases();entries=[];gold=[]
    for entry in cases:
        case=entry['case'];selection=json.loads(case.selection_json)
        # These are supplied inputs, never expected_handling, evidence_state or
        # authored outcome prose substituted for authoritative retrieval results.
        execution=dict(case_id=entry['case_id'],stage=case.stage,country=case.country,channel=case.channel,
            selection=selection,turns=json.loads(case.turns_json),target_rule_ids=list(case.rule_ids),
            reviewed_label_scope=case.label_scope,
            supplied_candidate=case.candidate,
            supplied_candidate_origin='FROZEN_REVIEWED_FIXTURE' if case.candidate is not None else 'ABSENT',
            fixture_evidence=json.loads(case.evidence_json),fixture_evidence_is_current_authority=False)
        # Supplemental scenario state is deliberately excluded by load_cases.
        # Frozen candidate/evidence bytes are explicit replay material, not a
        # current source capture or a generated output observation.
        required=['CURRENT_ACCESS_AND_SOURCE_CAPTURE','EXACT_PROMPT_POLICY_PROFILE_BINDING',
            'ALL_47_CHECK_RESULTS_AND_APPLICABILITY','OUTPUT_MODALITY_AND_APPLICABILITY_CAPTURE','POST_EXECUTION_AUTHORITY_RECHECK',
            'DURABLE_REQUEST_INVOCATION_PROVIDER_RESPONSE','RECORDED_USAGE_AND_UNKNOWN_USAGE']
        if case.stage in ('GENERATION','REFINEMENT'):
            required+=['ACTUAL_RETRIEVAL_RANKS_AND_SOURCE_PINS']
        if case.stage=='REFINEMENT':required+=['BOUND_PRIOR_OUTPUT_AND_ACTUAL_REFINEMENT']
        if case.stage=='CLARIFICATION':required+=['CURRENT_GAP_AND_PARENT_BINDING']
        blockers=['PIPELINE_STAGE_EXECUTION_NOT_OBSERVED','CURRENT_SOURCE_AUTHORITY_NOT_CAPTURED',
            'ALL_47_INTEGRATED_CHECKS_NOT_MEASURED']
        if case.candidate is not None:blockers.append('FROZEN_CANDIDATE_REPLAY_IS_NOT_STAGE_EXECUTION')
        if case.label_scope not in {'ISOLATED_RULE_HANDLING','GLOBAL_CAMPAIGN_RELEASE','STAGE_END_TO_END'}:
            blockers.append('REVIEWED_SCOPE_REQUIRES_EXPLICIT_QUALIFICATION_MAPPING')
        entries.append(dict(case_id=entry['case_id'],case_sha256=entry['case_sha256'],
            execution_input=execution,execution_input_sha256=digest(execution),
            required_evidence=required,blockers=blockers,qualifying_pipeline_execution=False))
        gold.append(dict(case_id=entry['case_id'],case_sha256=entry['case_sha256'],
            expected_decision=entry['expected_decision'],decision_target=entry['decision_target'],
            label_scope=case.label_scope))
    _require(len(entries)==159 and len({e['case_id'] for e in entries})==159,'PIPELINE_BENCHMARK_CASE_COVERAGE')
    source_hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in
        (DEFAULT,REVIEW,Path('config/evaluation/campaign_e1_draft_v02.json'),Path('config/evaluation/campaign_e1_brand_cases_v01.json'))}
    return dict(schema_version=VERSION,review_sha256=review['review_sha256'],
        evaluator_sha256=catalog.sha256,criterion_ids=list(RULE_IDS),source_artifact_sha256=source_hashes,
        execution_cases=entries,scoring_labels=gold)


def readiness(inventory):
    _require(inventory['schema_version']==VERSION,'PIPELINE_INVENTORY_VERSION')
    entries=inventory['execution_cases'];labels=inventory['scoring_labels']
    _require([e['case_id'] for e in entries]==[e['case_id'] for e in labels]
        and len(entries)==159,'PIPELINE_INVENTORY_COVERAGE')
    for entry in entries:
        _require(digest(entry['execution_input'])==entry['execution_input_sha256']
            and entry['qualifying_pipeline_execution'] is False,'PIPELINE_INPUT_CHANGED')
    return dict(status='PIPELINE_EXECUTION_REQUIRED',version=VERSION,inventory_sha256=digest(inventory),
        review_sha256=inventory['review_sha256'],evaluator_sha256=inventory['evaluator_sha256'],
        case_count=len(entries),stages=dict(Counter(e['execution_input']['stage'] for e in entries)),
        reviewed_label_scopes=dict(Counter(e['execution_input']['reviewed_label_scope'] for e in entries)),
        frozen_candidate_cases=sum(e['execution_input']['supplied_candidate'] is not None for e in entries),
        request_only_cases=sum(e['execution_input']['supplied_candidate'] is None for e in entries),
        blocker_counts=dict(sorted(Counter(b for e in entries for b in e['blockers']).items())),
        full_qualifying_pipeline_cases_executed=0,qualifying=False,release_allowed=False,
        model_calls=0,cloud_mutations=0,
        rendered_artifact_qualification='Separate actual artifact observations required when an artifact is produced; text identity fixtures do not assert pixel fidelity.',
        qualification_scope_note='All 159 reviewed cases retained; no isolated or scenario label is promoted into a stage/release label.')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    _require(not args.output.exists(),'PIPELINE_INVENTORY_REPORT_EXISTS')
    inventory=compile_inventory();report=readiness(inventory)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(dict(readiness=report,inventory=inventory),indent=2,ensure_ascii=False)+'\n')
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
