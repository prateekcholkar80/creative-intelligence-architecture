"""P6 measured-result qualification; no benchmark execution or approval.

These codecs validate supplied records. The protected reader must obtain them
from BigQuery, correlate native jobs and check current access/revocations. No P5
summary or smoke result is silently promoted into a P6 measurement.
"""
from dataclasses import dataclass
from datetime import datetime
import json
from google.cloud import bigquery
from src.application.campaign_acceptance_contract import RULE_IDS, Stage, _hash, _utc
from src.application.campaign_policy_approval_receipt import PolicyApprovalPins, _principal
from src.application.campaign_policy_approval_publisher import PolicyEvaluationQualification
from src.application.prompt_runtime_contract import InvocationPromptBinding
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import _require, _id, _digest, canonical_json_bytes, request_fingerprint
from src.ingestion.setup_workflow_tables import PROJECT

BENCHMARKS='campaign_quality_benchmark_documents'
REPORTS='campaign_quality_measurement_documents'
REVOCATIONS='campaign_quality_measurement_revocations'
MAX_DOCUMENT_BYTES=2097152
MAX_CASES=512
SCHEMAS={BENCHMARKS:(('document_sha256','STRING'),('canonical_json','STRING')),
    REPORTS:(('document_sha256','STRING'),('canonical_json','STRING')),
    REVOCATIONS:(('revocation_id','STRING'),('target_kind','STRING'),('target_sha256','STRING'),('revoked_at','TIMESTAMP'))}


def table_metadata(name):
    _require(name in SCHEMAS,'QUALITY_QUALIFICATION_TABLE')
    table=bigquery.Table(f'{PROJECT}.ci_brand_memory.{name}',schema=[bigquery.SchemaField(n,k,mode='REQUIRED') for n,k in SCHEMAS[name]])
    table.description='campaign-quality-qualification-v1: immutable measurements, never standalone release'
    return table


@dataclass(frozen=True,slots=True)
class QualificationRequirements:
    review_sha256: str
    case_index_sha256: str
    evaluator_sha256: str
    required_stages: tuple[str,...]

    def __post_init__(self):
        for value in (self.review_sha256,self.case_index_sha256,self.evaluator_sha256):_digest(value)
        _require(type(self.required_stages) is tuple and bool(self.required_stages)
            and self.required_stages==tuple(sorted(set(self.required_stages)))
            and set(self.required_stages)<={s.value for s in Stage},'QUALITY_QUALIFICATION_STAGES')


def _document(text,sha):
    _require(type(text) is str and len(text.encode())<=MAX_DOCUMENT_BYTES,'QUALITY_QUALIFICATION_DOCUMENT_SIZE')
    value=json.loads(text)
    _require(type(value) is dict and canonical_json_bytes(value).decode()==text and _hash(value)==sha,
        'QUALITY_QUALIFICATION_DOCUMENT_HASH')
    return value


def qualify_documents(benchmark_json,report_json,*,pins,requirements,invocations,bindings,requests,native_job_id,observed_at,valid_until):
    _require(type(pins) is PolicyApprovalPins and type(requirements) is QualificationRequirements,
        'QUALITY_QUALIFICATION_SETTINGS')
    _id(native_job_id);now=_utc(observed_at)
    benchmark=_document(benchmark_json,pins.benchmark_sha256);report=_document(report_json,pins.evaluation_sha256)
    _require(set(benchmark)=={'schema_version','benchmark_id','version','review_sha256','criterion_ids','evaluator_sha256','required_stages','case_index'}
        and benchmark['schema_version']=='campaign-quality-benchmark-v1'
        and benchmark['review_sha256']==requirements.review_sha256 and benchmark['evaluator_sha256']==requirements.evaluator_sha256
        and benchmark['criterion_ids']==list(RULE_IDS) and benchmark['required_stages']==list(requirements.required_stages),
        'QUALITY_QUALIFICATION_BENCHMARK_BINDING')
    _id(benchmark['benchmark_id']);_id(benchmark['version'])
    index=benchmark['case_index']
    _require(type(index) is list and 0<len(index)<=MAX_CASES and _hash(index)==requirements.case_index_sha256,
        'QUALITY_QUALIFICATION_REVIEWED_CASES')
    expected={};e2e_stages=set()
    for case in index:
        _require(type(case) is dict and set(case)=={'case_id','stage','case_sha256','label_scope','rule_ids','expected_decision','requires_provider','request_sha256'}
            and case['stage'] in requirements.required_stages and case['expected_decision'] in {'PASS','BLOCK','CLARIFY','NEEDS_REVIEW'}
            and case['label_scope'] in {'ISOLATED_RULE_HANDLING','GLOBAL_CAMPAIGN_RELEASE','STAGE_END_TO_END'}
            and type(case['requires_provider']) is bool and type(case['rule_ids']) is list
            and case['rule_ids']==sorted(set(case['rule_ids'])) and bool(case['rule_ids']) and set(case['rule_ids'])<=set(RULE_IDS),
            'QUALITY_QUALIFICATION_CASE_SPEC')
        _id(case['case_id']);_digest(case['case_sha256'])
        if case['requires_provider']:_digest(case['request_sha256'])
        else:_require(case['request_sha256'] is None,'QUALITY_QUALIFICATION_UNEXPECTED_REQUEST')
        _require(case['case_id'] not in expected,'QUALITY_QUALIFICATION_DUPLICATE_CASE')
        expected[case['case_id']]=case
        if case['label_scope']=='STAGE_END_TO_END':
            _require(case['requires_provider'],'QUALITY_QUALIFICATION_E2E_PROVIDER_REQUIRED');e2e_stages.add(case['stage'])
    _require(list(expected)==sorted(expected) and e2e_stages==set(requirements.required_stages),
        'QUALITY_QUALIFICATION_END_TO_END_COVERAGE')
    _require(set(report)=={'schema_version','evaluation_id','workspace_id','benchmark_sha256','evaluator_sha256','policy_bindings',
        'profiles','started_at','completed_at','valid_until','recorded_by','cases'}
        and report['schema_version']=='campaign-quality-measurement-v1' and report['workspace_id']==pins.workspace_id
        and report['benchmark_sha256']==pins.benchmark_sha256 and report['evaluator_sha256']==requirements.evaluator_sha256,
        'QUALITY_QUALIFICATION_REPORT_BINDING')
    _id(report['evaluation_id']);_principal(report['recorded_by'])
    started=_utc(datetime.fromisoformat(report['started_at']));completed=_utc(datetime.fromisoformat(report['completed_at']))
    expires=_utc(datetime.fromisoformat(report['valid_until']))
    _require(started<=completed<=now<min(expires,_utc(valid_until)),'QUALITY_QUALIFICATION_EXPIRED')
    policies=report['policy_bindings'];profiles=report['profiles']
    _require(type(policies) is dict and set(policies)=={'ACCEPTANCE','CLARIFICATION'}
        and type(profiles) is dict and set(profiles)==set(requirements.required_stages), 'QUALITY_QUALIFICATION_CONTEXT')
    for policy in policies.values():
        _require(type(policy) is dict and set(policy)=={'policy_id','policy_version','policy_sha256'},'QUALITY_QUALIFICATION_POLICY')
        _id(policy['policy_id']);_id(policy['policy_version']);_digest(policy['policy_sha256'])
    _require(policies[pins.kind]==dict(policy_id=pins.policy_id,policy_version=pins.policy_version,policy_sha256=pins.policy_sha256),
        'QUALITY_QUALIFICATION_POLICY_CHANGED')
    for profile in profiles.values():
        _require(type(profile) is dict and set(profile)=={'prompt_snapshot_sha256','model_profile_sha256','model_id'},
            'QUALITY_QUALIFICATION_PROFILE')
        _digest(profile['prompt_snapshot_sha256']);_digest(profile['model_profile_sha256']);_id(profile['model_id'])
    _require(pins.stage in profiles and profiles[pins.stage]['prompt_snapshot_sha256']==pins.prompt_snapshot_sha256
        and profiles[pins.stage]['model_profile_sha256']==pins.model_profile_sha256,'QUALITY_QUALIFICATION_PROFILE_CHANGED')
    cases=report['cases']
    _require(type(cases) is list and len(cases)==len(expected)
        and [case.get('case_id') for case in cases if type(case) is dict]==list(expected),'QUALITY_QUALIFICATION_CASE_COVERAGE')
    _require(type(invocations) is tuple and type(bindings) is tuple and len(invocations)<=MAX_CASES and len(bindings)<=MAX_CASES,
        'QUALITY_QUALIFICATION_PROVIDER_LIMIT')
    calls={};prompt_bindings={};request_rows={}
    _require(type(requests) is tuple and len(requests)<=MAX_CASES,'QUALITY_QUALIFICATION_REQUEST_LIMIT')
    for text in requests:
        request=decode_row(text,'requests')
        _require(request['request_id'] not in request_rows and request_fingerprint(request)==request['request_sha256'],
            'QUALITY_QUALIFICATION_REQUEST_CONFLICT')
        request_rows[request['request_id']]=request
    for text in invocations:
        call=decode_row(text,'model_invocations')
        _require(call['invocation_id'] not in calls,'QUALITY_QUALIFICATION_DUPLICATE_PROVIDER');calls[call['invocation_id']]=call
    for row in bindings:
        _require(type(row) is dict,'QUALITY_QUALIFICATION_BINDING')
        value=InvocationPromptBinding(**{k:v for k,v in row.items() if k not in {'schema_version','binding_sha256'}})
        _require(row==value.row() and value.invocation_id not in prompt_bindings,'QUALITY_QUALIFICATION_BINDING_HASH')
        prompt_bindings[value.invocation_id]=value
    used=set()
    for result in cases:
        spec=expected[result['case_id']]
        _require(set(result)=={'case_id','case_sha256','observed_decision','checks','provider'}
            and result['case_sha256']==spec['case_sha256'] and result['observed_decision']==spec['expected_decision'],
            'QUALITY_QUALIFICATION_CASE_MISMATCH')
        checks=result['checks']
        _require(type(checks) is list and len(checks)==len(RULE_IDS)
            and [v.get('rule_id') for v in checks if type(v) is dict]==list(RULE_IDS),'QUALITY_QUALIFICATION_CHECK_COVERAGE')
        for check in checks:
            _require(set(check)=={'rule_id','state','reason_code','evidence_sha256'}
                and check['state'] in {'PASS','FAIL','INCONCLUSIVE','NOT_APPLICABLE'}
                and check['reason_code'] not in {'CHECKER_NOT_IMPLEMENTED','APPLICABILITY_OR_COVERAGE_UNPROVEN'},
                'QUALITY_QUALIFICATION_UNMEASURED_CHECK')
            _id(check['reason_code']);_digest(check['evidence_sha256'])
            if check['rule_id'] in spec['rule_ids']:_require(check['state']!='NOT_APPLICABLE','QUALITY_QUALIFICATION_TARGET_SKIPPED')
        states={check['state'] for check in checks}
        decision=result['observed_decision']
        _require((decision!='PASS' or states<={'PASS','NOT_APPLICABLE'})
            and (decision!='BLOCK' or 'FAIL' in states)
            and (decision not in {'CLARIFY','NEEDS_REVIEW'} or bool(states&{'FAIL','INCONCLUSIVE'})),
            'QUALITY_QUALIFICATION_DECISION_CONFLICT')
        provider=result['provider']
        if not spec['requires_provider']:
            _require(provider is None,'QUALITY_QUALIFICATION_UNEXPECTED_PROVIDER');continue
        _require(type(provider) is dict and set(provider)=={'invocation_id','response_sha256','binding_sha256'},
            'QUALITY_QUALIFICATION_PROVIDER_REQUIRED')
        _id(provider['invocation_id']);_digest(provider['response_sha256']);_digest(provider['binding_sha256'])
        call=calls.get(provider['invocation_id']);binding=prompt_bindings.get(provider['invocation_id'])
        _require(call is not None and binding is not None and call['invocation_id'] not in used,
            'QUALITY_QUALIFICATION_PROVIDER_MISSING_OR_REUSED')
        used.add(call['invocation_id']);profile=profiles[spec['stage']];payload=json.loads(binding.binding_canonical_json)
        request=request_rows.get(call['request_id'])
        _require(request is not None and request['request_sha256']==spec['request_sha256']
            and all(request[key]==call[key] for key in ('workspace_id','conversation_id','request_id','owner_id','trace_id'))
            and request['principal_id']==binding.principal_id,'QUALITY_QUALIFICATION_REQUEST_MISMATCH')
        _require(call['workspace_id']==binding.workspace_id==pins.workspace_id
            and call['conversation_id']==binding.conversation_id and call['request_id']==binding.request_id
            and call['state']=='SUCCEEDED' and call['dispatch_receipt_id'] is not None and call['worker_id'] is not None
            and binding.stage==('PLANNER' if spec['stage'] in {'GENERATION','REFINEMENT'} else 'INTERPRETER')
            and call['profile'] in request['model_profiles'] and call['profile']==payload['profile'] and _hash(call['profile'])==profile['model_profile_sha256']
            and payload['eligibility']['snapshot_sha256']==profile['prompt_snapshot_sha256']
            and call['profile']['model_id']==call['returned_model_id']==profile['model_id']
            and call['finish_reason']=='STOP' and call['safety_outcome']=='PASSED'
            and call['response_sha256']==provider['response_sha256'] and binding.binding_sha256==provider['binding_sha256']
            and started<=call['started_at']<=binding.bound_at<=call['completed_at']<=completed,
            'QUALITY_QUALIFICATION_PROVIDER_MISMATCH')
    _require(used==set(calls)==set(prompt_bindings) and {v['request_id'] for v in calls.values()}==set(request_rows),
        'QUALITY_QUALIFICATION_PROVIDER_SCOPE')
    return PolicyEvaluationQualification(pins,native_job_id,now,min(expires,_utc(valid_until)))
