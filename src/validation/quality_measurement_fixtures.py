"""One synthetic end-to-end-shaped case; not part of the measured 159-case benchmark."""
from datetime import datetime,timedelta,timezone
from dataclasses import replace
from copy import deepcopy
import json
from src.validation.prompt_runtime_fixtures import runtime_fixtures
from src.application.campaign_acceptance_contract import RULE_IDS,_hash
from src.application.campaign_quality_qualification import QualificationRequirements
from src.application.campaign_quality_publication import MeasurementPublisherManifest
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
from src.application.campaign_policy_approval_receipt import PolicyApprovalPins
from src.application.campaign_permission_publication import Rows
from src.application.campaign_review_storage import PublicationControl,row_json
from src.application.workflow_contract import canonical_json_bytes
from src.application.workflow_codec import encode_row


def fixture(now=None):
    now=now or datetime.now(timezone.utc)
    _,contexts=runtime_fixtures(now-timedelta(seconds=5),scenarios=('quality',))
    context=contexts['quality'];bound=context['resolved'].binding
    actor=replace(context['identity'],issued_at=int(now.timestamp())-1,expires_at=int(now.timestamp())+599)
    call=deepcopy(context['invocation']);request=deepcopy(context['request']);profile=call['profile']
    call.update(state='SUCCEEDED',revision=3,dispatch_receipt_id='synthetic-dispatch',worker_id='synthetic-worker',
        returned_model_id=profile['model_id'],response_sha256='f'*64,finish_reason='STOP',safety_outcome='PASSED',
        started_at=now-timedelta(seconds=6),completed_at=now-timedelta(seconds=4))
    index=[dict(case_id='synthetic-storage-e2e',stage='GENERATION',case_sha256='c'*64,label_scope='STAGE_END_TO_END',
        rule_ids=list(RULE_IDS),expected_decision='PASS',requires_provider=True,request_sha256=request['request_sha256'])]
    requirements=QualificationRequirements('a'*64,_hash(index),'b'*64,('GENERATION',))
    benchmark=dict(schema_version='campaign-quality-benchmark-v1',benchmark_id='synthetic-storage-benchmark',version='v1',
        review_sha256=requirements.review_sha256,criterion_ids=list(RULE_IDS),evaluator_sha256=requirements.evaluator_sha256,
        required_stages=list(requirements.required_stages),case_index=index)
    report=dict(schema_version='campaign-quality-measurement-v1',evaluation_id='synthetic-storage-measurement',workspace_id=bound.workspace_id,
        benchmark_sha256=_hash(benchmark),evaluator_sha256=requirements.evaluator_sha256,
        policy_bindings={kind:dict(policy_id=kind.lower(),policy_version='fixture-v1',policy_sha256='d'*64)
            for kind in ('ACCEPTANCE','CLARIFICATION')},
        profiles={'GENERATION':dict(prompt_snapshot_sha256=json.loads(bound.binding_canonical_json)['eligibility']['snapshot_sha256'],
            model_profile_sha256=_hash(profile),model_id=profile['model_id'])},
        started_at=(now-timedelta(seconds=7)).isoformat(),completed_at=(now-timedelta(seconds=3)).isoformat(),
        valid_until=(now+timedelta(hours=1)).isoformat(),recorded_by=actor.principal.principal_id,
        cases=[dict(case_id=index[0]['case_id'],case_sha256='c'*64,observed_decision='PASS',
            checks=[dict(rule_id=rid,state='PASS',reason_code='SYNTHETIC_STORAGE_FIXTURE',evidence_sha256='e'*64) for rid in RULE_IDS],
            provider=dict(invocation_id=bound.invocation_id,response_sha256=call['response_sha256'],binding_sha256=bound.binding_sha256))])
    pins=PolicyApprovalPins(bound.workspace_id,'ACCEPTANCE','GENERATION','acceptance','fixture-v1','d'*64,
        report['profiles']['GENERATION']['prompt_snapshot_sha256'],_hash(profile),_hash(benchmark),_hash(report))
    manifest=MeasurementPublisherManifest(OperatorPublisherManifest('synthetic-measurement-publisher',actor.audience,
        (actor.principal.principal_id,),now-timedelta(seconds=1),now+timedelta(seconds=299)),
        pins.workspace_id,requirements.review_sha256,requirements.evaluator_sha256)
    control=PublicationControl(1,None,'0'*64,actor.principal.principal_id,now-timedelta(seconds=10))
    capture=dict(benchmark_json=canonical_json_bytes(benchmark).decode(),report_json=canonical_json_bytes(report).decode(),
        existing_benchmarks=(),invocations=(encode_row(call,'model_invocations'),),requests=(encode_row(request,'requests'),),
        bindings=(row_json(bound.row()),))
    return dict(identity=actor,manifest=manifest,control=Rows('CONTROL','campaign-permissions',row_json(control.row()),(row_json(control.row()),)),
        capture=capture,pins=pins,requirements=requirements,checked_at=now),dict(model_invocations=[call],requests=[request],
        invocation_prompt_bindings=[bound.row()],campaign_permission_controls=[control.row()],campaign_permission_control_history=[control.row()])
