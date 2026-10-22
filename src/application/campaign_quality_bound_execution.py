"""Opt-in full-authority E2 diagnostics. No dispatch, approval or response release."""
from datetime import datetime,timedelta,timezone
import json
from src.application.campaign_acceptance_contract import Stage,_hash,_utc
from src.application.campaign_permission_reader import PermissionRequest,Checkpoint,read_permission
from src.application.campaign_quality_authority import bind_provider,bind_permissions,validate_payload
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead,read_execution_authority
from src.application.campaign_quality_execution import (
    QualityRequest,QualityExecutionSettings,QualityExecutionResult,_context,
)
from src.application.campaign_quality_checks import QualityCandidate,check_candidate
from src.application.campaign_quality_product_reader import _access,read_product_fact
from src.application.workflow_contract import _require,canonical_json_bytes


def _capture(request,*,client,identity,started,clock):
    read=read_execution_authority(client,workspace_id=request.workspace_id,conversation_id=request.conversation_id,
        request_id=request.request_id,identity=identity,clock=clock)
    _require(type(read) is ExecutionAuthorityRead and type(read.content_canonical_json) is str
        and len(read.content_canonical_json.encode())<=65536,'QUALITY_AUTHORITY_REQUIRED')
    content=json.loads(read.content_canonical_json)
    _require(canonical_json_bytes(content).decode()==read.content_canonical_json
        and _hash(content)==read.content_sha256,'QUALITY_AUTHORITY_HASH_MISMATCH')
    now=_utc(clock())
    _require(started<=read.observed_at<=now<read.valid_until<=read.observed_at+timedelta(seconds=60),
        'QUALITY_AUTHORITY_EXPIRED')
    authority=validate_payload(content['authority'])
    _require(content['version']=='campaign-execution-authority-capture-v1'
        and content['authority_sha256']==_hash(authority)
        and all(authority[k]==getattr(request,k) for k in ('workspace_id','conversation_id','request_id','sku','country','channel'))
        and authority['principal_id']==identity.principal.principal_id and authority['stage']==request.stage.value,
        'QUALITY_AUTHORITY_SCOPE_MISMATCH')
    record=content['request_record'];pins=content['request_bindings']
    _require(pins['request_record_sha256']==_hash(record) and authority['request_sha256']==record['request_sha256']
        and all(authority[k]==record[k] for k in ('workspace_id','conversation_id','request_id','principal_id')),
        'QUALITY_AUTHORITY_REQUEST_MISMATCH')
    input_hash=record['request_sha256'] if request.stage is Stage.GENERATION else record['message_sha256']
    _require(input_hash is not None and request.user_input_sha256==input_hash
        and request.conversation_sha256==content['transcript_sha256'],'QUALITY_AUTHORITY_INPUT_MISMATCH')
    return read,content


def _permissions(authority,*,client,clock):
    snapshots=tuple(read_permission(client,PermissionRequest('CLAIM',Checkpoint(use['claim_id'])),clock=clock)
        for use in authority['claims'])
    return bind_permissions(authority,snapshots,checked_at=_utc(clock()))


def _lineage(content,settings,source_pins):
    authority=content['authority'];profile=json.loads(settings.model_profile_json)
    stage='PLANNER' if authority['stage']==Stage.GENERATION.value else 'INTERPRETER'
    calls=[c for c in content['calls'] if c['request_id']==authority['request_id'] and c['stage']==stage]
    _require(len(calls)==1,'QUALITY_PROVIDER_CALL_COVERAGE')
    call=calls[0]
    bindings=[b for b in content['bindings'] if b['invocation_id']==call['invocation_id']]
    _require(len(bindings)==1,'QUALITY_PROVIDER_BINDING_COVERAGE')
    binding=bindings[0];payload=json.loads(binding['binding_canonical_json'])
    _require(binding['workspace_id']==authority['workspace_id'] and binding['conversation_id']==authority['conversation_id']
        and binding['request_id']==authority['request_id'] and binding['principal_id']==authority['principal_id']
        and binding['prompt_key']==settings.behavior.prompt_key and binding['stage']==stage
        and payload['profile']==profile==call['profile'] and payload['eligibility']==source_pins['prompt']
        # started_at is immutable PLANNED creation time, not provider dispatch.
        # Registered dispatch atomically stores the later binding and claim.
        # Both source authority and planning must precede that binding, which
        # must precede the observed successful completion. Do not manufacture
        # a provider-start timestamp from the request's planning timestamp.
        and datetime.fromisoformat(content['authority_recorded_at'])
            <=datetime.fromisoformat(binding['bound_at'].replace('Z','+00:00'))
        and datetime.fromisoformat(call['started_at'].replace('Z','+00:00'))
            <=datetime.fromisoformat(binding['bound_at'].replace('Z','+00:00'))
            <=datetime.fromisoformat(call['completed_at'].replace('Z','+00:00')),
        'QUALITY_PROVIDER_BINDING_MISMATCH')
    return call,profile,binding['binding_sha256']


def run_authoritative_quality(request,build_observed_candidate,*,settings,client,runtime_reader,
        access_reader,identity,clock=lambda:datetime.now(timezone.utc),prepared_inputs=None,question_plan=None,full_evaluator=None):
    """Builder returns (exact-text candidate, observed provider-response text).

    The server-installed annotation adapter does not dispatch a model. Response
    bytes must match the already durable successful invocation. Planner projection
    requires its prepared envelope/manifest. Question copy requires the exact
    parent-pinned gap plan and versioned question projector.
    """
    _require(type(request) is QualityRequest and type(settings) is QualityExecutionSettings
        and callable(build_observed_candidate),'INVALID_QUALITY_EXECUTION')
    before=_access(access_reader,identity,request.workspace_id,request.conversation_id)
    started=_utc(clock())
    initial,content=_capture(request,client=client,identity=identity,started=started,clock=clock)
    sources,_=_context(request,settings,client=client,runtime_reader=runtime_reader,
        identity=identity,access=before,clock=clock)
    call,profile,binding_sha=_lineage(content,settings,sources)
    permission_sha=_permissions(content['authority'],client=client,clock=clock)
    product=read_product_fact(client,request.sku,clock=clock)
    built=build_observed_candidate(product.fact)
    _require(type(built) is tuple and len(built)==2 and type(built[0]) is QualityCandidate,
        'QUALITY_OBSERVED_CANDIDATE_REQUIRED')
    candidate,response=built
    _require(all(getattr(candidate,k)==getattr(request,k) for k in ('request_id','sku','stage','country','channel')),
        'QUALITY_EXECUTION_CANDIDATE_MISMATCH')
    projection_sha=None;input_sha=None;stored_binding=None;input_io=None;question_projection=None
    if question_plan is not None:
        from src.application.campaign_question_projection import bind_question_output,VERSION as QUESTION_PROJECTION_VERSION
        from dataclasses import asdict
        _require(prepared_inputs is None and request.stage is Stage.CLARIFICATION,'QUALITY_QUESTION_PROJECTION_STAGE')
        projection,provider_sha=bind_question_output(question_plan,response,authority=content['authority'],
            request=content['request_record'],invocation=call,profile=profile,binding_sha256=binding_sha)
        _require(candidate.text==projection.text and candidate.sections_json=='{}',
            'QUALITY_QUESTION_PROJECTION_COPY_CHANGED')
        projection_sha=projection.sha256
        question_projection=dict(version=QUESTION_PROJECTION_VERSION,**asdict(projection))
    elif prepared_inputs is None:
        provider_sha=bind_provider(call,request=content['request_record'],profile=profile,
            response_text=response,candidate_text=candidate.text)
    else:
        from src.application.campaign_dispatch_inputs import verify_stored
        from src.application.campaign_planner_inputs import bind_planner_output
        from src.application.prompt_runtime_contract import InvocationPromptBinding
        from src.application.control_io import ControlIO
        _require(request.stage is Stage.GENERATION,'QUALITY_PROJECTION_STAGE')
        row=next(b for b in content['bindings'] if b['invocation_id']==call['invocation_id'])
        stored_binding=InvocationPromptBinding(**{k:v for k,v in row.items()
            if k not in {'schema_version','binding_sha256','bound_at'}},
            bound_at=datetime.fromisoformat(row['bound_at'].replace('Z','+00:00')))
        input_io=ControlIO(client)
        verify_stored(input_io,prepared_inputs,stored_binding)
        manifest=json.loads(prepared_inputs.manifest_json)
        _require(manifest['selection_sha256']==_hash(content['authority'])
            and manifest['transcript_sha256']==content['transcript_sha256']
            and manifest['catalog_sha256']==content['catalog_sha256'], 'QUALITY_INPUT_CONTEXT_CHANGED')
        projection,provider_sha=bind_planner_output(prepared_inputs,response,invocation=call,
            request=content['request_record'],profile=profile,binding_sha256=binding_sha)
        _require(candidate.text==projection.copy_text
            and json.loads(candidate.sections_json)==dict(projection.copy_fields),'QUALITY_PROJECTION_COPY_CHANGED')
        projection_sha=projection.sha256;input_sha=prepared_inputs.sha256
    for use in content['authority']['claims']:
        _require(use['wording'] in candidate.text and all(q in candidate.text for q in use['qualifications']),
            'QUALITY_APPROVED_WORDING_MISSING')
    report=check_candidate(candidate,(product.fact,),checked_at=_utc(clock()),not_before=started)
    full_observation=None
    if full_evaluator is not None:
        from src.application.campaign_full_quality_checks import FullQualityEvaluator
        from src.application.campaign_transcript_history import read_bound_texts
        _require(type(full_evaluator) is FullQualityEvaluator,'QUALITY_FULL_EVALUATOR_REQUIRED')
        texts=read_bound_texts(client,content,workspace_id=request.workspace_id,
            conversation_id=request.conversation_id,clock=clock)
        turns=[dict(role=turn['role'],text=text) for turn,text in zip(content['authority']['transcript'],texts)]
        _require(len(texts)==len(content['authority']['transcript']),'QUALITY_FULL_TRANSCRIPT_COVERAGE')
        # Only the currently read product projection is supplied here. Missing
        # study, claim and visual sources remain missing, never invented.
        evidence=[dict(evidence_id=product.fact.source_id,kinds=['PRODUCT_FACT'],
            statement=product.fact.fields_json,provenance='CURRENT_PRODUCT_PROJECTION_NOT_CLAIM_OR_VISUAL_APPROVAL')]
        from src.application.campaign_evaluator_observer import DurableEvaluatorObserver,StageEvaluationBinding
        if type(full_evaluator.observe) is DurableEvaluatorObserver:
            full_evaluator=full_evaluator.for_stage(StageEvaluationBinding(
                request.workspace_id,request.conversation_id,request.request_id,identity.principal.principal_id,
                request.stage.value,call['invocation_id'],provider_sha,binding_sha,initial.content_sha256,candidate.sha256))
        full_observation=full_evaluator.assess(candidate,report,turns=turns,evidence=evidence,
            selection={k:content['authority'][k] for k in ('sku','country','channel','audience')})
    final_sources,at=_context(request,settings,client=client,runtime_reader=runtime_reader,
        identity=identity,access=before,clock=clock)
    final,final_content=_capture(request,client=client,identity=identity,started=at,clock=clock)
    final_permission_sha=_permissions(final_content['authority'],client=client,clock=clock)
    _require(sources==final_sources and content==final_content and permission_sha==final_permission_sha
        and initial.native_job_id!=final.native_job_id,'QUALITY_AUTHORITY_CONTEXT_CHANGED')
    if prepared_inputs is not None:
        verify_stored(input_io,prepared_inputs,stored_binding)
    after=_access(access_reader,identity,request.workspace_id,request.conversation_id)
    _require(before==after,'QUALITY_EXECUTION_ACCESS_CHANGED')
    ended=_utc(clock())
    _require(started<=ended<min(started+timedelta(seconds=60),initial.valid_until,final.valid_until,
        product.fact.valid_until,datetime.fromisoformat(sources['prompt']['eligible_until'])),
        'QUALITY_EXECUTION_EXPIRED')
    context=dict(version='campaign-quality-authoritative-execution-v1',request=request.payload(),
        authority_sha256=initial.content_sha256,authority_native_job_ids=[initial.native_job_id,final.native_job_id],
        catalog_sha256=content['catalog_sha256'],transcript_sha256=content['transcript_sha256'],
        claim_evidence_sha256=permission_sha,provider_sha256=provider_sha,
        prepared_inputs_sha256=input_sha,projection_sha256=projection_sha,invocation_binding_sha256=binding_sha,
        report_sha256=report.sha256,candidate_sha256=candidate.sha256,product_native_job_id=product.native_job_id,
        access=after.to_dict(),sources=sources,checked_at=ended.isoformat())
    if full_observation is not None:context['full_quality_observation']=full_observation
    if question_projection is not None:context['question_projection']=question_projection
    return QualityExecutionResult(report,canonical_json_bytes(context).decode(),_hash(context))
