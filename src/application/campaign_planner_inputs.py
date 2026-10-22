"""Server-owned planner input preparation; no dispatch, persistence or release."""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_planner_projection import validate_options,project_planner_response
from src.application.prompt_runtime_service import ResolvedPrompt
from src.application.workflow_contract import _require,canonical_json_bytes


def digest(text):return hashlib.sha256(text.encode('utf-8')).hexdigest()


@dataclass(frozen=True,slots=True)
class PlannerInputs:
    manifest_json: str
    system_instruction: str
    prompt: str
    options_json: str
    images: tuple

    @property
    def sha256(self):return _hash(json.loads(self.manifest_json))

    def project(self,response_text):
        result=project_planner_response(response_text,self.options_json)
        _require(result.options_sha256==json.loads(self.manifest_json)['options_sha256'],
            'PLANNER_INPUT_OPTIONS_CHANGED')
        return result


def prepare_planner_inputs(authority,resolved,*,evidence,options_json,transcript,images=(),
        checked_at=None):
    """Supplied server evidence is pinned, not independently authenticated here.

    transcript is ordered raw text matching the stored hash manifest. Images are
    (asset ID, immutable generation, MIME type, bytes) tuples from a trusted reader.
    Raw text is transient and must not be written into diagnostic manifests.
    """
    now=_utc(checked_at or datetime.now(timezone.utc))
    _require(type(authority) is ExecutionAuthorityRead and type(resolved) is ResolvedPrompt,
        'PLANNER_INPUT_AUTHORITY_REQUIRED')
    c=json.loads(authority.content_canonical_json)
    _require(canonical_json_bytes(c).decode()==authority.content_canonical_json
        and _hash(c)==authority.content_sha256 and authority.observed_at<=now<authority.valid_until,
        'PLANNER_INPUT_AUTHORITY_CHANGED')
    a=c['authority'];b=resolved.binding
    _require(all(a[k]==getattr(b,k) for k in ('workspace_id','conversation_id','request_id','principal_id'))
        and a['stage']=='GENERATION' and b.stage=='PLANNER','PLANNER_INPUT_SCOPE')
    eligibility=json.loads(b.binding_canonical_json)['eligibility']
    _require(b.bound_at<=now<datetime.fromisoformat(eligibility['eligible_until']), 'PLANNER_INPUT_EXPIRED')
    _require(type(transcript) is tuple and len(transcript)==len(a['transcript'])
        and all(type(text) is str and len(text.encode())<=65536 and digest(text)==turn['content_sha256']
            for text,turn in zip(transcript,a['transcript'])),'PLANNER_INPUT_TRANSCRIPT_INCOMPLETE')
    options=validate_options(options_json)
    _require(type(evidence) is dict and len(canonical_json_bytes(evidence))<=262144,'PLANNER_INPUT_EVIDENCE_SIZE')
    _require(type(images) is tuple and len(images)<=16,'PLANNER_INPUT_IMAGES')
    frozen=[];pins=[]
    for item in images:
        _require(type(item) is tuple and len(item)==4,'PLANNER_INPUT_IMAGES')
        asset,generation,mime,data=item
        _require(type(asset) is str and 0<len(asset)<=256 and type(generation) is str
            and generation.isascii() and generation.isdigit() and int(generation)>0
            and mime in ('image/png','image/jpeg','image/webp') and type(data) is bytes
            and 0<len(data)<=10*1024**2,'PLANNER_INPUT_IMAGES')
        frozen.append(item);pins.append(dict(asset_id=asset,generation=generation,mime_type=mime,
            byte_count=len(data),sha256=hashlib.sha256(data).hexdigest()))
    _require(len({p['asset_id'] for p in pins})==len(pins)
        and sum(p['byte_count'] for p in pins)<=20*1024**2,'PLANNER_INPUT_IMAGES')
    request=dict(selection=a,transcript=[dict(role=t['role'],text=text)
        for t,text in zip(a['transcript'],transcript)])
    variables=dict(campaign_request=canonical_json_bytes(request).decode(),
        evidence_bundle_json=canonical_json_bytes(evidence).decode(),creative_options_json=options_json)
    system,prompt=resolved.render(variables)
    manifest=dict(version='campaign-planner-inputs-v1',authority_sha256=authority.content_sha256,
        authority_native_job_id=authority.native_job_id,selection_sha256=_hash(a),catalog_sha256=c['catalog_sha256'],binding_sha256=b.binding_sha256,
        invocation_id=b.invocation_id,scope={k:a[k] for k in ('workspace_id','conversation_id','request_id','principal_id')},
        profile_sha256=_hash(json.loads(b.binding_canonical_json)['profile']),options_sha256=_hash(options),evidence_sha256=_hash(evidence),
        transcript_sha256=c['transcript_sha256'],system_sha256=digest(system),prompt_sha256=digest(prompt),
        images=pins,checked_at=now.isoformat())
    return PlannerInputs(canonical_json_bytes(manifest).decode(),system,prompt,options_json,tuple(frozen))


def bind_planner_output(prepared,response_text,*,invocation,request,profile,binding_sha256):
    """Diagnostic bridge to a durable outcome, not proof inputs were dispatched.

    Caller must retain the original server-prepared envelope and independently
    reread current authority/access/claim evidence before any result release.
    """
    from src.application.campaign_quality_authority import bind_provider
    _require(type(prepared) is PlannerInputs,'PLANNER_INPUT_REQUIRED')
    manifest=json.loads(prepared.manifest_json)
    _require(manifest['binding_sha256']==binding_sha256
        and manifest['invocation_id']==invocation['invocation_id']
        and all(manifest['scope'][k]==request[k] for k in ('workspace_id','conversation_id','request_id'))
        and manifest['scope']['principal_id']==request['principal_id']
        and manifest['profile_sha256']==_hash(profile)
        and manifest['system_sha256']==digest(prepared.system_instruction)
        and manifest['prompt_sha256']==digest(prepared.prompt)
        and manifest['images']==[dict(asset_id=a,generation=g,mime_type=m,byte_count=len(d),
            sha256=hashlib.sha256(d).hexdigest()) for a,g,m,d in prepared.images], 'PLANNER_INPUT_CHANGED')
    provider_sha=bind_provider(invocation,request=request,profile=profile,response_text=response_text,
        candidate_text=response_text)
    projection=prepared.project(response_text)
    return projection,_hash(dict(version='campaign-planner-outcome-projection-v1',
        prepared_inputs_sha256=prepared.sha256,provider_sha256=provider_sha,projection_sha256=projection.sha256))


def prepare_from_stored_transcript(client,resolved,*,evidence,options_json,images=(),
        clock=lambda:datetime.now(timezone.utc)):
    """Compose exact current authority and stored bodies; never use local turns."""
    from src.application.campaign_transcript import read_transcript
    from src.application.campaign_quality_authority_reader import read_execution_authority
    _require(type(resolved) is ResolvedPrompt,'PLANNER_INPUT_AUTHORITY_REQUIRED')
    binding=resolved.binding
    args=dict(identity=resolved.capture.request.identity,workspace_id=binding.workspace_id,
        conversation_id=binding.conversation_id,request_id=binding.request_id,clock=clock)
    before=read_execution_authority(client,**args)
    texts=read_transcript(client,**args)
    after=read_execution_authority(client,**args)
    _require(before.content_sha256==after.content_sha256 and before.native_job_id!=after.native_job_id,
        'PLANNER_INPUT_TRANSCRIPT_CHANGED')
    return prepare_planner_inputs(after,resolved,evidence=evidence,options_json=options_json,
        transcript=texts,images=images,checked_at=clock())
