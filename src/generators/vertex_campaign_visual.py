"""One-attempt Vertex image transport. Installation/durable admission are separate."""
from dataclasses import dataclass
import hashlib,io
from PIL import Image
from google import genai
from google.genai import types
from src.application.campaign_visual_request import validate_visual_request
from src.application.campaign_evaluator_store import canonical
from src.application.campaign_evidence_dispatch import provider_observation
from src.application.operation_budget import require_owned_worker,check_operation,operation_timeout
from src.application.workflow_contract import _require
from src.common.gemini_generation import safety_settings
from src.ingestion.setup_workflow_tables import PROJECT


@dataclass(frozen=True,slots=True)
class GeneratedVisual:
    raw:bytes
    mime_type:str
    response_manifest_json:str
    observation:object


def _name(value):return getattr(value,'name',value)


def extract_visual(response,model_id):
    feedback=getattr(response,'prompt_feedback',None)
    _require(_name(getattr(feedback,'block_reason',None)) in (None,'BLOCKED_REASON_UNSPECIFIED')
        and not any(getattr(r,'blocked',False) for r in (getattr(feedback,'safety_ratings',None) or [])),
        'VISUAL_PROVIDER_BLOCKED')
    candidates=getattr(response,'candidates',None) or []
    _require(len(candidates)==1 and _name(candidates[0].finish_reason)=='STOP'
        and not any(getattr(r,'blocked',False) for r in (getattr(candidates[0],'safety_ratings',None) or [])),
        'VISUAL_PROVIDER_INCOMPLETE')
    parts=getattr(getattr(candidates[0],'content',None),'parts',None) or []
    _require(0<len(parts)<=32,'VISUAL_PROVIDER_PARTS')
    manifest=[];images=[]
    for part in parts:
        thought=getattr(part,'thought',False) is True;inline=getattr(part,'inline_data',None);text=getattr(part,'text',None)
        _require((inline is None)!=(text is None),'VISUAL_PROVIDER_PARTS')
        if inline is not None:
            raw=inline.data;mime=inline.mime_type
            _require(type(raw) is bytes and 0<len(raw)<=5*1024**2 and mime in {'image/png','image/jpeg'},'VISUAL_PROVIDER_IMAGE')
            manifest.append(dict(kind='image',thought=thought,mime_type=mime,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
            if not thought:images.append((raw,mime))
        else:
            _require(type(text) is str and len(text.encode())<=32768,'VISUAL_PROVIDER_TEXT')
            manifest.append(dict(kind='text',thought=thought,bytes=len(text.encode()),sha256=hashlib.sha256(text.encode()).hexdigest()))
    _require(len(images)==1,'VISUAL_PROVIDER_IMAGE_COUNT')
    raw,mime=images[0]
    with Image.open(io.BytesIO(raw)) as image:
        _require(image.format=={'image/png':'PNG','image/jpeg':'JPEG'}[mime]
            and 128<=image.width<=2048 and 128<=image.height<=2048
            and abs(image.width/image.height-16/9)<=.06 and getattr(image,'n_frames',1)==1,'VISUAL_PROVIDER_IMAGE_SHAPE')
        image.verify()
    wire=canonical(dict(parts=manifest));observation=provider_observation(response,wire)
    _require(observation.returned_model_id==model_id,'VISUAL_PROVIDER_MODEL_UNVERIFIED')
    return GeneratedVisual(raw,mime,wire,observation)


class VertexCampaignVisualTransport:
    def __call__(self,request):
        require_owned_worker();check_operation();profile=validate_visual_request(request)
        _require(profile.endpoint_authorized,'VISUAL_ENDPOINT_AUTHORIZATION_REQUIRED')
        value=request.intent
        # Keep time for fresh-source checks and the original outcome publication.
        available=operation_timeout(180)
        _require(available>60,'VISUAL_COMPLETION_BUDGET_REQUIRED')
        provider_timeout=min(120,available-60)
        config=types.GenerateContentConfig(system_instruction=value['system'],candidate_count=1,
            response_modalities=['TEXT','IMAGE'],image_config=types.ImageConfig(aspect_ratio=profile.aspect_ratio,image_size=profile.image_size),
            safety_settings=safety_settings(),automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True))
        with genai.Client(vertexai=True,project=PROJECT,location=profile.location,
                http_options=types.HttpOptions(api_version=profile.api_version,timeout=int(provider_timeout*1000),
                    retry_options=types.HttpRetryOptions(attempts=1))) as client:
            response=client.models.generate_content(model=profile.model_id,
                contents=[types.Content(role='user',parts=[types.Part.from_text(text=value['content']),
                    types.Part.from_bytes(data=request.crop_png,mime_type='image/png')])],config=config)
        check_operation()
        return extract_visual(response,profile.model_id)
