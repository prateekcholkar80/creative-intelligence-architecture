"""Identity-only private composition from caller-verified product and image pins.

Pure rendering never authenticates, approves sources, publishes or grants release.
It reuses the established layout helpers without synthetic SKU/asset assumptions.
"""
from dataclasses import dataclass
from datetime import datetime
import hashlib,io,json
from pathlib import Path
from PIL import Image,ImageDraw,ImageOps

from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_planner_projection import project_planner_response
from src.application.campaign_planner_runtime import identity_options
from src.application.campaign_quality_product_reader import ProductFactRead
from src.application.workflow_contract import _require,_digest,_id
from src.generators.banner_preview_generator import validate_config,regions,inside,overlaps,text_block

VERSION='source-bound-identity-campaign-composition-v1'


@dataclass(frozen=True,slots=True)
class ProductImageInput:
    sku:str
    binding_id:str
    content_sha256:str
    lineage_json:str
    raw:bytes

    def __post_init__(self):
        _id(self.sku);_id(self.binding_id);_digest(self.content_sha256)
        _require(type(self.raw) is bytes and 0<len(self.raw)<=5*1024**2
            and hashlib.sha256(self.raw).hexdigest()==self.content_sha256,'CAMPAIGN_IMAGE_BYTES_CHANGED')
        _require(type(self.lineage_json) is str and len(self.lineage_json.encode())<=16384,'CAMPAIGN_IMAGE_LINEAGE')
        value=json.loads(self.lineage_json)
        _require(canonical(value)==self.lineage_json and type(value) is dict
            and value.get('sku')==self.sku and value.get('content_sha256')==self.content_sha256
            and value.get('binding_id')==self.binding_id,'CAMPAIGN_IMAGE_LINEAGE')


@dataclass(frozen=True,slots=True)
class RenderedCampaign:
    png:bytes
    metadata_json:str


@dataclass(frozen=True,slots=True)
class GeneratedSceneInput:
    sku:str
    source_image_binding_id:str
    record_id:str
    outcome_sha256:str
    content_sha256:str
    raw:bytes
    model_id:str

    def __post_init__(self):
        _id(self.sku);_id(self.source_image_binding_id)
        for name in ('record_id','outcome_sha256','content_sha256'):_digest(getattr(self,name))
        _require(self.model_id=='gemini-3.1-flash-image' and type(self.raw) is bytes
            and 0<len(self.raw)<=5*1024**2 and hashlib.sha256(self.raw).hexdigest()==self.content_sha256,
            'CAMPAIGN_SCENE_BYTES_CHANGED')

    def pins(self):
        return dict(sku=self.sku,source_image_binding_id=self.source_image_binding_id,record_id=self.record_id,
            outcome_sha256=self.outcome_sha256,content_sha256=self.content_sha256,model_id=self.model_id)


def render_identity_campaign(*,product,image,planner_response,options_json,checked_at,config,scene=None):
    _require(type(product) is ProductFactRead and type(image) is ProductImageInput
        and type(checked_at) is datetime and checked_at.tzinfo is not None,'CAMPAIGN_RENDER_INPUT')
    fact=product.fact;fields=json.loads(fact.fields_json)
    _require(fact.sku==image.sku and fact.access_allowed is True and fact.source_id==product.native_job_id
        and fact.observed_at<=checked_at<fact.valid_until,'CAMPAIGN_RENDER_PRODUCT_UNVERIFIED')
    # Exact current identity options prevent benefit/offer copy or style injection.
    _require(options_json==identity_options(fields),'CAMPAIGN_RENDER_OPTIONS_CHANGED')
    projection=project_planner_response(planner_response,options_json)
    draft=json.loads(projection.draft_canonical_json)
    _require(draft['headline']==fields['product_name'] and draft['body']=='' and draft['cta']=='View product',
        'CAMPAIGN_RENDER_COPY_OUTSIDE_IDENTITY_SCOPE')
    validate_config(config);boxes=regions(draft['layout']);size=(config['width'],config['height'])
    _require(all(inside(box,(0,0,*size)) for box in boxes.values())
        and all(not overlaps(boxes[a],boxes[b]) for a in boxes for b in boxes if a!=b),'CAMPAIGN_RENDER_REGIONS')
    with Image.open(io.BytesIO(image.raw)) as decoded:
        _require(decoded.format in {'PNG','JPEG','WEBP'} and 0<decoded.width*decoded.height<=24_000_000,
            'CAMPAIGN_RENDER_IMAGE_FORMAT')
        decoded.load();source=ImageOps.exif_transpose(decoded).convert('RGB')
        source_size=source.size
        px,py,pw,ph=boxes['product']
        contained=ImageOps.contain(source,(pw-24,ph-24),method=Image.Resampling.LANCZOS)
    if scene is None:canvas=Image.new('RGB',size,'#F7F7F5')
    else:
        _require(type(scene) is GeneratedSceneInput and scene.sku==fact.sku
            and scene.source_image_binding_id==image.binding_id,'CAMPAIGN_SCENE_SCOPE_CHANGED')
        with Image.open(io.BytesIO(scene.raw)) as generated:
            _require(generated.format in {'PNG','JPEG'} and generated.width*generated.height<=4_194_304
                and getattr(generated,'n_frames',1)==1,'CAMPAIGN_SCENE_FORMAT')
            generated.load();canvas=ImageOps.fit(generated.convert('RGB'),size,method=Image.Resampling.LANCZOS)
    draw=ImageDraw.Draw(canvas)
    centered=draft['layout']=='PRODUCT_CENTER_COPY_ABOVE'
    headline=text_block(draw,draft['headline'],config['heading_font_path'],boxes['headline'],
        config['heading_max_size'],config['heading_min_size'],'#242424',centered)
    x,y,w,h=boxes['cta'];draw.rectangle((x,y,x+w-1,y+h-1),fill='#242424')
    cta=text_block(draw,draft['cta'],config['body_font_path'],(x+18,y+16,w-36,h-26),20,18,'#FFFFFF',True)
    product_box=(px+(pw-contained.width)//2,py+(ph-contained.height)//2,contained.width,contained.height)
    canvas.paste(contained,product_box[:2])
    text_block(draw,'PRIVATE DRAFT - REVIEW REQUIRED',config['body_font_path'],(64,589,1072,20),13,13,'#242424')
    x,y,w,h=product_box
    _require(canvas.crop((x,y,x+w,y+h)).tobytes()==contained.tobytes(),'CAMPAIGN_RENDER_PRODUCT_OVERLAID')
    buffer=io.BytesIO();canvas.save(buffer,format='PNG',optimize=False);png=buffer.getvalue()
    with Image.open(io.BytesIO(png)) as saved:
        saved.load();_require(saved.format=='PNG' and saved.size==size
            and saved.tobytes()==canvas.tobytes(),'CAMPAIGN_RENDER_PNG_CHANGED')
    metadata=dict(version=VERSION,sku=fact.sku,method='DETERMINISTIC_SOURCE_COMPOSITION',
        status='PRIVATE_CANDIDATE_REQUIRES_PROTECTED_VALIDATION',content_sha256=hashlib.sha256(png).hexdigest(),
        width=size[0],height=size[1],mime_type='image/png',product_native_job_id=product.native_job_id,
        product_capture_sha256=fact.sha256,planner_response_sha256=projection.response_sha256,
        planner_projection_sha256=projection.sha256,options_sha256=projection.options_sha256,
        image=json.loads(image.lineage_json),image_input_size=list(source_size),layout=draft['layout'],
        copy=dict(headline=draft['headline'],body='',cta=draft['cta']),
        geometry=dict(regions=boxes,product_bounds=product_box),text_layout=dict(headline=headline,cta=cta),
        fonts={k:dict(sha256=hashlib.sha256(Path(config[k]).read_bytes()).hexdigest())
            for k in ('heading_font_path','body_font_path')},
        structural_checks=dict(identity_copy=True,source_bytes_hash=True,complete_image_contained=True,
            scaled_product_pixels_match=True,regions_disjoint=True,text_fits=True,png_lossless=True),
        required_remaining_checks=['CURRENT_SOURCE_AND_ACCESS','SCOPED_IMAGE_USE','BRAND_MARKET_CHANNEL',
            'RENDERED_VISUAL_FIDELITY_AND_CLAIMS','DURABLE_ARTIFACT_BINDING','EXACT_PROMPT_POLICY_ELIGIBILITY'],
        clinical_claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
    if scene is not None:
        metadata.update(version='vertex-scene-source-product-composition-v1',
            method='VERTEX_GENERATED_SCENE_WITH_PRESERVED_SOURCE_PRODUCT',generated_scene=scene.pins())
    return RenderedCampaign(png,canonical(dict(metadata,metadata_sha256=sha(metadata))))
