"""Render a review-only banner from a freshly grounded brief and original bytes.

Run: python -m src.generators.banner_preview_generator --request config/retrieval_request.example.json
Gemini receives live evidence/images through the brief generator. Pillow then
composes exact approved copy and the proportionally scaled complete original
product image. No local brief/cache is used for grounding; no cloud writes.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from google import genai
from google.cloud import bigquery, storage
from google.genai import types
from PIL import Image, ImageDraw, ImageFont, ImageOps

from src.generators.campaign_assets_generator import wrap_text
from src.generators.campaign_brief_generator import (
    CONFIG as BRIEF_CONFIG, assemble_brief, export_brief, generate_grounded_brief,
)
from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.brand_context import retrieve_brand_context
from src.retrieval.campaign_creatives import CONFIG as CAMPAIGN_CONFIG
from src.retrieval.campaign_performance import json_value
from src.retrieval.evidence_packet import EvidenceBundle
from src.retrieval.product_image import verify_image_bytes
from src.retrieval.product_knowledge import retrieve_product_knowledge
from src.retrieval.request import RetrievalRequest
from src.retrieval.sales_analytics import CONFIG as SALES_CONFIG

CONFIG = ROOT/'config/banner_preview_generator.json'
VERSION = 'grounded-banner-preview-v1'
LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class BannerPreview:
    png: bytes
    metadata: dict


def validate_config(config: dict) -> None:
    if (set(config) != {'width','height','heading_font_path','body_font_path','heading_max_size','heading_min_size'}
            or type(config['width']) is not int or type(config['height']) is not int
            or (config['width'],config['height']) != (1200,628)
            or type(config['heading_min_size']) is not int or type(config['heading_max_size']) is not int
            or not 24 <= config['heading_min_size'] <= config['heading_max_size'] <= 56):
        raise ValueError('Preview requires a 1200 x 628 canvas and readable bounded heading sizes')
    for key in ('heading_font_path','body_font_path'):
        if not isinstance(config[key],str) or not Path(config[key]).is_file():
            raise ValueError(f'Configure an available font file for {key}')
        ImageFont.truetype(config[key],24)


def validate_brief(brief: dict, bundle: EvidenceBundle) -> dict:
    """Rebuild critical fields from live evidence, rather than trusting a brief."""
    context = bundle.context
    if (brief.get('canonical_sku') != context['canonical_sku']
            or brief.get('request') != context['request'] or brief.get('snapshot_id') != context['snapshot_id']
            or brief.get('source_system') != 'SYNTHETIC_POC'
            or brief.get('status') != 'POC_REVIEW_DRAFT' or brief.get('approval_scope') != 'SYNTHETIC_POC_ONLY'
            or brief.get('evidence_source_of_truth') != 'BIGQUERY' or brief.get('approval_recheck_passed') is not True):
        raise ValueError('Only freshly grounded same-SKU POC review briefs can be rendered')
    expected = assemble_brief(context,brief['model_selection'],brief['model'])
    for key in ('copy','concept','visual_direction','product_image','grounded_evidence','historical_reference',
                'experiment','evidence_index'):
        if brief[key] != expected[key]:
            raise ValueError(f'Brief {key} differs from current grounding or validated creative selection')
    metadata = brief['product_image']['metadata']
    if (metadata['source_system'] != 'BEAUTYFEEDS' or metadata['sku'] != brief['canonical_sku']
            or metadata['asset_id'] != f'AST-{brief["canonical_sku"]}-PRIMARY'
            or not metadata['verification'] or any(v is not True for v in metadata['verification'].values())):
        raise ValueError('Verified canonical original product image required')
    raw = bundle.image_bytes[metadata['asset_id']]
    verify_image_bytes(metadata,raw)
    return metadata


def regions(layout: str) -> dict[str,tuple[int,int,int,int]]:
    """Rectangles are x/y/width/height; three bounded brief layouts are supported."""
    right = (668,66,468,488)
    if layout == 'PRODUCT_RIGHT_COPY_LEFT':
        return dict(headline=(64,123,536,262),body=(64,410,536,36),cta=(64,477,240,56),product=right)
    if layout == 'PRODUCT_LEFT_COPY_RIGHT':
        return dict(headline=(640,123,496,262),body=(640,410,496,36),cta=(640,477,240,56),product=(64,66,508,488))
    if layout == 'PRODUCT_CENTER_COPY_ABOVE':
        return dict(headline=(150,36,900,126),body=(150,173,900,28),cta=(480,513,240,52),product=(432,219,336,276))
    raise ValueError('Unsupported grounded layout')


def inside(inner: tuple, outer: tuple) -> bool:
    x,y,w,h = inner;a,b,c,d = outer
    return w>0 and h>0 and x>=a and y>=b and x+w<=a+c and y+h<=b+d


def overlaps(first: tuple, second: tuple) -> bool:
    x,y,w,h = first;a,b,c,d = second
    return x<a+c and a<x+w and y<b+d and b<y+h


def text_block(draw: ImageDraw.ImageDraw, text: str, font_path: str, box: tuple,
               max_size: int, min_size: int, fill: str, centered: bool = False) -> dict:
    x,y,width,height = box
    if not text:
        return dict(text=text,lines=[],font_size=None,bounds=[])
    for size in range(max_size,min_size-1,-1):
        font = ImageFont.truetype(font_path,size)
        lines = wrap_text(text,draw,font,width)
        line_boxes = [draw.textbbox((0,0),line,font=font,anchor='lt') for line in lines]
        spacing = round(size*1.24)
        if (len(lines)-1)*spacing+max(b[3] for b in line_boxes)>height:
            continue
        positions = [(x+(width-draw.textlength(line,font=font))/2 if centered else x,
                      y+i*spacing) for i,line in enumerate(lines)]
        bounds = [draw.textbbox(position,line,font=font,anchor='lt')
                  for position,line in zip(positions,lines)]
        rectangles = [(a,b,c-a,d-b) for a,b,c,d in bounds]
        if any(not inside(rect,box) for rect in rectangles):
            continue
        for position,line in zip(positions,lines):
            draw.text(position,line,font=font,fill=fill,anchor='lt')
        return dict(text=text,lines=lines,font_size=size,bounds=rectangles)
    raise ValueError('Copy cannot fit at the configured readable sizes; refusing to truncate')


def render_preview(brief: dict, bundle: EvidenceBundle, config: dict) -> BannerPreview:
    validate_config(config);source = validate_brief(brief,bundle)
    width,height = config['width'],config['height'];style = brief['visual_direction']
    boxes = regions(style['layout_id']);canvas_box = (0,0,width,height)
    if (any(not inside(box,canvas_box) for box in boxes.values())
            or any(overlaps(boxes[a],boxes[b]) for a in boxes for b in boxes if a != b)):
        raise ValueError('Rendering regions must be on-canvas and disjoint')
    canvas = Image.new('RGB',(width,height),style['background']);draw = ImageDraw.Draw(canvas)
    centered = style['layout_id'] == 'PRODUCT_CENTER_COPY_ABOVE'
    headline = text_block(draw,brief['copy']['headline'],config['heading_font_path'],boxes['headline'],
                          config['heading_max_size'],config['heading_min_size'],style['text_color'],centered)
    body = text_block(draw,brief['copy']['body'],config['body_font_path'],boxes['body'],
                      22,20,style['text_color'],centered)
    x,y,w,h = boxes['cta'];draw.rectangle((x,y,x+w-1,y+h-1),fill=style['text_color'])
    cta = text_block(draw,brief['copy']['cta'],config['body_font_path'],(x+18,y+16,w-36,h-26),
                     20,18,'#FFFFFF',True)
    # Keep every source pixel region: proportional resize only, no crop/removal/repaint.
    px,py,pw,ph = boxes['product']
    with Image.open(io.BytesIO(bundle.image_bytes[source['asset_id']])) as original:
        image = ImageOps.exif_transpose(original).convert('RGB')
        contained = ImageOps.contain(image,(pw-24,ph-24),method=Image.Resampling.LANCZOS)
    image_box = (px+(pw-contained.width)//2,py+(ph-contained.height)//2,contained.width,contained.height)
    draw.rectangle((px,py,px+pw-1,py+ph-1),fill='#FFFFFF')
    canvas.paste(contained,image_box[:2])
    footer = text_block(draw,'SYNTHETIC POC PREVIEW',config['body_font_path'],(64,589,1072,20),
                        13,13,style['text_color'])
    if canvas.crop((image_box[0],image_box[1],image_box[0]+image_box[2],image_box[1]+image_box[3])).tobytes() != contained.tobytes():
        raise ValueError('Original product image was altered or overlaid during composition')
    output = io.BytesIO();canvas.save(output,format='PNG',optimize=False);png = output.getvalue()
    with Image.open(io.BytesIO(png)) as decoded:
        if decoded.format != 'PNG' or decoded.size != (width,height):
            raise ValueError('Invalid banner output format or dimensions')
        decoded.verify()
    fonts = {key:dict(path=config[key],sha256=hashlib.sha256(Path(config[key]).read_bytes()).hexdigest())
             for key in ('heading_font_path','body_font_path')}
    metadata = dict(generator_version=VERSION,canonical_sku=brief['canonical_sku'],source_system='SYNTHETIC_POC',
        status='LOCAL_REVIEW_PREVIEW',approval_scope='SYNTHETIC_POC_ONLY',description_method='DETERMINISTIC_COMPOSITION',
        sha256=hashlib.sha256(png).hexdigest(),mime_type='image/png',width=width,height=height,
        concept=brief['concept'],copy=copy.deepcopy(brief['copy']),visual_direction=copy.deepcopy(style),
        original_image=copy.deepcopy(source),snapshot_id=brief['snapshot_id'],
        evidence_index=copy.deepcopy(brief['evidence_index']),font_files=fonts,render_config=copy.deepcopy(config),
        font_usage='Configured local fallback fonts; Canela/Brown licensed files are not supplied.',
        geometry=dict(regions=boxes,original_image_bounds=image_box),
        text_layout=dict(headline=headline,body=body,cta=cta,footer=footer),
        validation=dict(approved_copy=True,source_hash=True,complete_image_contained=True,
                        scaled_source_pixels_match=True,regions_disjoint=True,text_fits=True,png_verified=True),
        limitations=['This is a local POC preview, not a published asset or real-business approval.',
                    'Benefit claims are not authorized; website styling is visual inspiration only.',
                    'Original product pixels are resampled proportionally; packaging is not regenerated.'])
    return BannerPreview(png,metadata)


def generate_preview(client: bigquery.Client, gcs: storage.Client, model_client: genai.Client,
                     request: RetrievalRequest, campaign_config: dict, sales_config: dict,
                     brief_config: dict, config: dict,
                     on_stage: Callable[[str], None] | None = None) -> tuple[BannerPreview,dict,EvidenceBundle]:
    validate_config(config)
    callbacks = dict(on_stage=on_stage) if on_stage else {}
    brief,bundle = generate_grounded_brief(client,gcs,model_client,request,campaign_config,sales_config,brief_config,**callbacks)
    if on_stage:
        on_stage('RENDERING')
    preview = render_preview(brief,bundle,config)
    if on_stage:
        on_stage('VERIFYING')
    brand = retrieve_brand_context(client,request.canonical_sku)
    product = retrieve_product_knowledge(client,request.canonical_sku)
    without_time = lambda packet: {k:v for k,v in packet.items() if k != 'checked_at'}
    if (product != bundle.context['product_knowledge']
            or without_time(brand) != without_time(bundle.context['brand_context'])):
        raise ValueError('Product facts, policy or claim approval changed during rendering; generate again')
    preview.metadata.update(approval_recheck_passed=True,approval_checked_at=brand['checked_at'],
                            created_at=datetime.now(timezone.utc))
    return preview,brief,bundle


def export_preview(preview: BannerPreview, brief: dict, bundle: EvidenceBundle, output_directory: Path) -> Path:
    if (preview.metadata.get('approval_recheck_passed') is not True
            or hashlib.sha256(preview.png).hexdigest() != preview.metadata['sha256']):
        raise ValueError('Verified image and post-render approval recheck required')
    expected = render_preview(brief,bundle,preview.metadata['render_config'])
    if expected.png != preview.png or any(preview.metadata.get(k) != v for k,v in expected.metadata.items()):
        raise ValueError('Preview or its metadata changed after grounded rendering')
    payload = json.dumps(preview.metadata,indent=2,default=json_value)
    brief_path = export_brief(brief,bundle,output_directory)
    destination = brief_path.parent/'banner_preview.png'
    destination.write_bytes(preview.png)
    (brief_path.parent/'banner_preview.json').write_text(payload)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request',type=Path,required=True)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--brief-config',type=Path,default=BRIEF_CONFIG)
    parser.add_argument('--campaign-config',type=Path,default=CAMPAIGN_CONFIG)
    parser.add_argument('--sales-config',type=Path,default=SALES_CONFIG)
    parser.add_argument('--output-dir',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    request = RetrievalRequest.from_dict(json.loads(args.request.read_text()))
    config = json.loads(args.config.read_text());validate_config(config)
    brief_config = json.loads(args.brief_config.read_text())
    campaign_config = json.loads(args.campaign_config.read_text());sales_config = json.loads(args.sales_config.read_text())
    with genai.Client(vertexai=True,project=PROJECT,location=LOCATION,
                      http_options=types.HttpOptions(api_version='v1')) as model_client:
        preview,brief,bundle = generate_preview(bigquery.Client(project=PROJECT,location=LOCATION),
            storage.Client(project=PROJECT),model_client,request,campaign_config,sales_config,brief_config,config)
    output = export_preview(preview,brief,bundle,args.output_dir or ROOT/f'data/generated/banners/{request.canonical_sku}')
    LOG.info(json.dumps(dict(event='banner_preview_verified',sku=request.canonical_sku,output=str(output),
                            sha256=preview.metadata['sha256'],validation=preview.metadata['validation'])))


if __name__ == '__main__':
    main()
