"""Measured structural banner checks; never visual-judge or release authority."""
import hashlib,io,json
from pathlib import Path
from PIL import Image,ImageChops,ImageOps,ImageStat,ImageDraw
from src.application.campaign_evaluator_store import sha
from src.application.campaign_planner_projection import project_planner_response
from src.application.workflow_contract import _require
from src.generators.banner_preview_generator import regions

VERSION='private-banner-structural-measurement-v1'


def measure_banner(png,metadata,*,image,planner_response,options_json,config,scene=None):
    """Recompute dimensions, planned copy/geometry and actual product pixels."""
    projection=project_planner_response(planner_response,options_json)
    draft=json.loads(projection.draft_canonical_json);boxes=regions(draft['layout'])
    _require(type(png) is bytes and metadata['content_sha256']==hashlib.sha256(png).hexdigest()
        and metadata['planner_response_sha256']==projection.response_sha256
        and metadata['planner_projection_sha256']==projection.sha256,'ARTIFACT_MEASUREMENT_BINDING')
    with Image.open(io.BytesIO(png)) as banner,Image.open(io.BytesIO(image.raw)) as original:
        banner.load();original.load()
        _require(banner.format=='PNG' and banner.mode=='RGB','ARTIFACT_MEASUREMENT_FORMAT')
        source=ImageOps.exif_transpose(original).convert('RGB')
        px,py,pw,ph=boxes['product']
        expected=ImageOps.contain(source,(pw-24,ph-24),method=Image.Resampling.LANCZOS)
        bounds=(px+(pw-expected.width)//2,py+(ph-expected.height)//2,expected.width,expected.height)
        x,y,w,h=bounds;actual=banner.crop((x,y,x+w,y+h))
        difference=ImageChops.difference(actual,expected)
        mse=sum(ImageStat.Stat(difference).sum2)/(w*h*3)
        checks=dict(format_dimensions=banner.size==(config['width'],config['height'])
                and [metadata['width'],metadata['height']]==list(banner.size),
            planned_identity_copy=metadata['copy']==dict(headline=draft['headline'],body=draft['body'],cta=draft['cta']),
            planned_geometry=metadata['geometry']==dict(regions={k:list(v) for k,v in boxes.items()},product_bounds=list(bounds)),
            source_product_pixels=mse==0 and actual.tobytes()==expected.tobytes(),
            source_lineage=metadata['image']==json.loads(image.lineage_json),
            font_files=all(metadata['fonts'][k]['sha256']==hashlib.sha256(Path(config[k]).read_bytes()).hexdigest()
                for k in ('heading_font_path','body_font_path')))
        scene_measurements={}
        if scene is not None:
            from src.generators.source_bound_campaign_renderer import GeneratedSceneInput
            _require(type(scene) is GeneratedSceneInput and scene.sku==image.sku
                and scene.source_image_binding_id==image.binding_id,'ARTIFACT_MEASUREMENT_SCENE_SCOPE')
            with Image.open(io.BytesIO(scene.raw)) as generated:
                generated.load();expected_scene=ImageOps.fit(generated.convert('RGB'),banner.size,method=Image.Resampling.LANCZOS)
            mask=Image.new('L',banner.size,255);draw=ImageDraw.Draw(mask)
            for bx,by,bw,bh in list(boxes.values())+[(64,589,1072,20)]:
                draw.rectangle((bx,by,bx+bw-1,by+bh-1),fill=0)
            samples=sum(mask.histogram()[1:]);_require(samples>0,'ARTIFACT_MEASUREMENT_SCENE_EMPTY')
            delta=ImageChops.multiply(ImageChops.difference(banner,expected_scene),mask.convert('RGB'))
            scene_mse=sum(ImageStat.Stat(delta).sum2)/(samples*3)
            checks.update(generated_scene_lineage=metadata.get('generated_scene')==scene.pins(),generated_scene_pixels=scene_mse==0)
            scene_measurements=dict(generated_scene_unobscured_pixels=samples,generated_scene_region_mse=scene_mse)
        else:_require('generated_scene' not in metadata,'ARTIFACT_MEASUREMENT_SCENE_REQUIRED')
    return dict(version=VERSION,status='PASS' if all(checks.values()) else 'FAIL',checks=checks,
        measurements=dict(width=banner.width,height=banner.height,png_bytes=len(png),
            product_pixel_count=w*h,product_region_mse=mse,source_sha256=image.content_sha256,
            planner_projection_sha256=projection.sha256,artifact_sha256=hashlib.sha256(png).hexdigest(),**scene_measurements),
        measurement_sha256=sha(dict(checks=checks,artifact_sha256=hashlib.sha256(png).hexdigest())),
        unavailable_checks=['VISUAL_JUDGE_FIDELITY','BRAND_MARKET_CHANNEL','RENDERED_CLAIMS_AND_SAFETY',
            'FULL_47_CHECK_EVALUATION','QUALIFYING_PIPELINE_EVALUATION'],qualifying=False,release_allowed=False)
