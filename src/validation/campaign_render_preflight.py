"""Offline/cloud-image font and renderer verification; synthetic identity only."""
import argparse,hashlib,io,json
from datetime import datetime,timezone,timedelta
from pathlib import Path
import PIL
from PIL import Image,ImageFont
from src.application.campaign_artifact_measurement import measure_banner
from src.application.campaign_evaluator_store import canonical
from src.application.campaign_planner_runtime import identity_options
from src.application.campaign_quality_checks import SourceFact
from src.application.campaign_quality_product_reader import ProductFactRead
from src.application.workflow_contract import _require
from src.generators.source_bound_campaign_renderer import ProductImageInput,render_identity_campaign


def verify(config_path=Path('config/evaluation/rejuvenate_campaign_render.json')):
    config=json.loads(Path(config_path).read_text());provenance=json.loads(Path('assets/fonts/campaign/provenance.json').read_text())
    fonts={v['file']:v for v in provenance['files']};actual={}
    for key in ('heading_font_path','body_font_path'):
        path=Path(config[key]);pin=fonts[path.name]
        digest=hashlib.sha256(path.read_bytes()).hexdigest()
        _require(digest==pin['sha256'] and ImageFont.truetype(str(path),24).getlength('Rejuvenate')>0,'CAMPAIGN_FONT_PIN_CHANGED')
        _require(b'SIL OPEN FONT LICENSE' in path.with_name(path.stem+'-OFL.txt').read_bytes(),'CAMPAIGN_FONT_LICENSE_MISSING')
        actual[key]=dict(path=str(path),sha256=digest)
    now=datetime.now(timezone.utc);fields=dict(product_name='Rejuvenate Restorative Face Oil')
    product=ProductFactRead(SourceFact('synthetic-font-preflight','CI-SKU-0022',canonical(fields),now,now+timedelta(seconds=60),True),'synthetic-font-preflight')
    raw=io.BytesIO();Image.new('RGB',(460,760),'#C88B37').save(raw,format='PNG');png=raw.getvalue();digest=hashlib.sha256(png).hexdigest()
    image=ProductImageInput('CI-SKU-0022','synthetic-font-image',digest,canonical(dict(sku='CI-SKU-0022',binding_id='synthetic-font-image',content_sha256=digest)),png)
    options=identity_options(fields);base={k:v[0] for k,v in json.loads(options).items()};layouts={}
    for layout in json.loads(options)['layout']:
        response=canonical(dict(base,layout=layout))
        rendered=render_identity_campaign(product=product,image=image,planner_response=response,options_json=options,checked_at=now,config=config)
        score=measure_banner(rendered.png,json.loads(rendered.metadata_json),image=image,planner_response=response,options_json=options,config=config)
        _require(score['status']=='PASS','CAMPAIGN_RENDER_PREFLIGHT_FAILED')
        layouts[layout]=dict(status='PASS',png_sha256=hashlib.sha256(rendered.png).hexdigest(),product_region_mse=score['measurements']['product_region_mse'])
    return dict(status='PASS',pillow_version=PIL.__version__,fonts=actual,layouts=layouts,
        scope='SYNTHETIC_FONT_AND_RENDER_CONFIGURATION_ONLY',model_calls=0,qualifying=False,release_allowed=False)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--config',type=Path,default=Path('config/evaluation/rejuvenate_campaign_render.json'))
    print(json.dumps(verify(p.parse_args().config)))
