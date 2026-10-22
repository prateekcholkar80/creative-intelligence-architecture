"""Bounded metadata-only private image receipts across the owned-worker channel."""
import math,re
from src.application.sharing_host_contract import require
from src.application.campaign_evaluator_observer import USAGE

METHODS=frozenset({'vertex_visual_run','vertex_visual_recover','vertex_banner_run','vertex_banner_recover'})
FLAGS=('claim_approval','generation_authorized','qualifying','release_allowed')


def digest(value):require(type(value) is str and re.fullmatch('[a-f0-9]{64}',value))


def original(value,prefix,slot):
    require(type(value) is dict and set(value)=={'bucket','object_name','generation'}
        and value['bucket']=='ci-campaign-evidence-1042059373096'
        and value['object_name']=='campaign-evidence/'+prefix+slot
        and type(value['generation']) is str and re.fullmatch('[1-9][0-9]{0,24}',value['generation']))


def decode(method,value):
    require(method in METHODS and type(value) is dict and set(value)=={'run_id','case_id','phase','crop_binding_id','visual',
        'semantic_reranking','retrieval_use','artifact',*FLAGS}
        and value['phase']==('VISUAL' if method.startswith('vertex_visual') else 'BANNER')
        and all(value[k] is False for k in FLAGS)
        and value['semantic_reranking']=='NOT_EXECUTED'
        and value['retrieval_use']=='POST_PLANNER_PRIVATE_CALIBRATION_NOT_IN_PLANNER_INPUT'
        and type(value['crop_binding_id']) is str and re.fullmatch('eval_image_derivation_v1_[a-f0-9]{64}',value['crop_binding_id']))
    for k in ('run_id','case_id'):require(type(value[k]) is str and re.fullmatch('[A-Za-z0-9_-]{1,255}',value[k]))
    visual=value['visual']
    require(type(visual) is dict and set(visual)=={'record_id','outcome_sha256','response_sha256','returned_model_id','usage','original','content_sha256',*FLAGS}
        and all(visual[k] is False for k in FLAGS) and visual['returned_model_id']=='gemini-3.1-flash-image'
        and type(visual['usage']) is dict and set(visual['usage'])==set(USAGE)
        and all(n is None or type(n) is int and 0<=n<2**63 for n in visual['usage'].values()))
    for k in ('record_id','outcome_sha256','response_sha256','content_sha256'):digest(visual[k])
    original(visual['original'],'vertex_visual_v1_',visual['record_id'])
    artifact=value['artifact']
    if value['phase']=='VISUAL':require(artifact is None);return value
    require(type(artifact) is dict and set(artifact)=={'artifact_record_id','artifact_outcome_sha256','scoring_record_id',
        'scoring_outcome_sha256','original','content_sha256','measurements','qualifying','release_allowed'}
        and artifact['qualifying'] is False and artifact['release_allowed'] is False)
    for k in ('artifact_record_id','artifact_outcome_sha256','scoring_record_id','scoring_outcome_sha256','content_sha256'):digest(artifact[k])
    original(artifact['original'],'campaign_artifact_v1_',artifact['artifact_record_id'])
    score=artifact['measurements']
    require(type(score) is dict and set(score)=={'version','status','checks','measurements','measurement_sha256','unavailable_checks','qualifying','release_allowed'}
        and score['version']=='private-banner-structural-measurement-v1' and score['status'] in {'PASS','FAIL'}
        and score['qualifying'] is False and score['release_allowed'] is False
        and type(score['checks']) is dict and set(score['checks'])=={'format_dimensions','planned_identity_copy',
            'planned_geometry','source_product_pixels','source_lineage','font_files','generated_scene_lineage','generated_scene_pixels'}
        and all(type(b) is bool for b in score['checks'].values())
        and (score['status']=='PASS')==all(score['checks'].values())
        and score['unavailable_checks']==['VISUAL_JUDGE_FIDELITY','BRAND_MARKET_CHANNEL','RENDERED_CLAIMS_AND_SAFETY',
            'FULL_47_CHECK_EVALUATION','QUALIFYING_PIPELINE_EVALUATION'])
    digest(score['measurement_sha256']);m=score['measurements']
    require(type(m) is dict and set(m)=={'width','height','png_bytes','product_pixel_count','product_region_mse',
        'source_sha256','planner_projection_sha256','artifact_sha256','generated_scene_unobscured_pixels','generated_scene_region_mse'})
    for k in ('width','height','png_bytes','product_pixel_count','generated_scene_unobscured_pixels'):
        require(type(m[k]) is int and 0<m[k]<=20*1024**2)
    for k in ('product_region_mse','generated_scene_region_mse'):
        require(type(m[k]) in (float,int) and math.isfinite(m[k]) and 0<=m[k]<=255**2)
    for k in ('source_sha256','planner_projection_sha256','artifact_sha256'):digest(m[k])
    require(m['artifact_sha256']==artifact['content_sha256'])
    return value
