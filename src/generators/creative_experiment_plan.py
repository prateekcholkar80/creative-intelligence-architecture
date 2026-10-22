"""Draft an unlaunched POC layout experiment from current BigQuery/GCS evidence.

Run: python -m src.generators.creative_experiment_plan
Config selects approved assets and expresses planning preferences only.
Local exports never supply serving evidence. No cloud writes or model calls.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage

from src.generators.campaign_brief_generator import LAYOUTS
from src.generators.generated_banner_embeddings import CONFIG as EMBEDDING_CONFIG, validate_selection
from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.ingestion.generated_banner_reviews import CONFIG as REVIEW_CONFIG, read_asset
from src.retrieval.campaign_performance import json_value
from src.retrieval.generated_banner import retrieve_generated_banner
from src.retrieval.request import RetrievalRequest

CONFIG = ROOT/'config/creative_experiment_plan.json'
LOG = logging.getLogger(__name__)


def encode(value: object) -> str:
    return json.dumps(value,sort_keys=True,separators=(',',':'),default=json_value,allow_nan=False)


def validate_config(config: dict) -> None:
    if (not isinstance(config,dict) or set(config) != {'version','canonical_sku','control_asset_id',
            'variant_asset_id','allocation_percent','primary_metric'}
            or config['version'] != 'creative-experiment-plan-v1'
            or config['primary_metric'] != 'CONVERSION_RATE'
            or config['allocation_percent'] != {'A':50,'B':50}
            or any(type(value) is not int for value in config['allocation_percent'].values())):
        raise ValueError('Configure an equal-allocation POC conversion-rate experiment')
    for field in ('control_asset_id','variant_asset_id'):
        validate_selection(config['canonical_sku'],config[field])
    if config['control_asset_id'] == config['variant_asset_id']:
        raise ValueError('Control and variant must be distinct approved assets')


def assemble_plan(config: dict, assets: list[dict], retrieved: list[tuple[dict,bytes]]) -> dict:
    """Validate a controlled pair; use stored BQ creative choices, never cached facts."""
    validate_config(config)
    if len(assets) != 2 or len(retrieved) != 2:
        raise ValueError('Expected exactly two approved experiment arms')
    briefs=[]
    for label,field,asset,(metadata,raw) in zip(('A','B'),('control_asset_id','variant_asset_id'),assets,retrieved):
        if (asset['asset_id'] != config[field] or asset['sku'] != config['canonical_sku']
                or metadata['asset_id'] != asset['asset_id'] or metadata['canonical_sku'] != asset['sku']
                or asset['source_system'] != 'SYNTHETIC_POC' or asset['real_business_approved'] is not False
                or asset['status'] != 'REGISTERED_POC_DRAFT' or asset['approval_scope'] != 'SYNTHETIC_POC_ONLY'
                or metadata['source_of_truth'] != 'BIGQUERY' or metadata['source_system'] != 'SYNTHETIC_POC'
                or metadata['review']['status'] != 'APPROVED_FOR_POC'
                or metadata['review']['approval_scope'] != 'POC_VISUAL_REUSE_ONLY'
                or not metadata['review']['review_id'] or type(metadata['review']['revision']) is not int
                or metadata['review']['revision'] < 1 or not metadata['verification']
                or any(value is not True for value in metadata['verification'].values())
                or metadata['gcs_generation'] != asset['gcs_generation'] or metadata['gcs_uri'] != asset['gcs_uri']
                or metadata['content_sha256'] != asset['sha256']
                or metadata['snapshot_id'] != asset['knowledge_snapshot_id']
                or metadata['original_product_image'] != dict(asset_id=asset['source_image_asset_id'],
                    gcs_uri=asset['source_image_uri'],gcs_generation=asset['source_image_generation'],
                    content_sha256=asset['source_image_sha256'])
                or hashlib.sha256(raw).hexdigest() != asset['sha256'] or len(raw) != asset['byte_count']):
            raise ValueError(f'Arm {label} is unapproved, mismatched or lacks verified originals')
        brief=json.loads(asset['brief_json'])
        request=RetrievalRequest.from_dict(brief['request'])
        if (request.canonical_sku != asset['sku'] or request.objective != 'CONVERSION'
                or any(getattr(request,key) != asset[key] for key in ('objective','channel','audience'))
                or brief['copy']['headline'] != asset['headline'] or brief['copy']['body'] != asset['body_copy']
                or brief['copy']['cta'] != asset['call_to_action']
                or brief['copy']['claim_ids'] != asset['approved_claim_ids']):
            raise ValueError('Stored creative proposal must agree with canonical approved asset fields')
        briefs.append(brief)
    control,variant=assets
    invariant_fields=('sku','source_system','approval_scope','objective','channel','audience','headline',
        'body_copy','call_to_action','approved_claim_ids','source_image_asset_id','source_image_uri',
        'source_image_generation','source_image_sha256','width','height','mime_type','generator_version',
        'description_method','knowledge_snapshot_id')
    if any(control[key] != variant[key] for key in invariant_fields):
        raise ValueError('Experiment arms differ in copy, targeting, product image or rendering profile')
    first,second=briefs
    a,b=first['model_selection'],second['model_selection']
    if (set(a) != set(b) or sorted(key for key in a if a[key] != b[key]) != ['layout']
            or a['layout'] not in LAYOUTS or b['layout'] not in LAYOUTS
            or a['test_variable'] != 'LAYOUT' or first['request'] != second['request']
            or any(first['visual_direction'][key] != second['visual_direction'][key]
                   for key in ('background','text_color','typography'))
            or any(brief['visual_direction']['layout_id'] != draft['layout']
                   for brief,draft in zip(briefs,(a,b)))
            or second['model'].get('selection_method') != 'DETERMINISTIC_LAYOUT_VARIANT'
            or second['model'].get('parent_asset_id') != control['asset_id']):
        raise ValueError('Layout must be the only changed creative choice')
    if control['sha256'] == variant['sha256']:
        raise ValueError('Experiment images must be distinct')
    arms=[]
    for label,role,asset,brief,(metadata,_) in zip(('A','B'),('CONTROL','VARIANT'),assets,briefs,retrieved):
        arms.append(dict(arm=label,role=role,asset_id=asset['asset_id'],allocation_percent=config['allocation_percent'][label],
            layout_id=brief['model_selection']['layout'],layout=LAYOUTS[brief['model_selection']['layout']],
            gcs_uri=asset['gcs_uri'],gcs_generation=asset['gcs_generation'],sha256=asset['sha256'],
            review=copy.deepcopy(metadata['review']),embedding=copy.deepcopy(metadata['embedding']),
            verified_original=True))
    references=sorted({ref for asset,(metadata,_) in zip(assets,retrieved) for ref in (
        f'{PROJECT}.ci_creative.generated_campaign_assets#asset_id={asset["asset_id"]}',
        f'{PROJECT}.ci_creative.generated_asset_reviews#review_id={metadata["review"]["review_id"]}',
        f'{PROJECT}.ci_brand_memory.image_embeddings#embedding_id={metadata["embedding"]["embedding_id"]}',
        f'{PROJECT}.ci_product.product_assets#asset_id={asset["source_image_asset_id"]}',
        *(f'{PROJECT}.ci_brand_memory.product_claims#claim_id={claim}' for claim in asset['approved_claim_ids']))})
    plan=dict(version=config['version'],canonical_sku=config['canonical_sku'],source_system='SYNTHETIC_POC',
        evidence_source_of_truth='BIGQUERY',original_image_store='GCS',status='DRAFT_NOT_LAUNCHED',
        approval_scope='SYNTHETIC_POC_ONLY',experiment_approval_status='NOT_REVIEWED',
        knowledge_snapshot_id=control['knowledge_snapshot_id'],request=copy.deepcopy(first['request']),
        test_variable='LAYOUT',hypothesis='Changing the product/copy layout may change click-to-conversion rate.',
        arms=arms,copy=dict(headline=control['headline'],body=control['body_copy'],cta=control['call_to_action'],
                           approved_claim_ids=copy.deepcopy(control['approved_claim_ids'])),
        visual_invariants={key:copy.deepcopy(first['visual_direction'][key]) for key in ('background','text_color','typography')},
        product_image={key:control[key] for key in ('source_image_asset_id','source_image_uri','source_image_generation','source_image_sha256')},
        primary_metric=dict(name='CONVERSION_RATE',numerator='conversions',denominator='clicks',
            aggregation='SUM(conversions) / NULLIF(SUM(clicks), 0)',zero_denominator_result=None,
            definition_reference='sql/retrieve_campaign_performance.sql#conversion_rate'),
        delivery_plan=dict(status='PROPOSED_NOT_CONFIGURED',allocation='50/50',
            audience=control['audience'],channel=control['channel'],same_budget_and_schedule_required=True,
            budget_per_arm=None,currency=None,start_date=None,end_date=None,
            randomization='Proposed: mutually exclusive random audience assignment; keep assignment stable.',
            randomization_unit=None,attribution_window=None,conversion_event=None,sample_size=None,
            minimum_detectable_effect=None,stopping_rule=None),
        results=dict(A=None,B=None,winner=None,observed_lift=None,statistical_conclusion=None),
        prerequisites=['Choose budget, currency and a common schedule.',
            'Define assignment unit, delivery controls, conversion event and attribution window.',
            'Specify sample size and stopping rule before launch.',
            'Configure the platform experiment, tracking and delivery settings before launch.'],
        limitations=['These are SYNTHETIC_POC creatives; the plan has not been launched.',
            'Layout includes positioning, available text width, wrapping and product display size.',
            'Typography choices match; licensed font files are not supplied by the visual reference.',
            'Click-to-conversion rate measures outcomes among clicks; no causal or performance conclusion is supplied.',
            'No historical metrics or cosine distances are assigned as results for these banners.',
            'Exported files are review copies; re-read BigQuery eligibility before registration or launch.'],
        evidence_references=references,cloud_writes=False,model_calls=0)
    # Content identity excludes volatile retrieval timestamps; changes to reviews
    # or source identity create a distinct draft. This is not a live assignment ID.
    plan['plan_sha256']=hashlib.sha256(encode(plan).encode()).hexdigest()
    plan['experiment_id']='SYN-EXP-'+config['canonical_sku']+'-'+plan['plan_sha256']
    return plan


def generate_plan(client: bigquery.Client, gcs: storage.Client, config: dict,
                  embedding_config: dict, review_config: dict) -> tuple[dict,dict[str,bytes]]:
    validate_config(config)
    ids=[config['control_asset_id'],config['variant_asset_id']]
    assets=[read_asset(client,config['canonical_sku'],asset_id,review_config) for asset_id in ids]
    retrieved=[retrieve_generated_banner(client,gcs,config['canonical_sku'],asset_id,embedding_config) for asset_id in ids]
    plan=assemble_plan(config,assets,retrieved)
    # Recheck the whole pair after all downloads and assembly. A revocation or
    # identity change aborts export, rather than relying on an earlier approval.
    for asset_id,asset,(metadata,raw) in zip(ids,assets,retrieved):
        latest,new_raw=retrieve_generated_banner(client,gcs,config['canonical_sku'],asset_id,embedding_config)
        if (latest != metadata or new_raw != raw
                or read_asset(client,config['canonical_sku'],asset_id,review_config) != asset):
            raise ValueError('Experiment evidence or review changed during planning')
    plan.update(approval_recheck_passed=True,evidence_checked_at=datetime.now(timezone.utc))
    return plan,{label:raw for label,(_,raw) in zip(('A','B'),retrieved)}


def render_plan(plan: dict) -> str:
    lines=[f'A/B EXPERIMENT PLAN — {plan["canonical_sku"]}',
        'SYNTHETIC POC — DRAFT_NOT_LAUNCHED',f'Experiment: {plan["experiment_id"]}',
        f'Objective: {plan["request"]["objective"]}; channel: {plan["request"]["channel"]}; audience: {plan["request"]["audience"]}',
        '',f'Test variable: {plan["test_variable"]}',f'Hypothesis (untested): {plan["hypothesis"]}']
    for arm in plan['arms']:
        lines.extend([f'{arm["arm"]} ({arm["role"]}): {arm["layout"]}',
            f'  {arm["asset_id"]}; proposed allocation {arm["allocation_percent"]}%; POC review revision {arm["review"]["revision"]}.'])
    lines.extend(['',f'Headline: {plan["copy"]["headline"]}',f'Body: {plan["copy"]["body"]}',f'CTA: {plan["copy"]["cta"]}',
        'Copy, product source, palette, typography, audience and channel match.',
        'Primary metric: conversions / clicks, aggregated per arm; undefined when clicks are zero.',
        'Delivery proposal: 50/50; same budget and schedule required; neither is configured.',
        'Budget, dates, assignment unit, conversion event, attribution window, sample size and stopping rule: unset.',
        'Results, winner, observed lift and statistical conclusion: unset.','',
        'Before launch:',*plan['prerequisites'],'','Limitations:',*plan['limitations'],'',
        'Evidence references:',*plan['evidence_references']])
    return '\n'.join(lines)+'\n'


def export_plan(plan: dict, images: dict[str,bytes], directory: Path) -> Path:
    if plan.get('approval_recheck_passed') is not True or set(images) != {'A','B'}:
        raise ValueError('Rechecked evidence and two original images are required')
    for arm in plan['arms']:
        if hashlib.sha256(images[arm['arm']]).hexdigest() != arm['sha256']:
            raise ValueError('Experiment original image changed before export')
    identity={key:value for key,value in plan.items() if key not in
              ('plan_sha256','experiment_id','approval_recheck_passed','evidence_checked_at')}
    if hashlib.sha256(encode(identity).encode()).hexdigest() != plan['plan_sha256']:
        raise ValueError('Experiment plan changed after validation')
    if plan['experiment_id'] != 'SYN-EXP-'+plan['canonical_sku']+'-'+plan['plan_sha256']:
        raise ValueError('Experiment identity differs')
    payload=json.dumps(plan,indent=2,default=json_value,allow_nan=False);text=render_plan(plan)
    run=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output=directory/run;output.mkdir(parents=True,exist_ok=False)
    for label,raw in images.items():
        (output/f'{label}_banner.png').write_bytes(raw)
    (output/'experiment_plan.txt').write_text(text)
    destination=output/'experiment_plan.json';destination.write_text(payload)
    return destination


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--output-dir',type=Path)
    args=parser.parse_args();logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    config=json.loads(args.config.read_text());validate_config(config)
    plan,images=generate_plan(bigquery.Client(project=PROJECT,location=LOCATION),storage.Client(project=PROJECT),
        config,json.loads(EMBEDDING_CONFIG.read_text()),json.loads(REVIEW_CONFIG.read_text()))
    output=export_plan(plan,images,args.output_dir or ROOT/f'data/generated/experiment_plans/{config["canonical_sku"]}')
    LOG.info(json.dumps(dict(event='experiment_plan_verified',output=str(output),experiment_id=plan['experiment_id'],
        status=plan['status'],arms=2,cloud_writes=False,model_calls=0)))


if __name__ == '__main__':
    main()
