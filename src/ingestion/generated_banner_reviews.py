"""BigQuery generated-banner review history, current state, and POC retrieval gate.

Setup/backfill: python -m src.ingestion.generated_banner_reviews setup --verify
Inspect: append inspect --sku CI-SKU-0001 --asset-id <id>
Decide: append decide --sku <sku> --asset-id <id> --status APPROVED_FOR_POC \
  --reviewer <name> --notes <reason> --expected-revision <current> --request-id <unique-retry-key>
No human approvals are synthesized. System initialization creates pending only.
Review originals and notes remain available independently of serving eligibility.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from src.generators.generated_banner_embeddings import (
    ASSET_TARGET, ASSET_VIEW, EMBEDDING_VIEW, CONFIG as EMBEDDING_CONFIG, download_banner,
    embedding_view_sql, validate_selection,
)
from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.generators.knowledge_chunks_generator import create_view
from src.retrieval.campaign_performance import json_value

CONFIG=ROOT/'config/generated_banner_reviews.json'
HISTORY=f'{PROJECT}.ci_creative.generated_asset_reviews'
STATE=f'{PROJECT}.ci_creative.generated_asset_review_state'
CURRENT=f'{PROJECT}.ci_creative.current_generated_asset_reviews'
CURRENT_SQL=ROOT/'sql/current_generated_asset_reviews.sql'
WRITE_SQL=ROOT/'sql/record_generated_asset_review.sql'
STATUSES=frozenset({'PENDING_REVIEW','APPROVED_FOR_POC','REJECTED'})
SCOPE='POC_VISUAL_REUSE_ONLY'
LOG=logging.getLogger(__name__)
SCHEMA=[bigquery.SchemaField(name,'STRING',mode='REQUIRED') for name in (
    'review_id','request_id','request_sha256','asset_id','sku','review_status','reviewer','notes',
    'decision_origin','approval_scope','source_system','asset_sha256','gcs_uri','gcs_generation',
    'knowledge_snapshot_id')]+[bigquery.SchemaField('supersedes_review_id','STRING')]+[
    bigquery.SchemaField('revision','INTEGER',mode='REQUIRED'),
    bigquery.SchemaField('expected_revision','INTEGER',mode='REQUIRED'),
    bigquery.SchemaField('reviewed_at','TIMESTAMP',mode='REQUIRED')]


def validate_config(config: dict) -> None:
    if (set(config)!={'version','location','maximum_bytes_billed','maximum_notes_characters'}
            or config['version']!='generated-banner-reviews-v1' or config['location']!=LOCATION
            or type(config['maximum_bytes_billed']) is not int
            or not 128*1024*1024<=config['maximum_bytes_billed']<=1024*1024*1024
            or type(config['maximum_notes_characters']) is not int
            or not 1<=config['maximum_notes_characters']<=2000):
        raise ValueError('Invalid regional review configuration')


def query(client: bigquery.Client, sql: str, config: dict, **parameters) -> list[dict]:
    validate_config(config)
    params=[bigquery.ScalarQueryParameter(name,'INT64' if type(value) is int else 'STRING',value)
            for name,value in parameters.items()]
    return [dict(row) for row in client.query(sql,location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=params,maximum_bytes_billed=config['maximum_bytes_billed'])).result()]


def ensure_tables(client: bigquery.Client) -> None:
    for target in (HISTORY,STATE):
        table=bigquery.Table(target,schema=SCHEMA);table.clustering_fields=['asset_id','sku']
        client.create_table(table,exists_ok=True)
        actual=client.get_table(target)
        if [(f.name,f.field_type,f.mode) for f in actual.schema]!=[(f.name,f.field_type,f.mode) for f in SCHEMA]:
            raise ValueError('Review destination schema differs; refusing to alter it')
    create_view(client,CURRENT,CURRENT_SQL.read_text())


def read_asset(client: bigquery.Client, sku: str, asset_id: str, config: dict) -> dict:
    validate_selection(sku,asset_id)
    rows=query(client,f'SELECT * FROM `{ASSET_TARGET}` WHERE sku=@sku AND asset_id=@asset_id',config,sku=sku,asset_id=asset_id)
    if (len(rows)!=1 or rows[0]['source_system']!='SYNTHETIC_POC'
            or rows[0]['status']!='REGISTERED_POC_DRAFT' or rows[0]['real_business_approved'] is not False
            or rows[0]['approval_scope']!='SYNTHETIC_POC_ONLY'):
        raise ValueError('Expected one registered same-SKU generated POC draft')
    return rows[0]


def read_state(client: bigquery.Client, asset_id: str, config: dict) -> dict | None:
    rows=query(client,f'SELECT * FROM `{STATE}` WHERE asset_id=@asset_id',config,asset_id=asset_id)
    if len(rows)>1:
        raise ValueError('Duplicate authoritative review state')
    return rows[0] if rows else None


def review_event(asset: dict, state: dict | None, status: str, reviewer: str, notes: str,
                 request_id: str, expected_revision: int, config: dict, origin: str='HUMAN') -> dict:
    validate_config(config);validate_selection(asset['sku'],asset['asset_id'])
    if (status not in STATUSES or origin not in ('HUMAN','SYSTEM')
            or not isinstance(reviewer,str) or not reviewer.strip() or len(reviewer)>200
            or any(ord(c)<32 for c in reviewer)
            or not isinstance(notes,str) or not notes.strip() or len(notes)>config['maximum_notes_characters']
            or not isinstance(request_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,128}',request_id)
            or type(expected_revision) is not int or expected_revision<0):
        raise ValueError('Expected bounded status, reviewer, notes, revision and a stable request ID')
    if origin=='SYSTEM' and (status!='PENDING_REVIEW' or expected_revision!=0 or state is not None):
        raise ValueError('System initialization can only create pending review')
    if origin=='HUMAN' and (reviewer.upper().startswith('SYSTEM') or state is None):
        raise ValueError('Human decisions require an identified reviewer and initialized current state')
    payload=dict(request_id=request_id,asset_id=asset['asset_id'],sku=asset['sku'],review_status=status,
        reviewer=reviewer.strip(),notes=notes.strip(),decision_origin=origin,approval_scope=SCOPE,
        source_system='SYNTHETIC_POC',asset_sha256=asset['sha256'],gcs_uri=asset['gcs_uri'],
        gcs_generation=asset['gcs_generation'],knowledge_snapshot_id=asset['knowledge_snapshot_id'],
        expected_revision=expected_revision)
    digest=hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    review_id='REV-'+hashlib.sha256((asset['asset_id']+'\0'+request_id).encode()).hexdigest()
    return dict(review_id=review_id,request_sha256=digest,**payload,
        supersedes_review_id=state['review_id'] if state else None,
        revision=expected_revision+1,reviewed_at=datetime.now(timezone.utc))


def store_event(client: bigquery.Client, event: dict, config: dict) -> dict:
    existing=query(client,f'SELECT * FROM `{HISTORY}` WHERE review_id=@review_id',config,review_id=event['review_id'])
    if existing:
        if len(existing)!=1 or existing[0]['request_sha256']!=event['request_sha256']:
            raise ValueError('Review request ID already has a different decision or duplicate records')
        return existing[0]
    destination=client.get_table(HISTORY)
    if [(f.name,f.field_type,f.mode) for f in destination.schema]!=[(f.name,f.field_type,f.mode) for f in SCHEMA]:
        raise ValueError('Review destination schema differs')
    if set(event)!={f.name for f in destination.schema}:
        raise ValueError('Review event differs from destination schema')
    stage=f'{PROJECT}.ci_creative._stage_banner_review_{uuid.uuid4().hex}'
    table=bigquery.Table(stage,schema=destination.schema);table.expires=datetime.now(timezone.utc)+timedelta(hours=1)
    client.create_table(table)
    try:
        client.load_table_from_json([json.loads(json.dumps(event,default=json_value))],stage,location=LOCATION,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,autodetect=False,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY)).result()
        columns=[f.name for f in destination.schema]
        sql=WRITE_SQL.read_text().format(stage=stage,assets=ASSET_TARGET,history=HISTORY,state=STATE,current=CURRENT,grounded=ASSET_VIEW,
            columns=', '.join(columns),updates=', '.join(f'{name}=S.{name}' for name in columns if name!='asset_id'),
            values=', '.join('S.'+name for name in columns))
        query(client,sql,config)
    finally:
        client.delete_table(stage,not_found_ok=True)
    rows=query(client,f'SELECT * FROM `{HISTORY}` WHERE review_id=@review_id',config,review_id=event['review_id'])
    if len(rows)!=1 or rows[0]['request_sha256']!=event['request_sha256']:
        raise ValueError('Stored review differs from the requested event')
    return rows[0]


def initialize_review(client: bigquery.Client, asset: dict, config: dict) -> dict:
    ensure_tables(client)
    state=read_state(client,asset['asset_id'],config)
    if state is not None:
        return state
    event=review_event(asset,None,'PENDING_REVIEW','SYSTEM:REGISTRATION',
        'Registered generated banner awaits human review. No visual-reuse approval is granted.',
        'INITIAL-'+hashlib.sha256(asset['asset_id'].encode()).hexdigest(),0,config,origin='SYSTEM')
    return store_event(client,event,config)


def record_decision(client: bigquery.Client, gcs: storage.Client, sku: str, asset_id: str, status: str,
                    reviewer: str, notes: str, request_id: str, expected_revision: int, config: dict) -> dict:
    asset=read_asset(client,sku,asset_id,config)
    state=read_state(client,asset_id,config)
    event=review_event(asset,state,status,reviewer,notes,request_id,expected_revision,config)
    # Replayed requests return their original event even when another decision is newer.
    existing=query(client,f'SELECT * FROM `{HISTORY}` WHERE review_id=@review_id',config,review_id=event['review_id'])
    if existing:
        if len(existing)!=1 or existing[0]['request_sha256']!=event['request_sha256']:
            raise ValueError('Idempotency key reused for another decision')
        return existing[0]
    if state['revision']!=expected_revision:
        raise ValueError('Stale review revision; inspect the latest decision before submitting again')
    if status=='APPROVED_FOR_POC':
        rows=query(client,f'SELECT * FROM `{ASSET_VIEW}` WHERE sku=@sku AND asset_id=@asset_id',config,sku=sku,asset_id=asset_id)
        if len(rows)!=1 or rows[0]!=asset:
            raise ValueError('Current product, claim and policy grounding is required for POC approval')
        download_banner(gcs,asset)
    return store_event(client,event,config)


def publish_views(client: bigquery.Client, embedding_config: dict) -> None:
    """Create review pointer view; migrate only the known generated embedding view."""
    view=bigquery.Table(CURRENT);view.view_query=CURRENT_SQL.read_text()
    client.create_table(view,exists_ok=True)
    if client.get_table(CURRENT).view_query!=view.view_query:
        raise ValueError('Unexpected current-review view; refusing to replace')
    new=embedding_view_sql(embedding_config)
    # Legacy exact definition is retained as a migration guard, never blindly replaced.
    legacy=(ROOT/'sql/generated_banner_embeddings_before_reviews.sql').read_text().format(
        model=embedding_config['image_model'],dimension=embedding_config['image_dimension'],
        location=embedding_config['location'],version=embedding_config['version'])
    try:
        target=client.get_table(EMBEDDING_VIEW)
    except NotFound:
        target=bigquery.Table(EMBEDDING_VIEW);target.view_query=new;client.create_table(target)
        return
    if target.view_query not in (legacy,new):
        raise ValueError('Unexpected generated embedding view; refusing to replace unrelated SQL')
    if target.view_query!=new:
        target.view_query=new;client.update_table(target,['view_query'])
    if client.get_table(EMBEDDING_VIEW).view_query!=new:
        raise ValueError('Review-gated embedding view failed verification')


def inspect_review(client: bigquery.Client, gcs: storage.Client, sku: str, asset_id: str, config: dict) -> tuple[dict,bytes]:
    asset=read_asset(client,sku,asset_id,config);raw=download_banner(gcs,asset)
    state=read_state(client,asset_id,config)
    history=query(client,f'SELECT * FROM `{HISTORY}` WHERE asset_id=@asset_id ORDER BY revision',config,asset_id=asset_id)
    grounding=query(client,f'SELECT asset_id FROM `{ASSET_VIEW}` WHERE asset_id=@asset_id',config,asset_id=asset_id)
    if read_asset(client,sku,asset_id,config)!=asset or read_state(client,asset_id,config)!=state:
        raise ValueError('Registered banner or review state changed during inspection')
    return dict(source_of_truth='BIGQUERY',original_image_store='GCS',asset=asset,current_review=state,
        review_history=history,current_grounding_eligible=len(grounding)==1,
        review_scope=SCOPE,original_png_verified=True),raw


def verify_review_workflow(client: bigquery.Client, assets: list[dict], config: dict) -> dict:
    """Exercise actual current-pointer and serving SQL with SELECT-only decisions."""
    stats=query(client,(ROOT/'sql/validate_generated_asset_reviews.sql').read_text(),config)[0]
    if (any(stats[key]!=0 for key in ('invalid_pointers','missing_current_reviews','invalid_eligible_reviews'))
            or stats['product_vectors']!=20 or stats['historical_campaign_vectors']!=60):
        raise ValueError(f'Review integrity validation failed: {stats}')
    profile=json.loads(EMBEDDING_CONFIG.read_text())
    current=CURRENT_SQL.read_text().replace(f'`{HISTORY}`','fixture_history').replace(f'`{STATE}`','fixture_state')
    serving=embedding_view_sql(profile).replace(f'`{CURRENT}`','fixture_current')
    outcomes={}
    for asset in assets:
        # Toy review events are SELECT-only derivatives of a real current row.
        # Explicit fixture reviewer labels never enter the source-of-truth tables.
        base=f"""WITH base AS (SELECT * FROM `{STATE}` WHERE asset_id=@asset_id),
          pending AS (SELECT * REPLACE ('fixture-pending' AS review_id,1 AS revision,0 AS expected_revision,
            CAST(NULL AS STRING) AS supersedes_review_id,'PENDING_REVIEW' AS review_status,
            'SYSTEM' AS decision_origin) FROM base),
          approved AS (SELECT * REPLACE ('fixture-approved' AS review_id,2 AS revision,1 AS expected_revision,
            'fixture-pending' AS supersedes_review_id,'APPROVED_FOR_POC' AS review_status,
            'FIXTURE:READ_ONLY' AS reviewer,'HUMAN' AS decision_origin) FROM pending),
          rejected AS (SELECT * REPLACE ('fixture-rejected' AS review_id,3 AS revision,2 AS expected_revision,
            'fixture-approved' AS supersedes_review_id,'REJECTED' AS review_status) FROM approved),
          revoked AS (SELECT * REPLACE ('fixture-revoked' AS review_id,3 AS revision,2 AS expected_revision,
            'fixture-approved' AS supersedes_review_id,'PENDING_REVIEW' AS review_status) FROM approved),
        """
        cases={
            'pending':('SELECT * FROM pending','SELECT * FROM pending',0),
            'approved':('SELECT * FROM pending UNION ALL SELECT * FROM approved','SELECT * FROM approved',1),
            'rejected':('SELECT * FROM pending UNION ALL SELECT * FROM approved UNION ALL SELECT * FROM rejected','SELECT * FROM rejected',0),
            'revoked':('SELECT * FROM pending UNION ALL SELECT * FROM approved UNION ALL SELECT * FROM revoked','SELECT * FROM revoked',0),
            'stale_pointer':('SELECT * FROM pending UNION ALL SELECT * FROM approved UNION ALL SELECT * FROM rejected','SELECT * FROM approved',0),
            'duplicate_pointer':('SELECT * FROM pending UNION ALL SELECT * FROM approved','SELECT * FROM approved UNION ALL SELECT * FROM approved',0),
            'duplicate_event':('SELECT * FROM pending UNION ALL SELECT * FROM approved UNION ALL SELECT * FROM approved','SELECT * FROM approved',0),
        }
        for name,replacement in {
            'system_approval':"'SYSTEM' AS decision_origin",
            'wrong_scope':"'MANUFACTURER_APPROVED' AS approval_scope",
            'wrong_image':"REPEAT('0',64) AS asset_sha256",
            'wrong_generation':"'1' AS gcs_generation",
            'wrong_snapshot':"'stale' AS knowledge_snapshot_id",
            'cross_sku':"IF(sku='CI-SKU-0001','CI-SKU-0002','CI-SKU-0001') AS sku",
            'new_revision_asset':"CONCAT('SYN-GEN-',sku,'-',REPEAT('0',64)) AS asset_id",
        }.items():
            select=f'SELECT * REPLACE ({replacement}) FROM approved'
            cases[name]=(select,select,0)
        asset_outcomes={}
        for name,(history,state,expected) in cases.items():
            sql=(base+f'fixture_history AS ({history}),fixture_state AS ({state}), '
                 +f'fixture_current AS ({current}),fixture_eligible AS ({serving}) '
                 +'SELECT COUNT(*) n FROM fixture_eligible WHERE asset_id=@asset_id')
            count=query(client,sql,config,asset_id=asset['asset_id'])[0]['n']
            if count!=expected:
                raise ValueError(f'Review eligibility probe {name} failed: {count} != {expected}')
            asset_outcomes[name]=count
        actual=query(client,f'SELECT COUNT(*) n FROM `{EMBEDDING_VIEW}` WHERE asset_id=@asset_id',config,
                     asset_id=asset['asset_id'])[0]['n']
        state=read_state(client,asset['asset_id'],config)
        if state['review_status']!='APPROVED_FOR_POC' and actual!=0:
            raise ValueError('An unapproved banner leaked into creative retrieval')
        outcomes[asset['asset_id']]=dict(probes=asset_outcomes,actual_eligible_vectors=actual,
            actual_status=state['review_status'],revision=state['revision'])
    return dict(integrity=stats,read_only_probes=outcomes)


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=CONFIG)
    sub=parser.add_subparsers(dest='command',required=True)
    setup=sub.add_parser('setup');setup.add_argument('--verify',action='store_true')
    for name in ('inspect','decide'):
        child=sub.add_parser(name);child.add_argument('--sku',required=True);child.add_argument('--asset-id',required=True)
        child.add_argument('--output-dir',type=Path)
        if name=='decide':
            child.add_argument('--status',choices=sorted(STATUSES),required=True)
            child.add_argument('--reviewer',required=True);child.add_argument('--notes',required=True)
            child.add_argument('--expected-revision',type=int,required=True);child.add_argument('--request-id',required=True)
    args=parser.parse_args();logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    config=json.loads(args.config.read_text());validate_config(config)
    client=bigquery.Client(project=PROJECT,location=LOCATION)
    if args.command=='setup':
        ensure_tables(client)
        assets=query(client,f"SELECT * FROM `{ASSET_TARGET}` WHERE source_system='SYNTHETIC_POC'",config)
        states=[initialize_review(client,asset,config) for asset in assets]
        publish_views(client,json.loads(EMBEDDING_CONFIG.read_text()))
        report=dict(initialized_assets=len(states),current_statuses=[s['review_status'] for s in states])
        if args.verify:
            report['verification']=verify_review_workflow(client,assets,config)
    else:
        gcs=storage.Client(project=PROJECT)
        if args.command=='decide':
            decision=record_decision(client,gcs,args.sku,args.asset_id,args.status,args.reviewer,args.notes,
                                     args.request_id,args.expected_revision,config)
            report=dict(recorded_review=decision,current_review=read_state(client,args.asset_id,config))
        else:
            report,raw=inspect_review(client,gcs,args.sku,args.asset_id,config)
    run=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output=(getattr(args,'output_dir',None) or ROOT/'data/generated/reviews')/run;output.mkdir(parents=True,exist_ok=False)
    if args.command=='inspect':
        (output/'banner.png').write_bytes(raw)
    (output/'review_report.json').write_text(json.dumps(report,indent=2,default=json_value))
    LOG.info(json.dumps(dict(event='banner_review_'+args.command,output=str(output),**report),default=json_value)
             if args.command=='setup' else json.dumps(dict(event='banner_review_'+args.command,output=str(output))))


if __name__=='__main__':
    main()
