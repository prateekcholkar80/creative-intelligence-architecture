"""Register a freshly rebuilt POC experiment draft, never a local factual cache."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import uuid
from datetime import datetime,timedelta,timezone
from pathlib import Path

from google.cloud import bigquery,storage

from src.generators.creative_experiment_plan import CONFIG,encode,generate_plan
from src.generators.generated_banner_embeddings import CONFIG as EMBEDDING_CONFIG
from src.generators.knowledge_chunks_generator import create_view
from src.generators.sales_generator import PROJECT,LOCATION,ROOT
from src.ingestion.generated_banner_reviews import CONFIG as REVIEW_CONFIG,query
from src.retrieval.campaign_performance import json_value

TARGET=f'{PROJECT}.ci_creative.creative_experiments'
VIEW=f'{PROJECT}.ci_creative.current_creative_experiments'
ARM_SCHEMA=[bigquery.SchemaField(name,'STRING',mode='REQUIRED') for name in (
    'arm','role','asset_id','review_id','gcs_uri','gcs_generation','content_sha256','embedding_id','layout_id')]+[
    bigquery.SchemaField('allocation_percent','INTEGER',mode='REQUIRED'),
    bigquery.SchemaField('review_revision','INTEGER',mode='REQUIRED')]
SCHEMA=[bigquery.SchemaField(name,'STRING',mode='REQUIRED') for name in (
    'experiment_id','sku','source_system','status','approval_scope','plan_sha256','plan_json',
    'knowledge_snapshot_id','objective','channel','audience','test_variable','primary_metric')]+[
    bigquery.SchemaField('arms','RECORD',mode='REPEATED',fields=ARM_SCHEMA),
    bigquery.SchemaField('real_business_approved','BOOLEAN',mode='REQUIRED'),
    bigquery.SchemaField('budget_per_arm','NUMERIC'),bigquery.SchemaField('currency','STRING'),
    bigquery.SchemaField('start_date','DATE'),bigquery.SchemaField('end_date','DATE'),
    bigquery.SchemaField('winner','STRING'),
    bigquery.SchemaField('created_at','TIMESTAMP',mode='REQUIRED'),
    bigquery.SchemaField('evidence_checked_at','TIMESTAMP',mode='REQUIRED')]
LOG=logging.getLogger(__name__)


def validate_row(row: dict) -> dict:
    """Stored JSON is authoritative BQ evidence, but its content identity is checked."""
    plan=json.loads(row['plan_json'])
    identity={k:v for k,v in plan.items() if k not in
              ('plan_sha256','experiment_id','approval_recheck_passed','evidence_checked_at')}
    digest=hashlib.sha256(encode(identity).encode()).hexdigest()
    if (digest!=row['plan_sha256'] or plan['plan_sha256']!=digest
            or row['experiment_id']!='SYN-EXP-'+row['sku']+'-'+digest
            or plan['experiment_id']!=row['experiment_id'] or plan['canonical_sku']!=row['sku']
            or row['source_system']!='SYNTHETIC_POC' or row['status']!='DRAFT_NOT_LAUNCHED'
            or row['approval_scope']!='SYNTHETIC_POC_ONLY' or row['real_business_approved'] is not False
            or plan['status']!=row['status'] or plan['source_system']!=row['source_system']
            or plan['knowledge_snapshot_id']!=row['knowledge_snapshot_id']
            or any(plan['request'][key]!=row[key] for key in ('objective','channel','audience'))
            or row['test_variable']!='LAYOUT' or row['primary_metric']!='CONVERSION_RATE'
            or plan['test_variable']!=row['test_variable'] or plan['primary_metric']['name']!=row['primary_metric']
            or any(row[key] is not None for key in ('budget_per_arm','currency','start_date','end_date','winner'))
            or any(plan['delivery_plan'][key] is not None for key in ('budget_per_arm','currency','start_date',
                'end_date','randomization_unit','conversion_event','attribution_window','sample_size',
                'minimum_detectable_effect','stopping_rule'))
            or plan['approval_scope']!='SYNTHETIC_POC_ONLY'
            or plan['approval_recheck_passed'] is not True
            or any(value is not None for value in plan['results'].values()) or len(row['arms'])!=2):
        raise ValueError('Stored experiment metadata or content identity differs from its unlaunched plan')
    expected=[]
    for arm in plan['arms']:
        expected.append(dict(arm=arm['arm'],role=arm['role'],asset_id=arm['asset_id'],
            review_id=arm['review']['review_id'],review_revision=arm['review']['revision'],
            gcs_uri=arm['gcs_uri'],gcs_generation=arm['gcs_generation'],content_sha256=arm['sha256'],
            embedding_id=arm['embedding']['embedding_id'],layout_id=arm['layout_id'],
            allocation_percent=arm['allocation_percent']))
    if (row['arms']!=expected or [a['arm'] for a in expected]!=['A','B']
            or [a['role'] for a in expected]!=['CONTROL','VARIANT']
            or [a['allocation_percent'] for a in expected]!=[50,50]
            or len({a['asset_id'] for a in expected})!=2):
        raise ValueError('Stored experiment arms differ from the plan')
    return plan


def registration_row(plan: dict) -> dict:
    if plan.get('approval_recheck_passed') is not True:
        raise ValueError('Fresh approval recheck is required')
    stable={k:v for k,v in plan.items() if k!='evidence_checked_at'}
    row=dict(experiment_id=plan['experiment_id'],sku=plan['canonical_sku'],source_system='SYNTHETIC_POC',
        status='DRAFT_NOT_LAUNCHED',approval_scope='SYNTHETIC_POC_ONLY',plan_sha256=plan['plan_sha256'],
        plan_json=encode(stable),knowledge_snapshot_id=plan['knowledge_snapshot_id'],**{
            key:plan['request'][key] for key in ('objective','channel','audience')},
        test_variable='LAYOUT',primary_metric='CONVERSION_RATE',arms=[dict(
            arm=a['arm'],role=a['role'],asset_id=a['asset_id'],review_id=a['review']['review_id'],
            review_revision=a['review']['revision'],gcs_uri=a['gcs_uri'],gcs_generation=a['gcs_generation'],
            content_sha256=a['sha256'],embedding_id=a['embedding']['embedding_id'],layout_id=a['layout_id'],
            allocation_percent=a['allocation_percent']) for a in plan['arms']],real_business_approved=False,
        budget_per_arm=None,currency=None,start_date=None,end_date=None,winner=None,
        created_at=datetime.now(timezone.utc),evidence_checked_at=plan['evidence_checked_at'])
    validate_row(row)
    return row


def load_draft(client: bigquery.Client,row: dict,config: dict) -> dict:
    validate_row(row)
    table=bigquery.Table(TARGET,schema=SCHEMA);table.clustering_fields=['sku','status']
    client.create_table(table,exists_ok=True);destination=client.get_table(TARGET)
    if [f.to_api_repr() for f in destination.schema]!=[f.to_api_repr() for f in SCHEMA]:
        raise ValueError('Experiment destination schema differs; refusing to alter it')
    create_view(client,VIEW,(ROOT/'sql/current_creative_experiments.sql').read_text())
    stage=f'{PROJECT}.ci_creative._stage_creative_experiment_{uuid.uuid4().hex}'
    staging=bigquery.Table(stage,schema=destination.schema);staging.expires=datetime.now(timezone.utc)+timedelta(hours=1)
    client.create_table(staging)
    try:
        client.load_table_from_json([json.loads(encode(row))],stage,location=LOCATION,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,autodetect=False,
                                             write_disposition=bigquery.WriteDisposition.WRITE_EMPTY)).result()
        columns=[f.name for f in destination.schema]
        immutable=[c for c in columns if c not in ('created_at','evidence_checked_at')]
        comparisons=[('TO_JSON_STRING(T.arms) IS DISTINCT FROM TO_JSON_STRING(S.arms)' if c=='arms'
                      else f'T.{c} IS DISTINCT FROM S.{c}') for c in immutable]
        sql=(ROOT/'sql/register_creative_experiment.sql').read_text().format(
            stage=stage,target=TARGET,immutable_mismatch=' OR '.join(comparisons),
            columns=', '.join(columns),values=', '.join('S.'+c for c in columns))
        query(client,sql,config)
    finally:
        client.delete_table(stage,not_found_ok=True)
    stats=query(client,(ROOT/'sql/validate_creative_experiment.sql').read_text(),config,
                experiment_id=row['experiment_id'])[0]
    if stats!=dict(experiment_rows=1,invalid_drafts=0,stale_evidence=0,eligible_arms=2):
        raise ValueError(f'Experiment verification failed: {stats}')
    stored=query(client,f'SELECT * FROM `{TARGET}` WHERE experiment_id=@experiment_id AND source_system=\'SYNTHETIC_POC\'',
                 config,experiment_id=row['experiment_id'])
    if len(stored)!=1 or any(stored[0][c]!=row[c] for c in immutable):
        raise ValueError('Registered experiment differs from the validated plan')
    validate_row(stored[0])
    return stored[0]


def register_plan(client: bigquery.Client,gcs: storage.Client,config: dict,
                  embedding_config: dict,review_config: dict,expected_id: str | None=None) -> dict:
    plan,_=generate_plan(client,gcs,config,embedding_config,review_config)
    if expected_id is not None and plan['experiment_id']!=expected_id:
        raise ValueError('Current evidence differs from the reviewed draft; inspect a fresh plan')
    return load_draft(client,registration_row(plan),review_config)


def verify_evidence_gate(client: bigquery.Client,experiment_id: str,config: dict) -> dict:
    """SELECT-only fixtures exercise the actual current experiment view SQL."""
    template=(ROOT/'sql/current_creative_experiments.sql').read_text().rstrip().removesuffix(';').replace(
        f'`{TARGET}`','candidate_experiments')
    cases={'current_draft':None,'wrong_status':"'LAUNCHED' AS status",
        'wrong_scope':"'MANUFACTURER_APPROVED' AS approval_scope",'real_approval':'TRUE AS real_business_approved',
        'cross_sku':"'CI-SKU-0002' AS sku",'stale_snapshot':"'old' AS knowledge_snapshot_id"}
    for name,expression in {
        'stale_review':'arm.review_revision+1 AS review_revision',
        'stale_image_generation':"'1' AS gcs_generation",
        'stale_image_hash':"REPEAT('0',64) AS content_sha256",
        'wrong_embedding':"'OTHER' AS embedding_id",
    }.items():
        cases[name]=f'ARRAY(SELECT AS STRUCT arm.* REPLACE ({expression}) FROM UNNEST(arms) arm) AS arms'
    outcomes={}
    for name,replacement in cases.items():
        select='*' if replacement is None else f'* REPLACE ({replacement})'
        sql=(f'WITH candidate_experiments AS (SELECT {select} FROM `{TARGET}` WHERE experiment_id=@experiment_id), '
             f'current_view AS ({template}) SELECT COUNTIF(current_evidence_eligible) n FROM current_view')
        count=query(client,sql,config,experiment_id=experiment_id)[0]['n']
        if count!=(1 if name=='current_draft' else 0):
            raise ValueError(f'Experiment evidence gate probe failed: {name}')
        outcomes[name]=count
    return outcomes


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--expected-experiment-id')
    parser.add_argument('--verify-gates',action='store_true')
    parser.add_argument('--output-dir',type=Path,default=ROOT/'data/generated/experiment_registrations')
    args=parser.parse_args();logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    client=bigquery.Client(project=PROJECT,location=LOCATION)
    review_config=json.loads(REVIEW_CONFIG.read_text())
    row=register_plan(client,storage.Client(project=PROJECT),
        json.loads(args.config.read_text()),json.loads(EMBEDDING_CONFIG.read_text()),
        review_config,args.expected_experiment_id)
    run=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output=args.output_dir/run;output.mkdir(parents=True,exist_ok=False)
    (output/'registration_receipt.json').write_text(json.dumps(row,indent=2,default=json_value))
    if args.verify_gates:
        probes=verify_evidence_gate(client,row['experiment_id'],review_config)
        (output/'evidence_gate_verification.json').write_text(json.dumps(probes,indent=2))
    LOG.info(json.dumps(dict(event='experiment_registered',experiment_id=row['experiment_id'],
                             status=row['status'],output=str(output))))


if __name__=='__main__':
    main()
