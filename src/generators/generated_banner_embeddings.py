"""Embed registered POC banners and publish fail-closed generated-only eligibility.

Run: python -m src.generators.generated_banner_embeddings --sku CI-SKU-0001 --asset-id <registered-id>
BigQuery is the only vector cache. Original bytes are verified in GCS; this loader
never reads local banner/embedding exports and never changes historical views.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import math
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.cloud import bigquery, storage
from PIL import Image

from src.generators.embeddings_generator import (
    IMAGE_TARGET, SCHEMA, VertexEmbedder, record_identity, validate_cached, validate_vector,
)
from src.generators.knowledge_chunks_generator import create_view
from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT
from src.generators.campaign_assets_generator import BUCKET
from src.retrieval.campaign_performance import json_value

CONFIG = ROOT/'config/generated_banner_embeddings.json'
ASSET_TARGET = f'{PROJECT}.ci_creative.generated_campaign_assets'
ASSET_VIEW = f'{PROJECT}.ci_brand_memory.poc_generated_banner_assets'
EMBEDDING_VIEW = f'{PROJECT}.ci_brand_memory.poc_generated_banner_embeddings'
ASSET_SQL = ROOT/'sql/eligible_generated_banner_assets.sql'
EMBEDDING_SQL = ROOT/'sql/eligible_generated_banner_embeddings.sql'
VALIDATION_SQL = ROOT/'sql/validate_generated_banner_embeddings.sql'
LOG = logging.getLogger(__name__)


def validate_config(config: dict) -> None:
    if (set(config) != {'version','location','image_model','image_dimension','request_interval_seconds','max_attempts','maximum_bytes_billed'}
            or config['version'] != 'generated-banner-embeddings-v1' or config['location'] != LOCATION
            or config['image_model'] != 'multimodalembedding@001'
            or type(config['image_dimension']) is not int or config['image_dimension'] != 512
            or type(config['max_attempts']) is not int or not 1 <= config['max_attempts'] <= 8
            or type(config['request_interval_seconds']) not in (int,float)
            or not math.isfinite(config['request_interval_seconds']) or not 0 <= config['request_interval_seconds'] <= 60
            or type(config['maximum_bytes_billed']) is not int or config['maximum_bytes_billed'] <= 0):
        raise ValueError('Expected bounded regional generated-banner image embedding settings')


def validate_selection(sku: str, asset_id: str) -> None:
    if sku not in EXPECTED_SKUS or not isinstance(asset_id,str) or not re.fullmatch(rf'SYN-GEN-{re.escape(sku)}-[a-f0-9]{{64}}',asset_id):
        raise ValueError('A canonical SKU and its registered generated banner ID are required')


def publish_asset_view(client: bigquery.Client) -> None:
    create_view(client,ASSET_VIEW,ASSET_SQL.read_text())


def embedding_view_sql(config: dict) -> str:
    validate_config(config)
    return EMBEDDING_SQL.read_text().format(model=config['image_model'],dimension=config['image_dimension'],
                                          location=config['location'],version=config['version'])


def read_eligible_asset(client: bigquery.Client, sku: str, asset_id: str, config: dict) -> dict:
    validate_selection(sku,asset_id);validate_config(config)
    params = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('sku','STRING',sku),
        bigquery.ScalarQueryParameter('asset_id','STRING',asset_id)],maximum_bytes_billed=config['maximum_bytes_billed'])
    rows = [dict(r) for r in client.query(f'SELECT * FROM `{ASSET_VIEW}` WHERE sku=@sku AND asset_id=@asset_id',
                                         location=LOCATION,job_config=params).result()]
    if len(rows) != 1:
        raise ValueError('Registered banner is missing, duplicate, stale or no longer POC-eligible')
    return rows[0]


def image_source(asset: dict, raw: bytes) -> dict:
    sku = asset['sku'];validate_selection(sku,asset['asset_id'])
    if (asset['source_system'] != 'SYNTHETIC_POC' or asset['status'] != 'REGISTERED_POC_DRAFT'
            or asset['approval_scope'] != 'SYNTHETIC_POC_ONLY' or asset['real_business_approved'] is not False
            or asset['asset_role'] != 'GENERATED_CAMPAIGN_BANNER' or asset['mime_type'] != 'image/png'
            or asset['generator_version'] != 'grounded-banner-preview-v1'
            or asset['asset_id'] != f'SYN-GEN-{sku}-{asset["sha256"]}'
            or asset['gcs_uri'] != f'gs://{BUCKET}/generated_campaigns/{sku}/{asset["generator_version"]}/{asset["sha256"]}.png'
            or not isinstance(asset['gcs_generation'],str) or not re.fullmatch(r'[1-9][0-9]*',asset['gcs_generation'])
            or (asset['width'],asset['height']) != (1200,628) or len(raw) != asset['byte_count']
            or hashlib.sha256(raw).hexdigest() != asset['sha256']):
        raise ValueError('Invalid generated POC source identity, generation, dimensions or SHA-256')
    with Image.open(io.BytesIO(raw)) as image:
        if image.format != 'PNG' or image.size != (asset['width'],asset['height']):
            raise ValueError('Stored banner format/dimensions disagree with BigQuery')
        image.verify()
    return dict(modality='IMAGE',source_id=asset['asset_id'],source_system='SYNTHETIC_POC',sku=sku,
        campaign_id=None,source_reference=f'{ASSET_TARGET}#asset_id={asset["asset_id"]}',
        content_sha256=asset['sha256'],gcs_uri=asset['gcs_uri'],gcs_generation=asset['gcs_generation'],payload=raw)


def download_banner(gcs: storage.Client, asset: dict) -> bytes:
    expected = f'gs://{BUCKET}/generated_campaigns/{asset["sku"]}/{asset["generator_version"]}/{asset["sha256"]}.png'
    if asset['gcs_uri'] != expected:
        raise ValueError('Unexpected banner path')
    blob = gcs.bucket(BUCKET).blob(asset['gcs_uri'].removeprefix(f'gs://{BUCKET}/'))
    blob.reload()
    if (str(blob.generation) != asset['gcs_generation'] or blob.content_type != 'image/png'
            or blob.size != asset['byte_count']):
        raise ValueError('Current GCS banner differs from registered generation/MIME/size')
    raw = blob.download_as_bytes(if_generation_match=int(asset['gcs_generation']))
    image_source(asset,raw)
    return raw


def read_cloud_embedding(client: bigquery.Client, identity: dict, config: dict) -> dict | None:
    destination = client.get_table(IMAGE_TARGET)
    if {(f.name,f.field_type,f.mode) for f in destination.schema} != {(f.name,f.field_type,f.mode) for f in SCHEMA}:
        raise ValueError('Existing image embedding schema differs')
    params = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('embedding_id','STRING',identity['embedding_id'])],
                                   maximum_bytes_billed=config['maximum_bytes_billed'])
    rows = [dict(r) for r in client.query(f'SELECT * FROM `{IMAGE_TARGET}` WHERE embedding_id=@embedding_id',
                                         location=LOCATION,job_config=params).result()]
    if len(rows)>1:
        raise ValueError('Duplicate authoritative embedding IDs')
    return validate_cached(rows[0],identity) if rows else None


def load_embedding(client: bigquery.Client, row: dict, config: dict) -> dict:
    identity = record_identity(row,config);row = validate_cached(row,identity)
    destination = client.get_table(IMAGE_TARGET)
    if {(f.name,f.field_type,f.mode) for f in destination.schema} != {(f.name,f.field_type,f.mode) for f in SCHEMA}:
        raise ValueError('Destination image embedding schema differs')
    columns = [f.name for f in destination.schema]
    stage = f'{PROJECT}.ci_brand_memory._stage_generated_banner_embedding_{uuid.uuid4().hex}'
    table = bigquery.Table(stage,schema=destination.schema)
    table.expires = datetime.now(timezone.utc)+timedelta(hours=1)
    client.create_table(table)
    try:
        client.load_table_from_json([json.loads(json.dumps(row,default=json_value))],stage,location=LOCATION,
            job_config=bigquery.LoadJobConfig(schema=destination.schema,autodetect=False,
                write_disposition=bigquery.WriteDisposition.WRITE_EMPTY)).result()
        # Existing immutable vectors win during a concurrent run; never replace them.
        sql = f"""BEGIN TRANSACTION;
          ASSERT NOT EXISTS(SELECT T.embedding_id FROM `{IMAGE_TARGET}` T JOIN `{stage}` S
            USING(embedding_id,source_system) GROUP BY T.embedding_id HAVING COUNT(*)>1) AS 'Duplicate embedding keys';
          ASSERT EXISTS(SELECT 1 FROM `{stage}` S JOIN `{ASSET_VIEW}` A
            ON A.asset_id=S.source_id AND A.sku=S.sku AND A.sha256=S.content_sha256
              AND A.gcs_uri=S.gcs_uri AND A.gcs_generation=S.gcs_generation) AS 'Banner eligibility changed';
          MERGE `{IMAGE_TARGET}` T USING `{stage}` S
          ON T.embedding_id=S.embedding_id AND T.source_system=S.source_system
          WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({', '.join('S.'+c for c in columns)});
          COMMIT TRANSACTION;"""
        client.query(sql,location=LOCATION,job_config=bigquery.QueryJobConfig(maximum_bytes_billed=512*1024*1024)).result()
    finally:
        client.delete_table(stage,not_found_ok=True)
    stored = read_cloud_embedding(client,identity,config)
    if stored is None:
        raise ValueError('Generated banner vector was not stored in BigQuery')
    return stored


def embed_banner(client: bigquery.Client, gcs: storage.Client, embedder, sku: str, asset_id: str, config: dict) -> dict:
    validate_selection(sku,asset_id);validate_config(config)
    publish_asset_view(client)
    asset = read_eligible_asset(client,sku,asset_id,config)
    raw = download_banner(gcs,asset);source = image_source(asset,raw);identity = record_identity(source,config)
    row = read_cloud_embedding(client,identity,config)
    called = row is None
    if called:
        row = dict(**identity,vector=validate_vector(embedder(source),config['image_dimension']),created_at=datetime.now(timezone.utc))
    # Fail if any registered metadata/policy/eligibility changed during the model call.
    if read_eligible_asset(client,sku,asset_id,config) != asset:
        raise ValueError('Banner state changed while embedding; retry against current BigQuery')
    download_banner(gcs,asset)
    if called:
        row = load_embedding(client,row,config)
    create_view(client,EMBEDDING_VIEW,embedding_view_sql(config))
    params = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('asset_id','STRING',asset_id)],
                                   maximum_bytes_billed=config['maximum_bytes_billed'])
    stats = dict(next(iter(client.query(VALIDATION_SQL.read_text(),location=LOCATION,job_config=params).result())))
    if (stats.get('eligible_vectors') not in (0,1) or {k:v for k,v in stats.items() if k!='eligible_vectors'}
            != dict(stored_vectors=1,invalid_vectors=0,product_vectors=20,
                    historical_campaign_vectors=60,leaked_historical_vectors=0)):
        raise ValueError(f'Generated banner vector/eligibility validation failed: {stats}')
    report = dict(asset_id=asset_id,canonical_sku=sku,embedding_id=row['embedding_id'],dimension=row['dimension'],
        model_id=row['model_id'],model_location=row['model_location'],model_calls=int(called),cloud_reused=not called,
        content_sha256=row['content_sha256'],gcs_generation=row['gcs_generation'],embedding_created_at=row['created_at'],
        source_of_truth='BIGQUERY',source_system='SYNTHETIC_POC',retrieval_eligible=bool(stats['eligible_vectors']),
        retrieval_status='GENERATED_POC_VISUAL_REFERENCE' if stats['eligible_vectors'] else 'NOT_REVIEW_APPROVED',
        validation=stats,verified_at=datetime.now(timezone.utc))
    LOG.info(json.dumps(dict(event='generated_banner_embedding_verified',**report),default=json_value))
    return report


def verify_eligibility_filters(client: bigquery.Client, asset_id: str, config: dict) -> dict:
    """Exercise the actual SQL with SELECT-only altered fixtures, not DB mutations."""
    params = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('asset_id','STRING',asset_id)],
                                   maximum_bytes_billed=config['maximum_bytes_billed'])
    outcomes = {}
    cases = {
        'current_draft':None,
        'rejected_status':"'REJECTED' AS status",
        'real_business_approval':"TRUE AS real_business_approved",
        'unapproved_claim':"['NOT_APPROVED'] AS approved_claim_ids",
        'empty_policy':"'[]' AS policy_json",
        'stale_snapshot':"'stale' AS knowledge_snapshot_id",
        'stale_original_image':"'1' AS source_image_generation",
        'invalid_generation':"'0' AS gcs_generation",
        'invented_benefit_copy':"'Removes wrinkles' AS body_copy",
        'wrong_approval_scope':"'MANUFACTURER_APPROVED' AS approval_scope",
    }
    template = ASSET_SQL.read_text().replace(f'`{ASSET_TARGET}`','candidate_assets')
    for name,replacement in cases.items():
        select = '*' if replacement is None else f'* REPLACE ({replacement})'
        sql = (f'WITH candidate_assets AS (SELECT {select} FROM `{ASSET_TARGET}` WHERE asset_id=@asset_id), '
               f'eligible AS ({template}) SELECT COUNT(*) n FROM eligible')
        count = next(iter(client.query(sql,location=LOCATION,job_config=params).result()))['n']
        if count != (1 if replacement is None else 0):
            raise ValueError(f'Eligibility filter probe failed: {name} returned {count}')
        outcomes[name] = count
    # Removing current approved evidence must also exclude an otherwise unchanged row.
    claims = f'{PROJECT}.ci_brand_memory.poc_approved_product_claims'
    sql = (f'WITH candidate_assets AS (SELECT * FROM `{ASSET_TARGET}` WHERE asset_id=@asset_id), '
           f'current_claims AS (SELECT * FROM `{claims}` WHERE FALSE), eligible AS ('
           +template.replace(f'`{claims}`','current_claims')+') SELECT COUNT(*) n FROM eligible')
    count = next(iter(client.query(sql,location=LOCATION,job_config=params).result()))['n']
    if count != 0:
        raise ValueError('Eligibility did not exclude revoked current approval evidence')
    outcomes['revoked_current_approval'] = count
    vector_template = embedding_view_sql(config).replace(f'`{IMAGE_TARGET}`','candidate_vectors')
    for name,replacement in {
        'wrong_vector_dimension':'768 AS dimension',
        'wrong_vector_model':"'other-model' AS model_id",
        'stale_vector_generation':"'1' AS gcs_generation",
        'zero_vector':'ARRAY(SELECT CAST(0 AS FLOAT64) FROM UNNEST(vector)) AS vector',
    }.items():
        sql = (f'WITH candidate_vectors AS (SELECT * REPLACE ({replacement}) FROM `{IMAGE_TARGET}` '
               f'WHERE source_id=@asset_id), eligible AS ({vector_template}) SELECT COUNT(*) n FROM eligible')
        count = next(iter(client.query(sql,location=LOCATION,job_config=params).result()))['n']
        if count != 0:
            raise ValueError(f'Vector eligibility filter probe failed: {name}')
        outcomes[name] = count
    LOG.info(json.dumps(dict(event='generated_banner_eligibility_filters_verified',outcomes=outcomes)))
    return outcomes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True);parser.add_argument('--asset-id',required=True)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--verify-filters',action='store_true',help='Run SELECT-only negative eligibility probes')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    config = json.loads(args.config.read_text());validate_config(config);validate_selection(args.sku,args.asset_id)
    embedder = VertexEmbedder(config)
    try:
        client = bigquery.Client(project=PROJECT,location=LOCATION)
        report = embed_banner(client,storage.Client(project=PROJECT),
                              embedder,args.sku,args.asset_id,config)
        if args.verify_filters:
            report['eligibility_filter_probes'] = verify_eligibility_filters(client,args.asset_id,config)
    finally:
        embedder.text_client.close();embedder.session.close()
    output = args.output or ROOT/f'data/generated/embeddings/{args.asset_id}_validation.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report,indent=2,default=json_value))


if __name__ == '__main__':
    main()
