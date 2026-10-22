"""Embed eligible current knowledge and original GCS images with Vertex AI ADC.

Run: python -m src.generators.embeddings_generator
Inventory only (no model calls or cloud writes): append --dry-run.
Text and image vectors occupy separate spaces; never compare them directly.
Completed responses are checkpointed individually, then scoped BigQuery MERGEs
use explicit destination schemas. Unchanged reruns reuse local or cloud results.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import google.auth
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, storage
from google import genai
from google.genai import types

from src.common.bq_merge import upsert
from src.generators.campaign_assets_generator import BUCKET
from src.generators.knowledge_chunks_generator import ELIGIBLE_VIEW, VISUAL_VIEW, create_view
from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT, SOURCE

DATASET = f'{PROJECT}.ci_brand_memory'
TEXT_TARGET = f'{DATASET}.text_embeddings'
IMAGE_TARGET = f'{DATASET}.image_embeddings'
LOG = logging.getLogger(__name__)
SCHEMA = [bigquery.SchemaField(n, 'STRING', mode='REQUIRED') for n in (
    'embedding_id', 'modality', 'source_id', 'source_system', 'source_reference',
    'content_sha256', 'model_id', 'model_location', 'task_type', 'generator_version')
] + [bigquery.SchemaField(n, 'STRING') for n in ('sku', 'campaign_id', 'gcs_uri', 'gcs_generation')] + [
    bigquery.SchemaField('dimension', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('vector', 'FLOAT', mode='REPEATED'),
    bigquery.SchemaField('created_at', 'TIMESTAMP', mode='REQUIRED')]


def validate_config(config: dict) -> None:
    if (config['location'] != LOCATION or config['text_model'] != 'gemini-embedding-001'
            or config['image_model'] != 'multimodalembedding@001'
            or config['text_task'] != 'RETRIEVAL_DOCUMENT'
            or config['text_dimension'] not in (768, 1536, 3072)
            or config['image_dimension'] not in (128, 256, 512, 1408)):
        raise ValueError('Unsupported regional embedding configuration')
    if (not re.fullmatch(r'[A-Za-z0-9_-]+', config['version'])
            or not isinstance(config['max_attempts'], int) or not 1 <= config['max_attempts'] <= 8
            or not 0 <= config['request_interval_seconds'] <= 60):
        raise ValueError('Invalid retry/version configuration')
    directory = (ROOT / config['cache_directory']).resolve()
    if not directory.is_relative_to(ROOT / 'data/generated'):
        raise ValueError('Cache must remain inside data/generated')


def validate_vector(vector: list[float], dimension: int) -> list[float]:
    if len(vector) != dimension or any(isinstance(v, bool) or not isinstance(v, (int, float))
            or not math.isfinite(v) for v in vector):
        raise ValueError('Invalid embedding dimension or nonfinite values')
    values = [float(v) for v in vector]
    if math.fsum(v*v for v in values) <= 0:
        raise ValueError('Zero embedding vector')
    return values


def text_source(chunk: dict) -> dict:
    kind = chunk['chunk_type']
    if kind == 'WEBSITE_VISUAL_REFERENCE':
        if (chunk['retrieval_status'] != 'VISUAL_REFERENCE' or chunk['sku'] is not None
                or chunk['claim_id'] is not None or chunk['brand'] != 'Naked & Thriving'
                or chunk['approval_scope'] != 'VISUAL_INSPIRATION_ONLY'
                or chunk['source_system'] != 'OFFICIAL_BRAND_WEBSITE'):
            raise ValueError('Invalid visual-only reference')
    elif (kind not in {'PRODUCT_KNOWLEDGE', 'INGREDIENT_KNOWLEDGE', 'POC_RULE',
                        'POC_APPROVED_CLAIM', 'CAMPAIGN_CREATIVE', 'CAMPAIGN_LEARNING'}
            or chunk['retrieval_status'] not in {'FACTUAL_CONTEXT', 'POC_POLICY', 'POC_CONTEXT', 'POC_APPROVED'}):
        raise ValueError('Review notes and raw daily data must not be embedded')
    if chunk['sku'] is not None and chunk['sku'] not in EXPECTED_SKUS:
        raise ValueError('Unknown SKU')
    digest = hashlib.sha256(chunk['chunk_text'].encode()).hexdigest()
    if digest != chunk['content_sha256'] or not chunk['chunk_text'].strip():
        raise ValueError('Text source hash mismatch')
    return dict(modality='TEXT', source_id=chunk['chunk_id'], source_system=chunk['source_system'],
        source_reference=chunk['source_reference'], content_sha256=digest, sku=chunk['sku'],
        campaign_id=chunk['campaign_id'], gcs_uri=None, gcs_generation=None,
        payload=chunk['chunk_text'])


def image_source(asset: dict, generation: int, raw: bytes) -> dict:
    sku = asset['sku']; uri = asset['gcs_uri']
    if sku not in EXPECTED_SKUS or not raw or generation <= 0 or not asset['asset_id']:
        raise ValueError('Invalid image identity or bytes')
    if asset['source_system'] == 'BEAUTYFEEDS':
        if uri != f'gs://{BUCKET}/products/{sku}/primary.jpg' or asset['campaign_id'] is not None:
            raise ValueError('Product image differs from canonical evidence')
    elif asset['source_system'] == SOURCE:
        if (not asset['campaign_id'] or not uri.startswith(f'gs://{BUCKET}/campaigns/{asset["campaign_id"]}/template-v1/')
                or not asset['sha256']):
            raise ValueError('Campaign image differs from canonical evidence')
    else:
        raise ValueError('Unsupported image provenance')
    digest = hashlib.sha256(raw).hexdigest()
    if asset.get('sha256') and digest != asset['sha256']:
        raise ValueError('GCS image does not match asset hash')
    return dict(modality='IMAGE', source_id=asset['asset_id'], source_system=asset['source_system'],
        source_reference=asset['source_reference'], content_sha256=digest, sku=sku,
        campaign_id=asset['campaign_id'], gcs_uri=uri, gcs_generation=str(generation), payload=raw)


def record_identity(source: dict, config: dict) -> dict:
    image = source['modality'] == 'IMAGE'
    row = {k:source[k] for k in ('modality','source_id','source_system','source_reference',
        'content_sha256','sku','campaign_id','gcs_uri','gcs_generation')}
    row.update(model_id=config['image_model' if image else 'text_model'], model_location=config['location'],
        dimension=config['image_dimension' if image else 'text_dimension'],
        task_type='IMAGE' if image else config['text_task'], generator_version=config['version'])
    encoded = json.dumps(row, sort_keys=True, separators=(',', ':')).encode()
    return dict(embedding_id='EMB-'+hashlib.sha256(encoded).hexdigest(), **row)


def validate_cached(row: dict, identity: dict) -> dict:
    if set(row) != {f.name for f in SCHEMA} or any(row.get(k) != v for k,v in identity.items()):
        raise ValueError('Cached embedding identity/schema mismatch')
    result = dict(row)
    result['vector'] = validate_vector(list(row['vector']), identity['dimension'])
    if isinstance(result['created_at'], str):
        result['created_at'] = datetime.fromisoformat(result['created_at'])
    if not isinstance(result['created_at'], datetime) or result['created_at'].tzinfo is None:
        raise ValueError('Cached timestamp requires a timezone')
    return result


def embed_or_reuse(source: dict, config: dict, cloud_cache: dict, directory: Path,
                   embed: Callable[[dict], list[float]]) -> tuple[dict, bool]:
    identity = record_identity(source, config)
    identifier = identity['embedding_id']
    path = directory / (identifier+'.json')
    if identifier in cloud_cache:
        return validate_cached(cloud_cache[identifier], identity), False
    if path.exists():
        return validate_cached(json.loads(path.read_text()), identity), False
    vector = validate_vector(embed(source), identity['dimension'])
    row = dict(**identity, vector=vector, created_at=datetime.now(timezone.utc))
    directory.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(row, default=str))
    temporary.replace(path)
    return row, True


class VertexEmbedder:
    """ADC only. Retry transient responses; reject truncation and invalid vectors."""
    def __init__(self, config: dict):
        self.config = config
        self.text_client = genai.Client(vertexai=True, project=PROJECT, location=config['location'],
            http_options=types.HttpOptions(api_version='v1'))
        credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        self.session = AuthorizedSession(credentials)
        self.last_request = 0.0

    def __call__(self, source: dict) -> list[float]:
        for attempt in range(self.config['max_attempts']):
            time.sleep(max(0, self.config['request_interval_seconds']-(time.monotonic()-self.last_request)))
            self.last_request = time.monotonic()
            try:
                if source['modality'] == 'TEXT':
                    response = self.text_client.models.embed_content(model=self.config['text_model'],
                        contents=source['payload'], config=types.EmbedContentConfig(
                            task_type=self.config['text_task'], output_dimensionality=self.config['text_dimension'],
                            auto_truncate=False, http_options=types.HttpOptions(timeout=60000)))
                    if len(response.embeddings or []) != 1:
                        raise ValueError('Expected one text embedding')
                    result = response.embeddings[0]
                    if result.statistics and result.statistics.truncated:
                        raise ValueError('Text was truncated')
                    return list(result.values or [])
                url = (f'https://{self.config["location"]}-aiplatform.googleapis.com/v1/projects/{PROJECT}'
                    f'/locations/{self.config["location"]}/publishers/google/models/{self.config["image_model"]}:predict')
                response = self.session.post(url, json={
                    'instances':[{'image':{'bytesBase64Encoded':base64.b64encode(source['payload']).decode()}}],
                    'parameters':{'dimension':self.config['image_dimension']}}, timeout=60)
                response.raise_for_status()
                predictions = response.json()['predictions']
                if len(predictions) != 1:
                    raise ValueError('Expected one image embedding')
                return predictions[0]['imageEmbedding']
            except Exception as error:
                code = getattr(error, 'code', None) or getattr(getattr(error, 'response', None), 'status_code', None)
                if code not in (429, 500, 502, 503, 504) or attempt+1 == self.config['max_attempts']:
                    raise
                LOG.warning(json.dumps({'event':'embedding_retry', 'attempt':attempt+1, 'code':code}))
                time.sleep(min(2**attempt, 30))
        raise RuntimeError('Retries exhausted')


def read_sources(client: bigquery.Client, gcs: storage.Client) -> list[dict]:
    def query(sql: str) -> list[dict]:
        return [dict(r) for r in client.query(sql, location=LOCATION).result()]
    chunks = query(f'SELECT * FROM `{ELIGIBLE_VIEW}` UNION ALL SELECT * FROM `{VISUAL_VIEW}`')
    if not chunks or len({c['chunk_id'] for c in chunks}) != len(chunks):
        raise ValueError('Unique eligible current chunks required')
    sources = [text_source(c) for c in chunks]
    products = query(f"""SELECT a.asset_id,a.sku,a.gcs_uri,a.source_system,
        CAST(NULL AS STRING) campaign_id,CAST(NULL AS STRING) sha256,
        CONCAT('{PROJECT}.ci_product.product_assets#asset_id=',a.asset_id) source_reference
        FROM `{PROJECT}.ci_product.product_assets` a
        JOIN `{PROJECT}.ci_product.product_master` p USING(sku)
        WHERE a.source_system='BEAUTYFEEDS' AND p.source_system='BEAUTYFEEDS'
          AND a.is_primary AND a.gcs_uri=p.primary_image_uri""")
    # Assets must be referenced by the current eligible snapshot, not an arbitrary campaign period.
    campaigns = query(f"""SELECT DISTINCT a.asset_id,a.sku,a.gcs_uri,a.source_system,a.campaign_id,a.sha256,
        CONCAT('{PROJECT}.ci_creative.campaign_assets#asset_id=',a.asset_id) source_reference
        FROM `{PROJECT}.ci_creative.campaign_assets` a JOIN `{ELIGIBLE_VIEW}` k
          ON a.asset_id=k.asset_id AND a.sku=k.sku AND a.campaign_id=k.campaign_id
        WHERE a.source_system='SYNTHETIC_POC' AND a.generator_version='template-v1'
          AND a.gcs_uri IN UNNEST(k.image_uris)""")
    if (len(products) != 20 or {p['sku'] for p in products} != EXPECTED_SKUS
            or len(campaigns) != 60 or len({c['asset_id'] for c in campaigns}) != 60
            or any(sum(c['sku']==sku for c in campaigns) != 3 for sku in EXPECTED_SKUS)):
        raise ValueError('Expected 20 existing primary images and 60 current campaign banners')
    for asset in products+campaigns:
        prefix = f'gs://{BUCKET}/'
        if not asset['gcs_uri'].startswith(prefix):
            raise ValueError('Unexpected image bucket')
        blob = gcs.bucket(BUCKET).blob(asset['gcs_uri'].removeprefix(prefix))
        blob.reload()
        generation = int(blob.generation)
        raw = blob.download_as_bytes(if_generation_match=generation)
        sources.append(image_source(asset, generation, raw))
    return sorted(sources, key=lambda s:(s['modality'],s['source_id']))


def cloud_cache(client: bigquery.Client, target: str) -> dict:
    try:
        table = client.get_table(target)
    except NotFound:
        return {}
    if {(f.name,f.field_type,f.mode) for f in table.schema} != {(f.name,f.field_type,f.mode) for f in SCHEMA}:
        raise ValueError('Destination embedding schema differs')
    rows = [dict(r) for r in client.query(f'SELECT * FROM `{target}`', location=LOCATION).result()]
    if len({r['embedding_id'] for r in rows}) != len(rows):
        raise ValueError('Duplicate cached embedding IDs')
    return {r['embedding_id']:r for r in rows}


def publish_views(client: bigquery.Client, config: dict) -> None:
    # Fixed model names and region are validated before interpolation.
    for name, view in (('poc_generation_text_embeddings',ELIGIBLE_VIEW),
                       ('poc_visual_reference_embeddings',VISUAL_VIEW)):
        create_view(client,f'{DATASET}.{name}',f"""SELECT k.*,e.embedding_id,e.vector,e.model_id,e.dimension,
            e.model_location,e.task_type FROM `{view}` k JOIN `{TEXT_TARGET}` e
            ON k.chunk_id=e.source_id AND k.source_system=e.source_system AND k.content_sha256=e.content_sha256
            WHERE e.modality='TEXT' AND e.model_id='{config['text_model']}'
              AND e.dimension={config['text_dimension']} AND e.task_type='{config['text_task']}'
              AND e.model_location='{config['location']}' AND e.generator_version='{config['version']}'""")
    create_view(client,f'{DATASET}.poc_image_embeddings',f"""WITH current_assets AS (
        SELECT a.asset_id,a.sku,a.gcs_uri,a.source_system,CAST(NULL AS STRING) campaign_id,
          CAST(NULL AS STRING) sha256 FROM `{PROJECT}.ci_product.product_assets` a
        JOIN `{PROJECT}.ci_product.product_master` p USING(sku)
        WHERE a.is_primary AND a.source_system='BEAUTYFEEDS' AND p.source_system='BEAUTYFEEDS'
          AND a.gcs_uri=p.primary_image_uri
        UNION ALL SELECT DISTINCT a.asset_id,a.sku,a.gcs_uri,a.source_system,a.campaign_id,a.sha256
        FROM `{PROJECT}.ci_creative.campaign_assets` a JOIN `{ELIGIBLE_VIEW}` k
          ON a.asset_id=k.asset_id AND a.sku=k.sku AND a.campaign_id=k.campaign_id
        WHERE a.source_system='SYNTHETIC_POC' AND a.generator_version='template-v1'
          AND a.gcs_uri IN UNNEST(k.image_uris)
      ) SELECT e.* FROM `{IMAGE_TARGET}` e JOIN current_assets a
        ON a.asset_id=e.source_id AND a.sku=e.sku AND a.gcs_uri=e.gcs_uri
          AND a.source_system=e.source_system AND (a.sha256 IS NULL OR a.sha256=e.content_sha256)
      WHERE e.modality='IMAGE' AND e.model_id='{config['image_model']}'
        AND e.dimension={config['image_dimension']} AND e.task_type='IMAGE'
        AND e.model_location='{config['location']}' AND e.generator_version='{config['version']}'
      QUALIFY ROW_NUMBER() OVER(PARTITION BY e.source_id,e.source_system
        ORDER BY CAST(e.gcs_generation AS BIGNUMERIC) DESC,e.created_at DESC)=1""")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,default=ROOT/'config/embeddings.json')
    parser.add_argument('--dry-run',action='store_true')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    config = json.loads(args.config.read_text()); validate_config(config)
    client = bigquery.Client(project=PROJECT,location=LOCATION)
    sources = read_sources(client,storage.Client(project=PROJECT))
    counts = {kind:sum(s['modality']==kind for s in sources) for kind in ('TEXT','IMAGE')}
    LOG.info(json.dumps({'event':'embedding_inventory',**counts,'dry_run':args.dry_run}))
    if args.dry_run:
        return
    cache = {**cloud_cache(client,TEXT_TARGET),**cloud_cache(client,IMAGE_TARGET)}
    directory = ROOT/config['cache_directory']
    embedder = VertexEmbedder(config)
    rows = []; calls = 0; cloud_hits = 0
    for index, source in enumerate(sources):
        cloud_hits += record_identity(source,config)['embedding_id'] in cache
        row, generated = embed_or_reuse(source,config,cache,directory,embedder)
        rows.append(row); calls += generated
        if (index+1)%10 == 0:
            LOG.info(json.dumps({'event':'embedding_progress','completed':index+1,'model_calls':calls}))
    for modality,target in (('TEXT',TEXT_TARGET),('IMAGE',IMAGE_TARGET)):
        payload = [r for r in rows if r['modality']==modality]
        upsert(client,target,SCHEMA,payload,['embedding_id','source_system'])
        stored = cloud_cache(client,target)
        for row in payload:
            if validate_cached(stored[row['embedding_id']],record_identity(row,config)) != row:
                raise ValueError('Stored vector differs from generated payload')
    publish_views(client,config)
    stats = dict(next(iter(client.query((ROOT/'sql/validate_embeddings.sql').read_text(),location=LOCATION).result())))
    if (stats['knowledge_vectors'] != counts['TEXT']-stats['visual_vectors']
            or stats['knowledge_vectors']+stats['visual_vectors'] != counts['TEXT']
            or stats['image_vectors'] != counts['IMAGE'] or stats['product_vectors'] != 20
            or stats['campaign_vectors'] != 60 or any(v for k,v in stats.items() if k.startswith('invalid_'))):
        raise ValueError(f'Embedding validation failed: {stats}')
    report = {'event':'embeddings_verified',**stats,'model_calls':calls,
        'reused':len(rows)-calls,'cloud_reused':cloud_hits,
        'verified_at':datetime.now(timezone.utc).isoformat()}
    (ROOT/'data/generated/embeddings_validation.json').write_text(json.dumps(report,indent=2))
    LOG.info(json.dumps(report))


if __name__ == '__main__':
    main()
