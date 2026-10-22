"""Rank same-SKU synthetic creative references using authoritative BigQuery vectors.

Run: python -m src.retrieval.campaign_creatives --request config/retrieval_request.example.json
Query embeddings are ephemeral RETRIEVAL_QUERY inputs. No documents, vectors,
campaigns or images are written to the cloud. The JSON export is for inspection,
never a runtime fallback. Banner references are returned; image bytes are a later step.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.cloud import bigquery
from google.genai import types

from src.generators.campaign_assets_generator import BUCKET
from src.generators.embeddings_generator import validate_vector
from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.request import CHANNELS, OBJECTIVES, RetrievalRequest

SQL = ROOT/'sql/retrieve_campaign_creatives.sql'
CONFIG = ROOT/'config/campaign_retrieval.json'
LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class CampaignCreativeRead:
    """Validated creative packet and the native BigQuery read that produced it."""

    packet: dict
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str,str,str]:
    job_id=getattr(job,'job_id',None);project=getattr(job,'project',None);location=getattr(job,'location',None)
    if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
            or project!=PROJECT or location!=LOCATION):
        raise ValueError('BigQuery creative read job identity does not match the fixed project and region')
    return job_id,project,location


def validate_config(config: dict) -> None:
    if (config['model_id'] != 'gemini-embedding-001' or config['location'] != LOCATION
            or type(config['dimension']) is not int or config['dimension'] != 768):
        raise ValueError('Query model, region and dimension must match the current BigQuery document vectors')
    if (type(config['top_k']) is not int or not 1 <= config['top_k'] <= 20
            or type(config['maximum_bytes_billed']) is not int or config['maximum_bytes_billed'] <= 0):
        raise ValueError('Invalid result limit or billing cap')


def query_text(request: RetrievalRequest) -> str:
    # Styling references have a separate authority scope and retrieval path.
    return (f'Synthetic campaign creative reference for {request.canonical_sku}. '
            f'Objective: {request.objective}. Channel: {request.channel}. Audience: {request.audience}.')


def embed_query(client: genai.Client, text: str, config: dict) -> list[float]:
    validate_config(config)
    response = client.models.embed_content(model=config['model_id'], contents=text,
        config=types.EmbedContentConfig(task_type='RETRIEVAL_QUERY',
            output_dimensionality=config['dimension'], auto_truncate=False,
            http_options=types.HttpOptions(timeout=60000)))
    if len(response.embeddings or []) != 1:
        raise ValueError('Expected exactly one query embedding')
    embedding = response.embeddings[0]
    if embedding.statistics and embedding.statistics.truncated:
        raise ValueError('Query embedding was truncated')
    return validate_vector(list(embedding.values or []),config['dimension'])


def build_packet(request: RetrievalRequest, rows: list[dict], config: dict) -> dict:
    validate_config(config)
    creatives = [dict(row) for row in rows]
    if (len(creatives) > config['top_k']
            or len({c['chunk_id'] for c in creatives}) != len(creatives)
            or len({c['asset_id'] for c in creatives}) != len(creatives)):
        raise ValueError('Duplicate creatives or result limit exceeded')
    snapshots = {c['snapshot_id'] for c in creatives}
    if creatives and (len(snapshots) != 1 or not next(iter(snapshots))):
        raise ValueError('Creatives must share one current BigQuery snapshot')
    for creative in creatives:
        campaign_ref = f'{PROJECT}.ci_creative.campaign_master#campaign_id={creative["campaign_id"]}'
        asset_ref = f'{PROJECT}.ci_creative.campaign_assets#asset_id={creative["asset_id"]}'
        expected_image = f'gs://{BUCKET}/products/{request.canonical_sku}/primary.jpg'
        expected_banner = (f'gs://{BUCKET}/campaigns/{creative["campaign_id"]}/template-v1/'
                           f'{creative["banner_sha256"]}.png')
        distance = creative['distance']
        if (creative['sku'] != request.canonical_sku or creative['chunk_type'] != 'CAMPAIGN_CREATIVE'
                or creative['source_system'] != 'SYNTHETIC_POC'
                or creative['retrieval_status'] != 'POC_CONTEXT'
                or creative['approval_scope'] != 'SYNTHETIC_POC_ONLY' or creative['claim_id'] is not None
                or creative['model_id'] != config['model_id'] or creative['dimension'] != config['dimension']
                or creative['model_location'] != config['location'] or creative['task_type'] != 'RETRIEVAL_DOCUMENT'
                or creative['channel'] not in CHANNELS or creative['objective'] not in OBJECTIVES
                or creative['source_reference'] != campaign_ref
                or not {campaign_ref,asset_ref}.issubset(creative['evidence_references'])
                or creative['source_image_uri'] != expected_image or creative['gcs_uri'] != expected_banner
                or not {expected_image,expected_banner}.issubset(creative['image_uris'])
                or len(creative['banner_sha256']) != 64
                or any(ch not in '0123456789abcdef' for ch in creative['banner_sha256'])
                or not creative['chunk_text'].strip()
                or hashlib.sha256(creative['chunk_text'].encode()).hexdigest() != creative['content_sha256']
                or isinstance(distance,bool) or not isinstance(distance,(int,float))
                or not math.isfinite(distance) or not -1e-12 <= distance <= 2+1e-12):
            raise ValueError('Invalid creative identity, eligibility, provenance, vector profile, or distance')
        creative['objective_match'] = creative['objective'] == request.objective
        creative['channel_match'] = creative['channel'] == request.channel
    creatives.sort(key=lambda c:(c['distance'],c['chunk_id']))
    for rank,creative in enumerate(creatives,1):
        creative['rank'] = rank
    return dict(request=request.to_dict(),source_of_truth='BIGQUERY',
        source_system='SYNTHETIC_POC',approval_scope='SYNTHETIC_POC_ONLY',
        snapshot_id=next(iter(snapshots)) if snapshots else None,
        query_embedding=dict(model_id=config['model_id'],dimension=config['dimension'],
            model_location=config['location'],task_type='RETRIEVAL_QUERY',
            query_text=query_text(request),stored=False),
        search=dict(method='EXACT_COSINE',top_k=config['top_k'],lower_distance_is_closer=True),
        creatives=creatives,
        usage_notes=['Synthetic POC creative references, not actual historical business campaigns.',
            'Objective and channel are ranking preferences; match flags expose differences.',
            'Similarity is not evidence of campaign effectiveness or approved benefit claims.',
            'Banner references are returned here; original image bytes require verified GCS retrieval.'])


def capture_campaign_creatives(client: bigquery.Client, embedding_client: genai.Client,
                               request: RetrievalRequest, config: dict) -> CampaignCreativeRead:
    """Execute and capture one fresh authoritative ranked-creative read."""
    validate_config(config)
    vector = embed_query(embedding_client,query_text(request),config)
    parameters = [bigquery.ScalarQueryParameter('sku','STRING',request.canonical_sku),
        bigquery.ArrayQueryParameter('query_vector','FLOAT64',vector),
        bigquery.ScalarQueryParameter('model','STRING',config['model_id']),
        bigquery.ScalarQueryParameter('dimension','INT64',config['dimension']),
        bigquery.ScalarQueryParameter('location','STRING',config['location']),
        bigquery.ScalarQueryParameter('top_k','INT64',config['top_k'])]
    query_job=client.query(SQL.read_text(),location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=parameters,maximum_bytes_billed=config['maximum_bytes_billed'],
        use_query_cache=False))
    rows=list(query_job.result())
    job_id,project,location=_validate_read_job(query_job)
    return CampaignCreativeRead(packet=build_packet(request,rows,config),query_job_id=job_id,
        observed_at=datetime.now(timezone.utc),project=project,location=location)


def retrieve_campaign_creatives(client: bigquery.Client, embedding_client: genai.Client,
                                request: RetrievalRequest, config: dict) -> dict:
    """Compatibility facade returning only the validated authoritative packet."""
    return capture_campaign_creatives(client,embedding_client,request,config).packet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request',type=Path,required=True)
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--output',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    request = RetrievalRequest.from_dict(json.loads(args.request.read_text()))
    config = json.loads(args.config.read_text());validate_config(config)
    client = bigquery.Client(project=PROJECT,location=LOCATION)
    with genai.Client(vertexai=True,project=PROJECT,location=config['location'],
                      http_options=types.HttpOptions(api_version='v1')) as embedding_client:
        packet = retrieve_campaign_creatives(client,embedding_client,request,config)
    packet['retrieved_at'] = datetime.now(timezone.utc).isoformat()
    output = args.output or ROOT/f'data/generated/retrieval/{request.canonical_sku}_campaign_creatives.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(packet,indent=2,default=lambda value:value.isoformat()))
    LOG.info(json.dumps({'event':'campaign_creatives_retrieved','sku':request.canonical_sku,
        'creatives':len(packet['creatives']),'output':str(output)}))


if __name__ == '__main__':
    main()
