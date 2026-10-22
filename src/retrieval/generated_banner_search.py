"""Search eligible generated POC banner images by a short visual description.

Run: python -m src.retrieval.generated_banner_search --sku CI-SKU-0001 \
  --query 'Minimal skincare banner, off-white background, product on right'
One ephemeral multimodal text query ranks authoritative BigQuery image vectors.
Returns generation-pinned original GCS PNG bytes and scoped evidence metadata.
No cloud writes or local serving cache; CLI exports are review copies only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, storage

from src.generators.embeddings_generator import validate_vector
from src.generators.generated_banner_embeddings import validate_config as validate_embedding_config
from src.generators.generated_banner_embeddings import EMBEDDING_VIEW, validate_selection
from src.generators.sales_generator import EXPECTED_SKUS, LOCATION, PROJECT, ROOT
from src.retrieval.campaign_performance import json_value
from src.retrieval.generated_banner import (
    GeneratedBannerRead, capture_generated_banner, retrieve_generated_banner,
)

CONFIG = ROOT/'config/generated_banner_retrieval.json'
SQL = ROOT/'sql/retrieve_generated_banners.sql'
VALIDATION_SQL = ROOT/'sql/validate_generated_banner_search.sql'
LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class GeneratedBannerSearchRead:
    """Verified ranked generated images and all native eligibility read identities."""

    packet: dict
    images: list[tuple[dict,bytes]]
    ranking_job_ids: tuple[str,str]
    banner_reads: tuple[GeneratedBannerRead,...]
    observed_at: datetime
    project: str
    location: str

    @property
    def source_revision(self) -> str:
        identities=[*self.ranking_job_ids,*(read.source_revision for read in self.banner_reads)]
        return 'bqset-'+hashlib.sha256('\n'.join(identities).encode()).hexdigest()


def embedding_config(config: dict) -> dict:
    """Reuse the registered image profile for original-byte identity checks."""
    keys = ('location','image_model','image_dimension','request_interval_seconds',
            'max_attempts','maximum_bytes_billed')
    return dict(version=config['embedding_version'], **{key:config[key] for key in keys})


def validate_config(config: dict) -> None:
    expected = {'version','embedding_version','location','image_model','image_dimension',
                'request_interval_seconds','max_attempts','maximum_bytes_billed','top_k',
                'max_query_characters','max_query_words'}
    if not isinstance(config,dict) or set(config) != expected:
        raise ValueError('Expected generated-banner search settings')
    validate_embedding_config(embedding_config(config))
    if (config['version'] != 'generated-banner-retrieval-v1'
            or type(config['top_k']) is not int or not 1 <= config['top_k'] <= 20
            or type(config['max_query_characters']) is not int or not 1 <= config['max_query_characters'] <= 128
            or type(config['max_query_words']) is not int or not 1 <= config['max_query_words'] <= 20):
        raise ValueError('Invalid search version, result limit or short-query limits')


def validate_text(query: str, config: dict) -> str:
    validate_config(config)
    # The model supports English short text and can internally shorten >32 tokens.
    # These conservative application limits are characters/words, not a tokenizer.
    if (not isinstance(query,str) or not query.strip() or not query.isascii()
            or any(ord(c)<32 or ord(c)==127 for c in query)):
        raise ValueError('Use a nonempty, single-line English visual description')
    text = query.strip()
    if len(text)>config['max_query_characters'] or len(text.split())>config['max_query_words']:
        raise ValueError('Visual description exceeds the configured short-query limits; shorten it explicitly')
    return text


def validate_query(sku: str, query: str, config: dict) -> str:
    if not isinstance(sku,str) or sku not in EXPECTED_SKUS:
        raise ValueError('Expected an existing canonical CI-SKU key')
    return validate_text(query,config)


class MultimodalTextQueryEmbedder:
    """ADC REST predict: textEmbedding shares the stored image vector space."""

    def __init__(self, config: dict):
        validate_config(config)
        self.config = dict(config)
        credentials,_ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        self.session = AuthorizedSession(credentials)
        self.last_request = 0.0
        self.model_calls = 0

    def __call__(self, text: str) -> list[float]:
        # Also validate direct embedder usage before any outbound request.
        text = validate_text(text,self.config)
        url = (f'https://{self.config["location"]}-aiplatform.googleapis.com/v1/projects/{PROJECT}'
               f'/locations/{self.config["location"]}/publishers/google/models/{self.config["image_model"]}:predict')
        for attempt in range(self.config['max_attempts']):
            time.sleep(max(0,self.config['request_interval_seconds']-(time.monotonic()-self.last_request)))
            self.last_request = time.monotonic()
            self.model_calls += 1
            try:
                response = self.session.post(url,json={'instances':[{'text':text}],
                    'parameters':{'dimension':self.config['image_dimension']}},timeout=60)
                response.raise_for_status()
                predictions = response.json()['predictions']
                if not isinstance(predictions,list) or len(predictions)!=1:
                    raise ValueError('Expected one multimodal text query embedding')
                return validate_vector(list(predictions[0]['textEmbedding']),self.config['image_dimension'])
            except Exception as error:
                code = getattr(error,'code',None) or getattr(getattr(error,'response',None),'status_code',None)
                if code not in (429,500,502,503,504) or attempt+1==self.config['max_attempts']:
                    raise
                LOG.warning(json.dumps(dict(event='banner_query_embedding_retry',attempt=attempt+1,code=code)))
                time.sleep(min(2**attempt,30))
        raise RuntimeError('Query embedding retries exhausted')

    def close(self) -> None:
        self.session.close()


def search_parameters(sku: str, vector: list[float], config: dict) -> bigquery.QueryJobConfig:
    validate_config(config)
    if sku not in EXPECTED_SKUS:
        raise ValueError('Expected an existing canonical CI-SKU key')
    vector = validate_vector(vector,config['image_dimension'])
    params = [bigquery.ScalarQueryParameter('sku','STRING',sku),
        bigquery.ArrayQueryParameter('query_vector','FLOAT64',vector),
        bigquery.ScalarQueryParameter('model','STRING',config['image_model']),
        bigquery.ScalarQueryParameter('dimension','INT64',config['image_dimension']),
        bigquery.ScalarQueryParameter('location','STRING',config['location']),
        bigquery.ScalarQueryParameter('top_k','INT64',config['top_k'])]
    return bigquery.QueryJobConfig(query_parameters=params,maximum_bytes_billed=config['maximum_bytes_billed'],
                                   use_query_cache=False)


def _ranked_read(client: bigquery.Client, sku: str, vector: list[float],
                 config: dict) -> tuple[list[dict],object]:
    query_job=client.query(SQL.read_text(),location=LOCATION,job_config=search_parameters(sku,vector,config))
    rows=[dict(row) for row in query_job.result()]
    if len(rows)>config['top_k'] or len({row['asset_id'] for row in rows})!=len(rows):
        raise ValueError('Duplicate banners or result limit exceeded')
    snapshots={row['knowledge_snapshot_id'] for row in rows}
    if rows and (len(snapshots)!=1 or not next(iter(snapshots))):
        raise ValueError('Search results must share one current BigQuery snapshot')
    for row in rows:
        validate_selection(sku,row['asset_id'])
        distance=row['distance']
        if (row['sku']!=sku or type(distance) not in (int,float) or not math.isfinite(distance)
                or not -1e-12<=distance<=2+1e-12):
            raise ValueError('Invalid same-SKU search result or cosine distance')
    return sorted(rows,key=lambda row:(row['distance'],row['asset_id'])),query_job


def read_ranked_banners(client: bigquery.Client, sku: str, vector: list[float], config: dict) -> list[dict]:
    return _ranked_read(client,sku,vector,config)[0]


def _ranking_job_id(job: object) -> str:
    job_id=getattr(job,'job_id',None)
    if (not isinstance(job_id,str) or not job_id or len(job_id)>128
            or any(c.isspace() for c in job_id) or getattr(job,'project',None)!=PROJECT
            or getattr(job,'location',None)!=LOCATION):
        raise ValueError('BigQuery generated-banner ranking does not match the fixed project and region')
    return job_id


def verify_search_queries(client: bigquery.Client, sku: str, config: dict) -> dict:
    """Run production SQL against SELECT-only vectors with known cosine distances."""
    settings = dict(config,top_k=20)
    vector = [1.0]+[0.0]*511
    sql = SQL.read_text()
    source = f'FROM `{EMBEDDING_VIEW}`'
    if sql.count(source)!=1:
        raise ValueError('Search validation requires exactly one generated eligibility source')
    fixture_sql = VALIDATION_SQL.read_text()+sql.replace(source,'FROM fixture')
    rows = [dict(row) for row in client.query(fixture_sql,location=LOCATION,
        job_config=search_parameters(sku,vector,settings)).result()]
    expected = [('00-same-a',0.0),('01-same-b',0.0),('02-near',1-1/math.sqrt(2)),
                ('03-orthogonal',1.0),('04-opposite',2.0)]
    if (len(rows)!=len(expected) or any(row['asset_id']!=name
            or not math.isclose(row['distance'],distance,abs_tol=1e-12)
            for row,(name,distance) in zip(rows,expected))):
        raise ValueError('BigQuery cosine ranking, stable tie order or eligibility prefilters failed')
    # Same production query verifies top_k, rather than slicing the validated result.
    limited = [dict(row) for row in client.query(fixture_sql,location=LOCATION,
        job_config=search_parameters(sku,vector,dict(config,top_k=2))).result()]
    if limited!=rows[:2]:
        raise ValueError('BigQuery top_k limit failed')
    return dict(read_only=True,fixture_vectors=17,ranked_vectors=5,excluded_vectors=12,
                cosine_distances_and_ties_verified=True,top_k_verified=True)


def search_generated_banners(client: bigquery.Client, gcs: storage.Client,
                             embed: Callable[[str],list[float]], sku: str, query: str,
                             config: dict) -> tuple[dict,list[tuple[dict,bytes]]]:
    text = validate_query(sku,query,config)
    vector = validate_vector(embed(text),config['image_dimension'])
    ranked = read_ranked_banners(client,sku,vector,config)
    images = []
    for rank,selection in enumerate(ranked,1):
        metadata,raw = retrieve_generated_banner(client,gcs,sku,selection['asset_id'],embedding_config(config))
        expected = dict(sku=metadata['canonical_sku'],asset_id=metadata['asset_id'],
            review_id=metadata['review']['review_id'],review_revision=metadata['review']['revision'],
            embedding_id=metadata['embedding']['embedding_id'],gcs_uri=metadata['gcs_uri'],
            gcs_generation=metadata['gcs_generation'],sha256=metadata['content_sha256'],
            knowledge_snapshot_id=metadata['snapshot_id'])
        if any(selection[key]!=value for key,value in expected.items()):
            raise ValueError('Ranked banner identity changed before original-image retrieval')
        evidence = sorted(set(metadata['evidence_references']) | {
            f'{PROJECT}.ci_brand_memory.image_embeddings#embedding_id={selection["embedding_id"]}'})
        metadata = dict(metadata,rank=rank,distance=selection['distance'],evidence_references=evidence)
        images.append((metadata,raw))
    # Recheck the whole ranked batch after all downloads with the same ephemeral vector.
    if read_ranked_banners(client,sku,vector,config)!=ranked:
        raise ValueError('Ranked eligibility or image identity changed during search')
    packet = dict(canonical_sku=sku,query=text,source_of_truth='BIGQUERY',original_image_store='GCS',
        retrieval_method='EXACT_COSINE_IMAGE_SEARCH',distance_type='COSINE',
        retrieval_status='GENERATED_POC_VISUAL_REFERENCE',source_system='SYNTHETIC_POC',
        approval_scope='SYNTHETIC_POC_ONLY',usage_scope='NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE',
        snapshot_id=ranked[0]['knowledge_snapshot_id'] if ranked else None,
        query_embedding=dict(model_id=config['image_model'],model_location=config['location'],
            dimension=config['image_dimension'],input_modality='TEXT',ephemeral=True,
            query_sha256=hashlib.sha256(text.encode()).hexdigest()),
        top_k=config['top_k'],results=[metadata for metadata,_ in images],
        verification=dict(current_ranked_eligibility=True,original_images_verified=True),
        usage_notes=['Lower cosine distance means closer in embedding space, not a probability or performance metric.',
            'Query text expresses visual preferences and supplies no approved product claims.',
            'Generated POC visual references supply no actual campaign performance or benefit evidence.',
            'Pass returned original PNG bytes to the model for visual reasoning.',
            'Short-query limits reduce model shortening risk; they are not an exact token count.',
            'Cloud errors propagate; local exports are never a serving fallback.'])
    return packet,images


def capture_generated_banner_search(client: bigquery.Client, gcs: storage.Client,
                                    embed: Callable[[str],list[float]], sku: str, query: str,
                                    config: dict) -> GeneratedBannerSearchRead:
    """Capture ranked eligibility, exact approved-image rechecks and original bytes."""
    text=validate_query(sku,query,config)
    vector=validate_vector(embed(text),config['image_dimension'])
    ranked,first_job=_ranked_read(client,sku,vector,config)
    images=[];reads=[]
    for rank,selection in enumerate(ranked,1):
        captured=capture_generated_banner(client,gcs,sku,selection['asset_id'],embedding_config(config))
        metadata=captured.packet
        expected=dict(sku=metadata['canonical_sku'],asset_id=metadata['asset_id'],
            review_id=metadata['review']['review_id'],review_revision=metadata['review']['revision'],
            embedding_id=metadata['embedding']['embedding_id'],gcs_uri=metadata['gcs_uri'],
            gcs_generation=metadata['gcs_generation'],sha256=metadata['content_sha256'],
            knowledge_snapshot_id=metadata['snapshot_id'])
        if any(selection[key]!=value for key,value in expected.items()):
            raise ValueError('Ranked banner identity changed before original-image retrieval')
        evidence=sorted(set(metadata['evidence_references']) | {
            f'{PROJECT}.ci_brand_memory.image_embeddings#embedding_id={selection["embedding_id"]}'})
        images.append((dict(metadata,rank=rank,distance=selection['distance'],evidence_references=evidence),
                       captured.raw))
        reads.append(captured)
    current,second_job=_ranked_read(client,sku,vector,config)
    if current!=ranked:
        raise ValueError('Ranked eligibility or image identity changed during search')
    ranking_ids=(_ranking_job_id(first_job),_ranking_job_id(second_job))
    packet=dict(canonical_sku=sku,query=text,source_of_truth='BIGQUERY',original_image_store='GCS',
        retrieval_method='EXACT_COSINE_IMAGE_SEARCH',distance_type='COSINE',
        retrieval_status='GENERATED_POC_VISUAL_REFERENCE',source_system='SYNTHETIC_POC',
        approval_scope='SYNTHETIC_POC_ONLY',usage_scope='NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE',
        snapshot_id=ranked[0]['knowledge_snapshot_id'] if ranked else None,
        query_embedding=dict(model_id=config['image_model'],model_location=config['location'],
            dimension=config['image_dimension'],input_modality='TEXT',ephemeral=True,
            query_sha256=hashlib.sha256(text.encode()).hexdigest()),top_k=config['top_k'],
        results=[metadata for metadata,_ in images],
        verification=dict(current_ranked_eligibility=True,original_images_verified=True),
        usage_notes=['Lower cosine distance means closer in embedding space, not a probability or performance metric.',
            'Query text expresses visual preferences and supplies no approved product claims.',
            'Generated POC visual references supply no actual campaign performance or benefit evidence.',
            'Pass returned original PNG bytes to the model for visual reasoning.',
            'Short-query limits reduce model shortening risk; they are not an exact token count.',
            'Cloud errors propagate; local exports are never a serving fallback.'])
    return GeneratedBannerSearchRead(packet=packet,images=images,ranking_job_ids=ranking_ids,
        banner_reads=tuple(reads),observed_at=datetime.now(timezone.utc),project=PROJECT,location=LOCATION)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True)
    parser.add_argument('--query',required=True,help='Short English visual description, at most 128 characters/20 words')
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--top-k',type=int)
    parser.add_argument('--output-dir',type=Path)
    parser.add_argument('--verify-search',action='store_true',help='Also run read-only BigQuery ranking/filter fixtures')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    config = json.loads(args.config.read_text())
    if args.top_k is not None:
        config['top_k'] = args.top_k
    try:
        validate_query(args.sku,args.query,config)
    except ValueError as error:
        parser.error(str(error))
    client = bigquery.Client(project=PROJECT,location=LOCATION)
    validation = verify_search_queries(client,args.sku,config) if args.verify_search else None
    embedder = MultimodalTextQueryEmbedder(config)
    try:
        packet,images = search_generated_banners(client,
            storage.Client(project=PROJECT),embedder,args.sku,args.query,config)
    finally:
        embedder.close()
    packet['model_calls'] = embedder.model_calls
    if validation is not None:
        packet['search_validation'] = validation
    packet['retrieved_at'] = datetime.now(timezone.utc).isoformat()
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output = (args.output_dir or ROOT/f'data/generated/retrieval/generated_banner_search/{args.sku}')/run
    output.mkdir(parents=True,exist_ok=False)
    for metadata,raw in images:
        (output/f'{metadata["rank"]:02}-{metadata["asset_id"]}.png').write_bytes(raw)
    (output/'metadata.json').write_text(json.dumps(packet,indent=2,default=json_value))
    LOG.info(json.dumps(dict(event='generated_banners_searched',sku=args.sku,images=len(images),
        model_calls=embedder.model_calls,output=str(output),distances=[m['distance'] for m,_ in images])))


if __name__ == '__main__':
    main()
