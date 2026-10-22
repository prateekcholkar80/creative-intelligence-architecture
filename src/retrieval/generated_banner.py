"""Retrieve one current eligible generated POC banner and verified original bytes.

Run: python -m src.retrieval.generated_banner --sku CI-SKU-0001 --asset-id <registered-id>
Exact BigQuery eligibility lookup; originals come from generation-pinned GCS.
Exports are review copies, never a serving fallback. This image is a generated
POC visual reference and supplies no historical effectiveness/benefit evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage

from src.generators.embeddings_generator import record_identity, validate_vector
from src.generators.generated_banner_embeddings import (
    CONFIG, EMBEDDING_VIEW, download_banner, image_source, validate_config, validate_selection,
)
from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.campaign_performance import json_value

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class GeneratedBannerRead:
    """Verified approved banner and both native BigQuery eligibility reads."""

    packet: dict
    raw: bytes
    query_job_ids: tuple[str,str]
    observed_at: datetime
    project: str
    location: str

    @property
    def source_revision(self) -> str:
        encoded='\n'.join(self.query_job_ids).encode()
        return 'bq2-'+hashlib.sha256(encoded).hexdigest()


def _validate_read_jobs(jobs: tuple[object,object]) -> tuple[tuple[str,str],str,str]:
    identities=[]
    for job in jobs:
        job_id=getattr(job,'job_id',None);project=getattr(job,'project',None);location=getattr(job,'location',None)
        if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
                or project!=PROJECT or location!=LOCATION):
            raise ValueError('BigQuery generated-banner read does not match the fixed project and region')
        identities.append(job_id)
    return (identities[0],identities[1]),PROJECT,LOCATION


def _retrieve_generated_banner(client: bigquery.Client, gcs: storage.Client, sku: str,
                               asset_id: str, config: dict) -> tuple[dict,bytes,tuple[object,object]]:
    validate_selection(sku,asset_id);validate_config(config)
    params = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('sku','STRING',sku),
        bigquery.ScalarQueryParameter('asset_id','STRING',asset_id)],
        maximum_bytes_billed=config['maximum_bytes_billed'],use_query_cache=False)
    def read() -> tuple[dict,object]:
        query_job=client.query(f'SELECT * FROM `{EMBEDDING_VIEW}` WHERE sku=@sku AND asset_id=@asset_id',
                               location=LOCATION,job_config=params)
        rows=[dict(r) for r in query_job.result()]
        if len(rows) != 1:
            raise ValueError('Generated banner is missing, duplicate or no longer eligible')
        return rows[0],query_job
    row,first_job = read()
    if (row['sku'] != sku or row['asset_id'] != asset_id or row['model_id'] != config['image_model']
            or row['model_location'] != config['location'] or row['dimension'] != config['image_dimension']
            or row['task_type'] != 'IMAGE' or row['retrieval_status'] != 'GENERATED_POC_VISUAL_REFERENCE'
            or row['review_status'] != 'APPROVED_FOR_POC' or row['review_approval_scope'] != 'POC_VISUAL_REUSE_ONLY'
            or type(row['review_revision']) is not int or row['review_revision']<1 or not row['review_id']
            or row['usage_scope'] != 'NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE'):
        raise ValueError('Invalid generated-only retrieval scope or vector profile')
    validate_vector(list(row['vector']),row['dimension'])
    raw = download_banner(gcs,row)
    identity = record_identity(image_source(row,raw),config)
    if row['embedding_id'] != identity['embedding_id']:
        raise ValueError('Stored vector identity differs from the original registered banner')
    current,second_job=read()
    if current != row:
        raise ValueError('Banner eligibility or metadata changed during retrieval')
    packet = dict(canonical_sku=sku,asset_id=asset_id,source_of_truth='BIGQUERY',original_image_store='GCS',
        source_system='SYNTHETIC_POC',status=row['status'],approval_scope=row['approval_scope'],
        review=dict(review_id=row['review_id'],revision=row['review_revision'],status=row['review_status'],
                    approval_scope=row['review_approval_scope']),
        retrieval_status=row['retrieval_status'],usage_scope=row['usage_scope'],
        gcs_uri=row['gcs_uri'],gcs_generation=row['gcs_generation'],content_sha256=row['sha256'],
        mime_type=row['mime_type'],width=row['width'],height=row['height'],byte_count=row['byte_count'],
        original_product_image=dict(asset_id=row['source_image_asset_id'],gcs_uri=row['source_image_uri'],
            gcs_generation=row['source_image_generation'],content_sha256=row['source_image_sha256']),
        copy=dict(headline=row['headline'],body=row['body_copy'],cta=row['call_to_action']),
        visual_description=row['visual_description'],description_method=row['description_method'],
        approved_claim_ids=row['approved_claim_ids'],evidence_references=row['evidence_references'],
        snapshot_id=row['knowledge_snapshot_id'],embedding=dict(embedding_id=row['embedding_id'],
            model_id=row['model_id'],model_location=row['model_location'],dimension=row['dimension'],task_type='IMAGE',
            created_at=row['embedding_created_at']),
        verification=dict(current_eligibility=True,generation_match=True,sha256_match=True,
                          format_match=True,dimensions_match=True,vector_identity_match=True),
        usage_notes=['Generated POC draft visual reference, not actual historical campaign evidence.',
            'Similarity provides no benefit approval, causal lift or proof of effectiveness.',
            'Pass these verified original PNG bytes to the model for visual reasoning.',
            'Cloud failures never fall back to local JSON or embedding checkpoints.'])
    return packet,raw,(first_job,second_job)


def capture_generated_banner(client: bigquery.Client, gcs: storage.Client, sku: str,
                             asset_id: str, config: dict) -> GeneratedBannerRead:
    """Capture approved exact retrieval with pre/post-download native read identity."""
    packet,raw,jobs=_retrieve_generated_banner(client,gcs,sku,asset_id,config)
    job_ids,project,location=_validate_read_jobs(jobs)
    return GeneratedBannerRead(packet=packet,raw=raw,query_job_ids=job_ids,
        observed_at=datetime.now(timezone.utc),project=project,location=location)


def retrieve_generated_banner(client: bigquery.Client, gcs: storage.Client, sku: str,
                              asset_id: str, config: dict) -> tuple[dict,bytes]:
    """Compatibility facade retaining exact eligibility and byte rechecks."""
    packet,raw,_=_retrieve_generated_banner(client,gcs,sku,asset_id,config)
    return packet,raw


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True);parser.add_argument('--asset-id',required=True)
    parser.add_argument('--config',type=Path,default=CONFIG);parser.add_argument('--output-dir',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    config = json.loads(args.config.read_text())
    packet,raw = retrieve_generated_banner(bigquery.Client(project=PROJECT,location=LOCATION),storage.Client(project=PROJECT),
                                         args.sku,args.asset_id,config)
    run = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:8]
    output = (args.output_dir or ROOT/f'data/generated/retrieval/generated_banners/{args.asset_id}')/run
    output.mkdir(parents=True,exist_ok=False)
    (output/'banner.png').write_bytes(raw)
    (output/'metadata.json').write_text(json.dumps(packet,indent=2,default=json_value))
    LOG.info(json.dumps(dict(event='generated_banner_retrieved',sku=args.sku,asset_id=args.asset_id,
                            images=1,output=str(output),verification=packet['verification'])))


if __name__ == '__main__':
    main()
