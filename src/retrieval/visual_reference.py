"""Retrieve styling-only context from BigQuery; local JSON is an output only.

Run: python -m src.retrieval.visual_reference --reference NAKED_AND_THRIVING
This exact reference lookup performs no model calls or cloud writes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.request import VISUAL_REFERENCES

SQL = ROOT/'sql/retrieve_visual_reference.sql'
SECTIONS = frozenset({'typography','colors','layout','creative_patterns'})
LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class VisualReferenceRead:
    packet: dict
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str,str,str]:
    job_id=getattr(job,'job_id',None);project=getattr(job,'project',None);location=getattr(job,'location',None)
    if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
            or project!=PROJECT or location!=LOCATION):
        raise ValueError('BigQuery visual-reference read job identity does not match the fixed project and region')
    return job_id,project,location


def validate_reference(reference: str) -> None:
    if not isinstance(reference,str) or reference not in VISUAL_REFERENCES:
        raise ValueError('Expected NONE or NAKED_AND_THRIVING')


def build_packet(reference: str, rows: list[dict]) -> dict:
    validate_reference(reference)
    chunks = [dict(r) for r in rows]
    if reference == 'NONE':
        if chunks:
            raise ValueError('No styling context allowed when reference is NONE')
        return dict(visual_reference='NONE',approval_scope=None,visual_chunks=[])
    if not chunks or len({c['chunk_id'] for c in chunks}) != len(chunks):
        raise ValueError('Unique nonempty styling chunks required')
    snapshots = {c['snapshot_id'] for c in chunks}
    if len(snapshots) != 1 or not next(iter(snapshots)):
        raise ValueError('Styling must come from one current BigQuery snapshot')
    sections = set()
    for chunk in chunks:
        section = chunk['source_reference'].rsplit('#',1)[-1]
        if (chunk['chunk_type'] != 'WEBSITE_VISUAL_REFERENCE' or chunk['brand'] != 'Naked & Thriving'
                or chunk['source_system'] != 'OFFICIAL_BRAND_WEBSITE'
                or chunk['retrieval_status'] != 'VISUAL_REFERENCE'
                or chunk['approval_scope'] != 'VISUAL_INSPIRATION_ONLY'
                or any(chunk[k] is not None for k in ('sku','claim_id','campaign_id','asset_id'))
                or section not in SECTIONS or not chunk['evidence_references']
                or not chunk['chunk_text'].strip()
                or hashlib.sha256(chunk['chunk_text'].encode()).hexdigest() != chunk['content_sha256']):
            raise ValueError('Invalid styling scope, provenance, section, or hash')
        sections.add(section)
    if sections != SECTIONS:
        raise ValueError('Typography, colors, layout, and creative patterns are all required')
    return dict(visual_reference=reference,reference_brand='Naked & Thriving',
        approval_scope='VISUAL_INSPIRATION_ONLY',snapshot_id=next(iter(snapshots)),
        visual_chunks=sorted(chunks,key=lambda c:(c['source_reference'],c['chunk_id'])),
        usage_notes=['Use only for CSS, typography, colors, layout, imagery, and visual hierarchy.',
            'Product identity and approved claim wording must come from their own BigQuery evidence.',
            'This reference does not establish manufacturer brand policy, brand tone, benefits, or campaign effectiveness.'])


def capture_visual_reference(client: bigquery.Client, reference: str,
                             maximum_bytes_billed: int = 64*1024*1024) -> VisualReferenceRead:
    validate_reference(reference)
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed <= 0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    if reference == 'NONE':
        raise ValueError('NONE has no native BigQuery evidence read')
    query_job=client.query(SQL.read_text(),location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter('brand','STRING','Naked & Thriving')],
        maximum_bytes_billed=maximum_bytes_billed,use_query_cache=False))
    rows=list(query_job.result());job_id,project,location=_validate_read_job(query_job)
    return VisualReferenceRead(build_packet(reference,rows),job_id,datetime.now(timezone.utc),project,location)


def retrieve_visual_reference(client: bigquery.Client, reference: str,
                              maximum_bytes_billed: int = 64*1024*1024) -> dict:
    validate_reference(reference)
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed <= 0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    if reference=='NONE':
        return build_packet(reference,[])
    return capture_visual_reference(client,reference,maximum_bytes_billed).packet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reference',choices=sorted(VISUAL_REFERENCES),required=True)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--maximum-bytes-billed',type=int,default=64*1024*1024)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    client = bigquery.Client(project=PROJECT,location=LOCATION)
    packet = retrieve_visual_reference(client,args.reference,args.maximum_bytes_billed)
    packet['retrieved_at'] = datetime.now(timezone.utc).isoformat()
    output = args.output or ROOT/f'data/generated/retrieval/{args.reference.lower()}_visual_reference.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(packet,indent=2))
    LOG.info(json.dumps({'event':'visual_reference_retrieved','reference':args.reference,
        'chunks':len(packet['visual_chunks']),'output':str(output)}))


if __name__=='__main__':
    main()
