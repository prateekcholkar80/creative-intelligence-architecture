"""Assemble a complete validated planning-evidence bundle without persistence.

The caller supplies projections from fresh authoritative readers. Product and
brand reads intentionally overlap on the POC product-name claim; assembly keeps
the brand-policy projection only after proving both projections describe the
same authoritative claim. No local fallback, source read, audit, or write occurs.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from src.application.access_policy import OwnedResource
from src.application.evidence_contract import validate_evidence_bundle
from src.application.workflow_contract import ContractError


@dataclass(frozen=True,slots=True)
class PlanningEvidenceBundle:
    evidence_snapshot_id: str
    knowledge_snapshot_id: str
    canonical_sku: str
    rows: tuple[dict,...]


def _claim_rows(rows: list[dict]) -> dict[str,dict]:
    result={}
    for row in rows:
        if row.get('source_kind')=='POC_CLAIM':
            identifier=row.get('source_record_id')
            if identifier in result:
                raise ContractError('DUPLICATE_EVIDENCE_REFERENCE')
            result[identifier]=row
    return result


def _same_claim(left: dict, right: dict) -> bool:
    keys=('evidence_ref_id','canonical_sku','source_kind','source_table','source_record_id',
        'source_system','content_sha256','evidence_schema_version','evidence_payload',
        'approval_reference','review_revision')
    return all(left.get(key)==right.get(key) for key in keys)


def assemble_planning_evidence(*, product_rows: list[dict], brand_rows: list[dict],
                               sales_rows: list[dict], product_image_rows: list[dict],
                               request: dict, job: dict, attempt: dict,
                               conversation: OwnedResource,
                               campaign_creative_rows: list[dict] | None = None,
                               campaign_image_rows: list[dict] | None = None,
                               campaign_performance_rows: list[dict] | None = None,
                               generated_image_rows: list[dict] | None = None,
                               visual_reference_rows: list[dict] | None = None) -> PlanningEvidenceBundle:
    """Return one complete, deterministic, validated in-memory planning bundle."""
    required=(product_rows,brand_rows,sales_rows,product_image_rows)
    optional=(campaign_creative_rows or [],campaign_image_rows or [],
              campaign_performance_rows or [],generated_image_rows or [],visual_reference_rows or [])
    if any(not isinstance(group,list) for group in (*required,*optional)):
        raise ContractError('INVALID_EVIDENCE_BUNDLE')

    product_claims,brand_claims=_claim_rows(product_rows),_claim_rows(brand_rows)
    if (not product_claims or set(product_claims)!=set(brand_claims)
            or any(not _same_claim(product_claims[key],brand_claims[key]) for key in product_claims)):
        raise ContractError('EVIDENCE_SOURCE_CONFLICT')

    # Current policy/approval eligibility comes from the dedicated brand read.
    rows=[row for row in product_rows if row.get('source_kind')!='POC_CLAIM']
    rows.extend(brand_rows)
    for group in (sales_rows,product_image_rows,*optional):
        rows.extend(group)
    rows=deepcopy(rows)
    rows.sort(key=lambda row:(row['source_kind'],row['rank'] or 0,row['source_record_id'],
                              row['evidence_ref_id']))
    validate_evidence_bundle(rows,request=request,job=job,attempt=attempt,conversation=conversation)
    evidence_ids={row['evidence_snapshot_id'] for row in rows}
    knowledge_ids={row['knowledge_snapshot_id'] for row in rows
                   if row['knowledge_snapshot_id'] is not None}
    return PlanningEvidenceBundle(evidence_snapshot_id=next(iter(evidence_ids)),
        knowledge_snapshot_id=next(iter(knowledge_ids)),canonical_sku=job['canonical_sku'],
        rows=tuple(rows))
