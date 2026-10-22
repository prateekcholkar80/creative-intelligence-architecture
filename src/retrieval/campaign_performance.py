"""Retrieve validated campaign totals directly from BigQuery structured analytics.

Run: python -m src.retrieval.campaign_performance --sku CI-SKU-0001
Optional --campaign-id filters select only previously retrieved campaign IDs.
No model calls, embedding reads, cloud writes, or local JSON evidence fallback.
NUMERIC values remain Decimal in memory and are exported as exact strings.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from google.cloud import bigquery

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.product_knowledge import validate_sku
from src.retrieval.request import CANONICAL_SKUS, CHANNELS, OBJECTIVES

SQL = ROOT/'sql/retrieve_campaign_performance.sql'
LOG = logging.getLogger(__name__)
DEFAULT_MAXIMUM_BYTES_BILLED = 128*1024*1024
ATTRIBUTION_MODEL = 'SYNTHETIC_SALES_CAPPED_ATTRIBUTION'


@dataclass(frozen=True)
class CampaignPerformanceRead:
    """Validated campaign-performance packet and native BigQuery read identity."""

    packet: dict
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str,str,str]:
    job_id=getattr(job,'job_id',None);project=getattr(job,'project',None);location=getattr(job,'location',None)
    if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
            or project!=PROJECT or location!=LOCATION):
        raise ValueError('BigQuery performance read job identity does not match the fixed project and region')
    return job_id,project,location


def validate_selection(sku: str, campaign_ids: list[str] | None) -> None:
    validate_sku(sku)
    if campaign_ids is not None and (not isinstance(campaign_ids,list)
            or any(not isinstance(identifier,str) or not re.fullmatch(
                rf'SYN-CMP-[0-9]{{8}}-{re.escape(sku)}-[0-9]{{2}}',identifier) for identifier in campaign_ids)
            or len(set(campaign_ids)) != len(campaign_ids)):
        raise ValueError('Campaign IDs must be unique synthetic IDs for the requested canonical SKU')


def aggregate_ratio(numerator: int | Decimal, denominator: int | Decimal) -> Decimal | None:
    """Verify BigQuery NUMERIC division; an undefined ratio stays null."""
    return (Decimal(numerator)/Decimal(denominator)).quantize(
        Decimal('0.000000001'),rounding=ROUND_HALF_UP) if denominator else None


def build_packet(sku: str, rows: list[dict], campaign_ids: list[str] | None = None) -> dict:
    validate_selection(sku,campaign_ids)
    summaries = [dict(row) for row in rows]
    ids = [s['campaign_id'] for s in summaries]
    validate_selection(sku,ids)
    if campaign_ids is not None and set(ids) != set(campaign_ids):
        raise ValueError('Requested campaigns are missing or no longer eligible in BigQuery')
    snapshots = {s['snapshot_id'] for s in summaries}
    if summaries and (len(snapshots) != 1 or not next(iter(snapshots))):
        raise ValueError('Campaigns must belong to one current BigQuery knowledge snapshot')
    for summary in summaries:
        start,end = summary['start_date'],summary['end_date']
        counts = ('impressions','clicks','conversions','attributed_units')
        monetary = ('budget','spend','attributed_revenue')
        if (summary['sku'] != sku or summary['source_system'] != 'SYNTHETIC_POC'
                or summary['attribution_model'] != ATTRIBUTION_MODEL
                or summary['asset_id'] != f'SYN-AST-{summary["campaign_id"]}-template-v1'
                or summary['channel'] not in CHANNELS or summary['objective'] not in OBJECTIVES
                or not summary['currency'] or not summary['campaign_name'] or not summary['audience']
                or type(start) is not date or type(end) is not date or end < start
                or summary['expected_days'] != (end-start).days+1
                or summary['performance_rows'] != summary['expected_days']
                or summary['performance_days'] != summary['expected_days']
                or summary['first_performance_date'] != start or summary['last_performance_date'] != end
                or summary['invalid_rows'] != 0
                or any(type(summary[name]) is not int or summary[name] < 0 for name in counts)
                or any(not isinstance(summary[name],Decimal) or not summary[name].is_finite()
                       or summary[name] < 0 for name in monetary)
                or summary['spend'] != summary['budget']
                or not summary['conversions'] <= summary['clicks'] <= summary['impressions']
                or summary['attributed_units'] < summary['conversions']):
            raise ValueError('Invalid campaign provenance, daily coverage, spend, or performance totals')
        for name,numerator,denominator in (
                ('ctr','clicks','impressions'),('conversion_rate','conversions','clicks'),
                ('roas','attributed_revenue','spend')):
            ratio = summary[name]
            if ((ratio is not None and (not isinstance(ratio,Decimal) or not ratio.is_finite()))
                    or ratio != aggregate_ratio(summary[numerator],summary[denominator])):
                raise ValueError('Performance ratios must use aggregate totals with null for zero denominators')
        summary['evidence_references'] = [
            f'{PROJECT}.ci_creative.campaign_master#campaign_id={summary["campaign_id"]}',
            f'{PROJECT}.ci_creative.campaign_assets#asset_id={summary["asset_id"]}',
            f'{PROJECT}.ci_creative.campaign_daily_performance#campaign_id={summary["campaign_id"]}'
                f'&asset_id={summary["asset_id"]}&source_system=SYNTHETIC_POC'
                f'&start_date={start.isoformat()}&end_date={end.isoformat()}']
    return dict(canonical_sku=sku,source_of_truth='BIGQUERY',source_system='SYNTHETIC_POC',
        snapshot_id=next(iter(snapshots)) if snapshots else None,
        retrieval_method='STRUCTURED_CAMPAIGN_AGGREGATION',summaries=sorted(summaries,key=lambda s:s['campaign_id']),
        usage_notes=['Metrics are synthetic POC simulations, not actual historical business results.',
            'CTR = total clicks / total impressions; conversion rate = total conversions / total clicks; '
            'ROAS = total attributed revenue / total modeled spend. Undefined ratios are null.',
            'Attribution is simulated and capped during generation; these totals do not establish causal lift '
            'or prove creative effectiveness. EMAIL spend represents modeled delivery/operating cost.',
            'Compare monetary amounts within the same currency; do not infer approved product claims.'])


def capture_campaign_performance(client: bigquery.Client, sku: str, campaign_ids: list[str] | None = None,
                                 maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> CampaignPerformanceRead:
    """Execute and capture a fresh performance read for a nonempty/all selection."""
    validate_selection(sku,campaign_ids)
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed <= 0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    if campaign_ids == []:
        raise ValueError('An empty selection has no native BigQuery evidence read')
    params = [bigquery.ScalarQueryParameter('sku','STRING',sku),
        bigquery.ScalarQueryParameter('all_campaigns','BOOL',campaign_ids is None),
        bigquery.ArrayQueryParameter('campaign_ids','STRING',campaign_ids or [])]
    query_job=client.query(SQL.read_text(),location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=params,maximum_bytes_billed=maximum_bytes_billed,use_query_cache=False))
    rows=list(query_job.result())
    packet=build_packet(sku,rows,campaign_ids)
    job_id,project,location=_validate_read_job(query_job)
    return CampaignPerformanceRead(packet=packet,query_job_id=job_id,
        observed_at=datetime.now(timezone.utc),project=project,location=location)


def retrieve_campaign_performance(client: bigquery.Client, sku: str, campaign_ids: list[str] | None = None,
                                 maximum_bytes_billed: int = DEFAULT_MAXIMUM_BYTES_BILLED) -> dict:
    """Compatibility facade; explicit empty selection performs no query."""
    validate_selection(sku,campaign_ids)
    if type(maximum_bytes_billed) is not int or maximum_bytes_billed <= 0:
        raise ValueError('maximum_bytes_billed must be a positive integer')
    if campaign_ids == []:
        return build_packet(sku,[],[])
    return capture_campaign_performance(client,sku,campaign_ids,maximum_bytes_billed).packet


def json_value(value: object) -> str:
    if isinstance(value,Decimal):
        return str(value)
    if isinstance(value,(date,datetime)):
        return value.isoformat()
    raise TypeError(f'Unsupported JSON type: {type(value).__name__}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True,choices=sorted(CANONICAL_SKUS))
    parser.add_argument('--campaign-id',action='append',dest='campaign_ids')
    parser.add_argument('--maximum-bytes-billed',type=int,default=DEFAULT_MAXIMUM_BYTES_BILLED)
    parser.add_argument('--output',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    packet = retrieve_campaign_performance(bigquery.Client(project=PROJECT,location=LOCATION),
        args.sku,args.campaign_ids,args.maximum_bytes_billed)
    packet['retrieved_at'] = datetime.now(timezone.utc).isoformat()
    output = args.output or ROOT/f'data/generated/retrieval/{args.sku}_campaign_performance.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(packet,indent=2,default=json_value))
    LOG.info(json.dumps({'event':'campaign_performance_retrieved','sku':args.sku,
        'campaigns':len(packet['summaries']),
        'daily_rows':sum(s['performance_rows'] for s in packet['summaries']),'output':str(output)}))


if __name__ == '__main__':
    main()
