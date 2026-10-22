"""Retrieve monthly sales, equal-window trends, and synthetic campaign-period totals.

Run: python -m src.retrieval.sales_analytics --sku CI-SKU-0001
Only BigQuery is used for business evidence. Config defines the query scope, not
sales facts. Local exports/audits never serve as fallback or counterfactual data.
"""
from __future__ import annotations

import argparse
import calendar
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from google.cloud import bigquery

from src.generators.sales_generator import LOCATION, PROJECT, ROOT
from src.retrieval.campaign_performance import aggregate_ratio, json_value, validate_selection
from src.retrieval.product_knowledge import validate_sku
from src.retrieval.request import CANONICAL_SKUS, OBJECTIVES

CONFIG = ROOT/'config/sales_retrieval.json'
SQL = ROOT/'sql/retrieve_sales_analytics.sql'
LOG = logging.getLogger(__name__)
COUNTS = ('units_sold','orders')
MONEY = ('gross_sales','discount_amount','refund_amount','net_sales')


@dataclass(frozen=True)
class SalesAnalyticsRead:
    """Validated structured-sales packet and its native BigQuery read identity."""

    packet: dict
    query_job_id: str
    observed_at: datetime
    project: str
    location: str


def _validate_read_job(job: object) -> tuple[str,str,str]:
    job_id=getattr(job,'job_id',None);project=getattr(job,'project',None);location=getattr(job,'location',None)
    if (not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',job_id)
            or project!=PROJECT or location!=LOCATION):
        raise ValueError('BigQuery sales read job identity does not match the fixed project and region')
    return job_id,project,location


def query_scope(config: dict) -> tuple[date,date,date,date]:
    start,end = date.fromisoformat(config['start_date']),date.fromisoformat(config['end_date'])
    window = config['trend_window_days']
    if (type(window) is not int or window<1 or (end-start).days+1<2*window
            or config['channel'] != 'ECOMMERCE'
            or type(config['maximum_bytes_billed']) is not int or config['maximum_bytes_billed']<=0):
        raise ValueError('Scope must cover two complete equal windows with a supported channel and positive billing cap')
    return start,end,end-timedelta(days=window-1),end-timedelta(days=2*window-1)


def validate_totals(row: dict, start: date, end: date) -> None:
    days = (end-start).days+1
    if (row['row_count'] != days or row['day_count'] != days
            or row['first_date'] != start or row['last_date'] != end
            or any(type(row[name]) is not int or row[name]<0 for name in COUNTS)
            or row['orders']>row['units_sold']
            or any(not isinstance(row[name],Decimal) or not row[name].is_finite() or row[name]<0 for name in MONEY)
            or row['net_sales'] != row['gross_sales']-row['discount_amount']-row['refund_amount']):
        raise ValueError('Invalid sales date coverage, counts, or financial totals')


def build_packet(sku: str, result: dict, config: dict) -> dict:
    validate_sku(sku)
    start,end,recent_start,previous_start = query_scope(config)
    products = [dict(p) for p in result['products']]
    if len(products) != 1:
        raise ValueError('Expected exactly one current canonical product in BigQuery')
    product = products[0]
    if (product['sku'] != sku or product['source_system'] != 'BEAUTYFEEDS'
            or not product['product_name'] or not product['brand'] or not product['currency']):
        raise ValueError('Invalid product identity or currency')
    totals = dict(result['totals'])
    validate_totals(totals,start,end)
    if totals['invalid_rows'] != 0 or totals['currency_count'] != 1:
        raise ValueError('Invalid daily financials or mixed currencies')
    monthly = [dict(m) for m in result['monthly']]
    months = [];cursor = start.replace(day=1)
    while cursor<=end:
        months.append(cursor)
        cursor = (cursor.replace(day=28)+timedelta(days=4)).replace(day=1)
    if len(monthly) != len(months) or {m['month_start'] for m in monthly} != set(months):
        raise ValueError('Missing or duplicate monthly summaries')
    for month in monthly:
        first = month['month_start'];last = first.replace(day=calendar.monthrange(first.year,first.month)[1])
        validate_totals(month,max(start,first),min(end,last))
    if any(sum(m[name] for m in monthly) != totals[name] for name in (*COUNTS,*MONEY)):
        raise ValueError('Monthly totals do not reconcile with the full period')
    windows = [dict(w) for w in result['windows']]
    by_window = {w['window_name']:w for w in windows}
    if len(windows) != 2 or set(by_window) != {'RECENT','PREVIOUS'}:
        raise ValueError('Two complete, unique trend windows required')
    validate_totals(by_window['RECENT'],recent_start,end)
    validate_totals(by_window['PREVIOUS'],previous_start,recent_start-timedelta(days=1))
    if any(sum(w[name] for w in windows)>totals[name] for name in (*COUNTS,*MONEY)):
        raise ValueError('Trend-window totals exceed the containing sales period')
    trends = dict(result['trends'])
    for name,field in (('units','units_sold'),('orders','orders'),('net_sales','net_sales')):
        change = by_window['RECENT'][field]-by_window['PREVIOUS'][field]
        fraction = trends[f'{name}_change_fraction']
        if (trends[f'{name}_change'] != change
                or (fraction is not None and (not isinstance(fraction,Decimal) or not fraction.is_finite()))
                or fraction != aggregate_ratio(change,by_window['PREVIOUS'][field])):
            raise ValueError('Trend differences/ratios must use the two aggregate windows')
    campaigns = [dict(c) for c in result['campaign_periods']]
    validate_selection(sku,[c['campaign_id'] for c in campaigns])
    if len(campaigns) != result['campaign_count'] or result['overlapping_campaign_days'] != 0:
        raise ValueError('Missing, duplicated, or overlapping synthetic campaigns')
    for campaign in campaigns:
        first,last = campaign['start_date'],campaign['end_date']
        if (campaign['sku'] != sku or campaign['source_system'] != 'SYNTHETIC_POC'
                or campaign['currency'] != product['currency'] or campaign['objective'] not in OBJECTIVES
                or type(first) is not date or type(last) is not date or last<first
                or campaign['effective_start'] != max(start,first) or campaign['effective_end'] != min(end,last)):
            raise ValueError('Invalid campaign-period evidence')
        validate_totals(campaign,campaign['effective_start'],campaign['effective_end'])
        campaign['modeled_incremental_units'] = None
        campaign['uplift_status'] = 'BASELINE_NOT_PRESENT_IN_QUERIED_BIGQUERY_SALES'
        campaign['evidence_references'] = [
            f'{PROJECT}.ci_creative.campaign_master#campaign_id={campaign["campaign_id"]}',
            sales_reference(sku,config,campaign['effective_start'],campaign['effective_end'])]
    if any(sum(c[name] for c in campaigns)>totals[name] for name in (*COUNTS,*MONEY)):
        raise ValueError('Disjoint campaign-period totals exceed the containing sales period')
    return dict(canonical_sku=sku,product=product,source_of_truth='BIGQUERY',source_system='SYNTHETIC_POC',
        channel=config['channel'],currency=product['currency'],start_date=start,end_date=end,
        trend_window_days=config['trend_window_days'],totals=totals,
        monthly=sorted(monthly,key=lambda m:m['month_start']),
        windows=by_window,trends=trends,campaign_periods=campaigns,
        evidence_references=[f'{PROJECT}.ci_product.product_master#sku={sku}',sales_reference(sku,config,start,end)],
        usage_notes=['Sales are synthetic POC simulations, not actual historical business results.',
            'Recent means the last configured window ending at end_date, not today; change fractions are ratios, not percentages.',
            'Campaign-period sales include modeled campaign effects. Period totals do not establish incremental or causal lift.',
            'The queried BigQuery sales schema has no counterfactual baseline. Incremental uplift is unavailable; '
            'local campaign_sales_effects.json is an audit export and is not used for retrieval.'])


def sales_reference(sku: str, config: dict, start: date, end: date) -> str:
    return (f'{PROJECT}.ci_sales.sku_daily_sales#sku={sku}&channel={config["channel"]}'
            f'&source_system=SYNTHETIC_POC&start_date={start.isoformat()}&end_date={end.isoformat()}')


def capture_sales_analytics(client: bigquery.Client, sku: str, config: dict) -> SalesAnalyticsRead:
    """Execute and capture one fresh authoritative structured-sales read."""
    validate_sku(sku)
    start,end,recent_start,previous_start = query_scope(config)
    params = [bigquery.ScalarQueryParameter('sku','STRING',sku),
        bigquery.ScalarQueryParameter('channel','STRING',config['channel']),
        *[bigquery.ScalarQueryParameter(name,'DATE',value) for name,value in (
            ('start_date',start),('end_date',end),('recent_start',recent_start),('previous_start',previous_start))]]
    query_job=client.query(SQL.read_text(),location=LOCATION,job_config=bigquery.QueryJobConfig(
        query_parameters=params,maximum_bytes_billed=config['maximum_bytes_billed'],use_query_cache=False))
    result=list(query_job.result())
    if len(result) != 1:
        raise ValueError('Expected one BigQuery sales aggregation result')
    packet=build_packet(sku,dict(result[0]),config)
    job_id,project,location=_validate_read_job(query_job)
    return SalesAnalyticsRead(packet=packet,query_job_id=job_id,observed_at=datetime.now(timezone.utc),
        project=project,location=location)


def retrieve_sales_analytics(client: bigquery.Client, sku: str, config: dict) -> dict:
    """Compatibility facade returning only the validated authoritative packet."""
    return capture_sales_analytics(client,sku,config).packet


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sku',required=True,choices=sorted(CANONICAL_SKUS))
    parser.add_argument('--config',type=Path,default=CONFIG)
    parser.add_argument('--output',type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(levelname)s %(message)s')
    config = json.loads(args.config.read_text())
    packet = retrieve_sales_analytics(bigquery.Client(project=PROJECT,location=LOCATION),args.sku,config)
    packet['retrieved_at'] = datetime.now(timezone.utc).isoformat()
    output = args.output or ROOT/f'data/generated/retrieval/{args.sku}_sales_analytics.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(packet,indent=2,default=json_value))
    LOG.info(json.dumps({'event':'sales_analytics_retrieved','sku':args.sku,
        'days':packet['totals']['day_count'],'months':len(packet['monthly']),
        'campaigns':len(packet['campaign_periods']),'output':str(output)}))


if __name__ == '__main__':
    main()
