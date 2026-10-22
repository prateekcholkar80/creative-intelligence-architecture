"""Opt-in owned P6 native read fixtures; no serving or model writes.

Only fixture preflight and exact table references are adapted. Production reader
SQL/config/native correlation/decoding run against real BigQuery jobs unchanged.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
import re
from unittest.mock import patch
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application import campaign_permission_reader as reader
from src.application.campaign_permission_storage import TABLE_SCHEMAS,table_metadata
from src.application.workflow_contract import ContractError,canonical_json_bytes
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION,DATASET,digest
from src.ingestion.setup_workflow_tables import validate_table
from src.validation.campaign_permission_fixtures import fixtures,CASES

TTL_MS=7200000
OUTPUT=Path('data/generated/campaign_permission_diagnostics')


class Scope:
    def __init__(self,run_id):
        if type(run_id) is not str or re.fullmatch('[0-9a-f]{32}',run_id) is None:raise ValueError('Invalid owned run ID')
        self.run_id=run_id;self.dataset_id='ci_campaign_permission_it_'+run_id
        self.dataset=PROJECT+'.'+self.dataset_id
        self.labels={'purpose':'campaign_permission_integration','run_id':run_id}
        self.created=None

    def table(self,name):
        if name not in TABLE_SCHEMAS:raise ValueError('Unrelated fixture table')
        return self.dataset+'.'+name

    def relocate(self,sql):
        for name in TABLE_SCHEMAS:
            sql=sql.replace(f'`{PROJECT}.{DATASET}.{name}`',f'`{self.table(name)}`')
        self.assert_sql(sql)
        return sql

    def assert_sql(self,sql):
        refs=re.findall(r'`([^`]+)`',sql)
        bare=re.sub(r'`[^`]+`',' ',sql)
        if (not refs or not set(refs)<={self.table(n) for n in TABLE_SCHEMAS}
            or f'{PROJECT}.{DATASET}' in sql
            or re.search(r'\b(?:FROM|JOIN|INTO|TABLE)\s+[\w-]+\.[\w.-]+',bare,re.I)
            or re.search(r'\b(?:INSERT|UPDATE|DELETE|MERGE|DROP|TRUNCATE)\b',bare,re.I)):
            raise ValueError('Unsafe fixture SQL')

    def own(self,client):
        ds=client.get_dataset(self.dataset,retry=None,timeout=20)
        if (ds.project!=PROJECT or ds.dataset_id!=self.dataset_id
            or (ds.location or '').lower()!=LOCATION or ds.labels!=self.labels
            or ds.default_table_expiration_ms!=TTL_MS or self.created is None or ds.created!=self.created):
            raise ValueError('Fixture ownership differs; refusing use/cleanup')
        return ds

    def expected(self,name):
        metadata=table_metadata(name,PROJECT).to_api_repr()
        metadata['tableReference']['datasetId']=self.dataset_id
        return bigquery.Table.from_api_repr(metadata)

    def inspect(self,client):
        self.own(client)
        for name in TABLE_SCHEMAS:
            actual=client.get_table(self.table(name),retry=None,timeout=20)
            if (actual.expires is None or actual.created is None
                or abs((actual.expires-actual.created).total_seconds()-TTL_MS/1000)>120):
                raise ValueError('Fixture expiry differs')
            # Validate all production schema/layout constraints on a metadata copy
            # after positively checking the actual owned resource expiry. Physical
            # table expiry and exact native jobs are never changed or fabricated.
            copy=bigquery.Table.from_api_repr(deepcopy(actual.to_api_repr()));copy.expires=None
            validate_table(copy,self.expected(name))
        return {'status':'VERIFIED'}


def validate_report(report):
    if (len(report['cases'])!=len(CASES) or {c['case'] for c in report['cases']}!={c for c,_ in CASES}
        or not all(c['status']=='PASSED' and type(c['native_job_id']) is str and c['native_job_id'] for c in report['cases'])
        or len({c['native_job_id'] for c in report['cases']})!=len(CASES)
        or report.get('errors',[])
        or not report['fixture_rows_preserved'] or not report['cleanup_verified']
        or report['serving_writes']!=0 or report['model_calls']!=0):
        raise ValueError('Incomplete native permission report')
    expected=dict(CASES)
    for case in report['cases']:
        if case['reason_code']!=expected[case['case']]:raise ValueError('Native denial report differs')


def _json_row(row):
    return {key:(value.astimezone(timezone.utc).isoformat() if isinstance(value,datetime) else value)
            for key,value in dict(row).items()}


def fixture_bytes(value):
    # Deliberately invalid oversized cells must remain visible in preservation
    # snapshots. This probe-only serializer never changes production JSON limits.
    raw=json.dumps(value,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode()
    if len(raw)>8*1024*1024:raise ValueError('Fixture preservation snapshot too large')
    return raw


def snapshot(client,scope):
    scope.own(client);result={}
    for name in TABLE_SCHEMAS:
        sql=f'SELECT * FROM `{scope.table(name)}`';scope.assert_sql(sql)
        cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=256*1024*1024,job_timeout_ms=60000)
        job=client.query(sql,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
        rows=[_json_row(r) for r in job.result(timeout=60,max_results=1000)]
        if len(rows)>=1000:raise ValueError('Fixture snapshot overflow')
        result[name]=sorted(rows,key=fixture_bytes)
    return result


def load_rows(client,scope,name,values):
    cfg=bigquery.LoadJobConfig(schema=scope.expected(name).schema,write_disposition='WRITE_APPEND')
    # Upload APIs use num_retries, unlike query/metadata RPC retry arguments.
    job=client.load_table_from_json([_json_row(r) for r in values],scope.table(name),job_config=cfg,
        job_id='campaign_permission_seed_'+scope.run_id+'_'+name,location=LOCATION,num_retries=0,timeout=20)
    job.result(timeout=90)
    return job.job_id


def run(client,scope,report):
    if client.project!=PROJECT:raise ValueError('Wrong native fixture project')
    ds=bigquery.Dataset(scope.dataset);ds.location=LOCATION;ds.labels=scope.labels;ds.default_table_expiration_ms=TTL_MS
    created=client.create_dataset(ds,retry=None,timeout=20)
    scope.created=created.created
    if scope.created is None:raise ValueError('Missing owned creation stamp')
    scope.own(client)
    for name in TABLE_SCHEMAS:
        client.create_table(scope.expected(name),retry=None,timeout=20)
    now=datetime.now(timezone.utc);rows,contexts=fixtures(now)
    for name,values in rows.items():
        scope.own(client)
        report['load_jobs'].append(load_rows(client,scope,name,values))
    scope.inspect(client);before=snapshot(client,scope)
    original_spec=reader.query_spec
    def relocated(request):
        sql,cfg=original_spec(request);return scope.relocate(sql),cfg
    with patch.object(reader,'query_spec',side_effect=relocated),patch.object(reader,'inspect',side_effect=scope.inspect):
        for name,expected in CASES:
            scope.own(client);context=contexts[name];reason=None;native=None
            try:
                result=reader.read_permission(client,context['request'])
                native=result.native_job_id
                if expected is not None or result.state.current.sha256!=context['expected_record'].sha256:
                    raise ValueError('Unexpected permission or decoded record')
                if name=='valid_claim' and len(result.evidence_states)!=1:raise ValueError('Positive join missing evidence')
            except ContractError as error:
                reason=error.code;native=getattr(error,'native_job_id',None)
                if expected!=reason or native is None:raise
            report['cases'].append(dict(case=name,status='PASSED',reason_code=reason,native_job_id=native))
            print(json.dumps(dict(case=name,status='PASSED')),flush=True)
    after=snapshot(client,scope)
    report['fixture_rows_preserved']=before==after
    report['fixture_before_sha256']=digest(fixture_bytes(before))
    report['fixture_after_sha256']=digest(fixture_bytes(after))
    if before!=after:raise ValueError('Reader changed fixture rows')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true',required=True)
    args=parser.parse_args();scope=Scope(uuid4().hex)
    report=dict(run_id=scope.run_id,dataset=scope.dataset,status='FAILED',cases=[],load_jobs=[],
        fixture_rows_preserved=False,cleanup_verified=False,serving_writes=0,model_calls=0,
        started_at=datetime.now(timezone.utc).isoformat(),errors=[])
    OUTPUT.mkdir(parents=True,exist_ok=True);path=OUTPUT/(scope.run_id+'.json')
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        try:run(client,scope,report)
        except Exception as error:report['errors'].append(type(error).__name__+': '+str(error))
        finally:
            if scope.created is not None:
                try:
                    scope.own(client);client.delete_dataset(scope.dataset,delete_contents=True,retry=None,timeout=20)
                    try:client.get_dataset(scope.dataset,retry=None,timeout=20)
                    except NotFound:report['cleanup_verified']=True
                    else:raise ValueError('Fixture dataset still exists')
                except Exception as error:report['errors'].append('cleanup: '+type(error).__name__+': '+str(error))
            report['completed_at']=datetime.now(timezone.utc).isoformat()
            if not report['errors']:
                try:validate_report(report);report['status']='PASSED'
                except Exception as error:report['errors'].append(str(error))
            path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(status=report['status'],report=str(path),errors=report['errors'])),flush=True)
    if report['status']!='PASSED':raise SystemExit(1)


if __name__=='__main__':main()
