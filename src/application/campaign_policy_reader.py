"""Fresh read-only decoder for the stored P6 DRAFT configuration.

No local manifest fallback, approval inference, permission storage or runtime
activation. Only the draft-storage-v1 format is supported; approved operational
policy will require its own reviewed lifecycle/storage contract.
"""
from __future__ import annotations
import argparse
from dataclasses import dataclass
from itertools import islice
import json
from uuid import uuid4

from google.cloud import bigquery
from src.application.campaign_acceptance_contract import RULE_IDS, _hash, _utc
from src.application.workflow_contract import ContractError, _require, _id, _digest, canonical_json_bytes
from src.ingestion.configure_campaign_acceptance import (
    PROJECT, LOCATION, DATASET, HEADER, RULES, POLICY_ID, VERSION, SOURCES,
    preflight, settings, digest,
)

MAX_ROWS = 50
MAX_JSON_BYTES = 65536
HEADER_KEYS = {'contract_version','brand','status','approved_by','approved_at','serving_enabled',
    'specification_sha256','rules_sha256','rule_count','stages','channels','requested_markets',
    'confirmed_english_markets','country_scope_status','audience','outputs','approval_workflow','publication_notes'}
RULE_KEYS = {'rule_id','area','acceptance_rule','failure_handling','applicability_status','checker_status'}
HEADER_KEYS_V02=HEADER_KEYS|{'benchmark_countries','deferred_benchmark_regions','brand_directives','predecessor_version'}
ROW_KEYS = {'record_kind','policy_id','version','rule_id','status','specification_sha256',
            'payload_sha256','payload_canonical_json','recorded_at'}


def parse_payload(value):
    _require(type(value) is str, 'INVALID_CAMPAIGN_POLICY_JSON')
    try:
        raw=value.encode('utf-8')
        _require(len(raw)<=MAX_JSON_BYTES,'CAMPAIGN_POLICY_JSON_TOO_LARGE')
        data=json.loads(value)
        _require(type(data) is dict and canonical_json_bytes(data)==raw,
                 'NONCANONICAL_CAMPAIGN_POLICY_JSON')
        return data
    except (ValueError,TypeError,UnicodeError,RecursionError):
        raise ContractError('INVALID_CAMPAIGN_POLICY_JSON') from None


@dataclass(frozen=True, slots=True)
class DraftPolicySnapshot:
    policy_id: str
    version: str
    specification_sha256: str
    header_sha256: str
    header_canonical_json: str
    rule_canonical_json: tuple[str, ...]
    snapshot_sha256: str
    native_job_id: str

    def require_serving_ready(self):
        # This format has no reviewed operational checks/country overlays and
        # can never become ready by toggling a stored boolean/status.
        raise ContractError('CAMPAIGN_POLICY_NOT_SERVING_READY')


def decode_snapshot(rows, *, policy_id, version, specification_sha256,
                    header_sha256, native_job_id):
    for value in (policy_id,version,native_job_id): _id(value)
    _digest(specification_sha256); _digest(header_sha256)
    _require(type(rows) is tuple and len(rows)==48,'CAMPAIGN_POLICY_ROW_COVERAGE_REQUIRED')
    header=None; rules={}; metadata=[]
    for supplied in rows:
        row=dict(supplied)
        _require(set(row)==ROW_KEYS,'INVALID_CAMPAIGN_POLICY_ROW_SHAPE')
        _require(row['policy_id']==policy_id and row['version']==version
                 and row['specification_sha256']==specification_sha256
                 and row['status']=='DRAFT','CAMPAIGN_POLICY_SCOPE_MISMATCH')
        stamp=_utc(row['recorded_at'])
        _digest(row['payload_sha256'])
        payload=parse_payload(row['payload_canonical_json'])
        _require(digest(row['payload_canonical_json'].encode())==row['payload_sha256'],
                 'CAMPAIGN_POLICY_HASH_MISMATCH')
        if row['record_kind']=='HEADER':
            _require(header is None and row['rule_id'] is None
                and set(payload)==(HEADER_KEYS_V02 if version=='draft-v0.2' else HEADER_KEYS),
                     'INVALID_CAMPAIGN_POLICY_HEADER')
            if version=='draft-v0.2':
                _require(payload['benchmark_countries']==['AU','CA','GB','US']
                    and payload['deferred_benchmark_regions']==['EUROPE'] and payload['predecessor_version']==VERSION
                    and payload['brand_directives']==dict(ai_people='PROHIBITED',before_after_imagery='PROHIBITED',
                        efficacy_changing_retouching='PROHIBITED',authority='EXPLICIT_USER_DIRECTION_2026_10_15'),
                    'CAMPAIGN_POLICY_DIRECTIVES_CONFLICT')
            _require(row['payload_sha256']==header_sha256
                and payload['contract_version']=='campaign-acceptance-draft-storage-v1'
                and payload['specification_sha256']==specification_sha256
                and payload['status']=='DRAFT' and payload['approved_by'] is None
                and payload['approved_at'] is None and payload['serving_enabled'] is False
                and type(payload['rule_count']) is int and payload['rule_count']==47
                and payload['country_scope_status']==('PENDING_REVIEWED_COUNTRY_CHANNEL_OVERLAYS'
                    if version=='draft-v0.2' else 'PENDING_EXACT_COUNTRIES')
                and payload['outputs']=='DRAFTS_ONLY','CAMPAIGN_POLICY_HEADER_PIN_MISMATCH')
            # The trusted full header digest also pins every scope/brand field.
            header=(payload,row['payload_canonical_json'])
        else:
            _require(row['record_kind']=='RULE' and set(payload)==RULE_KEYS,
                     'INVALID_CAMPAIGN_POLICY_RULE')
            rule_id=row['rule_id']
            _require(type(rule_id) is str and rule_id in RULE_IDS and rule_id not in rules
                and payload['rule_id']==rule_id
                and payload['area']=={'A':'ACCURACY','G':'GROUNDING','C':'COMPLIANCE','U':'USABILITY'}[rule_id[0]]
                and payload['applicability_status']=='PENDING_REVIEW'
                and payload['checker_status']=='NOT_IMPLEMENTED', 'CAMPAIGN_POLICY_RULE_CONFLICT')
            for key in ('acceptance_rule','failure_handling'):
                _require(type(payload[key]) is str and 0<len(payload[key])<=4096,
                         'INVALID_CAMPAIGN_POLICY_RULE_TEXT')
            rules[rule_id]=(payload,row['payload_canonical_json'])
        metadata.append(dict(kind=row['record_kind'],rule_id=row['rule_id'],
                             sha256=row['payload_sha256'],recorded_at=stamp.isoformat()))
    _require(header is not None and tuple(sorted(rules))==RULE_IDS,
             'CAMPAIGN_POLICY_ROW_COVERAGE_REQUIRED')
    _require(_hash([rules[k][0] for k in RULE_IDS])==header[0]['rules_sha256'],
             'CAMPAIGN_POLICY_RULESET_HASH_MISMATCH')
    snapshot_hash=_hash(dict(policy_id=policy_id,version=version,
        rows=sorted(metadata,key=lambda r:(r['kind'],r['rule_id'] or ''))))
    return DraftPolicySnapshot(policy_id,version,specification_sha256,header_sha256,
        header[1],tuple(rules[k][1] for k in RULE_IDS),snapshot_hash,native_job_id)


def query_spec(policy_id,version):
    _id(policy_id); _id(version)
    sql=f'''SELECT 'HEADER' record_kind,policy_id,version,CAST(NULL AS STRING) rule_id,
 status,specification_sha256,payload_sha256,payload_canonical_json,recorded_at
 FROM `{PROJECT}.{DATASET}.{HEADER}` WHERE policy_id=@policy_id AND version=@version
 UNION ALL
 SELECT 'RULE' record_kind,policy_id,version,rule_id,status,specification_sha256,
 payload_sha256,payload_canonical_json,recorded_at
 FROM `{PROJECT}.{DATASET}.{RULES}` WHERE policy_id=@policy_id AND version=@version
 ORDER BY record_kind,rule_id LIMIT {MAX_ROWS}'''
    cfg=settings([bigquery.ScalarQueryParameter('policy_id','STRING',policy_id),
                  bigquery.ScalarQueryParameter('version','STRING',version)])
    return sql,cfg


def read_policy(client, *, policy_id, version, specification_sha256, header_sha256):
    # Validate trusted pins before any provider call.
    _id(policy_id); _id(version); _digest(specification_sha256); _digest(header_sha256)
    _require(client.project==PROJECT,'CAMPAIGN_POLICY_PROJECT_MISMATCH')
    _require(not preflight(client,apply=False),'CAMPAIGN_POLICY_TABLES_MISSING')
    sql,cfg=query_spec(policy_id,version)
    native_id='campaign_policy_read_'+uuid4().hex
    job=client.query(sql,job_config=cfg,job_id=native_id,location=LOCATION,
                     retry=None,job_retry=None,timeout=20)
    values=tuple(dict(r) for r in islice(job.result(timeout=60,max_results=MAX_ROWS,page_size=MAX_ROWS),MAX_ROWS))
    authoritative=client.get_job(native_id,location=LOCATION,retry=None,timeout=20)
    actual=authoritative.to_api_repr().get('configuration',{})
    _require(authoritative.project==PROJECT and authoritative.location.lower()==LOCATION
        and authoritative.job_id==native_id and authoritative.query==sql
        and authoritative.state=='DONE' and authoritative.error_result is None
        and actual.get('jobTimeoutMs')=='60000'
        and all(actual.get('query',{}).get(k)==v for k,v in cfg.to_api_repr()['query'].items()),
        'CAMPAIGN_POLICY_NATIVE_JOB_MISMATCH')
    return decode_snapshot(values,policy_id=policy_id,version=version,
        specification_sha256=specification_sha256,header_sha256=header_sha256,native_job_id=native_id)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--specification-sha256',required=True)
    parser.add_argument('--header-sha256',required=True)
    parser.add_argument('--draft-version',choices=tuple(SOURCES),default=VERSION)
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        snapshot=read_policy(client,policy_id=POLICY_ID,version=args.draft_version,
            specification_sha256=args.specification_sha256,header_sha256=args.header_sha256)
        print(json.dumps(dict(status='VERIFIED_DRAFT',policy_id=snapshot.policy_id,
            version=snapshot.version,rule_count=len(snapshot.rule_canonical_json),
            snapshot_sha256=snapshot.snapshot_sha256,native_job_id=snapshot.native_job_id,
            serving_ready=False),indent=2))


if __name__=='__main__':main()
