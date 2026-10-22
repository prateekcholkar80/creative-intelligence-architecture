"""Configure only the documented P6 DRAFT rules. No approval or serving activation.

--apply creates missing exact tables and submits one deterministic atomic insert.
--check reads and compares every scoped row. Conflicting rows are never replaced.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from src.application.campaign_acceptance_contract import RULE_IDS
from src.application.workflow_contract import canonical_json_bytes
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, SchemaMismatch, validate_table

DATASET = 'ci_brand_memory'
POLICY_ID = 'naked-and-thriving-campaign-acceptance'
VERSION = 'draft-v0.1'
HEADER = 'campaign_acceptance_policy_versions'
RULES = 'campaign_acceptance_rule_versions'
SOURCE = Path(__file__).resolve().parents[2] / 'docs/CAMPAIGN_ACCEPTANCE_POLICY.md'
SOURCES = {VERSION: SOURCE, 'draft-v0.2': SOURCE.with_name('CAMPAIGN_ACCEPTANCE_POLICY_V02.md')}
TIMEOUT = 20


def digest(data):
    return hashlib.sha256(data).hexdigest()


def manifest(path=SOURCE, *, version=VERSION):
    if version not in SOURCES:raise ValueError('Unsupported DRAFT version')
    raw = path.read_bytes()
    definitions = []
    for line in raw.decode('utf-8').splitlines():
        if not line.startswith('| '):
            continue
        parts = [p.strip() for p in line.strip('|').split('|')]
        if parts[0] in RULE_IDS:
            if len(parts) != 3:
                raise ValueError('Malformed acceptance rule')
            definitions.append(dict(rule_id=parts[0], area={'A':'ACCURACY', 'G':'GROUNDING',
                'C':'COMPLIANCE', 'U':'USABILITY'}[parts[0][0]],
                acceptance_rule=parts[1], failure_handling=parts[2],
                applicability_status='PENDING_REVIEW', checker_status='NOT_IMPLEMENTED'))
    definitions.sort(key=lambda row: row['rule_id'])
    if tuple(r['rule_id'] for r in definitions) != RULE_IDS:
        raise ValueError('Exact unique 47-rule coverage required')
    source_hash = digest(raw)
    header = dict(contract_version='campaign-acceptance-draft-storage-v1',
        brand='Naked & Thriving', status='DRAFT', approved_by=None, approved_at=None,
        serving_enabled=False, specification_sha256=source_hash,
        rules_sha256=digest(canonical_json_bytes(definitions)), rule_count=len(definitions),
        stages=['INTERPRETATION','CLARIFICATION','GENERATION','REFINEMENT'],
        channels=['META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'],
        requested_markets=['NORTH_AMERICA','CANADA','EUROPE','UK','AUSTRALIA'],
        confirmed_english_markets=['CANADA','EUROPE'], country_scope_status='PENDING_EXACT_COUNTRIES',
        audience='NEW_AND_RETURNING_CUSTOMERS_35_PLUS', outputs='DRAFTS_ONLY',
        approval_workflow='APPROVED_CLAIM_LIBRARY_PLUS_MARKETING_MANAGER_FINAL_CAMPAIGN_REVIEW',
        publication_notes='Full specification contains playbook conflicts, evidence requirements and pending review inputs.')
    if version=='draft-v0.2':
        header.update(benchmark_countries=['AU','CA','GB','US'],deferred_benchmark_regions=['EUROPE'],
            country_scope_status='PENDING_REVIEWED_COUNTRY_CHANNEL_OVERLAYS',
            brand_directives=dict(ai_people='PROHIBITED',before_after_imagery='PROHIBITED',
                efficacy_changing_retouching='PROHIBITED',authority='EXPLICIT_USER_DIRECTION_2026_10_15'),
            predecessor_version=VERSION)
    rows = []
    for item in [header] + definitions:
        encoded = canonical_json_bytes(item)
        row = dict(policy_id=POLICY_ID, version=version, status='DRAFT',
            specification_sha256=source_hash, payload_sha256=digest(encoded),
            payload_canonical_json=encoded.decode('utf-8'))
        if 'rule_id' in item:
            row['rule_id'] = item['rule_id']
        rows.append(row)
    return rows


def table(name):
    fields = ['policy_id','version'] + (['rule_id'] if name == RULES else []) + [
        'status','specification_sha256','payload_sha256','payload_canonical_json']
    schema = [bigquery.SchemaField(f, 'STRING', mode='REQUIRED') for f in fields]
    schema.append(bigquery.SchemaField('recorded_at', 'TIMESTAMP', mode='REQUIRED'))
    result = bigquery.Table(f'{PROJECT}.{DATASET}.{name}', schema=schema)
    result.clustering_fields = ['policy_id','version']
    result.description = 'P6 draft-only versioned acceptance configuration; not approval or runtime activation'
    return result


def preflight(client, *, apply):
    if client.project != PROJECT:
        raise ValueError('Wrong project')
    ds = client.get_dataset(f'{PROJECT}.{DATASET}', retry=None, timeout=TIMEOUT)
    if (ds.project != PROJECT or ds.dataset_id != DATASET or ds.location.lower() != LOCATION
            or ds.default_table_expiration_ms is not None
            or ds.default_partition_expiration_ms is not None):
        raise SchemaMismatch('Dataset identity/location/expiration differs')
    missing = []
    for name in (HEADER, RULES):
        try:
            actual = client.get_table(table(name).reference, retry=None, timeout=TIMEOUT)
        except NotFound:
            missing.append(name)
        else:
            validate_table(actual, table(name))
    if apply:
        for name in missing:
            try:
                client.create_table(table(name), retry=None, timeout=TIMEOUT)
            except Conflict:
                pass
            validate_table(client.get_table(table(name).reference, retry=None, timeout=TIMEOUT), table(name))
    return missing


def settings(parameters):
    return bigquery.QueryJobConfig(query_parameters=parameters, use_query_cache=False,
                                  maximum_bytes_billed=256*1024*1024, job_timeout_ms=60000)


def write_spec(rows, *, version=VERSION):
    """Bounded SQL; exact replay or conflict, never update/delete existing rows."""
    if (version not in SOURCES or type(rows) is not list or len(rows)!=48
            or any(type(row) is not dict or row.get('version')!=version or row.get('policy_id')!=POLICY_ID
                or row.get('status')!='DRAFT' for row in rows)):
        raise ValueError('Exact scoped DRAFT rows required')
    if rows!=manifest(SOURCES[version],version=version):raise ValueError('DRAFT rows differ from the pinned specification')
    head, rules = rows[0], rows[1:]
    params = [bigquery.ScalarQueryParameter('policy_id','STRING',POLICY_ID),
              bigquery.ScalarQueryParameter('version','STRING',version),
              bigquery.ScalarQueryParameter('header','STRING',canonical_json_bytes(head).decode()),
              bigquery.ArrayQueryParameter('rules','STRING',
                  [canonical_json_bytes(row).decode() for row in rules])]
    sql = f'''
CREATE TEMP TABLE expected AS
SELECT JSON_VALUE(j, '$.policy_id') policy_id, JSON_VALUE(j, '$.version') version,
 JSON_VALUE(j, '$.rule_id') rule_id, JSON_VALUE(j, '$.status') status,
 JSON_VALUE(j, '$.specification_sha256') specification_sha256,
 JSON_VALUE(j, '$.payload_sha256') payload_sha256,
 JSON_VALUE(j, '$.payload_canonical_json') payload_canonical_json
FROM UNNEST(ARRAY_CONCAT([@header], @rules)) j;
BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{HEADER}` WHERE policy_id=@policy_id AND version=@version)
 IN (0,1) AS 'HEADER_DUPLICATE';
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{RULES}` WHERE policy_id=@policy_id AND version=@version)
 IN (0,47) AS 'RULE_PARTIAL_STATE';
ASSERT ((SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{HEADER}` WHERE policy_id=@policy_id AND version=@version)=0)
 = ((SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{RULES}` WHERE policy_id=@policy_id AND version=@version)=0)
 AS 'POLICY_PARTIAL_STATE';
ASSERT NOT EXISTS (
 SELECT policy_id,version,CAST(NULL AS STRING) rule_id,status,specification_sha256,payload_sha256,payload_canonical_json
 FROM `{PROJECT}.{DATASET}.{HEADER}` WHERE policy_id=@policy_id AND version=@version
 EXCEPT DISTINCT SELECT * FROM expected WHERE rule_id IS NULL) AS 'HEADER_CONFLICT';
ASSERT NOT EXISTS (
 SELECT policy_id,version,rule_id,status,specification_sha256,payload_sha256,payload_canonical_json
 FROM `{PROJECT}.{DATASET}.{RULES}` WHERE policy_id=@policy_id AND version=@version
 EXCEPT DISTINCT SELECT * FROM expected WHERE rule_id IS NOT NULL) AS 'RULE_CONFLICT';
ASSERT (SELECT COUNT(DISTINCT rule_id) FROM `{PROJECT}.{DATASET}.{RULES}` WHERE policy_id=@policy_id AND version=@version)
 = (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{RULES}` WHERE policy_id=@policy_id AND version=@version)
 AS 'RULE_DUPLICATE';
IF (SELECT COUNT(*) FROM `{PROJECT}.{DATASET}.{HEADER}` WHERE policy_id=@policy_id AND version=@version)=0 THEN
 INSERT `{PROJECT}.{DATASET}.{HEADER}` (policy_id,version,status,specification_sha256,payload_sha256,payload_canonical_json,recorded_at)
 SELECT policy_id,version,status,specification_sha256,payload_sha256,payload_canonical_json,CURRENT_TIMESTAMP()
 FROM expected WHERE rule_id IS NULL;
 INSERT `{PROJECT}.{DATASET}.{RULES}` (policy_id,version,rule_id,status,specification_sha256,payload_sha256,payload_canonical_json,recorded_at)
 SELECT *,CURRENT_TIMESTAMP() FROM expected WHERE rule_id IS NOT NULL;
END IF;
COMMIT TRANSACTION;
'''
    return sql, settings(params)


def verify(client, rows, *, version=VERSION):
    if version not in SOURCES:raise ValueError('Unsupported DRAFT version')
    results = []
    for name in (HEADER, RULES):
        cfg = settings([bigquery.ScalarQueryParameter('policy_id','STRING',POLICY_ID),
                        bigquery.ScalarQueryParameter('version','STRING',version)])
        sql = f'SELECT * FROM `{PROJECT}.{DATASET}.{name}` WHERE policy_id=@policy_id AND version=@version'
        values = list(client.query(sql, job_config=cfg, location=LOCATION, retry=None,
                                   timeout=TIMEOUT).result(timeout=60))
        for value in values:
            row = dict(value)
            stamp = row.pop('recorded_at')
            if stamp is None or stamp.tzinfo is None:
                raise ValueError('Invalid recorded timestamp')
            if digest(row['payload_canonical_json'].encode()) != row['payload_sha256']:
                raise ValueError('Stored payload hash differs')
            results.append(row)
    if sorted(results, key=lambda r:r.get('rule_id','')) != sorted(rows, key=lambda r:r.get('rule_id','')):
        raise ValueError('Authoritative full-row comparison differs')
    return dict(status='VERIFIED_DRAFT', policy_id=POLICY_ID, version=version,
                header_rows=1, rule_rows=47, specification_sha256=rows[0]['specification_sha256'],
                policy_payload_sha256=rows[0]['payload_sha256'], serving_enabled=False)


def configure(client, *, apply=False, version=VERSION):
    if version not in SOURCES:raise ValueError('Unsupported DRAFT version')
    rows = manifest(SOURCES[version],version=version)
    missing = preflight(client, apply=apply)
    if missing and not apply:
        return dict(status='MISSING_TABLES', missing_tables=missing)
    job_id = 'campaign_acceptance_draft_' + digest(f'{PROJECT}/{DATASET}/{POLICY_ID}/{version}'.encode())
    if apply:
        sql, cfg = write_spec(rows,version=version)
        try:
            job = client.get_job(job_id, location=LOCATION, retry=None, timeout=TIMEOUT)
        except NotFound:
            try:
                job = client.query(sql, job_config=cfg, job_id=job_id, location=LOCATION,
                                   retry=None, job_retry=None, timeout=TIMEOUT)
            except Conflict:
                job = client.get_job(job_id, location=LOCATION, retry=None, timeout=TIMEOUT)
        actual = job.to_api_repr().get('configuration', {}).get('query', {})
        expected = cfg.to_api_repr()['query']
        if (job.project != PROJECT or job.location.lower() != LOCATION or job.job_id != job_id
                or job.query != sql or any(actual.get(k) != v for k,v in expected.items())
                or job.to_api_repr().get('configuration',{}).get('jobTimeoutMs') != '60000'):
            raise ValueError('Original job identity/config conflict; no new submission')
        job.result(timeout=90)
    return dict(verify(client, rows,version=version), created_tables=missing if apply else [], original_write_job_id=job_id,
                project=PROJECT, dataset=DATASET, location=LOCATION)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--apply', action='store_true'); mode.add_argument('--check', action='store_true')
    parser.add_argument('--draft-version',choices=tuple(SOURCES),default=VERSION)
    args = parser.parse_args()
    with bigquery.Client(project=PROJECT, location=LOCATION) as client:
        print(json.dumps(configure(client, apply=args.apply,version=args.draft_version), indent=2))


if __name__ == '__main__':
    main()
