"""Protected, read-only P6 measurement capture; never executes or approves a benchmark."""
from datetime import datetime, timedelta, timezone
from itertools import islice
import json
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_quality_qualification import (
    BENCHMARKS, REPORTS, REVOCATIONS, MAX_DOCUMENT_BYTES, MAX_CASES,
    QualificationRequirements, table_metadata, qualify_documents,
)
from src.application.campaign_policy_approval_receipt import PolicyApprovalPins, _principal
from src.application.campaign_publication_reader import _identity
from src.application.identity import IapIdentityAdapter
from src.application.operation_budget import require_owned_worker, operation_call, operation_iter
from src.application.control_io import query_config
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_schema import TABLES as WORKFLOW
from src.application.prompt_runtime_contract import TABLES as RUNTIME
from src.application.workflow_contract import _require, _digest
from src.application.campaign_quality_measurement_proof import GROUPS, verify_measurement_proof
from src.application.campaign_review_storage import table_metadata as control_table
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, validate_table


def capture_sql():
    fields=['CURRENT_TIMESTAMP() observed_at']
    for key, table, param in (('benchmarks',BENCHMARKS,'benchmark_sha256'),('reports',REPORTS,'evaluation_sha256')):
        scope=f'document_sha256=@{param}'
        fields += [f'ARRAY(SELECT IF(BYTE_LENGTH(canonical_json)<={MAX_DOCUMENT_BYTES},canonical_json,NULL) FROM `{PROJECT}.ci_brand_memory.{table}` WHERE {scope} LIMIT 2) {key}',
            f'(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{table}` WHERE {scope}) {key}_count']
    fields.append(f'''(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{REVOCATIONS}` WHERE
        (target_kind='BENCHMARK' AND target_sha256=@benchmark_sha256) OR
        (target_kind='REPORT' AND target_sha256=@evaluation_sha256)) revocation_count''')
    ids=f'''SELECT JSON_VALUE(c,'$.provider.invocation_id') FROM `{PROJECT}.ci_brand_memory.{REPORTS}`,
        UNNEST(JSON_QUERY_ARRAY(canonical_json,'$.cases')) c WHERE document_sha256=@evaluation_sha256'''
    invocation_scope=f'workspace_id=@workspace_id AND invocation_id IN ({ids})'
    request_scope=f'''workspace_id=@workspace_id AND request_id IN
        (SELECT request_id FROM `{PROJECT}.ci_application.model_invocations` WHERE {invocation_scope})'''
    for key,table,scope in (('invocations','model_invocations',invocation_scope),
        ('bindings','invocation_prompt_bindings',invocation_scope),('requests','requests',request_scope)):
        fields += [f'ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=65536,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_application.{table}` t WHERE {scope} LIMIT {MAX_CASES+1}) {key}',
            f'(SELECT COUNT(*) FROM `{PROJECT}.ci_application.{table}` WHERE {scope}) {key}_count']
    for key,table,scope,limit in (
        ('publication_controls','campaign_permission_controls',"control_id='campaign-permissions'",2),
        ('publication_history','campaign_permission_control_history',"control_id='campaign-permissions'",65),
        ('measurement_publications','campaign_permission_publication_audits',"permission_kind='QUALITY_MEASUREMENT' AND record_id=@evaluation_sha256",2)):
        fields += [f'ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=65536,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_brand_memory.{table}` t WHERE {scope} LIMIT {limit}) {key}',
            f'(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{table}` WHERE {scope}) {key}_count']
    return 'SELECT '+',\n'.join(fields)


SQL=capture_sql()


def decode_capture(row, *, pins, requirements, recorder_ids, publisher_manifest_sha256, native_job_id, started, ended):
    groups=('benchmarks','reports','invocations','bindings','requests')
    _require(type(row) is dict and set(row)=={'observed_at','revocation_count'}|set(groups)|{g+'_count' for g in groups}|set(GROUPS)|{g+'_count' for g in GROUPS},
        'QUALITY_QUALIFICATION_CAPTURE')
    _require(started<=row['observed_at']<=ended and (ended-started).total_seconds()<=60,
        'QUALITY_QUALIFICATION_CAPTURE_EXPIRED')
    _require(type(row['revocation_count']) is int and row['revocation_count']==0,'QUALITY_QUALIFICATION_REVOKED')
    size=0
    for group in groups:
        values=row[group];count=row[group+'_count'];limit=1 if group in groups[:2] else MAX_CASES
        _require(type(count) is int and type(values) is list and len(values)==count and 0<=count<=limit
            and (group not in groups[:2] or count==1),'QUALITY_QUALIFICATION_CAPTURE_COVERAGE')
        for value in values:
            _require(type(value) is str and len(value.encode())<=(MAX_DOCUMENT_BYTES if group in groups[:2] else 65536),
                'QUALITY_QUALIFICATION_CAPTURE_SIZE')
            size+=len(value.encode())
    _require(size<=8*MAX_DOCUMENT_BYTES,'QUALITY_QUALIFICATION_CAPTURE_SIZE')
    _require(json.loads(row['reports'][0]).get('recorded_by') in recorder_ids,'QUALITY_QUALIFICATION_RECORDER_DENIED')
    verify_measurement_proof(row,pins=pins,requirements=requirements,publisher_manifest_sha256=publisher_manifest_sha256,
        report=json.loads(row['reports'][0]),observed_at=row['observed_at'])
    return qualify_documents(row['benchmarks'][0],row['reports'][0],pins=pins,requirements=requirements,
        invocations=tuple(row['invocations']),requests=tuple(row['requests']),
        bindings=tuple(decode_binding(value) for value in row['bindings']),native_job_id=native_job_id,
        observed_at=row['observed_at'],valid_until=started+timedelta(seconds=60))


def decode_binding(value):
    row=json.loads(value)
    _require(type(row) is dict and type(row.get('bound_at')) is str,'QUALITY_QUALIFICATION_BINDING_CAPTURE')
    row['bound_at']=datetime.fromisoformat(row['bound_at'].replace('Z','+00:00'))
    return row


class QualityQualificationReader:
    def __init__(self, *, client, identity_adapter, pins, requirements, reader_ids, recorder_ids, publisher_manifest_sha256,
                 clock=lambda: datetime.now(timezone.utc)):
        _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None
            and type(identity_adapter) is IapIdentityAdapter and type(pins) is PolicyApprovalPins
            and type(requirements) is QualificationRequirements,'QUALITY_QUALIFICATION_READER_CONFIGURATION')
        for values in (reader_ids,recorder_ids):
            _require(type(values) is tuple and 0<len(values)<=32 and values==tuple(sorted(set(values))),
                'QUALITY_QUALIFICATION_READER_AUTHORITY')
            for value in values:_principal(value)
        _digest(publisher_manifest_sha256);self.publisher_manifest_sha256=publisher_manifest_sha256
        self.client,self.verifier,self.pins,self.requirements=client,identity_adapter,pins,requirements
        self.reader_ids,self.recorder_ids,self.clock=reader_ids,recorder_ids,clock

    def _actor(self,headers):
        require_owned_worker();actor=self.verifier.authenticate(headers)
        _identity(actor,self.verifier.settings,self.clock())
        _require(actor.principal.principal_id in self.reader_ids,'QUALITY_QUALIFICATION_READER_DENIED')
        return actor

    def read(self,headers,pins):
        _require(pins==self.pins,'QUALITY_QUALIFICATION_READER_PINS_CHANGED')
        actor=self._actor(headers)
        tables=[table_metadata(name) for name in (BENCHMARKS,REPORTS,REVOCATIONS)]
        tables += [WORKFLOW[name].table(PROJECT) for name in ('model_invocations','requests')]
        tables += [RUNTIME['invocation_prompt_bindings'].table(PROJECT)]
        tables += [control_table(name,PROJECT) for name in ('campaign_permission_controls','campaign_permission_control_history','campaign_permission_publication_audits')]
        for expected in tables:
            validate_table(operation_call(self.client.get_table,expected.reference,retry=None,timeout=10),expected)
        started=self.clock();native_id='quality_qualification_read_'+uuid4().hex
        config=query_config([bigquery.ScalarQueryParameter(name,'STRING',getattr(pins,name))
            for name in ('workspace_id','benchmark_sha256','evaluation_sha256')])
        job=operation_call(self.client.query,SQL,job_config=config,job_id=native_id,project=PROJECT,location=LOCATION,
            retry=None,job_retry=None,timeout=20)
        operation_call(job.result,max_results=0,retry=None,job_retry=None,timeout=45)
        native=operation_call(self.client.get_job,native_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
        _require(matches_sharing_job(native,job_id=native_id,sql=SQL,config=config)
            and native.state=='DONE' and native.error_result is None,'QUALITY_QUALIFICATION_NATIVE_MISMATCH')
        rows=list(islice(operation_iter(operation_call(native.result,max_results=2,page_size=2,
            retry=None,job_retry=None,timeout=10)),2))
        _require(len(rows)==1,'QUALITY_QUALIFICATION_RESULT')
        current=self._actor(headers)
        _require(current.principal==actor.principal and current.audience==actor.audience,'QUALITY_QUALIFICATION_ACTOR_CHANGED')
        return decode_capture(dict(rows[0]),pins=pins,requirements=self.requirements,recorder_ids=self.recorder_ids,
            publisher_manifest_sha256=self.publisher_manifest_sha256,native_job_id=native_id,started=started,ended=self.clock())
