"""Canonical extraction/omission storage; immutable originals remain separate."""
import hashlib,json
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,Conflict
from src.application.campaign_attachment_service import PROJECT,LOCATION,TABLE as SUBMISSIONS,SQL as SUBMIT_SQL
from src.application.campaign_evidence_gaps import canonical
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require,_digest
from src.ingestion.setup_workflow_tables import validate_table

TABLES={kind:f'{PROJECT}.ci_application.campaign_evidence_{kind}' for kind in ('extractions','turn_links')}


def table_metadata(kind):
    _require(kind in TABLES,'DOCUMENT_STORE_KIND')
    table=bigquery.Table(TABLES[kind],schema=[bigquery.SchemaField('record_id','STRING',mode='REQUIRED'),
        bigquery.SchemaField('record_json','STRING',mode='REQUIRED'),bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')])
    table.clustering_fields=['record_id'];return table


def extraction_sql():
    table=TABLES['extractions']
    return f'''BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `{SUBMISSIONS}` WHERE submission_id=@submission_id AND record_json=@source)=1 AS 'DOCUMENT_SOURCE_CHANGED';
ASSERT (SELECT COUNT(*) FROM `{table}` WHERE record_id=@id)<=1 AS 'DOCUMENT_DUPLICATE';
ASSERT NOT EXISTS(SELECT 1 FROM `{table}` WHERE record_id=@id AND record_json!=@record) AS 'DOCUMENT_CONFLICT';
INSERT INTO `{table}` (record_id,record_json,recorded_at)
SELECT @id,@record,CURRENT_TIMESTAMP() FROM UNNEST([1]) WHERE NOT EXISTS(SELECT 1 FROM `{table}` WHERE record_id=@id);
COMMIT TRANSACTION;
SELECT record_json FROM `{table}` WHERE record_id=@id;'''


class DocumentStore:
    def __init__(self,client):
        _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,'DOCUMENT_STORE_CLIENT')
        self.client=client

    def preflight(self,kind):validate_table(self.client.get_table(TABLES[kind],retry=None,timeout=20),table_metadata(kind))

    def query(self,sql,parameters,*,job_id=None,write=False):
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=(256 if write else 128)*1024*1024,job_timeout_ms=60000,query_parameters=parameters)
        if job_id:
            try:job=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
            except NotFound:
                try:job=self.client.query(sql,job_id=job_id,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
                except Conflict:job=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        else:
            job_id='document_read_'+uuid4().hex
            job=self.client.query(sql,job_id=job_id,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=job_id,sql=sql,config=cfg),'DOCUMENT_JOB_MISMATCH')
        rows=list(job.result(timeout=60,max_results=2,retry=None,job_retry=None))
        native=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(native,job_id=job_id,sql=sql,config=cfg)
            and native.state=='DONE' and native.error_result is None,'DOCUMENT_JOB_UNVERIFIED')
        return tuple(dict(r) for r in rows)

    def read(self,kind,record_id):
        _digest(record_id);self.preflight(kind)
        rows=self.query(f'SELECT record_json FROM `{TABLES[kind]}` WHERE record_id=@id LIMIT 2',
            [bigquery.ScalarQueryParameter('id','STRING',record_id)])
        _require(len(rows)<=1,'DOCUMENT_RECORD_DUPLICATE')
        if not rows:return None
        raw=rows[0]['record_json'];_require(type(raw) is str and len(raw.encode())<=65536,'DOCUMENT_RECORD_SIZE')
        value=json.loads(raw)
        if kind=='extractions':_require(hashlib.sha256(canonical(value).encode()).hexdigest()==record_id,'DOCUMENT_RECORD_HASH')
        return value

    def find_extraction(self,submission_id,*,workspace_id,conversation_id,principal_id):
        from src.application.campaign_document_extraction import VERSION
        self.preflight('extractions');_digest(submission_id)
        sql=f'''SELECT record_id FROM `{TABLES['extractions']}` WHERE
JSON_VALUE(record_json,'$.submission_id')=@submission_id AND JSON_VALUE(record_json,'$.extraction.version')=@version
AND JSON_VALUE(record_json,'$.scope.workspace_id')=@workspace_id
AND JSON_VALUE(record_json,'$.scope.conversation_id')=@conversation_id
AND JSON_VALUE(record_json,'$.principal_id')=@principal_id LIMIT 2'''
        params=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in dict(submission_id=submission_id,version=VERSION,
            workspace_id=workspace_id,conversation_id=conversation_id,principal_id=principal_id).items()]
        rows=self.query(sql,params);_require(len(rows)<=1,'DOCUMENT_EXTRACTION_AMBIGUOUS')
        if not rows:return None
        _digest(rows[0]['record_id']);return rows[0]['record_id']

    def submission(self,submission_id,*,workspace_id,conversation_id,principal_id):
        _digest(submission_id)
        rows=self.query(f'SELECT record_json FROM `{SUBMISSIONS}` WHERE submission_id=@id LIMIT 2',
            [bigquery.ScalarQueryParameter('id','STRING',submission_id)])
        _require(len(rows)==1,'DOCUMENT_SUBMISSION_MISSING')
        raw=rows[0]['record_json'];_require(type(raw) is str and len(raw.encode())<=65536,'DOCUMENT_RECORD_SIZE')
        value=json.loads(raw);base=dict(value);base.pop('original',None)
        _require(hashlib.sha256(canonical(base).encode()).hexdigest()==submission_id
            and value['principal_id']==principal_id and value['scope']['workspace_id']==workspace_id
            and value['scope']['conversation_id']==conversation_id and value['eligible_for_generation'] is False,
            'DOCUMENT_SUBMISSION_SCOPE')
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024*1024,job_timeout_ms=60000,query_parameters=[
                bigquery.ScalarQueryParameter('id','STRING',submission_id),bigquery.ScalarQueryParameter('record','STRING',raw)])
        job_id='evidence_submission_v1_'+submission_id
        job=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=job_id,sql=SUBMIT_SQL,config=cfg)
            and job.state=='DONE' and job.error_result is None,'DOCUMENT_SUBMISSION_NATIVE_UNVERIFIED')
        return value

    def save_extraction(self,record,*,submission):
        self.preflight('extractions');raw=canonical(record);source=canonical(submission)
        record_id=hashlib.sha256(raw.encode()).hexdigest();sql=extraction_sql()
        params=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in dict(id=record_id,record=raw,
            submission_id=record['submission_id'],source=source).items()]
        rows=self.query(sql,params,job_id='document_extract_v1_'+record_id,write=True)
        _require(rows==({'record_json':raw},),'DOCUMENT_EXTRACTION_RECEIPT')
        return record_id

    def verify_extraction(self,record_id,record,*,submission):
        """Read the original transaction only; never replay an extraction write."""
        _digest(record_id);raw=canonical(record)
        _require(hashlib.sha256(raw.encode()).hexdigest()==record_id,'DOCUMENT_RECORD_HASH')
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=256*1024*1024,job_timeout_ms=60000,query_parameters=[
                bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in dict(id=record_id,record=raw,
                    submission_id=record['submission_id'],source=canonical(submission)).items()])
        job_id='document_extract_v1_'+record_id
        job=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(job,job_id=job_id,sql=extraction_sql(),config=cfg)
            and job.state=='DONE' and job.error_result is None,'DOCUMENT_EXTRACTION_NATIVE_UNVERIFIED')
        # Canonical stored row/hash and transaction parameters bind the durable
        # destination. Anonymous original SELECT results may expire and need
        # not be readable across principals; never replay to recreate them.
        return job_id
