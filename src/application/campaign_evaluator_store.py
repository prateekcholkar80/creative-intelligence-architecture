"""Immutable evaluator reservations/outcomes; no provider dispatch or approval.

One global native slot per evaluation/run/stage/chunk. Original jobs survive
unknown responses. Raw evaluation payload tables have 30-day partition expiry.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib,json
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound,Conflict
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require,_digest
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table

TABLES={kind:f'{PROJECT}.ci_application.campaign_evaluator_{kind}' for kind in ('intents','outcomes')}
MAX_RECORD_BYTES=196608


def canonical(value):
    raw=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
    _require(len(raw.encode())<=MAX_RECORD_BYTES,'EVALUATOR_RECORD_SIZE')
    return raw


def sha(value):return hashlib.sha256(canonical(value).encode()).hexdigest()


def table_metadata(kind):
    _require(kind in TABLES,'EVALUATOR_TABLE_KIND')
    table=bigquery.Table(TABLES[kind],schema=[bigquery.SchemaField('record_id','STRING',mode='REQUIRED'),
        bigquery.SchemaField('record_json','STRING',mode='REQUIRED'),bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')])
    table.time_partitioning=bigquery.TimePartitioning(type_='DAY',field='recorded_at',expiration_ms=30*86400000)
    table.require_partition_filter=True;table.clustering_fields=['record_id']
    return table


def recent(alias=''):
    return f"{alias}recorded_at BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()"


def write_sql(kind):
    _require(kind in TABLES,'EVALUATOR_TABLE_KIND');table=TABLES[kind]
    guard=''
    if kind=='outcomes':
        guard=f"ASSERT (SELECT COUNT(*) FROM `{TABLES['intents']}` WHERE record_id=@id AND record_json=@intent AND {recent()})=1 AS 'EVALUATOR_INTENT_CHANGED';\n"
    return f'''BEGIN TRANSACTION;
{guard}ASSERT (SELECT COUNT(*) FROM `{table}` WHERE record_id=@id AND {recent()})<=1 AS 'EVALUATOR_DUPLICATE';
ASSERT NOT EXISTS(SELECT 1 FROM `{table}` WHERE record_id=@id AND {recent()} AND record_json!=@record) AS 'EVALUATOR_CONFLICT';
INSERT INTO `{table}` (record_id,record_json,recorded_at)
SELECT @id,@record,CURRENT_TIMESTAMP() FROM UNNEST([1]) WHERE NOT EXISTS(SELECT 1 FROM `{table}` WHERE record_id=@id AND {recent()});
COMMIT TRANSACTION;
SELECT record_json FROM `{table}` WHERE record_id=@id AND {recent()};'''


@dataclass(frozen=True,slots=True)
class Reservation:
    record_id:str
    intent_json:str
    newly_reserved:bool


class EvaluatorStore:
    def __init__(self,client):
        _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,'EVALUATOR_CLIENT')
        self.client=client

    def preflight(self):
        for kind in TABLES:validate_table(self.client.get_table(TABLES[kind],retry=None,timeout=20),table_metadata(kind))

    def _config(self,record_id,record,kind,intent=None):
        params=[bigquery.ScalarQueryParameter('id','STRING',record_id),bigquery.ScalarQueryParameter('record','STRING',record)]
        if kind=='outcomes':params.append(bigquery.ScalarQueryParameter('intent','STRING',intent))
        return bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=256*1024**2,job_timeout_ms=60000,query_parameters=params)

    def _write(self,kind,record_id,record,*,intent=None):
        sql=write_sql(kind);cfg=self._config(record_id,record,kind,intent);job_id='evaluator_'+kind+'_v1_'+record_id
        submitted=False
        try:job=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        except NotFound:
            try:
                job=self.client.query(sql,job_id=job_id,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
                submitted=True
            except Conflict:job=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
            # Other failures propagate. In particular a lost submit response must
            # never be interpreted as a new reservation that permits dispatch.
        _require(matches_sharing_job(job,job_id=job_id,sql=sql,config=cfg),'EVALUATOR_ORIGINAL_JOB_CONFLICT')
        rows=tuple(dict(r) for r in job.result(timeout=60,max_results=2,retry=None,job_retry=None))
        native=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(native,job_id=job_id,sql=sql,config=cfg) and native.state=='DONE'
            and native.error_result is None and rows==({'record_json':record},),'EVALUATOR_COMMIT_UNVERIFIED')
        return submitted

    def reserve(self,intent,*,nonce=None):
        self.preflight();_require(type(intent) is dict and 'reservation_nonce' not in intent,'EVALUATOR_INTENT')
        record_id=intent['record_id'];_digest(record_id)
        value=dict(intent,reservation_nonce=nonce or uuid4().hex);raw=canonical(value)
        new=self._write('intents',record_id,raw)
        return Reservation(record_id,raw,new)

    def publish(self,reservation,outcome):
        _require(type(reservation) is Reservation and type(outcome) is dict
            and outcome.get('record_id')==reservation.record_id
            and outcome.get('intent_sha256')==hashlib.sha256(reservation.intent_json.encode()).hexdigest(),
            'EVALUATOR_OUTCOME_BINDING')
        self.preflight();raw=canonical(outcome)
        self._write('outcomes',reservation.record_id,raw,intent=reservation.intent_json)
        return sha(outcome)

    def _read(self,kind,record_id):
        sql=f'SELECT record_json FROM `{TABLES[kind]}` WHERE record_id=@id AND {recent()} LIMIT 2'
        cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,
            query_parameters=[bigquery.ScalarQueryParameter('id','STRING',record_id)])
        job_id='evaluator_read_'+uuid4().hex
        job=self.client.query(sql,job_id=job_id,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
        rows=tuple(dict(r) for r in job.result(timeout=60,max_results=2,retry=None,job_retry=None))
        _require(matches_sharing_job(job,job_id=job_id,sql=sql,config=cfg) and job.state=='DONE'
            and job.error_result is None and len(rows)<=1,'EVALUATOR_READ_UNVERIFIED')
        if not rows:return None
        raw=rows[0]['record_json'];value=json.loads(raw)
        _require(canonical(value)==raw and value.get('record_id')==record_id,'EVALUATOR_RECORD_INVALID')
        return value

    def recover(self,intent):
        """Read-only cold recovery; never creates a reservation or a model call."""
        self.preflight();record_id=intent['record_id'];_digest(record_id)
        stored=self._read('intents',record_id)
        if stored is None:return None
        base=dict(stored);base.pop('reservation_nonce',None)
        _require(base==intent,'EVALUATOR_RECOVERY_INTENT_CHANGED')
        raw=canonical(stored)
        for kind,value in (('intents',stored),('outcomes',self._read('outcomes',record_id))):
            if value is None:return dict(state='UNKNOWN',record_id=record_id,release_allowed=False)
            cfg=self._config(record_id,canonical(value),kind,raw if kind=='outcomes' else None)
            job_id='evaluator_'+kind+'_v1_'+record_id
            job=self.client.get_job(job_id,location=LOCATION,retry=None,timeout=20)
            _require(matches_sharing_job(job,job_id=job_id,sql=write_sql(kind),config=cfg)
                and job.state=='DONE' and job.error_result is None,'EVALUATOR_RECOVERY_JOB_UNVERIFIED')
        _require(value.get('intent_sha256')==hashlib.sha256(raw.encode()).hexdigest(),'EVALUATOR_RECOVERY_OUTCOME_CHANGED')
        return value

    def recover_batch(self,intents):
        """One current row snapshot; verify every original job without replay.

        Returned stored intents include their original nonce for trusted callers.
        This is an action-local read, never an authority or outcome cache.
        """
        _require(type(intents) is tuple and 0<len(intents)<=32 and all(type(v) is dict for v in intents),
            'EVALUATOR_BATCH_INPUT')
        ids=[v['record_id'] for v in intents]
        for value in ids:_digest(value)
        _require(len(set(ids))==len(ids),'EVALUATOR_BATCH_DUPLICATE_INPUT');self.preflight()
        sql=' UNION ALL '.join(f"SELECT '{kind}' kind,record_id,record_json FROM `{TABLES[kind]}` WHERE {recent()} AND record_id IN UNNEST(@ids)" for kind in TABLES)
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,
            query_parameters=[bigquery.ArrayQueryParameter('ids','STRING',ids)])
        jid='evaluator_batch_read_'+uuid4().hex
        job=self.client.query(sql,job_id=jid,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
        rows=tuple(dict(r) for r in job.result(timeout=60,max_results=2*len(ids)+1,retry=None,job_retry=None))
        _require(matches_sharing_job(job,job_id=jid,sql=sql,config=cfg) and job.state=='DONE' and job.error_result is None
            and len(rows)<=2*len(ids),'EVALUATOR_BATCH_READ_UNVERIFIED')
        values={}
        for row in rows:
            key=(row['kind'],row['record_id']);raw=row['record_json'];value=json.loads(raw)
            _require(key not in values and key[0] in TABLES and key[1] in ids
                and canonical(value)==raw and value.get('record_id')==key[1],'EVALUATOR_BATCH_RECORD_INVALID')
            values[key]=value
        def verify(intent):
            slot=intent['record_id'];stored=values.get(('intents',slot));outcome=values.get(('outcomes',slot))
            if stored is None:
                _require(outcome is None,'EVALUATOR_BATCH_ORPHAN');return None,None
            base=dict(stored);base.pop('reservation_nonce',None)
            _require(base==intent,'EVALUATOR_RECOVERY_INTENT_CHANGED');raw=canonical(stored)
            for kind,value in (('intents',stored),('outcomes',outcome)):
                if value is None:return stored,dict(state='UNKNOWN',record_id=slot,release_allowed=False)
                config=self._config(slot,canonical(value),kind,raw if kind=='outcomes' else None)
                original_id='evaluator_'+kind+'_v1_'+slot
                original=self.client.get_job(original_id,location=LOCATION,retry=None,timeout=20)
                _require(matches_sharing_job(original,job_id=original_id,sql=write_sql(kind),config=config)
                    and original.state=='DONE' and original.error_result is None,'EVALUATOR_RECOVERY_JOB_UNVERIFIED')
            _require(outcome.get('intent_sha256')==hashlib.sha256(raw.encode()).hexdigest(),'EVALUATOR_RECOVERY_OUTCOME_CHANGED')
            return stored,outcome
        with ThreadPoolExecutor(max_workers=3,thread_name_prefix='evaluator-original-read') as pool:
            futures=[pool.submit(copy_context().run,verify,intent) for intent in intents]
            return tuple(f.result() for f in futures)
