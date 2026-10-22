"""Durable, owner-scoped upload intents; originals and per-file jobs stay unchanged."""
import hashlib,json
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_attachment_service import TABLE,LOCATION,prepare_submission,inspect_file
from src.application.campaign_evidence_gaps import canonical
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require

PREFIX='batch_v1_'
VERSION='campaign-evidence-batch-v1'


class ProtectedAttachmentBatches:
    def __init__(self,intake):self.intake=intake;self.store=intake.store

    def _rows(self,sql,values):
        cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
            maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,
            query_parameters=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in values.items()])
        jid='evidence_batch_read_'+uuid4().hex
        job=self.store.client.query(sql,job_id=jid,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
        rows=tuple(dict(r) for r in job.result(timeout=60,max_results=101,retry=None,job_retry=None))
        _require(matches_sharing_job(job,job_id=jid,sql=sql,config=cfg) and job.state=='DONE'
            and job.error_result is None,'ATTACHMENT_BATCH_READ_UNVERIFIED')
        return rows

    def start(self,headers,*,workspace_id,conversation_id,request_id,requirement_id,request_sha256,category,files):
        scope=dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id)
        actor,access,requests=self.intake._current(headers,**scope)
        matches=[r for r in requests if r.sha256==request_sha256
            and json.loads(r.canonical_json)['requirement_id']==requirement_id]
        _require(len(matches)==1 and type(files) is tuple and 0<len(files)<=100,'ATTACHMENT_BATCH_INVALID')
        prepared=[]
        for name,raw in files:
            sid,record=prepare_submission(matches[0],actor.principal.principal_id,choice='PROVIDE_EVIDENCE',
                category=category,filename=name,content=raw)
            prepared.append((sid,record,raw))
        base=dict(prepared[0][1]);base.pop('file')
        entries=[dict(submission_id=sid,file=r['file']) for sid,r,_ in prepared]
        _require(len({e['submission_id'] for e in entries})==len(entries),'ATTACHMENT_BATCH_DUPLICATE_FILE')
        # Reserve enough room for immutable GCS generation pins before staging.
        _require(len(canonical(dict(base=base,files=entries)).encode())<=48000,'ATTACHMENT_BATCH_TOO_LARGE')
        self.store.preflight()
        for entry,(sid,record,raw) in zip(entries,prepared):
            entry['original']=self.store.put_original(sid,raw,record['file'])
        manifest=dict(version=VERSION,batch_scope=scope,principal_id=actor.principal.principal_id,
            request_sha256=request_sha256,base=base,files=entries,generation_authorized=False)
        _require(len(canonical(manifest).encode())<=65536,'ATTACHMENT_BATCH_TOO_LARGE')
        batch_id=PREFIX+hashlib.sha256(canonical(manifest).encode()).hexdigest()
        for publish in (True,False):
            after,current,plans=self.intake._current(headers,**scope)
            _require(after.principal==actor.principal and current==access
                and any(r.sha256==request_sha256 for r in plans),'ATTACHMENT_AUTHORITY_CHANGED')
            if publish:self.store.record(batch_id,manifest)
        return batch_id

    def _manifest(self,headers,workspace_id,conversation_id,batch_id):
        actor,access=self.intake._actor(headers,workspace_id,conversation_id)
        _require(type(batch_id) is str and batch_id.startswith(PREFIX) and len(batch_id)==len(PREFIX)+64,
            'ATTACHMENT_BATCH_INVALID')
        rows=self._rows(f'SELECT record_json FROM `{TABLE}` WHERE submission_id=@id LIMIT 2',dict(id=batch_id))
        _require(len(rows)==1 and len(rows[0]['record_json'].encode())<=65536,'ATTACHMENT_BATCH_UNAVAILABLE')
        manifest=json.loads(rows[0]['record_json']);scope=manifest['batch_scope']
        _require(batch_id==PREFIX+hashlib.sha256(canonical(manifest).encode()).hexdigest()
            and manifest['version']==VERSION and manifest['generation_authorized'] is False
            and manifest['principal_id']==actor.principal.principal_id
            and scope['workspace_id']==workspace_id and scope['conversation_id']==conversation_id
            and type(manifest['files']) is list and 0<len(manifest['files'])<=100,'ATTACHMENT_BATCH_SCOPE')
        # Original job must exist; never manufacture an intent to recover one.
        self.store.client.get_job('evidence_submission_v1_'+batch_id,location=LOCATION,retry=None,timeout=20)
        self.store.record(batch_id,manifest) # Exact original-job readback only.
        after,current=self.intake._actor(headers,workspace_id,conversation_id)
        _require(after.principal==actor.principal and current==access,'ATTACHMENT_AUTHORITY_CHANGED')
        return manifest

    def list(self,headers,*,workspace_id,conversation_id,request_id):
        actor,access=self.intake._actor(headers,workspace_id,conversation_id)
        rows=self._rows(f'''SELECT submission_id,record_json FROM `{TABLE}`
WHERE JSON_VALUE(record_json,'$.version')=@version
AND JSON_VALUE(record_json,'$.batch_scope.workspace_id')=@workspace
AND JSON_VALUE(record_json,'$.batch_scope.conversation_id')=@conversation
AND JSON_VALUE(record_json,'$.batch_scope.request_id')=@request
AND JSON_VALUE(record_json,'$.principal_id')=@principal
ORDER BY recorded_at DESC LIMIT 20''',dict(version=VERSION,workspace=workspace_id,conversation=conversation_id,
            request=request_id,principal=actor.principal.principal_id))
        result=[]
        for row in rows:
            value=json.loads(row['record_json'])
            _require(row['submission_id']==PREFIX+hashlib.sha256(canonical(value).encode()).hexdigest(),'ATTACHMENT_BATCH_HASH')
            result.append(dict(batch_id=row['submission_id'],count=len(value['files'])))
        after,current=self.intake._actor(headers,workspace_id,conversation_id)
        _require(after.principal==actor.principal and current==access,'ATTACHMENT_AUTHORITY_CHANGED')
        return tuple(result)

    def state(self,headers,*,workspace_id,conversation_id,batch_id):
        m=self._manifest(headers,workspace_id,conversation_id,batch_id)
        # One native read for all completed per-file records. No filename-only matching.
        ids=[e['submission_id'] for e in m['files']]
        rows=self._rows(f'''SELECT submission_id,record_json FROM `{TABLE}`
WHERE submission_id IN (SELECT JSON_VALUE(x) FROM UNNEST(JSON_QUERY_ARRAY(@ids)) x) LIMIT 101''',dict(ids=canonical(ids)))
        _require(len({r['submission_id'] for r in rows})==len(rows),'ATTACHMENT_BATCH_RECORD_DUPLICATE')
        existing={r['submission_id']:r['record_json'] for r in rows};out=[]
        for entry in m['files']:
            sid=entry['submission_id'];base=dict(m['base'],file=entry['file'])
            _require(hashlib.sha256(canonical(base).encode()).hexdigest()==sid,'ATTACHMENT_BATCH_FILE_HASH')
            expected=dict(base,original=entry['original']);saved=sid in existing
            if saved:
                _require(existing[sid]==canonical(expected),'ATTACHMENT_BATCH_FILE_CONFLICT')
                self.store.client.get_job('evidence_submission_v1_'+sid,location=LOCATION,retry=None,timeout=20)
                self.store.record(sid,expected) # Checks exact original SQL/config/outcome.
            out.append(dict(submission_id=sid,filename=entry['file']['filename'],saved=saved))
        # State is descriptive only, never source/claim approval.
        self.intake._actor(headers,workspace_id,conversation_id)
        return tuple(out)

    def advance(self,headers,*,workspace_id,conversation_id,batch_id):
        m=self._manifest(headers,workspace_id,conversation_id,batch_id)
        state=self.state(headers,workspace_id=workspace_id,conversation_id=conversation_id,batch_id=batch_id)
        pending=next((i for i,r in enumerate(state) if not r['saved']),None)
        if pending is None:return state
        e=m['files'][pending];o=e['original'];f=e['file'];sid=e['submission_id']
        _require(o['bucket']==self.store.bucket and o['object_name']=='campaign-evidence/'+sid,
            'ATTACHMENT_BATCH_ORIGINAL_SCOPE')
        blob=self.store.storage.bucket(o['bucket']).blob(o['object_name'],generation=int(o['generation']))
        raw=blob.download_as_bytes(if_generation_match=int(o['generation']),retry=None,timeout=30)
        _require(inspect_file(f['filename'],raw,m['base']['selection']['category'])==f,'ATTACHMENT_BATCH_ORIGINAL_CHANGED')
        receipt=self.intake.submit(headers,**m['batch_scope'],requirement_id=m['base']['requirement_id'],
            request_sha256=m['request_sha256'],choice='PROVIDE_EVIDENCE',category=m['base']['selection']['category'],
            filename=f['filename'],content=raw)
        _require(receipt.submission_id==sid,'ATTACHMENT_BATCH_RECEIPT_CHANGED')
        return self.state(headers,workspace_id=workspace_id,conversation_id=conversation_id,batch_id=batch_id)
