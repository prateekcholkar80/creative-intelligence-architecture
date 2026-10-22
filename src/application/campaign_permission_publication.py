"""Pure P6 fenced publication plans. No query submission or serving activation.

No bootstrap: a complete preexisting control is required. Future grant/review and
permission writers must share this singleton fence. Historical receipt recovery,
IAP/grant provenance verification and uncertain-job reconciliation are pending.
"""
from dataclasses import dataclass
from datetime import timedelta
import json

from google.cloud import bigquery
from src.application.campaign_permission_lifecycle import Action,propose_permission
from src.application.campaign_permission_storage import (
    TABLE_SCHEMAS,PREFIX,decode_state,encode_record,head_row,
)
from src.application.campaign_review_storage import (
    SCHEMAS,STORAGE_VERSION,PublicationControl,decode_control_state,decode_review_state,
    row_json,parse_row,
)
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.workflow_contract import _require,_id,canonical_json_bytes
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,settings

MAX_CAPTURE_BYTES=65536
PLAN_TTL=timedelta(seconds=120)
PUBLICATION_BYTES=768*1024*1024
PUBLICATION_TIMEOUT_MS=120000


@dataclass(frozen=True,slots=True)
class Rows:
    kind: str
    record_id: str
    head_json: str | None
    history_json: tuple[str,...]

    def __post_init__(self):
        _id(self.record_id)
        _require(type(self.kind) is str and self.kind in {'CLAIM','OFFER','EVIDENCE','GRANT','REVIEW','CONTROL'}
            and (self.head_json is None or type(self.head_json) is str)
            and type(self.history_json) is tuple and len(self.history_json)<=64
            and all(type(v) is str for v in self.history_json),'INVALID_PERMISSION_PUBLICATION_ROWS')
        _require((self.head_json is None)==(not self.history_json),'PERMISSION_PUBLICATION_PARTIAL_STATE')
        _require(sum(len(v.encode()) for v in self.history_json)+(len(self.head_json.encode()) if self.head_json else 0)
                 <=MAX_CAPTURE_BYTES,'PERMISSION_PUBLICATION_CAPTURE_TOO_LARGE')

    def decoded(self):
        return (parse_row(self.head_json) if self.head_json else None,
                tuple(parse_row(v) for v in self.history_json))


@dataclass(frozen=True,slots=True)
class PublicationPlan:
    native_job_id: str
    command_sha256: str
    proposed_record_sha256: str
    candidate_control_sha256: str
    audit_json: str
    sql: str
    config_json: str
    execution_enabled: bool=False

    def config(self):return bigquery.QueryJobConfig.from_api_repr(json.loads(self.config_json))


def _expr(schema,source):
    terms=[]
    for name,kind,_ in schema:
        value=f"JSON_VALUE({source}, '$.{name}')"
        if kind!='STRING':value=f"CAST({value} AS {'INT64' if kind=='INTEGER' else kind})"
        terms.append(f'{value} AS {name}')
    return ','.join(terms)


def build_publication(action,*,identity,control,grant,permission,candidate=None,review=None,
                      evidence=(),checked_at,reason_code):
    now=_utc(checked_at)
    _require(isinstance(control,Rows) and control.kind=='CONTROL' and control.record_id=='campaign-permissions'
        and isinstance(grant,Rows) and grant.kind=='GRANT' and isinstance(permission,Rows)
        and permission.kind in PREFIX,'INVALID_PERMISSION_PUBLICATION_CAPTURE')
    control_head,control_history=control.decoded()
    before_control=decode_control_state(control_head,control_history,now)
    _require(before_control.revision<64,'PERMISSION_PUBLICATION_CONTROL_LIMIT')
    grant_head,grant_history=grant.decoded()
    grant_record=decode_review_state(grant_head,grant_history,kind='GRANT',record_id=grant.record_id,checked_at=now)
    head,history=permission.decoded()
    current=decode_state(head,history,kind=permission.kind,record_id=permission.record_id) if head else None
    review_record=None;review_head=None;review_history=()
    if review is not None:
        _require(isinstance(review,Rows) and review.kind=='REVIEW','INVALID_PERMISSION_PUBLICATION_REVIEW')
        review_head,review_history=review.decoded()
        review_record=decode_review_state(review_head,review_history,kind='REVIEW',record_id=review.record_id,checked_at=now)
    proposal=propose_permission(action,identity=identity,grant=grant_record.value,candidate=candidate,
        current=current,review=review_record.value if review_record else None,checked_at=now,reason_code=reason_code)
    _require(proposal.kind==permission.kind and proposal.record_id==permission.record_id,'PERMISSION_PUBLICATION_SCOPE_MISMATCH')
    _require(type(evidence) is tuple and len(evidence)<=32
        and all(isinstance(v,Rows) and v.kind=='EVIDENCE' for v in evidence), 'INVALID_PERMISSION_PUBLICATION_EVIDENCE')
    required=proposal.proposed_record.evidence if action is Action.APPROVE and permission.kind=='CLAIM' else ()
    _require(tuple(v.record_id for v in evidence)==tuple(v.evidence_id for v in required),
             'PERMISSION_PUBLICATION_EVIDENCE_COVERAGE')
    evidence_states=[]
    for source,pin in zip(evidence,required):
        ehead,ehistory=source.decoded()
        state=decode_state(ehead,ehistory,kind='EVIDENCE',record_id=source.record_id)
        _require(state.require_current(now).sha256==pin.sha256,'PERMISSION_PUBLICATION_EVIDENCE_CHANGED')
        evidence_states.append(state)
    captures=(control,grant,permission)+((review,) if review else ())+evidence
    _require(sum(sum(len(v.encode()) for v in c.history_json)+(len(c.head_json.encode()) if c.head_json else 0)
                 for c in captures)<=MAX_CAPTURE_BYTES,'PERMISSION_PUBLICATION_CAPTURE_TOO_LARGE')
    command=_hash(dict(proposal_sha256=proposal.sha256,
        publication_profile=dict(version='campaign-publication-query-v2',
            ttl_seconds=int(PLAN_TTL.total_seconds()),maximum_bytes_billed=PUBLICATION_BYTES,
            job_timeout_ms=PUBLICATION_TIMEOUT_MS),
        identity_window=dict(audience=identity.audience,issued_at=identity.issued_at,expires_at=identity.expires_at),
        captures=[dict(kind=c.kind,record_id=c.record_id,
        head=c.head_json,history=list(c.history_json)) for c in captures]))
    next_control=PublicationControl(before_control.revision+1,before_control.sha256,command,proposal.changed_by,now)
    new_history=encode_record(proposal.proposed_record,recorded_at=now);new_head=head_row(new_history)
    audit=dict(schema_version=STORAGE_VERSION,command_sha256=command,action=action.value,
        permission_kind=permission.kind,record_id=permission.record_id,previous_revision=proposal.expected_revision,
        revision=proposal.expected_revision+1,previous_record_sha256=proposal.expected_record_sha256,
        record_sha256=proposal.proposed_record.sha256,control_revision=next_control.revision,
        control_sha256=next_control.sha256,grant_id=grant.record_id,grant_sha256=grant_record.sha256,
        review_id=review.record_id if review else None,review_sha256=review_record.sha256 if review_record else None,
        changed_by=proposal.changed_by,reason_code=reason_code,recorded_at=now,outcome='APPLIED')
    params=[];sql=[]
    def scalar(name,value,kind='STRING'):
        params.append(bigquery.ScalarQueryParameter(name,kind,value))
    scalar('record_id',permission.record_id);scalar('control_id','campaign-permissions')
    scalar('command_sha256',command);scalar('grant_id',grant.record_id)
    if review:scalar('review_id',review.record_id)
    scalar('checked_at',now,'TIMESTAMP');scalar('plan_expires_at',now+PLAN_TTL,'TIMESTAMP')
    scalar('identity_issued_at',identity.issued_at,'INT64');scalar('identity_expires_at',identity.expires_at,'INT64')
    scalar('grant_effective_at',grant_record.value.effective_at,'TIMESTAMP')
    scalar('grant_valid_until',grant_record.value.valid_until,'TIMESTAMP')
    if review_record:
        scalar('reviewed_at',review_record.value.reviewed_at,'TIMESTAMP')
        scalar('review_valid_until',review_record.value.valid_until,'TIMESTAMP')
        candidate_approval=proposal.proposed_record.review if permission.kind=='EVIDENCE' else proposal.proposed_record.approval
        scalar('candidate_valid_until',candidate_approval.valid_until,'TIMESTAMP')
    def temp(name,schema,rows):
        if not rows:
            fields=','.join(f'{n} {"INT64" if k=="INTEGER" else k}' for n,k,_ in schema)
            sql.append(f'CREATE TEMP TABLE {name} ({fields});')
            return
        params.append(bigquery.ArrayQueryParameter(name,'STRING',[row_json(v) for v in rows]))
        sql.append(f'CREATE TEMP TABLE {name} AS SELECT {_expr(schema,"item")} FROM UNNEST(@{name}) item;')
    schemas=SCHEMAS|TABLE_SCHEMAS
    phead=PREFIX[permission.kind]+'_heads';phist=PREFIX[permission.kind]+'_versions'
    temp('old_control',SCHEMAS['campaign_permission_controls'],[control_head])
    temp('old_control_history',SCHEMAS['campaign_permission_control_history'],control_history)
    temp('new_control',SCHEMAS['campaign_permission_controls'],[next_control.row()])
    temp('old_head',TABLE_SCHEMAS[phead],[head] if head else [])
    temp('old_history',TABLE_SCHEMAS[phist],history)
    temp('new_head',TABLE_SCHEMAS[phead],[new_head]);temp('new_history',TABLE_SCHEMAS[phist],[new_history])
    temp('expected_audit',SCHEMAS['campaign_permission_publication_audits'],[audit])
    temp('grant_head',SCHEMAS['campaign_reviewer_grants_heads'],[grant_head])
    temp('grant_history',SCHEMAS['campaign_reviewer_grants_versions'],grant_history)
    if review_record:
        temp('review_head',SCHEMAS['campaign_review_captures_heads'],[review_head])
        temp('review_history',SCHEMAS['campaign_review_captures_versions'],review_history)
    for i,(source,state) in enumerate(zip(evidence,evidence_states)):
        ehead,ehistory=source.decoded()
        scalar(f'evidence_id_{i}',source.record_id)
        scalar(f'evidence_effective_{i}',state.current.review.effective_at,'TIMESTAMP')
        scalar(f'evidence_expiry_{i}',state.current.review.valid_until,'TIMESTAMP')
        temp(f'evidence_head_{i}',TABLE_SCHEMAS[PREFIX['EVIDENCE']+'_heads'],[ehead])
        temp(f'evidence_history_{i}',TABLE_SCHEMAS[PREFIX['EVIDENCE']+'_versions'],ehistory)
    def table(name):return f'`{PROJECT}.{DATASET}.{name}`'
    def exact(name,expected,scope,label):
        return (f"ASSERT (SELECT COUNT(*) FROM {table(name)} WHERE {scope})=(SELECT COUNT(*) FROM {expected})\n"
                f"AND NOT EXISTS(SELECT * FROM {table(name)} WHERE {scope} EXCEPT DISTINCT SELECT * FROM {expected})\n"
                f"AND NOT EXISTS(SELECT * FROM {expected} EXCEPT DISTINCT SELECT * FROM {table(name)} WHERE {scope}) AS '{label}_EXACT';")
    sql+=['CREATE TEMP TABLE publication_result(state STRING,command_sha256 STRING,record_sha256 STRING,control_sha256 STRING);',
          'BEGIN\nBEGIN TRANSACTION;',
          "ASSERT @checked_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@plan_expires_at AS 'PUBLICATION_EXPIRED';",
          "ASSERT TIMESTAMP_SECONDS(@identity_issued_at)<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<TIMESTAMP_SECONDS(@identity_expires_at) AS 'PUBLICATION_IDENTITY_EXPIRED';",
          "ASSERT @grant_effective_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@grant_valid_until AS 'PUBLICATION_GRANT_EXPIRED';",
          exact('campaign_reviewer_grants_heads','grant_head','record_id=@grant_id','PUBLICATION_GRANT_HEAD'),
          exact('campaign_reviewer_grants_versions','grant_history','record_id=@grant_id','PUBLICATION_GRANT_HISTORY')]
    if review_record:
        sql += ["ASSERT @reviewed_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@review_valid_until AS 'PUBLICATION_REVIEW_EXPIRED';",
            "ASSERT CURRENT_TIMESTAMP()<@candidate_valid_until AS 'PUBLICATION_CANDIDATE_EXPIRED';",
            exact('campaign_review_captures_heads','review_head','record_id=@review_id','PUBLICATION_REVIEW_HEAD'),
            exact('campaign_review_captures_versions','review_history','record_id=@review_id','PUBLICATION_REVIEW_HISTORY')]
    for i in range(len(evidence)):
        sql += [f"ASSERT @evidence_effective_{i}<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@evidence_expiry_{i} AS 'PUBLICATION_EVIDENCE_EXPIRED';",
            exact(PREFIX['EVIDENCE']+'_heads',f'evidence_head_{i}',f'record_id=@evidence_id_{i}','PUBLICATION_EVIDENCE_HEAD'),
            exact(PREFIX['EVIDENCE']+'_versions',f'evidence_history_{i}',f'record_id=@evidence_id_{i}','PUBLICATION_EVIDENCE_HISTORY')]
    audit_table='campaign_permission_publication_audits'
    sql += [f"ASSERT (SELECT COUNT(*) FROM {table(audit_table)} WHERE command_sha256=@command_sha256)<=1 AS 'PUBLICATION_AUDIT_DUPLICATE';",
            f'IF (SELECT COUNT(*) FROM {table(audit_table)} WHERE command_sha256=@command_sha256)=1 THEN',
            exact(audit_table,'expected_audit','command_sha256=@command_sha256','PUBLICATION_REPLAY_AUDIT'),
            exact('campaign_permission_controls','new_control','control_id=@control_id','PUBLICATION_REPLAY_CONTROL'),
            exact('campaign_permission_control_history','(SELECT * FROM old_control_history UNION ALL SELECT * FROM new_control)',
                  'control_id=@control_id','PUBLICATION_REPLAY_CONTROL_HISTORY'),
            exact(phead,'new_head','record_id=@record_id','PUBLICATION_REPLAY_HEAD'),
            exact(phist,'(SELECT * FROM old_history UNION ALL SELECT * FROM new_history)','record_id=@record_id','PUBLICATION_REPLAY_HISTORY'),
            "INSERT publication_result SELECT 'REUSED',@command_sha256,record_sha256,(SELECT control_sha256 FROM new_control) FROM new_head;",
            'ELSE',
            exact('campaign_permission_controls','old_control','control_id=@control_id','PUBLICATION_CONTROL'),
            exact('campaign_permission_control_history','old_control_history','control_id=@control_id','PUBLICATION_CONTROL_HISTORY'),
            exact(phead,'old_head','record_id=@record_id','PUBLICATION_HEAD'),
            exact(phist,'old_history','record_id=@record_id','PUBLICATION_HISTORY')]
    def insert(name,expected):
        cols=','.join(n for n,_,_ in schemas[name])
        return f'INSERT {table(name)} ({cols}) SELECT {cols} FROM {expected};'
    def update(name,expected,scope):
        cols=[n for n,_,_ in schemas[name]]
        setters=','.join(f'{n}=c.{n}' for n in cols)
        return f'UPDATE {table(name)} t SET {setters} FROM {expected} c WHERE {scope};\nASSERT @@row_count=1 AS \'PUBLICATION_CAS_CONFLICT\';'
    sql += [update('campaign_permission_controls','new_control',
        't.control_id=@control_id AND t.revision=(SELECT revision FROM old_control) AND t.control_sha256=(SELECT control_sha256 FROM old_control)'),
        insert('campaign_permission_control_history','new_control'),insert(phist,'new_history')]
    if head is None:sql.append(insert(phead,'new_head'))
    else:sql.append(update(phead,'new_head','t.record_id=@record_id AND t.revision=(SELECT revision FROM old_head) AND t.record_sha256=(SELECT record_sha256 FROM old_head)'))
    sql += [insert(audit_table,'expected_audit'),
        "INSERT publication_result SELECT 'APPLIED',@command_sha256,record_sha256,(SELECT control_sha256 FROM new_control) FROM new_head;",
        'END IF;\nCOMMIT TRANSACTION;\nEXCEPTION WHEN ERROR THEN\nROLLBACK TRANSACTION;\nRAISE;\nEND;',
        'SELECT * FROM publication_result;']
    cfg=settings(params)
    cfg.maximum_bytes_billed=PUBLICATION_BYTES
    cfg.job_timeout_ms=PUBLICATION_TIMEOUT_MS
    script='\n'.join(sql)
    _require(script.count(';')<=64 and len(script.encode())<=128*1024,
             'PERMISSION_PUBLICATION_SCRIPT_LIMIT')
    cfg_json=canonical_json_bytes(cfg.to_api_repr()).decode()
    return PublicationPlan('campaign_permission_publish_'+command,command,proposal.proposed_record.sha256,
        next_control.sha256,row_json(audit),script,cfg_json)


def validate_result(plan,rows):
    _require(isinstance(plan,PublicationPlan) and type(rows) is tuple and len(rows)==1,
             'INVALID_PERMISSION_PUBLICATION_RESULT')
    row=dict(rows[0])
    _require(set(row)=={'state','command_sha256','record_sha256','control_sha256'}
        and row['state'] in {'APPLIED','REUSED'} and row['command_sha256']==plan.command_sha256
        and row['record_sha256']==plan.proposed_record_sha256
        and row['control_sha256']==plan.candidate_control_sha256,'PERMISSION_PUBLICATION_RESULT_MISMATCH')
    return row['state']
