"""Disabled fenced operator-policy publication; no inferred publisher or approval.

The explicit publisher manifest is trusted server configuration, not a browser
payload or an application ADMIN grant. Authoritative capture/execution must verify
it independently. This policy authorizes review management, never campaign release.
"""
from dataclasses import dataclass
from datetime import datetime,timedelta
import json,re
from google.cloud import bigquery
from src.application.identity import IapSettings
from src.application.campaign_publication_reader import _identity
from src.application.campaign_acceptance_contract import _utc,_hash
from src.application.campaign_operator_policy_storage import (
    POLICY_ID,HEAD_TABLE,HISTORY_TABLE,SCHEMAS as POLICY_SCHEMAS,
    OperatorPolicyVersion,decode_state,encode_record,head_row,
)
from src.application.campaign_permission_publication import (
    Rows,PublicationPlan,_expr,MAX_CAPTURE_BYTES,PLAN_TTL,PUBLICATION_BYTES,PUBLICATION_TIMEOUT_MS,
)
from src.application.campaign_review_storage import (
    SCHEMAS,STORAGE_VERSION,PublicationControl,decode_control_state,row_json,parse_row,
)
from src.application.workflow_contract import _require,_id
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,settings


@dataclass(frozen=True,slots=True)
class OperatorPublisherManifest:
    manifest_id:str
    expected_audience:str
    principal_ids:tuple[str,...]
    effective_at:datetime
    valid_until:datetime

    def __post_init__(self):
        _id(self.manifest_id);IapSettings(self.expected_audience)
        _require(type(self.principal_ids) is tuple and 0<len(self.principal_ids)<=16
            and all(type(p) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}',p) for p in self.principal_ids)
            and self.principal_ids==tuple(sorted(set(self.principal_ids))),
            'OPERATOR_PUBLISHER_PRINCIPALS')
        object.__setattr__(self,'effective_at',_utc(self.effective_at))
        object.__setattr__(self,'valid_until',_utc(self.valid_until))
        _require(timedelta(0)<self.valid_until-self.effective_at<=timedelta(seconds=300),
            'OPERATOR_PUBLISHER_WINDOW')

    @property
    def sha256(self):return _hash(dict(version='operator-publisher-manifest-v1',manifest_id=self.manifest_id,
        audience=self.expected_audience,principals=list(self.principal_ids),
        effective_at=self.effective_at.isoformat(),valid_until=self.valid_until.isoformat()))

    def require_actor(self,identity,now):
        now=_utc(now);_identity(identity,IapSettings(self.expected_audience),now)
        _require(identity.principal.principal_id in self.principal_ids
            and self.effective_at<=now<self.valid_until,'OPERATOR_PUBLISHER_DENIED')


def build_operator_publication(*,identity,manifest,control,head_json,history_json,candidate,checked_at):
    """Pure exact-source plan; cannot bootstrap the permission-control singleton."""
    now=_utc(checked_at)
    _require(type(manifest) is OperatorPublisherManifest,'OPERATOR_PUBLISHER_MANIFEST_REQUIRED')
    manifest.require_actor(identity,now)
    _require(type(control) is Rows and control.kind=='CONTROL' and control.record_id=='campaign-permissions',
        'OPERATOR_PUBLICATION_CONTROL_REQUIRED')
    old_control,control_history=control.decoded()
    fence=decode_control_state(old_control,control_history,now)
    _require(fence.revision<64,'OPERATOR_PUBLICATION_CONTROL_LIMIT')
    _require((head_json is None or type(head_json) is str) and type(history_json) is tuple
        and all(type(v) is str for v in history_json) and (head_json is None)==(not history_json),
        'OPERATOR_PUBLICATION_PARTIAL_STATE')
    _require(len(history_json)<64,'OPERATOR_PUBLICATION_HISTORY_LIMIT')
    _require(sum(len(v.encode()) for v in history_json+control.history_json)
        +len((head_json or '').encode())+len(control.head_json.encode())<=MAX_CAPTURE_BYTES,
        'OPERATOR_PUBLICATION_CAPTURE_LIMIT')
    previous=tuple(parse_row(v) for v in history_json)
    old_head=parse_row(head_json) if head_json else None
    current=decode_state(old_head,previous,checked_at=now).current if old_head else None
    _require(type(candidate) is OperatorPolicyVersion and candidate.changed_by==identity.principal.principal_id
        and candidate.recorded_at==now and candidate.policy.expected_audience==manifest.expected_audience,
        'OPERATOR_PUBLICATION_CANDIDATE')
    _require(candidate.policy.revision==(current.policy.revision+1 if current else 1)
        and candidate.parent_sha256==(current.sha256 if current else None),'OPERATOR_PUBLICATION_PARENT')
    if candidate.status=='ACTIVE':
        _require(candidate.policy.effective_at<=now<candidate.policy.valid_until,'OPERATOR_PUBLICATION_POLICY_EXPIRED')
    # Full decoder enforces terminal revocation, immutable revocation content,
    # exact genesis/revision chain, audience continuity and future-record denial.
    new=encode_record(candidate);new_head=head_row(new)
    decode_state(new_head,previous+(new,),checked_at=now)
    command=_hash(dict(version='operator-policy-publication-v1',manifest_sha256=manifest.sha256,
        candidate_sha256=candidate.sha256,control_head=control.head_json,control_history=list(control.history_json),
        old_head=head_json,old_history=list(history_json),
        identity=dict(principal=identity.principal.principal_id,audience=identity.audience,
            issued_at=identity.issued_at,expires_at=identity.expires_at),
        profile=dict(bytes=PUBLICATION_BYTES,timeout=PUBLICATION_TIMEOUT_MS,ttl=int(PLAN_TTL.total_seconds()))))
    next_control=PublicationControl(fence.revision+1,fence.sha256,command,identity.principal.principal_id,now)
    # A separate audit action/kind; manifest references are not reviewer grants
    # and must never be decoded by the CLAIM/OFFER/EVIDENCE receipt namespace.
    audit=dict(schema_version=STORAGE_VERSION,command_sha256=command,
        action='OPERATOR_POLICY_'+('REVOKE' if candidate.status=='REVOKED' else 'PUBLISH'),
        permission_kind='OPERATOR_POLICY',record_id=POLICY_ID,
        previous_revision=current.policy.revision if current else 0,revision=candidate.policy.revision,
        previous_record_sha256=current.sha256 if current else None,record_sha256=candidate.sha256,
        control_revision=next_control.revision,control_sha256=next_control.sha256,
        grant_id=manifest.manifest_id,grant_sha256=manifest.sha256,review_id=None,review_sha256=None,
        changed_by=identity.principal.principal_id,reason_code=candidate.reason_code,recorded_at=now,outcome='APPLIED')
    schemas=dict(SCHEMAS)|dict(POLICY_SCHEMAS);params=[];sql=[]
    def scalar(name,value,kind='STRING'):params.append(bigquery.ScalarQueryParameter(name,kind,value))
    for name,value in (('policy_id',POLICY_ID),('control_id','campaign-permissions'),('command_sha256',command)):
        scalar(name,value)
    until=min(now+PLAN_TTL,manifest.valid_until,datetime.fromtimestamp(identity.expires_at,tz=now.tzinfo))
    if candidate.status=='ACTIVE':until=min(until,candidate.policy.valid_until)
    scalar('checked_at',now,'TIMESTAMP');scalar('expires_at',until,'TIMESTAMP')
    def temp(name,schema,rows):
        if not rows:
            sql.append(f'CREATE TEMP TABLE {name} ('+','.join(f'{n} {"INT64" if k=="INTEGER" else k}' for n,k,_ in schema)+');')
        else:
            params.append(bigquery.ArrayQueryParameter(name,'STRING',[row_json(r) for r in rows]))
            sql.append(f"CREATE TEMP TABLE {name} AS SELECT {_expr(schema,'item')} FROM UNNEST(@{name}) item;")
    for name,table,values in (
        ('old_control','campaign_permission_controls',[old_control]),
        ('old_controls','campaign_permission_control_history',control_history),
        ('new_control','campaign_permission_controls',[next_control.row()]),
        ('old_head',HEAD_TABLE,[old_head] if old_head else []),('old_history',HISTORY_TABLE,previous),
        ('new_head',HEAD_TABLE,[new_head]),('new_history',HISTORY_TABLE,[new]),
        ('expected_audit','campaign_permission_publication_audits',[audit])):
        temp(name,schemas[table],values)
    def table(name):return f'`{PROJECT}.{DATASET}.{name}`'
    def exact(name,expected,scope,label):
        return (f'ASSERT (SELECT COUNT(*) FROM {table(name)} WHERE {scope})=(SELECT COUNT(*) FROM {expected})'
            f' AND NOT EXISTS(SELECT * FROM {table(name)} WHERE {scope} EXCEPT DISTINCT SELECT * FROM {expected})'
            f' AND NOT EXISTS(SELECT * FROM {expected} EXCEPT DISTINCT SELECT * FROM {table(name)} WHERE {scope})'
            f" AS 'OPERATOR_PUBLICATION_{label}';")
    def insert(name,source):
        cols=','.join(n for n,_,_ in schemas[name]);return f'INSERT {table(name)} ({cols}) SELECT {cols} FROM {source};'
    def update(name,source,where):
        fields=','.join(f'{n}=c.{n}' for n,_,_ in schemas[name])
        return f"UPDATE {table(name)} t SET {fields} FROM {source} c WHERE {where};\nASSERT @@row_count=1 AS 'OPERATOR_PUBLICATION_CAS';"
    audits='campaign_permission_publication_audits'
    sql+=['CREATE TEMP TABLE operator_result(state STRING,command_sha256 STRING,record_sha256 STRING,control_sha256 STRING);',
        'BEGIN\nBEGIN TRANSACTION;',"ASSERT @checked_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@expires_at AS 'OPERATOR_PUBLICATION_EXPIRED';",
        f"ASSERT (SELECT COUNT(*) FROM {table(audits)} WHERE command_sha256=@command_sha256)<=1 AS 'OPERATOR_PUBLICATION_DUPLICATE_AUDIT';",
        f'IF EXISTS(SELECT 1 FROM {table(audits)} WHERE command_sha256=@command_sha256) THEN',
        exact(audits,'expected_audit','command_sha256=@command_sha256','REPLAY_AUDIT'),
        exact(HEAD_TABLE,'new_head','policy_id=@policy_id','REPLAY_HEAD'),
        exact(HISTORY_TABLE,'(SELECT * FROM old_history UNION ALL SELECT * FROM new_history)','policy_id=@policy_id','REPLAY_HISTORY'),
        exact('campaign_permission_controls','new_control','control_id=@control_id','REPLAY_CONTROL'),
        exact('campaign_permission_control_history','(SELECT * FROM old_controls UNION ALL SELECT * FROM new_control)','control_id=@control_id','REPLAY_CONTROLS'),
        "INSERT operator_result SELECT 'REUSED',@command_sha256,record_sha256,(SELECT control_sha256 FROM new_control) FROM new_head;",
        'ELSE',exact(HEAD_TABLE,'old_head','policy_id=@policy_id','HEAD'),
        exact(HISTORY_TABLE,'old_history','policy_id=@policy_id','HISTORY'),
        exact('campaign_permission_controls','old_control','control_id=@control_id','CONTROL'),
        exact('campaign_permission_control_history','old_controls','control_id=@control_id','CONTROLS'),
        update('campaign_permission_controls','new_control','t.control_id=@control_id AND t.revision=(SELECT revision FROM old_control) AND t.control_sha256=(SELECT control_sha256 FROM old_control)'),
        insert('campaign_permission_control_history','new_control'),insert(HISTORY_TABLE,'new_history')]
    sql.append(insert(HEAD_TABLE,'new_head') if old_head is None else update(HEAD_TABLE,'new_head',
        't.policy_id=@policy_id AND t.revision=(SELECT revision FROM old_head) AND t.record_sha256=(SELECT record_sha256 FROM old_head)'))
    sql += [insert(audits,'expected_audit'),"INSERT operator_result SELECT 'APPLIED',@command_sha256,record_sha256,(SELECT control_sha256 FROM new_control) FROM new_head;",
        'END IF;\nCOMMIT TRANSACTION;\nEXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE;\nEND;',
        'SELECT * FROM operator_result;']
    script='\n'.join(sql)
    _require(script.count(';')<=64 and len(script.encode())<=128*1024,'OPERATOR_PUBLICATION_SQL_LIMIT')
    cfg=settings(params);cfg.maximum_bytes_billed=PUBLICATION_BYTES;cfg.job_timeout_ms=PUBLICATION_TIMEOUT_MS
    config_json=json.dumps(cfg.to_api_repr(),sort_keys=True,separators=(',',':'))
    _require(len(config_json.encode())<=4*MAX_CAPTURE_BYTES,'OPERATOR_PUBLICATION_CONFIG_LIMIT')
    # One global native slot per policy revision, independent of publisher,
    # timestamps and content. Cold/different intents cannot remint UNKNOWN.
    # A failed slot has no automatic retry/advance in this API.
    slot=_hash(['operator-policy-publication-slot-v1',POLICY_ID,candidate.policy.revision])
    return PublicationPlan('campaign_operator_publish_'+slot,command,candidate.sha256,next_control.sha256,
        row_json(audit),script,config_json)
