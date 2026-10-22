"""Pure disabled fenced GRANT/REVIEW transaction plans, sharing permission fence.

Exact operator-policy head/history captures are checked inside the transaction.
Trusted policy publication/revocation and management execution remain pending.
Existing audit schema is reused under a distinct REVIEW_CONTROL_* action domain.
"""
from google.cloud import bigquery
from src.application.campaign_review_publication_contract import propose_review_publication,ReviewAction
from src.application.campaign_permission_publication import (
    Rows,PublicationPlan,_expr,MAX_CAPTURE_BYTES,PLAN_TTL,PUBLICATION_BYTES,PUBLICATION_TIMEOUT_MS,
)
from src.application.campaign_review_storage import SCHEMAS,STORAGE_VERSION,PREFIX,encode_review,review_head,row_json,decode_control_state
from src.application.campaign_permission_storage import TABLE_SCHEMAS,PREFIX as PERMISSION_PREFIX
from src.application.campaign_operator_policy_storage import (
    POLICY_ID,HEAD_TABLE,HISTORY_TABLE,SCHEMAS as POLICY_SCHEMAS,decode_state as decode_policy,
)
from src.application.campaign_operator_policy_reader import OperatorPolicySnapshot
from src.application.campaign_review_storage import parse_row
from src.application.campaign_acceptance_contract import _utc,_hash
from src.application.workflow_contract import _require,_id,canonical_json_bytes
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,settings


def build_review_publication(action,*,identity,policy,policy_snapshot,control,target,value=None,grant=None,permission=None,checked_at,reason_code):
    now=_utc(checked_at)
    _require(type(policy_snapshot) is OperatorPolicySnapshot and policy_snapshot.checked_at==now
        and type(policy_snapshot.head_json) is str and type(policy_snapshot.history_json) is tuple
        and 1<=len(policy_snapshot.history_json)<=64
        and all(type(row) is str for row in policy_snapshot.history_json), 'REVIEW_PUBLICATION_POLICY_CAPTURE_REQUIRED')
    _id(policy_snapshot.native_job_id)
    _require(len(policy_snapshot.head_json.encode())+sum(len(row.encode()) for row in policy_snapshot.history_json)
        <=MAX_CAPTURE_BYTES,'PERMISSION_PUBLICATION_CAPTURE_TOO_LARGE')
    policy_head=parse_row(policy_snapshot.head_json);policy_history=tuple(parse_row(row) for row in policy_snapshot.history_json)
    policy_state=decode_policy(policy_head,policy_history,checked_at=now)
    _require(policy_state==policy_snapshot.state and policy_state.require_current(now)==policy,
        'REVIEW_PUBLICATION_POLICY_CAPTURE_CONFLICT')
    _require(isinstance(control,Rows) and control.kind=='CONTROL' and control.record_id=='campaign-permissions',
        'INVALID_REVIEW_PUBLICATION_CONTROL')
    old_control,control_history=control.decoded();before=decode_control_state(old_control,control_history,now)
    _require(before.revision<64,'PERMISSION_PUBLICATION_CONTROL_LIMIT')
    proposal=propose_review_publication(action,identity=identity,policy=policy,target=target,value=value,
        grant=grant,permission=permission,checked_at=now,reason_code=reason_code)
    captures=(control,target)+((grant,) if grant else ())+((permission,) if permission else ())
    _require(sum(sum(len(v.encode()) for v in source.history_json)+(len(source.head_json.encode()) if source.head_json else 0)
        for source in captures)+len(policy_snapshot.head_json.encode())+sum(len(row.encode()) for row in policy_snapshot.history_json)
        <=MAX_CAPTURE_BYTES,'PERMISSION_PUBLICATION_CAPTURE_TOO_LARGE')
    command=_hash(dict(version='campaign-review-publication-query-v2',proposal_sha256=proposal.sha256,
        policy_sha256=policy.sha256,identity_window=dict(audience=identity.audience,issued_at=identity.issued_at,expires_at=identity.expires_at),
        operator_policy=dict(head=policy_snapshot.head_json,history=list(policy_snapshot.history_json)),
        profile=dict(ttl_seconds=int(PLAN_TTL.total_seconds()),maximum_bytes_billed=PUBLICATION_BYTES,job_timeout_ms=PUBLICATION_TIMEOUT_MS),
        captures=[dict(kind=s.kind,record_id=s.record_id,head=s.head_json,history=list(s.history_json)) for s in captures]))
    from src.application.campaign_review_storage import PublicationControl
    new_control=PublicationControl(before.revision+1,before.sha256,command,identity.principal.principal_id,now)
    new_row=encode_review(proposal.record);old_head,old_history=target.decoded()
    g=proposal.record if target.kind=='GRANT' else proposal.grant_record
    audit=dict(schema_version=STORAGE_VERSION,command_sha256=command,action='REVIEW_CONTROL_'+action.value,
        permission_kind=target.kind,record_id=target.record_id,previous_revision=proposal.previous.revision if proposal.previous else 0,
        revision=proposal.record.revision,previous_record_sha256=proposal.previous.sha256 if proposal.previous else None,
        record_sha256=proposal.record.sha256,control_revision=new_control.revision,control_sha256=new_control.sha256,
        grant_id=g.record_id,grant_sha256=g.sha256,review_id=target.record_id if target.kind=='REVIEW' else None,
        review_sha256=proposal.record.sha256 if target.kind=='REVIEW' else None,
        changed_by=identity.principal.principal_id,reason_code=reason_code,recorded_at=now,outcome='APPLIED')
    _require(sum(len(row_json(row).encode()) for row in (new_row,review_head(new_row),new_control.row(),audit))<=MAX_CAPTURE_BYTES,
        'REVIEW_PUBLICATION_RECORD_TOO_LARGE')
    params=[];sql=[];schemas=SCHEMAS|TABLE_SCHEMAS
    def scalar(name,value,kind='STRING'):params.append(bigquery.ScalarQueryParameter(name,kind,value))
    for name,value in (('record_id',target.record_id),('control_id','campaign-permissions'),('command_sha256',command)):
        scalar(name,value)
    for name,value in (('checked_at',now),('plan_expires_at',now+PLAN_TTL),('policy_effective_at',policy.effective_at),('policy_valid_until',policy.valid_until)):
        scalar(name,value,'TIMESTAMP')
    scalar('identity_issued_at',identity.issued_at,'INT64');scalar('identity_expires_at',identity.expires_at,'INT64')
    scalar('operator_policy_id',POLICY_ID)
    def temp(name,schema,rows):
        if not rows:
            sql.append(f"CREATE TEMP TABLE {name} ("+','.join(f'{n} {"INT64" if k=="INTEGER" else k}' for n,k,_ in schema)+');');return
        params.append(bigquery.ArrayQueryParameter(name,'STRING',[row_json(row) for row in rows]))
        sql.append(f"CREATE TEMP TABLE {name} AS SELECT {_expr(schema,'item')} FROM UNNEST(@{name}) item;")
    prefix=PREFIX[target.kind];head=prefix+'_heads';history=prefix+'_versions'
    temp('old_control',SCHEMAS['campaign_permission_controls'],[old_control])
    temp('old_control_history',SCHEMAS['campaign_permission_control_history'],control_history)
    temp('new_control',SCHEMAS['campaign_permission_controls'],[new_control.row()])
    temp('old_target_head',SCHEMAS[head],[old_head] if old_head else [])
    temp('old_target_history',SCHEMAS[history],old_history)
    temp('new_target_head',SCHEMAS[head],[review_head(new_row)]);temp('new_target_history',SCHEMAS[history],[new_row])
    temp('expected_audit',SCHEMAS['campaign_permission_publication_audits'],[audit])
    temp('operator_policy_head',POLICY_SCHEMAS[HEAD_TABLE],[policy_head])
    temp('operator_policy_history',POLICY_SCHEMAS[HISTORY_TABLE],policy_history)
    sources=[]
    if grant:
        scalar('grant_id',grant.record_id);gh,gv=grant.decoded()
        temp('grant_head',SCHEMAS['campaign_reviewer_grants_heads'],[gh]);temp('grant_history',SCHEMAS['campaign_reviewer_grants_versions'],gv)
        sources.append(('campaign_reviewer_grants','grant','record_id=@grant_id'))
    if permission:
        scalar('permission_id',permission.record_id);ph,pv=permission.decoded();p=PERMISSION_PREFIX[permission.kind]
        temp('permission_head',TABLE_SCHEMAS[p+'_heads'],[ph]);temp('permission_history',TABLE_SCHEMAS[p+'_versions'],pv)
        sources.append((p,'permission','record_id=@permission_id'))
    def table(name):return f'`{PROJECT}.{DATASET}.{name}`'
    def exact(name,expected,scope,label):
        return (f"ASSERT (SELECT COUNT(*) FROM {table(name)} WHERE {scope})=(SELECT COUNT(*) FROM {expected})\n"
            f"AND NOT EXISTS(SELECT * FROM {table(name)} WHERE {scope} EXCEPT DISTINCT SELECT * FROM {expected})\n"
            f"AND NOT EXISTS(SELECT * FROM {expected} EXCEPT DISTINCT SELECT * FROM {table(name)} WHERE {scope}) AS '{label}';")
    sql+=['CREATE TEMP TABLE review_result(state STRING,command_sha256 STRING,record_sha256 STRING,control_sha256 STRING);',
        'BEGIN\nBEGIN TRANSACTION;',
        "ASSERT @checked_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@plan_expires_at AS 'REVIEW_PUBLICATION_EXPIRED';",
        "ASSERT TIMESTAMP_SECONDS(@identity_issued_at)<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<TIMESTAMP_SECONDS(@identity_expires_at) AS 'REVIEW_PUBLICATION_IDENTITY_EXPIRED';",
        "ASSERT @policy_effective_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@policy_valid_until AS 'REVIEW_PUBLICATION_POLICY_EXPIRED';"]
    # Check before the replay branch too: an old receipt cannot bypass a changed,
    # revoked, deleted or partial current operator policy.
    sql += [exact(HEAD_TABLE,'operator_policy_head','policy_id=@operator_policy_id','REVIEW_PUBLICATION_POLICY_HEAD'),
        exact(HISTORY_TABLE,'operator_policy_history','policy_id=@operator_policy_id','REVIEW_PUBLICATION_POLICY_HISTORY')]
    if action in {ReviewAction.CREATE_GRANT,ReviewAction.REVISE_GRANT,ReviewAction.CREATE_REVIEW}:
        scalar('candidate_valid_until',proposal.record.value.valid_until,'TIMESTAMP')
        sql.append("ASSERT CURRENT_TIMESTAMP()<@candidate_valid_until AS 'REVIEW_PUBLICATION_CANDIDATE_EXPIRED';")
    if action is ReviewAction.CREATE_REVIEW:
        scalar('grant_effective_at',proposal.grant_record.value.effective_at,'TIMESTAMP')
        scalar('grant_valid_until',proposal.grant_record.value.valid_until,'TIMESTAMP')
        sql.append("ASSERT @grant_effective_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@grant_valid_until AS 'REVIEW_PUBLICATION_GRANT_EXPIRED';")
    for prefix,name,scope in sources:
        sql += [exact(prefix+'_heads',name+'_head',scope,'REVIEW_PUBLICATION_'+name.upper()+'_HEAD'),
            exact(prefix+'_versions',name+'_history',scope,'REVIEW_PUBLICATION_'+name.upper()+'_HISTORY')]
    audits='campaign_permission_publication_audits'
    sql += [f"ASSERT (SELECT COUNT(*) FROM {table(audits)} WHERE command_sha256=@command_sha256)<=1 AS 'REVIEW_PUBLICATION_DUPLICATE_AUDIT';",
        f'IF (SELECT COUNT(*) FROM {table(audits)} WHERE command_sha256=@command_sha256)=1 THEN',
        exact(audits,'expected_audit','command_sha256=@command_sha256','REVIEW_PUBLICATION_REPLAY_AUDIT'),
        exact('campaign_permission_controls','new_control','control_id=@control_id','REVIEW_PUBLICATION_REPLAY_CONTROL'),
        exact('campaign_permission_control_history','(SELECT * FROM old_control_history UNION ALL SELECT * FROM new_control)',
            'control_id=@control_id','REVIEW_PUBLICATION_REPLAY_CONTROL_HISTORY'),
        exact(head,'new_target_head','record_id=@record_id','REVIEW_PUBLICATION_REPLAY_HEAD'),
        exact(history,'(SELECT * FROM old_target_history UNION ALL SELECT * FROM new_target_history)',
            'record_id=@record_id','REVIEW_PUBLICATION_REPLAY_HISTORY'),
        "INSERT review_result SELECT 'REUSED',@command_sha256,record_sha256,(SELECT control_sha256 FROM new_control) FROM new_target_head;",
        'ELSE',exact('campaign_permission_controls','old_control','control_id=@control_id','REVIEW_PUBLICATION_CONTROL'),
        exact('campaign_permission_control_history','old_control_history','control_id=@control_id','REVIEW_PUBLICATION_CONTROL_HISTORY'),
        exact(head,'old_target_head','record_id=@record_id','REVIEW_PUBLICATION_HEAD'),
        exact(history,'old_target_history','record_id=@record_id','REVIEW_PUBLICATION_HISTORY')]
    def insert(name,expected):
        columns=','.join(n for n,_,_ in schemas[name]);return f'INSERT {table(name)} ({columns}) SELECT {columns} FROM {expected};'
    def update(name,expected,condition):
        setters=','.join(f'{n}=c.{n}' for n,_,_ in schemas[name])
        return f"UPDATE {table(name)} t SET {setters} FROM {expected} c WHERE {condition};\nASSERT @@row_count=1 AS 'REVIEW_PUBLICATION_CAS_CONFLICT';"
    sql += [update('campaign_permission_controls','new_control','t.control_id=@control_id AND t.revision=(SELECT revision FROM old_control) AND t.control_sha256=(SELECT control_sha256 FROM old_control)'),
        insert('campaign_permission_control_history','new_control'),insert(history,'new_target_history')]
    sql += [insert(head,'new_target_head') if old_head is None else update(head,'new_target_head','t.record_id=@record_id AND t.revision=(SELECT revision FROM old_target_head) AND t.record_sha256=(SELECT record_sha256 FROM old_target_head)')]
    sql += [insert(audits,'expected_audit'),"INSERT review_result SELECT 'APPLIED',@command_sha256,record_sha256,(SELECT control_sha256 FROM new_control) FROM new_target_head;",
        'END IF;\nCOMMIT TRANSACTION;\nEXCEPTION WHEN ERROR THEN\nROLLBACK TRANSACTION;\nRAISE;\nEND;', 'SELECT * FROM review_result;']
    script='\n'.join(sql);_require(script.count(';')<=64 and len(script.encode())<=128*1024,'REVIEW_PUBLICATION_SQL_LIMIT')
    config=settings(params);config.maximum_bytes_billed=PUBLICATION_BYTES;config.job_timeout_ms=PUBLICATION_TIMEOUT_MS
    config_json=canonical_json_bytes(config.to_api_repr()).decode()
    _require(len(config_json.encode())<=4*MAX_CAPTURE_BYTES,'REVIEW_PUBLICATION_CONFIG_TOO_LARGE')
    return PublicationPlan('campaign_review_publish_'+command,command,proposal.record.sha256,new_control.sha256,
        row_json(audit),script,config_json)
