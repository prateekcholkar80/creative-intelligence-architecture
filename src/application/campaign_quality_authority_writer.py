"""Disabled server-only selection capture; no approvals, dispatch or raw chat text."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timezone
import json
from google.cloud import bigquery

from src.application.access_policy import Action,AccessDecision,Reason
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_quality_authority import TABLE,validate_payload
from src.application.campaign_quality_authority_reader import _read_native,decode_capture,EXTRA_TABLES
from src.application.control_io import ControlIO,ControlPlan
from src.application.identity import ensure_fresh
from src.application.workflow_contract import _require,canonical_json_bytes
from src.application.campaign_quality_request_reader import partition_predicate
from src.ingestion.setup_workflow_tables import PROJECT


@dataclass(frozen=True,slots=True)
class AuthorityWriterSettings:
    enabled: bool=False

    def __post_init__(self):_require(type(self.enabled) is bool,'INVALID_AUTHORITY_WRITER_SETTINGS')


def _authorize(reader,identity,payload):
    ensure_fresh(identity)
    access=reader.authorize(identity,Action.GENERATE_DRAFT,workspace_id=payload['workspace_id'])
    _require(type(access) is AccessDecision and access.allowed and access.action is Action.GENERATE_DRAFT
        and access.principal_id==identity.principal.principal_id and access.workspace_id==payload['workspace_id']
        and access.resource_id is None,'AUTHORITY_CAPTURE_DENIED')
    owner=reader.authorize(identity,Action.READ_DRAFT,workspace_id=payload['workspace_id'],
        conversation_id=payload['conversation_id'])
    _require(type(owner) is AccessDecision and owner.allowed and owner.action is Action.READ_DRAFT
        and owner.reason is Reason.ALLOWED_OWNER and owner.workspace_id==payload['workspace_id']
        and owner.principal_id==identity.principal.principal_id and owner.resource_id==payload['conversation_id'],
        'AUTHORITY_CAPTURE_DENIED')
    return access,owner


def prepare_authority_write(client,payload,*,identity,access_reader,settings=AuthorityWriterSettings(),
                            clock=lambda:datetime.now(timezone.utc),transcript_texts=None):
    _require(type(settings) is AuthorityWriterSettings and settings.enabled,'AUTHORITY_WRITES_DISABLED')
    payload=deepcopy(validate_payload(payload));before=_authorize(access_reader,identity,payload)
    _require(payload['principal_id']==identity.principal.principal_id,'AUTHORITY_CAPTURE_DENIED')
    row,_,started,ended=_read_native(client,**{k:payload[k] for k in ('workspace_id','conversation_id','request_id')},
        identity=identity,clock=clock)
    encoded=canonical_json_bytes(payload).decode();digest=_hash(payload)
    proposed={k:payload[k] for k in ('workspace_id','conversation_id','request_id','principal_id')}
    proposed.update(payload_canonical_json=encoded,payload_sha256=digest,recorded_at=row['observed_at'].isoformat())
    # Pure proposal validation uses an explicit copy; the source reader itself
    # never accepts supplied rows or falls back when authoritative data is absent.
    supplied=deepcopy(row)
    if not supplied['authorities']:supplied['authorities']=[json.dumps(proposed)]
    content=decode_capture(supplied,**{k:payload[k] for k in ('workspace_id','conversation_id','request_id')},
        identity=identity,started=started,ended=ended)
    _require(content['authority']==payload,'AUTHORITY_CAPTURE_CONFLICT')
    _require(not any(c['request_id']==payload['request_id'] for c in content['calls']),
        'AUTHORITY_CAPTURE_AFTER_DISPATCH')
    parameters=[bigquery.ScalarQueryParameter(k,'STRING',payload[k])
        for k in ('workspace_id','conversation_id','request_id','principal_id')]
    parameters += [bigquery.ScalarQueryParameter('payload','STRING',encoded),
        bigquery.ScalarQueryParameter('digest','STRING',digest),
        bigquery.ScalarQueryParameter('expires','TIMESTAMP',datetime.fromtimestamp(identity.expires_at,timezone.utc)),
        bigquery.ScalarQueryParameter('issuer','STRING',identity.issuer),
        bigquery.ScalarQueryParameter('subject','STRING',identity.subject)]
    # Compare exact native row strings, not a second Python approximation of
    # BigQuery JSON serialization. Every source affecting the proposal is fenced.
    tables={'workspaces':'workspaces','memberships':'memberships','conversations':'conversations',
        'requests':'requests',**EXTRA_TABLES,'bindings':'invocation_prompt_bindings'}
    guards=[]
    for name,table in tables.items():
        where='workspace_id=@workspace_id'
        if name=='memberships':where+=' AND principal_id=@principal_id'
        elif name!='workspaces':where+=' AND conversation_id=@conversation_id'
        where+=partition_predicate(table)
        # Empty ARRAY parameters lose their arrayValues marker in native job
        # metadata. A scalar canonical JSON array preserves an exact wire pin.
        parameters.append(bigquery.ScalarQueryParameter('expected_'+name,'STRING',
            canonical_json_bytes(row[name]).decode()))
        guards.append(f'''CREATE TEMP TABLE current_{name} AS SELECT TO_JSON_STRING(t) value
 FROM `{PROJECT}.ci_application.{table}` t WHERE {where};
 ASSERT NOT EXISTS(SELECT value FROM (
 SELECT value,'CURRENT' origin FROM current_{name} UNION ALL
 SELECT e,'EXPECTED' FROM UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(@expected_{name}))) e)
 GROUP BY value HAVING COUNTIF(origin='CURRENT')!=COUNTIF(origin='EXPECTED')) AS 'AUTHORITY_SOURCE_CHANGED';''')
    parameters.append(bigquery.ArrayQueryParameter('products','STRING',row['products']))
    parameters.append(bigquery.ArrayQueryParameter('catalog_skus','STRING',sorted(
        json.loads(v)['sku'] for v in row['products'])))
    guards.append(f'''CREATE TEMP TABLE current_products AS SELECT TO_JSON_STRING(STRUCT(sku,source_system,product_status)) value
 FROM `{PROJECT}.ci_product.product_master` WHERE sku IN UNNEST(@catalog_skus);
 ASSERT NOT EXISTS(SELECT value FROM (
 SELECT value,'CURRENT' origin FROM current_products UNION ALL SELECT e,'EXPECTED' FROM UNNEST(@products) e)
 GROUP BY value HAVING COUNTIF(origin='CURRENT')!=COUNTIF(origin='EXPECTED')) AS 'AUTHORITY_CATALOG_CHANGED';''')
    sql=f'''DECLARE receipt_state STRING DEFAULT 'APPLIED';
BEGIN
BEGIN TRANSACTION;
ASSERT CURRENT_TIMESTAMP()<@expires AS 'ASSERTION_EXPIRED';
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.ci_application.workspaces` WHERE workspace_id=@workspace_id
 AND status='ACTIVE')=1 AS 'WORKSPACE_DENIED';
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.ci_application.memberships` WHERE workspace_id=@workspace_id
 AND principal_id=@principal_id AND identity_issuer=@issuer AND identity_subject=@subject
 AND status='ACTIVE' AND 'CREATOR' IN UNNEST(roles))=1 AS 'MEMBERSHIP_DENIED';
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.ci_application.conversations` WHERE workspace_id=@workspace_id
 AND conversation_id=@conversation_id AND owner_id=@principal_id AND status='ACTIVE')=1 AS 'OWNER_DENIED';
IF EXISTS(SELECT 1 FROM `{TABLE}` WHERE workspace_id=@workspace_id AND request_id=@request_id) THEN
 ASSERT (SELECT COUNT(*) FROM `{TABLE}` WHERE workspace_id=@workspace_id AND request_id=@request_id)=1
 AND (SELECT COUNT(*) FROM `{TABLE}` WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
 AND request_id=@request_id AND principal_id=@principal_id AND payload_sha256=@digest
 AND payload_canonical_json=@payload)=1 AS 'AUTHORITY_CAPTURE_CONFLICT';
 SET receipt_state='REUSED';
ELSE
{chr(10).join(guards)}
ASSERT NOT EXISTS(SELECT 1 FROM `{PROJECT}.ci_application.model_invocations` WHERE workspace_id=@workspace_id
 AND request_id=@request_id{partition_predicate('model_invocations')}) AS 'AUTHORITY_CAPTURE_AFTER_DISPATCH';
UPDATE `{PROJECT}.ci_application.workspaces` SET revision=revision+1,updated_at=CURRENT_TIMESTAMP()
 WHERE workspace_id=@workspace_id AND status='ACTIVE';
ASSERT @@row_count=1 AS 'AUTHORITY_FENCE_FAILED';
INSERT INTO `{TABLE}` (workspace_id,conversation_id,request_id,principal_id,payload_canonical_json,payload_sha256,recorded_at)
 VALUES(@workspace_id,@conversation_id,@request_id,@principal_id,@payload,@digest,CURRENT_TIMESTAMP());
END IF;
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
 ROLLBACK TRANSACTION;
 RAISE;
END;
SELECT receipt_state AS state,@digest AS payload_sha256;'''
    if transcript_texts is not None:
        from src.application.campaign_transcript import SPEC,extend_publication
        from src.application.operation_budget import operation_call
        from src.ingestion.setup_workflow_tables import validate_table
        expected=SPEC.table(client.project)
        validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)
        sql,parameters=extend_publication(sql,parameters,payload,transcript_texts)
    cfg=bigquery.QueryJobConfig(query_parameters=parameters,use_legacy_sql=False,use_query_cache=False,create_session=False,
        maximum_bytes_billed=768*1024**2,job_timeout_ms=120000,
        script_options=bigquery.ScriptOptions(statement_timeout_ms=20000))
    # One global native slot per request, independent of actor/selection content.
    job_id='quality_authority_publish_'+_hash([payload[k] for k in ('workspace_id','conversation_id','request_id')])
    plan=ControlPlan(identity.principal.principal_id,payload['workspace_id'],digest,job_id,sql,
        json.dumps(cfg.to_api_repr()),'CAPTURE_AUTHORITY',encoded,'',TABLE)
    _require(_authorize(access_reader,identity,payload)==before,'AUTHORITY_ACCESS_CHANGED')
    return plan


def submit_authority_write(client,plan,*,identity,access_reader,settings=AuthorityWriterSettings(),recover_only=False):
    _require(type(settings) is AuthorityWriterSettings and settings.enabled and type(plan) is ControlPlan
        and plan.kind=='CAPTURE_AUTHORITY' and plan.candidate_table==TABLE,'AUTHORITY_WRITES_DISABLED')
    payload=validate_payload(json.loads(plan.candidate_json))
    _require(plan.actor_id==identity.principal.principal_id==payload['principal_id'],'AUTHORITY_CAPTURE_DENIED')
    before=_authorize(access_reader,identity,payload)
    io=ControlIO(client,result_timeout_seconds=90)
    if not recover_only:io.preflight()
    outcome=io.recover(plan) if recover_only else io.submit_once(plan)
    _require(before==_authorize(access_reader,identity,payload),'AUTHORITY_ACCESS_CHANGED')
    if outcome.rows:
        _require(len(outcome.rows)==1 and set(outcome.rows[0])=={'state','payload_sha256'}
            and outcome.rows[0]['state'] in {'APPLIED','REUSED'} and outcome.rows[0]['payload_sha256']==_hash(payload),
            'AUTHORITY_RECEIPT_MISMATCH')
    return outcome
