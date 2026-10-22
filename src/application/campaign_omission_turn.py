"""Explicit omission becomes a new immutable user turn; no inferred approval."""
from dataclasses import dataclass,replace
import hashlib,json
from google.cloud import bigquery
from src.application.campaign_attachment_contract import attachment_requests
from src.application.campaign_document_store import TABLES as DOCUMENT_TABLES,SUBMISSIONS
from src.application.campaign_clarification_policy import DATASET,VERSIONS,REVOCATIONS,read_policy
from src.application.campaign_quality_authority_reader import read_execution_authority
from src.application.campaign_quality_authority import TABLE as AUTHORITIES
from src.application.campaign_evidence_gaps import GapPlan,canonical
from src.application.workflow_contract import _require,retry_key_hash
from src.application.workflow_codec import decode_row


@dataclass(frozen=True,slots=True)
class OmissionContext:
    canonical_json:str

    @property
    def value(self):return json.loads(self.canonical_json)
    @property
    def sha256(self):return hashlib.sha256(self.canonical_json.encode()).hexdigest()
    @property
    def message(self):
        v=self.value
        return canonical(dict(type='EXPLICIT_CAMPAIGN_OMISSION',parent_request_id=v['source']['scope']['request_id'],
            source_submission_id=v['submission_id'],requirement_id=v['source']['requirement_id'],
            selected_alternative=v['source']['selection']['alternative'],
            omitted_fields=v['source']['collection_request']['missing_fields'],
            instruction='Revise this campaign using my selected omission. Do not include unsupported content or treat any evidence as approved.'))


def read_omission(intake,store,headers,*,workspace_id,conversation_id,submission_id):
    actor,access=intake._actor(headers,workspace_id,conversation_id)
    source=store.submission(submission_id,workspace_id=workspace_id,conversation_id=conversation_id,
        principal_id=actor.principal.principal_id)
    _require(source['selection']['choice']=='OMIT_UNSUPPORTED_CONTENT' and source['selection']['status']=='REQUEST_CHANGE_PENDING'
        and source['file'] is None and bool(source['selection']['alternative']),'OMISSION_EXPLICIT_CHOICE_REQUIRED')
    request_id=source['scope']['request_id'];reader=intake.plan_reader
    args=dict(workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id,identity=actor,clock=reader.clock)
    initial=read_execution_authority(reader.client,**args)
    plan=reader(headers,workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id)
    _require(type(plan) is GapPlan,'OMISSION_CURRENT_GAP_REQUIRED')
    matches=[r for r in attachment_requests(plan) if json.loads(r.canonical_json)['requirement_id']==source['requirement_id']]
    _require(len(matches)==1 and matches[0].sha256==source['selection']['request_sha256'],'OMISSION_SOURCE_CHANGED')
    policy=read_policy(reader.client,reader.policy_request,clock=reader.clock)
    _require(policy.request.policy_sha256==json.loads(plan.canonical_json)['policy_sha256'],'OMISSION_POLICY_CHANGED')
    final=read_execution_authority(reader.client,**args)
    after,after_access=intake._actor(headers,workspace_id,conversation_id)
    _require(initial.content_sha256==final.content_sha256 and initial.native_job_id!=final.native_job_id
        and after.principal==actor.principal and after_access==access
        and reader.clock()<min(initial.valid_until,final.valid_until,policy.valid_until),'OMISSION_CONTEXT_CHANGED')
    authority=json.loads(initial.content_canonical_json)['authority']
    selection={k:authority[k] for k in ('sku','country','channel','audience')}
    value=dict(version='campaign-omission-context-v1',submission_id=submission_id,source=source,selection=selection,
        parent_authority=authority,policy=dict(policy_id=policy.request.policy_id,version=policy.request.version,
            policy_sha256=policy.request.policy_sha256,policy_json=policy.policy_canonical_json),
        generation_authorized=False)
    context=OmissionContext(canonical(value))
    _require(len(context.message)<=2000 and len(context.message.encode())<=65536,'OMISSION_MESSAGE_SIZE')
    return context


def omission_key(workspace_id,conversation_id,principal_id,submission_id):
    return retry_key_hash(workspace_id=workspace_id,conversation_id=conversation_id,principal_id=principal_id,
        operation='INTERPRET',retry_key='omission-v1-'+submission_id)


def link_id(workspace_id,conversation_id,principal_id,submission_id):
    return hashlib.sha256(canonical([workspace_id,conversation_id,principal_id,submission_id]).encode()).hexdigest()


def bind_omission_plan(plan,context):
    _require(type(context) is OmissionContext,'OMISSION_TRUSTED_CONTEXT_REQUIRED')
    value=context.value;source=value['source'];scope=source['scope'];policy=value['policy']
    _require(source['selection']['choice']=='OMIT_UNSUPPORTED_CONTENT' and source['selection']['status']=='REQUEST_CHANGE_PENDING'
        and source['file'] is None and value['generation_authorized'] is False,'OMISSION_EXPLICIT_CHOICE_REQUIRED')
    params={p.name:p.value for p in plan.config().query_parameters}
    request=decode_row(params['request_candidate'],'requests')
    expected=omission_key(scope['workspace_id'],scope['conversation_id'],source['principal_id'],value['submission_id'])
    _require(plan.job_id=='interpreter_submit_v1_'+expected and request['request_id']=='interpret-v1-'+expected
        and request['message_sha256']==hashlib.sha256(context.message.encode()).hexdigest()
        and request['principal_id']==source['principal_id'] and request['conversation_id']==scope['conversation_id']
        and request['workspace_id']==scope['workspace_id'] and 'history_requests' in params,'OMISSION_TURN_SCOPE')
    table=DOCUMENT_TABLES['turn_links'];rid=link_id(scope['workspace_id'],scope['conversation_id'],source['principal_id'],value['submission_id'])
    guards=f'''
ASSERT (SELECT COUNT(*) FROM `{SUBMISSIONS}` WHERE submission_id=@omission_submission_id AND record_json=@omission_source)=1 AS 'OMISSION_SOURCE_CHANGED';
ASSERT (SELECT COUNT(*) FROM `{AUTHORITIES}` WHERE workspace_id=@workspace_id AND request_id=@omission_parent_request AND payload_canonical_json=@omission_parent_authority)=1 AS 'OMISSION_PARENT_CHANGED';
ASSERT (SELECT COUNT(*) FROM `{DATASET}.{VERSIONS}` WHERE policy_id=@omission_policy_id AND version=@omission_policy_version AND policy_sha256=@omission_policy_sha AND policy_canonical_json=@omission_policy_json)=1
 AND (SELECT COUNT(*) FROM `{DATASET}.{REVOCATIONS}` WHERE policy_id=@omission_policy_id AND version=@omission_policy_version AND policy_sha256=@omission_policy_sha)=0 AS 'OMISSION_POLICY_CHANGED';
'''
    sql=plan.sql.replace('BEGIN TRANSACTION;','BEGIN TRANSACTION;\n'+guards,1)
    sql=sql.replace(" AS 'INTERPRETER_SUBMIT_REPLAY_CONFLICT';",f" AND (SELECT COUNT(*) FROM `{table}` WHERE record_id=@omission_link_id AND record_json=@omission_link)=1 AS 'INTERPRETER_SUBMIT_REPLAY_CONFLICT';")
    sql=sql.replace(" ASSERT CURRENT_TIMESTAMP()<@expires_at",f" ASSERT (SELECT COUNT(*) FROM `{table}` WHERE record_id=@omission_link_id)=0 AS 'OMISSION_ALREADY_APPLIED';\n ASSERT CURRENT_TIMESTAMP()<@expires_at",1)
    sql=sql.replace(" SET result_state='APPLIED';",f" INSERT INTO `{table}` (record_id,record_json,recorded_at) VALUES(@omission_link_id,@omission_link,request_candidate.created_at);\n SET result_state='APPLIED';",1)
    sql=sql.replace(' AS authority_sha256;',' AS authority_sha256,@omission_link AS omission_link_row;')
    _require(sql!=plan.sql and ' AS omission_link_row;' in sql,'OMISSION_SQL_BINDING')
    link=dict(version='campaign-omission-turn-link-v1',workspace_id=scope['workspace_id'],conversation_id=scope['conversation_id'],
        principal_id=source['principal_id'],source_submission_id=value['submission_id'],source_record_sha256=hashlib.sha256(canonical(source).encode()).hexdigest(),
        parent_request_id=scope['request_id'],new_request_id=request['request_id'],new_invocation_id=json.loads(params['invocation_candidate'])['invocation_id'],
        native_job_id=plan.job_id,new_request_sha256=request['request_sha256'],message_sha256=request['message_sha256'],
        authority_sha256=params['authority_digest'],sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
        alternative=source['selection']['alternative'],status='REEVALUATION_REQUIRED',generation_authorized=False)
    extra=dict(omission_submission_id=value['submission_id'],omission_source=canonical(source),omission_parent_request=scope['request_id'],
        omission_parent_authority=canonical(value['parent_authority']),omission_policy_id=policy['policy_id'],
        omission_policy_version=policy['version'],omission_policy_sha=policy['policy_sha256'],omission_policy_json=policy['policy_json'],
        omission_link_id=rid,omission_link=canonical(link))
    cfg=plan.config();cfg.query_parameters=list(cfg.query_parameters)+[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in extra.items()]
    return replace(plan,sql=sql,config_json=json.dumps(cfg.to_api_repr(),sort_keys=True),kind='OMISSION_INTERPRETER_SUBMISSION')
