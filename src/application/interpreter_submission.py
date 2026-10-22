"""Protected durable interpreter turn admission; no model dispatch or release."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
import hashlib,json
from threading import RLock
from google.cloud import bigquery
from src.application.workflow_submission_service import ProtectedSubmissionService
from src.application.workflow_contract import (PREFERENCE_FIELDS,request_fingerprint,retry_key_hash,
    validate_request,validate_model_invocation,validate_profile,_id,_require)
from src.application.workflow_schema import TABLES,SCHEMA_VERSION
from src.application.workflow_codec import encode_row,decode_row
from src.application.campaign_transcript import SPEC,encode_turns,VERSION as TRANSCRIPT_VERSION
from src.application.control_io import ControlIO,ControlPlan,query_config
from src.application.identity import IapIdentityAdapter,ensure_fresh
from src.application.access_policy import POLICY_VERSION
from src.application.access_policy import Action,Principal,require_authorized
from src.application.workspace_contract import validate_workspace
from src.application.membership_contract import validate_membership
from src.application.conversation_contract import validate_conversation
from src.application.audit_contract import (AuditTarget,ResourceType,WorkflowAction,
    MutationObservation,MutationKind,MutationOutcome)
from src.application.workflow_invocation_result_query import _fields_expr,_audit
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.ingestion.setup_workflow_tables import PROJECT,DATASET_ID,validate_table


@dataclass(frozen=True,slots=True)
class InterpreterIntakeOptions:
    workspace_id:str
    conversation_id:str
    audiences:tuple[str,...]
    snapshot_sha256:str
    output_schema_version:str


@dataclass(frozen=True,slots=True)
class InterpreterSubmissionReceipt:
    workspace_id:str
    conversation_id:str
    request_id:str
    invocation_id:str
    native_job_id:str
    native_status:str
    @property
    def release_allowed(self):return False


def prepare_interpreter_submission(*,workspace,membership,conversation,resource,principal_id,
        message,retry_key,profile,observed_at,worker_id):
    _require(type(message) is str and 0<len(message)<=2000 and len(message.encode())<=65536,
        'INTERPRETER_MESSAGE_INVALID')
    _id(worker_id);validate_profile(profile);_require(profile['stage']=='INTERPRETER','INTERPRETER_PROFILE_REQUIRED')
    ws=workspace['workspace_id'];conv=conversation['conversation_id']
    validate_workspace(workspace,workspace_id=ws)
    member=validate_membership(membership,workspace_id=ws,principal_id=principal_id)
    actual_resource=validate_conversation(conversation,workspace_id=ws,conversation_id=conv)
    _require(resource==actual_resource and resource.owner_id==principal_id
        and workspace['status']=='ACTIVE' and conversation['status']=='ACTIVE'
        and max(workspace['updated_at'],membership['updated_at'],conversation['updated_at'])<=observed_at,
        'INTERPRETER_SUBMISSION_SCOPE_INVALID')
    require_authorized(Principal(principal_id),member,Action.GENERATE_DRAFT,workspace_id=ws)
    key=retry_key_hash(workspace_id=ws,principal_id=principal_id,conversation_id=conv,
        operation='INTERPRET',retry_key=retry_key)
    req=dict.fromkeys(f.name for f in TABLES['requests'].schema)
    req.update(schema_version=SCHEMA_VERSION,workspace_id=ws,conversation_id=conv,
        owner_id=principal_id,principal_id=principal_id,request_id='interpret-v1-'+key,
        session_id='session-v1-'+key,trace_id=key[:32],operation='INTERPRET',request_status='ACCEPTED',
        revision=1,submitted_preferences=dict(preferences=dict.fromkeys(PREFERENCE_FIELDS),renderer=None),
        unsupported_features=[],message_sha256=hashlib.sha256(message.encode()).hexdigest(),
        idempotency_key_sha256=key,request_sha256='0'*64,model_profiles=[deepcopy(profile)],
        policy_version=POLICY_VERSION,created_at=observed_at,updated_at=observed_at)
    req['request_sha256']=request_fingerprint(req);validate_request(req,conversation=resource)
    inv=dict.fromkeys(f.name for f in TABLES['model_invocations'].schema)
    for k in ('schema_version','workspace_id','conversation_id','owner_id','request_id','trace_id'):inv[k]=req[k]
    inv.update(invocation_id='interpreter-v1-'+key,stage='INTERPRETER',call_number=1,state='PLANNED',
        revision=1,profile=deepcopy(profile),usage_status='NOT_REPORTED',cost_status='NOT_ESTIMATED',started_at=observed_at)
    validate_model_invocation(inv,request=req,conversation=resource)
    turn=dict(request_id=req['request_id'],role='USER',content_sha256=req['message_sha256'],invocation_id=None)
    batch=encode_turns(dict(transcript=[turn]),(message,))
    transcript=dict(schema_version=TRANSCRIPT_VERSION,workspace_id=ws,conversation_id=conv,
        request_id=req['request_id'],batch_canonical_json=batch,recorded_at=observed_at)
    rows=dict(workspace_pin=workspace,membership_pin=membership,conversation_pin=conversation,
        request_candidate=req,invocation_candidate=inv,transcript_candidate=transcript)
    schemas=dict(workspace_pin=TABLES['workspaces'],membership_pin=TABLES['memberships'],
        conversation_pin=TABLES['conversations'],request_candidate=TABLES['requests'],
        invocation_candidate=TABLES['model_invocations'],transcript_candidate=SPEC)
    targets=[('request_audit',WorkflowAction.CREATE_REQUEST,ResourceType.REQUEST,req['request_id']),
        ('invocation_audit',WorkflowAction.CREATE_MODEL_INVOCATION,ResourceType.MODEL_INVOCATION,inv['invocation_id'])]
    for name,action,kind,rid in targets:
        target=AuditTarget(kind,rid,ws,conversation_id=conv,owner_id=principal_id,request_id=req['request_id'])
        rows[name]=decode_row(_audit(worker_id=worker_id,workspace_id=ws,action=action,target=target,
            mutation=MutationObservation(MutationKind.CREATE,MutationOutcome.APPLIED,1),
            trace_id=req['trace_id'],at=observed_at,audit_id='interpreter-submit-v1:'+name+':'+key),'audit_events')
        schemas[name]=TABLES['audit_events']
    from src.application.workspace_contract import propose_workspace_fence
    rows['workspace_candidate']=propose_workspace_fence(workspace,workspace_id=ws,
        expected_revision=workspace['revision'],observed_at=observed_at);schemas['workspace_candidate']=TABLES['workspaces']
    def encoded(name):
        if name=='transcript_candidate':
            return json.dumps(rows[name],sort_keys=True,default=lambda v:v.isoformat(),separators=(',',':'))
        return encode_row(rows[name],schemas[name].name)
    params={k:encoded(k) for k in rows}
    declarations='\n'.join(f"DECLARE {k} DEFAULT STRUCT({_fields_expr(schemas[k].schema,f'PARSE_JSON(@{k})')});" for k in rows)
    def count(name,var,extra=''):
        spec=schemas[var];part=spec.partition_field
        part=(f" AND t.{part} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')") if part else ''
        return f"(SELECT COUNT(*) FROM `{DATASET_ID}.{name}` t WHERE t.workspace_id=@workspace_id{part} AND {extra or 'TO_JSON_STRING(t)=TO_JSON_STRING('+var+')'})"
    def insert(var):
        spec=schemas[var];columns=', '.join(f.name for f in spec.schema)
        return f"INSERT INTO `{DATASET_ID}.{spec.name}` ({columns}) SELECT "+', '.join(var+'.'+f.name for f in spec.schema)+';'
    inserts='\n'.join(insert(v) for v in ('request_candidate','invocation_candidate','transcript_candidate','request_audit','invocation_audit'))
    sql=declarations+f'''
DECLARE result_state STRING DEFAULT 'REUSED';
BEGIN TRANSACTION;
ASSERT {count('memberships','membership_pin')}=1 AND membership_pin.status='ACTIVE' AS 'INTERPRETER_SUBMIT_MEMBER_CHANGED';
ASSERT {count('conversations','conversation_pin')}=1 AND conversation_pin.status='ACTIVE' AS 'INTERPRETER_SUBMIT_CONVERSATION_CHANGED';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.workspaces` t WHERE t.workspace_id=@workspace_id AND t.status='ACTIVE')=1 AS 'INTERPRETER_SUBMIT_WORKSPACE_INACTIVE';
IF {count('requests','request_candidate')}=1 THEN
 ASSERT {count('model_invocations','invocation_candidate')}=1 AND {count(SPEC.name,'transcript_candidate')}=1
 AND {count('audit_events','request_audit')}=1 AND {count('audit_events','invocation_audit')}=1 AS 'INTERPRETER_SUBMIT_REPLAY_CONFLICT';
ELSE
 ASSERT CURRENT_TIMESTAMP()<@expires_at AND {count('workspaces','workspace_pin')}=1 AS 'INTERPRETER_SUBMIT_FENCE_CHANGED';
 ASSERT {count('requests','request_candidate','t.request_id=request_candidate.request_id')}=0
 AND {count('model_invocations','invocation_candidate','t.invocation_id=invocation_candidate.invocation_id')}=0
 AND {count(SPEC.name,'transcript_candidate','t.request_id=request_candidate.request_id')}=0
 AND {count('audit_events','request_audit','t.audit_id IN (request_audit.audit_id,invocation_audit.audit_id)')}=0 AS 'INTERPRETER_SUBMIT_CONFLICT';
 UPDATE `{DATASET_ID}.workspaces` t SET revision=workspace_candidate.revision,updated_at=workspace_candidate.updated_at
 WHERE t.workspace_id=@workspace_id AND t.revision=workspace_pin.revision;
 ASSERT @@row_count=1 AS 'INTERPRETER_SUBMIT_FENCE_CONFLICT';
 {inserts}
 SET result_state='APPLIED';
END IF;
COMMIT TRANSACTION;
SELECT result_state AS state,TO_JSON_STRING(request_candidate) AS request_row,
 TO_JSON_STRING(invocation_candidate) AS invocation_row,TO_JSON_STRING(transcript_candidate) AS transcript_row;
'''
    values=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in params.items()]
    values += [bigquery.ScalarQueryParameter('workspace_id','STRING',ws),
        bigquery.ScalarQueryParameter('expires_at','TIMESTAMP',observed_at+timedelta(seconds=30))]
    return ControlPlan(principal_id,ws,key,'interpreter_submit_v1_'+key,sql,
        json.dumps(query_config(values).to_api_repr(),sort_keys=True),'INTERPRETER_SUBMISSION',
        params['request_candidate'],params['request_audit'],'requests')


class ProtectedInterpreterSubmissionService:
    def __init__(self,*,client,verifier,profile,worker_id='interpreter-submission',enabled=False,capture_history=False):
        _require(isinstance(verifier,IapIdentityAdapter) and type(enabled) is bool and type(capture_history) is bool,'INTERPRETER_SUBMISSION_COMPOSITION')
        validate_profile(profile);_require(profile['stage']=='INTERPRETER','INTERPRETER_PROFILE_REQUIRED')
        _id(worker_id)
        self.client,self.verifier,self.profile=client,verifier,deepcopy(profile)
        self.worker_id,self.enabled=worker_id,enabled
        self.capture_history=capture_history
        self.reader=ProtectedSubmissionService(client=client,verifier=verifier)
        self.io=ControlIO(client);self._lock=RLock();self._plans={}

    def intake_options(self,headers,*,workspace_id,conversation_id):
        from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
        actor=self.verifier.authenticate(headers);ensure_fresh(actor)
        before=self.reader._snapshot(actor,workspace_id,conversation_id)
        registry=BigQueryPromptRegistryReader(self.client)
        first=registry.read_exact(self.profile['prompt_id'],self.profile['prompt_version'])
        snapshot=first.state.head.snapshot
        _require(snapshot.task.value=='INTERPRETER' and snapshot.output_schema_version==self.profile['output_schema_version']
            and snapshot.model_id==self.profile['model_id'],'INTERPRETER_INTAKE_PROFILE_MISMATCH')
        schema=json.loads(snapshot.output_schema_json)['properties']['audience']
        values=schema.get('enum')
        _require(type(values) is list and 0<len(values)<=32 and len(set(values))==len(values)
            and all(type(v) is str and 0<len(v)<=160 for v in values),'INTERPRETER_AUDIENCE_SCHEMA_UNSUPPORTED')
        second=registry.read_exact(self.profile['prompt_id'],self.profile['prompt_version'])
        after=self.reader._snapshot(actor,workspace_id,conversation_id);ensure_fresh(actor)
        _require(first.state.head==second.state.head and before[0]<=first.observed_at<=second.observed_at<=after[0]
            and before[1:5]==after[1:5],'INTERPRETER_INTAKE_CONTEXT_CHANGED')
        return InterpreterIntakeOptions(workspace_id,conversation_id,tuple(values),snapshot.snapshot_sha256,snapshot.output_schema_version)

    def omission(self,headers,**parameters):
        _require(getattr(self,'omission_service',None) is not None,'OMISSION_RUNTIME_NOT_INSTALLED')
        return self.omission_service.apply(headers,**parameters)

    def submit(self,headers,*,workspace_id,conversation_id,message,retry_key,selection=None,omission_context=None):
        _require(self.enabled,'INTERPRETER_SUBMISSION_DISABLED')
        _require(omission_context is not None or not retry_key.startswith('omission-v1-'),'OMISSION_RESERVED_RETRY_KEY')
        _require(type(message) is str and 0<len(message)<=2000 and len(message.encode())<=65536,
            'INTERPRETER_MESSAGE_INVALID')
        if selection is not None:
            from src.application.grounded_interpreter_submission import validate_selection
            selection=validate_selection(selection)
        with self._lock:
            actor=self.verifier.authenticate(headers);ensure_fresh(actor)
            at,workspace,member,conversation,resource,_=self.reader._snapshot(actor,workspace_id,conversation_id)
            key=retry_key_hash(workspace_id=workspace_id,principal_id=actor.principal.principal_id,
                conversation_id=conversation_id,operation='INTERPRET',retry_key=retry_key)
            plan=self._plans.get(key)
            if plan is None:
                _require(len(self._plans)<32,'INTERPRETER_SUBMISSION_CAPACITY')
                self.io.preflight();validate_table(self.client.get_table(SPEC.table(PROJECT).reference,retry=None,timeout=20),SPEC.table(PROJECT))
                builder=prepare_interpreter_submission
                extra={}
                if selection is not None:
                    from src.application.grounded_interpreter_submission import prepare_grounded_submission
                    from src.application.campaign_quality_authority import TABLE,table_metadata
                    validate_table(self.client.get_table(TABLE,retry=None,timeout=20),table_metadata())
                    builder=prepare_grounded_submission;extra={'selection':selection}
                    if self.capture_history:
                        from src.application.campaign_transcript_history import read_history
                        history=read_history(self.client,identity=actor,workspace_id=workspace_id,
                            conversation_id=conversation_id,conversation=conversation,clock=lambda:datetime.now(timezone.utc))
                        after_at,after_ws,after_member,after_conv,after_resource,_=self.reader._snapshot(actor,workspace_id,conversation_id)
                        _require((workspace,member,conversation,resource)==(after_ws,after_member,after_conv,after_resource),
                            'INTERPRETER_SUBMISSION_CONTEXT_CHANGED')
                        at=after_at;extra['history']=history
                plan=builder(**extra,workspace=workspace,membership=member,conversation=conversation,
                    resource=resource,principal_id=actor.principal.principal_id,message=message,retry_key=retry_key,
                    profile=self.profile,observed_at=at,worker_id=self.worker_id)
                if omission_context is not None:
                    from src.application.campaign_omission_turn import bind_omission_plan
                    from src.application.campaign_document_store import DocumentStore
                    DocumentStore(self.client).preflight('turn_links')
                    plan=bind_omission_plan(plan,omission_context)
                self._plans[key]=plan
            _require((plan.kind=='OMISSION_INTERPRETER_SUBMISSION')==(omission_context is not None),'OMISSION_RETRY_KIND_CHANGED')
            _require(decode_row(plan.candidate_json,'requests')['message_sha256']==hashlib.sha256(message.encode()).hexdigest(),
                'INTERPRETER_SUBMISSION_RETRY_CONFLICT')
            parameters={p.name:p.value for p in plan.config().query_parameters}
            if selection is not None:
                _require('authority_payload' in parameters,'INTERPRETER_SUBMISSION_RETRY_CONFLICT')
                authority=json.loads(parameters['authority_payload'])
                _require(all(authority[k]==v for k,v in selection.items()),'INTERPRETER_SUBMISSION_RETRY_CONFLICT')
            else:
                _require('authority_payload' not in parameters,'INTERPRETER_SUBMISSION_RETRY_CONFLICT')
            outcome=self.io.submit_once(plan)
            self.reader._snapshot(actor,workspace_id,conversation_id);ensure_fresh(actor)
            params={p.name:p.value for p in plan.config().query_parameters}
            req=decode_row(params['request_candidate'],'requests');inv=decode_row(params['invocation_candidate'],'model_invocations')
            if outcome.native_status is SharingCommitStatus.COMMITTED:
                expected_transcript=json.loads(params['transcript_candidate'])
                actual_transcript=json.loads(outcome.rows[0]['transcript_row']) if len(outcome.rows)==1 else {}
                for row in (expected_transcript,actual_transcript):
                    if 'recorded_at' in row:
                        row['recorded_at']=datetime.fromisoformat(row['recorded_at'].replace('Z','+00:00'))
                _require(len(outcome.rows)==1 and outcome.rows[0]['state'] in {'APPLIED','REUSED'}
                    and decode_row(outcome.rows[0]['request_row'],'requests')==req
                    and decode_row(outcome.rows[0]['invocation_row'],'model_invocations')==inv
                    and actual_transcript==expected_transcript
                    and ('authority_digest' not in params or outcome.rows[0].get('authority_sha256')==params['authority_digest']),
                    'INTERPRETER_SUBMISSION_UNVERIFIED')
                if omission_context is not None:
                    _require(outcome.rows[0].get('omission_link_row')==params['omission_link'],'OMISSION_LINK_UNVERIFIED')
            if outcome.native_status is SharingCommitStatus.COMMITTED and selection is not None:
                from src.application.campaign_quality_authority_reader import read_execution_authority
                capture=read_execution_authority(self.client,identity=actor,workspace_id=workspace_id,
                    conversation_id=conversation_id,request_id=req['request_id'])
                content=json.loads(capture.content_canonical_json)
                _require(content['authority']==json.loads(params['authority_payload']),
                    'INTERPRETER_SUBMISSION_AUTHORITY_CHANGED')
                self.reader._snapshot(actor,workspace_id,conversation_id);ensure_fresh(actor)
            return InterpreterSubmissionReceipt(workspace_id,conversation_id,req['request_id'],inv['invocation_id'],
                plan.job_id,outcome.native_status.value)
