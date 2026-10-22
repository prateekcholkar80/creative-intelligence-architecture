"""4.4 opt-in IAP-controlled management; fenced changes with explicit audit rows.

Prepared envelopes are server-only. They are not access grants or serving data.
No default bootstrap owner, directory mapping, roles, restore or automatic retry.
"""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
import hashlib
import json
import re
from threading import RLock

from google.cloud import bigquery

from src.application.access_policy import Action,MembershipStatus,Role,require_authorized
from src.application.audit_contract import (AuditTarget,BootstrapObservation,MutationKind,
    MutationObservation,MutationOutcome,ResourceType,validate_audit_event)
from src.application.control_io import ControlIO,ControlPlan,ControlOutcome,query_config
from src.application.conversation_contract import (ConversationChange,propose_conversation_creation,
    validate_conversation,validate_conversation_change)
from src.application.identity import ISSUER,IapIdentityAdapter,ensure_fresh,principal_for_subject
from src.application.membership_contract import (AssignmentOrigin,validate_membership,
    validate_membership_creation,validate_membership_change)
from src.application.operation_budget import OperationSettings,check_operation,operation_lock,operation_scope,operation_call
from google.api_core.exceptions import NotFound
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_codec import decode_row,encode_row,sql_row
from src.application.workflow_contract import ContractError,_id,canonical_json_bytes
from src.application.workflow_schema import SCHEMA_VERSION,TABLES
from src.application.workspace_contract import (WorkspaceChange,propose_workspace_fence,
    validate_workspace,validate_workspace_change,validate_workspace_creation)
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

KINDS=frozenset(('MEMBER','SYNC','WORKSPACE_STATUS','ARCHIVE','CREATE_CONVERSATION','BOOTSTRAP'))


def now():return datetime.now(timezone.utc)
def digest(value):return hashlib.sha256(canonical_json_bytes(value)).hexdigest()
def table(name):return f'`{PROJECT}.ci_application.{name}`'
def parameter(name,value,kind='STRING'):return bigquery.ScalarQueryParameter(name,kind,value)
def require(condition,code='MANAGEMENT_DENIED'):
    if not condition:raise ContractError(code)


@dataclass(frozen=True,slots=True)
class WorkspaceBootstrapManifest:
    workspace_id: str
    owner_subject: str
    roles: tuple[str,...]
    manifest_id: str
    created_at: datetime
    expires_at: datetime

    def __post_init__(self):
        _id(self.workspace_id);_id(self.manifest_id)
        principal_for_subject(ISSUER,self.owner_subject)
        require(type(self.roles) is tuple and all(type(role) is str for role in self.roles) and len(set(self.roles))==len(self.roles)
            and 'ADMIN' in self.roles and all(role in {r.value for r in Role} for role in self.roles))
        require(isinstance(self.created_at,datetime) and self.created_at.tzinfo is not None
            and isinstance(self.expires_at,datetime) and self.expires_at.tzinfo is not None
            and timedelta(0)<self.expires_at-self.created_at<=timedelta(minutes=5))


@dataclass(frozen=True,slots=True)
class MembershipSyncMapping:
    """Explicit deployment-approved mapping, not a live Directory API connector."""
    mapping_id: str
    workspace_id: str
    subject: str
    roles: tuple[str,...]
    status: str
    verified_at: datetime
    expires_at: datetime

    def __post_init__(self):
        _id(self.mapping_id);_id(self.workspace_id);principal_for_subject(ISSUER,self.subject)
        require(type(self.roles) is tuple and all(type(role) is str for role in self.roles) and len(set(self.roles))==len(self.roles)
            and all(role in {r.value for r in Role} for role in self.roles) and self.status in {s.value for s in MembershipStatus})
        require(isinstance(self.verified_at,datetime) and self.verified_at.tzinfo is not None
            and isinstance(self.expires_at,datetime) and self.expires_at.tzinfo is not None
            and timedelta(0)<self.expires_at-self.verified_at<=timedelta(hours=1))


@dataclass(frozen=True,slots=True)
class ManagementSettings:
    enabled: bool = False
    bootstrap: WorkspaceBootstrapManifest | None = None
    sync_mappings: tuple[MembershipSyncMapping,...] = ()
    operation: OperationSettings = OperationSettings()
    maximum_members: int = 512

    def __post_init__(self):
        require(type(self.enabled) is bool and (self.bootstrap is None or type(self.bootstrap) is WorkspaceBootstrapManifest)
            and type(self.sync_mappings) is tuple and all(type(m) is MembershipSyncMapping for m in self.sync_mappings)
            and len(self.sync_mappings)<=512 and len({m.mapping_id for m in self.sync_mappings})==len(self.sync_mappings)
            and type(self.operation) is OperationSettings and type(self.maximum_members) is int and 1<=self.maximum_members<=512)


def audit_row(observation,target,mutation,trace_id,observed_at):
    bootstrap=isinstance(observation,BootstrapObservation)
    row=dict(schema_version=SCHEMA_VERSION,workspace_id=target.workspace_id,audit_id='manage-v1:'+trace_id,
        actor_id=observation.actor_id if bootstrap else observation.principal_id,actor_kind='USER',
        action='BOOTSTRAP_WORKSPACE' if bootstrap else observation.action.value,allowed=True,
        reason_code='EXPLICIT_OPERATOR' if bootstrap else observation.reason.value,policy_version=observation.policy_version,
        resource_type=target.resource_type.value,resource_id=target.resource_id,resource_revision=target.revision,
        conversation_id=target.conversation_id,owner_id=target.owner_id,request_id=target.request_id,job_id=target.job_id,
        trace_id=trace_id,membership_revision=None if bootstrap else observation.membership_revision,
        mutation_outcome=mutation.outcome.value,new_revision=mutation.new_revision,occurred_at=observed_at)
    validate_audit_event(row,observation=observation,target=target,mutation=mutation,
        observed_at=observed_at,recorded_at=observed_at,trace_id=trace_id)
    return row


SNAPSHOT_SQL=f'''BEGIN TRANSACTION;
CREATE TEMP TABLE snapshot AS SELECT CURRENT_TIMESTAMP() AS observed_at,
  ARRAY(SELECT TO_JSON_STRING(w) FROM {table('workspaces')} w WHERE workspace_id=@workspace_id LIMIT 2) AS workspaces,
  ARRAY(SELECT TO_JSON_STRING(m) FROM {table('memberships')} m WHERE workspace_id=@workspace_id ORDER BY principal_id LIMIT @member_limit) AS memberships,
  (SELECT COUNT(*) FROM {table('memberships')} WHERE workspace_id=@workspace_id) AS member_count,
  ARRAY(SELECT TO_JSON_STRING(c) FROM {table('conversations')} c WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id LIMIT 2) AS conversations;
COMMIT TRANSACTION;
SELECT * FROM snapshot;'''


class ProtectedManagementService:
    def __init__(self,*,client,verifier,settings=ManagementSettings()):
        require(isinstance(verifier,IapIdentityAdapter) and type(settings) is ManagementSettings)
        self.io=ControlIO(client);self.verifier,self.settings=verifier,settings
        self._lock=RLock();self._issued={};self._business={};self._attempted=set()

    def _identity(self,headers,*,write):
        if write:require(self.settings.enabled,'MANAGEMENT_DISABLED')
        actor=self.verifier.authenticate(headers);ensure_fresh(actor);return actor

    def _snapshot(self,workspace_id,conversation_id='unused'):
        row=self.io.read(SNAPSHOT_SQL,[parameter('workspace_id',workspace_id),parameter('conversation_id',conversation_id),
            parameter('member_limit',self.settings.maximum_members+1,'INT64')])
        require(set(row)=={'observed_at','workspaces','memberships','member_count','conversations'})
        require(len(row['workspaces'])==1 and type(row['member_count']) is int
            and row['member_count']==len(row['memberships'])<=self.settings.maximum_members)
        ws=decode_row(row['workspaces'][0],'workspaces');validate_workspace(ws,workspace_id=workspace_id)
        members=[decode_row(value,'memberships') for value in row['memberships']]
        require(len({m['principal_id'] for m in members})==len(members))
        for member in members:validate_membership(member,workspace_id=workspace_id,principal_id=member['principal_id'])
        require(isinstance(row['observed_at'],datetime) and row['observed_at'].tzinfo is not None
            and all(m['updated_at']<=row['observed_at'] for m in members) and ws['updated_at']<=row['observed_at'])
        return row,ws,members

    def _authorize(self,actor,kind,workspace_id,row,ws,members,*,recovery=False):
        member=next((m for m in members if m['principal_id']==actor.principal.principal_id),None)
        require(member is not None and member['identity_issuer']==actor.issuer and member['identity_subject']==actor.subject)
        membership=validate_membership(member,workspace_id=workspace_id,principal_id=actor.principal.principal_id)
        require(ws['status']=='ACTIVE' or kind=='WORKSPACE_STATUS','WORKSPACE_INACTIVE')
        action={'MEMBER':Action.MANAGE_MEMBERS,'SYNC':Action.MANAGE_MEMBERS,'WORKSPACE_STATUS':Action.MANAGE_WORKSPACE,
                'ARCHIVE':Action.ARCHIVE_CONVERSATION,'CREATE_CONVERSATION':Action.GENERATE_DRAFT}[kind]
        resource=None
        if kind=='ARCHIVE':
            require(len(row['conversations'])==1)
            conversation=decode_row(row['conversations'][0],'conversations')
            resource=validate_conversation(conversation,workspace_id=workspace_id,conversation_id=conversation['conversation_id'])
            require(recovery or conversation['status']=='ACTIVE','CONVERSATION_ARCHIVED')
        ensure_fresh(actor)
        return require_authorized(actor.principal,membership,action,workspace_id=workspace_id,resource=resource)

    def prepare(self,headers,*,kind,workspace_id,command_id,expected_workspace_revision=None,
                target_subject=None,roles=(),status=None,expected_revision=None,conversation_id=None,mapping_id=None,slot=1):
        require(kind in KINDS);_id(workspace_id);_id(command_id)
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            actor=self._identity(headers,write=True)
            self.io.preflight()
            if kind=='BOOTSTRAP':
                return self._bootstrap(actor,workspace_id,command_id,slot)
            require(type(slot) is int and slot==1 and type(expected_workspace_revision) is int and 1<=expected_workspace_revision<2**63)
            row,ws,members=self._snapshot(workspace_id,conversation_id or 'unused')
            decision=self._authorize(actor,kind,workspace_id,row,ws,members)
            timestamp=row['observed_at']
            business=digest(dict(kind=kind,workspace_id=workspace_id,command_id=command_id,actor=actor.principal.principal_id,
                target_subject=target_subject,roles=list(roles),status=status,expected_revision=expected_revision,
                expected_workspace_revision=expected_workspace_revision,conversation_id=conversation_id,mapping_id=mapping_id))
            job_id='management_v1_'+digest([PROJECT,workspace_id,actor.principal.principal_id,command_id])
            if job_id in self._issued:
                require(self._business[job_id]==business,'MANAGEMENT_COMMAND_CONFLICT');return self._issued[job_id]
            require(ws['revision']==expected_workspace_revision,'STALE_WORKSPACE_REVISION')
            fence=propose_workspace_fence(ws,workspace_id=workspace_id,expected_revision=ws['revision'],observed_at=timestamp) if ws['status']=='ACTIVE' else None
            before=None
            if kind in {'MEMBER','SYNC'}:
                origin=AssignmentOrigin.ADMIN
                if kind=='SYNC':
                    mapping=next((m for m in self.settings.sync_mappings if m.mapping_id==mapping_id),None)
                    require(mapping is not None and mapping.workspace_id==workspace_id
                        and mapping.verified_at<=timestamp<mapping.expires_at,'SYNC_MAPPING_UNAVAILABLE')
                    require(target_subject is None and roles==() and status is None,'SYNC_CALLER_MAPPING_DISABLED')
                    target_subject,roles,status=mapping.subject,mapping.roles,mapping.status;origin=AssignmentOrigin.SYNC
                target=principal_for_subject(ISSUER,target_subject).principal_id
                require(type(roles) is tuple and all(type(role) is str for role in roles) and len(set(roles))==len(roles) and all(role in {r.value for r in Role} for role in roles)
                    and status in {s.value for s in MembershipStatus})
                before=next((m for m in members if m['principal_id']==target),None)
                if before is None:
                    require(expected_revision is None)
                    candidate=dict(schema_version=SCHEMA_VERSION,workspace_id=workspace_id,principal_id=target,
                        identity_issuer=ISSUER,identity_subject=target_subject,roles=sorted(roles),status=status,revision=1,
                        assignment_origin=origin.value,updated_by=actor.principal.principal_id,created_at=timestamp,updated_at=timestamp)
                    validate_membership_creation(candidate,workspace_id=workspace_id,principal_id=target,observed_at=timestamp,
                        updated_by=actor.principal.principal_id,origin=origin)
                else:
                    candidate=before|dict(roles=sorted(roles),status=status,revision=before['revision']+1,
                        assignment_origin=origin.value,updated_by=actor.principal.principal_id,updated_at=timestamp)
                    validate_membership_change(before,candidate,workspace_id=workspace_id,principal_id=target,
                        expected_revision=expected_revision,observed_at=timestamp,updated_by=actor.principal.principal_id,origin=origin)
                after=[m for m in members if m['principal_id']!=target]+[candidate]
                require(any(m['status']=='ACTIVE' and 'ADMIN' in m['roles'] for m in after),'LAST_ADMIN_REQUIRED')
                candidate_table='memberships';audit_target=AuditTarget(ResourceType.MEMBERSHIP,target,workspace_id,
                    None if before is None else before['revision'])
            elif kind=='WORKSPACE_STATUS':
                require(status in {'ACTIVE','DISABLED'})
                candidate=ws|dict(status=status,revision=ws['revision']+1,updated_at=timestamp)
                validate_workspace_change(ws,candidate,workspace_id=workspace_id,expected_revision=ws['revision'],
                    observed_at=timestamp,change=WorkspaceChange.SET_STATUS)
                fence=candidate;candidate_table='workspaces';before=ws
                audit_target=AuditTarget(ResourceType.WORKSPACE,workspace_id,workspace_id,ws['revision'])
            else:
                _id(conversation_id)
                if kind=='CREATE_CONVERSATION':
                    require(not row['conversations'] and expected_revision is None)
                    actor_member=next(m for m in members if m['principal_id']==actor.principal.principal_id)
                    candidate=propose_conversation_creation(workspace=ws,workspace_id=workspace_id,conversation_id=conversation_id,
                        expected_workspace_revision=ws['revision'],actor=actor.principal,actor_membership=actor_member,observed_at=timestamp)
                else:
                    before=decode_row(row['conversations'][0],'conversations')
                    candidate=before|dict(status='ARCHIVED',access_revision=before['access_revision']+1,updated_at=timestamp)
                    validate_conversation_change(before,candidate,workspace_id=workspace_id,conversation_id=conversation_id,
                        expected_revision=expected_revision,observed_at=timestamp,change=ConversationChange.ARCHIVE)
                candidate_table='conversations';audit_target=AuditTarget(ResourceType.CONVERSATION,conversation_id,workspace_id,
                    None if before is None else before['access_revision'],conversation_id,candidate['owner_id'])
            revision=candidate['access_revision'] if candidate_table=='conversations' else candidate['revision']
            audit=audit_row(decision,audit_target,MutationObservation(MutationKind.CREATE if before is None else MutationKind.UPDATE,
                MutationOutcome.APPLIED,revision),business,timestamp)
            params=[parameter('workspace_id',workspace_id),parameter('actor_id',actor.principal.principal_id),
                parameter('actor_subject',actor.subject),parameter('actor_issuer',actor.issuer),
                parameter('assertion_expires_at',datetime.fromtimestamp(actor.expires_at+30,timezone.utc),'TIMESTAMP'),
                parameter('workspace_pin',row['workspaces'][0]),parameter('members_pin',json.dumps(row['memberships'],separators=(',',':'))),
                parameter('candidate',encode_row(candidate,candidate_table)),parameter('workspace',encode_row(fence,'workspaces')),
                parameter('audit',encode_row(audit,'audit_events')),parameter('audit_id',audit['audit_id']),
                parameter('start_at',timestamp-timedelta(minutes=5),'TIMESTAMP'),parameter('end_at',
                    min(timestamp+timedelta(minutes=5),mapping.expires_at) if kind=='SYNC' else timestamp+timedelta(minutes=5),'TIMESTAMP')]
            if candidate_table=='conversations':params+=[parameter('conversation_id',conversation_id)]
            if kind=='ARCHIVE':params+=[parameter('conversation_pin',row['conversations'][0])]
            sql=management_sql(kind,candidate_table,before is None)
            config=query_config(params)
            plan=ControlPlan(actor.principal.principal_id,workspace_id,command_id,job_id,sql,
                json.dumps(config.to_api_repr(),sort_keys=True),kind,encode_row(candidate,candidate_table),encode_row(audit,'audit_events'),candidate_table)
            require(len(self._issued)<1024,'MANAGEMENT_ADMISSION_FULL')
            self._issued[job_id]=plan;self._business[job_id]=business;return plan

    def _bootstrap(self,actor,workspace_id,command_id,slot):
        manifest=self.settings.bootstrap
        require(manifest is not None and manifest.workspace_id==workspace_id and manifest.manifest_id==command_id
            and manifest.owner_subject==actor.subject and manifest.created_at<=now()<manifest.expires_at,'BOOTSTRAP_NOT_AUTHORIZED')
        require(type(slot) is int and 1<=slot<=3)
        timestamp=manifest.created_at
        workspace=dict(schema_version=SCHEMA_VERSION,workspace_id=workspace_id,status='ACTIVE',revision=1,
            created_at=timestamp,updated_at=timestamp)
        owner=dict(schema_version=SCHEMA_VERSION,workspace_id=workspace_id,principal_id=actor.principal.principal_id,
            identity_issuer=ISSUER,identity_subject=actor.subject,roles=sorted(manifest.roles),status='ACTIVE',revision=1,
            assignment_origin='BOOTSTRAP',updated_by=actor.principal.principal_id,created_at=timestamp,updated_at=timestamp)
        validate_workspace_creation(workspace,workspace_id=workspace_id,observed_at=timestamp)
        validate_membership_creation(owner,workspace_id=workspace_id,principal_id=owner['principal_id'],observed_at=timestamp,
            updated_by=actor.principal.principal_id,origin=AssignmentOrigin.BOOTSTRAP)
        manifest_hash=digest([encode_row(workspace,'workspaces'),encode_row(owner,'memberships'),
            manifest.manifest_id,manifest.expires_at.isoformat()])
        audit=audit_row(BootstrapObservation(owner['principal_id'],workspace_id,manifest_hash),
            AuditTarget(ResourceType.WORKSPACE,workspace_id,workspace_id),
            MutationObservation(MutationKind.CREATE,MutationOutcome.APPLIED,1),manifest_hash,timestamp)
        params=[parameter('workspace_id',workspace_id),parameter('workspace',encode_row(workspace,'workspaces')),
            parameter('owner',encode_row(owner,'memberships')),parameter('audit',encode_row(audit,'audit_events')),
            parameter('audit_id',audit['audit_id']),parameter('expires_at',manifest.expires_at,'TIMESTAMP'),
            parameter('start_at',timestamp-timedelta(minutes=5),'TIMESTAMP'),parameter('end_at',timestamp+timedelta(minutes=5),'TIMESTAMP')]
        config=query_config(params);job_id=f'workspace_bootstrap_v1_{digest([PROJECT,workspace_id])}_{slot}'
        plan=ControlPlan(owner['principal_id'],workspace_id,command_id,job_id,bootstrap_sql(),
            json.dumps(config.to_api_repr(),sort_keys=True),'BOOTSTRAP',encode_row(workspace,'workspaces'),encode_row(audit,'audit_events'),'workspaces')
        previous=self._issued.get(job_id)
        if previous is not None:require(previous==plan,'BOOTSTRAP_MANIFEST_CONFLICT');return previous
        require(len(self._issued)<1024,'MANAGEMENT_ADMISSION_FULL');self._issued[job_id]=plan;return plan

    def _current(self,actor,plan):
        require(type(plan) is ControlPlan and plan.actor_id==actor.principal.principal_id and plan.kind in KINDS)
        _id(plan.workspace_id);_id(plan.command_id)
        candidate=decode_row(plan.candidate_json,plan.candidate_table)
        audit=decode_row(plan.audit_json,'audit_events')
        require(candidate['workspace_id']==plan.workspace_id and audit['workspace_id']==plan.workspace_id
            and audit['actor_id']==plan.actor_id and audit['mutation_outcome']=='APPLIED')
        params=plan.config().query_parameters
        values={p.name:p.value for p in params}
        require(len(values)==len(params) and values.get('workspace_id')==plan.workspace_id
            and values.get('audit')==plan.audit_json and values.get('audit_id')==audit['audit_id'])
        require(plan.config().to_api_repr()==query_config(params).to_api_repr(),'CONTROL_CONFIG_INVALID')
        if plan.kind=='BOOTSTRAP':
            manifest=self.settings.bootstrap
            require(manifest is not None and manifest.workspace_id==plan.workspace_id and manifest.owner_subject==actor.subject
                and manifest.manifest_id==plan.command_id,'BOOTSTRAP_NOT_AUTHORIZED')
            require(re.fullmatch('workspace_bootstrap_v1_'+digest([PROJECT,plan.workspace_id])+r'_[1-3]',plan.job_id)
                and plan.sql==bootstrap_sql() and plan.candidate_table=='workspaces'
                and values.get('workspace')==plan.candidate_json and values.get('expires_at')==manifest.expires_at)
        else:
            creating=audit['resource_revision'] is None
            require(plan.job_id=='management_v1_'+digest([PROJECT,plan.workspace_id,plan.actor_id,plan.command_id])
                and plan.sql==management_sql(plan.kind,plan.candidate_table,creating)
                and values.get('candidate')==plan.candidate_json and values.get('actor_id')==plan.actor_id
                and values.get('actor_subject')==actor.subject and values.get('actor_issuer')==actor.issuer)
            row,ws,members=self._snapshot(plan.workspace_id,candidate.get('conversation_id','unused'))
            self._authorize(actor,plan.kind,plan.workspace_id,row,ws,members,recovery=True)

    def _validate_outcome(self,plan,outcome):
        if outcome.native_status is not SharingCommitStatus.COMMITTED or not outcome.rows:return outcome
        row=outcome.rows[0]
        require(set(row)=={'state','candidate','audit'} and row['state'] in {'APPLIED','REUSED'},'MANAGEMENT_RESULT_INVALID')
        require(decode_row(row['candidate'],plan.candidate_table)==decode_row(plan.candidate_json,plan.candidate_table)
            and decode_row(row['audit'],'audit_events')==decode_row(plan.audit_json,'audit_events'),'MANAGEMENT_RESULT_INVALID')
        return outcome

    def submit(self,headers,plan):
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            actor=self._identity(headers,write=True);self._current(actor,plan)
            self.io.preflight()
            require(self._issued.get(plan.job_id) is plan,'MANAGEMENT_PLAN_NOT_ISSUED')
            if plan.job_id in self._attempted:return self.recover(headers,plan)
            self._attempted.add(plan.job_id)
            if plan.kind=='BOOTSTRAP':
                self._bootstrap(actor,plan.workspace_id,plan.command_id,int(plan.job_id.rsplit('_',1)[1]))
                for slot in range(1,int(plan.job_id.rsplit('_',1)[1])):
                    predecessor=ControlPlan(plan.actor_id,plan.workspace_id,plan.command_id,
                        plan.job_id.rsplit('_',1)[0]+'_'+str(slot),plan.sql,plan.config_json,plan.kind,
                        plan.candidate_json,plan.audit_json,plan.candidate_table)
                    require(self.io.recover(predecessor).native_status is SharingCommitStatus.NOT_COMMITTED,'BOOTSTRAP_RETRY_UNPROVEN')
            outcome=self._validate_outcome(plan,self.io.submit_once(plan));check_operation()
            self._current(self._identity(headers,write=False),plan);return outcome

    def recover(self,headers,plan):
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            actor=self._identity(headers,write=False);self._current(actor,plan)
            outcome=self._validate_outcome(plan,self.io.recover(plan));check_operation()
            self._current(self._identity(headers,write=False),plan);return outcome

    def original_plan(self,headers,*,workspace_id,command_id):
        """Read-only cold-worker reconstruction from the exact original SDK job.

        This never registers an issued proposal or authorizes resubmission.
        Missing/ambiguous native metadata stays unavailable, not a new command.
        """
        _id(workspace_id);_id(command_id)
        with operation_scope(self.settings.operation),operation_lock(self._lock):
            actor=self._identity(headers,write=False)
            manifest=self.settings.bootstrap
            bootstrap=manifest is not None and manifest.workspace_id==workspace_id and manifest.manifest_id==command_id
            ids=[f'workspace_bootstrap_v1_{digest([PROJECT,workspace_id])}_{slot}' for slot in range(1,4)] if bootstrap else [
                'management_v1_'+digest([PROJECT,workspace_id,actor.principal.principal_id,command_id])]
            jobs=[]
            for job_id in ids:
                try:job=self.io.original_job(job_id)
                except NotFound:continue
                require(isinstance(job,bigquery.QueryJob) and job.project==PROJECT and job.location==LOCATION
                    and job.job_id==job_id and job.parent_job_id is None,'MANAGEMENT_RECOVERY_UNVERIFIED')
                jobs.append(job)
            require(bool(jobs),'MANAGEMENT_RECOVERY_UNAVAILABLE')
            job=jobs[-1];params=job.query_parameters;values={p.name:p.value for p in params}
            require(len(params)==len(values) and values.get('workspace_id')==workspace_id)
            audit=decode_row(values['audit'],'audit_events')
            if bootstrap:
                kind,candidate_table,candidate='BOOTSTRAP','workspaces',values['workspace']
            else:
                action=audit['action']
                require(action in {'MANAGE_MEMBERS','MANAGE_WORKSPACE','ARCHIVE_CONVERSATION','GENERATE_DRAFT'})
                candidate_table={'MANAGE_MEMBERS':'memberships','MANAGE_WORKSPACE':'workspaces',
                    'ARCHIVE_CONVERSATION':'conversations','GENERATE_DRAFT':'conversations'}[action]
                candidate=values['candidate'];row=decode_row(candidate,candidate_table)
                kind={'MANAGE_WORKSPACE':'WORKSPACE_STATUS','ARCHIVE_CONVERSATION':'ARCHIVE','GENERATE_DRAFT':'CREATE_CONVERSATION'}.get(action)
                if kind is None:kind='SYNC' if row['assignment_origin']=='SYNC' else 'MEMBER'
            plan=ControlPlan(actor.principal.principal_id,workspace_id,command_id,job.job_id,job.query,
                json.dumps(query_config(params).to_api_repr(),sort_keys=True),kind,candidate,values['audit'],candidate_table)
            self._current(self._identity(headers,write=False),plan);check_operation()
            return plan


def _insert(name,param):
    columns=', '.join(f.name for f in TABLES[name].schema)
    return f'INSERT INTO {table(name)} ({columns}) SELECT r.* FROM (SELECT {sql_row(param,name)} AS r);'


def _update(name,param,key):
    fields=[f.name for f in TABLES[name].schema if f.name not in {'schema_version','workspace_id',key}]
    sets=', '.join(f'{field}=r.{field}' for field in fields)
    return f'''UPDATE {table(name)} t SET {sets} FROM (SELECT {sql_row(param,name)} AS r)
        WHERE t.workspace_id=@workspace_id AND t.{key}=r.{key};
        ASSERT @@row_count=1 AS 'CONTROL_CONFLICT';'''


def _finish():
    return '''COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
-- PROBE_AFTER_COMMIT
SELECT * FROM result;'''


def bootstrap_sql():
    absent='\n'.join(f"ASSERT (SELECT COUNT(*) FROM {table(name)} WHERE workspace_id=@workspace_id "
        + (f"AND {spec.partition_field} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31') " if spec.partition_field else '')
        + ")=0 AS 'BOOTSTRAP_PARTIAL';"
        for name,spec in TABLES.items() if name not in {'workspaces','memberships','conversations','jobs','audit_events'})
    return f'''CREATE TEMP TABLE result(state STRING,candidate STRING,audit STRING);
BEGIN
BEGIN TRANSACTION;
ASSERT CURRENT_TIMESTAMP()<@expires_at AS 'BOOTSTRAP_EXPIRED';
ASSERT (SELECT COUNT(*) FROM {table('workspaces')} WHERE workspace_id=@workspace_id)=0 AS 'BOOTSTRAP_EXISTS';
ASSERT (SELECT COUNT(*) FROM {table('memberships')} WHERE workspace_id=@workspace_id)=0 AS 'BOOTSTRAP_PARTIAL';
ASSERT (SELECT COUNT(*) FROM {table('conversations')} WHERE workspace_id=@workspace_id)=0 AS 'BOOTSTRAP_PARTIAL';
ASSERT (SELECT COUNT(*) FROM {table('jobs')} WHERE workspace_id=@workspace_id)=0 AS 'BOOTSTRAP_PARTIAL';
{absent}
ASSERT (SELECT COUNT(*) FROM {table('audit_events')} WHERE workspace_id=@workspace_id AND occurred_at BETWEEN
  TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0 AS 'BOOTSTRAP_PARTIAL';
{_insert('workspaces','workspace')}
{_insert('memberships','owner')}
-- PROBE_AFTER_FENCE
{_insert('audit_events','audit')}
INSERT INTO result SELECT 'APPLIED',TO_JSON_STRING({sql_row('workspace','workspaces')}),TO_JSON_STRING({sql_row('audit','audit_events')});
{_finish()}'''


def management_sql(kind,candidate_table,creating):
    require(kind in KINDS-{'BOOTSTRAP'} and candidate_table in {'memberships','workspaces','conversations'})
    role='CREATOR' if kind in {'ARCHIVE','CREATE_CONVERSATION'} else 'ADMIN'
    workspace_state="status IN ('ACTIVE','DISABLED')" if kind=='WORKSPACE_STATUS' else "status='ACTIVE'"
    key='principal_id' if candidate_table=='memberships' else 'conversation_id'
    mutation='' if candidate_table=='workspaces' else (_insert(candidate_table,'candidate') if creating else _update(candidate_table,'candidate',key))
    conversation_check=''
    if candidate_table=='conversations':
        condition="=0" if creating else "=1"
        conversation_check=f"ASSERT (SELECT COUNT(*) FROM {table('conversations')} WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id){condition} AS 'CONVERSATION_CONFLICT';"
        if kind=='ARCHIVE':
            conversation_check+=f"""ASSERT (SELECT TO_JSON_STRING(c) FROM {table('conversations')} c WHERE workspace_id=@workspace_id
              AND conversation_id=@conversation_id)=@conversation_pin AS 'CONVERSATION_CONFLICT';
              ASSERT (SELECT COUNT(*) FROM {table('conversations')} WHERE workspace_id=@workspace_id
              AND conversation_id=@conversation_id AND owner_id=@actor_id AND status='ACTIVE'
              AND access_revision=CAST(JSON_VALUE(PARSE_JSON(@candidate),'$.access_revision') AS INT64)-1)=1 AS 'CONVERSATION_CONFLICT';
              ASSERT (SELECT COUNT(*) FROM {table('jobs')} WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
                AND state IN ('QUEUED','RUNNING','RECOVERY_REQUIRED'))=0 AS 'CONVERSATION_BUSY';"""
    return f'''DECLARE receipt STRING;
CREATE TEMP TABLE result(state STRING,candidate STRING,audit STRING);
BEGIN
BEGIN TRANSACTION;
ASSERT CURRENT_TIMESTAMP()<@assertion_expires_at AND CURRENT_TIMESTAMP()<@end_at AS 'CONTROL_EXPIRED';
ASSERT (SELECT COUNT(*) FROM {table('workspaces')} WHERE workspace_id=@workspace_id)=1 AS 'WORKSPACE_CONFLICT';
ASSERT (SELECT COUNT(*) FROM {table('workspaces')} WHERE workspace_id=@workspace_id AND {workspace_state})=1 AS 'WORKSPACE_DENIED';
ASSERT (SELECT COUNT(*) FROM {table('memberships')} WHERE workspace_id=@workspace_id AND principal_id=@actor_id
  AND identity_issuer=@actor_issuer AND identity_subject=@actor_subject AND status='ACTIVE' AND '{role}' IN UNNEST(roles))=1 AS 'MEMBERSHIP_DENIED';
ASSERT (SELECT COUNT(*) FROM {table('audit_events')} WHERE workspace_id=@workspace_id AND audit_id=@audit_id
  AND occurred_at BETWEEN @start_at AND @end_at)<=1 AS 'AUDIT_CONFLICT';
SET receipt=(SELECT TO_JSON_STRING(a) FROM {table('audit_events')} a WHERE workspace_id=@workspace_id AND audit_id=@audit_id
  AND occurred_at BETWEEN @start_at AND @end_at);
IF receipt IS NOT NULL THEN
  ASSERT receipt=TO_JSON_STRING({sql_row('audit','audit_events')}) AS 'COMMAND_CONFLICT';
  INSERT INTO result SELECT 'REUSED',TO_JSON_STRING({sql_row('candidate',candidate_table)}),receipt;
ELSE
  ASSERT (SELECT TO_JSON_STRING(w) FROM {table('workspaces')} w WHERE workspace_id=@workspace_id)=@workspace_pin AS 'CONTROL_CONFLICT';
  ASSERT TO_JSON_STRING(ARRAY(SELECT TO_JSON_STRING(m) FROM {table('memberships')} m WHERE workspace_id=@workspace_id ORDER BY principal_id))=@members_pin AS 'MEMBERSHIP_CONFLICT';
  {conversation_check}
  {_update('workspaces','workspace','workspace_id')}
  -- PROBE_AFTER_FENCE
  {mutation}
  ASSERT (SELECT COUNT(*) FROM {table('memberships')} WHERE workspace_id=@workspace_id AND status='ACTIVE'
    AND 'ADMIN' IN UNNEST(roles))>=1 AS 'LAST_ADMIN_REQUIRED';
  {_insert('audit_events','audit')}
  INSERT INTO result SELECT 'APPLIED',TO_JSON_STRING({sql_row('candidate',candidate_table)}),TO_JSON_STRING({sql_row('audit','audit_events')});
END IF;
{_finish()}'''
