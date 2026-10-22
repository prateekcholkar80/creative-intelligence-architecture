"""Release-disabled marketing pilot; IAP-owned setup and bounded private hosts."""
import atexit,hashlib,json
from threading import RLock
from google.api_core.exceptions import NotFound
from src.application.identity import IapIdentityAdapter,IapSettings,ensure_fresh
from src.application.management_service import ProtectedManagementService,ManagementSettings,digest
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import _require
from src.application.workflow_submission_service import SubmissionSettings
from src.application.workflow_submission_host import WorkflowSubmissionConfig,WorkflowSubmissionHost
from src.application.workflow_history_service import HistorySettings
from src.application.workflow_history_host import WorkflowHistoryConfig,WorkflowHistoryHost
from src.application.workflow_read_host import WorkflowReadConfig,WorkflowReadHost
from src.application.sharing_operation_host import SharingHostSettings
from src.application.conversation_contract import validate_conversation
from src.ui.workflow_runtime import WorkflowUiRuntime,WorkflowUiSettings
from src.application.operation_budget import OperationSettings


DEFAULT_CONVERSATION='default'
EVALUATION_CONVERSATION='rejuvenate-evaluation-v1'


def conversation_id(principal_id,context=DEFAULT_CONVERSATION):
    _require(context in (DEFAULT_CONVERSATION,EVALUATION_CONVERSATION),'MARKETING_CONVERSATION_CONTEXT')
    value=principal_id if context==DEFAULT_CONVERSATION else principal_id+'|'+context
    return 'marketing-'+hashlib.sha256(value.encode()).hexdigest()[:32]


class MarketingPilot:
    def __init__(self,*,client,manifest,cursor_key):
        _require(type(manifest) is dict and set(manifest)=={'iap_audience','workspace_id','submission','interpreter_runtime','release_enabled'}
            and manifest['release_enabled'] is False and manifest['interpreter_runtime']['enabled'] is False,'MARKETING_RELEASE_MUST_BE_DISABLED')
        _require(type(cursor_key) is bytes and len(cursor_key)==32,'MARKETING_CURSOR_KEY_REQUIRED')
        self.client,self.manifest,self.cursor_key=client,manifest,cursor_key
        self.verifier=IapIdentityAdapter(IapSettings(manifest['iap_audience']))
        self.management=ProtectedManagementService(client=client,verifier=self.verifier,settings=ManagementSettings(enabled=True))
        self._lock=RLock();self._runtimes={};self._plans={}
        atexit.register(self.close)

    def inspect(self,headers,context=DEFAULT_CONVERSATION):
        actor=self.verifier.authenticate(headers);ensure_fresh(actor)
        workspace=self.manifest['workspace_id'];conv=conversation_id(actor.principal.principal_id,context)
        row,ws,members=self.management._snapshot(workspace,conv)
        _require(ws['status']=='ACTIVE','MARKETING_WORKSPACE_DISABLED')
        member=next((m for m in members if m['principal_id']==actor.principal.principal_id),None)
        _require(member is not None and member['status']=='ACTIVE' and member['identity_issuer']==actor.issuer
            and member['identity_subject']==actor.subject,'MARKETING_MEMBERSHIP_REQUIRED')
        _require(len(row['conversations'])<=1,'MARKETING_CONVERSATION_AMBIGUOUS')
        conversation=decode_row(row['conversations'][0],'conversations') if row['conversations'] else None
        if conversation is not None:
            resource=validate_conversation(conversation,workspace_id=workspace,conversation_id=conv)
            _require(resource.owner_id==actor.principal.principal_id and conversation['status']=='ACTIVE','MARKETING_CONVERSATION_DENIED')
        ensure_fresh(actor)
        return actor,ws,member,conversation,conv

    def setup(self,headers,action,context=DEFAULT_CONVERSATION):
        _require(action in {'CREATOR','CONVERSATION'},'MARKETING_SETUP_ACTION')
        with self._lock:
            actor,ws,member,conversation,conv=self.inspect(headers,context=context)
            if action=='CREATOR':
                _require('ADMIN' in member['roles'],'MARKETING_ADMIN_REQUIRED')
                if 'CREATOR' in member['roles']:return 'ALREADY_PRESENT'
                extra=dict(kind='MEMBER',target_subject=actor.subject,roles=tuple(sorted(set(member['roles'])|{'CREATOR'})),
                    status='ACTIVE',expected_revision=member['revision'])
            else:
                _require('CREATOR' in member['roles'],'MARKETING_CREATOR_REQUIRED')
                if conversation is not None:return 'ALREADY_PRESENT'
                extra=dict(kind='CREATE_CONVERSATION',conversation_id=conv)
            command='marketing-'+action.lower()+'-v1'
            if action=='CONVERSATION' and context!=DEFAULT_CONVERSATION:command+='-'+context
            key=(actor.principal.principal_id,action,context)
            plan=self._plans.get(key)
            if plan is None:
                _require(len(self._plans)<32,'MARKETING_SETUP_CAPACITY')
                native_id='management_v1_'+digest([self.client.project,ws['workspace_id'],actor.principal.principal_id,command])
                try:self.management.io.original_job(native_id)
                except NotFound:
                    plan=self.management.prepare(headers,workspace_id=ws['workspace_id'],command_id=command,
                        expected_workspace_revision=ws['revision'],**extra)
                    self._plans[key]=plan
                else:
                    plan=self.management.original_plan(headers,workspace_id=ws['workspace_id'],command_id=command)
                    return self.management.recover(headers,plan).native_status.value
            result=self.management.submit(headers,plan)
            self.inspect(headers,context=context)
            return result.native_status.value

    def runtime(self,headers,context=DEFAULT_CONVERSATION):
        with self._lock:
            actor,ws,member,conversation,conv=self.inspect(headers,context=context)
            _require('CREATOR' in member['roles'] and conversation is not None,'MARKETING_SETUP_REQUIRED')
            key=(actor.principal.principal_id,conv)
            if key in self._runtimes:
                runtime,hosts=self._runtimes[key]
                _require(all(not h.retired for h in hosts),'MARKETING_HOST_RETIRED')
                return runtime
            _require(len(self._runtimes)<2,'MARKETING_RUNTIME_CAPACITY')
            raw=dict(self.manifest['submission']);raw['model_profiles']=tuple(raw['model_profiles'])
            raw['operation']=OperationSettings(**raw.get('operation',{}));submission=SubmissionSettings(**raw)
            iap=self.verifier.settings;settings=SharingHostSettings(enabled=True)
            hosts=(WorkflowSubmissionHost(WorkflowSubmissionConfig(iap,submission,True,self.manifest['interpreter_runtime']),settings),
                WorkflowHistoryHost(WorkflowHistoryConfig(iap,HistorySettings(enabled=True,cursor_key=self.cursor_key)),settings),
                WorkflowReadHost(WorkflowReadConfig(iap,enabled=True),settings))
            try:
                for host in hosts:host.start()
                from src.application.campaign_attachment_composition import compose_attachments
                attachments=compose_attachments(client=self.client,verifier=self.verifier)
                runtime=WorkflowUiRuntime(*hosts,WorkflowUiSettings(ws['workspace_id'],conv),attachment_service=attachments)
            except BaseException:
                for host in reversed(hosts):host.close()
                raise
            self._runtimes[key]=(runtime,hosts)
            return runtime

    def reconnect(self,headers,context=DEFAULT_CONVERSATION):
        with self._lock:
            actor,_,_,_,conv=self.inspect(headers,context=context)
            item=self._runtimes.pop((actor.principal.principal_id,conv),None)
            if item:
                for host in reversed(item[1]):host.close()
            # Fresh hosts never replay an earlier submission or model operation.
            return self.runtime(headers,context=context)

    def close(self):
        with self._lock:
            for _,hosts in self._runtimes.values():
                for host in reversed(hosts):host.close()
            self._runtimes.clear()
