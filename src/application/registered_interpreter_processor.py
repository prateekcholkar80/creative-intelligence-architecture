"""Request-scoped registered interpreter execution in an owned serving worker."""
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
import json
from src.application.workflow_contract import _require
from src.application.operation_budget import require_owned_worker
from src.application.campaign_clarification_policy import read_policy
from src.application.campaign_clarification_prompts import definitions
from src.application.prompt_runtime_service import PromptRuntimeService
from src.application.prompt_runtime_reader import BigQueryPromptRuntimeReader
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.campaign_chat_evidence_service import ProtectedEvidenceChatService
from src.application.generation_admission import AdmissionSettings,ProtectedGenerationAdmission
from src.application.interpreter_outcome import ProtectedInterpreterOutcomeService
from src.application.interpreter_worker import InterpreterWorker
from src.application.campaign_quality_authority_reader import read_execution_authority
from src.application.campaign_acceptance_contract import _hash

@dataclass(frozen=True,slots=True)
class InterpreterProcessingResult:
    state:str
    reason:str|None=None
    @property
    def release_allowed(self):return False


class RegisteredInterpreterProcessor:
    def __init__(self,*,client,verifier,access_reader,policy_request,targets,vertex_transport,
                 provider_transport,profile,worker_id,enabled=False,clock=lambda:datetime.now(timezone.utc)):
        _require(type(enabled) is bool,'INTERPRETER_PROCESSOR_CONFIG')
        self.client,self.verifier,self.access_reader=client,verifier,access_reader
        self.policy_request,self.targets=policy_request,targets
        self.vertex_transport,self.provider_transport=vertex_transport,provider_transport
        self.profile,self.worker_id,self.enabled,self.clock=profile,worker_id,enabled,clock
        self._workers={}

    def process(self,headers,receipt):
        require_owned_worker()
        if not self.enabled:return InterpreterProcessingResult('HELD','INTERPRETER_PROCESSOR_DISABLED')
        _require(receipt.native_status=='COMMITTED','INTERPRETER_SUBMISSION_NOT_COMMITTED')
        key=(receipt.workspace_id,receipt.conversation_id,receipt.request_id,receipt.invocation_id)
        if key in self._workers:
            worker,plan=self._workers[key]
            worker.recover(headers,plan.job_id)
            return InterpreterProcessingResult('RECOVERED')
        _require(len(self._workers)<32,'INTERPRETER_PROCESSOR_CAPACITY')
        actor=self.verifier.authenticate(headers)
        authority=read_execution_authority(self.client,identity=actor,workspace_id=receipt.workspace_id,
            conversation_id=receipt.conversation_id,request_id=receipt.request_id,clock=self.clock)
        data=json.loads(authority.content_canonical_json)
        calls=[c for c in data['calls'] if c['invocation_id']==receipt.invocation_id]
        _require(len(calls)==1 and calls[0]['profile']==self.profile,'INTERPRETER_ORIGINAL_DISPATCH_REQUIRED')
        state=calls[0]['state']
        if state in {'SUCCEEDED','FAILED','BLOCKED'}:
            if state=='SUCCEEDED':
                from src.application.campaign_response_transcript import verify_stored_response
                verify_stored_response(self.client,calls[0],clock=self.clock)
            final=read_execution_authority(self.client,identity=actor,workspace_id=receipt.workspace_id,
                conversation_id=receipt.conversation_id,request_id=receipt.request_id,clock=self.clock)
            _require(final.content_sha256==authority.content_sha256 and final.native_job_id!=authority.native_job_id
                and self.clock()<min(final.valid_until,authority.valid_until),'INTERPRETER_RECOVERY_CONTEXT_CHANGED')
            return InterpreterProcessingResult(state+'_STORED','READ_ONLY_STORED_STATE_NOT_NATIVE_RECEIPT')
        if state!='PLANNED':
            return InterpreterProcessingResult('HELD','INTERPRETER_OUTCOME_RECONCILIATION_REQUIRED')
        policy=read_policy(self.client,self.policy_request,clock=self.clock)
        if not policy.approval_verified or json.loads(policy.policy_canonical_json)['status']!='APPROVED':
            return InterpreterProcessingResult('HELD','INTERPRETER_POLICY_REVIEW_REQUIRED')
        definition=definitions(policy,checked_at=self.clock())[0]
        target=next((t for t in self.targets if t[0]==definition.prompt_key),None)
        _require(target is not None,'INTERPRETER_REGISTERED_TARGET_REQUIRED')
        registry=BigQueryPromptRegistryReader(self.client)
        snapshot=registry.read_exact(target[1],target[2]).state.head.snapshot
        definition.verify(snapshot)
        # Do not spend a model call on an input the exact registered schema cannot represent.
        schema=json.loads(definition.output_schema_json)['properties']
        audience_enum=schema['audience'].get('enum')
        if audience_enum and data['authority']['audience'] not in audience_enum:
            return InterpreterProcessingResult('HELD','INTERPRETER_AUDIENCE_SCHEMA_MISMATCH')
        evidence=ProtectedEvidenceChatService(client=self.client,verifier=self.verifier,access_reader=self.access_reader,
            registry_reader=registry,policy_request=self.policy_request,targets=self.targets,clock=self.clock)
        adapter=evidence.intent_adapter(headers,workspace_id=receipt.workspace_id,conversation_id=receipt.conversation_id,
            request_id=receipt.request_id,transport=lambda profile,system,prompt,schema:self.provider_transport(profile,system,prompt,schema,snapshot))
        pins=frozenset({_hash(self.profile)})
        runtime=PromptRuntimeService(identity_adapter=self.verifier,access_reader=self.access_reader,
            reader=BigQueryPromptRuntimeReader(self.client),vertex_transport=self.vertex_transport,
            behaviors=(definition.behavior(snapshot),),approved_profiles=pins,enabled=True)
        admission=ProtectedGenerationAdmission(client=self.client,verifier=self.verifier,worker_id=self.worker_id,
            settings=AdmissionSettings(profile_sha256=pins,enabled=True,require_registered_prompts=True),
            model_call=adapter,prompt_runtime=runtime)
        publisher=ProtectedInterpreterOutcomeService(client=self.client,verifier=self.verifier,
            access_reader=self.access_reader,worker_id=self.worker_id,enabled=True)
        worker=InterpreterWorker(admission=admission,publisher=publisher,enabled=True,clock=self.clock)
        started=datetime.fromisoformat(data['request_record']['created_at'].replace('Z','+00:00'))
        plan=admission.prepare(headers,workspace_id=receipt.workspace_id,conversation_id=receipt.conversation_id,
            request_id=receipt.request_id,invocation_id=receipt.invocation_id,start_at=started-timedelta(seconds=1),
            end_at=self.clock()+timedelta(minutes=5))
        # Retain before dispatch. Reconnect only recovers this operation; never resubmits a model call.
        self._workers[key]=(worker,plan)
        result=worker.run(headers,plan)
        return InterpreterProcessingResult(result.outcome.native_status.value)
