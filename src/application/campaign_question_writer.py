"""Trusted owned-worker child writer; disabled by default, never releases text."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime,timezone
from threading import RLock
import hashlib,json
from google.api_core.exceptions import NotFound
from src.application.identity import IapIdentityAdapter,ensure_fresh
from src.application.operation_budget import require_owned_worker
from src.application.workflow_submission_service import ProtectedSubmissionService
from src.application.control_io import ControlIO
from src.application.workflow_contract import _require,_id,validate_profile,retry_key_hash
from src.application.workflow_codec import decode_row
from src.application.campaign_quality_authority_reader import read_execution_authority
from src.application.campaign_transcript_history import read_history
from src.application.campaign_evidence_conversation import read_bound_gaps
from src.application.campaign_question_submission import prepare_question_submission
from src.application.campaign_question_context_reader import read_question_context
from src.application.campaign_question_context import verify_transition,QuestionContextRead,QuestionContextTransition
from src.application.campaign_evidence_conversation import BoundGapRead
from src.application.interpreter_submission import InterpreterSubmissionReceipt
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status


@dataclass(frozen=True,slots=True)
class QuestionAdmissionContext:
    plan:object
    initial_gaps:BoundGapRead
    admitted:QuestionContextRead
    proof:QuestionContextTransition
    @property
    def release_allowed(self):return False


class ProtectedQuestionWriter:
    def __init__(self,*,client,verifier,access_reader,policy_request,profile,worker_id,
                 enabled=False,clock=lambda:datetime.now(timezone.utc)):
        _require(isinstance(verifier,IapIdentityAdapter) and type(enabled) is bool,'QUESTION_WRITER_COMPOSITION')
        validate_profile(profile);_id(worker_id)
        _require(profile['stage']=='INTERPRETER' and profile['output_schema_version']=='campaign-evidence-questions-v1',
            'QUESTION_WRITER_PROFILE')
        self.client,self.verifier,self.access_reader=client,verifier,access_reader
        self.policy_request,self.profile,self.worker_id=policy_request,deepcopy(profile),worker_id
        self.enabled,self.clock=enabled,clock
        self.reader=ProtectedSubmissionService(client=client,verifier=verifier)
        self.io=ControlIO(client);self._lock=RLock();self._plans={};self._prepared={};self._contexts={}

    def _prepare(self,actor,workspace_id,conversation_id,parent_request_id):
        before=self.reader._snapshot(actor,workspace_id,conversation_id)
        args=dict(identity=actor,workspace_id=workspace_id,conversation_id=conversation_id,
            request_id=parent_request_id,clock=self.clock)
        parent=read_execution_authority(self.client,**args)
        history=read_history(self.client,identity=actor,workspace_id=workspace_id,
            conversation_id=conversation_id,conversation=before[3],clock=self.clock)
        _require(history.turns and history.turns[-1]['request_id']==parent_request_id
            and history.turns[-1]['role']=='ASSISTANT','QUESTION_PARENT_NOT_LATEST_COMPLETE_TURN')
        users=[text for turn,text in zip(history.turns,history.texts)
            if turn['request_id']==parent_request_id and turn['role']=='USER']
        _require(len(users)==1,'QUESTION_PARENT_MESSAGE_REQUIRED')
        gaps=read_bound_gaps(self.client,self.policy_request,identity=actor,access_reader=self.access_reader,
            workspace_id=workspace_id,conversation_id=conversation_id,request_id=parent_request_id,
            observed_interpreter_response=history.texts[-1],clock=self.clock)
        final=read_execution_authority(self.client,**args)
        after=self.reader._snapshot(actor,workspace_id,conversation_id);ensure_fresh(actor)
        _require(before[0]<=parent.observed_at<=history.observed_at<=final.observed_at<=after[0]
            and before[1:5]==after[1:5] and parent.content_sha256==final.content_sha256
            and parent.native_job_id!=final.native_job_id
            and self.clock()<min(parent.valid_until,final.valid_until,gaps.valid_until),
            'QUESTION_WRITER_CONTEXT_CHANGED')
        authority=json.loads(parent.content_canonical_json)['authority']
        self.io.preflight()
        # Additional raw-body/authority/binding tables are independently preflighted
        # by their bounded readers; the owned native probe verifies this plan's SQL.
        plan=prepare_question_submission(parent=parent,gaps=gaps,history=history,
            selection={k:authority[k] for k in ('sku','country','channel','audience')},workspace=after[1],
            membership=after[2],conversation=after[3],resource=after[4],principal_id=actor.principal.principal_id,
            message=users[0],retry_key='question-parent-v1-'+hashlib.sha256(parent_request_id.encode()).hexdigest(),
            profile=self.profile,observed_at=after[0],worker_id=self.worker_id)
        before_context=read_question_context(self.client,identity=actor,access_reader=self.access_reader,
            workspace_id=workspace_id,conversation_id=conversation_id,clock=self.clock)
        _require(self.clock()<min(parent.valid_until,final.valid_until,gaps.valid_until),
            'QUESTION_WRITER_CONTEXT_CHANGED')
        self._prepared[plan.job_id]=(before_context,gaps)
        return plan

    def submit(self,headers,*,workspace_id,conversation_id,parent_request_id):
        require_owned_worker();_require(self.enabled,'QUESTION_WRITER_DISABLED')
        for value in (workspace_id,conversation_id,parent_request_id):_id(value)
        with self._lock:
            actor=self.verifier.authenticate(headers);ensure_fresh(actor)
            self.reader._snapshot(actor,workspace_id,conversation_id)
            retry='question-parent-v1-'+hashlib.sha256(parent_request_id.encode()).hexdigest()
            key=retry_key_hash(workspace_id=workspace_id,principal_id=actor.principal.principal_id,
                conversation_id=conversation_id,operation='INTERPRET',retry_key=retry)
            plan=self._plans.get(key)
            if plan is None:
                _require(len(self._plans)<32,'QUESTION_WRITER_CAPACITY')
                try:self.io.original_job('interpreter_submit_v1_'+key)
                except NotFound:
                    plan=self._prepare(actor,workspace_id,conversation_id,parent_request_id)
                    _require(plan.job_id=='interpreter_submit_v1_'+key,'QUESTION_WRITER_SLOT_CHANGED')
                    self._plans[key]=plan
                    outcome=self.io.submit_once(plan)
                else:
                    # A cold process cannot recreate timestamps/configuration or
                    # remint a child. It needs the exact original trusted plan.
                    _require(False,'QUESTION_ORIGINAL_PLAN_RECOVERY_REQUIRED')
            else:outcome=self.io.recover(plan)
            self.reader._snapshot(actor,workspace_id,conversation_id);ensure_fresh(actor)
            params={p.name:p.value for p in plan.config().query_parameters}
            request=decode_row(params['request_candidate'],'requests')
            invocation=decode_row(params['invocation_candidate'],'model_invocations')
            if outcome.native_status is Status.COMMITTED:
                _require(len(outcome.rows)==1,'QUESTION_WRITER_RECEIPT_UNVERIFIED')
                row=outcome.rows[0]
                _require(row.get('state') in {'APPLIED','REUSED'}
                    and decode_row(row['request_row'],'requests')==request
                    and decode_row(row['invocation_row'],'model_invocations')==invocation
                    and row.get('authority_sha256')==params['authority_digest'],
                    'QUESTION_WRITER_RECEIPT_UNVERIFIED')
                actual=json.loads(row['transcript_row']);expected=json.loads(params['transcript_candidate'])
                for value in (actual,expected):value['recorded_at']=datetime.fromisoformat(value['recorded_at'].replace('Z','+00:00'))
                _require(actual==expected,'QUESTION_WRITER_RECEIPT_UNVERIFIED')
                if plan.job_id not in self._contexts:
                    _require(plan.job_id in self._prepared,'QUESTION_ADMISSION_CONTEXT_UNAVAILABLE')
                    before_context,gaps=self._prepared[plan.job_id]
                    after_context=read_question_context(self.client,identity=actor,access_reader=self.access_reader,
                        workspace_id=workspace_id,conversation_id=conversation_id,clock=self.clock)
                    proof=verify_transition(before_context,after_context,plan,phase='ADMITTED',checked_at=self.clock())
                    self._contexts[plan.job_id]=QuestionAdmissionContext(plan,gaps,after_context,proof)
            return InterpreterSubmissionReceipt(workspace_id,conversation_id,request['request_id'],
                invocation['invocation_id'],plan.job_id,outcome.native_status.value)

    def admission_context(self,headers,receipt):
        """Trusted same-process handoff, never a serialized authorization token."""
        require_owned_worker();_require(self.enabled,'QUESTION_WRITER_DISABLED')
        _require(type(receipt) is InterpreterSubmissionReceipt and receipt.native_status=='COMMITTED',
            'QUESTION_ADMISSION_CONTEXT_UNAVAILABLE')
        with self._lock:
            actor=self.verifier.authenticate(headers);ensure_fresh(actor)
            self.reader._snapshot(actor,receipt.workspace_id,receipt.conversation_id)
            value=self._contexts.get(receipt.native_job_id)
            _require(type(value) is QuestionAdmissionContext,'QUESTION_ADMISSION_CONTEXT_UNAVAILABLE')
            p={v.name:v.value for v in value.plan.config().query_parameters}
            request=decode_row(p['request_candidate'],'requests');call=decode_row(p['invocation_candidate'],'model_invocations')
            _require(request['principal_id']==actor.principal.principal_id
                and request['workspace_id']==receipt.workspace_id and request['conversation_id']==receipt.conversation_id
                and request['request_id']==receipt.request_id and call['invocation_id']==receipt.invocation_id
                and self.clock()<min(value.admitted.valid_until,value.initial_gaps.valid_until),
                'QUESTION_ADMISSION_CONTEXT_CHANGED_OR_EXPIRED')
            return value
