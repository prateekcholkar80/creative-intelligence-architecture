"""IAP/ADMIN/hash-confirmed DRAFT import of exact clarification Vertex versions.

No ADC-to-user fallback, review-file replay, activation or model invocation.
"""
import argparse,getpass,json
from datetime import datetime,timezone
from pathlib import Path
from threading import RLock
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,ClarificationPolicyRead,read_policy
from src.application.campaign_clarification_prompts import definitions,INTENT_KEY,QUESTION_KEY
from src.application.planner_prompt_registration import PlannerPromptRegistrationOperator,live_operator,review_projection
from src.application.prompt_registration_command import PromptRegistrationCommandError
from src.application.prompt_contract import PromptTask,VertexPromptSnapshot
from src.application.vertex_prompt_reader import VertexPromptReadRequest
from src.application.operation_budget import OperationSettings,operation_scope
from src.application.identity import ASSERTION_HEADER,IapSettings
from src.application.workflow_contract import _require


class ClarificationReviewStore:
    """Process-local issued commands only; never deserialize client review data."""
    def __init__(self):
        self.issued = {}
        self.lock = RLock()


class ClarificationPromptRegistrationOperator(PlannerPromptRegistrationOperator):
    def __init__(self,*,policy_request,policy_reader,settings_snapshot,prompt_key,clock=lambda:datetime.now(timezone.utc),review_store=None,**kwargs):
        _require(type(policy_request) is ClarificationPolicyRequest and callable(policy_reader)
            and type(settings_snapshot) is VertexPromptSnapshot and prompt_key in {INTENT_KEY,QUESTION_KEY},
            'CLARIFICATION_REGISTRATION_COMPOSITION')
        super().__init__(config={},**kwargs)
        self.policy_request,self.policy_reader,self.settings_snapshot,self.prompt_key=policy_request,policy_reader,settings_snapshot,prompt_key
        store = ClarificationReviewStore() if review_store is None else review_store
        _require(type(store) is ClarificationReviewStore,'CLARIFICATION_REVIEW_STORE_INVALID')
        self.clock=clock;self._issued=store.issued;self._lock=store.lock;self._preview_policy_sha=None

    def _policy(self):
        value=self.policy_reader(self.policy_request)
        _require(type(value) is ClarificationPolicyRead and value.request==self.policy_request,
            'CLARIFICATION_REGISTRATION_POLICY_REQUIRED')
        # definitions validates canonical policy/hash and the fresh read window.
        selected=next(d for d in definitions(value,checked_at=self.clock()) if d.prompt_key==self.prompt_key)
        return value,selected

    def _preview(self,request,proposed_by):
        from src.application.prompt_import_preview import PromptImportPreviewRequest
        initial,definition=self._policy()
        _require(request.prompt_key==definition.prompt_key and request.task is PromptTask.INTERPRETER
            and request.output_schema_version==definition.output_schema_version
            and request.safety_policy_version==self.settings_snapshot.safety_policy_version,
            'CLARIFICATION_REGISTRATION_TARGET_MISMATCH')
        preview=self.preview_service.preview(PromptImportPreviewRequest(request,self.clock(),proposed_by))
        expected=definition.snapshot(self.settings_snapshot,vertex_prompt_id=request.prompt_id,vertex_version_id=request.version_id)
        _require(preview.candidate==expected,'CLARIFICATION_REGISTRATION_PARITY_FAILED')
        final,_=self._policy()
        _require(initial.record_sha256==final.record_sha256 and initial.native_job_id!=final.native_job_id,
            'CLARIFICATION_REGISTRATION_POLICY_CHANGED')
        self._preview_policy_sha=final.record_sha256
        return preview

    def inspect(self,request):
        with self._lock:return super().inspect(request)

    def prepare(self,headers,*,workspace_id,request):
        with self._lock:
            _require(len(self._issued)<32,'CLARIFICATION_REGISTRATION_CAPACITY')
            command=super().prepare(headers,workspace_id=workspace_id,request=request)
            self._issued[command.approved_preview_sha256]=(command,self._preview_policy_sha)
            return command

    def register(self,headers,command,*,confirmed_sha256):
        with self._lock:
            issued=self._issued.get(confirmed_sha256)
            _require(issued is not None and issued[0] is command,'CLARIFICATION_REVIEW_NOT_ISSUED')
            current,definition=self._policy()
            _require(current.record_sha256==issued[1],'CLARIFICATION_REGISTRATION_POLICY_CHANGED')
            definition.verify(command.approved_preview.candidate)
            candidate = command.approved_preview.candidate
            _require(candidate == definition.snapshot(self.settings_snapshot,
                vertex_prompt_id=candidate.vertex_prompt_id,vertex_version_id=candidate.vertex_version_id),
                'CLARIFICATION_REGISTRATION_PARITY_FAILED')
            result=super().register(headers,command,confirmed_sha256=confirmed_sha256)
            final,_=self._policy()
            _require(final.record_sha256==issued[1],'CLARIFICATION_REGISTRATION_POLICY_CHANGED_AFTER_COMMIT')
            return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--key',choices=(INTENT_KEY,QUESTION_KEY),required=True);p.add_argument('--prompt-id',required=True)
    p.add_argument('--version-id',required=True);p.add_argument('--policy-id',required=True)
    p.add_argument('--policy-version',required=True);p.add_argument('--policy-sha256',required=True)
    p.add_argument('--settings-prompt-id',required=True);p.add_argument('--settings-version-id',required=True)
    p.add_argument('--apply',action='store_true');p.add_argument('--workspace-id');p.add_argument('--review-file',type=Path)
    args=p.parse_args();settings=OperationSettings(120)
    if args.apply and not args.workspace_id:p.error('--apply requires --workspace-id')
    iap=IapSettings.from_environment() if args.apply else None
    headers={ASSERTION_HEADER:getpass.getpass('Fresh IAP assertion (never exported): ')} if args.apply else None
    request=ClarificationPolicyRequest(args.policy_id,args.policy_version,args.policy_sha256)
    with live_operator({},iap_settings=iap,execution_enabled=args.apply) as base:
        reader=base.preview_service.registry_reader
        with operation_scope(settings):
            model_settings=reader.read_exact(args.settings_prompt_id,args.settings_version_id).state.head.snapshot
            operator=ClarificationPromptRegistrationOperator(policy_request=request,
                policy_reader=lambda r:read_policy(reader.client,r),settings_snapshot=model_settings,prompt_key=args.key,
                preview_service=base.preview_service,command_service=base.command_service,identity_adapter=base.identity_adapter)
            policy=read_policy(reader.client,request)
            definition=next(d for d in definitions(policy,checked_at=datetime.now(timezone.utc)) if d.prompt_key==args.key)
            vertex_request=VertexPromptReadRequest(args.key,PromptTask.INTERPRETER,args.prompt_id,args.version_id,
                definition.output_schema_version,model_settings.safety_policy_version)
            if args.apply:
                command=operator.prepare(headers,workspace_id=args.workspace_id,request=vertex_request);preview=command.approved_preview
            else:preview=operator.inspect(vertex_request)
        review=review_projection(preview,authorized=args.apply);review['event']='clarification_prompt_registration_review'
        print(json.dumps(review,indent=2,default=str))
        if args.review_file:
            args.review_file.parent.mkdir(parents=True,exist_ok=True);args.review_file.write_text(json.dumps(review,indent=2,default=str)+'\n')
        if args.apply:
            # Deliberate human confirmation outside provider operation budgets.
            confirmed=input('Confirm the displayed preview SHA-256: ').strip()
            with operation_scope(settings):result=operator.register(headers,command,confirmed_sha256=confirmed)
            print(json.dumps(dict(native_job_id=result.native_job_id,status=result.registered.state.head.status.value,
                snapshot_sha256=result.registered.state.head.snapshot.snapshot_sha256)))

if __name__=='__main__':main()
