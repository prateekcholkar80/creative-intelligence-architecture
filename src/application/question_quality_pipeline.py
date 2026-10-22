"""Authoritative post-publication question checking and private view staging."""
from datetime import timedelta
import json

from src.application.campaign_acceptance_contract import Stage
from src.application.campaign_quality_authority_reader import read_execution_authority
from src.application.campaign_quality_bound_execution import run_authoritative_quality
from src.application.campaign_quality_checks import QualityCandidate
from src.application.campaign_quality_execution import QualityRequest,QualityExecutionSettings
from src.application.campaign_question_projection import bind_question_output
from src.application.campaign_question_view import QuestionTurn
from src.application.workflow_contract import _require


class QuestionQualityPublisher:
    """Trusted callback, not an endpoint; failed/unavailable checks cannot release."""
    def __init__(self,*,client,verifier,access_reader,runtime_reader,settings,evidence,clock,approval_guard=None):
        _require(type(settings) is QualityExecutionSettings,'QUESTION_QUALITY_SETTINGS_REQUIRED')
        self.client,self.verifier,self.access_reader=client,verifier,access_reader
        self.runtime_reader,self.settings,self.evidence,self.clock=runtime_reader,settings,evidence,clock
        if approval_guard is not None:
            from src.application.campaign_policy_approval_runtime import PolicyApprovalRuntimeGuard
            _require(type(approval_guard) is PolicyApprovalRuntimeGuard,'QUESTION_APPROVAL_GUARD_REQUIRED')
        self.approval_guard=approval_guard

    def __call__(self,headers,receipt,context,result):
        _require(result.outcome.native_status.value=='COMMITTED' and result.draft is not None,
            'QUESTION_QUALITY_COMMITTED_RESULT_REQUIRED')
        identity,_=self.evidence._identity(headers,receipt.workspace_id,receipt.conversation_id)
        capture=read_execution_authority(self.client,identity=identity,workspace_id=receipt.workspace_id,
            conversation_id=receipt.conversation_id,request_id=receipt.request_id,clock=self.clock)
        content=json.loads(capture.content_canonical_json)
        authority,request=content['authority'],content['request_record']
        calls=[c for c in content['calls'] if c['invocation_id']==receipt.invocation_id]
        _require(len(calls)==1,'QUESTION_QUALITY_INVOCATION_REQUIRED')
        projection,_=bind_question_output(context.initial_gaps.plan,result.draft.response_text,
            authority=authority,request=request,invocation=calls[0],profile=calls[0]['profile'],
            binding_sha256=result.draft.binding_sha256)
        quality_request=QualityRequest(receipt.workspace_id,receipt.conversation_id,receipt.request_id,
            authority['sku'],Stage.CLARIFICATION,authority['country'],authority['channel'],
            request['message_sha256'],content['transcript_sha256'])
        candidate=QualityCandidate(receipt.request_id,Stage.CLARIFICATION,authority['sku'],
            authority['country'],authority['channel'],projection.text,())
        sources=[]
        def build(fact):
            sources.append(fact)
            return candidate,result.draft.response_text
        guard=getattr(self,'approval_guard',None)
        checkpoint=guard.capture(headers) if guard is not None else None
        try:
            quality=run_authoritative_quality(quality_request,build,settings=self.settings,client=self.client,
                runtime_reader=self.runtime_reader,access_reader=self.access_reader,identity=identity,
                clock=self.clock,question_plan=context.initial_gaps.plan)
            if guard is not None:quality=guard.bind(headers,checkpoint,quality)
        finally:
            if guard is not None:guard.discard(checkpoint)
        turn=QuestionTurn(result.draft,candidate,quality,min(quality.report.checked_at+timedelta(seconds=30),
            context.initial_gaps.valid_until),tuple(sources))
        self.evidence.stage_questions(turn)
