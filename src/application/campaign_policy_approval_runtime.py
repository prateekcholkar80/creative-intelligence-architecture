"""Current policy review bindings around question quality checks; never activation."""
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from threading import RLock
from uuid import uuid4
from src.application.campaign_policy_approval_reader import PolicyApprovalReader
from src.application.campaign_policy_approval_receipt import CurrentPolicyApproval
from src.application.campaign_clarification_policy import ClarificationPolicyRequest, read_policy
from src.application.campaign_quality_execution import QualityExecutionResult
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import _require, _digest, canonical_json_bytes
from src.application.operation_budget import require_owned_worker


@dataclass(frozen=True, slots=True)
class ApprovalCheckpoint:
    nonce: str
    approvals: tuple[CurrentPolicyApproval, ...]
    clarification: object


class PolicyApprovalRuntimeGuard:
    def __init__(self, *, acceptance_reader, clarification_reader, clock=lambda: datetime.now(timezone.utc)):
        _require(type(acceptance_reader) is PolicyApprovalReader and type(clarification_reader) is PolicyApprovalReader
            and acceptance_reader.pins.kind == 'ACCEPTANCE' and clarification_reader.pins.kind == 'CLARIFICATION'
            and acceptance_reader.pins.workspace_id == clarification_reader.pins.workspace_id
            and acceptance_reader.pins.stage == clarification_reader.pins.stage == 'CLARIFICATION',
            'POLICY_APPROVAL_RUNTIME_CONFIGURATION')
        self.readers=(acceptance_reader,clarification_reader);self.clock=clock
        self._pending={};self._lock=RLock()
        pins=clarification_reader.pins
        self.policy_request=ClarificationPolicyRequest(pins.policy_id,pins.policy_version,pins.policy_sha256)

    def capture(self, headers):
        require_owned_worker()
        with self._lock:
            _require(len(self._pending)<32,'POLICY_APPROVAL_RUNTIME_CAPACITY')
            approvals=tuple(reader.read(headers) for reader in self.readers)
            policy=read_policy(self.readers[1].client,self.policy_request,clock=self.clock)
            token=ApprovalCheckpoint(uuid4().hex,approvals,policy)
            self._pending[token.nonce]=token
            return token

    def discard(self, token):
        with self._lock:
            if type(token) is ApprovalCheckpoint and self._pending.get(token.nonce) is token:
                del self._pending[token.nonce]

    def bind(self, headers, token, quality):
        require_owned_worker()
        with self._lock:
            _require(type(token) is ApprovalCheckpoint and self._pending.get(token.nonce) is token
                and type(quality) is QualityExecutionResult,'POLICY_APPROVAL_RUNTIME_CHECKPOINT')
            del self._pending[token.nonce]  # Consumed before any further I/O.
        current=tuple(reader.read(headers) for reader in self.readers)
        policy=read_policy(self.readers[1].client,self.policy_request,clock=self.clock)
        _require(type(quality.context_canonical_json) is str and len(quality.context_canonical_json.encode())<=65536,
            'POLICY_APPROVAL_RUNTIME_CONTEXT_SIZE')
        context=json.loads(quality.context_canonical_json)
        _require(canonical_json_bytes(context).decode()==quality.context_canonical_json
            and _hash(context)==quality.context_sha256
            and context['version']=='campaign-quality-authoritative-execution-v1'
            and context['report_sha256']==quality.report.sha256,'POLICY_APPROVAL_RUNTIME_CONTEXT_CHANGED')
        now=self.clock();request=context['request'];sources=context['sources']
        bindings=[]
        for reader,before,after in zip(self.readers,token.approvals,current):
            _require(type(before) is CurrentPolicyApproval and type(after) is CurrentPolicyApproval
                and before.receipt==after.receipt and before.receipt.pins==reader.pins
                and before.native_job_id!=after.native_job_id
                and before.observed_at<=after.observed_at<=now<min(before.valid_until,after.valid_until),
                'POLICY_APPROVAL_RUNTIME_AUTHORITY_CHANGED')
            _digest(before.publication_sha256);_digest(after.publication_sha256)
            _require(before.publication_sha256==after.publication_sha256,
                'POLICY_APPROVAL_RUNTIME_PUBLICATION_CHANGED')
            pins=after.receipt.pins
            _require(pins.workspace_id==request['workspace_id'] and pins.stage==request['stage']
                and pins.prompt_snapshot_sha256==sources['prompt']['snapshot_sha256']
                and pins.model_profile_sha256==sources['model_profile_sha256'], 'POLICY_APPROVAL_RUNTIME_PINS_CHANGED')
            if pins.kind=='ACCEPTANCE':
                _require(pins.policy_id==sources['policy']['policy_id'] and pins.policy_version==sources['policy']['version']
                    and pins.policy_sha256==sources['policy']['snapshot_sha256'], 'POLICY_APPROVAL_RUNTIME_ACCEPTANCE_CHANGED')
            bindings.append(dict(kind=pins.kind,receipt_id=after.receipt.receipt_id,receipt_sha256=after.receipt.sha256,
                authority_id=after.receipt.authority_id,authority_sha256=after.receipt.authority_sha256,
                pins_sha256=pins.sha256,publication_sha256=after.publication_sha256,native_job_ids=[before.native_job_id,after.native_job_id]))
        _require(token.clarification.request==policy.request==self.policy_request
            and token.clarification.policy_canonical_json==policy.policy_canonical_json
            and token.clarification.record_sha256==policy.record_sha256
            and token.clarification.native_job_id!=policy.native_job_id
            and token.clarification.observed_at<=policy.observed_at<=now<min(token.clarification.valid_until,policy.valid_until),
            'POLICY_APPROVAL_RUNTIME_CLARIFICATION_CHANGED')
        context['policy_review_bindings']=dict(version='campaign-policy-review-runtime-v1',receipts=bindings,
            clarification_native_job_ids=[token.clarification.native_job_id,policy.native_job_id],
            qualifying_evaluation_verified=False,release_allowed=False)
        encoded=canonical_json_bytes(context).decode()
        _require(len(encoded.encode())<=65536,'POLICY_APPROVAL_RUNTIME_CONTEXT_SIZE')
        return QualityExecutionResult(quality.report,encoded,_hash(context))
