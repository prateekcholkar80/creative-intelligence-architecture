"""Owned serving question pipeline. Configuration and registration never approve."""
from datetime import datetime, timezone
import json
from threading import RLock

from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_policy_approval_runtime import PolicyApprovalRuntimeGuard
from src.application.campaign_chat_evidence_service import ProtectedEvidenceChatService
from src.application.campaign_clarification_policy import read_policy
from src.application.campaign_clarification_prompts import definitions, QUESTION_KEY
from src.application.campaign_question_worker import RegisteredQuestionWorker, QuestionWorkerStatus
from src.application.campaign_question_writer import ProtectedQuestionWriter
from src.application.campaign_question_context_reader import read_question_context
from src.application.campaign_response_transcript import verify_stored_response
from src.application.workflow_codec import decode_row
from src.application.operation_budget import require_owned_worker
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.prompt_runtime_reader import BigQueryPromptRuntimeReader
from src.application.prompt_runtime_service import PromptRuntimeService
from src.application.workflow_contract import _require, validate_profile


class ServingQuestions:
    """Lazily bind the exact registered question version inside the owned host.

    No model or registry I/O occurs during process bootstrap. The actual runtime
    revalidates lifecycle/evaluation eligibility at dispatch; DRAFT cannot execute.
    """
    def __init__(self, *, submission, access_reader, policy_request, targets,
                 vertex_transport, provider_transport, enabled=False,
                 clock=lambda: datetime.now(timezone.utc), quality_policy=None, approval_guard=None):
        self.submission, self.access_reader = submission, access_reader
        self.policy_request, self.targets = policy_request, targets
        self.vertex_transport, self.provider_transport = vertex_transport, provider_transport
        _require(type(enabled) is bool, 'QUESTION_SERVING_CONFIG')
        self.enabled, self.clock = enabled, clock
        _require(approval_guard is None or type(approval_guard) is PolicyApprovalRuntimeGuard,
            'QUESTION_APPROVAL_GUARD_CONFIGURATION')
        self.approval_guard=approval_guard
        self.quality_policy=quality_policy
        self.registry = BigQueryPromptRegistryReader(submission.client)
        self.evidence = ProtectedEvidenceChatService(client=submission.client,
            verifier=submission.verifier, access_reader=access_reader, registry_reader=self.registry,
            policy_request=policy_request, targets=targets, clock=clock)
        self._lock, self._worker, self._snapshot_sha = RLock(), None, None

    def check(self, headers, **scope):
        require_owned_worker()
        return self.evidence.check(headers, **scope)

    def question_turn(self, headers, **scope):
        require_owned_worker()
        return self.evidence.question_turn(headers, **scope)

    def process(self, headers, receipt):
        require_owned_worker()
        if not self.enabled:
            return QuestionWorkerStatus('HELD', 'QUESTION_WORKER_DISABLED')
        with self._lock:
            # Authenticate/authorize before reading policy or prompt content.
            self.evidence._identity(headers, receipt.workspace_id, receipt.conversation_id)
            policy = read_policy(self.submission.client, self.policy_request, clock=self.clock)
            if not policy.approval_verified or json.loads(policy.policy_canonical_json)['status'] != 'APPROVED':
                return QuestionWorkerStatus('HELD', 'QUESTION_POLICY_REVIEW_REQUIRED')
            if self.approval_guard is None:
                return QuestionWorkerStatus('HELD','QUESTION_APPROVAL_RUNTIME_REQUIRED')
            definition = definitions(policy, checked_at=self.clock())[1]
            target = next((t for t in self.targets if t[0] == QUESTION_KEY), None)
            _require(target is not None, 'QUESTION_REGISTERED_TARGET_REQUIRED')
            snapshot = self.registry.read_exact(target[1], target[2]).state.head.snapshot
            definition.verify(snapshot)
            if self._worker is None:
                generation = json.loads(snapshot.generation_config_json)
                profile = dict(self.submission.profile, profile_id='registered-evidence-questions',
                    prompt_id=snapshot.vertex_prompt_id, prompt_version=snapshot.vertex_version_id,
                    model_id=snapshot.model_id, location=snapshot.vertex_location,
                    output_schema_version=snapshot.output_schema_version,
                    safety_policy_version=snapshot.safety_policy_version,
                    settings_sha256=_hash(dict(generation=generation, snapshot_sha256=snapshot.snapshot_sha256)),
                    max_output_tokens=generation['max_output_tokens'], seed=generation['seed'],
                    temperature=generation['temperature'], thinking_level=generation['thinking_level'],
                    thinking_control='NONE' if generation['thinking_level'] is None else 'LEVEL',
                    thinking_budget=None)
                validate_profile(profile)
                pins = frozenset({_hash(profile)})
                runtime = PromptRuntimeService(identity_adapter=self.submission.verifier,
                    access_reader=self.access_reader, reader=BigQueryPromptRuntimeReader(self.submission.client),
                    vertex_transport=self.vertex_transport, behaviors=(definition.behavior(snapshot),),
                    approved_profiles=pins, enabled=True)
                writer = ProtectedQuestionWriter(client=self.submission.client,
                    verifier=self.submission.verifier, access_reader=self.access_reader,
                    policy_request=self.policy_request, profile=profile,
                    worker_id=self.submission.worker_id, enabled=True, clock=self.clock)
                callback=None
                if self.quality_policy is not None:
                    from src.application.campaign_quality_execution import QualityExecutionSettings
                    from src.application.workflow_contract import canonical_json_bytes
                    from src.application.question_quality_pipeline import QuestionQualityPublisher
                    settings=QualityExecutionSettings(**self.quality_policy,behavior=definition.behavior(snapshot),
                        model_profile_json=canonical_json_bytes(profile).decode(),approved_profiles=pins)
                    callback=QuestionQualityPublisher(client=self.submission.client,verifier=self.submission.verifier,
                        access_reader=self.access_reader,runtime_reader=runtime.reader,settings=settings,
                        evidence=self.evidence,clock=self.clock,approval_guard=self.approval_guard)
                # An execution path without an installed checker must stay held.
                if callback is None:
                    return QuestionWorkerStatus('HELD','QUESTION_QUALITY_SETTINGS_REQUIRED')
                self._worker = RegisteredQuestionWorker(writer=writer, runtime=runtime,
                    provider_transport=lambda p,s,t,o: self.provider_transport(p,s,t,o,snapshot),
                    refresh_gaps=self._refresh_gaps, enabled=True, clock=self.clock,on_committed=callback)
                self._snapshot_sha = snapshot.snapshot_sha256
            _require(self._snapshot_sha == snapshot.snapshot_sha256, 'QUESTION_SERVING_SNAPSHOT_CHANGED')
            return self._worker.process(headers, receipt)

    def _refresh_gaps(self, headers, receipt):
        identity, access = self.evidence._identity(headers, receipt.workspace_id, receipt.conversation_id)
        capture = read_question_context(self.submission.client, identity=identity, access_reader=self.access_reader,
            workspace_id=receipt.workspace_id, conversation_id=receipt.conversation_id,
            clock=self.clock)
        records = capture.records()
        calls = [decode_row(row,'model_invocations') for row in records['model_invocations']]
        calls = [call for call in calls if call['request_id'] == receipt.request_id
            and call['invocation_id'] == receipt.invocation_id]
        _require(len(calls) == 1,'QUESTION_PARENT_RESPONSE_UNAVAILABLE')
        expected = verify_stored_response(self.submission.client,calls[0],clock=self.clock)
        bodies = [json.loads(row) for row in records['campaign_transcript_responses']]
        matches = [body for body in bodies if body['request_id'] == receipt.request_id
            and body['invocation_id'] == receipt.invocation_id and body['content_sha256'] == expected]
        import hashlib
        _require(len(matches) == 1 and hashlib.sha256(matches[0]['response_text'].encode()).hexdigest() == expected,
            'QUESTION_PARENT_RESPONSE_UNAVAILABLE')
        result = self.evidence.prepare_evidence(headers, workspace_id=receipt.workspace_id,
            conversation_id=receipt.conversation_id, request_id=receipt.request_id,
            observed_interpreter_response=matches[0]['response_text'])
        _, after = self.evidence._identity(headers, receipt.workspace_id, receipt.conversation_id)
        _require(access == after and self.clock() < capture.valid_until, 'QUESTION_SERVING_ACCESS_CHANGED')
        return result
