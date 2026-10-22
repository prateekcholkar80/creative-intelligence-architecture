"""Compose child admission, guarded question dispatch and durable publication.

This is an owned-process component. It neither installs HTTP routes nor grants
policy approval, evaluates output or releases provider text to the browser.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock

from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_evidence_dispatch import EvidenceQuestionDispatch
from src.application.campaign_question_context_guard import QuestionContextGuard
from src.application.campaign_question_context_reader import read_question_context
from src.application.campaign_question_writer import ProtectedQuestionWriter
from src.application.generation_admission import AdmissionSettings, ProtectedGenerationAdmission
from src.application.interpreter_outcome import ProtectedInterpreterOutcomeService
from src.application.interpreter_worker import InterpreterWorker
from src.application.operation_budget import require_owned_worker
from src.application.prompt_runtime_service import PromptRuntimeService
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import _require


@dataclass(frozen=True, slots=True)
class QuestionWorkerStatus:
    state: str
    reason: str | None = None

    @property
    def release_allowed(self): return False


class RegisteredQuestionWorker:
    """Server dependencies only; retries recover the retained original operation.

    refresh_gaps(headers, parent_receipt) must perform authoritative gap reads.
    The context guard independently rejects changed evidence or authority. The
    registered runtime still checks exact prompt eligibility before dispatch.
    """
    def __init__(self, *, writer, runtime, provider_transport, refresh_gaps,
                 enabled=False, clock=lambda: datetime.now(timezone.utc), on_committed=None):
        _require(type(writer) is ProtectedQuestionWriter
            and type(runtime) is PromptRuntimeService and type(enabled) is bool
            and callable(provider_transport) and callable(refresh_gaps)
            and (on_committed is None or callable(on_committed)),
            'QUESTION_WORKER_COMPOSITION')
        self.writer, self.runtime = writer, runtime
        self.provider_transport, self.refresh_gaps = provider_transport, refresh_gaps
        self.enabled, self.clock = enabled, clock
        self._lock, self._operations = RLock(), {}
        self.on_committed,self._contexts=on_committed,{}

    def _finish(self,headers,key,receipt,result):
        if (self.on_committed is not None and result.outcome.native_status.value=='COMMITTED'
                and result.draft is not None and result.draft.observation.state=='SUCCEEDED'):
            self.on_committed(headers,receipt,self._contexts[key][1],result)
        return QuestionWorkerStatus(result.outcome.native_status.value)

    def process(self, headers, parent_receipt):
        require_owned_worker()
        if not self.enabled:
            return QuestionWorkerStatus('HELD', 'QUESTION_WORKER_DISABLED')
        _require(parent_receipt.native_status == 'COMMITTED', 'QUESTION_PARENT_NOT_COMMITTED')
        key = (parent_receipt.workspace_id, parent_receipt.conversation_id,
               parent_receipt.request_id, parent_receipt.invocation_id)
        with self._lock:
            if key in self._operations:
                worker, plan = self._operations[key]
                result = worker.recover(headers, plan.job_id)
                receipt,context=self._contexts[key]
                return self._finish(headers,key,receipt,result)
            _require(len(self._operations) < 32, 'QUESTION_WORKER_CAPACITY')
            receipt = self.writer.submit(headers, workspace_id=parent_receipt.workspace_id,
                conversation_id=parent_receipt.conversation_id,
                parent_request_id=parent_receipt.request_id)
            if receipt.native_status != 'COMMITTED':
                return QuestionWorkerStatus('HELD', 'QUESTION_CHILD_NOT_COMMITTED')
            context = self.writer.admission_context(headers, receipt)
            # A DRAFT capture may be stored, but cannot authorize a model call.
            if not context.initial_gaps.policy_approval_verified:
                return QuestionWorkerStatus('HELD', 'QUESTION_POLICY_REVIEW_REQUIRED')
            guard_slot = {}

            def refresh():
                _require('guard' in guard_slot, 'QUESTION_CONTEXT_GUARD_NOT_INSTALLED')
                return guard_slot['guard']()

            adapter = EvidenceQuestionDispatch(context.initial_gaps, refresh=refresh,
                transport=self.provider_transport, clock=self.clock)
            pins = frozenset({_hash(self.writer.profile)})
            admission = ProtectedGenerationAdmission(client=self.writer.client,
                verifier=self.writer.verifier, worker_id=self.writer.worker_id,
                settings=AdmissionSettings(profile_sha256=pins, enabled=True,
                    require_registered_prompts=True), model_call=adapter,
                prompt_runtime=self.runtime)
            publisher = ProtectedInterpreterOutcomeService(client=self.writer.client,
                verifier=self.writer.verifier, access_reader=self.writer.access_reader,
                worker_id=self.writer.worker_id, enabled=True)
            worker = InterpreterWorker(admission=admission, publisher=publisher,
                enabled=True, clock=self.clock)
            parameters = {p.name: p.value for p in context.plan.config().query_parameters}
            request = decode_row(parameters['request_candidate'], 'requests')
            plan = admission.prepare(headers, workspace_id=receipt.workspace_id,
                conversation_id=receipt.conversation_id, request_id=receipt.request_id,
                invocation_id=receipt.invocation_id,
                start_at=request['created_at'] - timedelta(seconds=1),
                end_at=min(self.clock() + timedelta(minutes=5), context.initial_gaps.valid_until,
                           context.admitted.valid_until))

            def capture():
                actor = self.writer.verifier.authenticate(headers)
                return read_question_context(self.writer.client, identity=actor,
                    access_reader=self.writer.access_reader, workspace_id=receipt.workspace_id,
                    conversation_id=receipt.conversation_id, clock=self.clock)

            guard_slot['guard'] = QuestionContextGuard(initial=context.initial_gaps,
                admitted=context.admitted, admission_proof=context.proof, dispatch_plan=plan,
                read_context=capture,
                read_gaps=lambda: self.refresh_gaps(headers, parent_receipt), clock=self.clock)
            # Retain before any dispatch. Failed/unknown outcomes never remint a call.
            self._operations[key] = (worker, plan)
            self._contexts[key]=(receipt,context)
            result = worker.run(headers, plan)
            return self._finish(headers,key,receipt,result)
