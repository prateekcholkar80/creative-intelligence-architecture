"""Trusted one-shot interpreter worker coordination, disabled until configured.

This module never starts a thread/process from browser input or releases text.
"""
from dataclasses import dataclass,field
from datetime import datetime,timezone
from threading import RLock
from src.application.generation_admission import ProtectedGenerationAdmission
from src.application.interpreter_outcome import ProtectedInterpreterOutcomeService
from src.application.campaign_evidence_dispatch import EvidenceInvocationDraft
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.operation_budget import require_owned_worker,OperationStopped
from src.application.workflow_contract import _require


@dataclass(frozen=True,slots=True)
class InterpreterWorkerResult:
    outcome: object
    draft: EvidenceInvocationDraft | None = field(repr=False)
    @property
    def release_allowed(self):return False


class InterpreterWorker:
    def __init__(self,*,admission,publisher,enabled=False,clock=lambda:datetime.now(timezone.utc)):
        _require(type(admission) is ProtectedGenerationAdmission
            and type(publisher) is ProtectedInterpreterOutcomeService and type(enabled) is bool
            and admission.worker_id==publisher.worker_id
            and admission.settings.require_registered_prompts,
            'INTERPRETER_WORKER_COMPOSITION_INVALID')
        self.admission,self.publisher,self.enabled,self.clock=admission,publisher,enabled,clock
        self._lock=RLock();self._used=set();self._results={}

    def run(self,headers,admission_plan):
        require_owned_worker()
        with self._lock:
            _require(self.enabled,'INTERPRETER_WORKER_DISABLED')
            _require(len(self._used)<32 and admission_plan.job_id not in self._used,
                'INTERPRETER_WORKER_ORIGINAL_OPERATION_REQUIRED')
            self._used.add(admission_plan.job_id)
            draft=None
            try:
                draft=self.admission.dispatch(headers,admission_plan)
                _require(type(draft) is EvidenceInvocationDraft,'INTERPRETER_WORKER_PROVIDER_DRAFT_REQUIRED')
                observation=draft.observation
            except OperationStopped:
                # Do not perform I/O after the host deadline/cancellation fence.
                raise
            except Exception:
                # The call/claim may have happened. Preserve uncertainty, never
                # infer rollback, zero tokens or permission to call the model again.
                draft=None
                observation=ModelResultObservation('UNKNOWN',safe_error_code='CALL_OUTCOME_UNKNOWN')
            actor=self.admission._actor(headers)
            _,rows,_=self.admission._current(actor,admission_plan)
            invocation=rows['model_invocations']
            _require(invocation['stage']=='INTERPRETER' and invocation['worker_id']==self.publisher.worker_id,
                'INTERPRETER_WORKER_DISPATCH_SCOPE_INVALID')
            response_inputs={} if draft is None else dict(response_text=draft.response_text)
            result_plan=self.publisher.prepare(headers,**response_inputs,workspace=rows['workspaces'],
                membership=rows['memberships'],conversation=rows['conversations'],
                request=rows['requests'],invocation=invocation,observation=observation,observed_at=self.clock())
            # Retain the exact plan BEFORE publication so a lost response can
            # recover only this original native job without another model call.
            self._results[admission_plan.job_id]=(result_plan,draft)
            outcome=self.publisher.publish(headers,result_plan)
            return InterpreterWorkerResult(outcome,draft)

    def recover(self,headers,operation_id):
        with self._lock:
            item=self._results.get(operation_id)
            _require(item is not None,'INTERPRETER_WORKER_RECOVERY_PLAN_UNAVAILABLE')
            result_plan,draft=item
            return InterpreterWorkerResult(self.publisher.recover(headers,result_plan),draft)


_INSTALL_LOCK=RLock()
_WORKER=None


def install_interpreter_worker(worker):
    """Once-only trusted process bootstrap; never called from request form data."""
    global _WORKER
    _require(type(worker) is InterpreterWorker,'INTERPRETER_WORKER_COMPOSITION_INVALID')
    with _INSTALL_LOCK:
        _require(_WORKER is None or _WORKER is worker,'INTERPRETER_WORKER_ALREADY_INSTALLED')
        _WORKER=worker


def get_interpreter_worker():
    with _INSTALL_LOCK:return _WORKER
