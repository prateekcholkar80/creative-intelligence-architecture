"""Atomic trusted installation of existing workflow and interpreter runtimes.

Caller owns prestarted hosts/clients. This never grants eligibility or enables a
worker, and accepts no browser-supplied configuration or serialized credentials.
"""
from src.application import interpreter_worker as workers
from src.ui import workflow_runtime as workflows
from src.application.campaign_chat_evidence_service import ProtectedEvidenceChatService
from src.application.campaign_evidence_dispatch import EvidenceIntentAdapter,EvidenceQuestionDispatch
from src.application.workflow_contract import _require


def install_campaign_runtime(runtime,worker):
    _require(type(runtime) is workflows.WorkflowUiRuntime and type(worker) is workers.InterpreterWorker,
        'CAMPAIGN_RUNTIME_COMPOSITION_REQUIRED')
    evidence=runtime._evidence_service
    _require(type(evidence) is ProtectedEvidenceChatService
        and worker.admission.settings.require_registered_prompts
        and type(worker.admission._call) in (EvidenceIntentAdapter,EvidenceQuestionDispatch)
        and worker.publisher.io.client is worker.admission.io.client is evidence.client
        and worker.admission.verifier.settings == worker.publisher.verifier.settings == evidence.verifier.settings,
        'CAMPAIGN_RUNTIME_SCOPE_MISMATCH')
    for host in (runtime._submission,runtime._history,runtime._read):
        _require(host.worker_pid is not None and not host.retired
            and host.config.iap == evidence.verifier.settings,'CAMPAIGN_RUNTIME_HOST_NOT_READY')
    # Validate both slots before changing either. Stable lock order and reentrant
    # public installers avoid an interpreter-only or workflow-only partial install.
    with workers._INSTALL_LOCK,workflows._RUNTIME_LOCK:
        _require(workers._WORKER is None or workers._WORKER is worker,'INTERPRETER_WORKER_ALREADY_INSTALLED')
        _require(workflows._RUNTIME is None or workflows._RUNTIME is runtime,'WORKFLOW_RUNTIME_ALREADY_INSTALLED')
        workers.install_interpreter_worker(worker)
        workflows.install_workflow_runtime(runtime)
    return runtime
