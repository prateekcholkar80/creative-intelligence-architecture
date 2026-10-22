"""Server-only bridge from Streamlit request context to prestarted workflow hosts."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import RLock
import logging

from src.application.identity import ASSERTION_HEADER
from src.application.campaign_chat_evidence_service import ProtectedEvidenceChatService
from src.application.protected_records import ArtifactSummary, OutputSummary
from src.application.protected_service import ArtifactDownload, JobStatus
from src.application.sharing_operation_host import SharingHostError
from src.application.workflow_contract import PREFERENCE_FIELDS, _id
from src.application.workflow_history_host import WorkflowHistoryHost
from src.application.workflow_read_host import WorkflowReadHost
from src.application.workflow_submission_host import WorkflowSubmissionHost
from src.application.workflow_submission_service import SubmissionReceipt
from src.ui.workflow_session import (
    WorkflowSessionState, apply_history, apply_submission,
)


class WorkflowUiError(RuntimeError):
    """Safe UI runtime failure; never includes assertions or provider details."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class WorkflowUiSettings:
    workspace_id: str
    conversation_id: str
    page_size: int = 20

    def __post_init__(self):
        try:
            _id(self.workspace_id)
            _id(self.conversation_id)
        except Exception:
            raise ValueError('Invalid trusted workflow UI scope') from None
        if type(self.page_size) is not int or not 1 <= self.page_size <= 50:
            raise ValueError('Invalid trusted workflow history page size')


@dataclass(frozen=True, slots=True)
class WorkflowUiSubmissionResult:
    state: WorkflowSessionState
    receipt: SubmissionReceipt


@dataclass(frozen=True, slots=True)
class WorkflowUiJobDetails:
    status: JobStatus
    output: OutputSummary | None


def iap_headers(request_headers: Mapping[str, str]) -> dict[str, str]:
    """Copy only one IAP assertion from the server request context."""
    if not isinstance(request_headers, Mapping):
        raise WorkflowUiError('AUTHENTICATION_REQUIRED')
    get_all = getattr(request_headers, 'get_all', None)
    if callable(get_all):
        values = get_all(ASSERTION_HEADER)
    else:
        values = [value for key, value in request_headers.items()
            if type(key) is str and key.lower() == ASSERTION_HEADER]
    if (type(values) is not list or len(values) != 1
            or type(values[0]) is not str or not 1 <= len(values[0]) <= 16384):
        raise WorkflowUiError('AUTHENTICATION_REQUIRED')
    return {ASSERTION_HEADER: values[0]}


class WorkflowUiRuntime:
    """Use configured scope and prestarted hosts; never owns their lifecycle."""

    def __init__(self, submission_host: WorkflowSubmissionHost,
                 history_host: WorkflowHistoryHost, read_host: WorkflowReadHost,
                 settings: WorkflowUiSettings, *, evidence_service=None, attachment_service=None,
                 campaign_creation_service=None):
        if (type(submission_host) is not WorkflowSubmissionHost
                or type(history_host) is not WorkflowHistoryHost
                or type(read_host) is not WorkflowReadHost
                or type(settings) is not WorkflowUiSettings):
            raise ValueError('Typed workflow UI runtime composition required')
        if (submission_host.worker_pid is None or history_host.worker_pid is None
                or read_host.worker_pid is None or submission_host.retired
                or history_host.retired or read_host.retired):
            raise ValueError('Workflow hosts must be prestarted')
        self._submission = submission_host
        self._history = history_host
        self._read = read_host
        self.settings = settings
        from src.application.campaign_creation import ProtectedCampaignCreation
        if campaign_creation_service is not None:
            if (type(campaign_creation_service) is not ProtectedCampaignCreation
                    or any(host.config.iap != campaign_creation_service.verifier.settings
                        for host in (submission_host,history_host,read_host))):
                raise ValueError('Matching protected campaign creation service required')
        self._campaign_creation_service=campaign_creation_service
        if evidence_service is not None and type(evidence_service) is not ProtectedEvidenceChatService:
            raise ValueError('Typed protected evidence service required')
        self._evidence_service = evidence_service
        from src.application.campaign_attachment_service import ProtectedAttachmentService
        if attachment_service is not None and type(attachment_service) is not ProtectedAttachmentService:
            raise ValueError('Typed protected attachment service required')
        self._attachment_service = attachment_service
        self._document_service=None
        self._attachment_batches=None
        if attachment_service is not None:
            from src.application.campaign_attachment_batch import ProtectedAttachmentBatches
            self._attachment_batches=ProtectedAttachmentBatches(attachment_service)
            from src.application.campaign_document_service import ProtectedDocumentService
            from src.application.campaign_document_store import DocumentStore
            self._document_service=ProtectedDocumentService(intake=attachment_service,
                store=DocumentStore(attachment_service.store.client))

    @property
    def campaign_creation_available(self):
        from src.application.campaign_creation import ProtectedCampaignCreation
        return type(getattr(self,'_campaign_creation_service',None)) is ProtectedCampaignCreation

    def _campaign_creation_call(self,action,headers,**values):
        if not self.campaign_creation_available:
            raise WorkflowUiError('CAMPAIGN_PIPELINE_NOT_INSTALLED')
        if action not in {'products','submit','recover','jobs','job','artifact'}:
            raise WorkflowUiError('CAMPAIGN_ACTION_UNAVAILABLE')
        try:
            return getattr(self._campaign_creation_service,action)(iap_headers(headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id,**values)
        except Exception:
            raise WorkflowUiError('CAMPAIGN_OPERATION_UNVERIFIED') from None

    def campaign_products(self,headers):return self._campaign_creation_call('products',headers)
    def campaign_jobs(self,headers):return self._campaign_creation_call('jobs',headers)
    def campaign_job(self,headers,*,job_id):return self._campaign_creation_call('job',headers,job_id=job_id)
    def campaign_artifact(self,headers,*,job_id):return self._campaign_creation_call('artifact',headers,job_id=job_id)
    def campaign_submit(self,headers,*,sku,message,retry_key):
        return self._campaign_creation_call('submit',headers,sku=sku,message=message,retry_key=retry_key)
    def campaign_recover(self,headers,*,sku,message,retry_key):
        return self._campaign_creation_call('recover',headers,sku=sku,message=message,retry_key=retry_key)

    def campaign_deferred_download(self,headers,*,job_id,sku,content_sha256):
        """Per-click protected read without Streamlit context on a worker thread.

        Retains only the page assertion in a transient UI closure. The backend
        must validate its expiry and current authority on every invocation.
        This is not a durable worker capability or a refreshed browser token.
        """
        from src.application.campaign_creation import CampaignImage
        from src.application.workflow_contract import _digest
        headers=iap_headers(headers);_id(job_id);_id(sku);_digest(content_sha256)
        def fetch():
            current=self.campaign_artifact(headers,job_id=job_id)
            if (type(current) is not CampaignImage or current.job_id!=job_id or current.sku!=sku
                    or current.content_sha256!=content_sha256):
                raise WorkflowUiError('CAMPAIGN_ARTIFACT_UNVERIFIED')
            return current.png
        return fetch

    @property
    def attachments_available(self):
        service=getattr(self,'_attachment_service',None)
        return service is not None and service.enabled is True

    def _attachment_batch_call(self,action,headers,**values):
        if not self.attachments_available or self._attachment_batches is None:
            raise WorkflowUiError('ATTACHMENT_INTAKE_DISABLED')
        try:
            return getattr(self._attachment_batches,action)(iap_headers(headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id,**values)
        except Exception:
            raise WorkflowUiError('ATTACHMENT_BATCH_UNVERIFIED') from None

    def attachment_batches(self,headers,*,request_id):
        return self._attachment_batch_call('list',headers,request_id=request_id)

    def start_attachment_batch(self,headers,**values):
        return self._attachment_batch_call('start',headers,**values)

    def attachment_batch_state(self,headers,*,batch_id):
        return self._attachment_batch_call('state',headers,batch_id=batch_id)

    def advance_attachment_batch(self,headers,*,batch_id):
        return self._attachment_batch_call('advance',headers,batch_id=batch_id)

    def bind_evaluation_uploads(self,headers,*,request_id,submission_ids):
        if not self.attachments_available or self._document_service is None:
            raise WorkflowUiError('ATTACHMENT_INTAKE_DISABLED')
        from src.application.campaign_evaluation_uploads import ProtectedEvaluationUploadReader
        try:
            return ProtectedEvaluationUploadReader(self._document_service).publish_binding(iap_headers(headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id,
                request_id=request_id,submission_ids=submission_ids)
        except Exception:
            raise WorkflowUiError('EVALUATION_UPLOAD_BINDING_UNVERIFIED') from None

    def attachment_requests(self,request_headers,*,request_id):
        if not self.attachments_available:raise WorkflowUiError('ATTACHMENT_INTAKE_DISABLED')
        try:
            return self._attachment_service.requests(iap_headers(request_headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id,request_id=request_id)
        except Exception:
            raise WorkflowUiError('ATTACHMENT_REQUESTS_UNAVAILABLE') from None

    def verify_attachment_intake(self,request_headers):
        if not self.attachments_available:raise WorkflowUiError('ATTACHMENT_INTAKE_DISABLED')
        try:
            result=self._attachment_service.verify_intake(iap_headers(request_headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id)
            if result is not True:raise ValueError('Invalid attachment readiness')
            return True
        except Exception:
            raise WorkflowUiError('ATTACHMENT_STORAGE_UNAVAILABLE') from None

    def attachment_saved_requests(self,request_headers):
        if not self.attachments_available:raise WorkflowUiError('ATTACHMENT_INTAKE_DISABLED')
        try:
            return self._attachment_service.saved_requests(iap_headers(request_headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id)
        except Exception:raise WorkflowUiError('ATTACHMENT_HISTORY_UNAVAILABLE') from None

    def attachment_summaries(self,request_headers,*,request_id):
        if not self.attachments_available:raise WorkflowUiError('ATTACHMENT_INTAKE_DISABLED')
        try:
            return self._attachment_service.summaries(iap_headers(request_headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id,request_id=request_id)
        except Exception:raise WorkflowUiError('ATTACHMENT_HISTORY_UNAVAILABLE') from None

    def submit_attachment(self,request_headers,*,request_id,requirement_id,request_sha256,
                          choice,category=None,filename=None,content=None):
        if not self.attachments_available:raise WorkflowUiError('ATTACHMENT_INTAKE_DISABLED')
        try:
            return self._attachment_service.submit(iap_headers(request_headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id,
                request_id=request_id,requirement_id=requirement_id,request_sha256=request_sha256,
                choice=choice,category=category,filename=filename,content=content)
        except Exception as error:
            allowed={'ATTACHMENT_FORMAT_UNSUPPORTED','ATTACHMENT_FILE_SIZE','ATTACHMENT_IMAGE_REQUIRED',
                     'ATTACHMENT_REQUEST_CHANGED','ATTACHMENT_AUTHORITY_CHANGED'}
            code=getattr(error,'code',None)
            raise WorkflowUiError(code if code in allowed else 'ATTACHMENT_SUBMISSION_UNVERIFIED') from None

    def process_document(self,request_headers,*,submission_id,extract=False):
        if not self.attachments_available or self._document_service is None:
            raise WorkflowUiError('DOCUMENT_PROCESSING_NOT_INSTALLED')
        try:
            method=self._document_service.extract if extract else self._document_service.status
            return method(iap_headers(request_headers),workspace_id=self.settings.workspace_id,
                conversation_id=self.settings.conversation_id,submission_id=submission_id)
        except Exception as error:
            code=getattr(error,'code',None)
            allowed={'DOCUMENT_PARSER_TIMEOUT','DOCUMENT_TEXT_LIMIT','DOCUMENT_PAGE_LIMIT','DOCUMENT_CONTEXT_CHANGED',
                'DOCUMENT_ORIGINAL_CHANGED','DOCUMENT_ORIGINAL_HASH','DOCUMENT_EXTRACTION_AMBIGUOUS'}
            raise WorkflowUiError(code if code in allowed else 'DOCUMENT_PROCESSING_UNAVAILABLE') from None

    def apply_omission(self,request_headers,*,submission_id):
        try:
            return self._submission.apply_omission(iap_headers(request_headers),
                workspace_id=self.settings.workspace_id,conversation_id=self.settings.conversation_id,submission_id=submission_id)
        except Exception:raise WorkflowUiError('OMISSION_TURN_UNAVAILABLE') from None

    @property
    def interpretation_available(self):
        return (self._submission.config.interpretation_enabled and self._submission.worker_pid is not None
            and not self._submission.retired)

    def intake_options(self,request_headers):
        try:
            return self._submission.intake_options(iap_headers(request_headers),workspace_id=self.settings.workspace_id,
                conversation_id=self.settings.conversation_id)
        except Exception as error:
            raise self._safe_host_error(error) from None

    def interpret(self, request_headers, *, message, selection, retry_key):
        headers=iap_headers(request_headers)
        if not self.interpretation_available:
            raise WorkflowUiError('INTERPRETER_SUBMISSION_DISABLED')
        try:
            return self._submission.interpret(headers,workspace_id=self.settings.workspace_id,
                conversation_id=self.settings.conversation_id,message=message,selection=selection,retry_key=retry_key)
        except Exception as error:
            raise self._safe_host_error(error) from None

    def evidence_readiness(self, request_headers):
        headers = iap_headers(request_headers)
        if self._evidence_service is None:
            try:
                return self._submission.evidence(headers,workspace_id=self.settings.workspace_id,
                    conversation_id=self.settings.conversation_id).readiness
            except Exception as error:
                if isinstance(error,SharingHostError) and error.code=='INTERPRETER_SUBMISSION_DISABLED':
                    raise WorkflowUiError('CHAT_EVIDENCE_SERVICE_NOT_INSTALLED') from None
                raise WorkflowUiError('CHAT_EVIDENCE_ELIGIBILITY_UNAVAILABLE') from None
        try:
            return self._evidence_service.check(headers,
                workspace_id=self.settings.workspace_id,
                conversation_id=self.settings.conversation_id)
        except Exception:
            raise WorkflowUiError('CHAT_EVIDENCE_ELIGIBILITY_UNAVAILABLE') from None

    def question_turn(self, request_headers):
        headers=iap_headers(request_headers)
        if self._evidence_service is None:
            try:
                return self._submission.evidence(headers,workspace_id=self.settings.workspace_id,
                    conversation_id=self.settings.conversation_id).turn
            except Exception:
                raise WorkflowUiError('CHAT_QUESTIONS_UNAVAILABLE') from None
        try:
            return self._evidence_service.question_turn(headers,
                workspace_id=self.settings.workspace_id,
                conversation_id=self.settings.conversation_id)
        except Exception:
            raise WorkflowUiError('CHAT_QUESTIONS_UNAVAILABLE') from None

    def prepare_evidence(self, request_headers, *, request_id, observed_interpreter_response):
        """Server-only preparation for an already persisted interpreter outcome.

        The result is a private gap plan, never a user-released question or a
        generation grant. Backend rebinds the response to authoritative records.
        """
        headers = iap_headers(request_headers)
        if self._evidence_service is None:
            raise WorkflowUiError('CHAT_EVIDENCE_SERVICE_NOT_INSTALLED')
        try:
            return self._evidence_service.prepare_evidence(headers,
                workspace_id=self.settings.workspace_id,
                conversation_id=self.settings.conversation_id,
                request_id=request_id,
                observed_interpreter_response=observed_interpreter_response)
        except Exception:
            raise WorkflowUiError('CHAT_EVIDENCE_PREPARATION_UNAVAILABLE') from None

    @staticmethod
    def _safe_host_error(error: Exception) -> WorkflowUiError:
        diagnostic = getattr(error, 'code', None) if isinstance(error, SharingHostError) else None
        if diagnostic not in {'ACCESS_DENIED', 'SHARING_HOST_OPERATION_FAILED',
                'SHARING_HOST_DISABLED', 'SHARING_HOST_RETIRED', 'SHARING_HOST_NOT_STARTED',
                'SHARING_HOST_PROTOCOL_FAILED', 'SHARING_HOST_START_FAILED',
                'HISTORY_COMPOSITION_FAILED', 'HISTORY_IDENTITY_DENIED',
                'HISTORY_AUTHORIZATION_DENIED', 'HISTORY_CONTRACT_DENIED'}:
            diagnostic = 'UNCLASSIFIED_RUNTIME_FAILURE'
        logging.getLogger(__name__).warning('Protected workflow host failure: %s', diagnostic)
        code = 'ACCESS_DENIED' if (isinstance(error, SharingHostError)
            and error.code == 'ACCESS_DENIED') else 'WORKFLOW_RUNTIME_UNAVAILABLE'
        return WorkflowUiError(code)

    def load_history(self, state: WorkflowSessionState,
                     request_headers: Mapping[str, str], *, cursor: str | None = None
                     ) -> WorkflowSessionState:
        headers = iap_headers(request_headers)
        parameters = dict(workspace_id=self.settings.workspace_id,
            conversation_id=self.settings.conversation_id,
            page_size=self.settings.page_size)
        if cursor is not None:
            parameters['cursor'] = cursor
        try:
            page = self._history.list_jobs(headers, **parameters)
            return apply_history(state, page, requested_cursor=cursor)
        except WorkflowUiError:
            raise
        except Exception as error:
            raise self._safe_host_error(error) from None

    def submit(self, state: WorkflowSessionState, request_headers: Mapping[str, str],
               *, preferences: dict[str, str], retry_key: str) -> WorkflowUiSubmissionResult:
        headers = iap_headers(request_headers)
        if (type(preferences) is not dict or set(preferences) != set(PREFERENCE_FIELDS)
                or any(type(value) is not str for value in preferences.values())):
            raise WorkflowUiError('INVALID_CAMPAIGN_REQUEST')
        try:
            _id(retry_key)
            receipt = self._submission.submit(headers,
                workspace_id=self.settings.workspace_id,
                conversation_id=self.settings.conversation_id,
                preferences=dict(preferences), retry_key=retry_key)
            return WorkflowUiSubmissionResult(apply_submission(state, receipt), receipt)
        except WorkflowUiError:
            raise
        except Exception as error:
            raise self._safe_host_error(error) from None

    def job_details(self, request_headers: Mapping[str, str], *, job_id: str
                    ) -> WorkflowUiJobDetails:
        headers = iap_headers(request_headers)
        try:
            _id(job_id)
            status = self._read.get_job_status(headers,
                workspace_id=self.settings.workspace_id, job_id=job_id)
            output = None
            if status.state == 'SUCCEEDED':
                output = self._read.get_output_summary(headers,
                    workspace_id=self.settings.workspace_id, job_id=job_id)
                if (output.output_id != status.output_id
                        or output.canonical_sku != status.canonical_sku
                        or output.conversation_id != status.conversation_id):
                    raise WorkflowUiError('WORKFLOW_RUNTIME_UNAVAILABLE')
            return WorkflowUiJobDetails(status, output)
        except WorkflowUiError:
            raise
        except Exception as error:
            raise self._safe_host_error(error) from None

    def download(self, request_headers: Mapping[str, str], *, job_id: str,
                 artifact: ArtifactSummary) -> ArtifactDownload:
        headers = iap_headers(request_headers)
        if type(artifact) is not ArtifactSummary:
            raise WorkflowUiError('INVALID_ARTIFACT_SELECTION')
        try:
            _id(job_id)
            value = self._read.download_artifact(headers,
                workspace_id=self.settings.workspace_id, job_id=job_id,
                artifact_id=artifact.artifact_id)
            if (value.filename != artifact.filename
                    or value.content_type != artifact.content_type
                    or value.sha256 != artifact.sha256
                    or len(value.data) != artifact.size_bytes
                    or value.output.job_id != job_id):
                raise WorkflowUiError('WORKFLOW_RUNTIME_UNAVAILABLE')
            return value
        except WorkflowUiError:
            raise
        except Exception as error:
            raise self._safe_host_error(error) from None

    def deferred_download(self, request_headers: Mapping[str, str], *, job_id: str,
                          artifact: ArtifactSummary) -> Callable[[], bytes]:
        """Capture only the assertion; protected bytes are read when clicked."""
        headers = iap_headers(request_headers)
        if type(artifact) is not ArtifactSummary:
            raise WorkflowUiError('INVALID_ARTIFACT_SELECTION')

        def fetch() -> bytes:
            return self.download(headers, job_id=job_id, artifact=artifact).data

        return fetch


_RUNTIME_LOCK = RLock()
_RUNTIME: WorkflowUiRuntime | None = None


def install_workflow_runtime(runtime: WorkflowUiRuntime) -> None:
    """Install once from trusted bootstrap; Streamlit request code cannot replace it."""
    global _RUNTIME
    if type(runtime) is not WorkflowUiRuntime:
        raise ValueError('Typed workflow UI runtime required')
    with _RUNTIME_LOCK:
        if _RUNTIME is not None and _RUNTIME is not runtime:
            raise RuntimeError('Workflow UI runtime already installed')
        _RUNTIME = runtime


def get_workflow_runtime() -> WorkflowUiRuntime | None:
    with _RUNTIME_LOCK:
        return _RUNTIME
