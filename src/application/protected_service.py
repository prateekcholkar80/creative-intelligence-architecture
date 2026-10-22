"""Server-only authenticated read gateway; not wired to Streamlit or deployed.

HTTP hosts must pass observed request headers, never a browser identity object.
No generation, model dispatch, cloud writes or role fallback here.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from src.application.access_policy import AuthorizationError
from src.application.access_policy import Action, authorize
from src.application.access_capture import capture_access
from src.application.audit_contract import AuditTarget, ResourceType
from src.application.access_reader import AccessReadError, BigQueryAccessReader
from src.application.artifact_download import ArtifactReadError, GcsArtifactReader
from src.application.identity import IapIdentityAdapter, IdentityError
from src.application.protected_records import OutputMetadata, OutputSummary
from src.application.workflow_contract import ARTIFACT_TYPES, DEFAULT_ARTIFACT_LIMITS


class ProtectedServiceError(PermissionError):
    """One public denial for bad identity, unavailable records and forbidden content."""
    code = 'ACCESS_DENIED'

    def __init__(self, capture=None):
        self.capture = capture
        super().__init__('This request cannot be accessed with the current identity and permissions.')


@dataclass(frozen=True, slots=True)
class JobStatus:
    workspace_id: str
    conversation_id: str
    job_id: str
    canonical_sku: str
    state: str
    revision: int
    output_id: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ArtifactDownload:
    """Verified bytes and safe response metadata; no private GCS URL or identity."""
    data: bytes
    filename: str
    content_type: str
    sha256: str
    output: OutputMetadata


class ProtectedWorkflowService:
    """All exposed methods verify identity anew and use current database access."""

    def __init__(self, verifier: IapIdentityAdapter, reader: BigQueryAccessReader,
                 artifact_reader: GcsArtifactReader | None = None):
        if not isinstance(verifier, IapIdentityAdapter) or not isinstance(reader, BigQueryAccessReader):
            raise ValueError('Protected service requires configured server adapters')
        self.verifier, self.reader = verifier, reader
        if artifact_reader is not None and not isinstance(artifact_reader, GcsArtifactReader):
            raise ValueError('Protected downloads require a configured server adapter')
        self.artifact_reader = artifact_reader

    def get_job_status(self, headers: Mapping[str, str], *, workspace_id: str, job_id: str) -> JobStatus:
        try:
            identity = self.verifier.authenticate(headers)
            job = self.reader.read_job(identity, workspace_id=workspace_id, job_id=job_id).metadata
            return JobStatus(**{key: getattr(job, key) for key in JobStatus.__dataclass_fields__})
        except (IdentityError, AccessReadError, AuthorizationError):
            raise ProtectedServiceError() from None

    def download_artifact(self, headers: Mapping[str, str], *, workspace_id: str,
                          job_id: str, artifact_id: str) -> ArtifactDownload:
        try:
            identity = self.verifier.authenticate(headers)
            return self._download(identity,workspace_id=workspace_id,job_id=job_id,artifact_id=artifact_id)[0]
        except (IdentityError, AccessReadError, AuthorizationError, ArtifactReadError):
            raise ProtectedServiceError() from None

    def _download(self, identity, *, workspace_id,job_id,artifact_id):
        if self.artifact_reader is None:
            raise ArtifactReadError()
        record = self.reader.read_artifact(identity, workspace_id=workspace_id,
            job_id=job_id, artifact_id=artifact_id, limits=self.artifact_reader.settings.limits)
        raw = self.artifact_reader.read(record)
        # Reread the manifest and current access after GCS, before byte release.
        current = self.reader.read_artifact(identity, workspace_id=workspace_id,
            job_id=job_id, artifact_id=artifact_id, limits=self.artifact_reader.settings.limits)
        if (current.metadata != record.metadata or current.artifact != record.artifact
                or current.brief_json != record.brief_json):
            raise ArtifactReadError()
        return ArtifactDownload(raw,
            f'{record.artifact.artifact_id}.{ARTIFACT_TYPES[record.artifact.kind][0]}',
            record.artifact.content_type, record.artifact.sha256, record.metadata), current

    def read_api(self, headers, *, operation, parameters, trace_id):
        """Fresh read plus captured authorization for the private audit handoff."""
        try:
            identity=self.verifier.authenticate(headers)
            scope=dict(workspace_id=parameters['workspace_id'],job_id=parameters['job_id'])
            initial=self.reader.read_job(identity,**scope)
            if operation=='get_job_status':
                value=None
            elif operation=='get_output_summary':
                limits=(self.artifact_reader.settings.limits if self.artifact_reader
                    else DEFAULT_ARTIFACT_LIMITS)
                value=self.reader.read_output(identity,limits=limits,**scope).metadata
            elif operation=='download_artifact':
                value,_=self._download(identity,artifact_id=parameters['artifact_id'],**scope)
            else:
                raise ValueError('Fixed protected read operation required')
            current=self.reader.read_job(identity,**scope)
            job=current.metadata
            if (job.conversation_id!=initial.metadata.conversation_id or job.owner_id!=initial.metadata.owner_id
                    or job.request_id!=initial.metadata.request_id or job.canonical_sku!=initial.metadata.canonical_sku):
                raise ProtectedServiceError()
            if operation=='get_job_status':
                value=JobStatus(**{key:getattr(job,key) for key in JobStatus.__dataclass_fields__})
            else:
                output=value.output if isinstance(value,ArtifactDownload) else value
                if job.state!='SUCCEEDED' or output.output_id!=job.output_id or output.canonical_sku!=job.canonical_sku:
                    raise ProtectedServiceError()
            decision=self.reader.authorize(identity,Action.DOWNLOAD_ARTIFACT if operation=='download_artifact' else Action.READ_DRAFT,
                workspace_id=job.workspace_id,conversation_id=job.conversation_id)
            target=AuditTarget(ResourceType.CONVERSATION,job.conversation_id,job.workspace_id,
                decision.resource_revision,job.conversation_id,job.owner_id,job.request_id,job.job_id)
            return value,capture_access(decision,target=target,trace_id=trace_id)
        except AuthorizationError as error:
            raise ProtectedServiceError(capture_access(error.decision,target=None,trace_id=trace_id)) from None
        except IdentityError:
            decision=authorize(None,None,Action.DOWNLOAD_ARTIFACT if operation=='download_artifact' else Action.READ_DRAFT,
                workspace_id=parameters['workspace_id'])
            raise ProtectedServiceError(capture_access(decision,target=None,trace_id=trace_id)) from None
        except (AccessReadError,ArtifactReadError):
            raise ProtectedServiceError() from None

    def get_output_summary(self, headers: Mapping[str, str], *, workspace_id: str, job_id: str) -> OutputSummary:
        try:
            identity = self.verifier.authenticate(headers)
            limits=(self.artifact_reader.settings.limits if self.artifact_reader
                else DEFAULT_ARTIFACT_LIMITS)
            return self.reader.read_output(identity, workspace_id=workspace_id,
                job_id=job_id, limits=limits).metadata
        except (IdentityError, AccessReadError, AuthorizationError):
            raise ProtectedServiceError() from None
