"""Immutable access projections, not full workflow write-validation contracts."""
from dataclasses import dataclass
from datetime import datetime

from src.application.access_policy import AccessDecision


@dataclass(frozen=True, slots=True)
class JobMetadata:
    workspace_id: str
    conversation_id: str
    owner_id: str
    job_id: str
    request_id: str
    canonical_sku: str
    state: str
    revision: int
    active_attempt_id: str | None
    output_id: str | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True, slots=True)
class OutputMetadata:
    workspace_id: str
    conversation_id: str
    output_id: str
    job_id: str
    canonical_sku: str
    source_system: str
    approval_scope: str
    initial_review_status: str
    real_business_approved: bool
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ArtifactSummary:
    artifact_id: str
    kind: str
    filename: str
    content_type: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class OutputSummary:
    workspace_id: str
    conversation_id: str
    output_id: str
    job_id: str
    canonical_sku: str
    source_system: str
    approval_scope: str
    initial_review_status: str
    real_business_approved: bool
    created_at: datetime
    downloads: tuple[ArtifactSummary, ...]


@dataclass(frozen=True, slots=True)
class AuthorizedJob:
    metadata: JobMetadata
    access: AccessDecision


@dataclass(frozen=True, slots=True)
class AuthorizedOutput:
    metadata: OutputSummary
    access: AccessDecision


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    artifact_id: str
    kind: str
    gcs_uri: str
    gcs_generation: str
    sha256: str
    content_type: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class AuthorizedArtifact:
    """Internal validated manifest; never an enduring authorization grant."""
    metadata: OutputMetadata
    artifact: ArtifactManifest
    brief_json: bytes
    access: AccessDecision
