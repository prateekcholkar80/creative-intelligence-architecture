"""Bounded, generation-pinned GCS reads; no URLs, fallback files or writes."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
from io import BytesIO

from google.api_core.retry import Retry
from google.cloud import storage
from PIL import Image

from src.application.protected_records import AuthorizedArtifact
from src.application.operation_budget import OperationStopped, check_operation, operation_call, operation_retry
from src.application.workflow_contract import (
    ARTIFACT_BUCKET, ArtifactLimits, DEFAULT_ARTIFACT_LIMITS, validate_artifact,
)
from src.ingestion.setup_workflow_tables import PROJECT


class ArtifactReadError(PermissionError):
    def __init__(self):
        super().__init__('The recorded artifact could not be verified.')


@dataclass(frozen=True, slots=True)
class ArtifactReadSettings:
    timeout_seconds: int = 20
    limits: ArtifactLimits = DEFAULT_ARTIFACT_LIMITS

    def __post_init__(self):
        if (type(self.timeout_seconds) is not int or not 1 <= self.timeout_seconds <= 60
                or not isinstance(self.limits, ArtifactLimits)):
            raise ValueError('Invalid bounded artifact-reader settings')


class GcsArtifactReader:
    """Server-configured ADC client; authorization remains the service's job."""

    def __init__(self, client: storage.Client, settings: ArtifactReadSettings | None = None):
        if client.project != PROJECT:
            raise ValueError('Artifact reads are scoped to the configured project')
        self.client = client
        self.settings = ArtifactReadSettings() if settings is None else settings
        if not isinstance(self.settings, ArtifactReadSettings):
            raise ValueError('Artifact-reader settings must be configured by the server')

    def read(self, record: AuthorizedArtifact) -> bytes:
        try:
            if not isinstance(record, AuthorizedArtifact):
                raise ArtifactReadError()
            manifest, output = record.artifact, record.metadata
            validate_artifact(asdict(manifest), workspace_id=output.workspace_id,
                conversation_id=output.conversation_id, canonical_sku=output.canonical_sku,
                output_id=output.output_id, limits=self.settings.limits)
            object_name = manifest.gcs_uri.removeprefix(f'gs://{ARTIFACT_BUCKET}/')
            generation = int(manifest.gcs_generation)
            # Pin the object selector AND the request precondition. Never fetch
            # the latest generation, a signed URL or a caller-supplied bucket.
            blob = self.client.bucket(ARTIFACT_BUCKET).blob(object_name, generation=generation)
            bounded_retry = Retry(deadline=self.settings.timeout_seconds)
            options = dict(if_generation_match=generation, retry=operation_retry(bounded_retry))
            operation_call(blob.reload, timeout=self.settings.timeout_seconds, **options)
            if (type(blob.generation) is not int or blob.generation != generation
                    or type(blob.size) is not int or blob.size != manifest.size_bytes
                    or blob.content_type != manifest.content_type or blob.content_encoding is not None):
                raise ArtifactReadError()
            # A range caps the payload at the declared size plus one byte even
            # if a faulty response disagrees with the immutable object metadata.
            # Raw download prevents GCS decompressive transcoding. Our SHA-256
            # check is mandatory even when GCS skips checksums on ranged reads.
            raw = operation_call(blob.download_as_bytes, start=0, end=manifest.size_bytes,
                raw_download=True, checksum='auto', timeout=self.settings.timeout_seconds, **options)
            if (not isinstance(raw, bytes) or len(raw) != manifest.size_bytes
                    or hashlib.sha256(raw).hexdigest() != manifest.sha256):
                raise ArtifactReadError()
            if manifest.kind == 'BANNER_PNG':
                with Image.open(BytesIO(raw)) as image:
                    # The current POC renderer admits exactly this canvas size.
                    if image.format != 'PNG' or image.size != (1200, 628) or image.n_frames != 1:
                        raise ArtifactReadError()
                    image.verify()
                with Image.open(BytesIO(raw)) as image:
                    image.load()
            elif manifest.kind == 'BRIEF_JSON':
                if raw != record.brief_json:
                    raise ArtifactReadError()
            else:
                text = raw.decode('utf-8')
                if not text.strip() or any((ord(c) < 32 and c not in '\n\r\t') or ord(c) == 127 for c in text):
                    raise ArtifactReadError()
            check_operation()
            return raw
        except OperationStopped:
            raise
        except Exception:
            # Never surface paths, credentials, cloud response details or bytes.
            raise ArtifactReadError() from None
