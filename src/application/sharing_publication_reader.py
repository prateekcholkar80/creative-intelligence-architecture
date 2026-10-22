"""4.2 Critical/Must have/shared pilot: fresh authoritative signing metadata reads.

Fixed serving scope, ADC client, exact schema preflight and one bounded native
transaction. No publication/checkpoint mutation, key loading, fallback or READY.
Project number and consumer are trusted deployment inputs, not browser settings.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice
from uuid import uuid4

from google.cloud import bigquery

from src.application.sharing_jobs import matches_sharing_job
from src.application.operation_budget import OperationStopped, operation_call, operation_iter
from src.application.sharing_publication_codec import decode_read_result, normalize
from src.application.sharing_publication_contract import POLICY_ID
from src.application.sharing_publication_read_contract import (
    MAX_READ_HISTORY, MAX_READ_PUBLICATIONS, SigningReadLimits, SigningReadObservation, SigningReadRequest,
)
from src.application.sharing_publication_schema import TABLES
from src.application.workflow_contract import ContractError, MAX_JSON_BYTES, _require
from src.ingestion.setup_signing_tables import inspect
from src.ingestion.setup_workflow_tables import DATASET_ID, LOCATION, PROJECT


def _now() -> datetime:
    return datetime.now(timezone.utc)


def read_sql() -> str:
    """All resource expressions are installed constants; values are parameters.

    A temporary table retains the transaction snapshot after COMMIT. Aggregate
    selectors only choose a bounded interval; complete counts still reject any
    duplicate head/checkpoint. No version filter hides unsupported control rows.
    """
    refs = {name: f'`{DATASET_ID}.{name}`' for name in TABLES}
    projections = []
    for table, cte, result, limit in (
        ('signing_heads', 'heads', 'heads', 2),
        ('signing_checkpoints', 'checkpoints', 'checkpoints', 2),
        ('signing_publications', 'publications', 'publications', MAX_READ_PUBLICATIONS + 1),
        ('signing_key_history', 'history', 'history', MAX_READ_HISTORY + 1),
    ):
        columns = ', '.join(f.name if f.name != 'config_canonical_json' else
            f'IF(OCTET_LENGTH(config_canonical_json) <= {MAX_JSON_BYTES}, '
            'config_canonical_json, NULL) AS config_canonical_json' for f in TABLES[table].schema)
        projections.append(f'ARRAY(SELECT AS STRUCT {columns} FROM {cte} LIMIT {limit}) AS {result}')
    arrays = ',\n    '.join(projections)
    return f'''BEGIN TRANSACTION;
CREATE TEMP TABLE signing_read_snapshot AS
WITH heads AS (SELECT * FROM {refs['signing_heads']} WHERE policy_id=@policy_id),
checkpoints AS (SELECT * FROM {refs['signing_checkpoints']}
               WHERE policy_id=@policy_id AND consumer_id=@consumer_id),
bounds AS (SELECT
    COALESCE((SELECT MAX(revision) FROM heads), 0) AS last_revision,
    (SELECT MIN(publication_revision) FROM checkpoints) AS checkpoint_revision,
    IF(@base_revision > 0, @base_revision,
       COALESCE((SELECT MIN(publication_revision) FROM checkpoints), 1)) AS first_revision),
publications AS (SELECT p.* FROM {refs['signing_publications']} p CROSS JOIN bounds b
    WHERE p.policy_id=@policy_id AND
      (p.revision BETWEEN b.first_revision AND b.last_revision OR p.revision=b.checkpoint_revision)),
history AS (SELECT h.* FROM {refs['signing_key_history']} h CROSS JOIN bounds b
    WHERE h.policy_id=@policy_id AND
      (h.publication_revision BETWEEN b.first_revision AND b.last_revision
       OR h.publication_revision=b.checkpoint_revision))
SELECT CURRENT_TIMESTAMP() AS observed_at,
    @consumer_id AS consumer_id, @project_number AS project_number,
    @instance_id AS instance_id, @generation AS generation,
    @base_revision AS base_revision, NULLIF(@base_digest, '') AS base_digest,
    (SELECT COUNT(*) FROM heads) AS head_count,
    (SELECT COUNT(*) FROM checkpoints) AS checkpoint_count,
    (SELECT COUNT(*) FROM publications) AS publication_count,
    (SELECT COUNT(*) FROM history) AS history_count,
    (SELECT COUNT(*) FROM {refs['signing_publications']} p CROSS JOIN bounds b
     WHERE p.policy_id=@policy_id AND p.revision>b.last_revision) AS unheaded_publication_count,
    {arrays};
COMMIT TRANSACTION;
SELECT * FROM signing_read_snapshot;'''


def read_config(request: SigningReadRequest, limits: SigningReadLimits) -> bigquery.QueryJobConfig:
    _require(isinstance(request, SigningReadRequest) and isinstance(limits, SigningReadLimits),
             'INVALID_SIGNING_READ_REQUEST')
    ticket = request.ticket
    values = dict(policy_id=('STRING', POLICY_ID), consumer_id=('STRING', request.consumer_id),
        project_number=('STRING', request.project_number), instance_id=('STRING', ticket.instance_id),
        generation=('INT64', ticket.generation), base_revision=('INT64', ticket.base_revision),
        # BigQuery omits parameterValue for NULL in returned job metadata.
        # A digest is 64 hex characters, so empty is an unambiguous absent pin.
        base_digest=('STRING', ticket.base_digest or ''))
    return bigquery.QueryJobConfig(use_legacy_sql=False, use_query_cache=False, create_session=False,
        maximum_bytes_billed=limits.maximum_bytes_billed, job_timeout_ms=limits.job_timeout_ms,
        script_options=bigquery.ScriptOptions(statement_timeout_ms=limits.job_timeout_ms),
        query_parameters=[bigquery.ScalarQueryParameter(name, kind, value)
                          for name, (kind, value) in values.items()],
        labels={'workflow': 'signing-read-v1'})


class SigningReadError(RuntimeError):
    """Safe codes only; no raw SQL, configuration, provider messages or credentials."""
    def __init__(self, code: str):
        self.code = code
        super().__init__(f'Signing metadata read failed: {code}')


@dataclass(frozen=True, slots=True)
class SigningCapturedRead:
    """Server-internal validated rows; construction alone never proves live I/O."""
    request: SigningReadRequest
    observation: SigningReadObservation
    _rows: dict = field(repr=False, compare=False)

    def __post_init__(self):
        _require(isinstance(self.request, SigningReadRequest)
                 and isinstance(self.observation, SigningReadObservation), 'INVALID_SIGNING_CAPTURE')
        object.__setattr__(self, '_rows', deepcopy(self._rows))

    @property
    def rows(self) -> dict:
        return deepcopy(self._rows)


class BigQuerySigningReader:
    def __init__(self, *, client: bigquery.Client, limits: SigningReadLimits = SigningReadLimits()):
        _require(isinstance(client, bigquery.Client) and client.project == PROJECT
                 and client.location == LOCATION, 'INVALID_SIGNING_READ_CLIENT')
        _require(isinstance(limits, SigningReadLimits), 'INVALID_SIGNING_READ_LIMITS')
        self.client = client
        self.limits = limits

    def read(self, request: SigningReadRequest) -> SigningReadObservation:
        return self.capture(request).observation

    def capture(self, request: SigningReadRequest) -> SigningCapturedRead:
        _require(isinstance(request, SigningReadRequest), 'INVALID_SIGNING_READ_REQUEST')
        # No configurable serving tables, cached preflight, cached observations or
        # recovery resubmission. A failed read requires a new explicit refresh.
        try:
            _require(self.client.project == PROJECT and self.client.location == LOCATION,
                     'INVALID_SIGNING_READ_CLIENT')
            _require(self.client.default_query_job_config is None,
                     'INVALID_SIGNING_READ_CLIENT_DEFAULTS')
            _require(inspect(self.client)['status'] == 'VERIFIED', 'SIGNING_READ_RESOURCES_MISSING')
            sql, config = read_sql(), read_config(request, self.limits)
            expected_config = bigquery.QueryJobConfig.from_api_repr(deepcopy(config.to_api_repr()))
            job_id = 'signing_read_' + uuid4().hex
            job = operation_call(self.client.query, sql, job_config=config, job_id=job_id, project=PROJECT,
                location=LOCATION, retry=None, job_retry=None, timeout=self.limits.result_timeout_seconds)
            _require(matches_sharing_job(job, job_id=job_id, sql=sql, config=expected_config),
                     'SIGNING_READ_JOB_MISMATCH')
            result = operation_call(job.result, max_results=2, page_size=2, retry=None, job_retry=None,
                                    timeout=self.limits.result_timeout_seconds)
            rows = list(islice(operation_iter(result), 2))
            _require(job.state == 'DONE' and job.error_result is None
                     and matches_sharing_job(job, job_id=job_id, sql=sql, config=expected_config),
                     'SIGNING_READ_JOB_MISMATCH')
            normalized = [normalize(row) for row in rows]
            observation = decode_read_result(request, normalized, now=_now(), limits=self.limits)
            return SigningCapturedRead(request, observation, normalized[0])
        except OperationStopped:
            raise
        except ContractError as error:
            raise SigningReadError(error.code) from None
        except Exception:
            raise SigningReadError('SIGNING_READ_STORE_UNAVAILABLE') from None
