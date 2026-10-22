"""E2 authoritative persisted-request bindings; no transcript or release grant.

Uses existing workflow-v1 contracts, including their existing catalog scope.
Server composition supplies an already verified identity. No browser role,
local history, model dispatch, storage migration or serving integration.
"""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import islice
import json
from uuid import uuid4

from google.cloud import bigquery

from src.application.access_policy import Action, require_authorized
from src.application.campaign_acceptance_contract import _hash, _utc
from src.application.conversation_contract import validate_conversation
from src.application.identity import VerifiedIdentity, ensure_fresh
from src.application.membership_contract import validate_membership
from src.application.operation_budget import operation_call, operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_codec import decode_row, encode_row
from src.application.workflow_contract import _id, _require, canonical_json_bytes, validate_request
from src.application.workspace_contract import validate_workspace
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.retrieval.request import CANONICAL_SKUS
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES

MAX_REQUESTS = 50
MAX_ROW_BYTES = 64 * 1024
MAX_CAPTURE_BYTES = 1024 * 1024
FRESHNESS = timedelta(seconds=60)
DATASET = f'{PROJECT}.ci_application'


def partition_predicate(table):
    """Complete history window satisfies required native partition filters."""
    spec=WORKFLOW_TABLES.get(table)
    column=None if spec is None else spec.partition_field
    return (f" AND {column} BETWEEN TIMESTAMP('0001-01-01') "
        "AND TIMESTAMP('9999-12-31 23:59:59.999999+00')") if column else ''


def _rows(table, predicate, limit, order=''):
    return f'''ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(r))<=@row_bytes,
        TO_JSON_STRING(r), NULL) FROM `{DATASET}.{table}` r WHERE {predicate}{partition_predicate(table)}
        {order} LIMIT {limit}) AS {table}'''


SQL = f'''BEGIN TRANSACTION;
CREATE TEMP TABLE quality_request_capture AS SELECT CURRENT_TIMESTAMP() AS observed_at,
{_rows('workspaces', 'workspace_id=@workspace_id', 2)},
{_rows('memberships', 'workspace_id=@workspace_id AND principal_id=@principal_id', 2)},
{_rows('conversations', 'workspace_id=@workspace_id AND conversation_id=@conversation_id', 2)},
{_rows('requests', 'workspace_id=@workspace_id AND conversation_id=@conversation_id', MAX_REQUESTS+1,
       'ORDER BY created_at, request_id')},
(SELECT COUNT(*) FROM `{DATASET}.requests`
 WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id{partition_predicate('requests')}) AS request_count;
COMMIT TRANSACTION;
SELECT * FROM quality_request_capture;'''


@dataclass(frozen=True, slots=True)
class RequestContextRead:
    """Server-only capture; hashes pin persisted metadata, not unseen chat text."""
    bindings_canonical_json: str
    bindings_sha256: str
    native_job_id: str
    observed_at: datetime
    valid_until: datetime


def _decode_capture(row, *, workspace_id, conversation_id, request_id, identity, started, ended,
                    catalog=CANONICAL_SKUS):
    _require(type(row) is dict and set(row) == {'observed_at', 'workspaces', 'memberships',
        'conversations', 'requests', 'request_count'}, 'QUALITY_REQUEST_CAPTURE_SHAPE')
    observed = _utc(row['observed_at'])
    _require(started <= observed <= ended < started + FRESHNESS, 'QUALITY_REQUEST_CAPTURE_EXPIRED')
    for name in ('workspaces', 'memberships', 'conversations', 'requests'):
        values = row[name]
        _require(type(values) is list and all(type(text) is str
            and 0 < len(text.encode('utf-8')) <= MAX_ROW_BYTES for text in values),
            'QUALITY_REQUEST_CAPTURE_SIZE')
    _require(sum(len(text.encode('utf-8')) for name in ('workspaces', 'memberships',
        'conversations', 'requests') for text in row[name]) <= MAX_CAPTURE_BYTES,
        'QUALITY_REQUEST_CAPTURE_SIZE')
    _require(all(len(row[name]) == 1 for name in ('workspaces', 'memberships', 'conversations')),
        'QUALITY_REQUEST_ACCESS_COVERAGE')
    _require(type(row['request_count']) is int and 1 <= row['request_count'] <= MAX_REQUESTS
        and len(row['requests']) == row['request_count'], 'QUALITY_REQUEST_HISTORY_INCOMPLETE')
    workspace = decode_row(row['workspaces'][0], 'workspaces')
    membership = decode_row(row['memberships'][0], 'memberships')
    conversation = decode_row(row['conversations'][0], 'conversations')
    validate_workspace(workspace, workspace_id=workspace_id)
    member = validate_membership(membership, workspace_id=workspace_id,
        principal_id=identity.principal.principal_id)
    resource = validate_conversation(conversation, workspace_id=workspace_id, conversation_id=conversation_id)
    _require(workspace['status'] == membership['status'] == conversation['status'] == 'ACTIVE'
        and membership['identity_issuer'] == identity.issuer and membership['identity_subject'] == identity.subject
        and resource.owner_id == identity.principal.principal_id
        and max(workspace['updated_at'], membership['updated_at'], conversation['updated_at']) <= observed,
        'QUALITY_REQUEST_ACCESS_DENIED')
    require_authorized(identity.principal, member, Action.READ_DRAFT, workspace_id=workspace_id, resource=resource)
    requests = [decode_row(text, 'requests') for text in row['requests']]
    for request in requests:
        validate_request(request, conversation=resource, catalog=catalog)
        _require(conversation['created_at'] <= request['created_at'] <= request['updated_at'] <= observed,
            'QUALITY_REQUEST_TIMELINE')
    keys = [(request['created_at'], request['request_id']) for request in requests]
    _require(keys == sorted(keys) and len({r['request_id'] for r in requests}) == len(requests),
        'QUALITY_REQUEST_HISTORY_ORDER')
    selected = [request for request in requests if request['request_id'] == request_id]
    _require(len(selected) == 1, 'QUALITY_REQUEST_NOT_FOUND')
    request = selected[0]
    # Normalize storage JSON key order/timestamp spelling before hashing. Bind
    # every stored request field, not just the input fingerprint or last turn.
    projection = lambda item, table: json.loads(encode_row(item, table))
    history = dict(version='workflow-request-history-v1',
        conversation=projection(conversation, 'conversations'),
        requests=[dict(request_id=item['request_id'], revision=item['revision'],
            record_sha256=_hash(projection(item, 'requests'))) for item in requests])
    return dict(version='campaign-quality-request-bindings-v1', workspace_id=workspace_id,
        conversation_id=conversation_id, request_id=request_id,
        principal_id=identity.principal.principal_id,
        access_sha256=_hash(dict(workspace=projection(workspace, 'workspaces'),
            membership=projection(membership, 'memberships'), conversation=history['conversation'])),
        request_sha256=request['request_sha256'], message_sha256=request['message_sha256'],
        request_record_sha256=_hash(projection(request, 'requests')),
        request_revision=request['revision'], operation=request['operation'], request_status=request['request_status'],
        canonical_sku=request['canonical_sku'], channel=request['channel'],
        history_kind='PERSISTED_REQUEST_RECORDS', history_count=len(requests), history_sha256=_hash(history))


def read_request_context(client, *, workspace_id, conversation_id, request_id, identity,
                         clock=lambda: datetime.now(timezone.utc)):
    """One bounded atomic snapshot, independently correlated to the native job.

    Hashes describe all current stored requests in this conversation (up to 50),
    including the selected request. They do not represent assistant messages,
    output images, raw input text, request-time history or an approval.
    """
    for value in (workspace_id, conversation_id, request_id):
        _id(value)
    _require(type(identity) is VerifiedIdentity, 'QUALITY_REQUEST_IDENTITY_REQUIRED')
    ensure_fresh(identity)
    _require(client.project == PROJECT and client.location == LOCATION
        and client.default_query_job_config is None, 'QUALITY_REQUEST_CLIENT_SCOPE')
    started = _utc(clock())
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('workspace_id', 'STRING', workspace_id),
        bigquery.ScalarQueryParameter('conversation_id', 'STRING', conversation_id),
        bigquery.ScalarQueryParameter('principal_id', 'STRING', identity.principal.principal_id),
        bigquery.ScalarQueryParameter('row_bytes', 'INT64', MAX_ROW_BYTES)],
        use_legacy_sql=False, use_query_cache=False, create_session=False,
        maximum_bytes_billed=256 * 1024**2, job_timeout_ms=60000,
        script_options=bigquery.ScriptOptions(statement_timeout_ms=20000))
    expected = bigquery.QueryJobConfig.from_api_repr(deepcopy(cfg.to_api_repr()))
    native_id = 'quality_request_read_' + uuid4().hex
    submitted = operation_call(client.query, SQL, job_config=cfg, job_id=native_id,
        project=PROJECT, location=LOCATION, retry=None, job_retry=None, timeout=20)
    _require(matches_sharing_job(submitted, job_id=native_id, sql=SQL, config=expected),
        'QUALITY_REQUEST_NATIVE_MISMATCH')
    # Wait for this exact job only. The independent fetch then supplies both
    # final metadata and result rows; never mix results from another query.
    operation_call(submitted.result, max_results=0, retry=None, job_retry=None, timeout=60)
    native = operation_call(client.get_job, native_id, project=PROJECT, location=LOCATION, retry=None, timeout=20)
    _require(matches_sharing_job(native, job_id=native_id, sql=SQL, config=expected)
        and native.state == 'DONE' and native.error_result is None, 'QUALITY_REQUEST_NATIVE_MISMATCH')
    rows = list(islice(operation_iter(operation_call(native.result, max_results=2, page_size=2,
        retry=None, job_retry=None, timeout=20)), 2))
    _require(matches_sharing_job(native, job_id=native_id, sql=SQL, config=expected)
        and native.state == 'DONE' and native.error_result is None and len(rows) == 1,
        'QUALITY_REQUEST_NATIVE_MISMATCH')
    ended = _utc(clock())
    bindings = _decode_capture(dict(rows[0]), workspace_id=workspace_id, conversation_id=conversation_id,
        request_id=request_id, identity=identity, started=started, ended=ended)
    ensure_fresh(identity)
    encoded = canonical_json_bytes(bindings).decode()
    return RequestContextRead(encoded, _hash(bindings), native_id, _utc(rows[0]['observed_at']), started + FRESHNESS)
