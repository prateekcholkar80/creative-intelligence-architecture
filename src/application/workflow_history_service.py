"""Authoritative Priority 4.7 conversation/job history and cold reconnect."""
from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
from copy import deepcopy

from google.cloud import bigquery

from src.application.access_policy import Action, require_authorized
from src.application.control_io import ControlIO
from src.application.conversation_contract import validate_conversation
from src.application.identity import IapIdentityAdapter, ensure_fresh
from src.application.management_service import parameter, require, table
from src.application.membership_contract import validate_membership
from src.application.operation_budget import OperationSettings, operation_scope
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import (
    ContractError, _id, canonical_json_bytes, validate_job,
)
from src.application.workspace_contract import validate_workspace

MAX_PAGE_SIZE = 50

HISTORY_SQL = f'''DECLARE page_at TIMESTAMP DEFAULT COALESCE(@page_at, CURRENT_TIMESTAMP());
BEGIN TRANSACTION;
CREATE TEMP TABLE history_page AS
SELECT CURRENT_TIMESTAMP() AS observed_at, page_at,
  ARRAY(SELECT TO_JSON_STRING(w) FROM {table('workspaces')} w
    WHERE workspace_id=@workspace_id LIMIT 2) AS workspaces,
  ARRAY(SELECT TO_JSON_STRING(m) FROM {table('memberships')} m
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id LIMIT 2) AS memberships,
  ARRAY(SELECT TO_JSON_STRING(c) FROM {table('conversations')} c
    WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id LIMIT 2) AS conversations,
  ARRAY(SELECT AS STRUCT j.job,
      ARRAY_AGG(IF(r.request_id IS NULL, NULL, TO_JSON_STRING(r))
        IGNORE NULLS LIMIT 2) AS requests
    FROM (SELECT TO_JSON_STRING(source) AS job, workspace_id, request_id,
        created_at, job_id, ROW_NUMBER() OVER() AS history_row
      FROM {table('jobs')} source
      WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id
        AND owner_id=@principal_id AND created_at<=page_at
        AND (@before_at IS NULL OR created_at<@before_at
          OR (created_at=@before_at AND job_id<@before_id))) j
    LEFT JOIN {table('requests')} r
      ON r.workspace_id=j.workspace_id AND r.request_id=j.request_id
        AND r.created_at<=page_at
    GROUP BY j.job, j.created_at, j.job_id, j.history_row
    ORDER BY j.created_at DESC, j.job_id DESC LIMIT @fetch_limit) AS records;
COMMIT TRANSACTION;
SELECT * FROM history_page;'''


@dataclass(frozen=True, slots=True)
class HistorySettings:
    enabled: bool = False
    cursor_key: bytes = field(default=b'', repr=False)
    cursor_ttl_seconds: int = 3600
    operation: OperationSettings = OperationSettings()

    def __post_init__(self):
        require(type(self.enabled) is bool and type(self.cursor_key) is bytes
            and type(self.cursor_ttl_seconds) is int
            and 60 <= self.cursor_ttl_seconds <= 86400
            and type(self.operation) is OperationSettings, 'INVALID_HISTORY_SETTINGS')
        if self.enabled:
            require(32 <= len(self.cursor_key) <= 64, 'INVALID_HISTORY_SETTINGS')


@dataclass(frozen=True, slots=True)
class HistoryJob:
    job_id: str
    request_id: str
    canonical_sku: str
    state: str
    revision: int
    output_id: str | None
    safe_error_code: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ConversationHistoryPage:
    workspace_id: str
    conversation_id: str
    visibility: str
    access_revision: int
    conversation_created_at: datetime
    conversation_updated_at: datetime
    jobs: tuple[HistoryJob, ...]
    next_cursor: str | None


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b'=').decode('ascii')


def _unb64(value: str) -> bytes:
    return base64.b64decode(value + '=' * (-len(value) % 4), altchars=b'-_', validate=True)


class ProtectedHistoryService:
    """Read one owner-scoped page after one fresh transactional access snapshot."""

    def __init__(self, *, client: bigquery.Client, verifier: IapIdentityAdapter,
                 settings: HistorySettings = HistorySettings()):
        require(isinstance(verifier, IapIdentityAdapter) and type(settings) is HistorySettings,
            'INVALID_HISTORY_SERVICE')
        self.verifier, self.settings = verifier, settings
        self.io = ControlIO(client)

    def _identity(self, headers):
        require(self.settings.enabled, 'HISTORY_DISABLED')
        identity = self.verifier.authenticate(headers)
        ensure_fresh(identity)
        return identity

    def _seal(self, value: dict) -> str:
        body = canonical_json_bytes(value)
        signature = hmac.new(self.settings.cursor_key, body, hashlib.sha256).digest()
        return _b64(body) + '.' + _b64(signature)

    def _open(self, cursor: str, *, identity, workspace_id: str, conversation_id: str):
        try:
            require(type(cursor) is str and 1 <= len(cursor) <= 2048 and cursor.count('.') == 1,
                'INVALID_HISTORY_CURSOR')
            body_text, signature_text = cursor.split('.')
            body, signature = _unb64(body_text), _unb64(signature_text)
            require(_b64(body) == body_text and _b64(signature) == signature_text,
                'INVALID_HISTORY_CURSOR')
            expected = hmac.new(self.settings.cursor_key, body, hashlib.sha256).digest()
            require(len(signature) == 32 and hmac.compare_digest(signature, expected),
                'INVALID_HISTORY_CURSOR')
            value = json.loads(body)
            require(type(value) is dict and canonical_json_bytes(value) == body
                and set(value) == {'version', 'workspace_id', 'conversation_id', 'principal_id',
                    'page_at', 'before_at', 'before_id'} and value['version'] == 1
                and value['workspace_id'] == workspace_id
                and value['conversation_id'] == conversation_id
                and value['principal_id'] == identity.principal.principal_id,
                'INVALID_HISTORY_CURSOR')
            page_at = datetime.fromisoformat(value['page_at'])
            before_at = datetime.fromisoformat(value['before_at'])
            _id(value['before_id'])
            now = datetime.now(timezone.utc)
            require(page_at.tzinfo is not None and before_at.tzinfo is not None
                and before_at <= page_at <= now + timedelta(seconds=30)
                and now - page_at <= timedelta(seconds=self.settings.cursor_ttl_seconds),
                'INVALID_HISTORY_CURSOR')
            return page_at, before_at, value['before_id']
        except ContractError as error:
            if error.code == 'INVALID_HISTORY_CURSOR':
                raise
            raise ContractError('INVALID_HISTORY_CURSOR') from None
        except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError,
                binascii.Error):
            raise ContractError('INVALID_HISTORY_CURSOR') from None

    def _access(self, row, identity, workspace_id, conversation_id):
        require(set(row) == {'observed_at', 'page_at', 'workspaces', 'memberships',
            'conversations', 'records'} and isinstance(row['observed_at'], datetime)
            and row['observed_at'].tzinfo is not None and isinstance(row['page_at'], datetime)
            and row['page_at'].tzinfo is not None and row['page_at'] <= row['observed_at']
            and len(row['workspaces']) == 1
            and len(row['memberships']) == 1 and len(row['conversations']) == 1
            and type(row['records']) is list, 'HISTORY_ACCESS_UNAVAILABLE')
        workspace = decode_row(row['workspaces'][0], 'workspaces')
        membership = decode_row(row['memberships'][0], 'memberships')
        conversation = decode_row(row['conversations'][0], 'conversations')
        validate_workspace(workspace, workspace_id=workspace_id)
        member = validate_membership(membership, workspace_id=workspace_id,
            principal_id=identity.principal.principal_id)
        resource = validate_conversation(conversation, workspace_id=workspace_id,
            conversation_id=conversation_id)
        require(workspace['status'] == membership['status'] == conversation['status'] == 'ACTIVE'
            and membership['identity_issuer'] == identity.issuer
            and membership['identity_subject'] == identity.subject
            and resource.owner_id == identity.principal.principal_id
            and max(workspace['updated_at'], membership['updated_at'], conversation['updated_at'])
                <= row['observed_at'], 'HISTORY_ACCESS_UNAVAILABLE')
        require_authorized(identity.principal, member, Action.READ_DRAFT,
            workspace_id=workspace_id, resource=resource)
        ensure_fresh(identity)
        return conversation, resource

    def list_jobs(self, headers, *, workspace_id: str, conversation_id: str,
                  page_size: int = 20, cursor: str | None = None) -> ConversationHistoryPage:
        with operation_scope(self.settings.operation):
            identity = self._identity(headers)
            _id(workspace_id); _id(conversation_id)
            require(type(page_size) is int and 1 <= page_size <= MAX_PAGE_SIZE,
                'INVALID_HISTORY_PAGE_SIZE')
            page_at = before_at = before_id = None
            if cursor is not None:
                page_at, before_at, before_id = self._open(cursor, identity=identity,
                    workspace_id=workspace_id, conversation_id=conversation_id)
            row = self.io.read(HISTORY_SQL, [parameter('workspace_id', workspace_id),
                parameter('principal_id', identity.principal.principal_id),
                parameter('conversation_id', conversation_id),
                parameter('page_at', page_at, 'TIMESTAMP'),
                parameter('before_at', before_at, 'TIMESTAMP'),
                parameter('before_id', before_id),
                parameter('fetch_limit', page_size + 1, 'INT64')])
            conversation, resource = self._access(row, identity, workspace_id, conversation_id)
            require(page_at is None or row['page_at'] == page_at, 'INVALID_HISTORY_CURSOR')
            require(len(row['records']) <= page_size + 1, 'INVALID_HISTORY_PAGE')
            jobs = []
            order = []
            for record in row['records']:
                require(type(record) is dict and set(record) == {'job', 'requests'}
                    and type(record['requests']) is list and len(record['requests']) == 1,
                    'INVALID_HISTORY_RECORD')
                job = decode_row(record['job'], 'jobs')
                request = decode_row(record['requests'][0], 'requests')
                validate_job(job, request=request, conversation=resource)
                require(job['owner_id'] == identity.principal.principal_id
                    and job['created_at'] <= row['page_at'], 'INVALID_HISTORY_RECORD')
                order.append((job['created_at'], job['job_id']))
                jobs.append(HistoryJob(**{name: deepcopy(job[name])
                    for name in HistoryJob.__dataclass_fields__}))
            require(len(set(order)) == len(order)
                and order == sorted(order, reverse=True), 'INVALID_HISTORY_ORDER')
            visible = tuple(jobs[:page_size])
            next_cursor = None
            if len(jobs) > page_size:
                last = visible[-1]
                next_cursor = self._seal(dict(version=1, workspace_id=workspace_id,
                    conversation_id=conversation_id,
                    principal_id=identity.principal.principal_id,
                    page_at=row['page_at'].isoformat(), before_at=last.created_at.isoformat(),
                    before_id=last.job_id))
            return ConversationHistoryPage(workspace_id, conversation_id,
                conversation['visibility'], conversation['access_revision'],
                conversation['created_at'], conversation['updated_at'], visible, next_cursor)
