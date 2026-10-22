"""Server-sealed sharing intents; not an access grant or a business-data cache.

The signing key is runtime-only configuration. No key files, environment fallback
or local intent registry are created. Keep a stable managed key/configuration for
the seven-day intent lifetime; rotation/migration must be coordinated separately.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re

from src.application.access_policy import Visibility
from src.application.audit_contract import _opaque
from src.application.identity import VerifiedIdentity, ensure_fresh
from src.application.sharing_coordinator_contract import (
    SharingCommand, SharingCommitObservation, SharingCommitStatus, _within_window,
)
from src.application.sharing_query import SharingQuery, SharingQuerySettings
from src.application.workflow_contract import ContractError, _require, canonical_json_bytes
from src.ingestion.setup_workflow_tables import PROJECT

_DOMAIN = b'ci-sharing-intent-v1\0'


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode('ascii').rstrip('=')


def _unb64(value: str) -> bytes:
    decoded = base64.urlsafe_b64decode(value + '=' * (-len(value) % 4))
    _require(_b64(decoded) == value, 'INVALID_SHARING_SEAL')
    return decoded


def query_fingerprint(query: SharingQuery) -> str:
    """Pin exact SQL, requested configuration and execution scope before dispatch."""
    return hashlib.sha256(canonical_json_bytes(dict(project=PROJECT, location=query.location,
        sql=query.sql, config=query.job_config().to_api_repr()))).hexdigest()


@dataclass(frozen=True, slots=True)
class SharingAttempt:
    query: SharingQuery
    fingerprint: str
    job_id: str

    def __post_init__(self):
        _require(isinstance(self.query, SharingQuery), 'INVALID_SHARING_QUERY')
        _require(self.query.commit_observation.status is SharingCommitStatus.NOT_SUBMITTED,
                 'INVALID_SHARING_INITIAL_ATTEMPT')
        expected = query_fingerprint(self.query)
        _require(self.fingerprint == expected and self.job_id == 'share_v1_' + expected,
                 'SHARING_ATTEMPT_MISMATCH')


def initial_attempt(query: SharingQuery) -> SharingAttempt:
    fingerprint = query_fingerprint(query)
    return SharingAttempt(query, fingerprint, 'share_v1_' + fingerprint)


class SharingIntentSealer:
    """HMAC integrity envelope for immutable server-accepted command metadata.

    Tokens are signed, not encrypted; they must not contain private facts or
    raw assertions/subjects. They confer no role and never replace BigQuery reads.
    """
    def __init__(self, *, key: bytes, key_id: str, settings: SharingQuerySettings | None = None):
        _require(type(key) is bytes and 32 <= len(key) <= 64, 'INVALID_SHARING_SIGNING_KEY')
        _opaque(key_id)
        self._key, self.key_id = key, key_id
        self.settings = SharingQuerySettings() if settings is None else settings
        _require(isinstance(self.settings, SharingQuerySettings), 'INVALID_SHARING_QUERY_SETTINGS')

    def seal(self, query: SharingQuery) -> str:
        _require(query.settings == self.settings, 'SHARING_SETTINGS_MISMATCH')
        attempt = initial_attempt(query)
        command = query.command
        payload = dict(version='sharing-intent-v1', key_id=self.key_id,
            workspace_id=command.workspace_id, conversation_id=command.conversation_id,
            principal_id=command.principal_id, visibility=command.visibility.value,
            expected_membership_revision=command.expected_membership_revision,
            expected_conversation_revision=command.expected_conversation_revision,
            expected_workspace_revision=query.expected_workspace_revision,
            retry_key_sha256=command.retry_key_sha256,
            accepted_at=command.accepted_at.astimezone(timezone.utc).isoformat(),
            trace_id=command.trace_id, command_version=command.version,
            fingerprint=attempt.fingerprint)
        raw = canonical_json_bytes(payload)
        return 'share1.' + _b64(raw) + '.' + _b64(hmac.digest(self._key, _DOMAIN + raw, 'sha256'))

    def resolve(self, token: str, identity: VerifiedIdentity) -> SharingAttempt:
        ensure_fresh(identity)
        try:
            _require(isinstance(token, str) and len(token) <= 8192
                     and re.fullmatch(r'share1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}', token) is not None,
                     'INVALID_SHARING_SEAL')
            _, encoded, signature = token.split('.')
            raw, signed = _unb64(encoded), _unb64(signature)
            _require(hmac.compare_digest(hmac.digest(self._key, _DOMAIN + raw, 'sha256'), signed),
                     'INVALID_SHARING_SEAL')
            payload = json.loads(raw)
            fields = {'version', 'key_id', 'workspace_id', 'conversation_id', 'principal_id', 'visibility',
                      'expected_membership_revision', 'expected_conversation_revision',
                      'expected_workspace_revision', 'retry_key_sha256', 'accepted_at', 'trace_id',
                      'command_version', 'fingerprint'}
            _require(isinstance(payload, dict) and set(payload) == fields
                     and canonical_json_bytes(payload) == raw
                     and payload['version'] == 'sharing-intent-v1' and payload['key_id'] == self.key_id,
                     'INVALID_SHARING_SEAL')
            command = SharingCommand(payload['workspace_id'], payload['conversation_id'], payload['principal_id'],
                Visibility(payload['visibility']), payload['expected_membership_revision'],
                payload['expected_conversation_revision'], payload['retry_key_sha256'],
                datetime.fromisoformat(payload['accepted_at']), payload['trace_id'], payload['command_version'])
            _require(command.principal_id == identity.principal.principal_id, 'SHARING_IDENTITY_MISMATCH')
            _within_window(command, _now())
            query = SharingQuery(command, payload['expected_workspace_revision'],
                SharingCommitObservation(command.audit_id, SharingCommitStatus.NOT_SUBMITTED), self.settings)
            attempt = initial_attempt(query)
            _require(payload['fingerprint'] == attempt.fingerprint, 'SHARING_ATTEMPT_MISMATCH')
            return attempt
        except ContractError:
            raise
        except Exception:
            raise ContractError('INVALID_SHARING_SEAL') from None
