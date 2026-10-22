"""Offline controlled-retry proposals and runtime seals; no dispatch or proof.

NOT_COMMITTED observations must be produced by a trusted native-job verifier,
not supplied by a browser. A seal protects integrity, not current authorization
or commit truth. The executor must freshly verify the predecessor, controls and
receipt before issuing or submitting a retry. Existing execution stays disabled.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re

from src.application.identity import VerifiedIdentity, ensure_fresh
from src.application.sharing_coordinator_contract import (
    SharingCommitObservation, SharingCommitStatus, _intent_revision, _utc, _within_window,
)
from src.application.sharing_intent import (
    SharingAttempt, SharingIntentSealer, _b64, _unb64, initial_attempt, query_fingerprint,
)
from src.application.sharing_query import SharingQuery
from src.application.workflow_contract import ContractError, _require, canonical_json_bytes
from src.application.workspace_contract import MAX_REVISION

RETRY_VERSION = 'sharing-retry-v1'
MAX_RETRIES = 3
_DOMAIN = b'ci-sharing-retry-v1\0'


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class SharingRetryStep:
    expected_workspace_revision: int
    predecessor_observation: SharingCommitObservation
    observed_at: datetime

    def __post_init__(self):
        _intent_revision(self.expected_workspace_revision)
        _require(self.expected_workspace_revision < MAX_REVISION, 'WORKSPACE_REVISION_EXHAUSTED')
        _require(isinstance(self.predecessor_observation, SharingCommitObservation)
                 and self.predecessor_observation.status is SharingCommitStatus.NOT_COMMITTED,
                 'SHARING_RETRY_REQUIRES_NONCOMMIT')
        _utc(self.observed_at)


@dataclass(frozen=True, slots=True)
class SharingRetryAttempt:
    original: SharingAttempt
    steps: tuple[SharingRetryStep, ...]
    query: SharingQuery = field(init=False)
    fingerprint: str = field(init=False)
    job_id: str = field(init=False)

    def __post_init__(self):
        _require(isinstance(self.original, SharingAttempt), 'INVALID_SHARING_INITIAL_ATTEMPT')
        _require(initial_attempt(self.original.query) == self.original, 'SHARING_ATTEMPT_MISMATCH')
        _require(type(self.steps) is tuple and 1 <= len(self.steps) <= MAX_RETRIES,
                 'INVALID_SHARING_RETRY_CHAIN')
        command = self.original.query.command
        previous_job = self.original.job_id
        previous_fence = self.original.query.expected_workspace_revision
        previous_time = _utc(command.accepted_at)
        for ordinal, step in enumerate(self.steps, start=1):
            _require(isinstance(step, SharingRetryStep), 'INVALID_SHARING_RETRY_STEP')
            observation = step.predecessor_observation
            _require(observation.audit_id == command.audit_id
                     and observation.query_job_id == previous_job,
                     'SHARING_RETRY_PREDECESSOR_MISMATCH')
            _within_window(command, step.observed_at)
            _require(_utc(step.observed_at) >= previous_time, 'INVALID_SHARING_RETRY_TIME')
            _require(step.expected_workspace_revision >= previous_fence,
                     'SHARING_RETRY_FENCE_REGRESSION')
            # Prior rollback is NOT the new job's commit status. A competing
            # attempt's receipt can therefore be reused by the existing SQL.
            query = SharingQuery(command, step.expected_workspace_revision,
                SharingCommitObservation(command.audit_id, SharingCommitStatus.NOT_SUBMITTED),
                self.original.query.settings)
            fingerprint = hashlib.sha256(canonical_json_bytes(dict(
                version=RETRY_VERSION, original_fingerprint=self.original.fingerprint,
                predecessor_job_id=previous_job, ordinal=ordinal,
                query_fingerprint=query_fingerprint(query)))).hexdigest()
            previous_job = 'share_retry_v1_' + fingerprint
            previous_fence = step.expected_workspace_revision
            previous_time = _utc(step.observed_at)
        object.__setattr__(self, 'query', query)
        object.__setattr__(self, 'fingerprint', fingerprint)
        object.__setattr__(self, 'job_id', previous_job)


def prepare_retry(predecessor: SharingAttempt | SharingRetryAttempt, *,
                  commit_observation: SharingCommitObservation,
                  expected_workspace_revision: int, observed_at: datetime) -> SharingRetryAttempt:
    """Validate a supplied proposal; does not prove rollback or read a fence.

    Observation time is retained in the seal but excluded from job identity so
    replicas verifying the same predecessor/fence converge on one native job ID.
    """
    _require(isinstance(predecessor, (SharingAttempt, SharingRetryAttempt)),
             'INVALID_SHARING_RETRY_PREDECESSOR')
    original = predecessor.original if isinstance(predecessor, SharingRetryAttempt) else predecessor
    steps = predecessor.steps if isinstance(predecessor, SharingRetryAttempt) else ()
    return SharingRetryAttempt(original, steps + (SharingRetryStep(
        expected_workspace_revision, commit_observation, observed_at),))


class SharingRetrySealer(SharingIntentSealer):
    """Domain-separated retry seals, retaining unchanged share1 behavior.

    Trusted runtime keys/settings are inherited; no key registry or local state.
    Neither resolver grants access. Original SQL/settings remain version-pinned.
    """
    def seal_retry(self, attempt: SharingRetryAttempt) -> str:
        _require(isinstance(attempt, SharingRetryAttempt), 'INVALID_SHARING_RETRY_ATTEMPT')
        _require(SharingRetryAttempt(attempt.original, attempt.steps) == attempt,
                 'SHARING_ATTEMPT_MISMATCH')
        now = _now()
        _within_window(attempt.query.command, now)
        _require(_utc(attempt.steps[-1].observed_at) <= now, 'INVALID_SHARING_RETRY_TIME')
        payload = dict(version=RETRY_VERSION, key_id=self.key_id,
            original_token=self.seal(attempt.original.query), fingerprint=attempt.fingerprint,
            steps=[dict(expected_workspace_revision=step.expected_workspace_revision,
                        predecessor_job_id=step.predecessor_observation.query_job_id,
                        predecessor_status=step.predecessor_observation.status.value,
                        observed_at=_utc(step.observed_at).isoformat()) for step in attempt.steps])
        raw = canonical_json_bytes(payload)
        token = 'share-retry1.' + _b64(raw) + '.' + _b64(hmac.digest(self._key, _DOMAIN + raw, 'sha256'))
        _require(len(token) <= 8192, 'INVALID_SHARING_RETRY_SEAL')
        return token

    def resolve_retry(self, token: str, identity: VerifiedIdentity) -> SharingRetryAttempt:
        ensure_fresh(identity)
        try:
            _require(isinstance(token, str) and len(token) <= 8192 and re.fullmatch(
                r'share-retry1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}', token) is not None,
                'INVALID_SHARING_RETRY_SEAL')
            _, encoded, signature = token.split('.')
            raw, signed = _unb64(encoded), _unb64(signature)
            _require(hmac.compare_digest(hmac.digest(self._key, _DOMAIN + raw, 'sha256'), signed),
                     'INVALID_SHARING_RETRY_SEAL')
            payload = json.loads(raw)
            _require(isinstance(payload, dict) and set(payload) == {
                'version', 'key_id', 'original_token', 'fingerprint', 'steps'}
                and canonical_json_bytes(payload) == raw
                and payload['version'] == RETRY_VERSION and payload['key_id'] == self.key_id,
                'INVALID_SHARING_RETRY_SEAL')
            _require(type(payload['steps']) is list and 1 <= len(payload['steps']) <= MAX_RETRIES,
                     'INVALID_SHARING_RETRY_CHAIN')
            original = self.resolve(payload['original_token'], identity)
            steps = []
            for item in payload['steps']:
                _require(isinstance(item, dict) and set(item) == {
                    'expected_workspace_revision', 'predecessor_job_id', 'predecessor_status', 'observed_at'},
                    'INVALID_SHARING_RETRY_SEAL')
                steps.append(SharingRetryStep(item['expected_workspace_revision'],
                    SharingCommitObservation(original.query.command.audit_id,
                        SharingCommitStatus(item['predecessor_status']), item['predecessor_job_id']),
                    datetime.fromisoformat(item['observed_at'])))
            attempt = SharingRetryAttempt(original, tuple(steps))
            _require(payload['fingerprint'] == attempt.fingerprint, 'SHARING_ATTEMPT_MISMATCH')
            _require(_utc(attempt.steps[-1].observed_at) <= _now(), 'INVALID_SHARING_RETRY_TIME')
            return attempt
        except ContractError:
            raise
        except Exception:
            raise ContractError('INVALID_SHARING_RETRY_SEAL') from None
