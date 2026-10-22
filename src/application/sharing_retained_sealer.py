"""Opt-in retained v1 handlers with injected bytes; no Secret Manager/client I/O."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import re
from types import MappingProxyType
from typing import Mapping

from src.application.identity import VerifiedIdentity, ensure_fresh
from src.application.sharing_coordinator_contract import _within_window
from src.application.sharing_intent import SharingAttempt, _unb64, initial_attempt
from src.application.sharing_query import SharingQuery, SharingQuerySettings
from src.application.sharing_retry import SharingRetryAttempt, SharingRetrySealer
from src.application.sharing_signing_contract import SharingSigningKey, SharingSigningPolicy
from src.application.workflow_contract import ContractError, _require


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RetainedSharingSealer(SharingRetrySealer):
    """Select allowlisted local keys/profiles without changing existing v1 tokens.

    A retired key can sign bounded retries for its original acceptance cohort,
    but cannot issue new initial intents. Both layers retain the original key
    and profile. Replacement instances must receive identical trusted snapshots
    and pinned material. Revocation only takes effect once that snapshot is used.
    """
    def __init__(self, *, policy: SharingSigningPolicy, key_material: Mapping[str, bytes]):
        _require(isinstance(policy, SharingSigningPolicy), 'INVALID_SHARING_SIGNING_POLICY')
        _require(isinstance(key_material, Mapping) and set(key_material) == {
            key.key_id for key in policy.keys if not key.revoked}, 'SHARING_SIGNING_MATERIAL_UNAVAILABLE')
        material = dict(key_material)
        _require(all(type(value) is bytes and 32 <= len(value) <= 64 for value in material.values()),
                 'INVALID_SHARING_SIGNING_KEY')
        _require(len(set(material.values())) == len(material), 'SHARING_SIGNING_MATERIAL_REUSED')
        handlers = {}
        for key in policy.keys:
            if key.revoked:
                continue
            profile = policy.profile(key)
            profile.check_installed()
            handlers[key.key_id] = SharingRetrySealer(key=material[key.key_id],
                                                    key_id=key.key_id, settings=profile.settings)
        # Intentionally do not initialize the single-key superclass. All public
        # seal/resolve methods delegate to the per-key, pinned legacy handlers.
        self.policy = policy
        self._handlers = MappingProxyType(handlers)

    @property
    def settings(self) -> SharingQuerySettings:
        return self.policy.profile(self.policy.key(self.policy.active_key_id)).settings

    @property
    def key_id(self) -> str:
        return self.policy.active_key_id

    def _handler(self, key: SharingSigningKey, now: datetime) -> SharingRetrySealer:
        _require(not key.revoked, 'SHARING_SIGNING_KEY_REVOKED')
        _require(now < key.retain_until, 'SHARING_SIGNING_KEY_EXPIRED')
        self.policy.profile(key).check_installed()
        return self._handlers[key.key_id]

    def _check_attempt(self, key: SharingSigningKey, attempt: SharingAttempt | SharingRetryAttempt,
                       now: datetime) -> None:
        command = attempt.query.command
        _within_window(command, now)
        _require(key.issue_from <= command.accepted_at < key.issue_until,
                 'SHARING_SIGNING_ACCEPTANCE_MISMATCH')
        _require(attempt.query.settings == self.policy.profile(key).settings, 'SHARING_SETTINGS_MISMATCH')

    def _token_key(self, token: str, *, retry: bool) -> SharingSigningKey:
        """An unverified hint can select only one in-memory allowlisted handler.

        Signature/canonical form, exact fields and nested same-key identity are
        verified by the legacy handler before returning any attempt.
        """
        prefix = 'share-retry1' if retry else 'share1'
        code = 'INVALID_SHARING_RETRY_SEAL' if retry else 'INVALID_SHARING_SEAL'
        try:
            _require(type(token) is str and len(token) <= 8192 and re.fullmatch(
                prefix + r'\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}', token) is not None, code)
            payload = json.loads(_unb64(token.split('.')[1]))
            _require(type(payload) is dict and type(payload.get('key_id')) is str, code)
            return self.policy.key(payload['key_id'])
        except ContractError:
            raise
        except Exception:
            raise ContractError(code) from None

    def check_attempt(self, attempt: SharingAttempt | SharingRetryAttempt,
                      identity: VerifiedIdentity) -> None:
        """Check a previously resolved server attempt against this current policy.

        This does not verify a token signature, authenticate headers, check current
        conversation access or grant dispatch. The executor must resolve the seal
        first; this check preserves its exact cohort/settings across later gates.
        """
        ensure_fresh(identity)
        if isinstance(attempt, SharingRetryAttempt):
            _require(SharingRetryAttempt(attempt.original, attempt.steps) == attempt,
                     'SHARING_ATTEMPT_MISMATCH')
        else:
            _require(isinstance(attempt, SharingAttempt) and initial_attempt(attempt.query) == attempt,
                     'SHARING_ATTEMPT_MISMATCH')
        command = attempt.query.command
        _require(command.principal_id == identity.principal.principal_id, 'SHARING_IDENTITY_MISMATCH')
        candidates = [key for key in self.policy.keys
                      if key.issue_from <= command.accepted_at < key.issue_until]
        _require(len(candidates) == 1, 'SHARING_SIGNING_KEY_UNAVAILABLE')
        now = _now()
        self._handler(candidates[0], now)
        self._check_attempt(candidates[0], attempt, now)

    def seal(self, query: SharingQuery) -> str:
        _require(isinstance(query, SharingQuery), 'INVALID_SHARING_QUERY')
        now = _now()
        key = self.policy.key(self.policy.active_key_id)
        handler = self._handler(key, now)
        _require(key.issue_from <= now < key.issue_until, 'SHARING_SIGNING_KEY_NOT_ISSUING')
        self._check_attempt(key, initial_attempt(query), now)
        return handler.seal(query)

    def resolve(self, token: str, identity: VerifiedIdentity) -> SharingAttempt:
        ensure_fresh(identity)
        now = _now()
        key = self._token_key(token, retry=False)
        attempt = self._handler(key, now).resolve(token, identity)
        self._check_attempt(key, attempt, now)
        return attempt

    def seal_retry(self, attempt: SharingRetryAttempt) -> str:
        _require(isinstance(attempt, SharingRetryAttempt), 'INVALID_SHARING_RETRY_ATTEMPT')
        now = _now()
        accepted_at = attempt.query.command.accepted_at
        candidates = [key for key in self.policy.keys if key.issue_from <= accepted_at < key.issue_until]
        _require(len(candidates) == 1, 'SHARING_SIGNING_KEY_UNAVAILABLE')
        key = candidates[0]
        handler = self._handler(key, now)
        self._check_attempt(key, attempt, now)
        return handler.seal_retry(attempt)

    def resolve_retry(self, token: str, identity: VerifiedIdentity) -> SharingRetryAttempt:
        ensure_fresh(identity)
        now = _now()
        key = self._token_key(token, retry=True)
        attempt = self._handler(key, now).resolve_retry(token, identity)
        self._check_attempt(key, attempt, now)
        return attempt
