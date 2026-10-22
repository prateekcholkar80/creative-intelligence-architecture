"""Pure, trusted signing-policy snapshots; no secret loading or authorization.

Key IDs select local allowlisted handlers, never a user-supplied secret path.
Retain old policies/handlers through the original intent lifetime. Revocation is
an explicit emergency exception; publishing and synchronizing it is separate.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import hashlib
import re

from src.application.access_policy import POLICY_VERSION
from src.application.identity import ISSUER
from src.application.sharing_coordinator_contract import SHARING_CONTRACT_VERSION, _utc
from src.application.sharing_query import SharingQuerySettings, _render_sql
from src.application.sharing_retry import MAX_RETRIES, RETRY_VERSION
from src.application.workflow_contract import RETRY_WINDOW, _digest, _require, canonical_json_bytes
from src.application.workflow_schema import DATASET, LOCATION, SCHEMA_VERSION, TABLES
from src.ingestion.setup_workflow_tables import PROJECT

SIGNING_POLICY_VERSION = 'sharing-signing-policy-v1'
HANDLER_VERSION = 'sharing-seals-v1'
MAX_RETAINED_KEYS = 8


def _identifier(value: str) -> None:
    _require(type(value) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', value) is not None,
             'INVALID_SHARING_SIGNING_IDENTIFIER')


def execution_profile_fingerprint(settings: SharingQuerySettings) -> str:
    """Pin the installed v1 renderer, scope, schemas, settings and contracts.

    The per-attempt fingerprint additionally checks exact SDK config and parameters.
    This digest is compatibility metadata, not a native job or deployment attestation.
    """
    _require(isinstance(settings, SharingQuerySettings), 'INVALID_SHARING_QUERY_SETTINGS')
    return hashlib.sha256(canonical_json_bytes(dict(
        handler=HANDLER_VERSION, sql=_render_sql(), project=PROJECT, dataset=DATASET,
        location=LOCATION, settings=asdict(settings), schema_version=SCHEMA_VERSION,
        schemas={name: [field.to_api_repr() for field in TABLES[name].schema]
                 for name in ('workspaces', 'memberships', 'conversations', 'audit_events')},
        policy_version=POLICY_VERSION, identity_issuer=ISSUER,
        command_version=SHARING_CONTRACT_VERSION, retry_version=RETRY_VERSION,
        retry_window_seconds=int(RETRY_WINDOW.total_seconds()), max_retries=MAX_RETRIES,
        formats=['sharing-intent-v1', RETRY_VERSION]))).hexdigest()


@dataclass(frozen=True, slots=True)
class SharingSealProfile:
    profile_id: str
    settings: SharingQuerySettings
    fingerprint: str
    handler_version: str = HANDLER_VERSION

    def __post_init__(self):
        _identifier(self.profile_id)
        _require(isinstance(self.settings, SharingQuerySettings), 'INVALID_SHARING_QUERY_SETTINGS')
        _digest(self.fingerprint)
        _require(self.handler_version == HANDLER_VERSION, 'UNSUPPORTED_SHARING_SEAL_HANDLER')

    @classmethod
    def from_runtime(cls, profile_id: str, settings: SharingQuerySettings) -> SharingSealProfile:
        return cls(profile_id, settings, execution_profile_fingerprint(settings))

    def check_installed(self) -> None:
        _require(self.fingerprint == execution_profile_fingerprint(self.settings),
                 'SHARING_SEAL_PROFILE_UNAVAILABLE')


@dataclass(frozen=True, slots=True)
class SharingSigningKey:
    key_id: str
    profile_id: str
    issue_from: datetime
    issue_until: datetime
    retain_until: datetime
    revoked: bool = False

    def __post_init__(self):
        _identifier(self.key_id)
        _identifier(self.profile_id)
        for name in ('issue_from', 'issue_until', 'retain_until'):
            object.__setattr__(self, name, _utc(getattr(self, name)))
        _require(type(self.revoked) is bool, 'INVALID_SHARING_SIGNING_KEY_STATUS')
        # Equivalent to retain_until >= issue_until + seven days, without overflow.
        _require(self.issue_from < self.issue_until
                 and self.retain_until - self.issue_until >= RETRY_WINDOW,
                 'INVALID_SHARING_KEY_RETENTION')


@dataclass(frozen=True, slots=True)
class SharingSigningPolicy:
    active_key_id: str
    keys: tuple[SharingSigningKey, ...]
    profiles: tuple[SharingSealProfile, ...]
    version: str = SIGNING_POLICY_VERSION

    def __post_init__(self):
        _require(self.version == SIGNING_POLICY_VERSION, 'UNSUPPORTED_SHARING_SIGNING_POLICY')
        _identifier(self.active_key_id)
        _require(type(self.keys) is tuple and 1 <= len(self.keys) <= MAX_RETAINED_KEYS
                 and all(isinstance(key, SharingSigningKey) for key in self.keys)
                 and len({key.key_id for key in self.keys}) == len(self.keys),
                 'INVALID_SHARING_SIGNING_KEYS')
        _require(type(self.profiles) is tuple and 1 <= len(self.profiles) <= MAX_RETAINED_KEYS
                 and all(isinstance(profile, SharingSealProfile) for profile in self.profiles)
                 and len({profile.profile_id for profile in self.profiles}) == len(self.profiles),
                 'INVALID_SHARING_SEAL_PROFILES')
        _require(all(key.profile_id in {p.profile_id for p in self.profiles} for key in self.keys),
                 'SHARING_SEAL_PROFILE_UNAVAILABLE')
        active = self.key(self.active_key_id)
        _require(not active.revoked, 'SHARING_SIGNING_KEY_REVOKED')
        ordered = sorted(self.keys, key=lambda key: key.issue_from)
        _require(all(left.issue_until <= right.issue_from for left, right in zip(ordered, ordered[1:])),
                 'OVERLAPPING_SHARING_KEY_WINDOWS')

    def key(self, key_id: str) -> SharingSigningKey:
        for key in self.keys:
            if key.key_id == key_id:
                return key
        _require(False, 'SHARING_SIGNING_KEY_UNAVAILABLE')

    def profile(self, key: SharingSigningKey) -> SharingSealProfile:
        return next(profile for profile in self.profiles if profile.profile_id == key.profile_id)


def validate_signing_policy_transition(previous: SharingSigningPolicy, proposed: SharingSigningPolicy,
                                       *, changed_at: datetime) -> None:
    """Reject early removal/rebinding and reversal of recorded revocation.

    Call before trusted publication; this cannot synchronize replicas or prove
    that secret bytes behind an unchanged ID were not replaced.
    """
    _require(isinstance(previous, SharingSigningPolicy) and isinstance(proposed, SharingSigningPolicy),
             'INVALID_SHARING_SIGNING_POLICY')
    now = _utc(changed_at)
    active = proposed.key(proposed.active_key_id)
    _require(active.issue_from <= now < active.issue_until, 'SHARING_SIGNING_KEY_NOT_ISSUING')
    updated = {key.key_id: key for key in proposed.keys}
    known = {key.key_id for key in previous.keys}
    _require(all(key.issue_from >= now for key in proposed.keys if key.key_id not in known),
             'SHARING_SIGNING_KEY_BACKDATED')
    for old in previous.keys:
        new = updated.get(old.key_id)
        if new is None:
            _require(now >= old.retain_until, 'SHARING_SIGNING_KEY_REMOVED_EARLY')
            continue
        _require((old.key_id, old.profile_id, old.issue_from, old.issue_until, old.retain_until)
                 == (new.key_id, new.profile_id, new.issue_from, new.issue_until, new.retain_until)
                 and (not old.revoked or new.revoked), 'SHARING_SIGNING_KEY_REBOUND')
        _require(previous.profile(old) == proposed.profile(new), 'SHARING_SEAL_PROFILE_REBOUND')
