"""Pure revisioned signing publications; not a publisher, authority or lock."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from enum import Enum
import hashlib

from src.application.sharing_coordinator_contract import _utc
from src.application.sharing_secret_loader import (
    SharingSecretConfig, SharingSecretVersion, validate_secret_config_transition,
)
from src.application.sharing_signing_contract import SharingSealProfile, SharingSigningKey
from src.application.workflow_contract import _digest, _require, canonical_json_bytes
from src.application.workspace_contract import MAX_REVISION

PUBLICATION_VERSION = 'sharing-publication-v1'
POLICY_ID = 'sharing-signing'
MAX_KEY_HISTORY = 64  # The shared canonical JSON byte limit also applies.


class SigningPublicationStatus(str, Enum):
    ACTIVE = 'ACTIVE'
    STOPPED = 'STOPPED'


def _counter(value: int, *, minimum: int = 1) -> None:
    _require(type(value) is int and minimum <= value <= MAX_REVISION,
             'INVALID_SIGNING_PUBLICATION_COUNTER')


def _key_payload(key: SharingSigningKey) -> dict:
    return dict(key_id=key.key_id, profile_id=key.profile_id, revoked=key.revoked,
                issue_from=key.issue_from.isoformat(), issue_until=key.issue_until.isoformat(),
                retain_until=key.retain_until.isoformat())


def _profile_payload(profile: SharingSealProfile) -> dict:
    return dict(profile_id=profile.profile_id, settings=asdict(profile.settings),
                fingerprint=profile.fingerprint, handler_version=profile.handler_version)


def secret_config_payload(config: SharingSecretConfig) -> dict:
    """Canonical metadata projection shared by hashing and the storage codec."""
    _require(isinstance(config, SharingSecretConfig), 'INVALID_SHARING_SECRET_CONFIG')
    policy = config.policy
    return dict(project_id=config.project_id, project_number=config.project_number, location=config.location,
        policy_version=policy.version, active_key_id=policy.active_key_id,
        keys=[_key_payload(key) for key in sorted(policy.keys, key=lambda key: key.key_id)],
        profiles=[_profile_payload(p) for p in sorted(policy.profiles, key=lambda p: p.profile_id)],
        versions=[asdict(v) for v in sorted(config.versions, key=lambda v: v.key_id)])


def secret_config_digest(config: SharingSecretConfig) -> str:
    """Stable metadata digest, never secret bytes or proof of live access."""
    return hashlib.sha256(canonical_json_bytes(secret_config_payload(config))).hexdigest()


@dataclass(frozen=True, slots=True)
class SigningKeyHistory:
    key: SharingSigningKey
    secret: SharingSecretVersion
    profile: SharingSealProfile

    def __post_init__(self):
        _require(isinstance(self.key, SharingSigningKey) and isinstance(self.secret, SharingSecretVersion)
                 and isinstance(self.profile, SharingSealProfile)
                 and self.key.key_id == self.secret.key_id
                 and self.key.profile_id == self.profile.profile_id, 'INVALID_SIGNING_KEY_HISTORY')

    def payload(self) -> dict:
        return dict(key=_key_payload(self.key), secret=asdict(self.secret), profile=_profile_payload(self.profile))


def _records(config: SharingSecretConfig, status: SigningPublicationStatus) -> tuple[SigningKeyHistory, ...]:
    versions = {version.key_id: version for version in config.versions}
    return tuple(SigningKeyHistory(
        replace(key, revoked=True) if status is SigningPublicationStatus.STOPPED
            and key.key_id == config.policy.active_key_id else key,
        versions[key.key_id], config.policy.profile(key))
        for key in sorted(config.policy.keys, key=lambda key: key.key_id))


@dataclass(frozen=True, slots=True)
class SigningPublication:
    revision: int
    parent_digest: str | None
    config: SharingSecretConfig
    status: SigningPublicationStatus
    published_at: datetime
    history: tuple[SigningKeyHistory, ...]
    version: str = PUBLICATION_VERSION
    policy_id: str = POLICY_ID
    digest: str = field(init=False)

    def __post_init__(self):
        _counter(self.revision)
        _require(self.version == PUBLICATION_VERSION and self.policy_id == POLICY_ID,
                 'UNSUPPORTED_SIGNING_PUBLICATION')
        if self.revision == 1:
            _require(self.parent_digest is None, 'INVALID_SIGNING_PUBLICATION_PARENT')
        else:
            _digest(self.parent_digest)
        _require(isinstance(self.config, SharingSecretConfig) and isinstance(self.status, SigningPublicationStatus),
                 'INVALID_SIGNING_PUBLICATION')
        object.__setattr__(self, 'published_at', _utc(self.published_at))
        _require(type(self.history) is tuple and 1 <= len(self.history) <= MAX_KEY_HISTORY
                 and all(isinstance(record, SigningKeyHistory) for record in self.history),
                 'INVALID_SIGNING_KEY_HISTORY')
        ids = [record.key.key_id for record in self.history]
        _require(ids == sorted(set(ids))
                 and len({(record.secret.secret_id, record.secret.version) for record in self.history})
                     == len(self.history), 'INVALID_SIGNING_KEY_HISTORY')
        ordered = sorted(self.history, key=lambda record: record.key.issue_from)
        _require(all(left.key.issue_until <= right.key.issue_from for left, right in zip(ordered, ordered[1:])),
                 'OVERLAPPING_SIGNING_HISTORY_WINDOWS')
        profiles = {}
        for record in self.history:
            _require(profiles.get(record.profile.profile_id, record.profile) == record.profile,
                     'SIGNING_HISTORY_PROFILE_REBOUND')
            profiles[record.profile.profile_id] = record.profile
        current = {record.key.key_id: record for record in self.history}
        _require(all(current.get(record.key.key_id) == record for record in _records(self.config, self.status)),
                 'SIGNING_HISTORY_CONFIG_MISMATCH')
        if self.revision == 1:
            _require(self.history == _records(self.config, self.status), 'INVALID_SIGNING_INITIAL_HISTORY')
        if self.status is SigningPublicationStatus.ACTIVE:
            active = self.config.policy.key(self.config.policy.active_key_id)
            _require(active.issue_from <= self.published_at < active.issue_until,
                     'SHARING_SIGNING_KEY_NOT_ISSUING')
        payload = dict(version=self.version, policy_id=self.policy_id, revision=self.revision,
            parent_digest=self.parent_digest, config_digest=secret_config_digest(self.config),
            status=self.status.value, published_at=self.published_at.isoformat(),
            history=[record.payload() for record in self.history])
        object.__setattr__(self, 'digest', hashlib.sha256(canonical_json_bytes(payload)).hexdigest())


def initial_publication(config: SharingSecretConfig, *, published_at: datetime,
                        status: SigningPublicationStatus = SigningPublicationStatus.ACTIVE) -> SigningPublication:
    _require(isinstance(config, SharingSecretConfig) and isinstance(status, SigningPublicationStatus),
             'INVALID_SIGNING_PUBLICATION')
    return SigningPublication(1, None, config, status, published_at, _records(config, status))


def propose_publication(previous: SigningPublication, *, config: SharingSecretConfig,
                        status: SigningPublicationStatus, published_at: datetime) -> SigningPublication:
    """Propose against a supplied parent; the future BQ writer must fence it."""
    _require(isinstance(previous, SigningPublication) and isinstance(config, SharingSecretConfig)
             and isinstance(status, SigningPublicationStatus), 'INVALID_SIGNING_PUBLICATION')
    instant = _utc(published_at)
    _require(instant >= previous.published_at, 'SIGNING_PUBLICATION_TIME_REGRESSION')
    same_config = secret_config_digest(previous.config) == secret_config_digest(config)
    if status is SigningPublicationStatus.STOPPED:
        _require(same_config, 'SIGNING_STOP_CONFIG_CHANGED')
    elif not same_config:
        validate_secret_config_transition(previous.config, config, changed_at=instant)
    known = {record.key.key_id: record for record in previous.history}
    retained = {key.key_id for key in previous.config.policy.keys}
    known_profiles = {record.profile.profile_id: record.profile for record in previous.history}
    known_resources = {(record.secret.secret_id, record.secret.version): record.key.key_id
                       for record in previous.history}
    for record in _records(config, status):
        key_id = record.key.key_id
        _require(key_id not in known or key_id in retained, 'SIGNING_RETIRED_KEY_REINTRODUCED')
        _require(known_profiles.get(record.profile.profile_id, record.profile) == record.profile,
                 'SIGNING_HISTORY_PROFILE_REBOUND')
        resource = (record.secret.secret_id, record.secret.version)
        _require(known_resources.get(resource, key_id) == key_id, 'SIGNING_HISTORY_SECRET_REUSED')
        if key_id in known:
            old = known[key_id]
            _require(old.secret == record.secret and old.profile == record.profile
                     and replace(old.key, revoked=record.key.revoked) == record.key
                     and (not old.key.revoked or record.key.revoked), 'SIGNING_HISTORY_KEY_REBOUND')
        known[key_id] = record
    return SigningPublication(previous.revision + 1, previous.digest, config, status, instant,
                              tuple(known[key] for key in sorted(known)))


def validate_publication_transition(previous: SigningPublication, proposed: SigningPublication) -> None:
    _require(isinstance(proposed, SigningPublication), 'INVALID_SIGNING_PUBLICATION')
    expected = propose_publication(previous, config=proposed.config, status=proposed.status,
                                   published_at=proposed.published_at)
    _require(proposed.digest == expected.digest, 'SIGNING_PUBLICATION_CHAIN_MISMATCH')
