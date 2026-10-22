"""Pure replica refresh proposals; no authoritative reads, secret loads or swap.

The future runtime must serialize/CAS these states, persist authoritative floors,
and check readiness on every protected operation. Typed snapshots/load markers
alone never prove authority, successful key loading or cross-replica freshness.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum

from src.application.sharing_coordinator_contract import _utc
from src.application.sharing_publication_contract import (
    SigningPublication, SigningPublicationStatus, _counter, secret_config_digest,
    validate_publication_transition,
)
from src.application.sharing_secret_loader import SharingSecretConfig
from src.application.sharing_signing_contract import _identifier
from src.application.workflow_contract import _digest, _require

MAX_REFRESH_CHAIN = 16
MAX_FRESHNESS_SECONDS = 300


class SigningReplicaPhase(str, Enum):
    COLD = 'COLD'
    READING = 'READING'
    LOADING = 'LOADING'
    READY = 'READY'
    STOPPED = 'STOPPED'
    FAILED = 'FAILED'


@dataclass(frozen=True, slots=True)
class SigningRefreshSettings:
    max_age_seconds: int = 30

    def __post_init__(self):
        _require(type(self.max_age_seconds) is int and 1 <= self.max_age_seconds <= MAX_FRESHNESS_SECONDS,
                 'INVALID_SIGNING_REFRESH_SETTINGS')


@dataclass(frozen=True, slots=True)
class SigningReplicaState:
    instance_id: str
    head: SigningPublication | None = None
    generation: int = 0
    phase: SigningReplicaPhase = SigningReplicaPhase.COLD
    observed_at: datetime | None = None
    valid_until: datetime | None = None

    def __post_init__(self):
        _identifier(self.instance_id)
        _counter(self.generation, minimum=0)
        _require(self.head is None or isinstance(self.head, SigningPublication), 'INVALID_SIGNING_REPLICA_STATE')
        _require(isinstance(self.phase, SigningReplicaPhase), 'INVALID_SIGNING_REPLICA_STATE')
        if self.phase is SigningReplicaPhase.COLD:
            _require(self.generation == 0, 'INVALID_SIGNING_REPLICA_STATE')
        else:
            _require(self.generation > 0, 'INVALID_SIGNING_REPLICA_STATE')
        if self.phase in (SigningReplicaPhase.READY, SigningReplicaPhase.LOADING):
            _require(self.head is not None and self.head.status is SigningPublicationStatus.ACTIVE,
                     'INVALID_SIGNING_REPLICA_STATE')
            observed, expiry = _utc(self.observed_at), _utc(self.valid_until)
            active = self.head.config.policy.key(self.head.config.policy.active_key_id)
            _require(self.head.published_at <= observed < expiry <= active.issue_until
                     and expiry - observed <= timedelta(seconds=MAX_FRESHNESS_SECONDS),
                     'INVALID_SIGNING_REPLICA_LEASE')
            object.__setattr__(self, 'observed_at', observed)
            object.__setattr__(self, 'valid_until', expiry)
        else:
            _require(self.observed_at is None and self.valid_until is None, 'INVALID_SIGNING_REPLICA_LEASE')
        if self.phase is SigningReplicaPhase.STOPPED:
            _require(self.head is not None and self.head.status is SigningPublicationStatus.STOPPED,
                     'INVALID_SIGNING_REPLICA_STATE')


@dataclass(frozen=True, slots=True)
class SigningReadTicket:
    instance_id: str
    generation: int
    base_revision: int
    base_digest: str | None

    def __post_init__(self):
        _identifier(self.instance_id)
        _counter(self.generation)
        _counter(self.base_revision, minimum=0)
        if self.base_revision == 0:
            _require(self.base_digest is None, 'INVALID_SIGNING_REFRESH_TICKET')
        else:
            _digest(self.base_digest)


@dataclass(frozen=True, slots=True)
class SigningLoadTicket:
    instance_id: str
    generation: int
    revision: int
    digest: str
    observed_at: datetime
    valid_until: datetime

    def __post_init__(self):
        _identifier(self.instance_id)
        _counter(self.generation)
        _counter(self.revision)
        _digest(self.digest)
        object.__setattr__(self, 'observed_at', _utc(self.observed_at))
        object.__setattr__(self, 'valid_until', _utc(self.valid_until))
        _require(self.observed_at < self.valid_until
                 and self.valid_until - self.observed_at <= timedelta(seconds=MAX_FRESHNESS_SECONDS),
                 'INVALID_SIGNING_REFRESH_TICKET')


def begin_refresh(state: SigningReplicaState) -> tuple[SigningReplicaState, SigningReadTicket]:
    _require(isinstance(state, SigningReplicaState), 'INVALID_SIGNING_REPLICA_STATE')
    updated = replace(state, generation=state.generation + 1, phase=SigningReplicaPhase.READING,
                      observed_at=None, valid_until=None)
    return updated, SigningReadTicket(state.instance_id, updated.generation,
        0 if state.head is None else state.head.revision, None if state.head is None else state.head.digest)


def _check_read(state: SigningReplicaState, ticket: SigningReadTicket) -> None:
    _require(isinstance(state, SigningReplicaState) and isinstance(ticket, SigningReadTicket)
             and state.phase is SigningReplicaPhase.READING
             and (ticket.instance_id, ticket.generation) == (state.instance_id, state.generation)
             and ticket.base_revision == (0 if state.head is None else state.head.revision)
             and ticket.base_digest == (None if state.head is None else state.head.digest),
             'STALE_SIGNING_REFRESH_TICKET')


def _check_load(state: SigningReplicaState, ticket: SigningLoadTicket) -> None:
    _require(isinstance(state, SigningReplicaState) and isinstance(ticket, SigningLoadTicket)
             and state.phase is SigningReplicaPhase.LOADING
             and (ticket.instance_id, ticket.generation, ticket.revision, ticket.digest,
                  ticket.observed_at, ticket.valid_until)
                 == (state.instance_id, state.generation, state.head.revision, state.head.digest,
                     state.observed_at, state.valid_until), 'STALE_SIGNING_REFRESH_TICKET')


def observe_publications(state: SigningReplicaState, ticket: SigningReadTicket, *,
                         publications: tuple[SigningPublication, ...], observed_at: datetime,
                         now: datetime, settings: SigningRefreshSettings = SigningRefreshSettings()
                         ) -> tuple[SigningReplicaState, SigningLoadTicket | None]:
    """Validate a supplied bounded consecutive chain, pinning it before loading.

    The future authoritative reader must supply a unique current head and exact
    intervening revisions from one snapshot. These inputs cannot prove that read.
    """
    _check_read(state, ticket)
    _require(isinstance(settings, SigningRefreshSettings), 'INVALID_SIGNING_REFRESH_SETTINGS')
    observed, instant = _utc(observed_at), _utc(now)
    max_age = timedelta(seconds=settings.max_age_seconds)
    _require(observed <= instant and instant - observed < max_age,
             'STALE_SIGNING_PUBLICATION_OBSERVATION')
    _require(type(publications) is tuple and 1 <= len(publications) <= MAX_REFRESH_CHAIN
             and all(isinstance(p, SigningPublication) for p in publications)
             and len({p.revision for p in publications}) == len(publications),
             'INVALID_SIGNING_PUBLICATION_ROWS')
    head = state.head
    for publication in publications:
        _require(publication.published_at <= observed, 'INVALID_SIGNING_PUBLICATION_OBSERVATION')
        if head is None:
            _require(publication.revision == 1, 'SIGNING_PUBLICATION_HISTORY_REQUIRED')
        elif publication.revision <= head.revision:
            _require(publication.revision == head.revision and publication.digest == head.digest,
                     'SIGNING_PUBLICATION_ROLLBACK_OR_CONFLICT')
        else:
            _require(publication.revision == head.revision + 1, 'SIGNING_PUBLICATION_HISTORY_REQUIRED')
            validate_publication_transition(head, publication)
        head = publication
    if head.status is SigningPublicationStatus.STOPPED:
        return replace(state, head=head, phase=SigningReplicaPhase.STOPPED), None
    active = head.config.policy.key(head.config.policy.active_key_id)
    remaining = active.issue_until - observed
    if remaining <= timedelta(0):
        return replace(state, head=head, phase=SigningReplicaPhase.FAILED), None
    expiry = observed + min(max_age, remaining)
    if instant >= expiry:
        # Valid higher revision remains pinned even when it cannot become ready.
        return replace(state, head=head, phase=SigningReplicaPhase.FAILED), None
    updated = replace(state, head=head, phase=SigningReplicaPhase.LOADING, observed_at=observed, valid_until=expiry)
    return updated, SigningLoadTicket(state.instance_id, state.generation, head.revision, head.digest, observed, expiry)


def complete_refresh(state: SigningReplicaState, ticket: SigningLoadTicket, *,
                     loaded_config: SharingSecretConfig, now: datetime) -> SigningReplicaState:
    """Validate a trusted loader's supplied marker; this does not load/install keys."""
    _check_load(state, ticket)
    _require(secret_config_digest(loaded_config) == secret_config_digest(state.head.config),
             'SIGNING_LOADED_CONFIG_MISMATCH')
    instant = _utc(now)
    _require(ticket.observed_at <= instant < ticket.valid_until, 'SIGNING_REFRESH_EXPIRED')
    return replace(state, phase=SigningReplicaPhase.READY)


def fail_refresh(state: SigningReplicaState, ticket: SigningReadTicket | SigningLoadTicket) -> SigningReplicaState:
    if isinstance(ticket, SigningReadTicket):
        _check_read(state, ticket)
    else:
        _check_load(state, ticket)
    return replace(state, phase=SigningReplicaPhase.FAILED, observed_at=None, valid_until=None)


def require_signing_ready(state: SigningReplicaState, *, now: datetime) -> SigningPublication:
    _require(isinstance(state, SigningReplicaState) and state.phase is SigningReplicaPhase.READY,
             'SIGNING_REPLICA_NOT_READY')
    instant = _utc(now)
    _require(state.observed_at <= instant < state.valid_until, 'SIGNING_REFRESH_EXPIRED')
    return state.head
