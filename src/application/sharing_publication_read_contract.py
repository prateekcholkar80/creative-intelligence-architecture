"""4.1 High/Must have: pure validation of supplied snapshot projections.

No SDK reader, decoder, SQL, persistence or authority proof is implemented here.
4.2 must capture one fresh scoped snapshot and complete counts before truncation,
decode/recompute publication hashes, and verify resource/schema/job correlation.
Typed records, counts and timestamps are trusted inputs, not proof of that I/O.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re

from src.application.sharing_coordinator_contract import _utc
from src.application.sharing_publication_contract import (
    MAX_KEY_HISTORY, SigningKeyHistory, SigningPublication, _counter,
    validate_publication_transition,
)
from src.application.sharing_refresh_contract import MAX_REFRESH_CHAIN, SigningReadTicket
from src.application.sharing_signing_contract import _identifier
from src.application.workflow_contract import _digest, _require

MAX_READ_PUBLICATIONS = MAX_REFRESH_CHAIN + 1  # Older checkpoint anchor, if distinct.
MAX_READ_HISTORY = MAX_READ_PUBLICATIONS * MAX_KEY_HISTORY


@dataclass(frozen=True, slots=True)
class SigningReadLimits:
    maximum_bytes_billed: int = 64 * 1024**2
    job_timeout_ms: int = 20000
    result_timeout_seconds: int = 20
    max_age_seconds: int = 30

    def __post_init__(self):
        for value, upper in ((self.maximum_bytes_billed, 1024**3),
                             (self.job_timeout_ms, 60000),
                             (self.result_timeout_seconds, 60), (self.max_age_seconds, 300)):
            _require(type(value) is int and 1 <= value <= upper, 'INVALID_SIGNING_READ_LIMITS')


@dataclass(frozen=True, slots=True)
class SigningReadRequest:
    consumer_id: str
    project_number: str
    ticket: SigningReadTicket
    known_head: SigningPublication | None = None

    def __post_init__(self):
        _identifier(self.consumer_id)
        _require(type(self.project_number) is str
                 and re.fullmatch(r'[1-9][0-9]{0,19}', self.project_number) is not None,
                 'INVALID_SIGNING_READ_SCOPE')
        _require(isinstance(self.ticket, SigningReadTicket)
                 and (self.known_head is None or isinstance(self.known_head, SigningPublication)),
                 'INVALID_SIGNING_READ_REQUEST')
        _require((self.ticket.base_revision, self.ticket.base_digest)
                 == ((0, None) if self.known_head is None
                     else (self.known_head.revision, self.known_head.digest)),
                 'SIGNING_READ_BASE_MISMATCH')
        if self.known_head is not None:
            _require(self.known_head.config.project_number == self.project_number,
                     'INVALID_SIGNING_READ_SCOPE')


@dataclass(frozen=True, slots=True)
class SigningHeadRecord:
    revision: int
    digest: str
    published_at: datetime
    control_revision: int

    def __post_init__(self):
        _counter(self.revision)
        _counter(self.control_revision)
        _digest(self.digest)
        object.__setattr__(self, 'published_at', _utc(self.published_at))


@dataclass(frozen=True, slots=True)
class SigningCheckpointRecord:
    consumer_id: str
    checkpoint_revision: int
    publication_revision: int
    publication_digest: str
    updated_at: datetime

    def __post_init__(self):
        _identifier(self.consumer_id)
        _counter(self.checkpoint_revision)
        _counter(self.publication_revision)
        _digest(self.publication_digest)
        object.__setattr__(self, 'updated_at', _utc(self.updated_at))


@dataclass(frozen=True, slots=True)
class SigningHistoryRecord:
    publication_revision: int
    publication_digest: str
    record: SigningKeyHistory

    def __post_init__(self):
        _counter(self.publication_revision)
        _digest(self.publication_digest)
        _require(isinstance(self.record, SigningKeyHistory), 'INVALID_SIGNING_READ_HISTORY')


@dataclass(frozen=True, slots=True)
class SigningReadSnapshot:
    consumer_id: str
    ticket: SigningReadTicket
    observed_at: datetime
    heads: tuple[SigningHeadRecord, ...]
    checkpoints: tuple[SigningCheckpointRecord, ...]
    publications: tuple[SigningPublication, ...]
    history: tuple[SigningHistoryRecord, ...]
    head_count: int
    checkpoint_count: int
    publication_count: int
    history_count: int
    unheaded_publication_count: int


@dataclass(frozen=True, slots=True)
class SigningReadObservation:
    """Supplied validated projection; checkpoint_anchor never grants readiness."""
    checkpoint_anchor: SigningPublication | None
    publications: tuple[SigningPublication, ...]
    observed_at: datetime
    head_control_revision: int


def validate_publication_read(request: SigningReadRequest, snapshot: SigningReadSnapshot, *,
                              now: datetime, limits: SigningReadLimits = SigningReadLimits()
                              ) -> SigningReadObservation:
    """Reject ambiguous/incomplete supplied snapshots, not authenticate a read.

    Publications include the known/genesis/checkpoint anchor and all successors
    through the sole head, plus an older checkpoint body if outside that interval.
    No highest-row selection, missing-history skipping or readiness is inferred.
    """
    _require(isinstance(request, SigningReadRequest) and isinstance(snapshot, SigningReadSnapshot)
             and isinstance(limits, SigningReadLimits), 'INVALID_SIGNING_READ_SNAPSHOT')
    _require(snapshot.consumer_id == request.consumer_id and snapshot.ticket == request.ticket,
             'SIGNING_READ_SCOPE_MISMATCH')
    _require(type(snapshot.unheaded_publication_count) is int
             and snapshot.unheaded_publication_count == 0, 'SIGNING_READ_UNHEADED_PUBLICATION')
    observed, instant = _utc(snapshot.observed_at), _utc(now)
    _require(observed <= instant and instant - observed < timedelta(seconds=limits.max_age_seconds),
             'STALE_SIGNING_READ_SNAPSHOT')
    groups = ((snapshot.heads, snapshot.head_count, 1, 1, SigningHeadRecord),
              (snapshot.checkpoints, snapshot.checkpoint_count, 0, 1, SigningCheckpointRecord),
              (snapshot.publications, snapshot.publication_count, 1, MAX_READ_PUBLICATIONS, SigningPublication),
              (snapshot.history, snapshot.history_count, 1, MAX_READ_HISTORY, SigningHistoryRecord))
    for rows, count, lower, upper, kind in groups:
        _require(type(rows) is tuple and type(count) is int and lower <= count <= upper
                 and count == len(rows) and all(isinstance(row, kind) for row in rows),
                 'INVALID_SIGNING_READ_COUNTS')
    head = snapshot.heads[0]
    publications = {p.revision: p for p in snapshot.publications}
    _require(len(publications) == len(snapshot.publications), 'DUPLICATE_SIGNING_PUBLICATION')
    _require(all(p.config.project_number == request.project_number and p.published_at <= observed
                 for p in publications.values()), 'INVALID_SIGNING_READ_SCOPE')
    current = publications.get(head.revision)
    _require(current is not None and current.digest == head.digest
             and current.published_at == head.published_at, 'SIGNING_READ_HEAD_MISMATCH')
    checkpoint_anchor = None
    if snapshot.checkpoints:
        checkpoint = snapshot.checkpoints[0]
        checkpoint_anchor = publications.get(checkpoint.publication_revision)
        _require(checkpoint.consumer_id == request.consumer_id and checkpoint_anchor is not None
                 and checkpoint_anchor.digest == checkpoint.publication_digest
                 and checkpoint_anchor.published_at <= checkpoint.updated_at <= observed
                 and checkpoint.publication_revision <= head.revision,
                 'SIGNING_READ_CHECKPOINT_MISMATCH')
    anchor = request.known_head or checkpoint_anchor
    first_revision = 1 if anchor is None else anchor.revision
    _require(first_revision <= head.revision and head.revision - first_revision < MAX_REFRESH_CHAIN,
             'SIGNING_READ_HISTORY_REQUIRED')
    chain_revisions = set(range(first_revision, head.revision + 1))
    expected_revisions = chain_revisions | (set() if checkpoint_anchor is None else {checkpoint_anchor.revision})
    _require(set(publications) == expected_revisions, 'SIGNING_READ_HISTORY_REQUIRED')
    chain = tuple(publications[revision] for revision in sorted(chain_revisions))
    if anchor is not None:
        _require(chain[0].digest == anchor.digest, 'SIGNING_READ_ANCHOR_CONFLICT')
    for previous, proposed in zip(chain, chain[1:]):
        validate_publication_transition(previous, proposed)
    history = {(row.publication_revision, row.record.key.key_id): row for row in snapshot.history}
    _require(len(history) == len(snapshot.history), 'DUPLICATE_SIGNING_READ_HISTORY')
    expected_history = {(p.revision, record.key.key_id): SigningHistoryRecord(p.revision, p.digest, record)
                        for p in publications.values() for record in p.history}
    _require(history == expected_history, 'SIGNING_READ_HISTORY_MISMATCH')
    # A cold caller may seed COLD state from this anchor only after 4.2 proves the
    # authoritative checkpoint source. An already running replica cannot skip
    # intervening revisions just because another process advanced its checkpoint.
    return SigningReadObservation(checkpoint_anchor, chain, observed, head.control_revision)
