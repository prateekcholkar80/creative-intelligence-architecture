"""Exact policy-approval receipt storage and current-authority validation.

Separate authority namespace: CLAIM/OFFER/EVIDENCE operator grants never qualify.
These records prove a recorded review only. Qualifying evaluation verification and
per-response gates remain mandatory; a receipt alone cannot release an output.
"""
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
from google.cloud import bigquery
from src.application.campaign_acceptance_contract import _hash, _utc
from src.application.identity import IapSettings, VerifiedIdentity
from src.application.workflow_contract import _require, _id, _digest, canonical_json_bytes
from src.ingestion.setup_workflow_tables import PROJECT

VERSION = 'campaign-policy-approval-v1'
AUTHORITY = 'campaign_policy_approval_authorities'
RECEIPTS = 'campaign_policy_approval_receipts'
REVOCATIONS = 'campaign_policy_approval_revocations'
SCHEMAS = {
    AUTHORITY: (('authority_id', 'STRING'), ('authority_sha256', 'STRING'), ('canonical_json', 'STRING')),
    RECEIPTS: (('receipt_id', 'STRING'), ('authority_id', 'STRING'), ('receipt_sha256', 'STRING'), ('canonical_json', 'STRING')),
    REVOCATIONS: (('revocation_id', 'STRING'), ('target_kind', 'STRING'), ('target_id', 'STRING'), ('revoked_at', 'TIMESTAMP')),
}


def table_metadata(name):
    _require(name in SCHEMAS, 'POLICY_APPROVAL_TABLE')
    table = bigquery.Table(f'{PROJECT}.ci_brand_memory.{name}', schema=[
        bigquery.SchemaField(key, kind, mode='REQUIRED') for key, kind in SCHEMAS[name]])
    table.description = VERSION + ': immutable review evidence; no standalone release grant'
    return table


def _principal(value):
    _require(type(value) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}', value), 'POLICY_APPROVAL_PRINCIPAL')


def _text(value): return canonical_json_bytes(value).decode()


def _parse(text):
    _require(type(text) is str and len(text.encode()) <= 32768, 'POLICY_APPROVAL_SIZE')
    value = json.loads(text)
    _require(type(value) is dict and _text(value) == text, 'POLICY_APPROVAL_CANONICAL')
    return value


@dataclass(frozen=True, slots=True)
class PolicyApprovalPins:
    workspace_id: str
    kind: str
    stage: str
    policy_id: str
    policy_version: str
    policy_sha256: str
    prompt_snapshot_sha256: str
    model_profile_sha256: str
    benchmark_sha256: str
    evaluation_sha256: str

    def __post_init__(self):
        for key in ('workspace_id', 'policy_id', 'policy_version'): _id(getattr(self, key))
        _require(self.kind in {'ACCEPTANCE', 'CLARIFICATION'}
            and self.stage in {'INTERPRETATION', 'CLARIFICATION', 'GENERATION', 'REFINEMENT'}, 'POLICY_APPROVAL_SCOPE')
        for key in ('policy_sha256', 'prompt_snapshot_sha256', 'model_profile_sha256', 'benchmark_sha256', 'evaluation_sha256'):
            _digest(getattr(self, key))

    @property
    def sha256(self): return _hash(asdict(self))


@dataclass(frozen=True, slots=True)
class PolicyApprovalAuthority:
    authority_id: str
    audience: str
    workspace_id: str
    reviewer_ids: tuple[str, ...]
    kinds: tuple[str, ...]
    effective_at: datetime
    valid_until: datetime

    def __post_init__(self):
        _id(self.authority_id); IapSettings(self.audience); _id(self.workspace_id)
        _require(type(self.reviewer_ids) is tuple and 0 < len(self.reviewer_ids) <= 32
            and self.reviewer_ids == tuple(sorted(set(self.reviewer_ids))), 'POLICY_APPROVAL_REVIEWERS')
        for value in self.reviewer_ids: _principal(value)
        _require(type(self.kinds) is tuple and bool(self.kinds)
            and self.kinds == tuple(sorted(set(self.kinds)))
            and set(self.kinds) <= {'ACCEPTANCE', 'CLARIFICATION'}, 'POLICY_APPROVAL_AUTHORITY_KINDS')
        _require(_utc(self.effective_at) < _utc(self.valid_until), 'POLICY_APPROVAL_AUTHORITY_WINDOW')

    def payload(self):
        return dict(version=VERSION, authority_id=self.authority_id, audience=self.audience,
            workspace_id=self.workspace_id, reviewer_ids=list(self.reviewer_ids), kinds=list(self.kinds),
            effective_at=self.effective_at.isoformat(), valid_until=self.valid_until.isoformat())

    @property
    def sha256(self): return _hash(self.payload())


@dataclass(frozen=True, slots=True)
class PolicyApprovalReceipt:
    receipt_id: str
    pins: PolicyApprovalPins
    authority_id: str
    authority_sha256: str
    reviewer_id: str
    reviewed_at: datetime
    valid_until: datetime

    def __post_init__(self):
        _id(self.receipt_id); _id(self.authority_id); _digest(self.authority_sha256); _principal(self.reviewer_id)
        _require(type(self.pins) is PolicyApprovalPins
            and _utc(self.reviewed_at) < _utc(self.valid_until), 'POLICY_APPROVAL_RECEIPT_WINDOW')

    def payload(self):
        return dict(version=VERSION, receipt_id=self.receipt_id, pins=asdict(self.pins),
            authority_id=self.authority_id, authority_sha256=self.authority_sha256,
            reviewer_id=self.reviewer_id, reviewed_at=self.reviewed_at.isoformat(), valid_until=self.valid_until.isoformat())

    @property
    def sha256(self): return _hash(self.payload())


@dataclass(frozen=True, slots=True)
class CurrentPolicyApproval:
    receipt: PolicyApprovalReceipt
    native_job_id: str
    observed_at: datetime
    valid_until: datetime
    publication_sha256: str | None = None

    @property
    def release_allowed(self): return False  # Evaluation and response checks are separate.


def decode_authority(text):
    a = _parse(text)
    _require(set(a) == {'authority_id', 'authority_sha256', 'canonical_json'}, 'POLICY_APPROVAL_AUTHORITY_ROW')
    ap = _parse(a['canonical_json'])
    _require(set(ap) == {'version', 'authority_id', 'audience', 'workspace_id', 'reviewer_ids', 'kinds', 'effective_at', 'valid_until'}
        and ap['version'] == VERSION and type(ap['reviewer_ids']) is list and type(ap['kinds']) is list,
        'POLICY_APPROVAL_AUTHORITY_FORMAT')
    authority = PolicyApprovalAuthority(ap['authority_id'], ap['audience'], ap['workspace_id'], tuple(ap['reviewer_ids']),
        tuple(ap['kinds']), datetime.fromisoformat(ap['effective_at']), datetime.fromisoformat(ap['valid_until']))
    _require(_text(authority.payload()) == a['canonical_json'] and a['authority_id'] == authority.authority_id
        and a['authority_sha256'] == authority.sha256, 'POLICY_APPROVAL_CONTENT_CHANGED')
    return authority


def decode_receipt(text):
    r = _parse(text)
    _require(set(r) == {'receipt_id', 'authority_id', 'receipt_sha256', 'canonical_json'}, 'POLICY_APPROVAL_RECEIPT_ROW')
    rp = _parse(r['canonical_json'])
    _require(set(rp) == {'version', 'receipt_id', 'pins', 'authority_id', 'authority_sha256', 'reviewer_id', 'reviewed_at', 'valid_until'}
        and rp['version'] == VERSION and type(rp['pins']) is dict, 'POLICY_APPROVAL_RECEIPT_FORMAT')
    receipt = PolicyApprovalReceipt(rp['receipt_id'], PolicyApprovalPins(**rp['pins']), rp['authority_id'], rp['authority_sha256'],
        rp['reviewer_id'], datetime.fromisoformat(rp['reviewed_at']), datetime.fromisoformat(rp['valid_until']))
    _require(_text(receipt.payload()) == r['canonical_json'] and r['receipt_id'] == receipt.receipt_id
        and r['authority_id'] == receipt.authority_id and r['receipt_sha256'] == receipt.sha256,
        'POLICY_APPROVAL_CONTENT_CHANGED')
    return receipt


def decode_current(row, *, pins, authority_id, authority_sha256, receipt_id,
                   identity, native_job_id, started, ended):
    _require(type(pins) is PolicyApprovalPins and type(identity) is VerifiedIdentity, 'POLICY_APPROVAL_REQUEST')
    _id(authority_id); _id(receipt_id); _digest(authority_sha256); _id(native_job_id)
    _require(type(row) is dict and set(row) == {'observed_at', 'authorities', 'authority_count',
        'receipts', 'receipt_count', 'revocation_count'}, 'POLICY_APPROVAL_CAPTURE')
    observed = _utc(row['observed_at']); started = _utc(started); ended = _utc(ended)
    _require(started <= observed <= ended < started + timedelta(seconds=60)
        and identity.issued_at <= started.timestamp() and ended.timestamp() < identity.expires_at,
        'POLICY_APPROVAL_CAPTURE_EXPIRED')
    _require(all(type(row[key]) is int for key in ('authority_count', 'receipt_count', 'revocation_count'))
        and row['authority_count'] == row['receipt_count'] == 1 and row['revocation_count'] == 0
        and type(row['authorities']) is list and len(row['authorities']) == 1
        and type(row['receipts']) is list and len(row['receipts']) == 1, 'POLICY_APPROVAL_MISSING_DUPLICATE_OR_REVOKED')
    a = _parse(row['authorities'][0]); r = _parse(row['receipts'][0])
    authority = decode_authority(row['authorities'][0]); receipt = decode_receipt(row['receipts'][0])
    _require(a['authority_id'] == authority.authority_id == authority_id == r['authority_id'] == receipt.authority_id
        and a['authority_sha256'] == authority.sha256 == authority_sha256 == receipt.authority_sha256
        and receipt.receipt_id == receipt_id == r['receipt_id'] and r['receipt_sha256'] == receipt.sha256
        and authority.workspace_id == pins.workspace_id and authority.audience == identity.audience
        and receipt.pins == pins and pins.kind in authority.kinds and receipt.reviewer_id in authority.reviewer_ids,
        'POLICY_APPROVAL_BINDING_CHANGED')
    _require(authority.effective_at <= receipt.reviewed_at <= observed
        and ended < receipt.valid_until <= authority.valid_until, 'POLICY_APPROVAL_NOT_CURRENT')
    return CurrentPolicyApproval(receipt, native_job_id, observed, min(receipt.valid_until, started + timedelta(seconds=60), datetime.fromtimestamp(identity.expires_at, timezone.utc)))
