"""Immutable publication-audit/control proof for current policy-review receipts.

Supplied snapshots are not authentication. The installed reader must capture these
sources atomically and correlate its native job. This proves stored publication,
not qualifying measurements, a reviewer appointment or response release.
"""
from dataclasses import dataclass
from src.application.campaign_acceptance_contract import _hash, _utc
from src.application.campaign_policy_approval_receipt import (
    CurrentPolicyApproval, PolicyApprovalAuthority, _principal,
)
from src.application.campaign_review_storage import SCHEMAS, STORAGE_VERSION, decode_control_state, parse_row, row_json
from src.application.workflow_contract import _require, _digest

AUDIT = 'campaign_permission_publication_audits'
GROUP_LIMITS = {'publication_controls': 1, 'publication_history': 64,
                'authority_publications': 1, 'receipt_publications': 1}


@dataclass(frozen=True, slots=True)
class ApprovalPublicationProof:
    sha256: str
    authority_control_revision: int
    receipt_control_revision: int


def _audit(text):
    row = parse_row(text)
    _require(set(row) == {name for name, _, _ in SCHEMAS[AUDIT]}, 'POLICY_APPROVAL_AUDIT_SHAPE')
    for name, kind, mode in SCHEMAS[AUDIT]:
        value = row[name]
        if value is None and mode == 'NULLABLE': continue
        if kind == 'STRING': _require(type(value) is str, 'POLICY_APPROVAL_AUDIT_TYPE')
        elif kind == 'INTEGER': _require(type(value) is int, 'POLICY_APPROVAL_AUDIT_TYPE')
        else: _utc(value)
    return row


def verify_publication(row, *, current, authority):
    _require(type(current) is CurrentPolicyApproval and type(authority) is PolicyApprovalAuthority
        and type(row) is dict and set(row) == set(GROUP_LIMITS) | {k+'_count' for k in GROUP_LIMITS},
        'POLICY_APPROVAL_PUBLICATION_CAPTURE')
    total = 0
    for key, limit in GROUP_LIMITS.items():
        values = row[key]; count = row[key+'_count']
        _require(type(values) is list and type(count) is int and 1 <= count <= limit
            and len(values) == count and (key == 'publication_history' or count == 1),
            'POLICY_APPROVAL_PUBLICATION_MISSING_DUPLICATE_OR_OVERFLOW')
        for value in values:
            _require(type(value) is str and len(value.encode()) <= 65536, 'POLICY_APPROVAL_PUBLICATION_SIZE')
            total += len(value.encode())
    _require(total <= 131072, 'POLICY_APPROVAL_PUBLICATION_SIZE')
    head = parse_row(row['publication_controls'][0])
    history = tuple(sorted((parse_row(value) for value in row['publication_history']), key=lambda v: v['revision']))
    decode_control_state(head, history, current.observed_at)
    audits = [_audit(row[key][0]) for key in ('authority_publications', 'receipt_publications')]
    receipt = current.receipt
    _require(receipt.authority_id == authority.authority_id and receipt.authority_sha256 == authority.sha256,
        'POLICY_APPROVAL_PUBLICATION_AUTHORITY_CHANGED')
    for audit, kind, record_id, digest in zip(audits,
            ('AUTHORITY', 'RECEIPT'), (authority.authority_id, receipt.receipt_id), (authority.sha256, receipt.sha256)):
        _principal(audit['changed_by'])
        for name in ('command_sha256', 'record_sha256', 'control_sha256', 'grant_sha256'): _digest(audit[name])
        _require(audit['schema_version'] == STORAGE_VERSION
            and audit['action'] == 'POLICY_APPROVAL_CREATE_'+kind
            and audit['permission_kind'] == 'POLICY_'+kind and audit['record_id'] == record_id
            and audit['record_sha256'] == digest and audit['previous_revision'] == 0
            and audit['revision'] == 1 and audit['previous_record_sha256'] is None
            and audit['grant_id'] == authority.authority_id and audit['grant_sha256'] == authority.sha256
            and audit['outcome'] == 'APPLIED', 'POLICY_APPROVAL_PUBLICATION_AUDIT_CONFLICT')
        if kind == 'AUTHORITY':
            _require(audit['review_id'] is None and audit['review_sha256'] is None
                and authority.effective_at <= audit['recorded_at'] < authority.valid_until,
                'POLICY_APPROVAL_AUTHORITY_PUBLICATION_CONFLICT')
        else:
            _require(audit['review_id'] == receipt.receipt_id and audit['review_sha256'] == receipt.sha256
                and audit['changed_by'] == receipt.reviewer_id and audit['recorded_at'] == receipt.reviewed_at,
                'POLICY_APPROVAL_REVIEW_PUBLICATION_CONFLICT')
        revision = audit['control_revision']
        _require(2 <= revision <= len(history), 'POLICY_APPROVAL_PUBLICATION_CONTROL_MISSING')
        control = history[revision-1]
        _require(control['control_sha256'] == audit['control_sha256']
            and control['command_sha256'] == audit['command_sha256']
            and control['changed_by'] == audit['changed_by'] and control['recorded_at'] == audit['recorded_at'],
            'POLICY_APPROVAL_PUBLICATION_CONTROL_CONFLICT')
    first, last = audits
    _require(first['control_revision'] < last['control_revision']
        and first['recorded_at'] <= last['recorded_at'] <= current.observed_at,
        'POLICY_APPROVAL_PUBLICATION_ORDER')
    # Bind only the immutable relevant prefix. Later unrelated control revisions
    # do not invalidate a still-current approval; the full current chain was checked.
    digest = _hash(dict(version='policy-approval-publication-proof-v1',
        receipt_sha256=receipt.sha256, authority_sha256=authority.sha256,
        audits=[row_json(value) for value in audits],
        control_prefix=[row_json(value) for value in history[:last['control_revision']]]))
    return ApprovalPublicationProof(digest, first['control_revision'], last['control_revision'])
