"""Pure disabled policy-review publication plans; no appointments or approvals.

Sources and identities are supplied captures here. The trusted adapter must obtain
fresh authoritative sources and authenticate IAP. Publication records review;
qualifying evaluation and current response gates remain separate release conditions.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from google.cloud import bigquery
from src.application.campaign_policy_approval_receipt import (
    AUTHORITY, RECEIPTS, REVOCATIONS, SCHEMAS as APPROVAL_SCHEMAS,
    PolicyApprovalAuthority, PolicyApprovalReceipt, decode_authority, decode_receipt,
    _principal, _text, _parse,
)
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
from src.application.campaign_publication_reader import _identity
from src.application.identity import IapSettings
from src.application.campaign_acceptance_contract import _hash, _utc
from src.application.campaign_permission_publication import (
    Rows, PublicationPlan, _expr, MAX_CAPTURE_BYTES, PLAN_TTL, PUBLICATION_BYTES, PUBLICATION_TIMEOUT_MS,
)
from src.application.campaign_review_storage import SCHEMAS, STORAGE_VERSION, PublicationControl, decode_control_state, row_json
from src.application.workflow_contract import _require, _id, _digest
from src.ingestion.configure_campaign_acceptance import PROJECT, DATASET, settings

CONTROL = 'campaign_permission_controls'
HISTORY = 'campaign_permission_control_history'
AUDIT = 'campaign_permission_publication_audits'
ACTIONS = {'CREATE_AUTHORITY', 'CREATE_RECEIPT', 'REVOKE_AUTHORITY', 'REVOKE_RECEIPT'}


@dataclass(frozen=True, slots=True)
class PolicyAuthorityPublisherManifest:
    """Separate trusted capability, never inferred from operator-policy assignment."""
    manifest_id: str
    expected_audience: str
    principal_ids: tuple[str, ...]
    workspace_id: str
    effective_at: datetime
    valid_until: datetime

    def __post_init__(self):
        self._bounded(); _id(self.workspace_id)

    def _bounded(self):
        return OperatorPublisherManifest(self.manifest_id, self.expected_audience,
            self.principal_ids, self.effective_at, self.valid_until)

    @property
    def sha256(self):
        return _hash(dict(version='policy-authority-publisher-v1', bounded=self._bounded().sha256,
            workspace_id=self.workspace_id))

    def require_actor(self, identity, now): self._bounded().require_actor(identity, now)


@dataclass(frozen=True, slots=True)
class ApprovalPublicationSources:
    control: Rows
    authorities: tuple[str, ...]
    receipts: tuple[str, ...]
    revocations: tuple[str, ...]

    def __post_init__(self):
        _require(type(self.control) is Rows and self.control.kind == 'CONTROL'
            and self.control.record_id == 'campaign-permissions', 'POLICY_PUBLICATION_CONTROL')
        for values, limit in ((self.authorities, 1), (self.receipts, 1), (self.revocations, 64)):
            _require(type(values) is tuple and len(values) <= limit and all(type(v) is str for v in values),
                'POLICY_PUBLICATION_SOURCE_COVERAGE')
        _require(sum(len(v.encode()) for v in self.authorities + self.receipts + self.revocations)
            + len(self.control.head_json.encode()) + sum(len(v.encode()) for v in self.control.history_json)
            <= MAX_CAPTURE_BYTES, 'POLICY_PUBLICATION_SOURCE_SIZE')


def build_approval_publication(action, *, identity, expected_audience, sources, authority_id,
                               receipt_id=None, candidate=None, publisher_manifest=None,
                               confirmed_pins_sha256=None, checked_at, reason_code):
    now = _utc(checked_at); _id(authority_id); _id(reason_code)
    _require(action in ACTIONS and type(sources) is ApprovalPublicationSources
        and type(expected_audience) is IapSettings, 'POLICY_PUBLICATION_REQUEST')
    _identity(identity, expected_audience, now)
    actor = identity.principal.principal_id
    old_control, old_history = sources.control.decoded()
    control = decode_control_state(old_control, old_history, now)
    _require(control.revision < 64, 'POLICY_PUBLICATION_CONTROL_LIMIT')
    authority = decode_authority(sources.authorities[0]) if sources.authorities else None
    receipt = decode_receipt(sources.receipts[0]) if sources.receipts else None
    if authority:
        _require(authority.authority_id == authority_id and authority.audience == expected_audience.expected_audience,
            'POLICY_PUBLICATION_AUTHORITY_SCOPE')
    if receipt_id is not None: _id(receipt_id)
    _require((receipt_id is None) == action.endswith('AUTHORITY'), 'POLICY_PUBLICATION_RECEIPT_SCOPE')
    if receipt:
        _require(receipt.receipt_id == receipt_id and receipt.authority_id == authority_id
            and authority is not None and receipt.authority_sha256 == authority.sha256
            and receipt.pins.workspace_id == authority.workspace_id, 'POLICY_PUBLICATION_RECEIPT_SCOPE')
    revocations = tuple(_parse(value) for value in sources.revocations)
    for value in revocations:
        _require(set(value) == {name for name, _ in APPROVAL_SCHEMAS[REVOCATIONS]}
            and value['target_kind'] in {'AUTHORITY', 'RECEIPT'}
            and ((value['target_kind'] == 'AUTHORITY' and value['target_id'] == authority_id)
                or (value['target_kind'] == 'RECEIPT' and value['target_id'] == receipt_id)), 'POLICY_PUBLICATION_REVOCATION_SCOPE')
        _id(value['revocation_id'])
        _require(_utc(datetime.fromisoformat(value['revoked_at'])) <= now, 'POLICY_PUBLICATION_FUTURE_REVOCATION')
    _require(len({v['revocation_id'] for v in revocations}) == len(revocations), 'POLICY_PUBLICATION_DUPLICATE_REVOCATION')
    until = min(now + PLAN_TTL, datetime.fromtimestamp(identity.expires_at, now.tzinfo))
    if action != 'CREATE_RECEIPT':
        _require(type(publisher_manifest) is PolicyAuthorityPublisherManifest, 'POLICY_AUTHORITY_PUBLISHER_REQUIRED')
        publisher_manifest.require_actor(identity, now)
        until = min(until, publisher_manifest.valid_until)
    else:
        _require(publisher_manifest is None, 'POLICY_REVIEW_CANNOT_USE_PUBLISHER_ROLE')
    if action == 'CREATE_AUTHORITY':
        _require(type(candidate) is PolicyApprovalAuthority and candidate.authority_id == authority_id
            and not sources.authorities and not sources.receipts and not revocations
            and candidate.audience == expected_audience.expected_audience
            and candidate.workspace_id == publisher_manifest.workspace_id
            and candidate.effective_at <= now < candidate.valid_until, 'POLICY_AUTHORITY_CREATE_DENIED')
        destination = AUTHORITY
        new = dict(authority_id=authority_id, authority_sha256=candidate.sha256, canonical_json=_text(candidate.payload()))
        record_sha = candidate.sha256; until = min(until, candidate.valid_until)
    elif action == 'CREATE_RECEIPT':
        _require(type(candidate) is PolicyApprovalReceipt and authority is not None
            and not sources.receipts and not revocations and authority.effective_at <= now < authority.valid_until,
            'POLICY_RECEIPT_CREATE_DENIED')
        _require(candidate.receipt_id == receipt_id and candidate.authority_id == authority_id
            and candidate.authority_sha256 == authority.sha256 and candidate.reviewer_id == actor
            and actor in authority.reviewer_ids and candidate.pins.kind in authority.kinds
            and candidate.pins.workspace_id == authority.workspace_id and candidate.reviewed_at == now
            and now < candidate.valid_until <= authority.valid_until, 'POLICY_RECEIPT_REVIEWER_DENIED')
        _digest(confirmed_pins_sha256)
        _require(confirmed_pins_sha256 == candidate.pins.sha256, 'POLICY_RECEIPT_REVIEW_NOT_CONFIRMED')
        destination = RECEIPTS
        new = dict(receipt_id=receipt_id, authority_id=authority_id, receipt_sha256=candidate.sha256, canonical_json=_text(candidate.payload()))
        record_sha = candidate.sha256; until = min(until, candidate.valid_until)
    else:
        _require(candidate is None and authority is not None and authority.workspace_id == publisher_manifest.workspace_id,
            'POLICY_REVOCATION_AUTHORITY_DENIED')
        kind = 'AUTHORITY' if action == 'REVOKE_AUTHORITY' else 'RECEIPT'
        target = authority_id if kind == 'AUTHORITY' else receipt_id
        _require((kind == 'AUTHORITY' or receipt is not None)
            and not any(v['target_kind'] == kind and v['target_id'] == target for v in revocations),
            'POLICY_REVOCATION_TERMINAL_OR_MISSING')
        destination = REVOCATIONS
        new = dict(revocation_id='policy_revoke_' + _hash([kind, target]), target_kind=kind,
            target_id=target, revoked_at=now.isoformat())
        record_sha = _hash(new)
    _require(confirmed_pins_sha256 is None or action == 'CREATE_RECEIPT', 'POLICY_PUBLICATION_UNEXPECTED_CONFIRMATION')
    schema = {name: tuple((key, kind, 'REQUIRED') for key, kind in fields) for name, fields in APPROVAL_SCHEMAS.items()} | dict(SCHEMAS)
    command = _hash(dict(version='policy-approval-publication-v1', action=action, actor=actor,
        authority_id=authority_id, receipt_id=receipt_id, new=new,
        captured=dict(authorities=list(sources.authorities), receipts=list(sources.receipts), revocations=list(sources.revocations),
            control=sources.control.head_json, history=list(sources.control.history_json)),
        publisher=publisher_manifest.sha256 if publisher_manifest else None, confirmation=confirmed_pins_sha256,
        issued_at=identity.issued_at, expires_at=identity.expires_at,
        profile=dict(bytes=PUBLICATION_BYTES, timeout=PUBLICATION_TIMEOUT_MS)))
    next_control = PublicationControl(control.revision+1, control.sha256, command, actor, now)
    record_id = receipt_id or authority_id
    audit = dict(schema_version=STORAGE_VERSION, command_sha256=command, action='POLICY_APPROVAL_'+action,
        permission_kind='POLICY_'+('AUTHORITY' if receipt_id is None else 'RECEIPT'), record_id=record_id,
        previous_revision=0 if action.startswith('CREATE') else 1, revision=1 if action.startswith('CREATE') else 2,
        previous_record_sha256=None if action.startswith('CREATE') else (receipt.sha256 if receipt_id else authority.sha256),
        record_sha256=record_sha, control_revision=next_control.revision, control_sha256=next_control.sha256,
        grant_id=authority_id, grant_sha256=authority.sha256 if authority else candidate.sha256,
        review_id=receipt_id, review_sha256=record_sha if receipt_id else None,
        changed_by=actor, reason_code=reason_code, recorded_at=now, outcome='APPLIED')
    params = []; sql = []
    for key, value in dict(authority_id=authority_id, receipt_id=receipt_id or '', command_sha256=command,
                          record_sha256=record_sha, control_sha256=next_control.sha256).items():
        params.append(bigquery.ScalarQueryParameter(key, 'STRING', value))
    for key, value in dict(checked_at=now, expires_at=until).items(): params.append(bigquery.ScalarQueryParameter(key, 'TIMESTAMP', value))
    def temp(name, table, rows):
        if not rows:
            sql.append(f'CREATE TEMP TABLE {name} (' + ','.join(f'{n} {"INT64" if k=="INTEGER" else k}' for n,k,_ in schema[table]) + ');')
        else:
            params.append(bigquery.ArrayQueryParameter(name, 'STRING', [row_json(v) for v in rows]))
            sql.append(f"CREATE TEMP TABLE {name} AS SELECT {_expr(schema[table], 'item')} FROM UNNEST(@{name}) item;")
    for name, table, rows in (('expected_authority',AUTHORITY,tuple(_parse(v) for v in sources.authorities)),
        ('expected_receipt',RECEIPTS,tuple(_parse(v) for v in sources.receipts)), ('expected_revocations',REVOCATIONS,revocations),
        ('expected_control',CONTROL,(old_control,)),('expected_history',HISTORY,old_history),
        ('candidate_control',CONTROL,(next_control.row(),)),('candidate_record',destination,(new,)),('candidate_audit',AUDIT,(audit,))):
        temp(name,table,rows)
    def table(name): return f'`{PROJECT}.{DATASET}.{name}`'
    authority_scope = 'authority_id=@authority_id'; receipt_scope = 'receipt_id=@receipt_id'
    revocation_scope = "((target_kind='AUTHORITY' AND target_id=@authority_id) OR (target_kind='RECEIPT' AND target_id=@receipt_id))"
    def exact(name, expected, scope, label):
        return f"ASSERT (SELECT COUNT(*) FROM {table(name)} WHERE {scope})=(SELECT COUNT(*) FROM {expected}) AND NOT EXISTS(SELECT * FROM {table(name)} WHERE {scope} EXCEPT DISTINCT SELECT * FROM {expected}) AND NOT EXISTS(SELECT * FROM {expected} EXCEPT DISTINCT SELECT * FROM {table(name)} WHERE {scope}) AS 'POLICY_PUBLICATION_{label}';"
    def insert(name, source):
        cols=','.join(n for n,_,_ in schema[name]); return f'INSERT {table(name)} ({cols}) SELECT {cols} FROM {source};'
    sql += ['CREATE TEMP TABLE approval_result(state STRING,command_sha256 STRING,record_sha256 STRING,control_sha256 STRING);',
        'BEGIN\nBEGIN TRANSACTION;', "ASSERT @checked_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@expires_at AS 'POLICY_PUBLICATION_EXPIRED';",
        exact(AUTHORITY,'expected_authority',authority_scope,'AUTHORITY'),exact(RECEIPTS,'expected_receipt',receipt_scope,'RECEIPT'),
        exact(REVOCATIONS,'expected_revocations',revocation_scope,'REVOCATIONS'),
        exact(CONTROL,'expected_control',"control_id='campaign-permissions'",'CONTROL'),
        exact(HISTORY,'expected_history',"control_id='campaign-permissions'",'HISTORY'),
        f"ASSERT NOT EXISTS(SELECT 1 FROM {table(AUDIT)} WHERE command_sha256=@command_sha256) AS 'POLICY_PUBLICATION_AUDIT_CONFLICT';",
        f"UPDATE {table(CONTROL)} t SET " + ','.join(f'{n}=c.{n}' for n,_,_ in schema[CONTROL]) + " FROM candidate_control c WHERE t.control_id='campaign-permissions' AND t.control_sha256=(SELECT control_sha256 FROM expected_control);",
        "ASSERT @@row_count=1 AS 'POLICY_PUBLICATION_CAS';",insert(HISTORY,'candidate_control'), insert(destination,'candidate_record'), insert(AUDIT,'candidate_audit'),
        "INSERT approval_result VALUES('APPLIED',@command_sha256,@record_sha256,@control_sha256);",
        'COMMIT TRANSACTION;\nEXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE;\nEND;', 'SELECT * FROM approval_result;']
    script='\n'.join(sql)
    _require(script.count(';')<=64 and len(script.encode())<=128*1024,'POLICY_PUBLICATION_SQL_LIMIT')
    config=settings(params); config.maximum_bytes_billed=PUBLICATION_BYTES; config.job_timeout_ms=PUBLICATION_TIMEOUT_MS
    # Immutable target + action reserves one slot, regardless of actor/content/time.
    slot=_hash(['policy-approval-publication-slot-v1', action, record_id])
    config_json=json.dumps(config.to_api_repr(),sort_keys=True,separators=(',',':'))
    _require(len(config_json.encode())<=4*MAX_CAPTURE_BYTES,'POLICY_PUBLICATION_CONFIG_LIMIT')
    return PublicationPlan('campaign_policy_approval_'+slot, command, record_sha, next_control.sha256,
        row_json(audit), script, config_json)
