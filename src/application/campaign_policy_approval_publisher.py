"""Owned IAP policy-review writer, disabled by default; original-job recovery only.

Does not install reviewer authority, qualify an evaluation, promote prompts or
release text. Publication receipts are commit evidence, never serving tokens.
"""
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
from threading import RLock
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_policy_approval_publication import (
    AUTHORITY, RECEIPTS, REVOCATIONS, CONTROL, HISTORY, AUDIT, ACTIONS,
    ApprovalPublicationSources, PolicyAuthorityPublisherManifest, build_approval_publication,
)
from src.application.campaign_policy_approval_receipt import (
    PolicyApprovalAuthority, PolicyApprovalReceipt, PolicyApprovalPins, decode_authority, decode_receipt, _text,
    table_metadata as approval_table,
)
from src.application.campaign_publication_reader import _identity, _native_row
from src.application.campaign_review_storage import table_metadata as control_table, row_json, parse_row
from src.application.campaign_permission_publication import Rows, validate_result
from src.application.control_io import ControlIO, ControlPlan, ControlOutcome
from src.application.identity import IapIdentityAdapter
from src.application.operation_budget import require_owned_worker, operation_call
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_contract import _require, _id
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, validate_table

NAMES = (AUTHORITY, RECEIPTS, REVOCATIONS, CONTROL, HISTORY, AUDIT)


@dataclass(frozen=True, slots=True)
class PolicyEvaluationQualification:
    """Server-reader result, not browser input or a supplied pass boolean.

    The installed reader must verify the measured report and current controls.
    This contract does not implement that reader or qualify measurements itself.
    """
    pins: PolicyApprovalPins
    native_job_id: str
    observed_at: datetime
    valid_until: datetime


@dataclass(frozen=True, slots=True)
class ApprovalCapture:
    sources: ApprovalPublicationSources
    checked_at: datetime


@dataclass(frozen=True, slots=True)
class ApprovalIntent:
    action: str
    actor_id: str
    authority_id: str
    receipt_id: str | None
    candidate: object
    publication: object
    io_plan: ControlPlan
    qualification_native_job_id: str | None = None


def capture_sql():
    scopes = {
        'authorities': (AUTHORITY, 'authority_id=@authority_id'),
        'receipts': (RECEIPTS, 'receipt_id=@receipt_id'),
        'revocations': (REVOCATIONS, "((target_kind='AUTHORITY' AND target_id=@authority_id) OR (target_kind='RECEIPT' AND target_id=@receipt_id))"),
        'control': (CONTROL, "control_id='campaign-permissions'"),
        'history': (HISTORY, "control_id='campaign-permissions'"),
    }
    fields = ['CURRENT_TIMESTAMP() observed_at', '@read_id read_id']
    for key, (table, scope) in scopes.items():
        fields += [f'ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=65536,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_brand_memory.{table}` t WHERE {scope} LIMIT 65) {key}',
            f'(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{table}` WHERE {scope}) {key}_count']
    return 'SELECT '+',\n'.join(fields)


class PolicyApprovalPublisher:
    def __init__(self, *, client, identity_adapter, publisher_manifest=None, enabled=False, qualification_reader=None,
                 clock=lambda: datetime.now(timezone.utc)):
        _require(client.project == PROJECT and client.location == LOCATION and client.default_query_job_config is None
            and type(identity_adapter) is IapIdentityAdapter and type(enabled) is bool
            and (publisher_manifest is None or type(publisher_manifest) is PolicyAuthorityPublisherManifest),
            'POLICY_PUBLISHER_CONFIGURATION')
        if publisher_manifest is not None:
            _require(publisher_manifest.expected_audience == identity_adapter.settings.expected_audience,
                'POLICY_PUBLISHER_AUDIENCE')
        self.client, self.verifier, self.manifest = client, identity_adapter, publisher_manifest
        _require(qualification_reader is None or callable(qualification_reader),'POLICY_QUALIFICATION_READER_CONFIGURATION')
        self.qualification_reader=qualification_reader
        self.enabled, self.clock = enabled, clock
        self.io = ControlIO(client, result_timeout_seconds=90)
        self._lock, self._issued, self._slots, self._used = RLock(), {}, {}, set()

    def _actor(self, headers, action):
        require_owned_worker(); _require(self.enabled, 'POLICY_PUBLICATION_DISABLED')
        actor = self.verifier.authenticate(headers)
        _identity(actor, self.verifier.settings, self.clock())
        if action != 'CREATE_RECEIPT':
            _require(self.manifest is not None, 'POLICY_AUTHORITY_PUBLISHER_REQUIRED')
            self.manifest.require_actor(actor, self.clock())
        return actor

    def _capture(self, actor, authority_id, receipt_id):
        _identity(actor, self.verifier.settings, self.clock())
        for name in NAMES:
            expected = approval_table(name) if name in {AUTHORITY, RECEIPTS, REVOCATIONS} else control_table(name, PROJECT)
            validate_table(operation_call(self.client.get_table, expected.reference, retry=None, timeout=10), expected)
        started = self.clock(); read_id = 'policy-publication-read-'+uuid4().hex
        row = self.io.read(capture_sql(), [bigquery.ScalarQueryParameter('authority_id','STRING',authority_id),
            bigquery.ScalarQueryParameter('receipt_id','STRING',receipt_id or ''), bigquery.ScalarQueryParameter('read_id','STRING',read_id)])
        ended = self.clock(); _identity(actor, self.verifier.settings, ended)
        keys = ('authorities','receipts','revocations','control','history')
        _require(set(row) == {'observed_at','read_id'} | set(keys) | {k+'_count' for k in keys}
            and row['read_id'] == read_id and started <= row['observed_at'] <= ended < started+timedelta(seconds=60),
            'POLICY_PUBLICATION_CAPTURE_WINDOW')
        for key in keys:
            _require(type(row[key]) is list and type(row[key+'_count']) is int
                and len(row[key]) == row[key+'_count'] and len(row[key]) < 65,
                'POLICY_PUBLICATION_CAPTURE_COVERAGE')
            normalized = []
            for value in row[key]:
                _require(type(value) is str and len(value.encode()) <= 65536, 'POLICY_PUBLICATION_CAPTURE_SIZE')
                if key in {'control','history'}: normalized.append(row_json(_native_row(value)))
                else:
                    data = json.loads(value)
                    if key == 'revocations': data['revoked_at'] = datetime.fromisoformat(data['revoked_at'].replace('Z','+00:00')).isoformat()
                    normalized.append(_text(data))
            row[key] = tuple(normalized)
        _require(len(row['control']) == 1, 'POLICY_PUBLICATION_CONTROL_REQUIRED')
        controls = Rows('CONTROL','campaign-permissions',row['control'][0],
            tuple(sorted(row['history'], key=lambda v: parse_row(v)['revision'])))
        sources = ApprovalPublicationSources(controls,row['authorities'],row['receipts'],row['revocations'])
        # Full control-chain validation is also performed by the plan builder.
        from src.application.campaign_review_storage import decode_control_state
        decode_control_state(*controls.decoded(), ended)
        return ApprovalCapture(sources, ended)

    def _qualification(self, headers, pins, previous_job_id=None):
        _require(self.qualification_reader is not None,'POLICY_QUALIFYING_EVALUATION_UNAVAILABLE')
        proof=self.qualification_reader(headers,pins)
        _require(type(proof) is PolicyEvaluationQualification and type(proof.pins) is PolicyApprovalPins
            and proof.pins==pins and proof.observed_at<=self.clock()<proof.valid_until
            <=proof.observed_at+timedelta(seconds=60),'POLICY_QUALIFYING_EVALUATION_CHANGED')
        _id(proof.native_job_id)
        _require(proof.native_job_id!=previous_job_id,'POLICY_QUALIFYING_EVALUATION_NOT_REREAD')
        return proof.native_job_id

    def prepare(self, headers, *, action, authority_id, receipt_id=None, candidate=None,
                confirmed_pins_sha256=None, reason_code):
        with self._lock:
            _require(action in ACTIONS, 'POLICY_PUBLICATION_ACTION'); _id(authority_id)
            if receipt_id is not None: _id(receipt_id)
            actor = self._actor(headers, action)
            _require(candidate is None or type(candidate) in {PolicyApprovalAuthority,PolicyApprovalReceipt}, 'POLICY_PUBLICATION_CANDIDATE')
            # Review time is server-owned; exact reviewed pins and expiry stay bound.
            payload = candidate.payload() if candidate is not None else None
            if type(candidate) is PolicyApprovalReceipt: payload = {k:v for k,v in payload.items() if k != 'reviewed_at'}
            from src.application.campaign_acceptance_contract import _hash
            signature = _hash([action,authority_id,receipt_id,payload,confirmed_pins_sha256,reason_code])
            key = (action, receipt_id or authority_id)
            if key in self._slots:
                previous, old = self._slots[key]
                _require(previous == signature and old.actor_id == actor.principal.principal_id, 'POLICY_PUBLICATION_SLOT_CONFLICT')
                self._authorize(headers, old)
                return old
            _require(len(self._issued) < 32, 'POLICY_PUBLICATION_CAPACITY')
            captured = self._capture(actor, authority_id, receipt_id)
            qualification=None
            if type(candidate) is PolicyApprovalReceipt:
                candidate = replace(candidate, reviewed_at=captured.checked_at)
            plan = build_approval_publication(action, identity=actor, expected_audience=self.verifier.settings,
                sources=captured.sources, authority_id=authority_id, receipt_id=receipt_id, candidate=candidate,
                publisher_manifest=self.manifest if action != 'CREATE_RECEIPT' else None,
                confirmed_pins_sha256=confirmed_pins_sha256, checked_at=captured.checked_at, reason_code=reason_code)
            if action=='CREATE_RECEIPT':qualification=self._qualification(headers,candidate.pins)
            io_plan = ControlPlan(actor.principal.principal_id, authority_id, plan.command_sha256,
                plan.native_job_id, plan.sql, plan.config_json, 'POLICY_APPROVAL', '{}', plan.audit_json, RECEIPTS)
            intent = ApprovalIntent(action,actor.principal.principal_id,authority_id,receipt_id,candidate,plan,io_plan,qualification)
            _require(plan.native_job_id not in self._issued, 'POLICY_PUBLICATION_ORIGINAL_INTENT')
            self._issued[plan.native_job_id] = intent; self._slots[key] = (signature,intent)
            return intent

    def _authorize(self, headers, intent):
        _require(type(intent) is ApprovalIntent and self._issued.get(intent.io_plan.job_id) is intent,
            'POLICY_PUBLICATION_ORIGINAL_INTENT')
        actor = self._actor(headers, intent.action)
        _require(actor.principal.principal_id == intent.actor_id, 'POLICY_PUBLICATION_ACTOR_CHANGED')
        captured = self._capture(actor, intent.authority_id, intent.receipt_id)
        if intent.action == 'CREATE_RECEIPT':
            source = captured.sources
            _require(len(source.authorities) == 1 and not source.revocations, 'POLICY_PUBLICATION_AUTHORITY_REVOKED_OR_MISSING')
            authority = decode_authority(source.authorities[0]); receipt = intent.candidate
            now = self.clock()
            _require(authority.sha256 == receipt.authority_sha256 and authority.effective_at <= now < authority.valid_until
                and receipt.reviewed_at <= now < receipt.valid_until and receipt.reviewer_id == actor.principal.principal_id
                and actor.principal.principal_id in authority.reviewer_ids and receipt.pins.kind in authority.kinds
                and receipt.pins.workspace_id == authority.workspace_id
                and authority.audience == actor.audience, 'POLICY_PUBLICATION_REVIEW_AUTHORITY_CHANGED')
            self._qualification(headers,receipt.pins,intent.qualification_native_job_id)
        return captured

    def _result(self, headers, intent, result):
        captured = self._authorize(headers, intent)
        if result.job_id != intent.io_plan.job_id: return ControlOutcome(intent.io_plan.job_id,Status.UNKNOWN,submitted=result.submitted)
        if result.native_status is Status.COMMITTED:
            try:
                validate_result(intent.publication,result.rows)
                if intent.action == 'CREATE_AUTHORITY':
                    _require(len(captured.sources.authorities)==1 and not captured.sources.revocations
                        and decode_authority(captured.sources.authorities[0]) == intent.candidate, 'POLICY_PUBLICATION_POSTREAD')
                elif intent.action == 'CREATE_RECEIPT':
                    _require(len(captured.sources.receipts)==1
                        and decode_receipt(captured.sources.receipts[0]) == intent.candidate, 'POLICY_PUBLICATION_POSTREAD')
                else:
                    target_kind='AUTHORITY' if intent.action=='REVOKE_AUTHORITY' else 'RECEIPT'
                    target_id=intent.authority_id if target_kind=='AUTHORITY' else intent.receipt_id
                    from src.application.campaign_acceptance_contract import _hash
                    _require(sum(v.get('target_kind')==target_kind and v.get('target_id')==target_id
                        and _hash(v)==intent.publication.proposed_record_sha256
                        for v in (json.loads(value) for value in captured.sources.revocations))==1, 'POLICY_PUBLICATION_POSTREAD')
            except Exception: return ControlOutcome(result.job_id,Status.UNKNOWN,submitted=result.submitted)
        return result

    def publish(self, headers, intent):
        with self._lock:
            self._authorize(headers,intent)
            if intent.io_plan.job_id in self._used: return self.recover(headers,intent)
            self._used.add(intent.io_plan.job_id)
            result=self.io.submit_once(intent.io_plan)
            return self._result(headers,intent,result)

    def recover(self, headers, intent):
        with self._lock:
            self._authorize(headers,intent)
            return self._result(headers,intent,self.io.recover(intent.io_plan))
