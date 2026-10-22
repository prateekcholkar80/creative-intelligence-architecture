"""Read-only native-correlated policy review receipts, fixed trusted configuration."""
from datetime import datetime, timezone
from dataclasses import replace
from itertools import islice
import json
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_policy_approval_receipt import (
    AUTHORITY, RECEIPTS, REVOCATIONS, PolicyApprovalPins, table_metadata, decode_current, decode_authority, _text,
)
from src.application.campaign_publication_reader import _identity, _native_row
from src.application.campaign_review_storage import row_json, table_metadata as control_table
from src.application.campaign_policy_approval_proof import GROUP_LIMITS, verify_publication
from src.application.control_io import query_config
from src.application.identity import IapIdentityAdapter
from src.application.operation_budget import require_owned_worker, operation_call, operation_iter
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import _require, _id, _digest
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION, validate_table

BASE_KEYS = {'observed_at','authorities','authority_count','receipts','receipt_count','revocation_count'}
PROOF_KEYS = set(GROUP_LIMITS) | {k+'_count' for k in GROUP_LIMITS}
PROOF_TABLES = ('campaign_permission_controls','campaign_permission_control_history','campaign_permission_publication_audits')


def publication_fields():
    groups = {
        'publication_controls': (PROOF_TABLES[0], "control_id='campaign-permissions'", 2),
        'publication_history': (PROOF_TABLES[1], "control_id='campaign-permissions'", 65),
        'authority_publications': (PROOF_TABLES[2], "permission_kind='POLICY_AUTHORITY' AND record_id=@authority_id AND action='POLICY_APPROVAL_CREATE_AUTHORITY'", 2),
        'receipt_publications': (PROOF_TABLES[2], "permission_kind='POLICY_RECEIPT' AND record_id=@receipt_id AND action='POLICY_APPROVAL_CREATE_RECEIPT'", 2),
    }
    fields=[]
    for key,(table,scope,limit) in groups.items():
        fields += [f'ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=65536,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_brand_memory.{table}` t WHERE {scope} LIMIT {limit}) {key}',
            f'(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{table}` WHERE {scope}) {key}_count']
    return ',\n'.join(fields)


def decode_proven_snapshot(row, **scope):
    _require(type(row) is dict and set(row)==BASE_KEYS|PROOF_KEYS,'POLICY_APPROVAL_PROVEN_CAPTURE')
    row=dict(row);size=0
    for key,limit in dict(authorities=2,receipts=2,**{k:v+1 for k,v in GROUP_LIMITS.items()}).items():
        _require(type(row[key]) is list and len(row[key])<=limit,'POLICY_APPROVAL_PROVEN_COVERAGE')
        normalized=[]
        for value in row[key]:
            _require(type(value) is str and len(value.encode())<=65536,'POLICY_APPROVAL_SIZE')
            size+=len(value.encode());_require(size<=262144,'POLICY_APPROVAL_PROVEN_SIZE')
            normalized.append(row_json(_native_row(value)) if key in GROUP_LIMITS else _text(json.loads(value)))
        row[key]=normalized
    current=decode_current({k:row[k] for k in BASE_KEYS},**scope)
    authority=decode_authority(row['authorities'][0])
    proof=verify_publication({k:row[k] for k in PROOF_KEYS},current=current,authority=authority)
    return replace(current,publication_sha256=proof.sha256)


SQL = f'''SELECT CURRENT_TIMESTAMP() observed_at,
 ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=32768,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_brand_memory.{AUTHORITY}` t WHERE authority_id=@authority_id LIMIT 2) authorities,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{AUTHORITY}` WHERE authority_id=@authority_id) authority_count,
 ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=32768,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_brand_memory.{RECEIPTS}` t WHERE receipt_id=@receipt_id LIMIT 2) receipts,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{RECEIPTS}` WHERE receipt_id=@receipt_id) receipt_count,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{REVOCATIONS}`
 WHERE (target_kind='AUTHORITY' AND target_id=@authority_id)
 OR (target_kind='RECEIPT' AND target_id=@receipt_id)) revocation_count,
 {publication_fields()}'''


class PolicyApprovalReader:
    def __init__(self, *, client, identity_adapter, pins, authority_id, authority_sha256,
                 receipt_id, reader_ids, clock=lambda: datetime.now(timezone.utc)):
        _require(client.project == PROJECT and client.location == LOCATION
            and client.default_query_job_config is None and type(identity_adapter) is IapIdentityAdapter
            and type(pins) is PolicyApprovalPins, 'POLICY_APPROVAL_READER_CONFIGURATION')
        _id(authority_id); _id(receipt_id); _digest(authority_sha256)
        from src.application.campaign_policy_approval_receipt import _principal
        _require(type(reader_ids) is tuple and 0 < len(reader_ids) <= 32
            and reader_ids == tuple(sorted(set(reader_ids))), 'POLICY_APPROVAL_READER_AUTHORITY')
        for value in reader_ids: _principal(value)
        self.client, self.verifier, self.pins = client, identity_adapter, pins
        self.authority_id, self.authority_sha256, self.receipt_id = authority_id, authority_sha256, receipt_id
        self.reader_ids, self.clock = reader_ids, clock

    def _actor(self, headers):
        require_owned_worker()
        actor = self.verifier.authenticate(headers)
        _identity(actor, self.verifier.settings, self.clock())
        _require(actor.principal.principal_id in self.reader_ids, 'POLICY_APPROVAL_READER_DENIED')
        return actor

    def read(self, headers):
        actor = self._actor(headers)
        for name in (AUTHORITY, RECEIPTS, REVOCATIONS)+PROOF_TABLES:
            expected = table_metadata(name) if name not in PROOF_TABLES else control_table(name,PROJECT)
            validate_table(operation_call(self.client.get_table, expected.reference, retry=None, timeout=10), expected)
        started = self.clock()
        config = query_config([bigquery.ScalarQueryParameter('authority_id', 'STRING', self.authority_id),
            bigquery.ScalarQueryParameter('receipt_id', 'STRING', self.receipt_id)])
        native_id = 'policy_approval_read_' + uuid4().hex
        job = operation_call(self.client.query, SQL, job_config=config, job_id=native_id,
            project=PROJECT, location=LOCATION, retry=None, job_retry=None, timeout=20)
        operation_call(job.result, max_results=0, retry=None, job_retry=None, timeout=45)
        native = operation_call(self.client.get_job, native_id, project=PROJECT, location=LOCATION, retry=None, timeout=10)
        _require(matches_sharing_job(native, job_id=native_id, sql=SQL, config=config)
            and native.state == 'DONE' and native.error_result is None, 'POLICY_APPROVAL_NATIVE_MISMATCH')
        rows = list(islice(operation_iter(operation_call(native.result, max_results=2, page_size=2,
            retry=None, job_retry=None, timeout=10)), 2))
        _require(len(rows) == 1, 'POLICY_APPROVAL_RESULT')
        row = dict(rows[0])
        current_actor = self._actor(headers)
        _require(current_actor.principal == actor.principal and current_actor.audience == actor.audience,
            'POLICY_APPROVAL_ACTOR_CHANGED')
        return decode_proven_snapshot(row, pins=self.pins, authority_id=self.authority_id,
            authority_sha256=self.authority_sha256, receipt_id=self.receipt_id,
            identity=actor, native_job_id=native_id, started=started, ended=self.clock())
