from copy import deepcopy
from datetime import datetime,timezone
import json
from unittest import TestCase
from src.application import campaign_quality_authority_reader as reader
from src.application.campaign_quality_authority import VERSION,table_metadata
from src.application.workflow_codec import encode_row
from src.application.workflow_contract import ContractError,request_fingerprint,canonical_json_bytes
from src.application.campaign_acceptance_contract import Stage,_hash
from src.validation.control_fixtures import workflow
from tests.test_campaign_quality_request_reader import client as native_client,WS,CONV,REQUEST
from tests.test_workflow_history_service import identity


def fixture():
    rows=workflow(WS,'INTERPRETER');r=rows['requests'][0]
    r.update(workflow(WS)['requests'][0]['submitted_preferences']['preferences'])
    r.update(canonical_sku='CI-SKU-0021',request_status='READY')
    r['request_sha256']=request_fingerprint(r,catalog=frozenset({'CI-SKU-0021'}))
    at=datetime.now(timezone.utc)
    p=dict(version=VERSION,workspace_id=WS,conversation_id=CONV,request_id=REQUEST,
        principal_id=r['principal_id'],request_sha256=r['request_sha256'],sku=r['canonical_sku'],
        country='US',channel='META',stage=Stage.INTERPRETATION.value,formula_version=None,audience=r['audience'],
        transcript=[dict(request_id=REQUEST,role='USER',content_sha256=r['message_sha256'],invocation_id=None)],claims=[])
    auth={k:p[k] for k in ('workspace_id','conversation_id','request_id','principal_id')}
    auth.update(payload_canonical_json=canonical_json_bytes(p).decode(),payload_sha256=_hash(p),
        recorded_at=r['created_at'].isoformat())
    row=dict(observed_at=at,request_count=1,
        **{name:[encode_row(v,name) for v in rows[name]] for name in ('workspaces','memberships','conversations','requests')},
        products=[json.dumps(dict(sku=r['canonical_sku'],source_system='NAKED_AND_THRIVING_OFFICIAL',product_status='ACTIVE'))],
        authorities=[json.dumps(auth)],calls=[],calls_count=0,jobs=[],jobs_count=0,attempts=[],attempts_count=0,
        bindings=[],bindings_count=0)
    return row,p


class AuthorityReaderTests(TestCase):
    def setUp(self):
        self.row,self.payload=fixture();self.identity=identity()

    def read(self,row=None,mutate=None):
        row=self.row if row is None else row
        c=native_client(row,mutate)
        table=table_metadata();table._properties.update(type='TABLE',location=reader.LOCATION)
        c.get_table.return_value=table
        return reader.read_execution_authority(c,workspace_id=WS,conversation_id=CONV,request_id=REQUEST,
            identity=self.identity,clock=lambda:self.row['observed_at'])

    def test_native_catalog_allows_nt_without_remapping_legacy_or_fabricating_transcript(self):
        read=self.read();pins=json.loads(read.content_canonical_json)
        self.assertEqual(pins['authority'],self.payload)
        self.assertEqual(pins['request_bindings']['canonical_sku'],'CI-SKU-0021')
        self.assertEqual(pins['content_sha256'] if 'content_sha256' in pins else _hash(pins),read.content_sha256)
        self.assertEqual(len(pins['transcript_sha256']),64)

    def test_missing_conflicting_provenance_inactive_or_duplicate_catalog_denies(self):
        for products in ([],self.row['products']*2,[json.dumps(dict(sku='CI-SKU-0021',source_system='BEAUTYFEEDS',product_status='ACTIVE'))],
            [json.dumps(dict(sku='CI-SKU-0021',source_system='NAKED_AND_THRIVING_OFFICIAL',product_status='INACTIVE'))]):
            with self.assertRaises(ContractError):self.read(self.row|dict(products=products))

    def test_missing_duplicate_tampered_or_wrong_market_authority_denies(self):
        for records in ([],self.row['authorities']*2):
            with self.assertRaises(ContractError):self.read(self.row|dict(authorities=records))
        for change in (dict(payload_sha256='c'*64),dict(principal_id='other')):
            r=json.loads(self.row['authorities'][0]);r.update(change)
            with self.assertRaises(ContractError):self.read(self.row|dict(authorities=[json.dumps(r)]))

    def test_missing_transcript_turn_or_changed_message_never_inherits_authority(self):
        r=json.loads(self.row['authorities'][0]);p=deepcopy(self.payload)
        p['transcript'][0]['content_sha256']='c'*64
        r.update(payload_canonical_json=canonical_json_bytes(p).decode(),payload_sha256=_hash(p))
        with self.assertRaisesRegex(ContractError,'TRANSCRIPT_AUTHORITY_MISMATCH'):
            self.read(self.row|dict(authorities=[json.dumps(r)]))

    def test_native_sql_or_config_mismatch_denies(self):
        def mutate(job,kw):job._properties['configuration']['query']['query']='SELECT 1'
        with self.assertRaisesRegex(ContractError,'QUALITY_AUTHORITY_NATIVE_MISMATCH'):self.read(mutate=mutate)

    def test_lineage_partial_count_oversized_and_unscoped_rows_deny(self):
        for change in (dict(calls_count=1),dict(jobs_count=257),dict(attempts=[None],attempts_count=1)):
            with self.assertRaises(ContractError):self.read(self.row|change)

    def test_real_legacy_request_reader_default_still_denies_nt(self):
        from src.application.campaign_quality_request_reader import _decode_capture
        base={k:v for k,v in self.row.items() if k in {'observed_at','request_count','workspaces','memberships','conversations','requests'}}
        with self.assertRaises(ContractError):_decode_capture(base,workspace_id=WS,conversation_id=CONV,
            request_id=REQUEST,identity=self.identity,started=self.row['observed_at'],ended=self.row['observed_at'])
