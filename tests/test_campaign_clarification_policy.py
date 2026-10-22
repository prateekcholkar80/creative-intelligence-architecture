"""Policy fixtures are not business approval or deployed IAP proof."""
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.application import campaign_clarification_policy as store
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_acceptance_contract import _hash
from src.ingestion import configure_clarification_policy as setup

class ClarificationPolicyTests(TestCase):
    def setUp(self):
        self.policy=json.loads(Path('config/clarification/evidence_requirements_v1.json').read_text())
        self.encoded=canonical(self.policy);self.now=datetime(2026,10,16,tzinfo=timezone.utc)
        self.request=store.ClarificationPolicyRequest(self.policy['policy_id'],self.policy['version'],_hash(self.policy))
        self.record=dict(schema_version=store.VERSION,policy_id=self.request.policy_id,version=self.request.version,
            policy_sha256=self.request.policy_sha256,policy_canonical_json=self.encoded,recorded_at=self.now.isoformat())
    def row(self):return dict(observed_at=self.now,policies=[json.dumps(self.record)],policy_count=1,revocation_count=0)
    def decode(self,row=None,ended=None):return store.decode_policy(row or self.row(),self.request,native_job_id='native',started=self.now,ended=ended or self.now)
    def test_exact_round_trip_preserves_draft_and_never_approves(self):
        result=self.decode();self.assertEqual(result.policy_canonical_json,self.encoded)
        self.assertFalse(result.approval_verified)
    def test_absent_duplicate_revoked_or_partial_rows_deny(self):
        for changes in (dict(policies=[],policy_count=0),dict(policy_count=2),dict(revocation_count=1),dict(policies=[None])):
            with self.assertRaises(ValueError):self.decode(self.row()|changes)
    def test_changed_pins_payload_future_or_expired_rows_deny(self):
        for changes in (dict(policy_sha256='b'*64),dict(policy_canonical_json=self.encoded+' '),
            dict(recorded_at=(self.now+timedelta(seconds=1)).isoformat())):
            with self.assertRaises(ValueError):self.decode(self.row()|dict(policies=[json.dumps(self.record|changes)]))
        with self.assertRaises(ValueError):self.decode(ended=self.now+timedelta(seconds=60))
    def test_draft_global_slot_ignores_content_and_denies_approved_ingestion(self):
        a=store.draft_plan(self.encoded);policy=self.policy|dict(max_questions=2)
        b=store.draft_plan(canonical(policy));self.assertEqual(a.job_id,b.job_id);self.assertNotEqual(a.config_json,b.config_json)
        self.assertIn('POLICY_VERSION_CONFLICT',a.sql);self.assertIn('ROLLBACK TRANSACTION',a.sql)
        with self.assertRaises(ValueError):store.draft_plan(canonical(self.policy|dict(status='APPROVED')))
        with self.assertRaises(ValueError):store.publish_draft(Mock(),self.encoded)
    def test_metadata_setup_is_idempotent_and_never_queries(self):
        client=Mock(project=store.PROJECT,location=store.LOCATION)
        client.get_dataset.return_value=Mock(location=store.LOCATION,default_table_expiration_ms=None,default_partition_expiration_ms=None)
        tables={}
        def get(ref,**kw):
            if ref.table_id not in tables:raise NotFound('missing')
            return tables[ref.table_id]
        def create(table,**kw):
            table._properties.update(type='TABLE',location=store.LOCATION);tables[table.table_id]=table
            return table
        client.get_table.side_effect=get;client.create_table.side_effect=create
        self.assertEqual(setup.setup(client)['status'],'MISSING_RESOURCES')
        self.assertEqual(len(setup.setup(client,apply=True)['created_tables']),2)
        self.assertEqual(setup.setup(client,apply=True)['created_tables'],[])
        client.query.assert_not_called()
    def test_native_correlation_failure_is_not_absence(self):
        client=Mock(project=store.PROJECT,location=store.LOCATION,default_query_job_config=None)
        native=Mock(state='DONE',error_result=None);native.result.return_value=[self.row()]
        client.query.return_value=native;client.get_job.return_value=native
        with patch.object(store,'validate_table'),patch.object(store,'matches_sharing_job',return_value=True):
            self.assertEqual(store.read_policy(client,self.request,clock=lambda:self.now).policy_canonical_json,self.encoded)
            client.get_job.assert_called_once()
            with patch.object(store,'matches_sharing_job',return_value=False):
                with self.assertRaises(ValueError):store.read_policy(client,self.request,clock=lambda:self.now)

    def test_approved_marker_alone_is_not_review_authority(self):
        self.policy['status']='APPROVED';self.encoded=canonical(self.policy)
        self.request=store.ClarificationPolicyRequest(self.policy['policy_id'],self.policy['version'],_hash(self.policy))
        self.record.update(policy_canonical_json=self.encoded,policy_sha256=self.request.policy_sha256)
        self.assertFalse(self.decode().approval_verified)
