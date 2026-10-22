import json
from unittest import TestCase
from tests import test_interpreter_submission as fixtures
from src.application.grounded_interpreter_submission import prepare_grounded_submission,validate_selection
from src.application.workflow_codec import decode_row
from src.application.campaign_quality_authority import decode_authority
from types import SimpleNamespace

SELECT=dict(sku='CI-SKU-0021',country='US',channel='META',audience='Skincare shoppers')

class GroundedSubmissionTests(TestCase):
    def setUp(self):
        fixture=fixtures.InterpreterSubmissionTests();fixture.setUp();self.inputs=fixture.inputs
    def plan(self,**changes):return prepare_grounded_submission(selection=SELECT|changes,**self.inputs)
    def test_selection_is_part_of_fingerprint_and_atomic_authority(self):
        p=self.plan();params={v.name:v.value for v in p.config().query_parameters}
        req=decode_row(p.candidate_json,'requests');authority=json.loads(params['authority_payload'])
        self.assertEqual(req['submitted_preferences']['preferences']['canonical_sku'],'CI-SKU-0021')
        row={k:authority[k] for k in ('workspace_id','conversation_id','request_id','principal_id')}
        row.update(payload_canonical_json=params['authority_payload'],payload_sha256=params['authority_digest'],recorded_at=req['created_at'])
        self.assertEqual(decode_authority(row,request=req,identity=SimpleNamespace(principal=SimpleNamespace(principal_id=req['principal_id'])),observed_at=req['created_at']),authority)
        self.assertIn('CHAT_PRODUCT_UNAVAILABLE',p.sql);self.assertIn('CHAT_PRIOR_TRANSCRIPT_CAPTURE_REQUIRED',p.sql)
        self.assertEqual(p.sql.count('COMMIT TRANSACTION;'),1)
        self.assertEqual(authority['claims'],[]);self.assertIsNone(authority['formula_version'])
    def test_selection_changes_same_native_slot_but_changes_authority(self):
        first=self.plan();second=self.plan(country='CA')
        self.assertEqual(first.job_id,second.job_id)
        a={p.name:p.value for p in first.config().query_parameters};b={p.name:p.value for p in second.config().query_parameters}
        self.assertNotEqual(a['authority_digest'],b['authority_digest'])
    def test_no_default_market_channel_product_or_audience(self):
        for value in ({},SELECT|{'country':'EU'},SELECT|{'sku':'CI-SKU-9999'},SELECT|{'channel':'PAID_SOCIAL'},SELECT|{'audience':' '}):
            with self.assertRaises(ValueError):validate_selection(value)
    def test_email_family_explicit(self):
        req=decode_row(self.plan(channel='KLAVIYO_EMAIL').candidate_json,'requests')
        self.assertEqual(req['submitted_preferences']['preferences']['channel'],'EMAIL')

    def test_service_rechecks_authoritative_capture_after_commit(self):
        from unittest.mock import Mock,patch
        from google.cloud import bigquery
        from src.application.campaign_quality_authority import table_metadata,TABLE
        from src.application.campaign_transcript import SPEC
        from src.application.control_io import ControlOutcome
        from src.application.sharing_coordinator_contract import SharingCommitStatus
        fixture=fixtures.InterpreterSubmissionTests();fixture.setUp()
        instance=fixture.instance();self.addCleanup(instance.client.close);instance.enabled=True
        def metadata(reference,**kwargs):
            expected=table_metadata() if str(reference)==TABLE else SPEC.table(instance.client.project)
            value=expected.to_api_repr();value.update(type='TABLE',location='asia-south1')
            return bigquery.Table.from_api_repr(value)
        instance.client.get_table=Mock(side_effect=metadata)
        def commit(plan):
            params={p.name:p.value for p in plan.config().query_parameters}
            return ControlOutcome(plan.job_id,SharingCommitStatus.COMMITTED,(dict(state='APPLIED',
                request_row=params['request_candidate'],invocation_row=params['invocation_candidate'],
                transcript_row=params['transcript_candidate'],authority_sha256=params['authority_digest']),),True)
        instance.io.submit_once=Mock(side_effect=commit)
        def read(*args,**kwargs):
            plan=next(iter(instance._plans.values()));params={p.name:p.value for p in plan.config().query_parameters}
            return SimpleNamespace(content_canonical_json=json.dumps(dict(authority=json.loads(params['authority_payload']))))
        with patch('src.application.campaign_quality_authority_reader.read_execution_authority',side_effect=read) as capture:
            receipt=instance.submit({},**fixture.args(),selection=SELECT)
            self.assertEqual(receipt.native_status,'COMMITTED');capture.assert_called_once()
        with patch('src.application.campaign_quality_authority_reader.read_execution_authority',return_value=SimpleNamespace(content_canonical_json='{"authority":{}}')):
            with self.assertRaisesRegex(ValueError,'AUTHORITY_CHANGED'):
                instance.submit({},**fixture.args(),selection=SELECT)
