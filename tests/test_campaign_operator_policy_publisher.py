"""Owned publisher coordination with synthetic authority; no cloud writes."""
from dataclasses import replace
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_operator_policy_publisher as service
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.campaign_review_storage import row_json
from src.application.campaign_operator_policy_storage import encode_record,head_row
from src.application.identity import IapIdentityAdapter,IapSettings
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION
from tests.test_campaign_operator_policy_publication import manifest
from tests.test_campaign_review_publication_writer import OPERATOR,policy,control_rows
from tests.test_campaign_operator_policy_storage import record
from tests.test_campaign_permission_lifecycle import NOW,AUDIENCE


class OperatorPublisherTests(TestCase):
    def instance(self,enabled=True):
        verifier=IapIdentityAdapter(IapSettings(AUDIENCE));verifier.authenticate=Mock(return_value=OPERATOR)
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        with patch.object(service,'ControlIO'):
            result=service.OperatorPolicyPublisher(client=client,identity_adapter=verifier,
                manifest=manifest(),enabled=enabled,clock=lambda:NOW)
        result._capture=Mock(return_value=service.OperatorPublicationCapture(control_rows(),None,(),'read-one',NOW))
        return result

    def prepare(self,current):return current.prepare({},policy=policy(),reason_code='EXPLICIT_FIXTURE_POLICY')

    def committed(self,intent):
        value=intent.publication
        return ControlOutcome(value.native_job_id,Status.COMMITTED,(dict(state='APPLIED',
            command_sha256=value.command_sha256,record_sha256=value.proposed_record_sha256,
            control_sha256=value.candidate_control_sha256),),True)

    def test_disabled_never_authenticates_reads_or_writes(self):
        current=self.instance(False)
        with patch.object(service,'require_owned_worker'):
            with self.assertRaisesRegex(ValueError,'OPERATOR_PUBLISHER_DISABLED'):self.prepare(current)
        current.identity_adapter.authenticate.assert_not_called();current._capture.assert_not_called()
        current.io.submit_once.assert_not_called()

    def test_success_and_repeat_recover_original_without_second_submission(self):
        current=self.instance()
        with patch.object(service,'require_owned_worker'):
            intent=self.prepare(current);outcome=self.committed(intent)
            current.io.submit_once.return_value=current.io.recover.return_value=outcome
            self.assertIs(current.publish({},intent),outcome)
            self.assertIs(current.publish({},intent),outcome)
        current.io.submit_once.assert_called_once_with(intent.io_plan)
        current.io.recover.assert_called_once_with(intent.io_plan)
        self.assertGreaterEqual(current._capture.call_count,5)

    def test_unknown_or_lost_response_never_mints_retry(self):
        current=self.instance()
        with patch.object(service,'require_owned_worker'):
            intent=self.prepare(current)
            current.io.submit_once.side_effect=RuntimeError('lost result')
            with self.assertRaises(RuntimeError):current.publish({},intent)
            current.io.recover.return_value=ControlOutcome(intent.io_plan.job_id,Status.UNKNOWN)
            self.assertIs(current.publish({},intent).native_status,Status.UNKNOWN)
        current.io.submit_once.assert_called_once();current.io.recover.assert_called_once()

    def test_reprepare_same_revision_returns_original_and_changed_content_conflicts(self):
        current=self.instance()
        with patch.object(service,'require_owned_worker'):
            first=self.prepare(current)
            self.assertIs(self.prepare(current),first)
            with self.assertRaisesRegex(ValueError,'OPERATOR_POLICY_SLOT_CONFLICT'):
                current.prepare({},policy=replace(policy(),max_review_seconds=1),reason_code='EXPLICIT_FIXTURE_POLICY')
        current._capture.assert_called_once()

    def test_reconstructed_intent_or_revoked_publisher_never_submits(self):
        current=self.instance()
        with patch.object(service,'require_owned_worker'):
            intent=self.prepare(current)
            with self.assertRaisesRegex(ValueError,'OPERATOR_ORIGINAL_INTENT_REQUIRED'):
                current.publish({},replace(intent))
            current.clock=lambda:NOW+timedelta(seconds=120)
            with self.assertRaisesRegex(ValueError,'OPERATOR_PUBLISHER_DENIED'):current.publish({},intent)
        current.io.submit_once.assert_not_called()

    def test_corrupt_commit_receipt_is_unknown_and_read_failure_withholds(self):
        current=self.instance()
        with patch.object(service,'require_owned_worker'):
            intent=self.prepare(current)
            current.io.submit_once.return_value=replace(self.committed(intent),rows=({'state':'APPLIED'},))
            self.assertIs(current.publish({},intent).native_status,Status.UNKNOWN)
            current._capture.side_effect=ValueError('current source unavailable')
            with self.assertRaises(ValueError):current.recover({},intent)
        current.io.recover.assert_not_called()

    def test_current_read_rejects_revoked_policy(self):
        current=self.instance();first=record();revoked=record(2,first,status='REVOKED')
        rows=tuple(row_json(encode_record(r)) for r in (first,revoked))
        current._capture.return_value=service.OperatorPublicationCapture(control_rows(),
            row_json(head_row(encode_record(revoked))),rows,'read-current',NOW)
        with patch.object(service,'require_owned_worker'):
            with self.assertRaisesRegex(ValueError,'OPERATOR_POLICY_NOT_CURRENT'):current.read_current({})

    def test_capture_query_has_fixed_scope_counts_and_overflow_limits(self):
        sql=service.capture_sql()
        self.assertEqual(sql.count('LIMIT 65'),4)
        self.assertNotIn('UPDATE ',sql);self.assertNotIn('INSERT ',sql)
        self.assertIn("control_id='campaign-permissions'",sql)
        self.assertIn('policy_id=@policy_id',sql)

    def capture(self,change=None,records=()):
        current=self.instance()
        control=control_rows()
        head=row_json(head_row(encode_record(records[-1]))) if records else None
        def read(sql,parameters):
            marker=next(p.value for p in parameters if p.name=='read_id')
            row=dict(observed_at=NOW,read_id=marker,head=[head] if head else [],
                history=[row_json(encode_record(r)) for r in records],control=[control.head_json],
                controls=list(control.history_json))
            for key in service.SOURCES:row[key+'_count']=len(row[key])
            if change:change(row)
            return row
        current.io.read.side_effect=read
        with patch.object(service,'validate_table'):
            return service.OperatorPolicyPublisher._capture(current,OPERATOR)

    def test_native_unordered_json_is_canonicalized_and_empty_policy_is_readable(self):
        def native(row):
            for key in service.SOURCES:
                row[key]=[json.dumps(dict(reversed(list(json.loads(text).items())))) for text in row[key]]
        result=self.capture(native)
        self.assertIsNone(result.head_json);self.assertEqual(result.history_json,())
        self.assertEqual(result.control,control_rows())

    def test_capture_rejects_omitted_duplicate_overflow_and_orphan_rows(self):
        def duplicate(row):row['head']=[row_json(head_row(encode_record(record())))]*2;row['head_count']=2
        def orphan(row):row['history']=[row_json(encode_record(record()))];row['history_count']=1
        def overflow(row):row['controls']*=65;row['controls_count']=65
        def missing(row):row['controls']=[];row['controls_count']=0
        for change in (duplicate,orphan,overflow,missing,
                lambda r:r.update(history_count=1),lambda r:r.update(read_id='foreign-read'),
                lambda r:r.update(observed_at=NOW+timedelta(seconds=1))):
            with self.subTest(change=change),self.assertRaises(ValueError):self.capture(change)

    def test_capture_decodes_current_and_revoked_full_history(self):
        first=record();second=record(2,first,status='REVOKED')
        result=self.capture(records=(first,second))
        self.assertEqual(len(result.history_json),2)
        self.assertEqual(json.loads(result.head_json)['status'],'REVOKED')

    def test_foreign_native_receipt_is_never_accepted(self):
        current=self.instance()
        with patch.object(service,'require_owned_worker'):
            intent=self.prepare(current)
            current.io.submit_once.return_value=replace(self.committed(intent),job_id='foreign-job')
            self.assertIs(current.publish({},intent).native_status,Status.UNKNOWN)
