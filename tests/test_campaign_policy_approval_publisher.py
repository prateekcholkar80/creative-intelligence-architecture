"""Trusted writer coordination with synthetic identities/sources; no cloud calls."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock, patch
from itertools import count
from src.application import campaign_policy_approval_publisher as writer
from src.application.campaign_policy_approval_receipt import _text
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.identity import IapIdentityAdapter, IapSettings
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from tests.test_campaign_policy_approval_publication import inputs, manifest
from tests.test_campaign_permission_lifecycle import NOW, AUDIENCE, identity


class ApprovalPublisherTests(TestCase):
    def instance(self, enabled=True):
        args=inputs();actor=args['identity']
        verifier=IapIdentityAdapter(IapSettings(AUDIENCE));verifier.authenticate=Mock(return_value=actor)
        sequence=count()
        qualification=lambda headers,pins:writer.PolicyEvaluationQualification(pins,'fixture-evaluation-read-'+str(next(sequence)),NOW,NOW+timedelta(seconds=60))
        with patch.object(writer,'ControlIO'):
            service=writer.PolicyApprovalPublisher(client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None),
                identity_adapter=verifier,publisher_manifest=manifest(actor),enabled=enabled,qualification_reader=qualification,clock=lambda:NOW)
        service._capture=Mock(return_value=writer.ApprovalCapture(args['sources'],NOW))
        return service,args

    def prepare(self,service,args):
        return service.prepare({},action='CREATE_RECEIPT',authority_id=args['authority_id'],receipt_id=args['receipt_id'],
            candidate=args['candidate'],confirmed_pins_sha256=args['confirmed_pins_sha256'],reason_code=args['reason_code'])

    def committed(self,intent):
        p=intent.publication
        return ControlOutcome(p.native_job_id,Status.COMMITTED,(
            dict(state='APPLIED',command_sha256=p.command_sha256,record_sha256=p.proposed_record_sha256,control_sha256=p.candidate_control_sha256),),True)

    def install_postread(self,service,intent):
        candidate=intent.candidate
        encoded=_text(dict(receipt_id=candidate.receipt_id,authority_id=candidate.authority_id,
            receipt_sha256=candidate.sha256,canonical_json=_text(candidate.payload())))
        source=service._capture.return_value.sources
        service._capture.return_value=writer.ApprovalCapture(replace(source,receipts=(encoded,)),NOW)

    def test_disabled_never_reads_authenticates_or_submits(self):
        service,args=self.instance(False)
        with patch.object(writer,'require_owned_worker'):
            with self.assertRaisesRegex(ValueError,'DISABLED'):self.prepare(service,args)
        service._capture.assert_not_called();service.verifier.authenticate.assert_not_called();service.io.submit_once.assert_not_called()

    def test_success_and_repeat_only_recovers_original(self):
        service,args=self.instance()
        with patch.object(writer,'require_owned_worker'):
            intent=self.prepare(service,args);self.install_postread(service,intent)
            result=self.committed(intent);service.io.submit_once.return_value=service.io.recover.return_value=result
            self.assertIs(service.publish({},intent),result)
            self.assertIs(service.publish({},intent),result)
        service.io.submit_once.assert_called_once_with(intent.io_plan);service.io.recover.assert_called_once_with(intent.io_plan)

    def test_lost_response_never_resubmits(self):
        service,args=self.instance()
        with patch.object(writer,'require_owned_worker'):
            intent=self.prepare(service,args);service.io.submit_once.side_effect=RuntimeError('lost')
            with self.assertRaises(RuntimeError):service.publish({},intent)
            service.io.recover.return_value=ControlOutcome(intent.io_plan.job_id,Status.UNKNOWN)
            self.assertIs(service.publish({},intent).native_status,Status.UNKNOWN)
        service.io.submit_once.assert_called_once();service.io.recover.assert_called_once()

    def test_reconstructed_or_foreign_actor_intent_denied(self):
        service,args=self.instance()
        with patch.object(writer,'require_owned_worker'):
            intent=self.prepare(service,args)
            with self.assertRaisesRegex(ValueError,'ORIGINAL_INTENT'):service.publish({},replace(intent))
            service.verifier.authenticate.return_value=identity('foreign-reviewer')
            with self.assertRaisesRegex(ValueError,'ACTOR_CHANGED'):service.recover({},intent)
        service.io.submit_once.assert_not_called();service.io.recover.assert_not_called()

    def test_current_revocation_denies_before_and_after_io(self):
        service,args=self.instance()
        with patch.object(writer,'require_owned_worker'):
            intent=self.prepare(service,args);old=service._capture.return_value
            revoked=_text(dict(revocation_id='revoke-one',target_kind='AUTHORITY',target_id=args['authority_id'],revoked_at=NOW.isoformat()))
            denied=writer.ApprovalCapture(replace(old.sources,revocations=(revoked,)),NOW)
            service._capture.return_value=denied
            with self.assertRaisesRegex(ValueError,'REVOKED'):service.publish({},intent)
            service.io.submit_once.assert_not_called()
            service._capture.side_effect=[old,denied]
            service.io.submit_once.return_value=self.committed(intent)
            with self.assertRaisesRegex(ValueError,'REVOKED'):service.publish({},intent)
        service.io.submit_once.assert_called_once()

    def test_changed_logical_slot_denied_and_same_intent_retained(self):
        service,args=self.instance()
        with patch.object(writer,'require_owned_worker'):
            first=self.prepare(service,args);self.assertIs(self.prepare(service,args),first)
            args=dict(args,candidate=replace(args['candidate'],valid_until=NOW+timedelta(minutes=2)))
            with self.assertRaisesRegex(ValueError,'SLOT_CONFLICT'):self.prepare(service,args)

    def test_foreign_or_corrupt_receipt_and_missing_postread_are_unknown(self):
        service,args=self.instance()
        with patch.object(writer,'require_owned_worker'):
            intent=self.prepare(service,args)
            for outcome in (replace(self.committed(intent),job_id='other-job'),
                            replace(self.committed(intent),rows=({'state':'APPLIED'},)),self.committed(intent)):
                service.io.recover.return_value=outcome
                self.assertIs(service.recover({},intent).native_status,Status.UNKNOWN)

    def test_expired_review_and_source_failure_withhold(self):
        service,args=self.instance()
        with patch.object(writer,'require_owned_worker'):
            intent=self.prepare(service,args);service.clock=lambda:NOW+timedelta(minutes=31)
            with self.assertRaises(ValueError):service.recover({},intent)
            service.clock=lambda:NOW;service._capture.side_effect=ValueError('source unavailable')
            with self.assertRaisesRegex(ValueError,'source unavailable'):service.recover({},intent)
        service.io.recover.assert_not_called()

    def test_capture_sql_covers_shared_control_and_both_revocation_targets(self):
        sql=writer.capture_sql()
        self.assertEqual(sql.count('LIMIT 65'),5)
        self.assertIn("target_kind='AUTHORITY'",sql);self.assertIn("target_kind='RECEIPT'",sql)
        self.assertIn('campaign_permission_control_history',sql)
        self.assertNotIn('DELETE',sql);self.assertNotIn('INSERT',sql)

    def native_capture(self, change=None):
        service,args=self.instance()
        service._capture=writer.PolicyApprovalPublisher._capture.__get__(service)
        source=args['sources']
        def read(sql,parameters):
            row=dict(observed_at=NOW,read_id=next(p.value for p in parameters if p.name=='read_id'),
                authorities=list(source.authorities),receipts=[],revocations=[],
                control=[source.control.head_json],history=list(source.control.history_json))
            for key in ('authorities','receipts','revocations','control','history'):row[key+'_count']=len(row[key])
            if change:change(row)
            return row
        service.io.read.side_effect=read
        return service,args

    def test_real_capture_normalizes_and_validates_full_control_chain(self):
        service,args=self.native_capture()
        with patch.object(writer,'validate_table'):
            captured=service._capture(args['identity'],args['authority_id'],args['receipt_id'])
        self.assertEqual(captured.sources,args['sources'])
        self.assertEqual(service.client.get_table.call_count,6)

    def test_capture_overflow_count_conflict_and_partial_control_deny(self):
        changes=(lambda row:row.update(authorities_count=True),
            lambda row:row.update(authorities_count=2),lambda row:row.update(history=[],history_count=0),
            lambda row:row.update(authorities=['x'*65537]))
        for change in changes:
            service,args=self.native_capture(change)
            with patch.object(writer,'validate_table'),self.assertRaises(ValueError):
                service._capture(args['identity'],args['authority_id'],args['receipt_id'])

    def test_uninstalled_or_changed_qualifying_reader_cannot_record_approval(self):
        service,args=self.instance();service.qualification_reader=None
        with patch.object(writer,'require_owned_worker'):
            with self.assertRaisesRegex(ValueError,'QUALIFYING_EVALUATION_UNAVAILABLE'):self.prepare(service,args)
        service.io.submit_once.assert_not_called()
        service,args=self.instance()
        service.qualification_reader=lambda headers,pins:writer.PolicyEvaluationQualification(
            replace(pins,evaluation_sha256='0'*64),'wrong-evaluation',NOW,NOW+timedelta(seconds=60))
        with patch.object(writer,'require_owned_worker'):
            with self.assertRaisesRegex(ValueError,'QUALIFYING_EVALUATION_CHANGED'):self.prepare(service,args)
        service.io.submit_once.assert_not_called()

    def test_cached_qualifying_report_is_rejected_before_publication(self):
        service,args=self.instance()
        service.qualification_reader=lambda headers,pins:writer.PolicyEvaluationQualification(pins,'same-read',NOW,NOW+timedelta(seconds=60))
        with patch.object(writer,'require_owned_worker'):
            intent=self.prepare(service,args)
            with self.assertRaisesRegex(ValueError,'NOT_REREAD'):service.publish({},intent)
        service.io.submit_once.assert_not_called()
