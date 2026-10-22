"""Owned publication admission and original-job recovery; no live authority."""
from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_quality_publisher as publisher
from src.application.identity import IapIdentityAdapter,IapSettings
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.validation.quality_measurement_fixtures import fixture


class MeasurementPublisherTests(TestCase):
    def service(self,enabled=True):
        args,_=fixture();verifier=IapIdentityAdapter(IapSettings(args['identity'].audience))
        verifier.authenticate=Mock(return_value=args['identity'])
        client=Mock(project=publisher.PROJECT,location=publisher.LOCATION,default_query_job_config=None)
        with patch.object(publisher,'ControlIO'):
            service=publisher.MeasurementPublisher(client=client,identity_adapter=verifier,manifest=args['manifest'],
                enabled=enabled,clock=lambda:args['checked_at'])
        service._capture=Mock(return_value=(args['identity'],args['control'],args['capture'],args['checked_at']))
        return service,args

    def prepare(self,service,args):
        return service.prepare({},pins=args['pins'],requirements=args['requirements'],
            benchmark_json=args['capture']['benchmark_json'],report_json=args['capture']['report_json'])

    def test_disabled_cannot_capture_or_write(self):
        service,args=self.service(False)
        with patch.object(publisher,'require_owned_worker'),self.assertRaisesRegex(ValueError,'DISABLED'):
            self.prepare(service,args)
        service._capture.assert_not_called();service.io.submit_once.assert_not_called()

    def test_retained_intent_single_submit_then_original_recovery(self):
        service,args=self.service()
        with patch.object(publisher,'require_owned_worker'):
            intent=self.prepare(service,args)
            unknown=ControlOutcome(intent.io_plan.job_id,Status.UNKNOWN)
            service.io.submit_once.return_value=service.io.recover.return_value=unknown
            self.assertIs(service.publish({},intent),unknown)
            self.assertIs(service.publish({},intent),unknown)
            with self.assertRaisesRegex(ValueError,'ORIGINAL_INTENT_REQUIRED'):
                service.publish({},replace(intent))
            with self.assertRaisesRegex(ValueError,'ORIGINAL_INTENT_REQUIRED'):self.prepare(service,args)
        service.io.submit_once.assert_called_once_with(intent.io_plan)
        service.io.recover.assert_called_once_with(intent.io_plan)

    def test_commit_requires_qualifying_read_and_current_authority(self):
        service,args=self.service()
        with patch.object(publisher,'require_owned_worker'):
            intent=self.prepare(service,args)
            result=ControlOutcome(intent.io_plan.job_id,Status.COMMITTED,({'state':'APPLIED',
                'command_sha256':intent.publication.command_sha256,'record_sha256':args['pins'].evaluation_sha256,
                'control_sha256':intent.publication.candidate_control_sha256},))
            service.io.submit_once.return_value=result
            with patch.object(publisher,'QualityQualificationReader') as reader:
                reader.return_value.read.side_effect=ValueError('REVOKED')
                self.assertIs(service.publish({},intent).native_status,Status.UNKNOWN)
            service.verifier.authenticate.side_effect=ValueError('REVOKED_ACCESS')
            with self.assertRaisesRegex(ValueError,'REVOKED_ACCESS'):service.recover({},intent)
        service.io.recover.assert_not_called()

    def test_capture_sql_pins_native_provider_scope_and_revocations(self):
        sql=publisher.capture_sql()
        for token in ('@report_json','@read_id','workspace_id=@workspace_id',"target_kind='REPORT'",'history_count'):
            self.assertIn(token,sql)
        self.assertNotIn('INSERT',sql)

    def test_foreign_scope_or_changed_document_denies_before_cloud_capture(self):
        service,args=self.service()
        with patch.object(publisher,'require_owned_worker'):
            for changes in (dict(pins=replace(args['pins'],workspace_id='foreign')),
                            dict(benchmark_json='{}'),dict(report_json='x'*2097153)):
                values=dict(pins=args['pins'],requirements=args['requirements'],
                    benchmark_json=args['capture']['benchmark_json'],report_json=args['capture']['report_json'])|changes
                with self.subTest(changes=list(changes)),self.assertRaises(ValueError):service.prepare({},**values)
        service._capture.assert_not_called();service.io.submit_once.assert_not_called()
