"""Qualification source capture checks; synthetic records are not real measurements."""
from copy import deepcopy
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock, patch
from src.application import campaign_quality_qualification_reader as reader
from src.application.workflow_contract import canonical_json_bytes
from src.application.identity import IapIdentityAdapter, IapSettings
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from tests.test_campaign_quality_qualification import qualification_fixture
from tests.test_campaign_permission_lifecycle import identity, AUDIENCE


def capture_fixture():
    from tests.test_campaign_quality_publication import publication_fixture
    from src.application.campaign_quality_publication import build_publication
    from src.application.campaign_review_storage import PublicationControl,row_json
    import json
    args=publication_fixture();capture=args['capture'];plan=build_publication(**args)
    audit=json.loads(plan.audit_json);now=args['checked_at']
    control=PublicationControl(2,json.loads(args['control'].head_json)['control_sha256'],
        plan.command_sha256,audit['changed_by'],now)
    row=dict(observed_at=now,benchmarks=[capture['benchmark_json']],benchmarks_count=1,
        reports=[capture['report_json']],reports_count=1,revocation_count=0,
        invocations=list(capture['invocations']),invocations_count=len(capture['invocations']),
        bindings=list(capture['bindings']),bindings_count=len(capture['bindings']),
        requests=list(capture['requests']),requests_count=len(capture['requests']),
        publication_controls=[row_json(control.row())],publication_controls_count=1,
        publication_history=list(args['control'].history_json)+[row_json(control.row())],publication_history_count=2,
        measurement_publications=[plan.audit_json],measurement_publications_count=1)
    scope=dict(pins=args['pins'],requirements=args['requirements'],recorder_ids=(audit['changed_by'],),
        publisher_manifest_sha256=args['manifest'].sha256,
        native_job_id='native-read-fixture',started=now,ended=now)
    return row,scope


class QualificationReaderTests(TestCase):
    def test_exact_capture_and_current_revocation(self):
        row,scope=capture_fixture()
        result=reader.decode_capture(row,**scope)
        self.assertEqual(result.pins,scope['pins'])
        for count in (1,True,-1):
            with self.subTest(count=count),self.assertRaisesRegex(ValueError,'REVOKED'):
                reader.decode_capture(dict(row,revocation_count=count),**scope)

    def test_missing_duplicate_overflow_and_unauthorized_recorder_deny(self):
        row,scope=capture_fixture()
        for changes in (dict(reports=[]),dict(reports_count=2),dict(bindings_count=True),
                        dict(invocations_count=513),dict(reports=[None])):
            with self.subTest(changes=changes),self.assertRaises(ValueError):reader.decode_capture(dict(row,**changes),**scope)
        with self.assertRaisesRegex(ValueError,'RECORDER_DENIED'):
            reader.decode_capture(row,**dict(scope,recorder_ids=('iap-v1:'+'0'*64,)))

    def test_expired_future_and_changed_source_deny(self):
        row,scope=capture_fixture()
        with self.assertRaisesRegex(ValueError,'CAPTURE_EXPIRED'):
            reader.decode_capture(row,**dict(scope,ended=scope['ended']+timedelta(seconds=61)))
        with self.assertRaisesRegex(ValueError,'CAPTURE_EXPIRED'):
            reader.decode_capture(dict(row,observed_at=scope['ended']+timedelta(seconds=1)),**scope)
        corrupt=deepcopy(row);corrupt['reports'][0]=corrupt['reports'][0].replace('MEASURED_SYNTHETIC_FIXTURE','CHANGED')
        with self.assertRaisesRegex(ValueError,'DOCUMENT_HASH'):reader.decode_capture(corrupt,**scope)

    def test_native_correlation_and_reauthentication_are_required(self):
        row,scope=capture_fixture();actor=identity()
        verifier=IapIdentityAdapter(IapSettings(AUDIENCE));verifier.authenticate=Mock(return_value=actor)
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        service=reader.QualityQualificationReader(client=client,identity_adapter=verifier,pins=scope['pins'],
            requirements=scope['requirements'],reader_ids=(actor.principal.principal_id,),
            recorder_ids=scope['recorder_ids'],publisher_manifest_sha256=scope['publisher_manifest_sha256'],clock=lambda:scope['ended'])
        job=Mock(state='DONE',error_result=None);job.result.return_value=[row]
        client.query.return_value=client.get_job.return_value=job
        with patch.object(reader,'require_owned_worker'),patch.object(reader,'_identity'),patch.object(reader,'validate_table'),\
             patch.object(reader,'matches_sharing_job',return_value=True):
            result=service.read({},scope['pins'])
        self.assertTrue(result.native_job_id.startswith('quality_qualification_read_'))
        self.assertEqual(verifier.authenticate.call_count,2)
        with patch.object(reader,'require_owned_worker'),patch.object(reader,'_identity'),patch.object(reader,'validate_table'),\
             patch.object(reader,'matches_sharing_job',return_value=False),self.assertRaisesRegex(ValueError,'NATIVE_MISMATCH'):
            service.read({},scope['pins'])
        self.assertNotIn('INSERT',reader.SQL);self.assertNotIn('UPDATE',reader.SQL)
        self.assertIn('workspace_id=@workspace_id',reader.SQL)
        self.assertIn("target_kind='REPORT'",reader.SQL)

    def test_missing_spoofed_or_foreign_publication_proof_deny(self):
        row,scope=capture_fixture()
        for changes in (dict(measurement_publications=[],measurement_publications_count=0),
                        dict(publication_history=row['publication_history'][1:],publication_history_count=1)):
            with self.subTest(changes=changes),self.assertRaises(ValueError):reader.decode_capture(row|changes,**scope)
        with self.assertRaisesRegex(ValueError,'AUDIT_CONFLICT'):
            reader.decode_capture(row,**(scope|dict(publisher_manifest_sha256='0'*64)))

    def test_changed_publication_reason_is_not_accepted_as_measurement_proof(self):
        import json
        row,scope=capture_fixture()
        audit=json.loads(row['measurement_publications'][0]);audit['reason_code']='OTHER_ACTION'
        row['measurement_publications']=[canonical_json_bytes(audit).decode()]
        with self.assertRaisesRegex(ValueError,'AUDIT_CONFLICT'):reader.decode_capture(row,**scope)
