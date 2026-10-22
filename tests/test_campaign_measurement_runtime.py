"""Protected dependency installation, fresh authority and fail-closed qualification."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_measurement_runtime as runtime
from src.application.workflow_codec import encode_row
from src.validation.control_fixtures import controls,SUBJECT
from src.application.identity import VerifiedIdentity,principal_for_subject,ISSUER
from tests.test_campaign_permission_lifecycle import AUDIENCE,NOW
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest


def authority_fixture():
    seed=controls('ci-poc-marketing');member=seed['memberships'][0]
    actor=VerifiedIdentity(principal_for_subject(ISSUER,SUBJECT),ISSUER,SUBJECT,AUDIENCE,int(NOW.timestamp())-1,int(NOW.timestamp())+299)
    settings=runtime.MeasurementRuntimeSettings('ci-poc-marketing',actor.principal.principal_id,'a'*64,'b'*64)
    # Fixture clock follows its rows; no actual identity assertion is provided.
    now=max(NOW,member['updated_at'])+timedelta(seconds=1)
    actor=replace(actor,issued_at=int(now.timestamp())-1,expires_at=int(now.timestamp())+299)
    row=dict(observed_at=now,workspaces=[encode_row(seed['workspaces'][0],'workspaces')],workspace_count=1,
        memberships=[encode_row(member,'memberships')],membership_count=1)
    return row,dict(settings=settings,identity=actor,started=now,ended=now),seed


class MeasurementRuntimeTests(TestCase):
    def test_current_exact_authority_and_membership_revocation(self):
        row,args,seed=authority_fixture()
        self.assertEqual(runtime.verify_authority(row,**args),(1,1))
        for status in ('REVOKED','SUSPENDED'):
            changed=dict(seed['memberships'][0],status=status)
            with self.subTest(status=status),self.assertRaises(ValueError):
                runtime.verify_authority(row|dict(memberships=[encode_row(changed,'memberships')]),**args)
        with self.assertRaises(ValueError):runtime.verify_authority(row,**(args|dict(settings=replace(args['settings'],publisher_id='iap-v1:'+'0'*64))))

    def test_missing_duplicate_future_and_stale_capture_deny(self):
        row,args,_=authority_fixture()
        for changes in (dict(membership_count=0,memberships=[]),dict(workspace_count=True),dict(workspace_count=2),
            dict(observed_at=args['ended']+timedelta(seconds=1))):
            with self.subTest(changes=changes),self.assertRaises(ValueError):runtime.verify_authority(row|changes,**args)
        with self.assertRaises(ValueError):runtime.verify_authority(row,**(args|dict(ended=args['ended']+timedelta(seconds=61))))

    def test_installation_is_disabled_and_qualification_cannot_be_invented(self):
        row,args,_=authority_fixture();settings=args['settings'];actor=args['identity'];now=args['ended']
        manifest=OperatorPublisherManifest('explicit-measurement-read',AUDIENCE,(settings.publisher_id,),now,now+timedelta(seconds=299))
        with patch.object(runtime,'require_owned_worker'),patch.object(runtime,'ControlIO'),\
             patch.object(runtime,'MeasurementPublisher') as publisher,patch.object(runtime,'PolicyApprovalPublisher') as approvals:
            service=runtime.ProtectedMeasurementRuntime(client=Mock(),verifier=Mock(),settings=settings,session_manifest=manifest,clock=lambda:now)
            self.assertFalse(publisher.call_args.kwargs['enabled']);self.assertFalse(approvals.call_args.kwargs['enabled'])
            service._actor=Mock(return_value=actor)
            with self.assertRaisesRegex(ValueError,'UNAVAILABLE'):service.qualify({},Mock())
            with self.assertRaisesRegex(ValueError,'UNAVAILABLE'):service.approval_guard(Mock(),Mock())
            with self.assertRaisesRegex(ValueError,'QUALIFICATION_SCOPE'):service.configure_qualification(Mock())

    def test_current_rechecks_identity_and_native_membership_on_every_call(self):
        row,args,_=authority_fixture();now=args['ended'];settings=args['settings']
        service=object.__new__(runtime.ProtectedMeasurementRuntime)
        service.client=Mock();service.settings=settings;service.clock=lambda:now;service._qualification=None
        service._actor=Mock(return_value=args['identity']);service.io=Mock();service.io.read.return_value=row
        with patch.object(runtime,'validate_table'):
            result=service.current({})
            self.assertTrue(result['runtime_installed']);self.assertFalse(result['release_allowed'])
            service.io.read.return_value=row|dict(membership_count=0,memberships=[])
            with self.assertRaises(ValueError):service.current({})
        self.assertEqual(service.io.read.call_count,2)
        self.assertEqual(service._actor.call_count,3)
