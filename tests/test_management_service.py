"""4.4 offline authorization, fencing, lifecycle and conservative recovery."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from src.application.control_io import ControlOutcome, ControlPlan, ControlIO
from src.application.identity import ISSUER, VerifiedIdentity, IapIdentityAdapter, IapSettings, principal_for_subject
from src.application.management_service import (ProtectedManagementService, ManagementSettings,
    MembershipSyncMapping, WorkspaceBootstrapManifest, bootstrap_sql)
from src.application.access_policy import AuthorizationError
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_codec import encode_row, decode_row
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import SCHEMA_VERSION
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from tests.test_identity import AUDIENCE


NOW=datetime.now(timezone.utc)-timedelta(seconds=5)
SUBJECT='accounts.google.com:control-fixture'
ACTOR=principal_for_subject(ISSUER,SUBJECT).principal_id
WS='control-fixture-workspace'
CONV='control-fixture-conversation'


def identity(subject=SUBJECT):
    epoch=int(datetime.now(timezone.utc).timestamp())
    return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,epoch-10,epoch+590)


def workspace(**values):
    return dict(schema_version=SCHEMA_VERSION,workspace_id=WS,status='ACTIVE',revision=1,created_at=NOW,updated_at=NOW)|values


def member(subject=SUBJECT,**values):
    return dict(schema_version=SCHEMA_VERSION,workspace_id=WS,principal_id=principal_for_subject(ISSUER,subject).principal_id,
        identity_issuer=ISSUER,identity_subject=subject,roles=['ADMIN','CREATOR'],status='ACTIVE',revision=1,
        assignment_origin='ADMIN',updated_by=ACTOR,created_at=NOW,updated_at=NOW)|values


def conversation(**values):
    return dict(schema_version=SCHEMA_VERSION,workspace_id=WS,conversation_id=CONV,owner_id=ACTOR,
        visibility='PRIVATE',status='ACTIVE',access_revision=1,created_at=NOW,updated_at=NOW)|values


def snapshot(ws=None,members=None,convs=None):
    values=([member()] if members is None else members)
    return dict(observed_at=datetime.now(timezone.utc),workspaces=[encode_row(workspace() if ws is None else ws,'workspaces')],
        memberships=[encode_row(m,'memberships') for m in sorted(values,key=lambda m:m['principal_id'])],
        member_count=len(values),conversations=[encode_row(c,'conversations') for c in ([] if convs is None else convs)])


def client():
    return bigquery.Client(project=PROJECT,location=LOCATION,credentials=AnonymousCredentials())


def service(settings=None):
    auth=IapIdentityAdapter(IapSettings(AUDIENCE));auth.authenticate=Mock(side_effect=lambda headers:identity())
    svc=ProtectedManagementService(client=client(),verifier=auth,settings=settings or ManagementSettings(enabled=True))
    svc.io.read=Mock(side_effect=lambda *a,**k:snapshot())
    svc.io.preflight=Mock()
    return svc


def applied(plan,submitted=True):
    return ControlOutcome(plan.job_id,Status.COMMITTED,tuple([dict(state='APPLIED',candidate=plan.candidate_json,audit=plan.audit_json)]),submitted)


class ManagementTests(unittest.TestCase):
    def setUp(self):
        self.svc=service();self.addCleanup(self.svc.io.client.close)

    def prepare(self,kind='MEMBER',**values):
        args=dict(kind=kind,workspace_id=WS,command_id='fixture-command',expected_workspace_revision=1)
        if kind=='MEMBER':args.update(target_subject='accounts.google.com:new-member',roles=('VIEWER',),status='ACTIVE')
        return self.svc.prepare({},**(args|values))

    def test_defaults_deny_before_identity_or_cloud(self):
        self.svc.settings=ManagementSettings()
        with self.assertRaises(ContractError):self.prepare()
        self.svc.verifier.authenticate.assert_not_called();self.svc.io.read.assert_not_called()

    def test_member_creation_uses_destination_types_and_atomic_audit_fence(self):
        plan=self.prepare();row=decode_row(plan.candidate_json,'memberships')
        self.assertEqual(row['roles'],['VIEWER']);self.assertEqual(row['revision'],1)
        self.assertIn('LAST_ADMIN_REQUIRED',plan.sql);self.assertIn('ROLLBACK TRANSACTION',plan.sql)
        self.assertLess(plan.sql.index('UPDATE `'+PROJECT+'.ci_application.workspaces'),plan.sql.index('INSERT INTO `'+PROJECT+'.ci_application.memberships'))
        self.assertEqual(decode_row(plan.audit_json,'audit_events')['action'],'MANAGE_MEMBERS')
        self.assertNotIn('DELETE ',plan.sql);self.assertNotIn('MERGE ',plan.sql)

    def test_non_admin_cannot_manage_members(self):
        self.svc.io.read=Mock(return_value=snapshot(members=[member(roles=['CREATOR'])]))
        with self.assertRaises(AuthorizationError):self.prepare()

    def test_last_admin_demote_and_disable_are_denied(self):
        for roles,status in ((('CREATOR',),'ACTIVE'),(('ADMIN',),'REVOKED')):
            with self.subTest(roles=roles,status=status),self.assertRaises(ContractError) as caught:
                self.prepare(target_subject=SUBJECT,roles=roles,status=status,expected_revision=1)
            self.assertEqual(caught.exception.code,'LAST_ADMIN_REQUIRED')

    def test_two_admin_capture_can_propose_demotion_and_pins_entire_membership_set(self):
        self.svc.io.read=Mock(return_value=snapshot(members=[member(),member('accounts.google.com:other-admin')]))
        plan=self.prepare(target_subject=SUBJECT,roles=('VIEWER',),expected_revision=1)
        self.assertIn('MEMBERSHIP_CONFLICT',plan.sql)
        self.assertEqual(len(json.loads(next(p.value for p in plan.config().query_parameters if p.name=='members_pin'))),2)

    def test_same_command_reuses_envelope_changed_command_content_denied(self):
        first=self.prepare();self.assertIs(self.prepare(),first)
        with self.assertRaises(ContractError):self.prepare(roles=('CREATOR',))

    def test_stale_workspace_and_target_revisions_deny(self):
        with self.assertRaises(ContractError):self.prepare(expected_workspace_revision=2)
        with self.assertRaises(ContractError):self.prepare(target_subject=SUBJECT,expected_revision=2)

    def test_duplicate_or_missing_workspace_membership_capture_denied(self):
        for mode in ('workspaces','memberships','count'):
            row=snapshot()
            if mode=='count':row['member_count']=999
            else:row[mode]*=2
            self.svc.io.read=Mock(return_value=row)
            with self.subTest(mode=mode),self.assertRaises(ContractError):self.prepare()

    def test_disabled_workspace_blocks_members_but_admin_can_reactivate(self):
        self.svc.io.read=Mock(return_value=snapshot(ws=workspace(status='DISABLED')))
        with self.assertRaises(ContractError):self.prepare()
        plan=self.prepare('WORKSPACE_STATUS',status='ACTIVE')
        self.assertEqual(decode_row(plan.candidate_json,'workspaces')['status'],'ACTIVE')

    def test_private_conversation_creation_derives_owner(self):
        plan=self.prepare('CREATE_CONVERSATION',conversation_id=CONV)
        row=decode_row(plan.candidate_json,'conversations')
        self.assertEqual((row['owner_id'],row['visibility']),(ACTOR,'PRIVATE'))

    def test_archive_requires_creator_owner_and_full_pin_and_no_active_jobs(self):
        self.svc.io.read=Mock(return_value=snapshot(convs=[conversation()]))
        plan=self.prepare('ARCHIVE',conversation_id=CONV,expected_revision=1)
        self.assertEqual(decode_row(plan.candidate_json,'conversations')['status'],'ARCHIVED')
        self.assertIn('CONVERSATION_BUSY',plan.sql);self.assertIn('@conversation_pin',plan.sql)
        self.svc._issued.clear();self.svc.io.read=Mock(return_value=snapshot(convs=[conversation(owner_id='other')]))
        with self.assertRaises(AuthorizationError):self.prepare('ARCHIVE',conversation_id=CONV,expected_revision=1)

    def test_archived_conversation_cannot_archive_or_restore(self):
        self.svc.io.read=Mock(return_value=snapshot(convs=[conversation(status='ARCHIVED')]))
        with self.assertRaises(ContractError):self.prepare('ARCHIVE',conversation_id=CONV,expected_revision=1)

    def test_sync_uses_only_installed_mapping_and_caps_native_window(self):
        mapping=MembershipSyncMapping('map',WS,'accounts.google.com:mapped',('VIEWER',),'ACTIVE',NOW,NOW+timedelta(seconds=60))
        self.svc.settings=ManagementSettings(enabled=True,sync_mappings=(mapping,))
        plan=self.prepare('SYNC',mapping_id='map')
        row=decode_row(plan.candidate_json,'memberships');self.assertEqual(row['assignment_origin'],'SYNC')
        self.assertEqual(next(p.value for p in plan.config().query_parameters if p.name=='end_at'),mapping.expires_at)
        with self.assertRaises(ContractError):self.prepare('SYNC',mapping_id='map',roles=('ADMIN',))

    def test_expired_or_missing_sync_mapping_denies(self):
        for mapping in (None,MembershipSyncMapping('map',WS,'accounts.google.com:mapped',('ADMIN',),'ACTIVE',NOW-timedelta(minutes=2),NOW)):
            self.svc.settings=ManagementSettings(enabled=True,sync_mappings=() if mapping is None else (mapping,))
            with self.assertRaises(ContractError):self.prepare('SYNC',mapping_id='map')

    def test_lost_response_consumes_entry_and_recovers_without_submit(self):
        plan=self.prepare();self.svc.io.submit_once=Mock(return_value=ControlOutcome(plan.job_id,Status.UNKNOWN))
        self.svc.io.recover=Mock(return_value=applied(plan,False))
        self.assertEqual(self.svc.submit({},plan).native_status,Status.UNKNOWN)
        self.assertEqual(self.svc.submit({},plan).native_status,Status.COMMITTED)
        self.svc.io.submit_once.assert_called_once();self.svc.io.recover.assert_called_once()

    def test_unissued_or_tampered_plan_never_submits(self):
        plan=self.prepare();self.svc.io.submit_once=Mock()
        for changed in (replace(plan),replace(plan,workspace_id='other'),replace(plan,sql='SELECT 1'),replace(plan,job_id='arbitrary')):
            with self.assertRaises(ContractError):self.svc.submit({},changed)
        self.svc.io.submit_once.assert_not_called()

    def test_recovery_after_admin_revocation_denies_before_job_read(self):
        plan=self.prepare();self.svc.io.recover=Mock()
        self.svc.io.read=Mock(return_value=snapshot(members=[member(status='REVOKED')]))
        with self.assertRaises(AuthorizationError):self.svc.recover({},plan)
        self.svc.io.recover.assert_not_called()

    def test_mismatched_native_result_is_never_released(self):
        plan=self.prepare();self.svc.io.submit_once=Mock(return_value=replace(applied(plan),rows=({'state':'APPLIED','candidate':'{}','audit':plan.audit_json},)))
        with self.assertRaises(ContractError):self.svc.submit({},plan)

    def test_bootstrap_has_no_implicit_owner_or_roles(self):
        with self.assertRaises(ContractError):self.prepare('BOOTSTRAP')
        with self.assertRaises(ContractError):WorkspaceBootstrapManifest(WS,SUBJECT,('CREATOR',),'boot',NOW,NOW+timedelta(minutes=4))

    def boot(self,slot=1):
        self.svc.settings=ManagementSettings(enabled=True,bootstrap=WorkspaceBootstrapManifest(WS,SUBJECT,('ADMIN','CREATOR'),'boot',NOW,NOW+timedelta(minutes=4)))
        return self.prepare('BOOTSTRAP',command_id='boot',slot=slot)

    def test_explicit_bootstrap_binds_owner_and_global_slots_and_audit(self):
        plan=self.boot();self.svc.io.read.assert_not_called()
        self.assertTrue(plan.job_id.endswith('_1'));self.assertIn('BOOTSTRAP_PARTIAL',plan.sql)
        self.assertIn('.ci_application.model_invocations',plan.sql)
        self.assertEqual(decode_row(plan.audit_json,'audit_events')['actor_id'],ACTOR)

    def test_bootstrap_later_slot_requires_all_predecessor_positive_rollback(self):
        plan=self.boot(3);self.svc.io.submit_once=Mock(return_value=applied(plan))
        self.svc.io.recover=Mock(side_effect=[ControlOutcome('a',Status.NOT_COMMITTED),ControlOutcome('b',Status.UNKNOWN)])
        with self.assertRaises(ContractError):self.svc.submit({},plan)
        self.svc.io.submit_once.assert_not_called()

    def test_bootstrap_positive_rollback_allows_one_later_slot_submission(self):
        plan=self.boot(2);self.svc.io.submit_once=Mock(return_value=applied(plan))
        self.svc.io.recover=Mock(return_value=ControlOutcome('a',Status.NOT_COMMITTED))
        self.assertEqual(self.svc.submit({},plan).native_status,Status.COMMITTED)
        self.svc.io.submit_once.assert_called_once()

    def test_bootstrap_other_subject_cannot_prepare_or_recover(self):
        plan=self.boot();self.svc.verifier.authenticate=Mock(return_value=identity('accounts.google.com:other'))
        with self.assertRaises(ContractError):self.svc.recover({},plan)

    def test_cold_original_job_reconstruction_has_no_write_permission(self):
        plan=self.prepare()
        job=bigquery.QueryJob(plan.job_id,plan.sql,self.svc.io.client,job_config=plan.config())
        job._properties['jobReference']['location']=LOCATION
        self.svc.io.original_job=Mock(return_value=job)
        self.svc._issued.clear()
        recovered=self.svc.original_plan({},workspace_id=WS,command_id=plan.command_id)
        self.assertEqual(recovered,plan);self.assertEqual(self.svc._issued,{})
        self.svc.io.submit_once=Mock()
        with self.assertRaises(ContractError):self.svc.submit({},recovered)
        self.svc.io.submit_once.assert_not_called()

    def test_cold_recovery_missing_or_foreign_native_job_never_remints(self):
        plan=self.prepare();self.svc.io.original_job=Mock(side_effect=NotFound('absent'))
        with self.assertRaises(ContractError):self.svc.original_plan({},workspace_id=WS,command_id=plan.command_id)
        self.svc.io.original_job=Mock(return_value=bigquery.QueryJob('foreign','SELECT 1',self.svc.io.client))
        with self.assertRaises(ContractError):self.svc.original_plan({},workspace_id=WS,command_id=plan.command_id)


class ControlIOTests(unittest.TestCase):
    def test_result_wait_is_bounded_and_defaults_preserved(self):
        c=client();self.addCleanup(c.close)
        self.assertEqual(ControlIO(c).result_timeout_seconds,45)
        self.assertEqual(ControlIO(c,result_timeout_seconds=90).result_timeout_seconds,90)
        for value in (0,91,True,1.5):
            with self.assertRaises(ValueError):ControlIO(c,result_timeout_seconds=value)

    def setUp(self):
        self.client=client();self.addCleanup(self.client.close);self.io=ControlIO(self.client)
        svc=service();self.addCleanup(svc.io.client.close);self.plan=svc.prepare({},kind='MEMBER',workspace_id=WS,
            command_id='io-test',expected_workspace_revision=1,target_subject='accounts.google.com:new',roles=('VIEWER',),status='ACTIVE')

    def job(self):
        job=bigquery.QueryJob(self.plan.job_id,self.plan.sql,self.client,job_config=self.plan.config())
        job._properties['jobReference']['location']=LOCATION
        job._properties['status']={'state':'DONE'}
        job.result=Mock(return_value=[dict(state='APPLIED',candidate=self.plan.candidate_json,audit=self.plan.audit_json)])
        return job

    def test_known_job_is_read_only_and_does_not_authorize_dispatch(self):
        self.client.get_job=Mock(return_value=self.job());self.client.query=Mock()
        self.assertFalse(self.io.submit_once(self.plan).submitted);self.client.query.assert_not_called()

    def test_absence_permits_only_single_retry_disabled_submission(self):
        self.client.get_job=Mock(side_effect=NotFound('absent'));self.client.query=Mock(return_value=self.job())
        result=self.io.submit_once(self.plan);self.assertTrue(result.submitted)
        self.assertEqual(result.native_status,Status.COMMITTED)
        self.assertIsNone(self.client.query.call_args.kwargs['retry']);self.assertIsNone(self.client.query.call_args.kwargs['job_retry'])

    def test_uncertain_lookup_or_submission_never_resubmits(self):
        for side_effect in (ServiceUnavailable('unknown'),NotFound('absent')):
            self.client.get_job=Mock(side_effect=side_effect);self.client.query=Mock(side_effect=ServiceUnavailable('lost'))
            self.assertEqual(self.io.submit_once(self.plan).native_status,Status.UNKNOWN)
            self.assertLessEqual(self.client.query.call_count,1)

    def test_mismatched_native_job_does_not_release_results(self):
        job=self.job();job._properties['configuration']['query']['query']='SELECT 1'
        self.client.get_job=Mock(return_value=job)
        self.assertEqual(self.io.recover(self.plan).native_status,Status.UNKNOWN);job.result.assert_not_called()

    def test_longer_pending_wait_cannot_extend_operation_deadline(self):
        from src.application.operation_budget import operation_scope,OperationSettings
        job=self.job();job._properties['status']['state']='RUNNING'
        values=job.result.return_value
        def finish(**kwargs):
            job._properties['status']['state']='DONE'
            return values
        job.result.side_effect=finish
        io=ControlIO(self.client,result_timeout_seconds=90)
        with operation_scope(OperationSettings(timeout_seconds=1)):
            result=io._outcome(self.plan,job)
        self.assertEqual(result.native_status,Status.COMMITTED)
        self.assertGreater(job.result.call_args.kwargs['timeout'],0)
        self.assertLessEqual(job.result.call_args.kwargs['timeout'],1)

    def test_stop_during_longer_pending_wait_withholds_result(self):
        from src.application.operation_budget import (operation_scope,OperationSettings,CancellationSignal,OperationStopped)
        job=self.job();job._properties['status']['state']='RUNNING';signal=CancellationSignal()
        values=job.result.return_value
        def stopped(**kwargs):
            signal.cancel();job._properties['status']['state']='DONE'
            return values
        job.result.side_effect=stopped
        io=ControlIO(self.client,result_timeout_seconds=90)
        with self.assertRaises(OperationStopped),operation_scope(OperationSettings(),signal):
            io._outcome(self.plan,job)
        job.result.assert_called_once()


if __name__=='__main__':unittest.main()
