"""Fresh authoritative planned-call admission; mocks never prove native commits."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest
import time
from unittest.mock import Mock,patch

from src.application.access_policy import AuthorizationError
from src.application.control_io import ControlOutcome
from src.application.generation_admission import ProtectedGenerationAdmission, AdmissionSettings
from src.application.management_service import digest
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_codec import encode_row, decode_row
from src.application.workflow_contract import ContractError, request_fingerprint
from src.application.operation_budget import worker_operation_scope
from tests.test_invocation_contract import invocation
from tests.test_workflow_contract import TIME
from tests.test_management_service import WS,CONV,ACTOR,identity,workspace,member,conversation,client,service,applied


def records(stage='PLANNER'):
    inv,context=invocation(stage=stage)
    # Shift fixtures into a live bounded window without changing provenance.
    delta=datetime.now(timezone.utc)-TIME-timedelta(seconds=10)
    def shift(row):
        if row is None:return None
        row=deepcopy(row)
        for key,value in row.items():
            if isinstance(value,datetime):row[key]=value+delta
        row.update(workspace_id=WS,conversation_id=CONV,owner_id=ACTOR)
        if 'principal_id' in row:row['principal_id']=ACTOR
        return row
    req=shift(context['request']);req['request_sha256']=request_fingerprint(req)
    job=shift(context['job'])
    if job is not None:job['request_sha256']=req['request_sha256']
    attempt=shift(context['attempt']);inv=shift(inv)
    return dict(workspaces=workspace(),memberships=member(),conversations=conversation(),
        requests=req,jobs=job,job_attempts=attempt,model_invocations=inv)


def captured(rows):
    return dict(observed_at=datetime.now(timezone.utc),**{name:[] if row is None else [encode_row(row,name)] for name,row in rows.items()})


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.rows=records();auth=service().verifier
        self.guard=ProtectedGenerationAdmission(client=client(),verifier=auth,worker_id='test-worker',
            settings=AdmissionSettings(frozenset({digest(self.rows['model_invocations']['profile'])}),True),model_call=Mock(return_value='response'))
        self.addCleanup(self.guard.io.client.close)
        self.guard.io.read=Mock(side_effect=lambda *a,**k:captured(self.rows))
        self.guard.io.preflight=Mock()

    def prepare(self,**changes):
        args=dict(workspace_id=WS,conversation_id=CONV,request_id='test-request',invocation_id='test-invocation',
            job_id='test-job',attempt_id='test-attempt',start_at=datetime.now(timezone.utc)-timedelta(hours=1),
            end_at=datetime.now(timezone.utc)+timedelta(hours=1))
        return self.guard.prepare({},**(args|changes))

    def claim_io(self,plan,*,status=Status.COMMITTED,submitted=True,lost=False):
        def apply(value):
            if status is Status.COMMITTED:
                self.rows['model_invocations']=decode_row(plan.candidate_json,'model_invocations')
                params={p.name:p.value for p in plan.config().query_parameters}
                if 'attempt' in params:self.rows['job_attempts']=decode_row(params['attempt'],'job_attempts')
            return ControlOutcome(plan.job_id,status,() if lost else applied(plan).rows,submitted)
        self.guard.io.submit_once=Mock(side_effect=apply)
        self.guard.io.recover=Mock(return_value=applied(plan,False))

    def dispatch(self,plan):
        with worker_operation_scope(time.monotonic()+20):return self.guard.dispatch({},plan)

    def test_disabled_defaults_and_no_profiles_deny_without_calls(self):
        self.guard.settings=AdmissionSettings()
        with self.assertRaises(ContractError):self.prepare()
        self.guard.verifier.authenticate.assert_not_called();self.guard.io.read.assert_not_called()

    def test_planner_claim_validates_parent_and_keeps_unknown_usage(self):
        plan=self.prepare();row=decode_row(plan.candidate_json,'model_invocations')
        self.assertEqual(row['state'],'DISPATCHED');self.assertEqual(row['usage_status'],'NOT_REPORTED')
        self.assertIsNone(row['estimated_cost']);self.assertEqual(row['cost_status'],'NOT_ESTIMATED')
        self.assertIn('DISPATCH_INTENT_DUPLICATE',plan.sql);self.assertIn('DISPATCH_STAGE_DUPLICATE',plan.sql)
        self.assertIn('ROLLBACK TRANSACTION',plan.sql);self.assertNotIn('DELETE ',plan.sql)

    def test_interpreter_has_no_job_or_attempt_claim(self):
        self.rows=records('INTERPRETER');self.guard.settings=AdmissionSettings(frozenset({digest(self.rows['model_invocations']['profile'])}),True)
        plan=self.prepare(job_id='unused',attempt_id='unused')
        self.assertNotIn('UPDATE `'+self.guard.io.client.project+'.ci_application.job_attempts',plan.sql)
        self.assertIsNone(decode_row(plan.candidate_json,'model_invocations')['job_id'])

    def test_first_fresh_verified_claim_calls_only_installed_profile_once(self):
        plan=self.prepare();self.claim_io(plan)
        self.assertEqual(self.dispatch(plan),'response')
        self.guard._call.assert_called_once_with(self.rows['model_invocations']['profile'])
        with self.assertRaises(ContractError):self.dispatch(plan)
        self.guard._call.assert_called_once()

    def test_unknown_lost_response_and_historical_commit_never_call_model(self):
        for status,submitted,lost in ((Status.UNKNOWN,True,True),(Status.COMMITTED,False,False),(Status.COMMITTED,True,True)):
            self.setUp();plan=self.prepare();self.claim_io(plan,status=status,submitted=submitted,lost=lost)
            with self.subTest(status=status,submitted=submitted,lost=lost),self.assertRaises(ContractError):self.dispatch(plan)
            self.guard._call.assert_not_called()
            self.guard.recover({},plan);self.guard._call.assert_not_called()
            self.guard.io.submit_once.assert_called_once()

    def test_provider_failure_is_not_replayed_or_zero_cost(self):
        plan=self.prepare();self.claim_io(plan);self.guard._call.side_effect=RuntimeError('lost provider response')
        with self.assertRaises(RuntimeError):self.dispatch(plan)
        with self.assertRaises(ContractError):self.dispatch(plan)
        self.assertEqual(self.rows['model_invocations']['state'],'DISPATCHED')
        self.assertIsNone(self.rows['model_invocations']['input_tokens']);self.guard._call.assert_called_once()

    def test_current_non_creator_or_foreign_owner_denies(self):
        for mode in ('role','owner','workspace','conversation'):
            self.setUp()
            if mode=='role':self.rows['memberships']['roles']=['VIEWER']
            elif mode=='owner':self.rows['conversations']['owner_id']='other'
            else:self.rows[mode+'s']['status']='DISABLED' if mode=='workspace' else 'ARCHIVED'
            with self.subTest(mode=mode),self.assertRaises((ContractError,AuthorizationError)):self.prepare()
            self.guard._call.assert_not_called()

    def test_unpublished_profile_wrong_attempt_worker_and_expired_job_deny(self):
        for mode in ('profile','worker','deadline'):
            self.setUp()
            if mode=='profile':self.guard.settings=AdmissionSettings(frozenset(),True)
            elif mode=='worker':self.rows['job_attempts']['worker_id']='another-worker'
            else:self.rows['jobs']['deadline_at']=datetime.now(timezone.utc)-timedelta(seconds=1)
            with self.subTest(mode=mode),self.assertRaises(ContractError):self.prepare()

    def test_missing_or_duplicate_authoritative_records_never_fall_back(self):
        for mode in ('missing','duplicate'):
            row=captured(self.rows)
            row['requests']=[] if mode=='missing' else row['requests']*2
            self.guard.io.read=Mock(return_value=row)
            with self.subTest(mode=mode),self.assertRaises(ContractError):self.prepare()

    def test_closed_or_previously_dispatched_invocation_is_not_admitted(self):
        self.rows['model_invocations']['state']='DISPATCHED'
        with self.assertRaises(ContractError):self.prepare()

    def test_admission_window_must_include_request_and_current_time(self):
        for changes in (dict(start_at=datetime.now(timezone.utc)),dict(end_at=datetime.now(timezone.utc)-timedelta(seconds=1)),
                        dict(start_at=datetime.now(timezone.utc)-timedelta(days=3))):
            with self.subTest(changes=changes),self.assertRaises(ContractError):self.prepare(**changes)

    def test_fresh_revocation_between_commit_and_call_blocks_billable_dispatch(self):
        plan=self.prepare();self.claim_io(plan)
        apply=self.guard.io.submit_once.side_effect
        def revoke(value):
            outcome=apply(value);self.rows['memberships']['status']='REVOKED';return outcome
        self.guard.io.submit_once.side_effect=revoke
        with self.assertRaises(AuthorizationError):self.dispatch(plan)
        self.guard._call.assert_not_called()

    def test_tampered_or_unissued_plan_cannot_claim(self):
        plan=self.prepare();self.guard.io.submit_once=Mock()
        for changed in (replace(plan),replace(plan,sql='SELECT 1'),replace(plan,workspace_id='other')):
            with self.assertRaises(ContractError):self.guard.claim({},changed)
        self.guard.io.submit_once.assert_not_called()

    def test_private_incarnation_changes_receipt_but_not_global_native_slot(self):
        first=self.prepare()
        other=ProtectedGenerationAdmission(client=self.guard.io.client,verifier=self.guard.verifier,
            worker_id='test-worker',settings=self.guard.settings)
        other.io.read=self.guard.io.read
        other.io.preflight=Mock()
        second=other.prepare({},workspace_id=WS,conversation_id=CONV,request_id='test-request',invocation_id='test-invocation',
            job_id='test-job',attempt_id='test-attempt',start_at=datetime.now(timezone.utc)-timedelta(hours=1),end_at=datetime.now(timezone.utc)+timedelta(hours=1))
        self.assertEqual(first.job_id,second.job_id);self.assertNotEqual(first.candidate_json,second.candidate_json)

    def test_billable_call_outside_owned_worker_is_denied_before_claim(self):
        plan=self.prepare();self.guard.io.submit_once=Mock()
        with self.assertRaises(ValueError):self.guard.dispatch({},plan)
        self.guard.io.submit_once.assert_not_called();self.guard._call.assert_not_called()

    def test_read_only_recovery_survives_dispatch_deadline_and_profile_retirement(self):
        plan=self.prepare();self.claim_io(plan);self.guard.claim({},plan)
        self.rows['jobs']['deadline_at']=datetime.now(timezone.utc)-timedelta(seconds=1)
        self.guard.settings=AdmissionSettings(frozenset(),False)
        self.guard.worker_id='recovery-worker'
        result=self.guard.recover({},plan)
        self.assertEqual(result.native_status,Status.COMMITTED);self.guard._call.assert_not_called()

    def test_fork_inherited_service_cannot_reuse_incarnation_for_dispatch(self):
        plan=self.prepare();self.guard.io.submit_once=Mock()
        with patch('src.application.generation_admission.os.getpid',return_value=self.guard._process_id+1):
            with self.assertRaises(ContractError) as caught:self.dispatch(plan)
        self.assertEqual(caught.exception.code,'DISPATCH_PROCESS_CHANGED')
        self.guard.io.submit_once.assert_not_called();self.guard._call.assert_not_called()


if __name__=='__main__':unittest.main()
