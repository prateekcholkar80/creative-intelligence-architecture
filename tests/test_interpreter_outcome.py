from copy import deepcopy
from datetime import datetime,timedelta,timezone
import json,time
from unittest import TestCase
from unittest.mock import Mock,patch

from src.application.interpreter_outcome import (prepare_interpreter_outcome,validate_outcome,
    ProtectedInterpreterOutcomeService)
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.workflow_codec import decode_row
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.operation_budget import worker_operation_scope
from tests.test_generation_admission import records
from tests.test_management_service import client,service,ACTOR

class InterpreterOutcomeTests(TestCase):
    def setUp(self):
        self.rows=records('INTERPRETER')
        inv=self.rows['model_invocations'];inv.update(state='DISPATCHED',revision=2,
            dispatch_receipt_id='interpreter-dispatch',worker_id='interpreter-worker')
        self.inputs=dict(workspace=self.rows['workspaces'],membership=self.rows['memberships'],
            conversation=self.rows['conversations'],request=self.rows['requests'],invocation=inv,
            observation=ModelResultObservation('SUCCEEDED',returned_model_id=inv['profile']['model_id'],
                response_sha256='a'*64,finish_reason='STOP',safety_outcome='PASSED',input_tokens=10),
            observed_at=datetime.now(timezone.utc))

    def plan(self,**changes):return prepare_interpreter_outcome(**(self.inputs|changes))

    def outcome(self,plan,submitted=True):
        p={x.name:x.value for x in plan.config().query_parameters}
        return ControlOutcome(plan.job_id,Status.COMMITTED,(dict(state='APPLIED',
            invocation_row=p['invocation_candidate'],request_row=p['request_candidate'],
            result_audit_row=p['result_audit'],accounting_audit_row=p['accounting_audit']),),submitted)

    def test_success_closes_as_needing_clarification_without_inventing_preferences(self):
        plan=self.plan();candidate=decode_row(plan.candidate_json,'model_invocations')
        p={x.name:x.value for x in plan.config().query_parameters}
        request=decode_row(p['request_candidate'],'requests')
        self.assertEqual(candidate['stage'],'INTERPRETER');self.assertIsNone(candidate['job_id'])
        self.assertEqual(request['request_status'],'NEEDS_CLARIFICATION')
        self.assertEqual(candidate['input_tokens'],10);self.assertIsNone(candidate['output_tokens'])
        self.assertEqual(candidate['usage_status'],'PARTIAL')
        self.assertNotIn('ci_application.jobs',plan.sql);self.assertNotIn('DELETE',plan.sql)
        validate_outcome(plan,self.outcome(plan))

    def test_fixed_dispatch_slot_and_terminal_failure_unknown_scope(self):
        first=self.plan()
        unknown=self.plan(observation=ModelResultObservation('UNKNOWN',safe_error_code='CALL_OUTCOME_UNKNOWN'))
        self.assertEqual(first.job_id,unknown.job_id)
        p={x.name:x.value for x in unknown.config().query_parameters}
        self.assertEqual(decode_row(p['request_candidate'],'requests')['request_status'],'ACCEPTED')
        self.assertEqual(decode_row(unknown.candidate_json,'model_invocations')['state'],'UNKNOWN')
        failed=self.plan(observation=ModelResultObservation('FAILED',safe_error_code='RESPONSE_INVALID'))
        p={x.name:x.value for x in failed.config().query_parameters}
        self.assertEqual(decode_row(p['request_candidate'],'requests')['request_status'],'FAILED')

    def test_wrong_stage_owner_or_undispatched_invocation_deny(self):
        for change in (dict(stage='PLANNER'),dict(state='PLANNED'),dict(dispatch_receipt_id=None)):
            with self.subTest(change=change),self.assertRaises(ValueError):self.plan(invocation=self.inputs['invocation']|change)

    def test_result_corruption_never_becomes_receipt(self):
        plan=self.plan();out=self.outcome(plan)
        row=dict(out.rows[0],request_row='{}')
        with self.assertRaises(ValueError):validate_outcome(plan,ControlOutcome(plan.job_id,Status.COMMITTED,(row,),True))
        unknown=ControlOutcome(plan.job_id,Status.UNKNOWN)
        self.assertIs(validate_outcome(plan,unknown),unknown)

    def test_service_disabled_defaults_and_original_recovery_never_resubmit(self):
        c=client();self.addCleanup(c.close)
        verifier=service().verifier
        instance=ProtectedInterpreterOutcomeService(client=c,verifier=verifier,access_reader=Mock(),worker_id='interpreter-worker')
        with self.assertRaises(ValueError):instance.prepare({},**self.inputs)
        instance.enabled=True
        with patch('src.application.interpreter_outcome._access',return_value='current'),\
                patch.object(instance.io,'preflight'),worker_operation_scope(time.monotonic()+30):
            plan=instance.prepare({},**self.inputs)
            with patch.object(instance.io,'submit_once',return_value=ControlOutcome(plan.job_id,Status.UNKNOWN)) as submit:
                self.assertEqual(instance.publish({},plan).native_status,Status.UNKNOWN)
                with self.assertRaises(ValueError):instance.publish({},plan)
                submit.assert_called_once()
            with patch.object(instance.io,'recover',return_value=self.outcome(plan,False)),\
                    patch.object(instance.io,'submit_once') as submit:
                self.assertEqual(instance.recover({},plan).native_status,Status.COMMITTED)
                submit.assert_not_called()
