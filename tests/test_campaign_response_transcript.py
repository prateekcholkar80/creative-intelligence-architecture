import hashlib,json
from datetime import datetime,timezone
from unittest import TestCase
from src.application.interpreter_outcome import prepare_interpreter_outcome,validate_outcome
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.campaign_response_transcript import SPEC
from src.validation.interpreter_outcome_integration import fixture

class ResponseTranscriptTests(TestCase):
    def inputs(self):
        rows=fixture('response-fixture');text='{"features":[]}'
        return dict(workspace=rows['workspaces'][0],membership=rows['memberships'][0],conversation=rows['conversations'][0],
            request=rows['requests'][0],invocation=rows['model_invocations'][0],response_text=text,
            observation=ModelResultObservation('SUCCEEDED',response_sha256=hashlib.sha256(text.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED'),observed_at=datetime.now(timezone.utc))
    def test_atomic_response_and_exact_replay_use_same_native_slot(self):
        inputs=self.inputs();plan=prepare_interpreter_outcome(**inputs)
        params={p.name:p.value for p in plan.config().query_parameters}
        self.assertEqual(json.loads(params['response_candidate'])['response_text'],inputs['response_text'])
        self.assertEqual(plan.sql.count('COMMIT TRANSACTION;'),1);self.assertIn('RESPONSE_TRANSCRIPT_CONFLICT',plan.sql)
        self.assertEqual(SPEC.table('project').time_partitioning.expiration_ms,30*86400000)
        legacy=prepare_interpreter_outcome(**(inputs|dict(response_text=None)))
        self.assertEqual(plan.job_id,legacy.job_id)
    def test_changed_hash_or_uncertain_response_denied(self):
        inputs=self.inputs()
        for change in (dict(response_text='changed'),dict(observation=ModelResultObservation('UNKNOWN'))):
            with self.assertRaises(ValueError):prepare_interpreter_outcome(**(inputs|change))
    def test_receipt_requires_exact_response_body_scope(self):
        plan=prepare_interpreter_outcome(**self.inputs());params={p.name:p.value for p in plan.config().query_parameters}
        row=dict(state='APPLIED',invocation_row=params['invocation_candidate'],request_row=params['request_candidate'],
            result_audit_row=params['result_audit'],accounting_audit_row=params['accounting_audit'],response_row=params['response_candidate'])
        result=ControlOutcome(plan.job_id,SharingCommitStatus.COMMITTED,(row,),True)
        self.assertIs(validate_outcome(plan,result),result)
        body=json.loads(row['response_row']);body['conversation_id']='other';row['response_row']=json.dumps(body)
        with self.assertRaises(ValueError):validate_outcome(plan,result)
