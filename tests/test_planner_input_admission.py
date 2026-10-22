from unittest.mock import Mock,patch
from unittest import TestCase
from dataclasses import replace
from src.application.campaign_planner_inputs import PlannerInputs
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import canonical_json_bytes
from tests import test_prompt_runtime_admission as fixtures
from tests import test_campaign_planner_inputs as inputs

class InputAdmissionTests(TestCase):
    def setUp(self):
        self.f=fixtures.RegisteredAdmissionTests();self.f.setUp();self.addCleanup(patch.stopall)
        raw=inputs.PlannerInputTests();raw.setUp();p=raw.prepare();m=__import__('json').loads(p.manifest_json)
        b=self.f.resolved.binding
        m.update(scope={k:getattr(b,k) for k in ('workspace_id','conversation_id','request_id','principal_id')},
            binding_sha256=b.binding_sha256,invocation_id=b.invocation_id)
        self.prepared=replace(p,manifest_json=canonical_json_bytes(m).decode())
        patch('src.ingestion.setup_campaign_dispatch_inputs.inspect',return_value={'status':'VERIFIED'}).start()
        self.f.guard._input_builder=Mock(return_value=self.prepared)
        self.f.guard._inputs={}
    def test_atomic_input_insert_and_bound_callback(self):
        plan=self.f.prepare();self.f.claim_io(plan)
        self.assertIn('INPUT_MANIFEST_EXISTS',plan.sql)
        self.assertIn('planner_input_manifest',{p.name for p in plan.config().query_parameters})
        with patch('src.application.campaign_dispatch_inputs.verify_stored') as verify:
            self.assertEqual(self.f.dispatch(plan),'response')
            self.assertEqual(verify.call_count,2)
        self.f.guard._call.assert_called_once_with(self.f.rows['model_invocations']['profile'],self.f.resolved,self.prepared)
    def test_altered_manifest_denies_before_claim(self):
        plan=self.f.prepare();self.f.guard.io.submit_once=Mock();self.f.guard._inputs[plan.job_id]=replace(self.prepared,prompt='changed')
        with self.assertRaises(ValueError):self.f.guard.claim({},plan)
        self.f.guard.io.submit_once.assert_not_called()
    def test_missing_committed_manifest_prevents_provider_call(self):
        plan=self.f.prepare();self.f.claim_io(plan)
        with patch('src.application.campaign_dispatch_inputs.verify_stored',side_effect=ValueError('missing')):
            with self.assertRaises(ValueError):self.f.dispatch(plan)
        self.f.guard._call.assert_not_called()
