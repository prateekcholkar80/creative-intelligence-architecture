"""Existing one-shot admission with opt-in exact registered prompt binding."""
from dataclasses import replace
from datetime import datetime,timezone
import json
from unittest.mock import Mock,patch
import unittest

from src.application.generation_admission import AdmissionSettings,ProtectedGenerationAdmission
from src.application.management_service import digest
from src.application.prompt_evaluation_contract import _hash
from src.application.prompt_runtime_contract import InvocationPromptBinding,canonical
from src.application.prompt_runtime_service import PromptRuntimeService,ResolvedPrompt
from src.application.workflow_contract import ContractError,request_fingerprint
from tests import test_generation_admission as admission_tests
from tests.test_prompt_runtime_contract import capture,profile,behavior


class RegisteredAdmissionTests(unittest.TestCase):
    prepare=admission_tests.AdmissionTests.prepare
    claim_io=admission_tests.AdmissionTests.claim_io
    dispatch=admission_tests.AdmissionTests.dispatch

    def setUp(self):
        admission_tests.AdmissionTests.setUp(self)
        self.runtime=Mock(spec=PromptRuntimeService)
        self.guard.settings=replace(self.guard.settings,require_registered_prompts=True)
        self.guard._prompt_runtime=self.runtime
        patch('src.ingestion.setup_prompt_runtime_tables.inspect',return_value={'status':'VERIFIED'}).start()
        self.addCleanup(patch.stopall)
        inv=self.rows['model_invocations']
        inv['profile']=profile()
        request=self.rows['requests']
        request['model_profiles']=[p if p['stage']!='PLANNER' else profile() for p in request['model_profiles']]
        request['request_sha256']=request_fingerprint(request)
        self.rows['jobs']['request_sha256']=request['request_sha256']
        self.guard.settings=replace(self.guard.settings,profile_sha256=frozenset({digest(profile())}))
        from tests.test_management_service import identity as fixture_identity
        c=capture();identity=fixture_identity()
        ec=replace(c.evaluation.control,workspace_id=inv['workspace_id'])
        c=replace(c,request=replace(c.request,workspace_id=inv['workspace_id'],identity=identity),
            evaluation=replace(c.evaluation,control=ec,control_history=(ec,),
                request=replace(c.evaluation.request,workspace_id=inv['workspace_id'],identity=identity)),
            evaluation_control_anchor=ec)
        from src.application.prompt_runtime_contract import bind_invocation
        b=bind_invocation(c,c.target.head.snapshot,profile(),behavior(),frozenset({_hash(profile())}),
            conversation_id=inv['conversation_id'],request_id=inv['request_id'],
            invocation_id=inv['invocation_id'],checked_at=c.observed_at)
        self.resolved=ResolvedPrompt(b,c)
        self.runtime.resolve.return_value=self.resolved
        self.runtime.revalidate.return_value=self.resolved
        self.runtime.verify_committed_binding.return_value=self.resolved

    def test_atomic_registered_dispatch_calls_bound_adapter_once(self):
        p=self.prepare();self.claim_io(p)
        self.assertEqual(p.config().maximum_bytes_billed,768*1024**2)
        self.assertEqual(int(p.config().job_timeout_ms),120000)
        self.assertEqual(p.config().script_options.statement_byte_budget,512*1024**2)
        self.assertIn('RUNTIME_LIFECYCLE_FENCE_FAILED',p.sql)
        self.assertIn('invocation_prompt_bindings',p.sql)
        self.assertEqual(self.dispatch(p),'response')
        self.guard._call.assert_called_once_with(self.rows['model_invocations']['profile'],self.resolved)
        self.assertEqual(self.runtime.verify_committed_binding.call_count,2)
        with self.assertRaises(ContractError):self.dispatch(p)
        self.guard.recover({},p);self.guard._call.assert_called_once()

    def test_registered_native_budget_tampering_denies_before_submission(self):
        p=self.prepare();self.claim_io(p)
        cfg=p.config();cfg.maximum_bytes_billed=512*1024**2
        altered=replace(p,config_json=json.dumps(cfg.to_api_repr(),sort_keys=True))
        with self.assertRaises(ContractError):self.guard.claim({},altered)
        self.guard.io.submit_once.assert_not_called();self.guard._call.assert_not_called()

    def test_legacy_admission_retains_original_native_budget(self):
        admission_tests.AdmissionTests.setUp(self)
        p=self.prepare()
        self.assertEqual(p.config().maximum_bytes_billed,512*1024**2)
        self.assertEqual(int(p.config().job_timeout_ms),60000)
        self.assertNotIn('runtime_binding',{item.name for item in p.config().query_parameters})

    def test_registered_composition_installs_bounded_pending_wait(self):
        guard=ProtectedGenerationAdmission(client=self.guard.io.client,verifier=self.guard.verifier,
            worker_id='test-worker',settings=self.guard.settings,prompt_runtime=self.runtime)
        self.assertEqual(guard.io.result_timeout_seconds,90)

    def test_registered_runtime_required_and_plain_callback_cannot_fall_through(self):
        with self.assertRaises(ContractError):
            ProtectedGenerationAdmission(client=self.guard.io.client,verifier=self.guard.verifier,
                worker_id='test-worker',settings=self.guard.settings,model_call=Mock())
        self.runtime.resolve.side_effect=ContractError('RUNTIME_PROMPT_NOT_ACTIVE')
        with self.assertRaises(ContractError):self.prepare()
        self.guard._call.assert_not_called()

    def test_preclaim_changed_prompt_denies_without_native_submission(self):
        p=self.prepare();self.claim_io(p)
        self.runtime.revalidate.side_effect=ContractError('PROMPT_RUNTIME_PIN_CHANGED')
        with self.assertRaises(ContractError):self.dispatch(p)
        self.guard.io.submit_once.assert_not_called();self.guard._call.assert_not_called()

    def test_missing_or_corrupt_stored_binding_blocks_billable_call(self):
        p=self.prepare();self.claim_io(p)
        self.runtime.verify_committed_binding.side_effect=ContractError('PROMPT_RUNTIME_STORED_BINDING_MISMATCH')
        with self.assertRaises(ContractError):self.dispatch(p)
        self.guard._call.assert_not_called()
        self.assertEqual(self.rows['model_invocations']['state'],'DISPATCHED')
        with self.assertRaises(ContractError):self.dispatch(p)

    def test_post_call_retirement_withholds_result_without_model_replay(self):
        p=self.prepare();self.claim_io(p)
        self.runtime.verify_committed_binding.side_effect=[self.resolved,ContractError('RUNTIME_PROMPT_NOT_ACTIVE')]
        with self.assertRaises(ContractError):self.dispatch(p)
        self.guard._call.assert_called_once()
        self.guard.recover({},p);self.guard._call.assert_called_once()

    def test_unknown_historical_or_lost_claim_never_invokes_registered_adapter(self):
        from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
        for status,submitted,lost in ((Status.UNKNOWN,True,True),(Status.COMMITTED,False,False),
                                     (Status.COMMITTED,True,True)):
            self.setUp();p=self.prepare();self.claim_io(p,status=status,submitted=submitted,lost=lost)
            with self.subTest(status=status,submitted=submitted,lost=lost),self.assertRaises(ContractError):
                self.dispatch(p)
            self.guard._call.assert_not_called();self.runtime.verify_committed_binding.assert_not_called()
            self.guard.recover({},p);self.guard._call.assert_not_called()

    def test_bound_provider_failure_is_not_replayed_and_usage_remains_unknown(self):
        p=self.prepare();self.claim_io(p)
        self.guard._call.side_effect=TimeoutError('provider response lost')
        with self.assertRaises(TimeoutError):self.dispatch(p)
        with self.assertRaises(ContractError):self.dispatch(p)
        self.guard._call.assert_called_once()
        self.assertIsNone(self.rows['model_invocations']['input_tokens'])
        self.assertIsNone(self.rows['model_invocations']['estimated_cost'])

    def test_runtime_parameter_profile_tampering_denies_before_query(self):
        p=self.prepare();self.claim_io(p)
        params=p.config().query_parameters
        runtime=next(item for item in params if item.name=='runtime_binding')
        row=json.loads(runtime.value);data=json.loads(row['binding_canonical_json'])
        data['profile']['seed']=1;row['binding_canonical_json']=canonical(data)
        runtime.value=json.dumps(row)
        cfg=p.config();cfg.query_parameters=params
        altered=replace(p,config_json=json.dumps(cfg.to_api_repr(),sort_keys=True))
        with self.assertRaises(ContractError):self.guard.claim({},altered)
        self.guard.io.submit_once.assert_not_called();self.guard._call.assert_not_called()

if __name__=='__main__':unittest.main()
