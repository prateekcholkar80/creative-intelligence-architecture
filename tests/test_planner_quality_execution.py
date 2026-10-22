import json
from dataclasses import replace
from unittest import TestCase
from unittest.mock import patch,Mock
from src.application.campaign_planner_inputs import digest
from src.application.workflow_contract import canonical_json_bytes
from src.application.campaign_acceptance_contract import _hash
from tests import test_campaign_quality_bound_execution as bound
from tests import test_campaign_planner_inputs as inputs
from tests.test_campaign_planner_projection import response
from tests import test_campaign_quality_execution as quality
from src.application import campaign_quality_bound_execution as service

class PlannerQualityTests(TestCase):
    def setUp(self):
        self.f=bound.BoundExecutionTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        raw=inputs.PlannerInputTests();raw.setUp();p=raw.prepare()
        self.raw=response();self.f.content['calls'][0]['response_sha256']=digest(self.raw)
        m=json.loads(p.manifest_json);b=self.f.pins
        m.update(scope={k:self.f.content['authority'][k] for k in ('workspace_id','conversation_id','request_id','principal_id')},
            invocation_id=b['invocation_id'],binding_sha256=b['binding_sha256'],
            selection_sha256=_hash(self.f.content['authority']),catalog_sha256=self.f.content['catalog_sha256'],transcript_sha256=self.f.content['transcript_sha256'])
        self.p=replace(p,manifest_json=canonical_json_bytes(m).decode())
        self.io=patch('src.application.control_io.ControlIO',return_value=Mock()).start()
        self.verify=patch('src.application.campaign_dispatch_inputs.verify_stored').start()
        self.addCleanup(patch.stopall)
    def run_check(self,p=None,copy=None):
        def build(fact):
            c=quality.builder(fact)
            return replace(c,text=copy or self.p.project(self.raw).copy_text,
                sections_json=canonical_json_bytes(dict(self.p.project(self.raw).copy_fields)).decode()),self.raw
        return service.run_authoritative_quality(self.f.r,build,settings=self.f.s,client=self.f.fixture.client,
            runtime_reader=self.f.fixture.runtime,access_reader=self.f.fixture.access,
            identity=self.f.fixture.current.request.identity,clock=lambda:self.f.now,prepared_inputs=p or self.p)
    def test_projection_records_durable_input_and_copy_pins_without_release(self):
        r=self.run_check();context=json.loads(r.context_canonical_json)
        self.assertFalse(r.release_allowed)
        self.assertEqual(context['prepared_inputs_sha256'],self.p.sha256)
        self.assertEqual(self.verify.call_count,2)
    def test_changed_copy_or_selection_and_missing_final_manifest_deny(self):
        with self.assertRaises(ValueError):self.run_check(copy='changed')
        m=json.loads(self.p.manifest_json);m['selection_sha256']='b'*64
        with self.assertRaises(ValueError):self.run_check(p=replace(self.p,manifest_json=canonical_json_bytes(m).decode()))
        self.verify.side_effect=[None,ValueError('revoked')]
        with self.assertRaises(ValueError):self.run_check()
