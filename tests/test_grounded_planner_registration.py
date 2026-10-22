from dataclasses import replace
from unittest import TestCase
from unittest.mock import patch
from src.application.grounded_planner_registration import *
from src.application.campaign_grounded_planner_definition import GroundedPlannerDefinition,KEY
from src.application.vertex_prompt_reader import VertexPromptReadRequest
from src.application.prompt_contract import PromptTask
from tests import test_planner_prompt_registration as fixture
from tests.test_access_reader import WORKSPACE
from tests.test_prompt_contract import NOW


class GroundedRegistrationTests(TestCase):
    def setUp(self):
        self.f=fixture.PlannerPromptRegistrationTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.base=self.f.snapshot;self.snapshot=GroundedPlannerDefinition(self.base.output_schema_json).snapshot(self.base,vertex_prompt_id='999',vertex_version_id='1')
        self.target=GroundedPlannerTarget('999','1',self.snapshot.snapshot_sha256,self.base.vertex_prompt_id,self.base.vertex_version_id,self.base.snapshot_sha256)
        self.f.vertex_read.return_value=self.snapshot
        self.request=VertexPromptReadRequest(KEY,PromptTask.PLANNER,'999','1','campaign-draft-v1',self.base.safety_policy_version)
        clock=patch('src.application.grounded_planner_registration.datetime').start();clock.now.return_value=NOW
        self.operator=GroundedPlannerRegistrationOperator(target=self.target,settings_snapshot=self.base,review_store=GroundedReviewStore(),
            preview_service=self.f.preview,command_service=self.f.service,identity_adapter=self.f.identity)
    def test_exact_human_confirmed_import_only_creates_draft(self):
        cmd=self.operator.prepare(self.f.headers,workspace_id=WORKSPACE,request=self.request)
        result=self.operator.register(self.f.headers,cmd,confirmed_sha256=cmd.approved_preview_sha256)
        self.assertEqual(result.registered.state.head.status.value,'DRAFT');self.assertEqual(len(self.f.writer.calls),1)
    def test_wrong_hash_target_settings_or_changed_content_cannot_write(self):
        cmd=self.operator.prepare(self.f.headers,workspace_id=WORKSPACE,request=self.request)
        with self.assertRaises(ValueError):self.operator.register(self.f.headers,cmd,confirmed_sha256='a'*64)
        self.f.vertex_read.return_value=replace(self.snapshot,system_instruction='changed')
        with self.assertRaises(ValueError):self.operator.prepare(self.f.headers,workspace_id=WORKSPACE,request=self.request)
        with self.assertRaises(ValueError):self.operator.inspect(replace(self.request,prompt_id='998'))
        self.assertEqual(self.f.writer.calls,[])
        with self.assertRaises(ValueError):GroundedPlannerRegistrationOperator(target=self.target,
            settings_snapshot=replace(self.base,system_instruction='changed'),review_store=GroundedReviewStore(),
            preview_service=self.f.preview,command_service=self.f.service,identity_adapter=self.f.identity)
