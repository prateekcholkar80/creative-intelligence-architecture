from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.clarification_prompt_registration import ClarificationPromptRegistrationOperator
from src.application.prompt_import_preview import PromptImportPreviewService
from src.application.prompt_registration_command import PromptRegistrationCommandService
from src.application.vertex_prompt_reader import VertexPromptReadRequest
from src.application.prompt_contract import PromptTask
from tests import test_campaign_clarification_prompts as definition_fixtures
from tests import test_planner_prompt_registration as registration_fixtures
from tests.test_prompt_contract import NOW

class ClarificationRegistrationTests(TestCase):
    def setUp(self):
        self.f=registration_fixtures.PlannerPromptRegistrationTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        d=definition_fixtures.ClarificationPromptTests();d.setUp();self.policy=d.policy;self.definition=definition_fixtures.definitions(d.policy,checked_at=NOW)[0]
        self.base=definition_fixtures.snapshot();self.snapshot=self.definition.snapshot(self.base,vertex_prompt_id='1234567890',vertex_version_id='1')
        self.f.vertex_read.return_value=self.snapshot;self.counter=0
        def policy_reader(request):
            self.counter+=1;return replace(self.policy,native_job_id=f'policy-{self.counter}')
        self.reader=Mock(side_effect=policy_reader)
        self.operator=ClarificationPromptRegistrationOperator(policy_request=self.policy.request,policy_reader=self.reader,
            settings_snapshot=self.base,prompt_key=self.definition.prompt_key,clock=lambda:NOW,
            preview_service=self.f.preview,command_service=self.f.service,identity_adapter=self.f.identity)
        self.request=VertexPromptReadRequest(self.definition.prompt_key,PromptTask.INTERPRETER,'1234567890','1',self.definition.output_schema_version,self.base.safety_policy_version)
    def prepare(self):return self.operator.prepare(self.f.headers,workspace_id=registration_fixtures.WORKSPACE,request=self.request)
    def test_admin_preview_and_exact_hash_import_draft(self):
        command=self.prepare()
        result=self.operator.register(self.f.headers,command,confirmed_sha256=command.approved_preview_sha256)
        self.assertEqual(result.registered.state.head.status.value,'DRAFT');self.assertEqual(len(self.f.writer.calls),1)
    def test_wrong_hash_changed_policy_and_unissued_review_do_not_write(self):
        command=self.prepare()
        with self.assertRaises(ValueError):self.operator.register(self.f.headers,command,confirmed_sha256='a'*64)
        self.reader.side_effect=None;self.reader.return_value=replace(self.policy,record_sha256='b'*64)
        with self.assertRaises(ValueError):self.operator.register(self.f.headers,command,confirmed_sha256=command.approved_preview_sha256)
        self.assertEqual(self.f.writer.calls,[])
    def test_changed_vertex_content_and_revoked_policy_withhold_preview(self):
        self.f.vertex_read.return_value=replace(self.snapshot,system_instruction='changed')
        with self.assertRaises(ValueError):self.prepare()
        self.assertEqual(self.f.writer.calls,[])
        self.reader.side_effect=ValueError('policy revoked')
        with self.assertRaises(ValueError):self.prepare()
        self.assertEqual(self.f.writer.calls,[])
