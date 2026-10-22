from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import clarification_prompt_authoring as author
from src.application.planner_prompt_authoring import PromptAuthoringDiscovery,PromptVersionIdentity
from tests import test_campaign_clarification_prompts as fixture
from tests.test_prompt_contract import snapshot,NOW

class ClarificationAuthoringTests(TestCase):
    def test_grounded_planner_requires_explicit_task_and_exact_planner_readback(self):
        from src.application.campaign_grounded_planner_definition import GroundedPlannerDefinition
        from src.application.prompt_contract import PromptTask
        definition=GroundedPlannerDefinition(self.settings.output_schema_json)
        with self.assertRaises(ValueError):author.author_prompt(self.transport,definition,self.settings)
        self.transport.discover.assert_not_called()
        self.transport.discover.return_value=PromptAuthoringDiscovery(1,(self.identity,))
        exact=definition.snapshot(self.settings,vertex_prompt_id='123',vertex_version_id='1')
        with patch.object(author,'read_exact_vertex_prompt',return_value=exact) as read:
            result=author.author_prompt(self.transport,definition,self.settings,task=PromptTask.PLANNER)
        self.assertIs(result.snapshot.task,PromptTask.PLANNER)
        self.assertIs(read.call_args.args[1].task,PromptTask.PLANNER)
    def setUp(self):
        f=fixture.ClarificationPromptTests();f.setUp()
        self.definition=fixture.definitions(f.policy,checked_at=NOW)[0];self.settings=snapshot()
        self.identity=PromptVersionIdentity('123','1')
        self.exact=self.definition.snapshot(self.settings,vertex_prompt_id='123',vertex_version_id='1')
        self.transport=Mock(project=author.PROJECT,location=author.LOCATION)
    def test_wire_schema_and_deterministic_names_are_bound_to_settings(self):
        a=author.build_prompt(self.definition,self.settings);b=author.build_prompt(self.definition,self.settings)
        self.assertEqual(a[1:],b[1:]);self.assertIn('functionCallingConfig',author.planner_wire_metadata(a[0])['promptApiSchema']['multimodalPrompt']['promptMessage']['toolConfig'])
    def test_discovery_reuses_exact_version_without_create(self):
        self.transport.discover.return_value=PromptAuthoringDiscovery(1,(self.identity,))
        with patch.object(author,'read_exact_vertex_prompt',return_value=self.exact):
            self.assertEqual(author.author_prompt(self.transport,self.definition,self.settings).outcome,'REUSED')
        self.transport.create_initial_version.assert_not_called()
    def test_create_lost_response_recovers_once_without_second_mutation(self):
        self.transport.discover.side_effect=[PromptAuthoringDiscovery(0,()),PromptAuthoringDiscovery(1,(self.identity,))]
        self.transport.create_initial_version.side_effect=TimeoutError('lost response')
        with patch.object(author,'read_exact_vertex_prompt',return_value=self.exact),patch.object(author,'check_operation'):
            self.assertEqual(author.author_prompt(self.transport,self.definition,self.settings,apply=True).outcome,'RECOVERED')
        self.transport.create_initial_version.assert_called_once()
    def test_orphan_or_unknown_outcome_never_retries_create(self):
        self.transport.discover.return_value=PromptAuthoringDiscovery(1,())
        with self.assertRaises(Exception):author.author_prompt(self.transport,self.definition,self.settings,apply=True)
        self.transport.create_initial_version.assert_not_called()
        self.transport.discover.return_value=PromptAuthoringDiscovery(0,())
        self.transport.create_initial_version.side_effect=TimeoutError('lost')
        with patch.object(author,'check_operation'):
            with self.assertRaises(ValueError):author.author_prompt(self.transport,self.definition,self.settings,apply=True)
        self.transport.create_initial_version.assert_called_once()
    def test_default_denies_creation_and_parity_conflicts(self):
        self.transport.discover.return_value=PromptAuthoringDiscovery(0,())
        with self.assertRaises(ValueError):author.author_prompt(self.transport,self.definition,self.settings)
        self.transport.create_initial_version.assert_not_called()
        self.transport.discover.return_value=PromptAuthoringDiscovery(1,(self.identity,))
        with patch.object(author,'read_exact_vertex_prompt',return_value=self.settings):
            with self.assertRaises(ValueError):author.author_prompt(self.transport,self.definition,self.settings)

    def test_nested_question_wire_schema_hashes_without_widening_workflow_limits(self):
        f=fixture.ClarificationPromptTests();f.setUp()
        question=fixture.definitions(f.policy,checked_at=NOW)[1]
        prompt,sha,name,version=author.build_prompt(question,self.settings)
        self.assertEqual(len(sha),64);self.assertIn(question.prompt_key,name)
        metadata=author.planner_wire_metadata(prompt)
        with self.assertRaises(ValueError):author._hash(dict(definition_sha256=question.sha256,metadata=metadata))
        # Existing shallower intent hashes retain byte-for-byte identity.
        intent_prompt,intent_sha,_,_=author.build_prompt(self.definition,self.settings)
        self.assertEqual(intent_sha,author._hash(dict(definition_sha256=self.definition.sha256,
            metadata=author.planner_wire_metadata(intent_prompt))))
