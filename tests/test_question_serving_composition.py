from types import SimpleNamespace as NS
from unittest import TestCase
from unittest.mock import Mock, patch
from src.application.question_serving_composition import ServingQuestions
from src.application.interpreter_serving_composition import InterpreterServingSubmission

MODULE='src.application.question_serving_composition.'


class QuestionServingTests(TestCase):
    def pipeline(self, enabled):
        with patch(MODULE+'ProtectedEvidenceChatService'),patch(MODULE+'BigQueryPromptRegistryReader'):
            return ServingQuestions(submission=Mock(),access_reader=Mock(),policy_request=Mock(),
                targets=(),vertex_transport=Mock(),provider_transport=Mock(),enabled=enabled)

    def test_disabled_bootstrap_and_process_never_read_or_dispatch(self):
        pipeline=self.pipeline(False)
        with patch(MODULE+'require_owned_worker'),patch(MODULE+'read_policy') as policy:
            self.assertEqual(pipeline.process({},Mock()).reason,'QUESTION_WORKER_DISABLED')
        policy.assert_not_called();pipeline.evidence._identity.assert_not_called()
        pipeline.provider_transport.assert_not_called()

    def test_unverified_policy_holds_before_registry_and_writer(self):
        pipeline=self.pipeline(True)
        with patch(MODULE+'require_owned_worker'),patch(MODULE+'read_policy',return_value=NS(approval_verified=False)):
            self.assertEqual(pipeline.process({},Mock()).reason,'QUESTION_POLICY_REVIEW_REQUIRED')
        pipeline.evidence._identity.assert_called_once()
        pipeline.registry.read_exact.assert_not_called()
        self.assertIsNone(pipeline._worker)

    def test_access_denial_prevents_policy_read(self):
        pipeline=self.pipeline(True);pipeline.evidence._identity.side_effect=ValueError('denied')
        with patch(MODULE+'require_owned_worker'),patch(MODULE+'read_policy') as policy:
            with self.assertRaises(ValueError):pipeline.process({},Mock())
        policy.assert_not_called()

    def test_held_intent_does_not_start_child(self):
        submission,processor,questions=Mock(),Mock(),Mock()
        receipt=NS(native_status='COMMITTED');submission.submit.return_value=receipt
        processor.process.return_value=NS(state='HELD')
        self.assertIs(InterpreterServingSubmission(submission,processor,questions).submit({},message='x'),receipt)
        questions.process.assert_not_called()

    def test_committed_intent_enters_gated_child_without_text(self):
        submission,processor,questions=Mock(),Mock(),Mock()
        receipt=NS(native_status='COMMITTED');submission.submit.return_value=receipt
        processor.process.return_value=NS(state='COMMITTED')
        result=InterpreterServingSubmission(submission,processor,questions).submit({},message='x')
        self.assertIs(result,receipt);questions.process.assert_called_once_with({},receipt)

    def test_read_methods_use_same_owned_pipeline(self):
        questions=Mock();wrapper=InterpreterServingSubmission(Mock(),Mock(),questions)
        scope=dict(workspace_id='w',conversation_id='c')
        wrapper.evidence_readiness({},**scope);wrapper.question_turn({},**scope)
        questions.check.assert_called_once_with({},**scope)
        questions.question_turn.assert_called_once_with({},**scope)

    def test_bootstrap_installs_disabled_pipeline_without_registry_io(self):
        from contextlib import ExitStack
        from tests.test_interpreter_serving_composition import ServingCompositionTests
        from src.application.interpreter_serving_composition import compose
        submission=Mock();resources=ExitStack()
        from src.ingestion.setup_workflow_tables import PROJECT
        submission.client.project=PROJECT
        with resources,patch('src.application.interpreter_serving_composition.agentplatform.Client'),\
                patch('src.application.interpreter_serving_composition.AgentPlatformPromptVersionTransport'),\
                patch(MODULE+'ProtectedEvidenceChatService'),patch(MODULE+'BigQueryPromptRegistryReader') as registry:
            result=compose(submission,ServingCompositionTests().config(),resources)
        self.assertIsInstance(result.questions,ServingQuestions)
        self.assertFalse(result.questions.enabled)
        registry.return_value.read_exact.assert_not_called()

    def test_quality_configuration_requires_exact_policy_pins(self):
        from src.application.interpreter_serving_composition import validate_config
        from tests.test_interpreter_serving_composition import ServingCompositionTests
        raw=ServingCompositionTests().config()
        valid=dict(policy_id='policy',policy_version='v1',policy_specification_sha256='a'*64,
            policy_header_sha256='b'*64)
        self.assertEqual(validate_config(raw|{'question_quality_policy':valid})['question_quality_policy'],valid)
        for invalid in (valid|{'approved':True},valid|{'policy_header_sha256':'bad'}):
            with self.assertRaises(ValueError):validate_config(raw|{'question_quality_policy':invalid})

    def test_approved_metadata_cannot_bypass_missing_installed_review_guard(self):
        pipeline=self.pipeline(True)
        policy=NS(approval_verified=True,policy_canonical_json='{"status":"APPROVED"}')
        with patch(MODULE+'require_owned_worker'),patch(MODULE+'read_policy',return_value=policy):
            result=pipeline.process({},Mock())
        self.assertEqual(result.reason,'QUESTION_APPROVAL_RUNTIME_REQUIRED')
        pipeline.registry.read_exact.assert_not_called()
        pipeline.provider_transport.assert_not_called()

    def test_supplied_boolean_or_foreign_guard_is_rejected(self):
        with patch(MODULE+'ProtectedEvidenceChatService'),patch(MODULE+'BigQueryPromptRegistryReader'):
            for value in (True,Mock(),{'approved':True}):
                with self.subTest(value=value),self.assertRaisesRegex(ValueError,'GUARD_CONFIGURATION'):
                    ServingQuestions(submission=Mock(),access_reader=Mock(),policy_request=Mock(),targets=(),
                        vertex_transport=Mock(),provider_transport=Mock(),approval_guard=value)
