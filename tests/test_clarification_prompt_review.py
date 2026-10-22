from unittest import TestCase
from src.validation.clarification_prompt_review import review
from tests import test_campaign_clarification_prompts as fixtures
from tests.test_prompt_contract import NOW

class PromptReviewTests(TestCase):
    def test_review_has_two_pinned_definitions_without_fake_vertex_ids_or_registration(self):
        f=fixtures.ClarificationPromptTests();f.setUp();packet=review(f.policy,checked_at=NOW)
        self.assertEqual(packet['source_of_truth'],'BIGQUERY')
        self.assertEqual(packet['status'],'DRAFT_DEFINITIONS_NOT_REGISTERED')
        self.assertEqual(len(packet['prompts']),2)
        for p in packet['prompts']:
            self.assertNotIn('vertex_prompt_id',p);self.assertEqual(p['task'],'INTERPRETER')
            self.assertEqual(p['policy_sha256'],packet['policy_sha256'])
        self.assertFalse(packet['serving_enabled']);self.assertEqual(packet['model_calls'],0)
