import unittest
from types import SimpleNamespace

from google.genai import types

from src.common.gemini_generation import complete_response_text, generation_options


def response(reason='STOP', text='{"ready":"READY"}'):
    return SimpleNamespace(text=text,prompt_feedback=None,
                           candidates=[SimpleNamespace(finish_reason=reason,safety_ratings=[])])


class GeminiGenerationTests(unittest.TestCase):
    def test_native_sdk_stop_and_unspecified_prompt_feedback_are_accepted(self):
        result = types.GenerateContentResponse(
            prompt_feedback=types.GenerateContentResponsePromptFeedback(block_reason='BLOCKED_REASON_UNSPECIFIED'),
            candidates=[types.Candidate(finish_reason='STOP',content=types.Content(
                parts=[types.Part.from_text(text='{"ready":"READY"}')]))])
        self.assertEqual(complete_response_text(result),'{"ready":"READY"}')

    def test_valid_json_does_not_override_incomplete_or_blocked_finish_reasons(self):
        for reason in ('MAX_TOKENS','SAFETY','RECITATION','MALFORMED_FUNCTION_CALL','OTHER',None):
            with self.subTest(reason=reason),self.assertRaises(ValueError):
                complete_response_text(response(reason))

    def test_no_or_multiple_candidates_are_rejected(self):
        for candidates in ([],[response().candidates[0],response().candidates[0]]):
            result=response();result.candidates=candidates
            with self.assertRaises(ValueError):complete_response_text(result)

    def test_prompt_or_candidate_safety_blocks_override_valid_text(self):
        result=response();result.prompt_feedback=SimpleNamespace(block_reason='SAFETY')
        with self.assertRaises(ValueError):complete_response_text(result)
        result=response();result.prompt_feedback=SimpleNamespace(block_reason=None,
            safety_ratings=[SimpleNamespace(blocked=True)])
        with self.assertRaises(ValueError):complete_response_text(result)
        result=response();result.candidates[0].safety_ratings=[SimpleNamespace(blocked=True)]
        with self.assertRaises(ValueError):complete_response_text(result)

    def test_blank_or_missing_text_is_rejected(self):
        for text in ('',' ',None):
            with self.assertRaises(ValueError):complete_response_text(response(text=text))

    def test_unknown_model_or_mixed_controls_are_rejected(self):
        for model,options in (
            ('gemini-unvalidated',{}),('gemini-3.5-flash',dict(thinking_level='LOW',temperature=0.2)),
            ('gemini-2.5-flash',dict(thinking_level='LOW')),('gemini-3.5-flash',dict(thinking_level='INVALID'))):
            with self.subTest(model=model),self.assertRaises(ValueError):
                generation_options(model,seed=42,**options)


if __name__ == '__main__':
    unittest.main()
