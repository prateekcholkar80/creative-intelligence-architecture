"""Generation compatibility for the two tested campaign model configurations.

This is not the future user-selectable model catalog. Model selection remains
server configuration, and all generation still requires fresh cloud evidence.
"""
from __future__ import annotations

from google.genai import types

LEGACY_MODEL = 'gemini-2.5-flash'
CAMPAIGN_MODEL = 'gemini-3.5-flash'
THINKING_LEVELS = frozenset({'MINIMAL', 'LOW', 'MEDIUM', 'HIGH'})


def generation_options(model_id: str, *, seed: int, temperature: float | None = None,
                       thinking_level: str | None = None, interpretation: bool = False) -> dict:
    """Keep legacy sampling and Gemini 3 thinking controls mutually exclusive."""
    if model_id == LEGACY_MODEL:
        if thinking_level is not None:
            raise ValueError('Legacy model requires budget-based thinking configuration')
        options = dict(seed=seed, temperature=0 if interpretation else temperature)
        if interpretation:
            options['thinking_config'] = types.ThinkingConfig(thinking_budget=0)
        return options
    if model_id == CAMPAIGN_MODEL:
        level = 'MINIMAL' if interpretation else thinking_level
        if level not in THINKING_LEVELS or temperature is not None:
            raise ValueError('Gemini 3 requires a supported thinking level without sampling overrides')
        return dict(seed=seed, thinking_config=types.ThinkingConfig(thinking_level=level))
    raise ValueError('Campaign model has not been validated for this workflow')


def safety_settings() -> list[types.SafetySetting]:
    return [types.SafetySetting(category=category, threshold='BLOCK_MEDIUM_AND_ABOVE')
            for category in ('HARM_CATEGORY_HATE_SPEECH', 'HARM_CATEGORY_HARASSMENT',
                             'HARM_CATEGORY_SEXUALLY_EXPLICIT', 'HARM_CATEGORY_DANGEROUS_CONTENT')]


def complete_response_text(response: types.GenerateContentResponse) -> str:
    """A valid JSON fragment is insufficient if the service blocked or cut it off."""
    feedback = getattr(response, 'prompt_feedback', None)
    block_reason = getattr(feedback, 'block_reason', None) if feedback is not None else None
    block_reason = getattr(block_reason, 'name', block_reason)
    if block_reason not in (None, 'BLOCKED_REASON_UNSPECIFIED'):
        raise ValueError('Model prompt was blocked')
    if feedback is not None and any(getattr(rating, 'blocked', False) for rating in
                                   (getattr(feedback, 'safety_ratings', None) or [])):
        raise ValueError('Model prompt failed safety checks')
    candidates = getattr(response, 'candidates', None) or []
    if len(candidates) != 1:
        raise ValueError('Expected one complete model response')
    reason = getattr(candidates[0], 'finish_reason', None)
    reason = getattr(reason, 'name', reason)
    if reason != 'STOP':
        raise ValueError('Model response was blocked or incomplete')
    if any(getattr(rating, 'blocked', False) for rating in
           (getattr(candidates[0], 'safety_ratings', None) or [])):
        raise ValueError('Model response failed safety checks')
    text = response.text
    if not isinstance(text, str) or not text.strip():
        raise ValueError('Model returned no complete structured response')
    return text
