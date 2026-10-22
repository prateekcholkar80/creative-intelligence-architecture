"""Read and canonicalize one explicit Vertex AI Prompt Management version.

Vertex is the authoring/artifact registry.  This adapter never discovers a
version, restores one, or treats the current prompt as authoritative.  It reads
the operator-selected immutable version and converts it to the strict snapshot
that a later BigQuery registrar may persist as a DRAFT candidate.

The installed ``agentplatform`` 2.4.0 public ``prompts.get`` method does not
accept a version ID, and its PromptData decoder incorrectly types ToolConfig as
FunctionCallingConfig. The transport isolates one raw exact-version GET and a
strict wire decoder; it does not remove or default missing tool controls.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import math
import re
from typing import Any, Protocol

import agentplatform
from google.genai import types

from src.application.operation_budget import check_operation, operation_timeout
from src.application.prompt_contract import (
    VERTEX_PROJECT, PromptTask, VertexPromptSnapshot,
)
from src.application.workflow_contract import ContractError, canonical_json_bytes
from src.application.workflow_schema import LOCATION

_PLACEHOLDER = re.compile(r'\{([A-Za-z][A-Za-z0-9_]{0,63})\}')
_EXACT_NAME = re.compile(
    r'projects/([^/]+)/locations/([^/]+)/datasets/([^/]+)/datasetVersions/([^/]+)'
)
_VERTEX_RESOURCE_ID = re.compile(r'[1-9][0-9]{0,31}')
_READ_TIMEOUT_SECONDS = 10
# Fixed project-number alias, verified against Resource Manager on 2026-10-12.
# Vertex returns numeric project resource names even for project-ID requests.
VERTEX_PROJECT_NUMBER = '1042059373096'


def canonical_vertex_name(name: Any) -> str:
    """Normalize only this project's pinned native alias, never arbitrary IDs."""
    if type(name) is not str:
        _fail('VERTEX_PROMPT_VERSION_IDENTITY_MISMATCH')
    prefix = f'projects/{VERTEX_PROJECT_NUMBER}/'
    return (f'projects/{VERTEX_PROJECT}/' + name[len(prefix):]
            if name.startswith(prefix) else name)


class ExactPromptVersionTransport(Protocol):
    """Minimal read-only boundary used by the registration reader."""

    project: str
    location: str

    def get_exact_version(self, *, prompt_id: str, version_id: str,
                          timeout_ms: int) -> Any:
        """Return one exact version, never a current/latest prompt."""


@dataclass(frozen=True, slots=True)
class VertexPromptReadRequest:
    """Operator-selected identity plus application-owned contract versions."""

    prompt_key: str
    task: PromptTask
    prompt_id: str
    version_id: str
    output_schema_version: str
    safety_policy_version: str

    def __post_init__(self):
        if not isinstance(self.task, PromptTask):
            _fail('INVALID_PROMPT_TASK')
        for value in (self.prompt_id, self.version_id):
            if type(value) is not str or _VERTEX_RESOURCE_ID.fullmatch(value) is None:
                _fail('INVALID_VERTEX_PROMPT_RESOURCE')
        for value in (self.prompt_key, self.output_schema_version,
                      self.safety_policy_version):
            if (type(value) is not str or not 1 <= len(value) <= 128
                    or any(character.isspace() or ord(character) < 32
                           or ord(character) == 127 for character in value)):
                _fail('INVALID_IDENTIFIER')


class AgentPlatformPromptVersionTransport:
    """Exact-version transport for the installed Agent Platform SDK.

    The private SDK HTTP boundary is intentionally confined here. It issues one GET for
    ``datasetVersions/{version_id}`` with retries disabled; it never invokes the
    public current-version ``get`` or any list/mutation method.
    """

    def __init__(self, client: Any):
        api_client = getattr(client, '_api_client', None)
        project = getattr(api_client, 'project', None)
        location = getattr(api_client, 'location', None)
        if (project != VERTEX_PROJECT or location != LOCATION
                or not callable(getattr(api_client, 'request', None))):
            raise ValueError('Fixed-project/region exact-version Vertex client required')
        self.project = project
        self.location = location
        self._api = api_client

    def get_exact_version(self, *, prompt_id: str, version_id: str,
                          timeout_ms: int) -> Any:
        if type(timeout_ms) is not int or not 1 <= timeout_ms <= 30_000:
            raise ValueError('Vertex prompt read timeout must be 1..30000 milliseconds')
        for value in (prompt_id, version_id):
            if type(value) is not str or _VERTEX_RESOURCE_ID.fullmatch(value) is None:
                _fail('INVALID_VERTEX_PROMPT_RESOURCE')
        check_operation()
        options = types.HttpOptions(timeout=max(1, math.floor(operation_timeout(
            timeout_ms / 1000) * 1000)),
            retry_options=types.HttpRetryOptions(attempts=1))
        try:
            response = self._api.request('get',
                f'datasets/{prompt_id}/datasetVersions/{version_id}', {}, options)
        finally:
            check_operation()
        body = getattr(response, 'body', None)
        if type(body) is not str or len(body.encode('utf-8')) > 1_048_576:
            _fail('VERTEX_PROMPT_DATA_MISSING')
        try:
            value = json.loads(body)
        except (ValueError, TypeError):
            _fail('VERTEX_PROMPT_DATA_MISSING')
        expected = (f'projects/{self.project}/locations/{self.location}/datasets/'
                    f'{prompt_id}/datasetVersions/{version_id}')
        if type(value) is not dict or canonical_vertex_name(value.get('name')) != expected:
            _fail('VERTEX_PROMPT_VERSION_IDENTITY_MISMATCH')
        metadata = value.get('metadata')
        api = metadata.get('promptApiSchema') if type(metadata) is dict else None
        if (type(api) is not dict or api.get('apiSchemaVersion') != '1.0.0'
                or not set(api).issubset({'apiSchemaVersion', 'multimodalPrompt',
                                         'executions'})):
            _fail('VERTEX_PROMPT_DATA_MISSING')
        multimodal = api.get('multimodalPrompt')
        message = multimodal.get('promptMessage') if type(multimodal) is dict else None
        if type(multimodal) is not dict or set(multimodal) != {'promptMessage'} \
                or type(message) is not dict:
            _fail('VERTEX_PROMPT_DATA_MISSING')
        if not set(message).issubset({'contents', 'generationConfig', 'model',
                'safetySettings', 'systemInstruction', 'toolConfig', 'tools'}):
            _fail('VERTEX_PROMPT_DATA_UNSUPPORTED')
        # Validate the wire wrapper before translating to the SDK's defective
        # PromptData field type. No flattening of arbitrary or extra controls.
        if message.get('toolConfig') != {'functionCallingConfig': {'mode': 'NONE'}}:
            _fail('VERTEX_PROMPT_FUNCTION_CALLING_NOT_DISABLED')
        message = dict(message, toolConfig={'mode': 'NONE'})
        executions = api.get('executions', [])
        if type(executions) is not list or len(executions) > 32:
            _fail('VERTEX_PROMPT_VARIABLES_UNSUPPORTED')
        examples = []
        for execution in executions:
            if type(execution) is not dict or set(execution) != {'arguments'} \
                    or type(execution['arguments']) is not dict:
                _fail('VERTEX_PROMPT_VARIABLES_UNSUPPORTED')
            example = {}
            for key, argument in execution['arguments'].items():
                part_list = argument.get('partList') if type(argument) is dict else None
                parts = part_list.get('parts') if type(part_list) is dict else None
                if (type(argument) is not dict or set(argument) != {'partList'}
                        or type(part_list) is not dict or set(part_list) != {'parts'}
                        or type(parts) is not list or len(parts) != 1
                        or type(parts[0]) is not dict or set(parts[0]) != {'text'}
                        or type(parts[0]['text']) is not str):
                    _fail('VERTEX_PROMPT_VARIABLES_UNSUPPORTED')
                example[key] = types.Part(text=parts[0]['text'])
            examples.append(example)
        try:
            data = agentplatform.types.PromptData.model_validate(message)
        except (TypeError, ValueError):
            _fail('VERTEX_PROMPT_DATA_UNSUPPORTED')
        data.variables = examples
        prompt = agentplatform.types.Prompt(prompt_data=data)
        prompt._dataset_version = agentplatform.types.DatasetVersion(
            name=expected, model_reference=value.get('modelReference'))
        return prompt


def _fail(code: str) -> None:
    raise ContractError(code)


def _enum_value(value: Any, code: str) -> str:
    value = getattr(value, 'value', value)
    if type(value) is not str or not value:
        _fail(code)
    return value


def _single_text(content: Any, *, roles: frozenset[str | None], code: str) -> str:
    if content is None or getattr(content, 'role', None) not in roles:
        _fail(code)
    parts = getattr(content, 'parts', None)
    if type(parts) is not list or len(parts) != 1:
        _fail(code)
    part = parts[0]
    text = getattr(part, 'text', None)
    if type(text) is not str or not text:
        _fail(code)
    # A text part with any additional payload is ambiguous and unsupported.
    populated = part.model_dump(exclude_none=True) if hasattr(part, 'model_dump') else None
    if not isinstance(populated, dict) or set(populated) != {'text'}:
        _fail(code)
    return text


def _canonical_generation(prompt_data: Any) -> str:
    config = getattr(prompt_data, 'generation_config', None)
    if config is None:
        _fail('VERTEX_PROMPT_GENERATION_CONFIG_MISSING')
    supported = {'max_output_tokens', 'response_mime_type', 'response_schema', 'seed',
                 'temperature', 'thinking_config'}
    populated = config.model_dump(exclude_none=True) if hasattr(config, 'model_dump') else None
    if not isinstance(populated, dict) or not set(populated).issubset(supported):
        _fail('VERTEX_PROMPT_GENERATION_CONFIG_UNSUPPORTED')

    thinking = getattr(config, 'thinking_config', None)
    thinking_values = (thinking.model_dump(exclude_none=True)
                       if thinking is not None and hasattr(thinking, 'model_dump') else None)
    if (not isinstance(thinking_values, dict)
            or set(thinking_values) != {'thinking_level'}):
        _fail('VERTEX_PROMPT_THINKING_CONFIG_UNSUPPORTED')

    tools = getattr(prompt_data, 'tools', None)
    tool_config = getattr(prompt_data, 'tool_config', None)
    tool_values = (tool_config.model_dump(exclude_none=True)
                   if tool_config is not None and hasattr(tool_config, 'model_dump') else None)
    if tools not in (None, []) or not isinstance(tool_values, dict) \
            or tool_values != {'mode': 'NONE'}:
        _fail('VERTEX_PROMPT_FUNCTION_CALLING_NOT_DISABLED')

    payload = dict(
        automatic_function_calling_disabled=True,
        max_output_tokens=getattr(config, 'max_output_tokens', None),
        response_mime_type=getattr(config, 'response_mime_type', None),
        seed=getattr(config, 'seed', None),
        temperature=getattr(config, 'temperature', None),
        thinking_level=_enum_value(getattr(thinking, 'thinking_level', None),
                                   'VERTEX_PROMPT_THINKING_CONFIG_UNSUPPORTED'),
    )
    return canonical_json_bytes(payload).decode('utf-8')


def _canonical_safety(prompt_data: Any) -> str:
    settings = getattr(prompt_data, 'safety_settings', None)
    if type(settings) is not list or not settings:
        _fail('VERTEX_PROMPT_SAFETY_SETTINGS_MISSING')
    result = []
    for setting in settings:
        populated = (setting.model_dump(exclude_none=True)
                     if hasattr(setting, 'model_dump') else None)
        if not isinstance(populated, dict) or set(populated) != {'category', 'threshold'}:
            _fail('VERTEX_PROMPT_SAFETY_SETTINGS_UNSUPPORTED')
        result.append(dict(
            category=_enum_value(getattr(setting, 'category', None),
                                 'VERTEX_PROMPT_SAFETY_SETTINGS_UNSUPPORTED'),
            threshold=_enum_value(getattr(setting, 'threshold', None),
                                  'VERTEX_PROMPT_SAFETY_SETTINGS_UNSUPPORTED')))
    result.sort(key=lambda item: item['category'])
    if len({item['category'] for item in result}) != len(result):
        _fail('VERTEX_PROMPT_SAFETY_SETTINGS_UNSUPPORTED')
    return canonical_json_bytes(result).decode('utf-8')


def _canonical_schema(prompt_data: Any) -> str:
    config = getattr(prompt_data, 'generation_config', None)
    schema = getattr(config, 'response_schema', None) if config is not None else None
    if schema is None or not hasattr(schema, 'model_dump'):
        _fail('VERTEX_PROMPT_OUTPUT_SCHEMA_MISSING')
    value = schema.model_dump(mode='json', exclude_none=True)
    if not isinstance(value, dict):
        _fail('VERTEX_PROMPT_OUTPUT_SCHEMA_UNSUPPORTED')
    return canonical_json_bytes(value).decode('utf-8')


def _validate_variable_examples(prompt_data: Any, variables: tuple[str, ...]) -> None:
    """Validate Vertex execution examples without making their values authoritative."""
    examples = getattr(prompt_data, 'variables', None)
    if examples in (None, []):
        return
    if type(examples) is not list or len(examples) > 32:
        _fail('VERTEX_PROMPT_VARIABLES_UNSUPPORTED')
    expected = set(variables)
    for example in examples:
        if type(example) is not dict or set(example) != expected:
            _fail('VERTEX_PROMPT_VARIABLES_UNSUPPORTED')
        for value in example.values():
            populated = (value.model_dump(exclude_none=True)
                         if hasattr(value, 'model_dump') else None)
            if not isinstance(populated, dict) or set(populated) != {'text'}:
                _fail('VERTEX_PROMPT_VARIABLES_UNSUPPORTED')


def _snapshot(response: Any, request: VertexPromptReadRequest,
              *, project: str, location: str) -> VertexPromptSnapshot:
    version = getattr(response, '_dataset_version', None)
    name = getattr(version, 'name', None)
    match = _EXACT_NAME.fullmatch(canonical_vertex_name(name)) if isinstance(name, str) else None
    if match is None or match.groups() != (
            project, location, request.prompt_id, request.version_id):
        _fail('VERTEX_PROMPT_VERSION_IDENTITY_MISMATCH')
    prompt_data = getattr(response, 'prompt_data', None)
    if prompt_data is None:
        _fail('VERTEX_PROMPT_DATA_MISSING')
    system_instruction = _single_text(getattr(prompt_data, 'system_instruction', None),
        roles=frozenset((None, 'system')), code='VERTEX_PROMPT_SYSTEM_INSTRUCTION_UNSUPPORTED')
    contents = getattr(prompt_data, 'contents', None)
    if type(contents) is not list or len(contents) != 1:
        _fail('VERTEX_PROMPT_TEMPLATE_UNSUPPORTED')
    prompt_template = _single_text(contents[0], roles=frozenset((None, 'user')),
                                   code='VERTEX_PROMPT_TEMPLATE_UNSUPPORTED')
    variables = tuple(sorted(set(_PLACEHOLDER.findall(
        system_instruction + '\n' + prompt_template))))
    if not variables:
        _fail('VERTEX_PROMPT_VARIABLES_MISSING')
    _validate_variable_examples(prompt_data, variables)
    model_id = getattr(prompt_data, 'model', None)
    if type(model_id) is not str or not model_id:
        _fail('VERTEX_PROMPT_MODEL_MISSING')
    version_model = getattr(version, 'model_reference', None)
    if type(version_model) is not str or version_model != model_id:
        _fail('VERTEX_PROMPT_MODEL_IDENTITY_MISMATCH')
    return VertexPromptSnapshot(
        prompt_key=request.prompt_key,
        task=request.task,
        vertex_project=project,
        vertex_location=location,
        vertex_prompt_id=request.prompt_id,
        vertex_version_id=request.version_id,
        system_instruction=system_instruction,
        prompt_template=prompt_template,
        required_variables=variables,
        model_id=model_id,
        generation_config_json=_canonical_generation(prompt_data),
        safety_settings_json=_canonical_safety(prompt_data),
        output_schema_json=_canonical_schema(prompt_data),
        output_schema_version=request.output_schema_version,
        safety_policy_version=request.safety_policy_version,
    )


def read_exact_vertex_prompt(transport: ExactPromptVersionTransport,
                             request: VertexPromptReadRequest,
                             *, timeout_seconds: int = _READ_TIMEOUT_SECONDS
                             ) -> VertexPromptSnapshot:
    """Read one explicit immutable version and return its canonical snapshot."""
    if not isinstance(request, VertexPromptReadRequest):
        raise ValueError('A typed exact-version prompt request is required')
    if (getattr(transport, 'project', None) != VERTEX_PROJECT
            or getattr(transport, 'location', None) != LOCATION
            or not callable(getattr(transport, 'get_exact_version', None))):
        raise ValueError('Fixed-project/region exact-version Vertex transport required')
    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 30:
        raise ValueError('Vertex prompt read timeout must be 1..30 seconds')
    check_operation()
    timeout_ms = max(1, math.floor(operation_timeout(timeout_seconds) * 1000))
    try:
        response = transport.get_exact_version(prompt_id=request.prompt_id,
            version_id=request.version_id, timeout_ms=timeout_ms)
    finally:
        check_operation()
    return _snapshot(response, request, project=VERTEX_PROJECT, location=LOCATION)
