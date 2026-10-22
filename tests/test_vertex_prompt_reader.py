"""Offline exact-version Vertex prompt reader tests; no cloud calls."""
from types import SimpleNamespace
import json
import unittest
from unittest.mock import Mock, patch

import agentplatform
from google.genai import types

from src.application.operation_budget import (
    OperationSettings, OperationStopped, operation_scope,
)
from src.application.prompt_contract import VERTEX_PROJECT, PromptTask
from src.application.vertex_prompt_reader import (
    AgentPlatformPromptVersionTransport, VertexPromptReadRequest,
    VERTEX_PROJECT_NUMBER, read_exact_vertex_prompt,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION

PROMPT_ID = '1234567890'
VERSION_ID = '4'


def request(**changes):
    values = dict(prompt_key='campaign-planner', task=PromptTask.PLANNER,
        prompt_id=PROMPT_ID, version_id=VERSION_ID,
        output_schema_version='campaign-draft-v1',
        safety_policy_version='poc-safety-v1')
    return VertexPromptReadRequest(**(values | changes))


def response():
    generation = types.GenerationConfig(
        max_output_tokens=4096,
        response_mime_type='application/json',
        response_schema=types.Schema(type='OBJECT', properties={
            'concept': types.Schema(type='STRING')}, required=['concept']),
        seed=42,
        thinking_config=types.ThinkingConfig(thinking_level='LOW'))
    prompt = agentplatform.types.Prompt(prompt_data=agentplatform.types.PromptData(
        model='gemini-3.5-flash-001',
        system_instruction=types.Content(role='system', parts=[types.Part(
            text='Use only approved evidence for {campaign_request}.')]),
        contents=[types.Content(role='user', parts=[types.Part(
            text='Evidence:\n{evidence_bundle_json}\nOptions:\n{creative_options_json}')])],
        generation_config=generation,
        tool_config=types.FunctionCallingConfig(mode='NONE'),
        safety_settings=[
            types.SafetySetting(category='HARM_CATEGORY_HARASSMENT',
                                threshold='BLOCK_MEDIUM_AND_ABOVE'),
            types.SafetySetting(category='HARM_CATEGORY_DANGEROUS_CONTENT',
                                threshold='BLOCK_MEDIUM_AND_ABOVE'),
        ]))
    prompt._dataset_version = agentplatform.types.DatasetVersion(name=(
        f'projects/{VERTEX_PROJECT}/locations/{LOCATION}/datasets/{PROMPT_ID}'
        f'/datasetVersions/{VERSION_ID}'), model_reference='gemini-3.5-flash-001')
    return prompt


class FakeTransport:
    project = VERTEX_PROJECT
    location = LOCATION

    def __init__(self, value=None):
        self.value = response() if value is None else value
        self.calls = []

    def get_exact_version(self, **kwargs):
        self.calls.append(kwargs)
        return self.value


class VertexPromptReaderTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_reads_only_selected_version_and_builds_canonical_snapshot(self):
        transport = FakeTransport()
        snapshot = read_exact_vertex_prompt(transport, request())
        self.assertEqual(transport.calls, [dict(prompt_id=PROMPT_ID,
            version_id=VERSION_ID, timeout_ms=10_000)])
        self.assertEqual(snapshot.vertex_prompt_id, PROMPT_ID)
        self.assertEqual(snapshot.vertex_version_id, VERSION_ID)
        self.assertEqual(snapshot.required_variables, ('campaign_request',
            'creative_options_json', 'evidence_bundle_json'))
        self.assertEqual(json.loads(snapshot.generation_config_json), dict(
            automatic_function_calling_disabled=True, max_output_tokens=4096,
            response_mime_type='application/json', seed=42, temperature=None,
            thinking_level='LOW'))
        self.assertEqual([item['category'] for item in
            json.loads(snapshot.safety_settings_json)], [
                'HARM_CATEGORY_DANGEROUS_CONTENT', 'HARM_CATEGORY_HARASSMENT'])
        self.assertEqual(json.loads(snapshot.output_schema_json)['type'], 'OBJECT')
        self.assertEqual(len(snapshot.snapshot_sha256), 64)

    def test_request_and_transport_are_typed_fixed_scope_and_bounded(self):
        for transport, item, timeout in ((FakeTransport(), {}, 10),
                (SimpleNamespace(project='other', location=LOCATION), request(), 10),
                (SimpleNamespace(project=VERTEX_PROJECT, location='us-central1'), request(), 10),
                (FakeTransport(), request(), 0), (FakeTransport(), request(), 31),
                (FakeTransport(), request(), True)):
            with self.subTest(transport=transport, item=item, timeout=timeout), \
                    self.assertRaises(ValueError):
                read_exact_vertex_prompt(transport, item, timeout_seconds=timeout)

    def test_invalid_selected_identity_is_rejected_before_transport(self):
        for changes, code in ((dict(prompt_id='latest'), 'INVALID_VERTEX_PROMPT_RESOURCE'),
                (dict(version_id='0'), 'INVALID_VERTEX_PROMPT_RESOURCE'),
                (dict(version_id='04'), 'INVALID_VERTEX_PROMPT_RESOURCE'),
                (dict(task='PLANNER'), 'INVALID_PROMPT_TASK'),
                (dict(prompt_key='bad key'), 'INVALID_IDENTIFIER')):
            transport = FakeTransport()
            with self.subTest(changes=changes):
                self.assert_code(code, request, **changes)
                self.assertEqual(transport.calls, [])

    def test_operation_budget_caps_rpc_and_rejects_late_success(self):
        ticks = iter((100.0, 100.0, 100.0, 103.0, 105.0, 105.0))
        transport = FakeTransport()
        with patch('src.application.operation_budget.time.monotonic',
                   side_effect=lambda: next(ticks)):
            with self.assertRaises(OperationStopped) as caught:
                with operation_scope(OperationSettings(5)):
                    read_exact_vertex_prompt(transport, request(), timeout_seconds=10)
        self.assertEqual(caught.exception.code, 'OPERATION_DEADLINE_EXCEEDED')
        self.assertEqual(transport.calls[0]['timeout_ms'], 2_000)

    def test_exact_response_resource_identity_is_required(self):
        for name in (None,
                f'projects/other/locations/{LOCATION}/datasets/{PROMPT_ID}/datasetVersions/{VERSION_ID}',
                f'projects/{VERTEX_PROJECT}/locations/us-central1/datasets/{PROMPT_ID}/datasetVersions/{VERSION_ID}',
                f'projects/{VERTEX_PROJECT}/locations/{LOCATION}/datasets/999/datasetVersions/{VERSION_ID}',
                f'projects/{VERTEX_PROJECT}/locations/{LOCATION}/datasets/{PROMPT_ID}/datasetVersions/5',
                f'projects/{VERTEX_PROJECT}/locations/{LOCATION}/datasets/{PROMPT_ID}'):
            item = response()
            item._dataset_version = (None if name is None
                else agentplatform.types.DatasetVersion(name=name))
            with self.subTest(name=name):
                self.assert_code('VERTEX_PROMPT_VERSION_IDENTITY_MISMATCH',
                    read_exact_vertex_prompt, FakeTransport(item), request())

    def test_missing_or_non_text_prompt_content_fails_closed(self):
        cases = []
        item = response(); item.prompt_data = None
        cases.append(('VERTEX_PROMPT_DATA_MISSING', item))
        item = response(); item.prompt_data.system_instruction = None
        cases.append(('VERTEX_PROMPT_SYSTEM_INSTRUCTION_UNSUPPORTED', item))
        item = response(); item.prompt_data.system_instruction.parts.append(types.Part(text='extra'))
        cases.append(('VERTEX_PROMPT_SYSTEM_INSTRUCTION_UNSUPPORTED', item))
        item = response(); item.prompt_data.contents = []
        cases.append(('VERTEX_PROMPT_TEMPLATE_UNSUPPORTED', item))
        item = response(); item.prompt_data.contents[0].parts[0] = types.Part(
            inline_data=types.Blob(data=b'x', mime_type='image/png'))
        cases.append(('VERTEX_PROMPT_TEMPLATE_UNSUPPORTED', item))
        for code, item in cases:
            with self.subTest(code=code):
                self.assert_code(code, read_exact_vertex_prompt,
                                 FakeTransport(item), request())

    def test_variables_and_model_must_be_present_and_contract_valid(self):
        item = response()
        item.prompt_data.system_instruction.parts[0].text = 'No variables.'
        item.prompt_data.contents[0].parts[0].text = 'Still no variables.'
        self.assert_code('VERTEX_PROMPT_VARIABLES_MISSING',
                         read_exact_vertex_prompt, FakeTransport(item), request())
        item = response(); item.prompt_data.model = None
        self.assert_code('VERTEX_PROMPT_MODEL_MISSING',
                         read_exact_vertex_prompt, FakeTransport(item), request())
        item = response(); item._dataset_version.model_reference = 'gemini-other'
        self.assert_code('VERTEX_PROMPT_MODEL_IDENTITY_MISMATCH',
                         read_exact_vertex_prompt, FakeTransport(item), request())
        item = response(); item.prompt_data.contents[0].parts[0].text += '\n{bad-name}'
        # Invalid braces are retained as text but do not declare a valid variable.
        snapshot = read_exact_vertex_prompt(FakeTransport(item), request())
        self.assertNotIn('bad-name', snapshot.required_variables)

    def test_vertex_variable_examples_must_match_template_declarations(self):
        values = {name: types.Part(text='example') for name in (
            'campaign_request', 'creative_options_json', 'evidence_bundle_json')}
        item = response(); item.prompt_data.variables = [values]
        snapshot = read_exact_vertex_prompt(FakeTransport(item), request())
        self.assertEqual(snapshot.required_variables, tuple(sorted(values)))
        item = response(); item.prompt_data.variables = [values | {'extra': types.Part(text='x')}]
        self.assert_code('VERTEX_PROMPT_VARIABLES_UNSUPPORTED',
                         read_exact_vertex_prompt, FakeTransport(item), request())
        item = response(); item.prompt_data.variables = [values | {
            'campaign_request': types.Part(inline_data=types.Blob(
                data=b'x', mime_type='image/png'))}]
        self.assert_code('VERTEX_PROMPT_VARIABLES_UNSUPPORTED',
                         read_exact_vertex_prompt, FakeTransport(item), request())

    def test_generation_settings_require_explicit_safe_supported_shape(self):
        cases = []
        item = response(); item.prompt_data.generation_config = None
        cases.append(('VERTEX_PROMPT_GENERATION_CONFIG_MISSING', item))
        item = response(); item.prompt_data.generation_config.top_p = .8
        cases.append(('VERTEX_PROMPT_GENERATION_CONFIG_UNSUPPORTED', item))
        item = response(); item.prompt_data.generation_config.thinking_config.thinking_budget = 0
        cases.append(('VERTEX_PROMPT_THINKING_CONFIG_UNSUPPORTED', item))
        item = response(); item.prompt_data.tool_config = None
        cases.append(('VERTEX_PROMPT_FUNCTION_CALLING_NOT_DISABLED', item))
        item = response(); item.prompt_data.tool_config.mode = types.FunctionCallingConfigMode.AUTO
        cases.append(('VERTEX_PROMPT_FUNCTION_CALLING_NOT_DISABLED', item))
        item = response(); item.prompt_data.tools = [types.Tool(google_search=types.GoogleSearch())]
        cases.append(('VERTEX_PROMPT_FUNCTION_CALLING_NOT_DISABLED', item))
        for code, item in cases:
            with self.subTest(code=code):
                self.assert_code(code, read_exact_vertex_prompt,
                                 FakeTransport(item), request())

    def test_safety_and_output_schema_must_be_complete_and_lossless(self):
        cases = []
        item = response(); item.prompt_data.safety_settings = []
        cases.append(('VERTEX_PROMPT_SAFETY_SETTINGS_MISSING', item))
        item = response(); item.prompt_data.safety_settings[0].method = types.HarmBlockMethod.SEVERITY
        cases.append(('VERTEX_PROMPT_SAFETY_SETTINGS_UNSUPPORTED', item))
        item = response(); item.prompt_data.safety_settings.append(
            item.prompt_data.safety_settings[0].model_copy())
        cases.append(('VERTEX_PROMPT_SAFETY_SETTINGS_UNSUPPORTED', item))
        item = response(); item.prompt_data.generation_config.response_schema = None
        cases.append(('VERTEX_PROMPT_OUTPUT_SCHEMA_MISSING', item))
        for code, item in cases:
            with self.subTest(code=code):
                self.assert_code(code, read_exact_vertex_prompt,
                                 FakeTransport(item), request())

    def wire_transport(self, value=None):
        from agentplatform._genai import _prompt_management_utils as utils
        prompt = response()
        metadata = utils._create_dataset_metadata_from_prompt(prompt).model_dump(
            mode='json', by_alias=True, exclude_none=True)
        metadata['promptApiSchema']['multimodalPrompt']['promptMessage']['toolConfig'] = {
            'functionCallingConfig': {'mode': 'NONE'}}
        wire = dict(name=prompt._dataset_version.name,
                    modelReference=prompt.prompt_data.model, metadata=metadata)
        if value is not None:
            wire = value(wire)
        call = Mock(return_value=SimpleNamespace(body=json.dumps(wire)))
        client = SimpleNamespace(_api_client=SimpleNamespace(
            project=VERTEX_PROJECT, location=LOCATION, request=call))
        return AgentPlatformPromptVersionTransport(client), call

    def test_sdk_transport_uses_one_raw_exact_get_and_preserves_snapshot(self):
        transport, call = self.wire_transport()
        actual = read_exact_vertex_prompt(transport, request(), timeout_seconds=2)
        expected = read_exact_vertex_prompt(FakeTransport(), request())
        self.assertEqual(actual, expected)
        call.assert_called_once()
        method, path, data, options = call.call_args.args
        self.assertEqual((method, path, data), ('get',
            f'datasets/{PROMPT_ID}/datasetVersions/{VERSION_ID}', {}))
        self.assertEqual(options.timeout, 2000)
        self.assertEqual(options.retry_options.attempts, 1)

    def test_verified_numeric_project_alias_preserves_canonical_hash(self):
        def numeric(wire):
            wire['name'] = wire['name'].replace(VERTEX_PROJECT, VERTEX_PROJECT_NUMBER)
            return wire
        transport, _ = self.wire_transport(numeric)
        self.assertEqual(read_exact_vertex_prompt(transport, request()),
                         read_exact_vertex_prompt(FakeTransport(), request()))
        for wrong in ('1042059373097', 'other', 'us-central1'):
            def changed(wire):
                wire['name'] = wire['name'].replace(
                    LOCATION if wrong == 'us-central1' else VERTEX_PROJECT, wrong)
                return wire
            with self.subTest(wrong=wrong):
                transport, _ = self.wire_transport(changed)
                self.assert_code('VERTEX_PROMPT_VERSION_IDENTITY_MISMATCH',
                                 read_exact_vertex_prompt, transport, request())

    def test_wire_tool_wrapper_must_be_exactly_disabled_without_extra_controls(self):
        for config in (None, {'mode': 'NONE'},
                {'functionCallingConfig': {'mode': 'AUTO'}},
                {'functionCallingConfig': {'mode': 'NONE', 'allowedFunctionNames': ['x']}},
                {'functionCallingConfig': {'mode': 'NONE'}, 'retrievalConfig': {}}):
            def changed(wire):
                wire['metadata']['promptApiSchema']['multimodalPrompt'][
                    'promptMessage']['toolConfig'] = config
                return wire
            transport, _ = self.wire_transport(changed)
            with self.subTest(config=config):
                self.assert_code('VERTEX_PROMPT_FUNCTION_CALLING_NOT_DISABLED',
                                 read_exact_vertex_prompt, transport, request())

    def test_wire_unknown_message_schema_or_execution_shape_is_rejected(self):
        for part in ('message', 'schema', 'execution'):
            def changed(wire):
                api = wire['metadata']['promptApiSchema']
                if part == 'message':
                    api['multimodalPrompt']['promptMessage']['unhandledSetting'] = True
                elif part == 'schema':
                    api['structuredPrompt'] = {}
                else:
                    api['executions'] = [{'arguments': {'x': {'partList': {
                        'parts': [{'text': 'x'}, {'text': 'extra'}]}}}}]
                return wire
            transport, _ = self.wire_transport(changed)
            with self.subTest(part=part):
                self.assert_code({'message': 'VERTEX_PROMPT_DATA_UNSUPPORTED',
                                  'schema': 'VERTEX_PROMPT_DATA_MISSING',
                                  'execution': 'VERTEX_PROMPT_VARIABLES_UNSUPPORTED'}[part],
                                 read_exact_vertex_prompt, transport, request())

    def test_sdk_transport_rejects_wrong_scope_missing_raw_api_and_bad_input(self):
        for project, location, call in (('other', LOCATION, Mock()),
                (VERTEX_PROJECT, 'us-central1', Mock()),
                (VERTEX_PROJECT, LOCATION, None)):
            with self.subTest(project=project, location=location), self.assertRaises(ValueError):
                AgentPlatformPromptVersionTransport(SimpleNamespace(_api_client=
                    SimpleNamespace(project=project, location=location, request=call)))
        transport, call = self.wire_transport()
        for timeout in (0, 30_001, True, 1.5):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                transport.get_exact_version(prompt_id=PROMPT_ID,
                    version_id=VERSION_ID, timeout_ms=timeout)
        self.assert_code('INVALID_VERTEX_PROMPT_RESOURCE', transport.get_exact_version,
                         prompt_id=PROMPT_ID, version_id='latest', timeout_ms=1000)
        call.assert_not_called()


if __name__ == '__main__':
    unittest.main()
