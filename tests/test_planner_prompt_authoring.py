"""Offline initial Vertex planner authoring tests; no cloud calls."""
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import json

import agentplatform
from google.api_core.exceptions import InvalidArgument
from google.cloud import aiplatform_v1
from google.longrunning import operations_pb2
from google.protobuf.json_format import MessageToDict, ParseDict

from src.application.operation_budget import OperationSettings, operation_scope
from src.application.planner_prompt_authoring import (
    AgentPlatformPlannerPromptAuthoringTransport,
    PlannerPromptAuthoringError,
    PlannerPromptAuthoringOutcome,
    PlannerPromptAuthoringSettings,
    PromptAuthoringDiscovery,
    PromptVersionIdentity,
    author_initial_planner_prompt,
    build_initial_planner_prompt,
    recover_initial_planner_prompt,
    planner_wire_metadata,
)
from src.application.planner_prompt_parity import PlannerParityStatus
from src.application.prompt_contract import VERTEX_PROJECT
from src.application.vertex_prompt_reader import VertexPromptReadRequest, VERTEX_PROJECT_NUMBER
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION
from tests.test_vertex_prompt_reader import response


CONFIG = dict(model_id='gemini-3.5-flash', location=LOCATION,
              thinking_level='LOW', seed=42, max_output_tokens=4096)
IDENTITY = PromptVersionIdentity('1234567890', '1')
ENABLED = PlannerPromptAuthoringSettings(execution_enabled=True)


class FakeTransport:
    project = VERTEX_PROJECT
    location = LOCATION

    def __init__(self, discoveries=None, create_result=IDENTITY,
                 create_error=None, exact_response=None):
        self.discoveries = list(discoveries or [PromptAuthoringDiscovery(0, ())])
        self.create_result = create_result
        self.create_error = create_error
        self.exact_response = response() if exact_response is None else exact_response
        self.discover_calls = []
        self.create_calls = []
        self.exact_calls = []

    def discover(self, **kwargs):
        self.discover_calls.append(kwargs)
        value = self.discoveries.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    def create_initial_version(self, **kwargs):
        self.create_calls.append(kwargs)
        if self.create_error is not None:
            raise self.create_error
        return self.create_result

    def get_exact_version(self, **kwargs):
        self.exact_calls.append(kwargs)
        item = self.exact_response
        item._dataset_version.name = (
            f'projects/{VERTEX_PROJECT}/locations/{LOCATION}/datasets/'
            f'{kwargs["prompt_id"]}/datasetVersions/{kwargs["version_id"]}')
        item._dataset_version.model_reference = CONFIG['model_id']
        item.prompt_data.model = CONFIG['model_id']
        prompt, _, _ = build_initial_planner_prompt(CONFIG)
        item.prompt_data = prompt.prompt_data
        return item


class PlannerPromptAuthoringTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises((ContractError, PlannerPromptAuthoringError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_default_disabled_stops_before_discovery_or_create(self):
        transport = FakeTransport()
        self.assert_code('VERTEX_PROMPT_AUTHORING_DISABLED',
                         author_initial_planner_prompt, transport, CONFIG)
        self.assertEqual((transport.discover_calls, transport.create_calls), ([], []))

    def test_builds_exact_runtime_prompt_and_deterministic_names(self):
        first, prompt_name, version_name = build_initial_planner_prompt(CONFIG)
        second, second_prompt_name, second_version_name = build_initial_planner_prompt(CONFIG)
        self.assertEqual((prompt_name, version_name),
                         (second_prompt_name, second_version_name))
        self.assertTrue(prompt_name.startswith('ci-campaign-planner-initial-'))
        self.assertTrue(version_name.startswith('initial-'))
        data = first.prompt_data
        self.assertEqual(data.model, CONFIG['model_id'])
        self.assertEqual(data.tool_config.mode, 'NONE')
        self.assertEqual(data.generation_config.thinking_config.thinking_level, 'LOW')
        self.assertEqual(data.system_instruction.parts[0].text,
                         second.prompt_data.system_instruction.parts[0].text)
        self.assertEqual(set(data.variables[0]), {
            'campaign_request', 'creative_options_json', 'evidence_bundle_json'})

    def test_absent_prompt_creates_once_then_exact_reads_and_passes_parity(self):
        transport = FakeTransport()
        result = author_initial_planner_prompt(transport, CONFIG, ENABLED)
        self.assertIs(result.outcome, PlannerPromptAuthoringOutcome.CREATED)
        self.assertEqual(result.identity, IDENTITY)
        self.assertIs(result.parity.status, PlannerParityStatus.PASS)
        self.assertFalse(result.parity.registration_authorized)
        self.assertEqual(len(transport.discover_calls), 1)
        self.assertEqual(len(transport.create_calls), 1)
        self.assertEqual(len(transport.exact_calls), 1)
        self.assertLessEqual(transport.create_calls[0]['timeout_ms'], 20_000)

    def test_existing_exact_name_reuses_without_create(self):
        transport = FakeTransport([PromptAuthoringDiscovery(1, (IDENTITY,))])
        result = author_initial_planner_prompt(transport, CONFIG, ENABLED)
        self.assertIs(result.outcome, PlannerPromptAuthoringOutcome.REUSED)
        self.assertEqual(transport.create_calls, [])
        self.assertEqual(len(transport.exact_calls), 1)

    def test_lost_create_response_recovers_once_without_second_create(self):
        transport = FakeTransport(
            [PromptAuthoringDiscovery(0, ()),
             PromptAuthoringDiscovery(1, (IDENTITY,))],
            create_error=TimeoutError('response lost'))
        result = author_initial_planner_prompt(transport, CONFIG, ENABLED)
        self.assertIs(result.outcome, PlannerPromptAuthoringOutcome.RECOVERED)
        self.assertEqual(len(transport.create_calls), 1)
        self.assertEqual(len(transport.discover_calls), 2)

    def test_unknown_create_without_resource_fails_without_replay(self):
        transport = FakeTransport(
            [PromptAuthoringDiscovery(0, ()), PromptAuthoringDiscovery(0, ())],
            create_error=TimeoutError('unknown'))
        self.assert_code('VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN',
                         author_initial_planner_prompt, transport, CONFIG, ENABLED)
        self.assertEqual(len(transport.create_calls), 1)

    def test_definitive_provider_rejection_is_classified_without_replay(self):
        transport = FakeTransport(
            [PromptAuthoringDiscovery(0, ()), PromptAuthoringDiscovery(0, ())],
            create_error=InvalidArgument('bad prompt'))
        self.assert_code('VERTEX_PROMPT_CREATE_INVALID_ARGUMENT',
                         author_initial_planner_prompt, transport, CONFIG, ENABLED)
        self.assertEqual(len(transport.create_calls), 1)

    def test_read_only_recovery_verifies_existing_and_never_creates(self):
        transport = FakeTransport([PromptAuthoringDiscovery(1, (IDENTITY,))])
        result = recover_initial_planner_prompt(transport, CONFIG)
        self.assertIs(result.outcome, PlannerPromptAuthoringOutcome.RECOVERED)
        self.assertEqual(transport.create_calls, [])
        self.assertEqual(len(transport.exact_calls), 1)

    def test_read_only_recovery_reports_absence_without_create(self):
        transport = FakeTransport([PromptAuthoringDiscovery(0, ())])
        self.assert_code('VERTEX_PROMPT_AUTHORING_NOT_FOUND',
                         recover_initial_planner_prompt,
                         transport, CONFIG)
        self.assertEqual(transport.create_calls, [])

    def test_duplicate_or_partial_discovery_fails_before_create(self):
        cases = [PromptAuthoringDiscovery(2, (IDENTITY,)),
                 PromptAuthoringDiscovery(1, ()),
                 PromptAuthoringDiscovery(1, (IDENTITY,
                     PromptVersionIdentity('1234567890', '2')))]
        for discovery in cases:
            transport = FakeTransport([discovery])
            with self.subTest(discovery=discovery):
                self.assert_code('VERTEX_PROMPT_AUTHORING_CONFLICT',
                    author_initial_planner_prompt, transport, CONFIG, ENABLED)
                self.assertEqual(transport.create_calls, [])

    def test_exact_read_or_parity_failure_never_creates_again(self):
        item = response()
        transport = FakeTransport(exact_response=item)
        original = transport.get_exact_version

        def changed(**kwargs):
            value = original(**kwargs)
            value.prompt_data.generation_config.seed = 43
            return value

        transport.get_exact_version = changed
        self.assert_code('VERTEX_PROMPT_PARITY_FAILED',
                         author_initial_planner_prompt, transport, CONFIG, ENABLED)
        self.assertEqual(len(transport.create_calls), 1)

    def test_invalid_settings_identity_and_provider_shapes_fail_closed(self):
        with self.assertRaises(ContractError):
            PlannerPromptAuthoringSettings(execution_enabled=True,
                                            rpc_timeout_seconds=0)
        wrong = FakeTransport(); wrong.project = 'other'
        with self.assertRaises(ValueError):
            author_initial_planner_prompt(wrong, CONFIG, ENABLED)
        transport = FakeTransport(
            [PromptAuthoringDiscovery(0, ()), PromptAuthoringDiscovery(0, ())])
        transport.create_result = object()
        self.assert_code('VERTEX_PROMPT_CREATE_SDK_REJECTED',
                         author_initial_planner_prompt, transport, CONFIG, ENABLED)

    def test_operation_budget_caps_transport_timeouts(self):
        transport = FakeTransport()
        with operation_scope(OperationSettings(60)):
            author_initial_planner_prompt(transport, CONFIG, ENABLED)
        self.assertLessEqual(transport.create_calls[0]['timeout_seconds'], 60)
        self.assertLessEqual(transport.create_calls[0]['timeout_ms'], 20_000)

    def test_native_adapter_discovers_exact_names_with_bounded_no_retry_reads(self):
        prompt_name = 'ci-campaign-planner-initial-0123456789abcdef'
        version_name = 'initial-0123456789abcdef'
        dataset = aiplatform_v1.Dataset(name=(
            f'projects/{VERTEX_PROJECT_NUMBER}/locations/{LOCATION}/datasets/1234567890'),
            display_name=prompt_name)
        version = aiplatform_v1.DatasetVersion(name=(
            f'{dataset.name}/datasetVersions/1'), display_name=version_name)
        datasets = SimpleNamespace(
            list_datasets=Mock(return_value=iter([dataset])),
            list_dataset_versions=Mock(return_value=iter([version])))
        client = SimpleNamespace(_api_client=SimpleNamespace(
            project=VERTEX_PROJECT, location=LOCATION, request=Mock()))
        adapter = AgentPlatformPlannerPromptAuthoringTransport(client, datasets)
        found = adapter.discover(prompt_display_name=prompt_name,
            version_display_name=version_name, timeout_ms=2500)
        self.assertEqual(found, PromptAuthoringDiscovery(1, (IDENTITY,)))
        for call in (datasets.list_datasets, datasets.list_dataset_versions):
            call.assert_called_once()
            args = call.call_args.kwargs
            self.assertEqual(args['timeout'], 2.5)
            self.assertIsNone(args['retry'])
        self.assertEqual(datasets.list_datasets.call_args.kwargs['request']['page_size'], 2)
        self.assertEqual(datasets.list_dataset_versions.call_args.kwargs['request']['page_size'], 33)
        self.assertNotIn('filter', datasets.list_dataset_versions.call_args.kwargs['request'])
        self.assertEqual(datasets.list_dataset_versions.call_args.kwargs[
            'request']['parent'],
            f'projects/{VERTEX_PROJECT}/locations/{LOCATION}/datasets/1234567890')

    def test_native_version_discovery_filters_locally_and_refuses_truncation_or_foreign_parent(self):
        name = f'projects/{VERTEX_PROJECT_NUMBER}/locations/{LOCATION}/datasets/1234567890'
        dataset = aiplatform_v1.Dataset(name=name, display_name='target')
        def make(version, label='other', parent=name):
            return aiplatform_v1.DatasetVersion(
                name=f'{parent}/datasetVersions/{version}', display_name=label)
        client = SimpleNamespace(_api_client=SimpleNamespace(
            project=VERTEX_PROJECT, location=LOCATION, request=Mock()))
        cases = ([make(2), make(1, 'initial')],
                 [make(i) for i in range(1, 34)],
                 [make(1, 'initial', name.replace('1234567890', '999'))],
                 [make(1, 'initial'), make(1, 'initial')])
        for index, versions in enumerate(cases):
            data = SimpleNamespace(list_datasets=Mock(return_value=iter([dataset])),
                list_dataset_versions=Mock(return_value=iter(versions)))
            adapter = AgentPlatformPlannerPromptAuthoringTransport(client, data)
            with self.subTest(index=index):
                if index == 0:
                    self.assertEqual(adapter.discover(prompt_display_name='target',
                        version_display_name='initial', timeout_ms=2500),
                        PromptAuthoringDiscovery(1, (IDENTITY,)))
                else:
                    self.assert_code('VERTEX_PROMPT_AUTHORING_CONFLICT' if index == 1
                        else 'VERTEX_PROMPT_AUTHORING_DISCOVERY_MISMATCH', adapter.discover,
                        prompt_display_name='target', version_display_name='initial',
                        timeout_ms=2500)

    def fixture_adapter(self):
        prompt, pn, vn = build_initial_planner_prompt(CONFIG)
        dataset = aiplatform_v1.Dataset(name=(
            f'projects/{VERTEX_PROJECT_NUMBER}/locations/{LOCATION}/datasets/1234567890'),
            display_name=pn, metadata=planner_wire_metadata(prompt),
            model_reference=prompt.prompt_data.model)
        version = aiplatform_v1.DatasetVersion(name=f'{dataset.name}/datasetVersions/1',
            display_name=vn, metadata=dataset.metadata,
            model_reference=dataset.model_reference)
        replies = []
        def completed(value, kind):
            name = f'{dataset.name}/operations/{kind}'
            native = operations_pb2.Operation(name=name, done=True)
            native.response.Pack(type(value).pb(value))
            replies.append(native)
            return SimpleNamespace(operation=operations_pb2.Operation(name=name))
        operations = SimpleNamespace(get_operation=Mock(side_effect=lambda *a, **k: replies.pop(0)))
        datasets = SimpleNamespace(
            transport=SimpleNamespace(operations_client=operations),
            create_dataset=Mock(side_effect=lambda **k: completed(dataset, '10')),
            create_dataset_version=Mock(side_effect=lambda **k: completed(version, '11')),
            list_datasets=Mock(return_value=iter(())))
        api = SimpleNamespace(project=VERTEX_PROJECT, location=LOCATION,
            request=Mock(return_value=SimpleNamespace(body=json.dumps(
                MessageToDict(aiplatform_v1.DatasetVersion.pb(version))))))
        return AgentPlatformPlannerPromptAuthoringTransport(
            SimpleNamespace(_api_client=api), datasets), datasets, api, operations

    def test_native_create_and_exact_wire_read_round_trip_all_controls(self):
        adapter, datasets, api, operations = self.fixture_adapter()
        result = author_initial_planner_prompt(adapter, CONFIG, ENABLED)
        self.assertEqual(result.identity, IDENTITY)
        self.assertIs(result.parity.status, PlannerParityStatus.PASS)
        self.assertFalse(result.parity.registration_authorized)
        datasets.create_dataset.assert_called_once()
        datasets.create_dataset_version.assert_called_once()
        metadata = datasets.create_dataset.call_args.kwargs['request']['dataset'].metadata
        self.assertEqual(set(metadata), {'hasPromptVariable', 'promptApiSchema', 'promptType'})
        message = metadata['promptApiSchema']['multimodalPrompt']['promptMessage']
        self.assertEqual(dict(message['toolConfig']['functionCallingConfig']), {'mode': 'NONE'})
        self.assertEqual(len(metadata['promptApiSchema']['executions']), 1)
        for call in (datasets.create_dataset, datasets.create_dataset_version):
            self.assertIsNone(call.call_args.kwargs['retry'])
        self.assertEqual(operations.get_operation.call_count, 2)
        for call in operations.get_operation.call_args_list:
            self.assertIsNone(call.kwargs['retry'])
            self.assertLessEqual(call.kwargs['timeout'], 20)
        api.request.assert_called_once()

    def test_wire_patch_changes_only_wrapper_and_passes_native_parser(self):
        from agentplatform._genai import _prompt_management_utils as utils
        prompt, _, _ = build_initial_planner_prompt(CONFIG)
        original = utils._create_dataset_metadata_from_prompt(
            prompt, variables=prompt.prompt_data.variables).model_dump(
                mode='json', by_alias=True, exclude_none=True)
        corrected = planner_wire_metadata(prompt)
        m = corrected['promptApiSchema']['multimodalPrompt']['promptMessage']
        native = aiplatform_v1.GenerateContentRequest.pb(aiplatform_v1.GenerateContentRequest())
        ParseDict(m, native)
        self.assertEqual(native.tool_config.function_calling_config.mode, 3)
        original['promptApiSchema']['multimodalPrompt']['promptMessage']['toolConfig'] = {
            'functionCallingConfig': {'mode': 'NONE'}}
        self.assertEqual(corrected, original)
        self.assertEqual(prompt.prompt_data.tool_config.mode, 'NONE')

    def test_unsupported_or_enabled_tool_control_stops_before_create(self):
        from google.genai import types
        for config in (None, types.FunctionCallingConfig(mode='AUTO'),
                       types.FunctionCallingConfig(mode='NONE', allowed_function_names=['x'])):
            prompt, pn, vn = build_initial_planner_prompt(CONFIG)
            prompt.prompt_data.tool_config = config
            adapter, datasets, _, _ = self.fixture_adapter()
            with self.subTest(config=config), self.assertRaises(ValueError):
                adapter.create_initial_version(prompt=prompt, prompt_display_name=pn,
                    version_display_name=vn, timeout_seconds=60,
                    max_poll_interval_seconds=5, timeout_ms=2500)
            datasets.create_dataset.assert_not_called()

    def test_operation_timeout_retains_original_name_without_replay(self):
        adapter, _, _, _ = self.fixture_adapter()
        name = f'projects/{VERTEX_PROJECT_NUMBER}/locations/{LOCATION}/datasets/1234567890/operations/10'
        with patch('src.application.planner_prompt_authoring.time.monotonic',
                   side_effect=[0.0, 61.0]):
            with self.assertRaises(PlannerPromptAuthoringError) as caught:
                adapter._wait(SimpleNamespace(operation=SimpleNamespace(name=name)),
                    aiplatform_v1.Dataset, timeout_seconds=60, timeout_ms=2500,
                    max_poll_interval_seconds=5)
        self.assertEqual(caught.exception.operation_name, name)
        self.assertEqual(caught.exception.code, 'VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN')
        transport = FakeTransport([PromptAuthoringDiscovery(0, ()),
            PromptAuthoringDiscovery(0, ())], create_error=caught.exception)
        with self.assertRaises(PlannerPromptAuthoringError) as final:
            author_initial_planner_prompt(transport, CONFIG, ENABLED)
        self.assertEqual(final.exception.operation_name, name)
        self.assertEqual(len(transport.create_calls), 1)

    def test_operation_rejects_foreign_identity_or_wrong_response_type(self):
        adapter, _, _, operations = self.fixture_adapter()
        for name in ('projects/other/locations/asia-south1/operations/10',
                     'projects/1042059373097/locations/asia-south1/operations/10'):
            self.assert_code('VERTEX_PROMPT_AUTHORING_OPERATION_MISMATCH', adapter._wait,
                SimpleNamespace(operation=SimpleNamespace(name=name)),
                aiplatform_v1.Dataset, timeout_seconds=60, timeout_ms=2500,
                max_poll_interval_seconds=5)
        operations.get_operation.assert_not_called()


if __name__ == '__main__':
    unittest.main()
