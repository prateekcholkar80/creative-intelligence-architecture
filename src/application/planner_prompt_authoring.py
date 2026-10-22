"""Default-disabled initial Vertex planner prompt authoring and exact verification.

This adapter creates at most one prompt plus its initial immutable version.  A
deterministic display name supports read-before-create reuse and recovery after an
uncertain SDK response.  It never writes BigQuery, registers lifecycle state,
activates a prompt, or invokes a model.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from enum import Enum
from itertools import islice
import json
from pathlib import Path
import re
import time
from typing import Any, Protocol

import agentplatform
from google.api_core import exceptions as google_exceptions
from google.cloud import aiplatform_v1
from google.genai import types
from google.protobuf.json_format import ParseDict

from src.application.operation_budget import (
    OperationSettings,
    OperationStopped,
    check_operation,
    operation_iter,
    operation_scope,
    operation_timeout,
)
from src.application.planner_prompt_parity import (
    PLANNER_OUTPUT_SCHEMA_VERSION,
    PLANNER_PROMPT_KEY,
    PLANNER_SAFETY_POLICY_VERSION,
    PlannerParityReport,
    PlannerParityStatus,
    compare_planner_prompt,
    derive_planner_runtime_contract,
)
from src.application.prompt_contract import (
    VERTEX_PROJECT,
    PromptTask,
    VertexPromptSnapshot,
)
from src.application.vertex_prompt_reader import (
    AgentPlatformPromptVersionTransport,
    VertexPromptReadRequest,
    read_exact_vertex_prompt,
    canonical_vertex_name,
)
from src.application.workflow_contract import _require, canonical_json_bytes
from src.application.workflow_schema import LOCATION

_RESOURCE_ID = re.compile(r'[1-9][0-9]{0,31}')
_DATASET_NAME = re.compile(
    r'projects/([^/]+)/locations/([^/]+)/datasets/([^/]+)')
_VERSION_NAME = re.compile(
    r'projects/([^/]+)/locations/([^/]+)/datasets/([^/]+)/datasetVersions/([^/]+)')
_DISPLAY_PREFIX = 'ci-campaign-planner-initial'
_MAX_DISCOVERY_MATCHES = 2
_MAX_DISCOVERY_VERSIONS = 32


class PlannerPromptAuthoringError(RuntimeError):
    def __init__(self, code: str, *, operation_name: str | None = None):
        self.code = code
        self.operation_name = operation_name
        super().__init__('Planner prompt authoring is unavailable.')


@dataclass(frozen=True, slots=True)
class PlannerPromptAuthoringSettings:
    execution_enabled: bool = False
    rpc_timeout_seconds: int = 20
    operation_timeout_seconds: int = 300
    max_poll_interval_seconds: int = 5

    def __post_init__(self):
        _require(type(self.execution_enabled) is bool
                 and type(self.rpc_timeout_seconds) is int
                 and 1 <= self.rpc_timeout_seconds <= 30
                 and type(self.operation_timeout_seconds) is int
                 and 30 <= self.operation_timeout_seconds <= 300
                 and type(self.max_poll_interval_seconds) is int
                 and 1 <= self.max_poll_interval_seconds <= 10,
                 'INVALID_PLANNER_PROMPT_AUTHORING_SETTINGS')


@dataclass(frozen=True, slots=True)
class PromptVersionIdentity:
    prompt_id: str
    version_id: str

    def __post_init__(self):
        _require(type(self.prompt_id) is str
                 and _RESOURCE_ID.fullmatch(self.prompt_id) is not None
                 and type(self.version_id) is str
                 and _RESOURCE_ID.fullmatch(self.version_id) is not None,
                 'INVALID_VERTEX_PROMPT_VERSION_IDENTITY')


@dataclass(frozen=True, slots=True)
class PromptAuthoringDiscovery:
    matching_prompts: int
    versions: tuple[PromptVersionIdentity, ...]

    def __post_init__(self):
        _require(type(self.matching_prompts) is int
                 and 0 <= self.matching_prompts <= _MAX_DISCOVERY_MATCHES
                 and type(self.versions) is tuple
                 and len(self.versions) <= _MAX_DISCOVERY_MATCHES
                 and all(isinstance(value, PromptVersionIdentity)
                         for value in self.versions)
                 and ((self.matching_prompts == 0 and not self.versions)
                      or self.matching_prompts > 0),
                 'INVALID_VERTEX_PROMPT_AUTHORING_DISCOVERY')


class PlannerPromptAuthoringOutcome(str, Enum):
    CREATED = 'CREATED'
    REUSED = 'REUSED'
    RECOVERED = 'RECOVERED'


@dataclass(frozen=True, slots=True)
class PlannerPromptAuthoringResult:
    outcome: PlannerPromptAuthoringOutcome
    prompt_display_name: str
    version_display_name: str
    identity: PromptVersionIdentity
    snapshot: VertexPromptSnapshot
    parity: PlannerParityReport

    def __post_init__(self):
        _require(isinstance(self.outcome, PlannerPromptAuthoringOutcome)
                 and type(self.prompt_display_name) is str
                 and type(self.version_display_name) is str
                 and isinstance(self.identity, PromptVersionIdentity)
                 and isinstance(self.snapshot, VertexPromptSnapshot)
                 and isinstance(self.parity, PlannerParityReport)
                 and self.snapshot.vertex_prompt_id == self.identity.prompt_id
                 and self.snapshot.vertex_version_id == self.identity.version_id
                 and self.parity.candidate_snapshot_sha256
                 == self.snapshot.snapshot_sha256
                 and self.parity.status is PlannerParityStatus.PASS
                 and self.parity.registration_authorized is False,
                 'INVALID_PLANNER_PROMPT_AUTHORING_RESULT')


class PlannerPromptAuthoringTransport(Protocol):
    project: str
    location: str

    def discover(self, *, prompt_display_name: str,
                 version_display_name: str,
                 timeout_ms: int) -> PromptAuthoringDiscovery: ...

    def create_initial_version(self, *, prompt: Any,
                               prompt_display_name: str,
                               version_display_name: str,
                               timeout_seconds: int,
                               max_poll_interval_seconds: int,
                               timeout_ms: int) -> PromptVersionIdentity: ...

    def get_exact_version(self, *, prompt_id: str, version_id: str,
                          timeout_ms: int) -> Any: ...


def _http_options(timeout_ms: int) -> types.HttpOptions:
    if type(timeout_ms) is not int or not 1 <= timeout_ms <= 30_000:
        raise ValueError('Vertex prompt timeout must be 1..30000 milliseconds')
    return types.HttpOptions(timeout=timeout_ms,
        retry_options=types.HttpRetryOptions(attempts=1))


def _dataset_id(name: Any) -> str:
    match = _DATASET_NAME.fullmatch(canonical_vertex_name(name)) if isinstance(name, str) else None
    if (match is None or match.group(1) != VERTEX_PROJECT
            or match.group(2) != LOCATION):
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_AUTHORING_IDENTITY_MISMATCH')
    return PromptVersionIdentity(match.group(3), '1').prompt_id


def _version_identity(name: Any) -> PromptVersionIdentity:
    match = _VERSION_NAME.fullmatch(canonical_vertex_name(name)) if isinstance(name, str) else None
    if (match is None or match.group(1) != VERTEX_PROJECT
            or match.group(2) != LOCATION):
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_AUTHORING_IDENTITY_MISMATCH')
    return PromptVersionIdentity(match.group(3), match.group(4))


def planner_wire_metadata(prompt: Any) -> dict:
    """Correct only the known SDK ToolConfig wrapper; preserve every other field."""
    from agentplatform._genai import _prompt_management_utils as utils
    utils._raise_for_invalid_prompt(prompt)
    data = getattr(prompt, 'prompt_data', None)
    if data is None or data.tool_config is None:
        raise ValueError('Explicit planner prompt and disabled tool control required')
    if data.tool_config.model_dump(mode='json', exclude_none=True) != {'mode': 'NONE'} \
            or data.tools not in (None, []):
        raise ValueError('Planner function calling must be disabled without tools')
    metadata = utils._create_dataset_metadata_from_prompt(
        prompt, variables=data.variables if data.variables else None).model_dump(
            mode='json', by_alias=True, exclude_none=True)
    message = metadata['promptApiSchema']['multimodalPrompt']['promptMessage']
    message['toolConfig'] = {'functionCallingConfig': {'mode': 'NONE'}}
    # Native parsing catches incorrect nesting or unsupported fields before RPC.
    ParseDict(message, aiplatform_v1.GenerateContentRequest.pb(
        aiplatform_v1.GenerateContentRequest()))
    return metadata


class AgentPlatformPlannerPromptAuthoringTransport:
    """Native discovery/writes/polling plus one raw SDK exact-version GET."""

    def __init__(self, client: Any, dataset_client: Any | None = None):
        api = getattr(client, '_api_client', None)
        if (getattr(api, 'project', None) != VERTEX_PROJECT
                or getattr(api, 'location', None) != LOCATION
                or not callable(getattr(api, 'request', None))):
            raise ValueError('Fixed-project/region Vertex authoring client required')
        self.project = api.project
        self.location = api.location
        self._exact = AgentPlatformPromptVersionTransport(client)
        self._datasets = (aiplatform_v1.DatasetServiceClient(client_options={
            'api_endpoint': f'{LOCATION}-aiplatform.googleapis.com'})
            if dataset_client is None else dataset_client)
        host = getattr(getattr(self._datasets, 'transport', None), '_host', None)
        if (dataset_client is None
                and host not in (f'{LOCATION}-aiplatform.googleapis.com',
                                 f'{LOCATION}-aiplatform.googleapis.com:443')):
            raise ValueError('Regional Vertex Dataset client required')

    def discover(self, *, prompt_display_name: str,
                 version_display_name: str,
                 timeout_ms: int) -> PromptAuthoringDiscovery:
        _http_options(timeout_ms)
        datasets = list(islice(operation_iter(
            self._datasets.list_datasets(request={
                'parent': f'projects/{self.project}/locations/{self.location}',
                'page_size': _MAX_DISCOVERY_MATCHES,
                'filter': f'display_name="{prompt_display_name}"'},
                retry=None, timeout=operation_timeout(timeout_ms / 1000))),
            _MAX_DISCOVERY_MATCHES))
        if any(getattr(item, 'display_name', None) != prompt_display_name
               for item in datasets):
            raise PlannerPromptAuthoringError(
                'VERTEX_PROMPT_AUTHORING_DISCOVERY_MISMATCH')
        identities = []
        for dataset in datasets:
            prompt_id = _dataset_id(getattr(dataset, 'name', None))
            versions = list(islice(operation_iter(
                self._datasets.list_dataset_versions(request={
                    'parent': f'projects/{self.project}/locations/{self.location}/datasets/{prompt_id}',
                    # Native version listing rejects display_name filters.
                    # Scan a bounded complete list and compare names locally.
                    'page_size': _MAX_DISCOVERY_VERSIONS + 1},
                    retry=None, timeout=operation_timeout(timeout_ms / 1000))),
                _MAX_DISCOVERY_VERSIONS + 1))
            if len(versions) > _MAX_DISCOVERY_VERSIONS:
                raise PlannerPromptAuthoringError('VERTEX_PROMPT_AUTHORING_CONFLICT')
            pins = [_version_identity(getattr(item, 'name', None)) for item in versions]
            if len(set(pins)) != len(pins) or any(pin.prompt_id != prompt_id for pin in pins):
                raise PlannerPromptAuthoringError(
                    'VERTEX_PROMPT_AUTHORING_DISCOVERY_MISMATCH')
            identities.extend(pin for pin, item in zip(pins, versions)
                              if getattr(item, 'display_name', None) == version_display_name)
        return PromptAuthoringDiscovery(len(datasets), tuple(identities))

    def _wait(self, operation: Any, result_type: Any, *, timeout_seconds: int,
              timeout_ms: int, max_poll_interval_seconds: int) -> Any:
        """Bound every original-operation GET; never replay a create on timeout."""
        name = getattr(getattr(operation, 'operation', None), 'name', None)
        if (type(name) is not str or re.fullmatch(
                rf'projects/{re.escape(self.project)}/locations/{self.location}/'
                r'(?:datasets/[1-9][0-9]*/)?operations/[1-9][0-9]*',
                canonical_vertex_name(name)) is None):
            raise PlannerPromptAuthoringError('VERTEX_PROMPT_AUTHORING_OPERATION_MISMATCH')
        deadline = time.monotonic() + operation_timeout(timeout_seconds)
        while True:
            check_operation()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PlannerPromptAuthoringError('VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN',
                                                  operation_name=name)
            try:
                native = self._datasets.transport.operations_client.get_operation(
                    name, retry=None,
                    timeout=operation_timeout(min(timeout_ms / 1000, remaining)))
            except OperationStopped:
                raise
            except Exception:
                raise PlannerPromptAuthoringError('VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN',
                                                  operation_name=name) from None
            check_operation()
            if native.name != name:
                raise PlannerPromptAuthoringError('VERTEX_PROMPT_AUTHORING_OPERATION_MISMATCH',
                                                  operation_name=name)
            if time.monotonic() >= deadline:
                raise PlannerPromptAuthoringError('VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN',
                                                  operation_name=name)
            if native.done:
                if native.HasField('error'):
                    raise google_exceptions.from_grpc_status(native.error.code,
                        native.error.message, errors=(native.error,))
                result = result_type.pb(result_type())
                if not native.HasField('response') or not native.response.Unpack(result):
                    raise PlannerPromptAuthoringError('VERTEX_PROMPT_AUTHORING_OPERATION_MISMATCH',
                                                      operation_name=name)
                return result_type.wrap(result)
            time.sleep(min(max_poll_interval_seconds,
                           operation_timeout(max(0.001, deadline - time.monotonic()))))

    def create_initial_version(self, *, prompt: Any,
                               prompt_display_name: str,
                               version_display_name: str,
                               timeout_seconds: int,
                               max_poll_interval_seconds: int,
                               timeout_ms: int) -> PromptVersionIdentity:
        from agentplatform._genai import _prompt_management_utils
        data = getattr(prompt, 'prompt_data', None)
        if data is None:
            raise ValueError('Prompt data is required')
        metadata = planner_wire_metadata(prompt)
        parent = f'projects/{VERTEX_PROJECT}/locations/{LOCATION}'
        dataset = aiplatform_v1.Dataset(
            display_name=prompt_display_name,
            metadata_schema_uri=_prompt_management_utils.PROMPT_SCHEMA_URI,
            metadata=metadata,
            model_reference=data.model)
        create_dataset = self._datasets.create_dataset(
            request={'parent': parent, 'dataset': dataset},
            retry=None, timeout=operation_timeout(timeout_ms / 1000))
        dataset = self._wait(create_dataset, aiplatform_v1.Dataset,
            timeout_seconds=timeout_seconds, timeout_ms=timeout_ms,
            max_poll_interval_seconds=max_poll_interval_seconds)
        check_operation()
        prompt_id = _dataset_id(getattr(dataset, 'name', None))
        if (getattr(dataset, 'display_name', None) != prompt_display_name
                or getattr(dataset, 'model_reference', None) != data.model
                or _dataset_id(getattr(dataset, 'name', None)) != prompt_id):
            raise PlannerPromptAuthoringError(
                'VERTEX_PROMPT_AUTHORING_CREATE_MISMATCH')
        create_version = self._datasets.create_dataset_version(
            request={'parent': dataset.name, 'dataset_version':
                     aiplatform_v1.DatasetVersion(
                         display_name=version_display_name)},
            retry=None, timeout=operation_timeout(timeout_ms / 1000))
        version = self._wait(create_version, aiplatform_v1.DatasetVersion,
            timeout_seconds=timeout_seconds, timeout_ms=timeout_ms,
            max_poll_interval_seconds=max_poll_interval_seconds)
        check_operation()
        if getattr(version, 'display_name', None) != version_display_name:
            raise PlannerPromptAuthoringError(
                'VERTEX_PROMPT_AUTHORING_CREATE_MISMATCH')
        identity = _version_identity(getattr(version, 'name', None))
        if identity.prompt_id != prompt_id:
            raise PlannerPromptAuthoringError(
                'VERTEX_PROMPT_AUTHORING_CREATE_MISMATCH')
        return identity

    def get_exact_version(self, *, prompt_id: str, version_id: str,
                          timeout_ms: int) -> Any:
        return self._exact.get_exact_version(prompt_id=prompt_id,
            version_id=version_id, timeout_ms=timeout_ms)


def _display_names(contract_hash: str) -> tuple[str, str]:
    suffix = contract_hash[:16]
    return f'{_DISPLAY_PREFIX}-{suffix}', f'initial-{suffix}'


def build_initial_planner_prompt(config: dict) -> tuple[Any, str, str]:
    """Build one SDK prompt from the current runtime contract without I/O."""
    contract = derive_planner_runtime_contract(config)
    generation = json.loads(contract.generation_config_json)
    schema = json.loads(contract.output_schema_shape_json)
    safety = json.loads(contract.safety_settings_json)
    request_options = json.loads(contract.allowed_request_options_json)
    variable_example = {
        'campaign_request': types.Part(text=canonical_json_bytes({
            'canonical_sku': request_options['canonical_sku'][0],
            'objective': request_options['objective'][0],
            'audience': 'POC example audience',
            'channel': request_options['channel'][0],
            'visual_reference': request_options['visual_reference'][0],
        }).decode('utf-8')),
        'creative_options_json': types.Part(text=canonical_json_bytes({
            'source': 'FRESH_CODE_VALIDATED_RUNTIME_OPTIONS',
        }).decode('utf-8')),
        'evidence_bundle_json': types.Part(text=canonical_json_bytes({
            'source_of_truth': 'BIGQUERY', 'example_only': True,
        }).decode('utf-8')),
    }
    prompt = agentplatform.types.Prompt(prompt_data=agentplatform.types.PromptData(
        model=contract.model_id,
        system_instruction=types.Content(role='system', parts=[
            types.Part(text=contract.system_instruction)]),
        contents=[types.Content(role='user', parts=[
            types.Part(text=contract.prompt_template)])],
        generation_config=types.GenerationConfig(
            max_output_tokens=generation['max_output_tokens'],
            response_mime_type=generation['response_mime_type'],
            response_schema=types.Schema(**schema),
            seed=generation['seed'],
            temperature=generation['temperature'],
            thinking_config=types.ThinkingConfig(
                thinking_level=generation['thinking_level'])),
        tool_config=types.FunctionCallingConfig(mode='NONE'),
        safety_settings=[types.SafetySetting(**item) for item in safety],
        variables=[variable_example],
    ))
    return (prompt, *_display_names(contract.contract_sha256))


def _single_identity(discovery: PromptAuthoringDiscovery) -> PromptVersionIdentity | None:
    if discovery.matching_prompts == 0:
        return None
    if discovery.matching_prompts != 1 or len(discovery.versions) != 1:
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_AUTHORING_CONFLICT')
    return discovery.versions[0]


def _create_error_code(error: Exception) -> str:
    if isinstance(error, PlannerPromptAuthoringError) and error.operation_name is not None:
        return 'VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN'
    if (isinstance(error, PlannerPromptAuthoringError)
            and error.code == 'VERTEX_PROMPT_AUTHORING_CREATE_MISMATCH'):
        return 'VERTEX_PROMPT_CREATE_SDK_REJECTED'
    if isinstance(error, google_exceptions.PermissionDenied):
        return 'VERTEX_PROMPT_CREATE_PERMISSION_DENIED'
    if isinstance(error, google_exceptions.InvalidArgument):
        return 'VERTEX_PROMPT_CREATE_INVALID_ARGUMENT'
    if isinstance(error, google_exceptions.FailedPrecondition):
        return 'VERTEX_PROMPT_CREATE_FAILED_PRECONDITION'
    if isinstance(error, google_exceptions.ResourceExhausted):
        return 'VERTEX_PROMPT_CREATE_RESOURCE_EXHAUSTED'
    if isinstance(error, ValueError):
        return 'VERTEX_PROMPT_CREATE_SDK_REJECTED'
    return 'VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN'


def _verify_identity(transport: PlannerPromptAuthoringTransport,
                     identity: PromptVersionIdentity, config: dict,
                     selected: PlannerPromptAuthoringSettings,
                     outcome: PlannerPromptAuthoringOutcome,
                     prompt_name: str,
                     version_name: str) -> PlannerPromptAuthoringResult:
    request = VertexPromptReadRequest(
        prompt_key=PLANNER_PROMPT_KEY,
        task=PromptTask.PLANNER,
        prompt_id=identity.prompt_id,
        version_id=identity.version_id,
        output_schema_version=PLANNER_OUTPUT_SCHEMA_VERSION,
        safety_policy_version=PLANNER_SAFETY_POLICY_VERSION)
    try:
        snapshot = read_exact_vertex_prompt(
            transport, request, timeout_seconds=selected.rpc_timeout_seconds)
        parity = compare_planner_prompt(snapshot, config)
    except OperationStopped:
        raise
    except Exception:
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_EXACT_VERIFICATION_FAILED') from None
    if parity.status is not PlannerParityStatus.PASS:
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_PARITY_FAILED')
    return PlannerPromptAuthoringResult(
        outcome, prompt_name, version_name, identity, snapshot, parity)


def recover_initial_planner_prompt(
        transport: PlannerPromptAuthoringTransport,
        config: dict,
        settings: PlannerPromptAuthoringSettings | None = None,
        ) -> PlannerPromptAuthoringResult:
    """Read-only deterministic-name recovery; never creates a prompt."""
    selected = PlannerPromptAuthoringSettings() if settings is None else settings
    _require(isinstance(selected, PlannerPromptAuthoringSettings),
             'INVALID_PLANNER_PROMPT_AUTHORING_SETTINGS')
    if (getattr(transport, 'project', None) != VERTEX_PROJECT
            or getattr(transport, 'location', None) != LOCATION):
        raise ValueError('Fixed-project/region Vertex authoring transport required')
    _, prompt_name, version_name = build_initial_planner_prompt(config)
    timeout_ms = max(1, int(operation_timeout(
        selected.rpc_timeout_seconds) * 1000))
    try:
        discovery = transport.discover(prompt_display_name=prompt_name,
            version_display_name=version_name, timeout_ms=timeout_ms)
        identity = _single_identity(discovery)
    except (OperationStopped, PlannerPromptAuthoringError):
        raise
    except Exception:
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_AUTHORING_RECOVERY_UNAVAILABLE') from None
    if identity is None:
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_AUTHORING_NOT_FOUND')
    return _verify_identity(transport, identity, config, selected,
        PlannerPromptAuthoringOutcome.RECOVERED, prompt_name, version_name)


def author_initial_planner_prompt(
        transport: PlannerPromptAuthoringTransport,
        config: dict,
        settings: PlannerPromptAuthoringSettings | None = None,
        ) -> PlannerPromptAuthoringResult:
    """Reuse or create once, then exact-read and require runtime parity."""
    selected = PlannerPromptAuthoringSettings() if settings is None else settings
    _require(isinstance(selected, PlannerPromptAuthoringSettings),
             'INVALID_PLANNER_PROMPT_AUTHORING_SETTINGS')
    if not selected.execution_enabled:
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_AUTHORING_DISABLED')
    if (getattr(transport, 'project', None) != VERTEX_PROJECT
            or getattr(transport, 'location', None) != LOCATION):
        raise ValueError('Fixed-project/region Vertex authoring transport required')
    prompt, prompt_name, version_name = build_initial_planner_prompt(config)

    def discover() -> PromptAuthoringDiscovery:
        check_operation()
        timeout_ms = max(1, int(operation_timeout(
            selected.rpc_timeout_seconds) * 1000))
        try:
            result = transport.discover(prompt_display_name=prompt_name,
                version_display_name=version_name, timeout_ms=timeout_ms)
        finally:
            check_operation()
        if not isinstance(result, PromptAuthoringDiscovery):
            raise PlannerPromptAuthoringError(
                'VERTEX_PROMPT_AUTHORING_DISCOVERY_MISMATCH')
        return result

    try:
        identity = _single_identity(discover())
    except (OperationStopped, PlannerPromptAuthoringError):
        raise
    except Exception:
        raise PlannerPromptAuthoringError(
            'VERTEX_PROMPT_AUTHORING_PREFLIGHT_UNAVAILABLE') from None

    outcome = PlannerPromptAuthoringOutcome.REUSED
    if identity is None:
        check_operation()
        timeout_seconds = max(1, int(operation_timeout(
            selected.operation_timeout_seconds)))
        timeout_ms = max(1, int(operation_timeout(
            selected.rpc_timeout_seconds) * 1000))
        try:
            identity = transport.create_initial_version(
                prompt=prompt,
                prompt_display_name=prompt_name,
                version_display_name=version_name,
                timeout_seconds=timeout_seconds,
                max_poll_interval_seconds=selected.max_poll_interval_seconds,
                timeout_ms=timeout_ms)
            check_operation()
            if not isinstance(identity, PromptVersionIdentity):
                raise PlannerPromptAuthoringError(
                    'VERTEX_PROMPT_AUTHORING_CREATE_MISMATCH')
            outcome = PlannerPromptAuthoringOutcome.CREATED
        except OperationStopped:
            raise
        except Exception as error:
            create_error = error
            try:
                identity = _single_identity(discover())
            except OperationStopped:
                raise
            except Exception:
                raise PlannerPromptAuthoringError(
                    'VERTEX_PROMPT_CREATE_OUTCOME_UNKNOWN') from None
            if identity is None:
                raise PlannerPromptAuthoringError(
                    _create_error_code(create_error), operation_name=getattr(
                        create_error, 'operation_name', None)) from None
            outcome = PlannerPromptAuthoringOutcome.RECOVERED

    return _verify_identity(transport, identity, config, selected, outcome,
                            prompt_name, version_name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path,
        default=Path('config/campaign_brief_generator.json'))
    parser.add_argument('--apply', action='store_true',
        help='Create or recover the deterministic initial Vertex prompt version')
    parser.add_argument('--recover-only', action='store_true',
        help='Read and verify the deterministic prompt version without creating it')
    parser.add_argument('--timeout-seconds', type=int, default=300)
    args = parser.parse_args()
    if args.apply and args.recover_only:
        parser.error('--apply and --recover-only are mutually exclusive')
    config = json.loads(args.config.read_text())
    contract = derive_planner_runtime_contract(config)
    _, prompt_name, version_name = build_initial_planner_prompt(config)
    if not args.apply and not args.recover_only:
        print(json.dumps({
            'event': 'planner_prompt_authoring_plan',
            'project': VERTEX_PROJECT,
            'location': LOCATION,
            'prompt_display_name': prompt_name,
            'version_display_name': version_name,
            'runtime_contract_sha256': contract.contract_sha256,
            'mutation_performed': False,
        }, sort_keys=True))
        return
    settings = PlannerPromptAuthoringSettings(
        execution_enabled=args.apply,
        operation_timeout_seconds=args.timeout_seconds)
    with operation_scope(OperationSettings(args.timeout_seconds)):
        client = agentplatform.Client(project=VERTEX_PROJECT, location=LOCATION)
        transport = AgentPlatformPlannerPromptAuthoringTransport(client)
        try:
            result = (author_initial_planner_prompt(transport, config, settings)
                      if args.apply else
                      recover_initial_planner_prompt(transport, config, settings))
        except PlannerPromptAuthoringError as error:
            print(json.dumps({
                'event': 'planner_prompt_authoring_failed',
                'error_code': error.code,
                'project': VERTEX_PROJECT,
                'location': LOCATION,
                'prompt_display_name': prompt_name,
                'version_display_name': version_name,
                'mutation_retried': False,
                'original_operation_name': error.operation_name,
            }, sort_keys=True))
            raise SystemExit(1) from None
    print(json.dumps({
        'event': 'planner_prompt_authoring_verified',
        'outcome': result.outcome.value,
        'project': VERTEX_PROJECT,
        'location': LOCATION,
        'prompt_id': result.identity.prompt_id,
        'version_id': result.identity.version_id,
        'prompt_display_name': result.prompt_display_name,
        'version_display_name': result.version_display_name,
        'runtime_contract_sha256': result.parity.runtime_contract.contract_sha256,
        'snapshot_sha256': result.snapshot.snapshot_sha256,
        'template_sha256': result.snapshot.template_sha256,
        'parity_status': result.parity.status.value,
        'registration_authorized': result.parity.registration_authorized,
    }, sort_keys=True))


if __name__ == '__main__':
    main()
