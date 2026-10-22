"""Batch 16: immutable runtime eligibility and invocation prompt pins.

Supplied contracts are not authorization or proof of a native commit. Bindings
are appended atomically by the protected dispatch claim, never by a model call.
"""
from dataclasses import dataclass, field as derived
from datetime import datetime
import json
import re
from types import MappingProxyType

from src.application.prompt_contract import PromptLifecycle, PromptTask, VertexPromptSnapshot
from src.application.prompt_evaluation_contract import EvaluationStatus, _hash, _utc
from src.application.prompt_lifecycle_contract import LifecycleSnapshot
from src.application.workflow_contract import _digest, _id, _require, canonical_json_bytes, validate_profile
from src.application.workflow_schema import TableSpec, field

STORAGE_VERSION = 'invocation-prompt-binding-v1'
TABLES = MappingProxyType({'invocation_prompt_bindings': TableSpec(
    'invocation_prompt_bindings', (
        field('schema_version'), field('workspace_id'), field('conversation_id'),
        field('request_id'), field('invocation_id'), field('principal_id'),
        field('prompt_key'), field('stage'), field('binding_sha256'),
        field('binding_canonical_json'), field('bound_at', 'TIMESTAMP'),
    ), ('workspace_id', 'invocation_id'), ('workspace_id', 'invocation_id'), append_only=True)})


def canonical(value):
    return canonical_json_bytes(value).decode('utf-8')


@dataclass(frozen=True, slots=True)
class RuntimeBehavior:
    """Installed application contract; it contains no editable prompt language."""
    prompt_key: str
    task: PromptTask
    required_variables: tuple[str, ...]
    output_schema_shape_json: str
    safety_settings_json: str
    output_schema_version: str
    safety_policy_version: str

    def __post_init__(self):
        for value in (self.prompt_key, self.output_schema_version, self.safety_policy_version):
            _id(value)
        _require(isinstance(self.task, PromptTask) and type(self.required_variables) is tuple
                 and 1 <= len(self.required_variables) <= 32
                 and self.required_variables == tuple(sorted(set(self.required_variables))),
                 'INVALID_RUNTIME_BEHAVIOR')
        for value in self.required_variables:
            _require(type(value) is str and re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', value),
                     'INVALID_RUNTIME_VARIABLE')
        for value in (self.output_schema_shape_json, self.safety_settings_json):
            _require(type(value) is str and len(value.encode('utf-8')) <= 65536
                     and canonical(json.loads(value)) == value, 'INVALID_RUNTIME_BEHAVIOR_JSON')


def planner_behavior(config):
    from src.application.planner_prompt_parity import (
        derive_planner_runtime_contract, PLANNER_PROMPT_KEY, PLANNER_REQUIRED_VARIABLES,
    )
    contract = derive_planner_runtime_contract(config)
    return RuntimeBehavior(PLANNER_PROMPT_KEY, PromptTask.PLANNER, PLANNER_REQUIRED_VARIABLES,
        contract.output_schema_shape_json, contract.safety_settings_json,
        contract.output_schema_version, contract.safety_policy_version)


def validate_runtime_eligibility(capture: LifecycleSnapshot, *, checked_at: datetime):
    _require(type(capture) is LifecycleSnapshot, 'INVALID_RUNTIME_CAPTURE')
    at = _utc(checked_at)
    head, control, evaluation = capture.target.head, capture.control, capture.evaluation
    _require(control is not None and capture.active == capture.target
             and head.status is PromptLifecycle.ACTIVE
             and control.active_snapshot_sha256 == head.snapshot.snapshot_sha256,
             'RUNTIME_PROMPT_NOT_ACTIVE')
    _require(evaluation is not None and evaluation.record is not None
             and capture.evaluation_control_anchor is not None, 'RUNTIME_EVALUATION_REQUIRED')
    record, current = evaluation.record, evaluation.control
    _require(record.status is EvaluationStatus.PASSED
             and record.registry_record in capture.target.history
             and record.registry_record.status is PromptLifecycle.REGISTERED
             and record.registry_record.snapshot == head.snapshot
             and head.evaluation_id == record.evaluation_id,
             'RUNTIME_EVALUATION_MISMATCH')
    _require(record.policy == current.policy and record.benchmark == current.benchmark,
             'RUNTIME_EVALUATION_POLICY_CHANGED')
    _require(record.completed_at <= head.changed_at
             and all(item.changed_at >= record.completed_at for item in capture.target.history
                     if item.evaluation_id == record.evaluation_id)
             and max(capture.observed_at, head.changed_at, control.changed_at,
                     current.updated_at) <= at < min(record.valid_until, current.policy.valid_until),
             'RUNTIME_EVALUATION_EXPIRED_OR_EARLY')
    return dict(
        snapshot_sha256=head.snapshot.snapshot_sha256, template_sha256=head.snapshot.template_sha256,
        registry_revision=head.revision, registry_digest=head.digest,
        lifecycle_revision=control.revision, lifecycle_digest=control.digest,
        evaluation_id=record.evaluation_id, evaluation_digest=record.digest,
        evaluation_control_revision=current.revision, evaluation_control_digest=current.digest,
        evaluation_anchor_revision=capture.evaluation_control_anchor.revision,
        evaluation_anchor_digest=capture.evaluation_control_anchor.digest,
        benchmark_sha256=current.benchmark.sha256, policy_sha256=current.policy.sha256,
        eligible_until=min(record.valid_until, current.policy.valid_until).isoformat())


def validate_runtime_profile(snapshot, profile, behavior, approved_profile_sha256):
    _require(type(snapshot) is VertexPromptSnapshot and type(behavior) is RuntimeBehavior
             and type(approved_profile_sha256) is frozenset and 1 <= len(approved_profile_sha256) <= 32,
             'INVALID_RUNTIME_PROFILE_POLICY')
    validate_profile(profile)
    _require(_hash(profile) in approved_profile_sha256, 'RUNTIME_PROFILE_NOT_APPROVED')
    _require(snapshot.prompt_key == behavior.prompt_key and snapshot.task is behavior.task
             and snapshot.required_variables == behavior.required_variables
             and snapshot.safety_settings_json == behavior.safety_settings_json
             and snapshot.output_schema_version == behavior.output_schema_version
             and snapshot.safety_policy_version == behavior.safety_policy_version,
             'RUNTIME_BEHAVIOR_MISMATCH')
    from src.application.planner_prompt_parity import _schema_shape
    _require(canonical(_schema_shape(json.loads(snapshot.output_schema_json)))
             == behavior.output_schema_shape_json, 'RUNTIME_OUTPUT_SCHEMA_MISMATCH')
    _require(profile['stage'] == snapshot.task.value and profile['model_id'] == snapshot.model_id
             and profile['location'] == snapshot.vertex_location
             and profile['prompt_id'] == snapshot.vertex_prompt_id
             and profile['prompt_version'] == snapshot.vertex_version_id
             and profile['output_schema_version'] == snapshot.output_schema_version
             and profile['safety_policy_version'] == snapshot.safety_policy_version,
             'RUNTIME_PROFILE_BINDING_MISMATCH')
    generation = json.loads(snapshot.generation_config_json)
    _require(generation == dict(automatic_function_calling_disabled=True,
        max_output_tokens=profile['max_output_tokens'], response_mime_type='application/json',
        seed=profile['seed'], temperature=profile['temperature'],
        thinking_level=profile['thinking_level']) and profile['thinking_budget'] is None
        and profile['thinking_control'] == ('NONE' if profile['thinking_level'] is None else 'LEVEL'),
        'RUNTIME_GENERATION_SETTINGS_MISMATCH')


@dataclass(frozen=True, slots=True)
class InvocationPromptBinding:
    workspace_id: str
    conversation_id: str
    request_id: str
    invocation_id: str
    principal_id: str
    prompt_key: str
    stage: str
    binding_canonical_json: str
    bound_at: datetime
    binding_sha256: str = derived(init=False)

    def __post_init__(self):
        for value in (self.workspace_id, self.conversation_id, self.request_id,
                      self.invocation_id, self.prompt_key):
            _id(value)
        _id(self.principal_id, maximum=256)
        _require(self.stage in {task.value for task in PromptTask}, 'INVALID_RUNTIME_BINDING_STAGE')
        object.__setattr__(self, 'bound_at', _utc(self.bound_at))
        _require(type(self.binding_canonical_json) is str
                 and len(self.binding_canonical_json.encode('utf-8')) <= 262144,
                 'INVALID_RUNTIME_BINDING_SIZE')
        payload = json.loads(self.binding_canonical_json)
        _require(canonical(payload) == self.binding_canonical_json
                 and set(payload) == {'snapshot', 'profile', 'eligibility'}, 'INVALID_RUNTIME_BINDING')
        values = dict(payload['snapshot'])
        for key in ('generation_config', 'safety_settings', 'output_schema'):
            values[key+'_json'] = canonical(values.pop(key))
        values['task'] = PromptTask(values['task'])
        values['required_variables'] = tuple(values['required_variables'])
        snapshot = VertexPromptSnapshot(**values)
        validate_profile(payload['profile'])
        p,pins=payload['profile'],payload['eligibility']
        expected={'snapshot_sha256','template_sha256','registry_revision','registry_digest',
            'lifecycle_revision','lifecycle_digest','evaluation_id','evaluation_digest',
            'evaluation_control_revision','evaluation_control_digest','evaluation_anchor_revision',
            'evaluation_anchor_digest','benchmark_sha256','policy_sha256','eligible_until'}
        _require(type(pins) is dict and set(pins)==expected,'INVALID_RUNTIME_ELIGIBILITY_PINS')
        for key,value in pins.items():
            if key.endswith(('_sha256','_digest')):_digest(value)
            elif key.endswith('_revision'):
                _require(type(value) is int and 1<=value<=128,'INVALID_RUNTIME_ELIGIBILITY_PINS')
        _id(pins['evaluation_id'])
        _require(self.bound_at<_utc(datetime.fromisoformat(pins['eligible_until']))
            and pins['snapshot_sha256']==snapshot.snapshot_sha256
            and pins['template_sha256']==snapshot.template_sha256
            and p['model_id']==snapshot.model_id and p['prompt_id']==snapshot.vertex_prompt_id
            and p['prompt_version']==snapshot.vertex_version_id and p['location']==snapshot.vertex_location
            and p['output_schema_version']==snapshot.output_schema_version
            and p['safety_policy_version']==snapshot.safety_policy_version,
            'INVALID_RUNTIME_BINDING_PINS')
        _require(snapshot.prompt_key == self.prompt_key and snapshot.task.value == self.stage
                 and payload['profile']['stage'] == self.stage, 'RUNTIME_BINDING_SCOPE_MISMATCH')
        object.__setattr__(self, 'binding_sha256', _hash(self.payload()))

    def payload(self):
        return dict(schema_version=STORAGE_VERSION, workspace_id=self.workspace_id,
            conversation_id=self.conversation_id, request_id=self.request_id,
            invocation_id=self.invocation_id, principal_id=self.principal_id,
            prompt_key=self.prompt_key, stage=self.stage,
            binding_canonical_json=self.binding_canonical_json, bound_at=self.bound_at.isoformat())

    def row(self):
        return self.payload() | dict(bound_at=self.bound_at, binding_sha256=self.binding_sha256)


def bind_invocation(capture, snapshot, profile, behavior, approved_profiles, *, conversation_id,
                    request_id, invocation_id, checked_at):
    pins = validate_runtime_eligibility(capture, checked_at=checked_at)
    _require(snapshot == capture.target.head.snapshot, 'RUNTIME_VERTEX_CANONICAL_MISMATCH')
    validate_runtime_profile(snapshot, profile, behavior, approved_profiles)
    return InvocationPromptBinding(capture.request.workspace_id, conversation_id, request_id,
        invocation_id, capture.request.identity.principal.principal_id, snapshot.prompt_key,
        snapshot.task.value, canonical(dict(snapshot=snapshot.canonical_payload(), profile=profile,
                                          eligibility=pins)), checked_at)


def render_prompt(snapshot, variables):
    """Single-pass substitution prevents data containing braces from becoming variables."""
    _require(type(snapshot) is VertexPromptSnapshot and type(variables) is dict
             and set(variables) == set(snapshot.required_variables)
             and all(type(value) is str and len(value.encode('utf-8')) <= 262144
                     for value in variables.values()), 'RUNTIME_VARIABLES_MISMATCH')
    def render(text):
        result = re.sub(r'\{([A-Za-z][A-Za-z0-9_]*)\}', lambda m: variables[m[1]], text)
        _require(len(result.encode('utf-8')) <= 1048576, 'RUNTIME_CONTEXT_TOO_LARGE')
        return result
    return render(snapshot.system_instruction), render(snapshot.prompt_template)
