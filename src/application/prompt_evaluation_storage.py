"""Explicit evaluation storage schemas and pure canonical codecs; no cloud I/O."""
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from decimal import Decimal
import json
from types import MappingProxyType

from google.cloud import bigquery

from src.application.prompt_contract import PromptTask
from src.application.prompt_evaluation_contract import (
    EVALUATION_CONTRACT_VERSION, EvaluationBenchmark, EvaluationCaseResult,
    EvaluationCaseSpec, EvaluationCategory, EvaluationCheck, EvaluationOutcome,
    EvaluationPolicy, EvaluationThreshold, PromptEvaluationRecord, _hash, _utc,
)
from src.application.prompt_registry_codec import PromptRegistryState, normalize
from src.application.workflow_contract import ContractError, _digest, _id, _require, canonical_json_bytes
from src.application.workflow_schema import TableSpec, field

STORAGE_VERSION = 'prompt-evaluation-storage-v1'
MAX_CONTROL_HISTORY = 64
CONTROL_FIELDS = (
    field('schema_version'), field('workspace_id'), field('prompt_key'), field('task'),
    field('revision', 'INTEGER'), field('parent_digest', mode='NULLABLE'),
    field('control_digest'), field('benchmark_sha256'), field('policy_sha256'),
    field('binding_canonical_json'), field('updated_at', 'TIMESTAMP'), field('updated_by'),
)
TABLES = MappingProxyType({spec.name: spec for spec in (
    TableSpec('prompt_evaluation_controls', CONTROL_FIELDS,
              ('workspace_id', 'prompt_key'), ('workspace_id', 'prompt_key')),
    TableSpec('prompt_evaluation_control_history', CONTROL_FIELDS,
              ('workspace_id', 'prompt_key', 'revision'),
              ('workspace_id', 'prompt_key', 'revision'), append_only=True),
    TableSpec('prompt_evaluations', (
        field('schema_version'), field('workspace_id'), field('prompt_key'), field('evaluation_id'),
        field('snapshot_sha256'), field('registry_digest'), field('registry_revision', 'INTEGER'),
        field('control_digest'), field('control_revision', 'INTEGER'), field('evaluation_digest'),
        field('status'), field('completed_at', 'TIMESTAMP'), field('valid_until', 'TIMESTAMP'),
        field('recorded_by'), field('result_count', 'INTEGER'), field('payload_canonical_json'),
    ), ('workspace_id', 'evaluation_id'), ('workspace_id', 'prompt_key', 'evaluation_id'), append_only=True),
    TableSpec('prompt_evaluation_cases', (
        field('schema_version'), field('workspace_id'), field('evaluation_id'), field('case_id'),
        field('invocation_id'), field('response_sha256', mode='NULLABLE'),
        field('observation_sha256'), field('observation_canonical_json'),
    ), ('workspace_id', 'evaluation_id', 'case_id'),
       ('workspace_id', 'evaluation_id', 'case_id'), append_only=True),
)})


def canonical(value):
    return canonical_json_bytes(value).decode('utf-8')


def parse_canonical(value):
    _require(type(value) is str and len(value.encode('utf-8')) <= 65536,
             'INVALID_EVALUATION_STORAGE_JSON')
    try:
        decoded = json.loads(value)
        _require(canonical(decoded) == value, 'NONCANONICAL_EVALUATION_STORAGE_JSON')
        return decoded
    except (ValueError, TypeError, UnicodeError):
        raise ContractError('INVALID_EVALUATION_STORAGE_JSON') from None


def shape(row, table):
    row = normalize(row)
    _require(type(row) is dict and set(row) == {f.name for f in TABLES[table].schema},
             'INVALID_EVALUATION_STORAGE_SHAPE')
    for f in TABLES[table].schema:
        value = row[f.name]
        if value is None and f.mode == 'NULLABLE':
            continue
        if f.field_type == 'STRING':
            _require(type(value) is str, 'INVALID_EVALUATION_STORAGE_TYPE')
        elif f.field_type == 'INTEGER':
            _require(type(value) is int and -(2**63) <= value < 2**63,
                     'INVALID_EVALUATION_STORAGE_TYPE')
        elif f.field_type == 'TIMESTAMP':
            _utc(value)
    _require(row['schema_version'] == STORAGE_VERSION, 'INVALID_EVALUATION_STORAGE_VERSION')
    return row


def binding_payload(benchmark, policy):
    return dict(contract_version=EVALUATION_CONTRACT_VERSION,
        benchmark=dict(benchmark_id=benchmark.benchmark_id, version=benchmark.version,
            task=benchmark.task.value, cases=[case.payload() for case in benchmark.cases]),
        policy=dict(policy_id=policy.policy_id, version=policy.version,
            benchmark_sha256=policy.benchmark_sha256, valid_until=policy.valid_until.isoformat(),
            evaluator_version=policy.evaluator_version,
            allowed_returned_models=list(policy.allowed_returned_models),
            thresholds=[item.payload() for item in policy.thresholds]))


def case_spec(data):
    _require(type(data) is dict and set(data) == {
        'case_id', 'category', 'input_sha256', 'expected_sha256', 'evidence_sha256', 'expected_outcome'},
        'INVALID_EVALUATION_CASE_STORAGE')
    return EvaluationCaseSpec(data['case_id'], EvaluationCategory(data['category']),
        data['input_sha256'], data['expected_sha256'], data['evidence_sha256'],
        EvaluationOutcome(data['expected_outcome']))


def decode_binding(value):
    data = parse_canonical(value)
    _require(type(data) is dict and set(data) == {'contract_version', 'benchmark', 'policy'}
             and data['contract_version'] == EVALUATION_CONTRACT_VERSION,
             'INVALID_EVALUATION_BINDING_STORAGE')
    b, p = data['benchmark'], data['policy']
    _require(type(b) is dict and set(b) == {'benchmark_id', 'version', 'task', 'cases'}
             and type(p) is dict and set(p) == {'policy_id', 'version', 'benchmark_sha256',
                 'valid_until', 'evaluator_version', 'allowed_returned_models', 'thresholds'}
             and type(b['cases']) is list and type(p['thresholds']) is list
             and type(p['allowed_returned_models']) is list, 'INVALID_EVALUATION_BINDING_STORAGE')
    benchmark = EvaluationBenchmark(b['benchmark_id'], b['version'], PromptTask(b['task']),
        tuple(case_spec(case) for case in b['cases']))
    for item in p['thresholds']:
        _require(type(item) is dict and set(item) == {'check', 'minimum_pass_rate'}
                 and type(item['minimum_pass_rate']) is str, 'INVALID_EVALUATION_THRESHOLD_STORAGE')
    policy = EvaluationPolicy(p['policy_id'], p['version'], p['benchmark_sha256'],
        datetime.fromisoformat(p['valid_until']), tuple(EvaluationThreshold(
            EvaluationCheck(item['check']), Decimal(item['minimum_pass_rate'])) for item in p['thresholds']),
        tuple(p['allowed_returned_models']), p['evaluator_version'])
    _require(canonical(binding_payload(benchmark, policy)) == value,
             'INVALID_EVALUATION_BINDING_STORAGE')
    return benchmark, policy


@dataclass(frozen=True, slots=True)
class EvaluationControl:
    workspace_id: str
    prompt_key: str
    revision: int
    parent_digest: str | None
    benchmark: EvaluationBenchmark
    policy: EvaluationPolicy
    updated_at: datetime
    updated_by: str
    digest: str = dataclass_field(init=False)

    def __post_init__(self):
        _id(self.workspace_id)
        _id(self.prompt_key)
        _id(self.updated_by, maximum=256)
        _require(type(self.revision) is int and 1 <= self.revision <= MAX_CONTROL_HISTORY,
                 'INVALID_EVALUATION_CONTROL_REVISION')
        if self.revision == 1:
            _require(self.parent_digest is None, 'INVALID_EVALUATION_CONTROL_PARENT')
        else:
            _digest(self.parent_digest)
        _require(isinstance(self.benchmark, EvaluationBenchmark)
                 and isinstance(self.policy, EvaluationPolicy)
                 and self.policy.benchmark_sha256 == self.benchmark.sha256,
                 'INVALID_EVALUATION_CONTROL_BINDING')
        object.__setattr__(self, 'updated_at', _utc(self.updated_at))
        _require(self.updated_at < self.policy.valid_until, 'EXPIRED_EVALUATION_CONTROL')
        object.__setattr__(self, 'digest', _hash(self.payload()))

    def payload(self):
        return dict(storage_version=STORAGE_VERSION, workspace_id=self.workspace_id,
            prompt_key=self.prompt_key, revision=self.revision, parent_digest=self.parent_digest,
            binding=binding_payload(self.benchmark, self.policy), updated_at=self.updated_at.isoformat(),
            updated_by=self.updated_by)


def encode_control(control):
    _require(isinstance(control, EvaluationControl), 'INVALID_EVALUATION_CONTROL')
    row = dict(schema_version=STORAGE_VERSION, workspace_id=control.workspace_id,
        prompt_key=control.prompt_key, task=control.benchmark.task.value, revision=control.revision,
        parent_digest=control.parent_digest, control_digest=control.digest,
        benchmark_sha256=control.benchmark.sha256, policy_sha256=control.policy.sha256,
        binding_canonical_json=canonical(binding_payload(control.benchmark, control.policy)),
        updated_at=control.updated_at, updated_by=control.updated_by)
    return shape(row, 'prompt_evaluation_controls')


def decode_control(row):
    row = shape(row, 'prompt_evaluation_controls')
    benchmark, policy = decode_binding(row['binding_canonical_json'])
    control = EvaluationControl(row['workspace_id'], row['prompt_key'], row['revision'],
        row['parent_digest'], benchmark, policy, row['updated_at'], row['updated_by'])
    _require(encode_control(control) == row, 'EVALUATION_CONTROL_HASH_MISMATCH')
    return control


def decode_control_history(head, rows):
    _require(type(rows) is list and 1 <= len(rows) <= MAX_CONTROL_HISTORY,
             'INVALID_EVALUATION_CONTROL_HISTORY')
    decoded = tuple(decode_control(row) for row in rows)
    versions = {}
    for i, item in enumerate(decoded, 1):
        _require(item.workspace_id == head.workspace_id and item.prompt_key == head.prompt_key
                 and item.benchmark.task is head.benchmark.task and item.revision == i,
                 'EVALUATION_CONTROL_HISTORY_MISMATCH')
        for kind, identifier, version, digest in (
                ('benchmark', item.benchmark.benchmark_id, item.benchmark.version, item.benchmark.sha256),
                ('policy', item.policy.policy_id, item.policy.version, item.policy.sha256)):
            key = kind, identifier, version
            _require(versions.get(key, digest) == digest, 'EVALUATION_VERSION_REBOUND')
            versions[key] = digest
        if i > 1:
            prior = decoded[i - 2]
            _require(item.parent_digest == prior.digest and item.updated_at >= prior.updated_at,
                     'EVALUATION_CONTROL_HISTORY_MISMATCH')
    _require(decoded[-1] == head, 'EVALUATION_CONTROL_HEAD_MISMATCH')
    return decoded


def encode_evaluation(control, record):
    _require(isinstance(record, PromptEvaluationRecord)
             and control.prompt_key == record.registry_record.snapshot.prompt_key
             and control.benchmark == record.benchmark and control.policy == record.policy
             and control.updated_at <= record.started_at, 'EVALUATION_CONTROL_RECORD_MISMATCH')
    row = dict(schema_version=STORAGE_VERSION, workspace_id=control.workspace_id,
        prompt_key=control.prompt_key, evaluation_id=record.evaluation_id,
        snapshot_sha256=record.registry_record.snapshot.snapshot_sha256,
        registry_digest=record.registry_record.digest, registry_revision=record.registry_record.revision,
        control_digest=control.digest, control_revision=control.revision,
        evaluation_digest=record.digest, status=record.status.value,
        completed_at=record.completed_at, valid_until=record.valid_until, recorded_by=record.recorded_by,
        result_count=len(record.results), payload_canonical_json=canonical(record.payload()))
    cases = [dict(schema_version=STORAGE_VERSION, workspace_id=control.workspace_id,
        evaluation_id=record.evaluation_id, case_id=item.case.case_id, invocation_id=item.invocation_id,
        response_sha256=item.response_sha256, observation_sha256=_hash(item.payload()),
        observation_canonical_json=canonical(item.payload())) for item in record.results]
    return shape(row, 'prompt_evaluations'), [shape(item, 'prompt_evaluation_cases') for item in cases]


def decode_evaluation(control, row, cases, state: PromptRegistryState):
    row = shape(row, 'prompt_evaluations')
    _require(type(cases) is list and len(cases) == row['result_count']
             and isinstance(state, PromptRegistryState), 'INVALID_EVALUATION_CASE_COUNT')
    payload = parse_canonical(row['payload_canonical_json'])
    _require(type(payload) is dict and set(payload) == {'contract_version', 'evaluation_id',
        'registry_digest', 'registry_revision', 'snapshot_sha256', 'template_sha256',
        'benchmark_sha256', 'policy_sha256', 'evaluator_version', 'recorded_by', 'started_at',
        'completed_at', 'valid_until', 'status', 'results'}, 'INVALID_EVALUATION_PAYLOAD')
    anchors = [record for record in state.history if record.digest == row['registry_digest']]
    _require(len(anchors) == 1, 'EVALUATION_REGISTRY_ANCHOR_MISSING')
    results = []
    for case in cases:
        case = shape(case, 'prompt_evaluation_cases')
        data = parse_canonical(case['observation_canonical_json'])
        _require(type(data) is dict and set(data) == {'case', 'observed_outcome',
            'response_sha256', 'requested_model_id', 'returned_model_id', 'invocation_id', 'checks'}
            and type(data['checks']) is dict, 'INVALID_EVALUATION_OBSERVATION')
        results.append(EvaluationCaseResult(case_spec(data['case']), None
            if data['observed_outcome'] is None else EvaluationOutcome(data['observed_outcome']),
            data['response_sha256'], data['requested_model_id'], data['returned_model_id'],
            data['invocation_id'], tuple((EvaluationCheck(key), value)
                for key, value in sorted(data['checks'].items()))))
    record = PromptEvaluationRecord(payload['evaluation_id'], anchors[0], control.benchmark,
        control.policy, payload['evaluator_version'], payload['recorded_by'],
        datetime.fromisoformat(payload['started_at']), datetime.fromisoformat(payload['completed_at']),
        datetime.fromisoformat(payload['valid_until']), tuple(results))
    expected_row, expected_cases = encode_evaluation(control, record)
    _require(expected_row == row and expected_cases == cases,
             'EVALUATION_STORAGE_HASH_MISMATCH')
    return record
