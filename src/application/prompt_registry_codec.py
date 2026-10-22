"""Pure prompt registry row codec and registration-state validation.

This module has no clients or I/O. It maps validated prompt contracts to the
exact BigQuery destination schemas and reconstructs those contracts from native
BigQuery rows. Recalculated hashes and the complete supplied lifecycle chain are
required before a row set can become a future writer plan.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from google.cloud import bigquery

from src.application.prompt_contract import (
    PROMPT_CONTRACT_VERSION, PromptLifecycle, PromptRegistryRecord, PromptTask,
    VertexPromptSnapshot, initial_prompt_record, validate_prompt_transition,
)
from src.application.prompt_registry_schema import STORAGE_VERSION, TABLES
from src.application.workflow_contract import ContractError, _require

MAX_PROMPT_HISTORY = 64


def normalize(value):
    """Convert SDK rows recursively without accepting arbitrary serializers."""
    if isinstance(value, bigquery.Row):
        return {key: normalize(value[key]) for key in value.keys()}
    if type(value) is dict:
        return {key: normalize(child) for key, child in value.items()}
    if type(value) is list:
        return [normalize(child) for child in value]
    return value


def _utc(value: datetime) -> datetime:
    _require(isinstance(value, datetime) and value.tzinfo is not None
             and value.utcoffset() is not None, 'INVALID_PROMPT_STORAGE_TYPE')
    try:
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        raise ContractError('INVALID_PROMPT_STORAGE_TYPE') from None


def _row(row: dict, table: str) -> None:
    fields = TABLES[table].schema
    _require(type(row) is dict and set(row) == {field.name for field in fields},
             'INVALID_PROMPT_STORAGE_SHAPE')
    for field in fields:
        value = row[field.name]
        if field.mode == 'NULLABLE' and value is None:
            continue
        if field.mode == 'REPEATED':
            _require(type(value) is list and all(type(item) is str for item in value),
                     'INVALID_PROMPT_STORAGE_TYPE')
            continue
        if field.field_type == 'STRING':
            _require(type(value) is str, 'INVALID_PROMPT_STORAGE_TYPE')
        elif field.field_type == 'INTEGER':
            _require(type(value) is int and -(2**63) <= value < 2**63,
                     'INVALID_PROMPT_STORAGE_TYPE')
        elif field.field_type == 'TIMESTAMP':
            _utc(value)
        else:
            raise ContractError('INVALID_PROMPT_STORAGE_SCHEMA')
    _require(row['schema_version'] == STORAGE_VERSION,
             'INVALID_PROMPT_STORAGE_VERSION')


def _identity(snapshot: VertexPromptSnapshot) -> dict:
    return dict(prompt_key=snapshot.prompt_key, task=snapshot.task.value,
        snapshot_sha256=snapshot.snapshot_sha256,
        template_sha256=snapshot.template_sha256,
        vertex_project=snapshot.vertex_project,
        vertex_location=snapshot.vertex_location,
        vertex_prompt_id=snapshot.vertex_prompt_id,
        vertex_version_id=snapshot.vertex_version_id)


def _lifecycle(record: PromptRegistryRecord) -> dict:
    return dict(revision=record.revision, parent_digest=record.parent_digest,
        status=record.status.value, evaluation_id=record.evaluation_id,
        changed_at=record.changed_at, changed_by=record.changed_by,
        reason_code=record.reason_code, record_digest=record.digest)


@dataclass(frozen=True, slots=True)
class PromptRegistryState:
    """Bounded supplied current state; never proves an authoritative read."""

    head: PromptRegistryRecord
    history: tuple[PromptRegistryRecord, ...]

    def __post_init__(self):
        _require(isinstance(self.head, PromptRegistryRecord)
                 and type(self.history) is tuple
                 and 1 <= len(self.history) <= MAX_PROMPT_HISTORY
                 and all(isinstance(item, PromptRegistryRecord)
                         for item in self.history), 'INVALID_PROMPT_REGISTRY_STATE')
        first = self.history[0]
        expected_first = initial_prompt_record(first.snapshot,
            imported_at=first.changed_at, imported_by=first.changed_by)
        _require(first == expected_first and first.digest == expected_first.digest,
                 'INVALID_PROMPT_REGISTRY_INITIAL_STATE')
        _require(self.head == self.history[-1]
                 and self.head.digest == self.history[-1].digest,
                 'PROMPT_REGISTRY_HEAD_MISMATCH')
        seen = set()
        for index, item in enumerate(self.history, start=1):
            _require(item.revision == index and item.snapshot == first.snapshot
                     and item.digest not in seen,
                     'PROMPT_REGISTRY_HISTORY_MISMATCH')
            seen.add(item.digest)
        for previous, proposed in zip(self.history, self.history[1:]):
            validate_prompt_transition(previous, proposed)


def initial_registration_state(record: PromptRegistryRecord) -> PromptRegistryState:
    """Create the only state shape eligible for a future initial-import writer."""
    return PromptRegistryState(record, (record,))


def encode_registry_row(state: PromptRegistryState) -> dict:
    """Encode the current destination row; no persistence or authorization."""
    _require(isinstance(state, PromptRegistryState), 'INVALID_PROMPT_REGISTRY_STATE')
    head, first = state.head, state.history[0]
    snapshot = head.snapshot
    row = dict(schema_version=STORAGE_VERSION) | _identity(snapshot) | dict(
        system_instruction=snapshot.system_instruction,
        prompt_template=snapshot.prompt_template,
        required_variables=list(snapshot.required_variables),
        model_id=snapshot.model_id,
        generation_config_canonical_json=snapshot.generation_config_json,
        safety_settings_canonical_json=snapshot.safety_settings_json,
        output_schema_canonical_json=snapshot.output_schema_json,
        output_schema_version=snapshot.output_schema_version,
        safety_policy_version=snapshot.safety_policy_version,
        prompt_contract_version=snapshot.contract_version,
        imported_at=first.changed_at, imported_by=first.changed_by,
    ) | _lifecycle(head)
    _row(row, 'prompt_registry')
    return row


def encode_history_row(record: PromptRegistryRecord) -> dict:
    """Encode one append-only lifecycle row."""
    _require(isinstance(record, PromptRegistryRecord),
             'INVALID_PROMPT_REGISTRY_RECORD')
    row = (dict(schema_version=STORAGE_VERSION) | _identity(record.snapshot)
           | _lifecycle(record))
    _row(row, 'prompt_registry_history')
    return row


def encode_registry_state(state: PromptRegistryState) -> tuple[dict, tuple[dict, ...]]:
    """Return exact current/history row proposals for a future fenced writer."""
    current = encode_registry_row(state)
    history = tuple(encode_history_row(record) for record in state.history)
    # Exercise the independent decoder so an encoder/schema drift fails locally.
    decoded = decode_registry_state(current, list(history))
    _require(decoded == state, 'PROMPT_REGISTRY_CODEC_MISMATCH')
    return current, history


def _status(value: str) -> PromptLifecycle:
    try:
        return PromptLifecycle(value)
    except (TypeError, ValueError):
        raise ContractError('INVALID_PROMPT_STORAGE_STATUS') from None


def _task(value: str) -> PromptTask:
    try:
        return PromptTask(value)
    except (TypeError, ValueError):
        raise ContractError('INVALID_PROMPT_STORAGE_TASK') from None


def _decode_snapshot(row: dict) -> VertexPromptSnapshot:
    snapshot = VertexPromptSnapshot(
        prompt_key=row['prompt_key'], task=_task(row['task']),
        vertex_project=row['vertex_project'],
        vertex_location=row['vertex_location'],
        vertex_prompt_id=row['vertex_prompt_id'],
        vertex_version_id=row['vertex_version_id'],
        system_instruction=row['system_instruction'],
        prompt_template=row['prompt_template'],
        required_variables=tuple(row['required_variables']),
        model_id=row['model_id'],
        generation_config_json=row['generation_config_canonical_json'],
        safety_settings_json=row['safety_settings_canonical_json'],
        output_schema_json=row['output_schema_canonical_json'],
        output_schema_version=row['output_schema_version'],
        safety_policy_version=row['safety_policy_version'],
        contract_version=row['prompt_contract_version'])
    _require(snapshot.snapshot_sha256 == row['snapshot_sha256']
             and snapshot.template_sha256 == row['template_sha256'],
             'PROMPT_STORAGE_SNAPSHOT_HASH_MISMATCH')
    return snapshot


def _decode_record(row: dict, snapshot: VertexPromptSnapshot) -> PromptRegistryRecord:
    expected = _identity(snapshot)
    _require(all(row[name] == value for name, value in expected.items()),
             'PROMPT_STORAGE_IDENTITY_MISMATCH')
    record = PromptRegistryRecord(revision=row['revision'],
        parent_digest=row['parent_digest'], snapshot=snapshot,
        status=_status(row['status']), changed_at=row['changed_at'],
        changed_by=row['changed_by'], reason_code=row['reason_code'],
        evaluation_id=row['evaluation_id'], contract_version=PROMPT_CONTRACT_VERSION)
    _require(record.digest == row['record_digest'],
             'PROMPT_STORAGE_RECORD_HASH_MISMATCH')
    return record


def decode_registry_state(registry_row, history_rows) -> PromptRegistryState:
    """Decode and verify a complete bounded current/history projection."""
    current = normalize(registry_row)
    history = normalize(history_rows)
    _row(current, 'prompt_registry')
    _require(type(history) is list and 1 <= len(history) <= MAX_PROMPT_HISTORY,
             'INVALID_PROMPT_REGISTRY_HISTORY')
    snapshot = _decode_snapshot(current)
    records = []
    for item in history:
        _row(item, 'prompt_registry_history')
        records.append(_decode_record(item, snapshot))
    head = _decode_record(current, snapshot)
    state = PromptRegistryState(head, tuple(records))
    _require(_utc(current['imported_at']) == state.history[0].changed_at
             and current['imported_by'] == state.history[0].changed_by,
             'PROMPT_STORAGE_IMPORT_IDENTITY_MISMATCH')
    return state
