"""Parameterized fenced control/result transactions; constructs plans, never submits."""
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import re

from google.cloud import bigquery

from src.application.identity import VerifiedIdentity
from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_evaluation_contract import PromptEvaluationRecord, _hash, _utc
from src.application.prompt_evaluation_storage import (
    EvaluationControl, TABLES, canonical, encode_control, encode_evaluation,
    decode_control_history,
)
from src.application.prompt_registry_codec import PromptRegistryState, encode_registry_state
from src.application.prompt_registry_schema import TABLES as REGISTRY_TABLES
from src.application.workflow_contract import _require
from src.application.workflow_schema import SCHEMA_VERSION, LOCATION
from src.ingestion.setup_workflow_tables import DATASET_ID, PROJECT


class EvaluationWriteKind(str, Enum):
    CONTROL = 'CONTROL'
    EVALUATION = 'EVALUATION'


def wire(value):
    def scalar(item):
        if isinstance(item, datetime):
            return item.astimezone(timezone.utc).isoformat()
        raise TypeError()
    return json.dumps(value, default=scalar, sort_keys=True, ensure_ascii=False,
        allow_nan=False, separators=(',', ':'))


def json_row(fields, source):
    values = []
    for f in fields:
        value = f"JSON_VALUE({source}, '$.{f.name}')"
        if f.mode == 'REPEATED':
            value = f"JSON_VALUE_ARRAY({source}, '$.{f.name}')"
        elif f.field_type in {'INTEGER', 'TIMESTAMP'}:
            value = f"CAST({value} AS {'INT64' if f.field_type == 'INTEGER' else 'TIMESTAMP'})"
        values.append(f'{value} AS {f.name}')
    return ', '.join(values)


def columns(table, alias=''):
    return ', '.join((alias + '.' if alias else '') + f.name for f in table.schema)


def same_rows(left, right):
    # Compare exact schema-ordered native JSON, including repeated registry fields.
    return f'''NOT EXISTS (SELECT TO_JSON_STRING(t) FROM {left} t EXCEPT DISTINCT SELECT TO_JSON_STRING(t) FROM {right} t)
AND NOT EXISTS (SELECT TO_JSON_STRING(t) FROM {right} t EXCEPT DISTINCT SELECT TO_JSON_STRING(t) FROM {left} t)
AND (SELECT COUNT(*) FROM {left}) = (SELECT COUNT(*) FROM {right})'''


def access_sql():
    return f'''ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.workspaces`
WHERE workspace_id=@workspace_id) = 1 AS 'WORKSPACE_CONFLICT';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.workspaces`
WHERE workspace_id=@workspace_id AND schema_version='{SCHEMA_VERSION}'
AND status='ACTIVE' AND revision BETWEEN 1 AND 9223372036854775806) = 1 AS 'WORKSPACE_DENIED';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.memberships`
WHERE workspace_id=@workspace_id AND principal_id=@principal_id) = 1 AS 'MEMBERSHIP_CONFLICT';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.memberships`
WHERE workspace_id=@workspace_id AND principal_id=@principal_id
AND schema_version='{SCHEMA_VERSION}' AND status='ACTIVE' AND 'ADMIN' IN UNNEST(roles)
AND identity_issuer=@issuer AND identity_subject=@subject
AND revision=@membership_revision) = 1 AS 'MEMBERSHIP_DENIED';'''


def fence_sql():
    return f'''UPDATE `{DATASET_ID}.workspaces`
SET revision=revision+1, updated_at=GREATEST(updated_at, CURRENT_TIMESTAMP())
WHERE workspace_id=@workspace_id AND status='ACTIVE' AND revision < 9223372036854775807;
ASSERT @@row_count=1 AS 'WORKSPACE_FENCE_FAILED';'''


@dataclass(frozen=True, slots=True)
class PromptEvaluationWrite:
    kind: EvaluationWriteKind
    control: EvaluationControl
    identity: VerifiedIdentity
    membership_revision: int
    previous: EvaluationControl | None = None
    record: PromptEvaluationRecord | None = None
    registry_state: PromptRegistryState | None = None
    control_history: tuple[EvaluationControl, ...] = ()

    def __post_init__(self):
        _require(isinstance(self.kind, EvaluationWriteKind)
                 and isinstance(self.control, EvaluationControl)
                 and isinstance(self.identity, VerifiedIdentity)
                 and type(self.membership_revision) is int and self.membership_revision >= 1,
                 'INVALID_EVALUATION_WRITE')
        _require(type(self.control_history) is tuple, 'INVALID_EVALUATION_CONTROL_HISTORY')
        if self.kind is EvaluationWriteKind.CONTROL:
            _require(self.record is None and self.registry_state is None
                     and self.control.updated_by == self.identity.principal.principal_id,
                     'INVALID_EVALUATION_CONTROL_WRITE')
            if self.previous is None:
                _require(self.control.revision == 1 and not self.control_history,
                         'INVALID_EVALUATION_CONTROL_PARENT')
            else:
                _require(isinstance(self.previous, EvaluationControl)
                         and self.previous.workspace_id == self.control.workspace_id
                         and self.previous.prompt_key == self.control.prompt_key
                         and self.previous.benchmark.task is self.control.benchmark.task
                         and self.control.revision == self.previous.revision + 1
                         and self.control.parent_digest == self.previous.digest
                         and self.control.updated_at >= self.previous.updated_at,
                         'INVALID_EVALUATION_CONTROL_PARENT')
                decode_control_history(self.control, [encode_control(item)
                    for item in self.control_history + (self.control,)])
        else:
            _require(self.previous is None and isinstance(self.record, PromptEvaluationRecord)
                     and isinstance(self.registry_state, PromptRegistryState)
                     and self.registry_state.head == self.record.registry_record
                     and self.registry_state.head.status is PromptLifecycle.REGISTERED
                     and self.record.recorded_by == self.identity.principal.principal_id,
                     'INVALID_EVALUATION_RESULT_WRITE')
            encode_evaluation(self.control, self.record)
            decode_control_history(self.control, [encode_control(item) for item in self.control_history])

    @property
    def job_id(self):
        # Competing content/actors for one logical revision/evaluation share a native slot.
        logical = [self.kind.value, self.control.workspace_id,
            self.control.prompt_key, self.control.revision] if self.kind is EvaluationWriteKind.CONTROL \
            else [self.kind.value, self.control.workspace_id, self.record.evaluation_id]
        return 'prompt_evaluation_write_' + _hash(logical)

    @property
    def digest(self):
        return self.control.digest if self.record is None else self.record.digest

    @property
    def logical_id(self):
        return str(self.control.revision) if self.record is None else self.record.evaluation_id

    def parameters(self):
        values = dict(workspace_id=('STRING', self.control.workspace_id),
            prompt_key=('STRING', self.control.prompt_key), principal_id=('STRING', self.identity.principal.principal_id),
            issuer=('STRING', self.identity.issuer), subject=('STRING', self.identity.subject),
            membership_revision=('INT64', self.membership_revision),
            assertion_expires_at=('TIMESTAMP', datetime.fromtimestamp(self.identity.expires_at, timezone.utc)),
            control_json=('STRING', wire(encode_control(self.control))),
            control_history_json=('STRING', wire([encode_control(item) for item in self.control_history])),
            receipt_digest=('STRING', self.digest), logical_id=('STRING', self.logical_id))
        if self.kind is EvaluationWriteKind.CONTROL:
            values['previous_json'] = ('STRING', wire([] if self.previous is None else [encode_control(self.previous)]))
        else:
            row, cases = encode_evaluation(self.control, self.record)
            registry, history = encode_registry_state(self.registry_state)
            values.update(evaluation_id=('STRING', self.record.evaluation_id),
                evaluation_json=('STRING', wire(row)), cases_json=('STRING', wire(cases)),
                registry_json=('STRING', wire(registry)), registry_history_json=('STRING', wire(history)))
        return values

    def job_config(self):
        values = self.parameters()
        _require(set(re.findall(r'(?<!@)@([a-z_]+)', self.sql)) == set(values),
                 'INVALID_EVALUATION_SQL_PARAMETERS')
        return bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter(name, kind, value) for name, (kind, value) in values.items()],
            use_legacy_sql=False, use_query_cache=False, create_session=False,
            maximum_bytes_billed=512 * 1024**2, job_timeout_ms=60000,
            script_options=bigquery.ScriptOptions(statement_timeout_ms=20000),
            labels={'workflow': 'prompt-evaluation-write-v1'})

    @property
    def sql(self):
        control = TABLES['prompt_evaluation_controls']
        history = TABLES['prompt_evaluation_control_history']
        prefix = f'''DECLARE receipt_state STRING DEFAULT 'APPLIED';
BEGIN
BEGIN TRANSACTION;
{access_sql()}
ASSERT CURRENT_TIMESTAMP() < @assertion_expires_at AS 'ASSERTION_EXPIRED';
CREATE TEMP TABLE candidate_control AS SELECT {json_row(control.schema, 'PARSE_JSON(@control_json)')};
CREATE TEMP TABLE current_control AS SELECT {columns(control)} FROM `{DATASET_ID}.{control.name}`
WHERE workspace_id=@workspace_id AND prompt_key=@prompt_key;
CREATE TEMP TABLE control_history AS SELECT {columns(history)} FROM `{DATASET_ID}.{history.name}`
WHERE workspace_id=@workspace_id AND prompt_key=@prompt_key;
CREATE TEMP TABLE expected_control_history AS SELECT {json_row(history.schema, 'item')}
FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@control_history_json))) item;
ASSERT (SELECT COUNT(*) FROM current_control) <= 1 AS 'CONTROL_DUPLICATE';
ASSERT (SELECT COUNT(*) FROM control_history) <= 64 AS 'CONTROL_HISTORY_OVERFLOW';
'''
        if self.kind is EvaluationWriteKind.CONTROL:
            body = f'''CREATE TEMP TABLE previous_control AS SELECT {json_row(control.schema, 'item')}
FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@previous_json))) item;
CREATE TEMP TABLE matching_history AS SELECT * FROM control_history
WHERE revision=(SELECT revision FROM candidate_control);
IF (SELECT COUNT(*) FROM matching_history) > 0 THEN
  ASSERT {same_rows('matching_history', 'candidate_control')} AS 'CONTROL_REPLAY_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM current_control) = 1 AS 'CONTROL_HEAD_MISSING';
  ASSERT (SELECT revision FROM current_control) >= (SELECT revision FROM candidate_control) AS 'CONTROL_HEAD_REGRESSION';
  SET receipt_state='REUSED';
ELSE
  ASSERT {same_rows('current_control', 'previous_control')} AS 'CONTROL_STALE';
  ASSERT {same_rows('control_history', 'expected_control_history')} AS 'CONTROL_HISTORY_STALE';
  ASSERT (SELECT COUNT(*) FROM control_history) = (SELECT revision-1 FROM candidate_control) AS 'CONTROL_HISTORY_PARTIAL';
  ASSERT (SELECT updated_at FROM candidate_control) <= CURRENT_TIMESTAMP()
    AND CURRENT_TIMESTAMP() < (SELECT CAST(JSON_VALUE(PARSE_JSON(binding_canonical_json), '$.policy.valid_until') AS TIMESTAMP)
      FROM candidate_control) AS 'CONTROL_EXPIRED';
  ASSERT NOT EXISTS (SELECT 1 FROM control_history h CROSS JOIN candidate_control c
    WHERE h.task != c.task OR
    (JSON_VALUE(PARSE_JSON(h.binding_canonical_json), '$.benchmark.benchmark_id') = JSON_VALUE(PARSE_JSON(c.binding_canonical_json), '$.benchmark.benchmark_id')
     AND JSON_VALUE(PARSE_JSON(h.binding_canonical_json), '$.benchmark.version') = JSON_VALUE(PARSE_JSON(c.binding_canonical_json), '$.benchmark.version') AND h.benchmark_sha256 != c.benchmark_sha256) OR
    (JSON_VALUE(PARSE_JSON(h.binding_canonical_json), '$.policy.policy_id') = JSON_VALUE(PARSE_JSON(c.binding_canonical_json), '$.policy.policy_id')
     AND JSON_VALUE(PARSE_JSON(h.binding_canonical_json), '$.policy.version') = JSON_VALUE(PARSE_JSON(c.binding_canonical_json), '$.policy.version') AND h.policy_sha256 != c.policy_sha256)) AS 'IMMUTABLE_VERSION_CONFLICT';
  {fence_sql()}
  IF (SELECT COUNT(*) FROM previous_control)=0 THEN
    INSERT INTO `{DATASET_ID}.{control.name}` ({columns(control)}) SELECT {columns(control)} FROM candidate_control;
  ELSE
    UPDATE `{DATASET_ID}.{control.name}` target SET
    {', '.join(f'{f.name}=c.{f.name}' for f in control.schema if f.name not in {'workspace_id', 'prompt_key'})}
    FROM candidate_control c WHERE target.workspace_id=@workspace_id AND target.prompt_key=@prompt_key;
    ASSERT @@row_count=1 AS 'CONTROL_FENCE_FAILED';
  END IF;
  INSERT INTO `{DATASET_ID}.{history.name}` ({columns(history)}) SELECT {columns(history)} FROM candidate_control;
END IF;'''
        else:
            evaluations, cases = TABLES['prompt_evaluations'], TABLES['prompt_evaluation_cases']
            registry, registry_history = REGISTRY_TABLES['prompt_registry'], REGISTRY_TABLES['prompt_registry_history']
            body = f'''CREATE TEMP TABLE candidate_evaluation AS SELECT {json_row(evaluations.schema, 'PARSE_JSON(@evaluation_json)')};
CREATE TEMP TABLE candidate_cases AS SELECT {json_row(cases.schema, 'item')}
FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@cases_json))) item;
CREATE TEMP TABLE stored_evaluation AS SELECT {columns(evaluations)} FROM `{DATASET_ID}.{evaluations.name}`
WHERE workspace_id=@workspace_id AND evaluation_id=@evaluation_id;
CREATE TEMP TABLE stored_cases AS SELECT {columns(cases)} FROM `{DATASET_ID}.{cases.name}`
WHERE workspace_id=@workspace_id AND evaluation_id=@evaluation_id;
IF (SELECT COUNT(*) FROM stored_evaluation)>0 THEN
  ASSERT {same_rows('stored_evaluation', 'candidate_evaluation')} AS 'EVALUATION_REPLAY_CONFLICT';
  ASSERT {same_rows('stored_cases', 'candidate_cases')} AS 'EVALUATION_CASE_REPLAY_CONFLICT';
  SET receipt_state='REUSED';
ELSE
  ASSERT (SELECT COUNT(*) FROM stored_cases)=0 AS 'ORPHAN_EVALUATION_CASES';
  ASSERT {same_rows('current_control', 'candidate_control')} AS 'EVALUATION_CONTROL_STALE';
  ASSERT {same_rows('control_history', 'expected_control_history')} AS 'CONTROL_HISTORY_STALE';
  ASSERT (SELECT COUNT(*) FROM control_history WHERE control_digest=(SELECT control_digest FROM candidate_control))=1 AS 'CONTROL_HISTORY_MISSING';
  ASSERT (SELECT completed_at FROM candidate_evaluation) <= CURRENT_TIMESTAMP()
    AND CURRENT_TIMESTAMP() < (SELECT valid_until FROM candidate_evaluation) AS 'EVALUATION_EXPIRED';
  CREATE TEMP TABLE candidate_registry AS SELECT {json_row(registry.schema, 'PARSE_JSON(@registry_json)')};
  CREATE TEMP TABLE candidate_registry_history AS SELECT {json_row(registry_history.schema, 'item')}
    FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@registry_history_json))) item;
  CREATE TEMP TABLE current_registry AS SELECT {columns(registry)} FROM `{DATASET_ID}.{registry.name}`
    WHERE prompt_key=@prompt_key AND snapshot_sha256=(SELECT snapshot_sha256 FROM candidate_evaluation);
  CREATE TEMP TABLE current_registry_history AS SELECT {columns(registry_history)} FROM `{DATASET_ID}.{registry_history.name}`
    WHERE changed_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31 23:59:59.999999+00')
    AND prompt_key=@prompt_key AND snapshot_sha256=(SELECT snapshot_sha256 FROM candidate_evaluation);
  ASSERT {same_rows('current_registry', 'candidate_registry')} AS 'REGISTRY_STALE';
  ASSERT {same_rows('current_registry_history', 'candidate_registry_history')} AS 'REGISTRY_HISTORY_STALE';
  {fence_sql()}
  INSERT INTO `{DATASET_ID}.{evaluations.name}` ({columns(evaluations)}) SELECT {columns(evaluations)} FROM candidate_evaluation;
  INSERT INTO `{DATASET_ID}.{cases.name}` ({columns(cases)}) SELECT {columns(cases)} FROM candidate_cases;
END IF;'''
        return prefix + body + '''
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
SELECT receipt_state AS state, CURRENT_TIMESTAMP() AS observed_at,
@workspace_id AS workspace_id, @logical_id AS logical_id, @receipt_digest AS record_digest;'''


def validate_write_receipt(plan, rows):
    _require(type(rows) is list and len(rows) == 1 and type(rows[0]) is dict,
             'INVALID_EVALUATION_RECEIPT')
    row = rows[0]
    _require(set(row) == {'state', 'observed_at', 'workspace_id', 'logical_id', 'record_digest'}
             and row['state'] in {'APPLIED', 'REUSED'} and row['workspace_id'] == plan.control.workspace_id
             and row['logical_id'] == plan.logical_id and row['record_digest'] == plan.digest,
             'EVALUATION_RECEIPT_MISMATCH')
    at = _utc(row['observed_at'])
    earliest = plan.control.updated_at if plan.record is None else plan.record.completed_at
    _require(at >= earliest, 'EVALUATION_RECEIPT_TIME_MISMATCH')
    return dict(row, observed_at=at)
