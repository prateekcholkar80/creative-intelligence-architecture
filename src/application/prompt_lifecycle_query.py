"""Explicit-schema atomic lifecycle plans; construction performs no cloud I/O."""
from datetime import datetime, timezone
import re

from google.cloud import bigquery

from src.application.prompt_evaluation_query import access_sql, fence_sql, columns, json_row, same_rows, wire
from src.application.prompt_evaluation_storage import (
    TABLES as EVALUATION_TABLES, encode_control as encode_evaluation_control, encode_evaluation,
)
from src.application.prompt_lifecycle_contract import TABLES, LifecycleWrite, encode_control
from src.application.prompt_registry_codec import encode_registry_state, encode_registry_row, encode_history_row
from src.application.prompt_registry_schema import TABLES as REGISTRY_TABLES
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import DATASET_ID

HISTORY_WINDOW = "changed_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31 23:59:59.999999+00')"
# Replacement/rollback mutate two registry versions after the full evaluation
# gate. Native execution exhausted 512 MiB before the registry UPDATE, where
# BigQuery required a further 20 MiB. Keep an explicit cumulative script cap.
MAXIMUM_WRITE_BYTES_BILLED = 768 * 1024**2


def parameters(plan):
    _require(isinstance(plan, LifecycleWrite), 'INVALID_LIFECYCLE_WRITE')
    source = plan.capture
    request = source.request
    identity = request.identity
    heads, history = [], []
    for state in plan.before_states:
        head, chain = encode_registry_state(state)
        heads.append(head)
        history.extend(chain)
    values = dict(workspace_id=('STRING', request.workspace_id), prompt_key=('STRING', request.prompt_key),
        principal_id=('STRING', identity.principal.principal_id), issuer=('STRING', identity.issuer),
        subject=('STRING', identity.subject), membership_revision=('INT64', request.membership_revision),
        assertion_expires_at=('TIMESTAMP', datetime.fromtimestamp(identity.expires_at, timezone.utc)),
        expected_control_json=('STRING', wire([] if source.control is None else [encode_control(source.control)])),
        expected_control_history_json=('STRING', wire([encode_control(c) for c in source.control_history])),
        candidate_control_json=('STRING', wire(encode_control(plan.control))),
        expected_heads_json=('STRING', wire(heads)), expected_history_json=('STRING', wire(history)),
        expected_active_json=('STRING', wire([] if source.active is None else [encode_registry_row(source.active)])),
        candidate_heads_json=('STRING', wire([encode_registry_row(s) for s in plan.after_states])),
        candidate_history_json=('STRING', wire([encode_history_row(s.head) for s in plan.after_states])))
    if source.evaluation is not None:
        ev = source.evaluation
        report, cases = encode_evaluation(source.evaluation_control_anchor, ev.record)
        # The decoder retained the original control pin in the report, but the
        # gate also pins today's selected control and its complete revision chain.
        values.update(evaluation_id=('STRING', ev.record.evaluation_id),
            evaluation_control_json=('STRING', wire(encode_evaluation_control(ev.control))),
            evaluation_control_history_json=('STRING', wire([encode_evaluation_control(c) for c in ev.control_history])),
            evaluation_json=('STRING', wire(report)), evaluation_cases_json=('STRING', wire(cases)))
    return values


def config(plan):
    values = parameters(plan)
    _require(set(re.findall(r'(?<!@)@([a-z_]+)', sql(plan))) == set(values),
             'INVALID_LIFECYCLE_SQL_PARAMETERS')
    return bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(k, kind, value)
        for k, (kind, value) in values.items()], use_legacy_sql=False, use_query_cache=False,
        create_session=False, maximum_bytes_billed=MAXIMUM_WRITE_BYTES_BILLED, job_timeout_ms=60000,
        script_options=bigquery.ScriptOptions(statement_timeout_ms=20000),
        labels={'workflow': 'prompt-lifecycle-write-v1'})


def temp(name, spec, parameter, *, array=True):
    source = 'item' if array else f'PARSE_JSON(@{parameter})'
    suffix = f' FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@{parameter}))) item' if array else ''
    return f'CREATE TEMP TABLE {name} AS SELECT {json_row(spec.schema, source)}{suffix};'


def sql(plan):
    _require(isinstance(plan, LifecycleWrite), 'INVALID_LIFECYCLE_WRITE')
    control, control_history = TABLES['prompt_lifecycle_controls'], TABLES['prompt_lifecycle_history']
    registry, history = REGISTRY_TABLES['prompt_registry'], REGISTRY_TABLES['prompt_registry_history']
    setup = '\n'.join((
        temp('expected_control', control, 'expected_control_json'),
        temp('expected_control_history', control_history, 'expected_control_history_json'),
        temp('candidate_control', control, 'candidate_control_json', array=False),
        temp('expected_heads', registry, 'expected_heads_json'),
        temp('expected_history', history, 'expected_history_json'),
        temp('expected_active', registry, 'expected_active_json'),
        temp('candidate_heads', registry, 'candidate_heads_json'),
        temp('candidate_history', history, 'candidate_history_json'),
    ))
    gate = ''
    if plan.capture.evaluation is not None:
        sources = (
            ('evaluation_control', 'prompt_evaluation_controls', 'evaluation_control_json', False),
            ('evaluation_control_history', 'prompt_evaluation_control_history', 'evaluation_control_history_json', True),
            ('evaluation', 'prompt_evaluations', 'evaluation_json', False),
            ('evaluation_cases', 'prompt_evaluation_cases', 'evaluation_cases_json', True),
        )
        for name, table, parameter, array in sources:
            spec = EVALUATION_TABLES[table]
            predicate = 'workspace_id=@workspace_id AND ' + ('prompt_key=@prompt_key'
                if 'control' in name else 'evaluation_id=@evaluation_id')
            gate += f'''{temp('expected_' + name, spec, parameter, array=array)}
CREATE TEMP TABLE current_{name} AS SELECT {columns(spec)} FROM `{DATASET_ID}.{table}` WHERE {predicate};
ASSERT {same_rows('current_' + name, 'expected_' + name)} AS 'LIFECYCLE_{name.upper()}_STALE';
'''
        gate += '''ASSERT (SELECT status FROM current_evaluation)='PASSED'
AND CURRENT_TIMESTAMP() < (SELECT valid_until FROM current_evaluation)
AND CURRENT_TIMESTAMP() < (SELECT CAST(JSON_VALUE(PARSE_JSON(binding_canonical_json), '$.policy.valid_until') AS TIMESTAMP)
    FROM current_evaluation_control) AS 'LIFECYCLE_EVALUATION_EXPIRED_OR_FAILED';'''
    updates = ', '.join(f'{f.name}=c.{f.name}' for f in registry.schema
        if f.name in {'revision', 'parent_digest', 'status', 'evaluation_id', 'changed_at',
                      'changed_by', 'reason_code', 'record_digest'})
    return f'''DECLARE receipt_state STRING DEFAULT 'APPLIED';
BEGIN
BEGIN TRANSACTION;
{access_sql()}
ASSERT CURRENT_TIMESTAMP() < @assertion_expires_at AS 'ASSERTION_EXPIRED';
{setup}
CREATE TEMP TABLE current_control AS SELECT {columns(control)} FROM `{DATASET_ID}.{control.name}`
WHERE prompt_key=@prompt_key;
CREATE TEMP TABLE current_control_history AS SELECT {columns(control_history)} FROM `{DATASET_ID}.{control_history.name}`
WHERE prompt_key=@prompt_key;
CREATE TEMP TABLE matching_receipt AS SELECT * FROM current_control_history
WHERE revision=(SELECT revision FROM candidate_control);
ASSERT (SELECT COUNT(*) FROM current_control)<=1 AS 'LIFECYCLE_CONTROL_DUPLICATE';
CREATE TEMP TABLE current_heads AS SELECT {', '.join('r.' + f.name for f in registry.schema)}
FROM `{DATASET_ID}.{registry.name}` r WHERE EXISTS (SELECT 1 FROM expected_heads e
    WHERE r.vertex_project=e.vertex_project AND r.vertex_location=e.vertex_location
    AND r.vertex_prompt_id=e.vertex_prompt_id AND r.vertex_version_id=e.vertex_version_id);
CREATE TEMP TABLE current_history AS SELECT {', '.join('h.' + f.name for f in history.schema)}
FROM `{DATASET_ID}.{history.name}` h WHERE {HISTORY_WINDOW}
AND EXISTS (SELECT 1 FROM expected_heads e WHERE h.prompt_key=e.prompt_key AND h.snapshot_sha256=e.snapshot_sha256);
IF (SELECT COUNT(*) FROM matching_receipt)>0 THEN
  ASSERT {same_rows('matching_receipt', 'candidate_control')} AS 'LIFECYCLE_RECEIPT_CONFLICT';
  ASSERT (SELECT COUNT(*) FROM current_control)=1
  AND (SELECT revision FROM current_control)>=(SELECT revision FROM candidate_control) AS 'LIFECYCLE_HEAD_REGRESSION';
  CREATE TEMP TABLE expected_replay_history AS SELECT * FROM expected_history UNION ALL SELECT * FROM candidate_history;
  CREATE TEMP TABLE actual_replay_history AS SELECT h.* FROM current_history h JOIN candidate_heads c
    ON h.prompt_key=c.prompt_key AND h.snapshot_sha256=c.snapshot_sha256 WHERE h.revision<=c.revision;
  ASSERT {same_rows('actual_replay_history', 'expected_replay_history')} AS 'LIFECYCLE_PARTIAL_REPLAY';
  ASSERT (SELECT COUNT(*) FROM current_heads)=(SELECT COUNT(*) FROM candidate_heads)
    AND (SELECT COUNT(*) FROM current_heads r JOIN candidate_heads c
      ON r.prompt_key=c.prompt_key AND r.snapshot_sha256=c.snapshot_sha256)=(SELECT COUNT(*) FROM candidate_heads)
    AND NOT EXISTS (SELECT 1 FROM current_heads r JOIN candidate_heads c
      ON r.prompt_key=c.prompt_key AND r.snapshot_sha256=c.snapshot_sha256 WHERE r.revision<c.revision)
    AS 'LIFECYCLE_REPLAY_HEAD_MISSING';
  SET receipt_state='REUSED';
ELSE
  ASSERT {same_rows('current_control', 'expected_control')} AS 'LIFECYCLE_CONTROL_STALE';
  ASSERT {same_rows('current_control_history', 'expected_control_history')} AS 'LIFECYCLE_CONTROL_HISTORY_STALE';
  ASSERT {same_rows('current_heads', 'expected_heads')} AS 'LIFECYCLE_REGISTRY_STALE';
  ASSERT {same_rows('current_history', 'expected_history')} AS 'LIFECYCLE_REGISTRY_HISTORY_STALE';
  CREATE TEMP TABLE current_active AS SELECT {columns(registry)} FROM `{DATASET_ID}.{registry.name}`
    WHERE prompt_key=@prompt_key AND status='ACTIVE';
  ASSERT {same_rows('current_active', 'expected_active')} AS 'LIFECYCLE_ACTIVE_STALE';
  ASSERT (SELECT changed_at FROM candidate_control)<=CURRENT_TIMESTAMP() AS 'LIFECYCLE_FUTURE_COMMAND';
  {gate}
  {fence_sql()}
  IF (SELECT COUNT(*) FROM expected_control)=0 THEN
    ASSERT (SELECT COUNT(*) FROM current_control_history)=0 AS 'LIFECYCLE_PARTIAL_GENESIS';
    INSERT INTO `{DATASET_ID}.{control.name}` ({columns(control)}) SELECT {columns(control)} FROM candidate_control;
  ELSE
    UPDATE `{DATASET_ID}.{control.name}` t SET {', '.join(f'{f.name}=c.{f.name}' for f in control.schema if f.name != 'prompt_key')}
    FROM candidate_control c WHERE t.prompt_key=@prompt_key
      AND t.control_digest=(SELECT control_digest FROM expected_control)
      AND t.revision=(SELECT revision FROM expected_control);
    ASSERT @@row_count=1 AS 'LIFECYCLE_GLOBAL_FENCE_FAILED';
  END IF;
  UPDATE `{DATASET_ID}.{registry.name}` t SET {updates}
  FROM candidate_heads c JOIN expected_heads e ON c.prompt_key=e.prompt_key AND c.snapshot_sha256=e.snapshot_sha256
  WHERE t.prompt_key=c.prompt_key AND t.snapshot_sha256=c.snapshot_sha256 AND t.revision=e.revision AND t.record_digest=e.record_digest;
  ASSERT @@row_count=(SELECT COUNT(*) FROM candidate_heads) AS 'LIFECYCLE_REGISTRY_FENCE_FAILED';
  INSERT INTO `{DATASET_ID}.{history.name}` ({columns(history)}) SELECT {columns(history)} FROM candidate_history;
  INSERT INTO `{DATASET_ID}.{control_history.name}` ({columns(control_history)}) SELECT {columns(control_history)} FROM candidate_control;
  ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.{registry.name}` WHERE prompt_key=@prompt_key AND status='ACTIVE')<=1 AS 'LIFECYCLE_ACTIVE_DUPLICATE';
  ASSERT (SELECT active_snapshot_sha256 FROM candidate_control) IS NOT DISTINCT FROM
    (SELECT snapshot_sha256 FROM `{DATASET_ID}.{registry.name}` WHERE prompt_key=@prompt_key AND status='ACTIVE') AS 'LIFECYCLE_ACTIVE_PIN_MISMATCH';
END IF;
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
SELECT receipt_state AS state, prompt_key, revision, control_digest, command_digest,
CURRENT_TIMESTAMP() AS observed_at FROM candidate_control;'''


def validate_receipt(plan, rows):
    _require(isinstance(plan, LifecycleWrite) and type(rows) is list and len(rows)==1
             and type(rows[0]) is dict, 'INVALID_LIFECYCLE_RECEIPT')
    row = rows[0]
    _require(set(row)=={'state', 'prompt_key', 'revision', 'control_digest', 'command_digest', 'observed_at'}
             and row['state'] in {'APPLIED', 'REUSED'} and row['prompt_key']==plan.control.prompt_key
             and type(row['revision']) is int and row['revision']==plan.control.revision
             and row['control_digest']==plan.control.digest and row['command_digest']==plan.command_digest
             and isinstance(row['observed_at'], datetime) and row['observed_at'].tzinfo is not None
             and row['observed_at'].utcoffset() is not None and row['observed_at']>=plan.changed_at,
             'INVALID_LIFECYCLE_RECEIPT')
    return dict(row)
