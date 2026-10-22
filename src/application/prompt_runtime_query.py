"""Explicit-schema prompt pins appended in the existing atomic dispatch claim."""
import json

from google.cloud import bigquery

from src.application.prompt_evaluation_query import columns,json_row,same_rows,wire
from src.application.prompt_evaluation_storage import encode_control as encode_evaluation_control,encode_evaluation
from src.application.prompt_lifecycle_contract import encode_control
from src.application.prompt_registry_codec import encode_registry_state
from src.application.prompt_runtime_contract import TABLES,InvocationPromptBinding
from src.application.prompt_runtime_reader import read_sources
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import DATASET_ID

MAXIMUM_DISPATCH_BYTES_BILLED = 768 * 1024**2
DISPATCH_JOB_TIMEOUT_MS = 120000


def dispatch_config(params):
    """Bound the complete registered dispatch script, retaining statement caps."""
    from src.application.control_io import query_config
    from src.application.sharing_query import SharingQuerySettings
    config=query_config(params, SharingQuerySettings(maximum_bytes_billed=MAXIMUM_DISPATCH_BYTES_BILLED))
    config.job_timeout_ms=DISPATCH_JOB_TIMEOUT_MS
    return config


def source_rows(capture):
    registry,history=encode_registry_state(capture.target)
    evaluation=capture.evaluation
    report,cases=encode_evaluation(capture.evaluation_control_anchor,evaluation.record)
    return dict(controls=[encode_control(capture.control)],
        control_history=[encode_control(item) for item in capture.control_history],
        target_heads=[registry],target_history=list(history),active_heads=[registry],active_history=list(history),
        prompt_evaluation_controls=[encode_evaluation_control(evaluation.control)],
        prompt_evaluation_control_history=[encode_evaluation_control(item) for item in evaluation.control_history],
        prompt_evaluations=[report],prompt_evaluation_cases=list(cases))


def parameters(resolved):
    binding=resolved.binding
    values=dict(runtime_prompt_key=binding.prompt_key,runtime_binding=wire(binding.row()))
    values.update({'runtime_'+name+'_json':wire(rows) for name,rows in source_rows(resolved.capture).items()})
    return [bigquery.ScalarQueryParameter(name,'STRING',value) for name,value in values.items()]


def claim_sql(base_sql):
    """Read pins and write the shared control row to conflict with concurrent promotion."""
    checks=[]
    comparisons=[]
    for name,spec,predicate,_,_ in read_sources():
        predicate=predicate.replace('@prompt_key','@runtime_prompt_key')
        for other,_,_,_,_ in read_sources():
            predicate=predicate.replace(other+'_snapshot','runtime_'+other+'_snapshot')
        checks.append(f'CREATE TEMP TABLE runtime_{name}_snapshot AS SELECT {columns(spec,"r")} '
            f'FROM `{DATASET_ID}.{spec.name}` r WHERE {predicate};')
        expected=(f'(SELECT {json_row(spec.schema,"item")} '
            f'FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@runtime_{name}_json))) item)')
        comparisons.append('('+same_rows('runtime_'+name+'_snapshot',expected)+')')
    # Inline deterministic parameter-only expectations. This avoids ten native
    # CREATE jobs while preserving typed projections, duplicates and all pins.
    checks.append("ASSERT "+'\nAND '.join(comparisons)+" AS 'RUNTIME_PROMPT_CONFLICT';")
    spec=TABLES['invocation_prompt_bindings']
    checks.append(f'''ASSERT (SELECT COUNT(*) FROM runtime_target_heads_snapshot WHERE status='ACTIVE')=1 AS 'RUNTIME_PROMPT_INELIGIBLE';
ASSERT (SELECT COUNT(*) FROM runtime_prompt_evaluations_snapshot WHERE status='PASSED'
 AND CURRENT_TIMESTAMP()<valid_until)=1 AS 'RUNTIME_EVALUATION_EXPIRED';
ASSERT CURRENT_TIMESTAMP()<TIMESTAMP(JSON_VALUE(PARSE_JSON(JSON_VALUE(PARSE_JSON(@runtime_binding),
 '$.binding_canonical_json')),'$.eligibility.eligible_until')) AS 'RUNTIME_POLICY_EXPIRED';
UPDATE `{DATASET_ID}.prompt_lifecycle_controls` SET control_digest=control_digest
WHERE prompt_key=@runtime_prompt_key AND control_digest=(SELECT control_digest FROM runtime_controls_snapshot);
ASSERT @@row_count=1 AS 'RUNTIME_LIFECYCLE_FENCE_FAILED';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.invocation_prompt_bindings`
WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id)=0 AS 'RUNTIME_BINDING_ALREADY_EXISTS';
INSERT INTO `{DATASET_ID}.invocation_prompt_bindings` ({columns(spec)})
SELECT {json_row(spec.schema,'PARSE_JSON(@runtime_binding)')};''')
    marker='-- PROBE_AFTER_FENCE'
    _require(base_sql.count(marker)==1,'INVALID_RUNTIME_DISPATCH_SQL')
    return base_sql.replace(marker,marker+'\n'+'\n'.join(checks))


def binding_from_parameters(params):
    values={p.name:p.value for p in params}
    expected={'runtime_prompt_key','runtime_binding'}|{'runtime_'+name+'_json' for name,_,_,_,_ in read_sources()}
    _require({name for name in values if name.startswith('runtime_')}==expected,'INVALID_RUNTIME_PARAMETERS')
    row=json.loads(values['runtime_binding'])
    from datetime import datetime
    binding=InvocationPromptBinding(**({k:v for k,v in row.items() if k not in {'schema_version','binding_sha256','bound_at'}}
        | dict(bound_at=datetime.fromisoformat(row['bound_at']))))
    _require(wire(binding.row())==values['runtime_binding']
             and values['runtime_prompt_key']==binding.prompt_key
             and binding.workspace_id==values.get('workspace_id')
             and binding.conversation_id==values.get('conversation_id')
             and binding.request_id==values.get('request_id') and binding.invocation_id==values.get('invocation_id')
             and binding.principal_id==values.get('principal_id'), 'INVALID_RUNTIME_PARAMETER_BINDING')
    return binding
