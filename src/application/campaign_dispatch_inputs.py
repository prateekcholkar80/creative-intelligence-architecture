"""Immutable planner-input manifest appended inside the dispatch transaction."""
import json,hashlib
from google.cloud import bigquery
from src.application.campaign_planner_inputs import PlannerInputs,digest
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import _require,canonical_json_bytes
from src.application.workflow_schema import TableSpec,field
from src.application.prompt_evaluation_query import columns,json_row,wire
from src.ingestion.setup_workflow_tables import DATASET_ID,validate_table

VERSION='campaign-dispatch-inputs-v1'
SPEC=TableSpec('invocation_input_manifests',tuple(field(k) for k in (
    'schema_version','workspace_id','conversation_id','request_id','invocation_id','principal_id',
    'binding_sha256','manifest_sha256','manifest_canonical_json')),
    ('workspace_id','invocation_id'),('workspace_id','invocation_id'),append_only=True)


def validate_prepared(prepared,binding):
    _require(type(prepared) is PlannerInputs,'DISPATCH_INPUT_REQUIRED')
    m=json.loads(prepared.manifest_json)
    _require(canonical_json_bytes(m).decode()==prepared.manifest_json and m['version']=='campaign-planner-inputs-v1'
        and m['scope']=={k:getattr(binding,k) for k in ('workspace_id','conversation_id','request_id','principal_id')}
        and m['invocation_id']==binding.invocation_id and m['binding_sha256']==binding.binding_sha256
        and m['profile_sha256']==_hash(json.loads(binding.binding_canonical_json)['profile'])
        and m['options_sha256']==_hash(json.loads(prepared.options_json))
        and m['system_sha256']==digest(prepared.system_instruction) and m['prompt_sha256']==digest(prepared.prompt)
        and m['images']==[dict(asset_id=a,generation=g,mime_type=t,byte_count=len(d),sha256=hashlib.sha256(d).hexdigest())
            for a,g,t,d in prepared.images], 'DISPATCH_INPUT_CHANGED')
    from src.application.campaign_planner_projection import validate_options
    validate_options(prepared.options_json)
    return dict(schema_version=VERSION,**m['scope'],invocation_id=binding.invocation_id,
        binding_sha256=binding.binding_sha256,manifest_sha256=prepared.sha256,
        manifest_canonical_json=prepared.manifest_json)


def parameter(prepared,binding):
    return bigquery.ScalarQueryParameter('planner_input_manifest','STRING',wire(validate_prepared(prepared,binding)))


def claim_sql(sql):
    marker='-- PROBE_AFTER_FENCE'
    _require(sql.count(marker)==1,'INVALID_INPUT_DISPATCH_SQL')
    return sql.replace(marker,marker+f'''
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.{SPEC.name}`
 WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id)=0 AS 'INPUT_MANIFEST_EXISTS';
ASSERT JSON_VALUE(PARSE_JSON(@planner_input_manifest),'$.invocation_id')=@invocation_id
 AND JSON_VALUE(PARSE_JSON(@planner_input_manifest),'$.workspace_id')=@workspace_id
 AND JSON_VALUE(PARSE_JSON(@planner_input_manifest),'$.request_id')=@request_id
 AND JSON_VALUE(PARSE_JSON(@planner_input_manifest),'$.principal_id')=@principal_id
 AS 'INPUT_MANIFEST_SCOPE';
ASSERT JSON_VALUE(PARSE_JSON(@planner_input_manifest),'$.binding_sha256')=
 JSON_VALUE(PARSE_JSON(@runtime_binding),'$.binding_sha256') AS 'INPUT_BINDING_CHANGED';
ASSERT CURRENT_TIMESTAMP()>=TIMESTAMP(JSON_VALUE(PARSE_JSON(JSON_VALUE(PARSE_JSON(@planner_input_manifest),
 '$.manifest_canonical_json')),'$.checked_at'))
 AND CURRENT_TIMESTAMP()<TIMESTAMP_ADD(TIMESTAMP(JSON_VALUE(PARSE_JSON(JSON_VALUE(PARSE_JSON(@planner_input_manifest),
 '$.manifest_canonical_json')),'$.checked_at')),INTERVAL 60 SECOND) AS 'INPUT_MANIFEST_EXPIRED';
ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.campaign_request_authorities`
 WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id AND request_id=@request_id
 AND principal_id=@principal_id AND payload_sha256=JSON_VALUE(PARSE_JSON(JSON_VALUE(
 PARSE_JSON(@planner_input_manifest),'$.manifest_canonical_json')),'$.selection_sha256'))=1 AS 'INPUT_SELECTION_CHANGED';
INSERT INTO `{DATASET_ID}.{SPEC.name}` ({columns(SPEC)})
SELECT {json_row(SPEC.schema,'PARSE_JSON(@planner_input_manifest)')};''')


def verify_stored(io,prepared,binding):
    expected=validate_prepared(prepared,binding)
    from src.application.operation_budget import operation_call
    validate_table(operation_call(io.client.get_table,SPEC.table(io.client.project).reference,retry=None,timeout=20),SPEC.table(io.client.project))
    row=io.read(f'''SELECT ARRAY(SELECT TO_JSON_STRING(t) FROM `{DATASET_ID}.{SPEC.name}` t
 WHERE workspace_id=@workspace_id AND invocation_id=@invocation_id LIMIT 2) AS manifests''',[
        bigquery.ScalarQueryParameter('workspace_id','STRING',binding.workspace_id),
        bigquery.ScalarQueryParameter('invocation_id','STRING',binding.invocation_id)])
    _require(set(row)=={'manifests'} and type(row['manifests']) is list and len(row['manifests'])==1
        and json.loads(row['manifests'][0])==expected,'DISPATCH_INPUT_STORAGE_CHANGED')
    return expected
