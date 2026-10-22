"""Disabled-by-composition atomic question-child plan; no model call or release."""
from dataclasses import replace
import json
from google.cloud import bigquery
from src.application.grounded_interpreter_submission import prepare_grounded_submission
from src.application.campaign_question_binding import bind_question_authority
from src.application.campaign_quality_authority import TABLE
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import _require, canonical_json_bytes
from src.application.workflow_schema import TABLES
from src.application.prompt_runtime_contract import TABLES as BINDING_TABLES
from src.application.workflow_invocation_result_query import _fields_expr
from src.ingestion.setup_workflow_tables import DATASET_ID


def prepare_question_submission(*, parent, gaps, history, **inputs):
    """Reuse the request/invocation/transcript/fence transaction and original slot.

    Inputs and captures are trusted server values. Callers must freshly authenticate,
    capture, preflight and recover through the existing protected writer. This pure
    builder does not install a writer or attest that any transaction committed.
    """
    _require(history is not None, 'QUESTION_HISTORY_REQUIRED')
    plan = prepare_grounded_submission(history=history, **inputs)
    config = plan.config()
    parameters = {p.name:p.value for p in config.query_parameters}
    child = json.loads(parameters['authority_payload'])
    bound = bind_question_authority(child, parent=parent, gaps=gaps,
        profile=inputs['profile'], checked_at=inputs['observed_at'])
    payload = canonical_json_bytes(bound).decode()
    replacements = {'authority_payload':payload, 'authority_digest':_hash(bound)}
    replacements['expires_at'] = min(parameters['expires_at'], parent.valid_until, gaps.valid_until)
    config.query_parameters = [bigquery.ScalarQueryParameter(p.name,p.type_,
        replacements.get(p.name,p.value)) for p in config.query_parameters]
    content = json.loads(parent.content_canonical_json)
    request = content['request_record']
    calls = [c for c in content['calls'] if c['invocation_id'] == bound['question_parent']['invocation_id']]
    bindings = [b for b in content['bindings'] if b['invocation_id'] == calls[0]['invocation_id']]
    _require(len(bindings) == 1 and bindings[0]['prompt_key'] == 'campaign-evidence-intent',
        'QUESTION_PARENT_PROMPT_REQUIRED')
    # Both replay and mutation require the original parent evidence. The history
    # guard additionally pins all request/call/body/response rows before mutation.
    checks = []; declarations = []
    for name, value, key, partition in (
        ('requests',request,'request_id','created_at'),
        ('model_invocations',calls[0],'invocation_id','started_at'),
        ('invocation_prompt_bindings',bindings[0],'invocation_id',None)):
        parameter = 'question_parent_' + key + ('_binding' if partition is None else '')
        config.query_parameters = list(config.query_parameters) + [
            bigquery.ScalarQueryParameter(parameter,'STRING',canonical_json_bytes(value).decode())]
        spec = (BINDING_TABLES if partition is None else TABLES)[name]
        declarations.append(f"DECLARE {parameter}_pin DEFAULT STRUCT("
            + _fields_expr(spec.schema,f'PARSE_JSON(@{parameter})') + ');')
        predicate = '' if partition is None else (
            f" AND t.{partition} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')")
        checks.append(f"ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.{name}` t "
            f"WHERE t.workspace_id=@workspace_id AND t.{key}=JSON_VALUE(PARSE_JSON(@{parameter}), '$.{key}')"
            f"{predicate})=1 AND (SELECT COUNT(*) FROM `{DATASET_ID}.{name}` t "
            f"WHERE t.workspace_id=@workspace_id{predicate} "
            f"AND TO_JSON_STRING(t)=TO_JSON_STRING({parameter}_pin))=1 "
            "AS 'QUESTION_PARENT_CHANGED';")
    source = canonical_json_bytes(content['authority']).decode()
    config.query_parameters = list(config.query_parameters) + [
        bigquery.ScalarQueryParameter('question_parent_authority','STRING',source),
        bigquery.ScalarQueryParameter('question_parent_authority_digest','STRING',_hash(content['authority']))]
    checks.append(f"ASSERT (SELECT COUNT(*) FROM `{TABLE}` WHERE workspace_id=@workspace_id "
        "AND request_id=JSON_VALUE(PARSE_JSON(@question_parent_authority), '$.request_id'))=1 "
        f"AND (SELECT COUNT(*) FROM `{TABLE}` WHERE workspace_id=@workspace_id "
        "AND payload_canonical_json=@question_parent_authority AND payload_sha256=@question_parent_authority_digest)=1 "
        "AS 'QUESTION_PARENT_AUTHORITY_CHANGED';")
    marker = 'BEGIN TRANSACTION;'
    _require(plan.sql.count(marker) == 1, 'QUESTION_SUBMISSION_SQL_SHAPE')
    sql = '\n'.join(declarations)+'\n'+plan.sql.replace(marker,marker+'\n'+'\n'.join(checks),1)
    return replace(plan,sql=sql,config_json=json.dumps(config.to_api_repr(),sort_keys=True),
        kind='QUESTION_INTERPRETER_SUBMISSION')
