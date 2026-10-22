"""Explicit single-operator Vertex creation/recovery for clarification definitions.

Read-before-create is not concurrent-operator serialization. Ambiguous/orphan or
multiple discoveries deny without another create. No BigQuery registry write,
activation, app-role inference or model invocation occurs here.
"""
import argparse,json,hashlib
from dataclasses import dataclass
from datetime import datetime,timezone
from pathlib import Path
import agentplatform
from google.cloud import bigquery
from google.genai import types
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,read_policy,PROJECT,LOCATION
from src.application.campaign_clarification_prompts import definitions,INTENT_KEY,QUESTION_KEY
from src.application.planner_prompt_authoring import (
    AgentPlatformPlannerPromptAuthoringTransport,PromptVersionIdentity,_single_identity,planner_wire_metadata,
)
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.prompt_contract import PromptTask,VertexPromptSnapshot
from src.application.vertex_prompt_reader import VertexPromptReadRequest,read_exact_vertex_prompt
from src.application.operation_budget import OperationSettings,operation_scope,check_operation,OperationStopped
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import _require


@dataclass(frozen=True,slots=True)
class ClarificationAuthoringResult:
    outcome: str
    snapshot: VertexPromptSnapshot
    authoring_sha256: str
    prompt_display_name: str
    version_display_name: str


def build_prompt(definition,settings):
    _require(type(settings) is VertexPromptSnapshot,'CLARIFICATION_AUTHORING_SETTINGS')
    g=json.loads(settings.generation_config_json)
    prompt=agentplatform.types.Prompt(prompt_data=agentplatform.types.PromptData(
        model=settings.model_id,system_instruction=types.Content(role='system',parts=[types.Part(text=definition.system_instruction)]),
        contents=[types.Content(role='user',parts=[types.Part(text=definition.prompt_template)])],
        generation_config=types.GenerationConfig(max_output_tokens=g['max_output_tokens'],response_mime_type=g['response_mime_type'],
            response_schema=types.Schema(**json.loads(definition.output_schema_json)),seed=g['seed'],temperature=g['temperature'],
            thinking_config=types.ThinkingConfig(thinking_level=g['thinking_level'])),
        tool_config=types.FunctionCallingConfig(mode='NONE'),
        safety_settings=[types.SafetySetting(**s) for s in json.loads(settings.safety_settings_json)],
        variables=[{name:types.Part(text='{"example_only":true,"source":"SERVER_BOUND_BIGQUERY_CONTEXT"}')
            for name in definition.required_variables}]))
    # Run the known native protobuf/wire validation before any RPC.
    metadata=planner_wire_metadata(prompt)
    # Native protobuf metadata nests the response schema deeper than workflow
    # records. Bound its canonical wire bytes without widening workflow limits.
    encoded=json.dumps(dict(definition_sha256=definition.sha256,metadata=metadata),sort_keys=True,
        ensure_ascii=False,allow_nan=False,separators=(',',':')).encode('utf-8')
    _require(len(encoded)<=262144,'CLARIFICATION_AUTHORING_METADATA_SIZE')
    sha=hashlib.sha256(encoded).hexdigest()
    return prompt,sha,'ci-'+definition.prompt_key+'-'+sha,'initial-'+sha


def author_prompt(transport,definition,settings,*,apply=False,task=PromptTask.INTERPRETER):
    _require(type(apply) is bool and transport.project==PROJECT and transport.location==LOCATION,
        'CLARIFICATION_AUTHORING_SCOPE')
    _require(type(task) is PromptTask and getattr(definition,'task',PromptTask.INTERPRETER) is task,
        'CLARIFICATION_AUTHORING_TASK')
    prompt,sha,name,version_name=build_prompt(definition,settings)
    def discover():
        return _single_identity(transport.discover(prompt_display_name=name,version_display_name=version_name,timeout_ms=20000))
    identity=discover();outcome='REUSED'
    if identity is None:
        _require(apply,'CLARIFICATION_PROMPT_NOT_FOUND')
        check_operation()
        try:
            identity=transport.create_initial_version(prompt=prompt,prompt_display_name=name,version_display_name=version_name,
                timeout_seconds=240,max_poll_interval_seconds=5,timeout_ms=20000)
            _require(type(identity) is PromptVersionIdentity,'CLARIFICATION_CREATE_IDENTITY');outcome='CREATED'
        except OperationStopped:
            raise
        except Exception:
            # Recovery is read-only. No second create after any uncertain response.
            identity=discover();_require(identity is not None,'CLARIFICATION_CREATE_OUTCOME_UNKNOWN');outcome='RECOVERED'
    request=VertexPromptReadRequest(definition.prompt_key,task,identity.prompt_id,identity.version_id,
        definition.output_schema_version,settings.safety_policy_version)
    exact=read_exact_vertex_prompt(transport,request,timeout_seconds=20)
    expected=definition.snapshot(settings,vertex_prompt_id=identity.prompt_id,vertex_version_id=identity.version_id)
    _require(exact==expected,'CLARIFICATION_VERTEX_PARITY_FAILED')
    return ClarificationAuthoringResult(outcome,exact,sha,name,version_name)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--key',choices=(INTENT_KEY,QUESTION_KEY),required=True)
    p.add_argument('--policy-id',required=True);p.add_argument('--policy-version',required=True);p.add_argument('--policy-sha256',required=True)
    p.add_argument('--settings-prompt-id',required=True);p.add_argument('--settings-version-id',required=True)
    p.add_argument('--apply',action='store_true');p.add_argument('--output',type=Path,required=True);args=p.parse_args()
    report=dict(status='FAILED',prompt_key=args.key,execution_requested=args.apply,bigquery_registration=False,model_calls=0,serving_enabled=False)
    try:
        with operation_scope(OperationSettings(300)),bigquery.Client(project=PROJECT,location=LOCATION) as client:
            base=BigQueryPromptRegistryReader(client).read_exact(args.settings_prompt_id,args.settings_version_id).state.head.snapshot
            request=ClarificationPolicyRequest(args.policy_id,args.policy_version,args.policy_sha256)
            initial=read_policy(client,request);choices=definitions(initial,checked_at=datetime.now(timezone.utc))
            definition=next(d for d in choices if d.prompt_key==args.key)
            vertex=agentplatform.Client(project=PROJECT,location=LOCATION)
            transport=AgentPlatformPlannerPromptAuthoringTransport(vertex)
            result=author_prompt(transport,definition,base,apply=args.apply)
            final=read_policy(client,request)
            _require(initial.record_sha256==final.record_sha256 and initial.native_job_id!=final.native_job_id,
                'CLARIFICATION_AUTHORING_POLICY_CHANGED')
            report.update(status='EXACT_VERTEX_VERSION_VERIFIED',outcome=result.outcome,
                vertex_prompt_id=result.snapshot.vertex_prompt_id,vertex_version_id=result.snapshot.vertex_version_id,
                snapshot_sha256=result.snapshot.snapshot_sha256,template_sha256=result.snapshot.template_sha256,
                authoring_sha256=result.authoring_sha256,definition_sha256=definition.sha256,
                policy_sha256=request.policy_sha256,policy_record_sha256=final.record_sha256,
                settings_snapshot_sha256=base.snapshot_sha256,model_id=result.snapshot.model_id,
                prompt_display_name=result.prompt_display_name,version_display_name=result.version_display_name)
    except Exception as error:
        report.update(error_code=getattr(error,'code',type(error).__name__))
        raise
    finally:
        args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2,default=str)+'\n')
        print(json.dumps(report,sort_keys=True),flush=True)

if __name__=='__main__':main()
