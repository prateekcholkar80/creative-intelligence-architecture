"""Pinned evaluation-only Vertex transport, one attempt under owned supervision."""
import json
import hashlib
from google import genai
from google.genai import types
from src.application.campaign_evaluator_observer import EvaluatorProfile,EvaluatorProviderResponse
from src.application.campaign_semantic_checks import output_schema,VERSION as LEGACY_CONTRACT,STRICT_VERSION as STRICT_CONTRACT,APPLICABILITY_VERSION as APPLICABILITY_CONTRACT
from src.application.campaign_evidence_dispatch import provider_observation
from src.application.operation_budget import require_owned_worker,operation_timeout,check_operation
from src.application.workflow_contract import _require,_digest
from src.application.campaign_planner_projection import FIELDS
from src.common.gemini_generation import complete_response_text
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

VERSION='campaign-evaluator-vertex-transport-v1'
STRICT_VERSION='campaign-evaluator-vertex-transport-v2'
APPLICABILITY_VERSION='campaign-evaluator-vertex-transport-v3'


class PinnedVertexEvaluatorTransport:
    @staticmethod
    def configuration(profile):
        _require(type(profile) is EvaluatorProfile,'EVALUATOR_TRANSPORT_PROFILE')
        value=json.loads(profile.settings_json)
        _require(type(value) is dict and set(value)=={'version','source_snapshot_sha256','generation_config','safety_settings'}
            and value['version'] in (VERSION,STRICT_VERSION,APPLICABILITY_VERSION) and type(value['generation_config']) is dict
            and type(value['safety_settings']) is list,'EVALUATOR_TRANSPORT_SETTINGS')
        _digest(value['source_snapshot_sha256'])
        generation=dict(value['generation_config'])
        _require(generation.pop('automatic_function_calling_disabled',None) is True,'EVALUATOR_TRANSPORT_TOOLS')
        _require('thinking_level' in generation and not ({'tools','tool_config','system_instruction','response_schema','thinking_config'}&set(generation)),
            'EVALUATOR_TRANSPORT_OVERRIDES')
        level=generation.pop('thinking_level')
        return generation,level,value['safety_settings']

    def __call__(self,profile,system,content,rule_ids):
        version=json.loads(profile.settings_json).get('version')
        _require(version in (VERSION,STRICT_VERSION,APPLICABILITY_VERSION),'EVALUATOR_TRANSPORT_SETTINGS')
        return self._generate(profile,system,content,output_schema(rule_ids,APPLICABILITY_CONTRACT if version==APPLICABILITY_VERSION else STRICT_CONTRACT if version==STRICT_VERSION else LEGACY_CONTRACT))

    def interpretation(self,profile,system,content,schema):
        _require(type(schema) is dict and set(schema.get('properties',{}))=={'schema_version','features','formula_version','audience'},
            'EVALUATION_INTERPRETATION_SCHEMA')
        return self._generate(profile,system,content,schema)

    def planner(self,profile,system,content,schema,images):
        """Server-pinned inline originals only; no URI fetching or provider retry."""
        _require(type(profile) is EvaluatorProfile and type(images) is tuple and 0<len(images)<=16
            and type(schema) is dict and set(schema.get('properties',{}))==FIELDS
            and set(schema.get('required',[]))==FIELDS,
            'EVALUATION_PLANNER_IMAGES')
        parts=[types.Part.from_text(text=content)];seen=set();total=0
        for item in images:
            _require(type(item) is tuple and len(item)==4,'EVALUATION_PLANNER_IMAGES')
            asset,generation,mime,data=item
            _require(type(asset) is str and asset not in seen and type(generation) is str
                and generation.isascii() and generation.isdigit() and int(generation)>0
                and mime in ('image/png','image/jpeg','image/webp') and type(data) is bytes
                and 0<len(data)<=5*1024**2,'EVALUATION_PLANNER_IMAGES')
            seen.add(asset);total+=len(data)
            _require(total<=20*1024**2,'EVALUATION_PLANNER_IMAGES')
            parts.append(types.Part.from_bytes(data=data,mime_type=mime))
        _require(hashlib.sha256(system.encode()).hexdigest()==profile.template_sha256,'EVALUATION_PLANNER_TEMPLATE')
        return self._generate(profile,system,[types.Content(role='user',parts=parts)],schema)

    def _generate(self,profile,system,content,schema):
        require_owned_worker();check_operation()
        generation,level,safety=self.configuration(profile)
        config=types.GenerateContentConfig(**generation,system_instruction=system,
            response_schema=schema,thinking_config=None if level is None else types.ThinkingConfig(thinking_level=level),
            safety_settings=[types.SafetySetting(**s) for s in safety],
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            tool_config=types.ToolConfig(function_calling_config=types.FunctionCallingConfig(mode='NONE')))
        with genai.Client(vertexai=True,project=PROJECT,location=LOCATION,
            http_options=types.HttpOptions(timeout=int(operation_timeout(75)*1000),
                retry_options=types.HttpRetryOptions(attempts=1))) as client:
            response=client.models.generate_content(model=profile.model_id,contents=content,config=config)
        raw=complete_response_text(response)
        _require(0<len(raw.encode())<=65536,'EVALUATOR_TRANSPORT_OUTPUT_SIZE')
        return EvaluatorProviderResponse(raw,provider_observation(response,raw))
