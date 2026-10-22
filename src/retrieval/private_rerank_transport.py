"""Pinned private relevance evaluator using the existing regional GenAI SDK."""
from google import genai
from google.genai import types
from src.application.campaign_evaluator_observer import EvaluatorProviderResponse
from src.application.campaign_evidence_dispatch import provider_observation
from src.application.operation_budget import require_owned_worker,operation_timeout,check_operation
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT
from src.common.gemini_generation import complete_response_text
from src.retrieval.private_semantic_rerank import RerankRequest,SYSTEM


class PinnedRerankTransport:
    def __call__(self,request):
        require_owned_worker();check_operation();_require(type(request) is RerankRequest,'PRIVATE_RERANK_TRANSPORT')
        profile=request.profile
        config=types.GenerateContentConfig(temperature=profile.temperature,max_output_tokens=profile.max_output_tokens,
            response_mime_type='application/json',response_schema=request.intent['response_schema'],system_instruction=SYSTEM,
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            tool_config=types.ToolConfig(function_calling_config=types.FunctionCallingConfig(mode='NONE')))
        with genai.Client(vertexai=True,project=PROJECT,location=profile.location,
            http_options=types.HttpOptions(api_version=profile.api_version,timeout=int(operation_timeout(75)*1000),
                retry_options=types.HttpRetryOptions(attempts=1))) as client:
            response=client.models.generate_content(model=profile.model_id,contents=request.content,config=config)
        text=complete_response_text(response);_require(0<len(text.encode())<=16384,'PRIVATE_RERANK_OUTPUT_SIZE')
        return EvaluatorProviderResponse(text,provider_observation(response,text))
