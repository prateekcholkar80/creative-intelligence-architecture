"""Pinned SDK transport; one text per RPC, no retries or truncation."""
from google import genai
from google.genai import types
from src.application.operation_budget import require_owned_worker,check_operation,operation_timeout
from src.ingestion.setup_workflow_tables import PROJECT
from src.application.workflow_contract import _require
from src.retrieval.private_text_embeddings import TextEmbeddingProfile,TextEmbeddingResponse,validate_response


class PinnedTextEmbeddingTransport:
    def __call__(self,profile,text):
        require_owned_worker();check_operation()
        _require(type(profile) is TextEmbeddingProfile and type(text) is str
            and 0<len(text.encode())<=4096,'PRIVATE_EMBEDDING_TRANSPORT_INPUT')
        with genai.Client(vertexai=True,project=PROJECT,location=profile.location,
            http_options=types.HttpOptions(api_version='v1',timeout=int(operation_timeout(60)*1000),
                retry_options=types.HttpRetryOptions(attempts=1))) as client:
            response=client.models.embed_content(model=profile.model_id,contents=text,
                config=types.EmbedContentConfig(task_type=profile.task_type,
                    output_dimensionality=profile.dimension,auto_truncate=False))
        _require(response.embeddings is not None and len(response.embeddings)==1,'PRIVATE_EMBEDDING_RESPONSE_COUNT')
        embedding=response.embeddings[0];stats=embedding.statistics
        # This SDK response has no returned model identity; do not substitute
        # the requested model. Missing statistics cannot prove no truncation.
        tokens=None if stats is None else stats.token_count
        # Vertex statistics use a float field for integral token counts.
        if type(tokens) is float and tokens.is_integer():tokens=int(tokens)
        result=TextEmbeddingResponse(tuple(embedding.values or ()),
            None if stats is None else stats.truncated,tokens,None)
        validate_response(result,profile)
        return result
