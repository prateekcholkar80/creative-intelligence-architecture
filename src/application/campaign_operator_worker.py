"""Owned operator runtime, retained across calls; no automatic restart or replay."""
from contextlib import ExitStack
from dataclasses import asdict
import re
import socket
import sys
import time
import traceback
from datetime import timedelta
from src.application.campaign_operator_host import config_from_wire, request_values
from src.application.campaign_operator_runtime import compose_operator_runtime
from src.application.identity import IapIdentityAdapter, IdentityError
from src.application.operation_budget import OperationStopped, worker_operation_scope,require_owned_worker
from src.application.operation_channel import receive, send
from src.application.operation_transport import create_workflow_read_clients
from src.application.sharing_host_contract import require
from src.application.workflow_contract import ContractError,_require


EVALUATION_VALIDATION_CODES=frozenset({
    'SEMANTIC_RESULT_SCHEMA','SEMANTIC_RESULT_COVERAGE','SEMANTIC_RESULT_EVIDENCE',
    'SEMANTIC_QUOTE_NOT_OBSERVED','SEMANTIC_MISSING_CANDIDATE',
    'SEMANTIC_CANDIDATE_ANCHOR_REQUIRED','SEMANTIC_PASS_WITH_UNRESOLVED_CHECK',
    'SEMANTIC_STAGE_APPLICABILITY','SEMANTIC_STAGE_OUTPUT_REQUIRED','SEMANTIC_SOURCE_APPLICABILITY_BINDING',
    'INTERPRETATION_EVALUATION_SOURCE_APPLICABILITY_UNAVAILABLE',
    'EVALUATOR_RECOVERY_INTENT_CHANGED','EVALUATOR_RECOVERY_JOB_UNVERIFIED','EVALUATOR_RECOVERY_OUTCOME_CHANGED',
    'EVALUATOR_RECORD_INVALID','INTERPRETATION_EVALUATION_SOURCE_CHANGED','INTERPRETATION_EVALUATOR_CHANGED',
    'INTERPRETATION_ASSESSMENT_UNAVAILABLE','INTERPRETATION_EVALUATION_SCHEMA',
})

EMBEDDING_VALIDATION_CODES=frozenset({
    'CORPUS_EMBEDDING_CAPTURE_REQUIRED','CORPUS_EMBEDDING_BINDING_CHANGED',
    'CORPUS_EMBEDDING_SOURCE_CHANGED','CORPUS_EMBEDDING_INPUT_CHANGED','CORPUS_EMBEDDING_DISABLED',
    'CORPUS_EXTRACTION_REQUIRED','CORPUS_EXTRACTION_CHANGED','CORPUS_BINDING_NOT_VERIFIED',
    'CORPUS_BINDING_NATIVE_UNVERIFIED','CORPUS_SELECTED_SOURCE_CHANGED','CORPUS_PREPARATION_SOURCE_OR_ACCESS_CHANGED',
    'CORPUS_BOUND_RECORDS_CHANGED','CORPUS_BOUND_REFRESH_UNVERIFIED','CORPUS_PREPARATION_CAPTURE_EXPIRED',
    'PRIVATE_EMBEDDING_SOURCE_UNVERIFIED','PRIVATE_EMBEDDING_RECOVERY_NO_DISPATCH',
    'PRIVATE_EMBEDDING_OUTCOME_UNUSABLE','PRIVATE_EMBEDDING_OUTCOME_CHANGED',
    'PRIVATE_EMBEDDING_DISPATCH_CONSUMED','PRIVATE_EMBEDDING_RESERVATION_NO_DISPATCH',
    'PRIVATE_INDEX_SOURCE_UNVERIFIED','PRIVATE_INDEX_EMBEDDING_MISSING','PRIVATE_INDEX_RECOVERY_NO_WRITE',
    'PRIVATE_INDEX_OUTCOME_UNVERIFIED','EVALUATOR_RECOVERY_INTENT_CHANGED','EVALUATOR_RECOVERY_JOB_UNVERIFIED',
    'EVALUATOR_RECOVERY_OUTCOME_CHANGED','EVALUATOR_ORIGINAL_JOB_CONFLICT','EVALUATOR_COMMIT_UNVERIFIED',
    'EVALUATOR_READ_UNVERIFIED','EVALUATOR_RECORD_INVALID',
})


def contract_failure_code(error,method):
    from src.application.campaign_vertex_receipt import METHODS
    if method in METHODS and type(error) is ContractError and error.code in {
            'VERTEX_CAMPAIGN_PLANNER_ORIGINAL_UNAVAILABLE','VERTEX_CAMPAIGN_CROP_CHANGED','VERTEX_CAMPAIGN_PROMPT_CHANGED',
            'VERTEX_CAMPAIGN_SOURCE_CHANGED','VERTEX_CAMPAIGN_SOURCE_EXPIRED','VERTEX_CAMPAIGN_OWNER_CHANGED',
            'VISUAL_COMPLETION_BUDGET_REQUIRED','VISUAL_ENDPOINT_AUTHORIZATION_REQUIRED','VISUAL_ORIGINAL_MISSING',
            'VISUAL_ORIGINAL_UNCERTAIN','VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN','VISUAL_OBJECT_GENERATION_CHANGED',
            'VISUAL_OBJECT_BYTES_CHANGED','ARTIFACT_ORIGINAL_MISSING','ARTIFACT_ORIGINAL_UNCERTAIN',
            'ARTIFACT_SCORE_ORIGINAL_MISSING','ARTIFACT_SCORE_ORIGINAL_UNCERTAIN','ARTIFACT_GENERATION_CHANGED'}:
        return error.code
    if method=='artifact_preflight' and type(error) is ContractError and error.code in {
            'ARTIFACT_PREFLIGHT_CONFIG_CHANGED','ARTIFACT_PREFLIGHT_SOURCE_CHANGED',
            'ARTIFACT_PREFLIGHT_REQUIRES_DISABLED_EXECUTION','CAMPAIGN_FONT_PIN_CHANGED',
            'CAMPAIGN_FONT_LICENSE_MISSING','CAMPAIGN_RENDER_PREFLIGHT_FAILED'}:
        return error.code
    if method in {'product_image_prepare','product_image_recover'} and type(error) is ContractError and error.code in {
            'PRODUCT_IMAGE_ORIGINAL_MISSING','PRODUCT_IMAGE_BINDING_NOT_VERIFIED',
            'PRODUCT_IMAGE_BINDING_CHANGED','PRODUCT_IMAGE_ORIGINAL_JOB_UNVERIFIED',
            'PRODUCT_IMAGE_SOURCE_EXPIRED','PRODUCT_IMAGE_SOURCE_OR_ACCESS_CHANGED',
            'PRODUCT_IMAGE_OBJECT_CHANGED','PRODUCT_IMAGE_BYTES_CHANGED',
            'IMAGE_DERIVATION_PARENT_CHANGED','IMAGE_DERIVATION_BYTES_OR_PROFILE_CHANGED',
            'IMAGE_DERIVATION_OUTPUT_CHANGED','IMAGE_DERIVATION_PIXELS_CHANGED'}:
        return error.code
    if method in {'corpus_retrieval_run','corpus_retrieval_recover'} and type(error) is ContractError and (
            error.code in EMBEDDING_VALIDATION_CODES or error.code.startswith(('PRIVATE_RETRIEVAL_','PRIVATE_MEASUREMENT_','CORPUS_RETRIEVAL_'))):
        return error.code
    if method in {'embedding_run','embedding_recover'} and type(error) is ContractError and error.code in EMBEDDING_VALIDATION_CODES:
        return error.code
    if method in {'corpus_prepare','corpus_recover'} and type(error) is ContractError and error.code in {
            'CORPUS_EXTRACTION_REQUIRED','CORPUS_EXTRACTION_UNAVAILABLE','CORPUS_BINDING_NOT_VERIFIED',
            'CORPUS_BINDING_NATIVE_UNVERIFIED','CORPUS_SELECTED_SOURCE_CHANGED',
            'CORPUS_PREPARATION_SOURCE_OR_ACCESS_CHANGED','CORPUS_PREPARATION_CAPTURE_EXPIRED'}:
        return error.code
    if method=='planner_run' and type(error) is ContractError and error.code=='PLANNER_SESSION_REFRESH_REQUIRED':
        return error.code
    if method in {'evaluation_run','evaluation_recover'} and type(error) is ContractError and error.code in EVALUATION_VALIDATION_CODES:
        return error.code
    return 'ACCESS_DENIED'


def operation_failure_code(error):
    """Fixed exception category and source location only; never values or messages."""
    categories={TypeError:'TYPE',AttributeError:'ATTRIBUTE',KeyError:'KEY',FileNotFoundError:'FILE',ImportError:'IMPORT'}
    category=categories.get(type(error),'DEPENDENCY')
    frames=traceback.extract_tb(error.__traceback__)
    frame=next((f for f in reversed(frames) if '/src/' in f.filename),None)
    location=('L'+str(frame.lineno)) if frame else 'UNKNOWN'
    return 'OPERATOR_FAILURE_'+category+'_'+location


def refresh_measurement_action(config,measurement,headers):
    """Fresh IAP per bounded read/evaluation action; retain worker and pending plans."""
    from src.application.campaign_measurement_runtime import ProtectedMeasurementRuntime
    from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
    from src.application.campaign_publication_reader import _identity
    require_owned_worker()
    _require(type(measurement) is ProtectedMeasurementRuntime and config.measurement==measurement.settings
        and not config.writes_enabled and config.policy is None
        and measurement.publisher.enabled is False and measurement.approval_publisher.enabled is False
        and config.manifest.principal_ids==(measurement.settings.publisher_id,)
        and config.manifest.expected_audience==config.iap.expected_audience,'MEASUREMENT_ACTION_CONFIGURATION')
    actor=measurement.verifier.authenticate(headers);now=measurement.clock()
    _identity(actor,config.iap,now)
    _require(actor.principal.principal_id==measurement.settings.publisher_id
        and config.manifest.effective_at<=now,'MEASUREMENT_RUNTIME_PUBLISHER_DENIED')
    # No old-manifest expiry can be treated as current identity/role authority.
    # This new manifest is issued only after fresh IAP and fixed-publisher checks.
    measurement.manifest=OperatorPublisherManifest('explicit-pilot-measurement-action',config.iap.expected_audience,
        (measurement.settings.publisher_id,),now,now+timedelta(seconds=config.operation.timeout_seconds))


def execute(service, config, method, headers, parameters, measurement=None, evaluation=None, planner=None,corpus=None,embedding=None,corpus_retrieval=None,product_image=None,artifact=None,vertex=None):
    from src.application.campaign_vertex_receipt import METHODS
    if method in METHODS:
        require(vertex is not None and config.vertex is not None and config.vertex.enabled)
        return vertex.execute(headers,phase='VISUAL' if method.startswith('vertex_visual') else 'BANNER',
            recover_only=method.endswith('recover'))
    if method=='artifact_preflight':
        require(artifact is not None and config.artifact is not None and not config.artifact.enabled)
        return artifact.preflight(headers)
    if method in {'product_image_prepare','product_image_recover'}:
        require(product_image is not None and config.product_image is not None and config.planner is not None
            and config.measurement is not None and (config.product_image.enabled or method=='product_image_recover'))
        return product_image.prepare(headers,workspace_id=config.measurement.workspace_id,
            conversation_id=config.planner.conversation_id,request_id=config.planner.request_id,
            recover_only=method=='product_image_recover')
    if method in {'corpus_retrieval_run','corpus_retrieval_recover'}:
        require(corpus_retrieval is not None and config.corpus_retrieval is not None
            and (config.corpus_retrieval.enabled or method=='corpus_retrieval_recover'))
        return corpus_retrieval.execute(headers,recover_only=method=='corpus_retrieval_recover')
    if method in {'embedding_run','embedding_recover'}:
        require(embedding is not None and config.embedding is not None
            and (config.embedding.enabled or method=='embedding_recover'))
        return embedding.execute(headers,recover_only=method=='embedding_recover')
    if method in {'corpus_prepare','corpus_recover'}:
        require(corpus is not None and config.corpus is not None and config.corpus.enabled)
        return corpus.prepare(headers,recover_only=method=='corpus_recover')
    if method in {'retrieval_measure','retrieval_recover'}:
        require(planner is not None)
        return planner.measure_retrieval(headers,recover_only=method=='retrieval_recover')
    if method in {'planner_run','planner_recover'}:
        require(planner is not None)
        return planner.execute(headers,recover_only=method=='planner_recover')
    if method in {'evaluation_run','evaluation_recover'}:
        require(evaluation is not None)
        return evaluation.execute(headers,recover_only=method=='evaluation_recover')
    if method=='measurement_current':
        require(measurement is not None)
        return measurement.current(headers)
    if method == 'prepare': return asdict(service.prepare(headers))
    if method == 'current': return service.current(headers)
    require(method in {'publish', 'recover'})
    require(method != 'publish' or config.writes_enabled)
    return service.execute(headers, confirmed_sha256=parameters['confirmed_sha256'], recover=method == 'recover')


def serve(channel):
    channel.setblocking(False)
    hello = receive(channel, lambda: 30); require(set(hello) == {'config', 'nonce'})
    config = config_from_wire(hello['config']); require(config.enabled)
    send(channel, {'ready': hello['nonce']}, lambda: 30)
    service = None
    measurement = None
    evaluation = None
    planner = None
    corpus = None
    embedding = None
    corpus_retrieval = None
    product_image = None
    artifact = None
    vertex = None
    with ExitStack() as resources:
        while True:
            message = receive(channel, lambda: 30, poll_seconds=30)
            require(set(message) == {'id', 'deadline', 'method', 'headers', 'parameters'}
                and type(message['id']) is str and re.fullmatch(r'[0-9a-f]{32}', message['id']))
            values = request_values(message['method'], message['headers'], message['parameters'])
            def remaining():
                left = message['deadline'] - time.monotonic()
                if left <= 0: raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
                return left
            try:
                with worker_operation_scope(message['deadline']):
                    if service is None:
                        clients = create_workflow_read_clients(settings=config.operation)
                        resources.callback(clients.close)
                        # Reads/previews are enabled; both host and worker gate mutations.
                        service = compose_operator_runtime(client=clients.bigquery,
                            identity_adapter=IapIdentityAdapter(config.iap), manifest=config.manifest,
                            policy=config.policy, reason_code=config.reason_code, status=config.status, enabled=True)
                    if config.measurement is not None and measurement is None:
                        from src.application.campaign_measurement_runtime import ProtectedMeasurementRuntime
                        from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
                        bounded=OperatorPublisherManifest('explicit-pilot-measurement-runtime',config.iap.expected_audience,
                            (config.measurement.publisher_id,),config.manifest.effective_at,config.manifest.valid_until)
                        measurement=ProtectedMeasurementRuntime(client=clients.bigquery,
                            verifier=IapIdentityAdapter(config.iap),settings=config.measurement,session_manifest=bounded)
                    if config.evaluation is not None and evaluation is None:
                        from src.application.campaign_interpretation_evaluation import ProtectedInterpretationEvaluation
                        require(measurement is not None)
                        evaluation=ProtectedInterpretationEvaluation(measurement,config.evaluation)
                    if config.planner is not None and planner is None:
                        from src.application.campaign_planner_runtime import ProtectedPlannerEvaluation
                        from src.application.campaign_attachment_composition import compose_attachments
                        from src.application.campaign_document_service import ProtectedDocumentService
                        from src.application.campaign_document_store import DocumentStore
                        require(measurement is not None)
                        attachments=compose_attachments(client=clients.bigquery,verifier=measurement.verifier)
                        require(attachments is not None)
                        documents=ProtectedDocumentService(intake=attachments,store=DocumentStore(clients.bigquery))
                        planner=ProtectedPlannerEvaluation(measurement=measurement,documents=documents,settings=config.planner)
                    if config.corpus is not None and corpus is None:
                        from src.application.campaign_corpus_preparation import ProtectedCorpusPreparation
                        require(planner is not None)
                        corpus=ProtectedCorpusPreparation(documents,config.corpus,clock=measurement.clock)
                    if config.product_image is not None and product_image is None:
                        from src.application.campaign_product_image_service import ProtectedProductImage
                        require(planner is not None)
                        product_image=ProtectedProductImage(planner.documents,config.product_image,clock=measurement.clock)
                    if config.embedding is not None and embedding is None:
                        from src.application.campaign_corpus_embedding import ProtectedCorpusEmbedding
                        from src.application.campaign_evaluator_store import EvaluatorStore
                        require(corpus is not None)
                        embedding=ProtectedCorpusEmbedding(corpus,EvaluatorStore(clients.bigquery),config.embedding)
                    if config.corpus_retrieval is not None and corpus_retrieval is None:
                        from src.application.campaign_corpus_retrieval import ProtectedCorpusRetrieval
                        require(embedding is not None)
                        corpus_retrieval=ProtectedCorpusRetrieval(embedding,config.corpus_retrieval)
                    if config.artifact is not None and artifact is None:
                        from src.application.campaign_retrieved_planner_runtime import ProtectedRetrievedPlanner
                        from src.application.campaign_artifact_runtime import ProtectedCampaignArtifact
                        require(planner is not None and corpus_retrieval is not None and product_image is not None)
                        retrieved=ProtectedRetrievedPlanner(planner,corpus_retrieval,config.retrieved_planner)
                        artifact=ProtectedCampaignArtifact(retrieved,product_image,config.artifact)
                    if config.vertex is not None and vertex is None:
                        from src.application.campaign_vertex_runtime import ProtectedVertexCampaign
                        require(planner is not None and corpus_retrieval is not None and product_image is not None)
                        vertex=ProtectedVertexCampaign(planner,corpus_retrieval,product_image,config.vertex)
                    from src.application.campaign_vertex_receipt import METHODS
                    if values['method'] in METHODS|{'artifact_preflight','measurement_current','evaluation_run','evaluation_recover','planner_run','planner_recover','retrieval_measure','retrieval_recover','corpus_prepare','corpus_recover','embedding_run','embedding_recover','corpus_retrieval_run','corpus_retrieval_recover','product_image_prepare','product_image_recover'}:
                        refresh_measurement_action(config,measurement,values['headers'])
                    if values['method'] in {'corpus_retrieval_run','corpus_retrieval_recover'}:
                        from src.application.campaign_corpus_retrieval import retrieval_progress
                        def progress(event):send(channel,dict(id=message['id'],progress=event),remaining)
                        with retrieval_progress(progress):
                            result=execute(service,config,values['method'],values['headers'],values['parameters'],measurement,evaluation,planner,corpus,embedding,corpus_retrieval,product_image,artifact,vertex)
                    else:
                        result = execute(service, config, values['method'], values['headers'], values['parameters'],measurement,evaluation,planner,corpus,embedding,corpus_retrieval,product_image,artifact,vertex)
                    send(channel, dict(id=message['id'], result=result), remaining)
            except (IdentityError, ContractError) as error: send(channel, dict(id=message['id'], error=contract_failure_code(error,values['method'])), remaining)
            except OperationStopped as error: send(channel, dict(id=message['id'], error=error.code), remaining)
            except Exception as error: send(channel, dict(id=message['id'], error=operation_failure_code(error)), remaining)


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit(): raise SystemExit(2)
    channel = socket.socket(fileno=int(sys.argv[1]))
    try: serve(channel)
    except BaseException: pass
    finally: channel.close()


if __name__ == '__main__': main()
