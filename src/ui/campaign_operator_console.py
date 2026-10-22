"""Read-only protected operator installation. No reviewer seed or policy approval."""
from datetime import datetime, timedelta, timezone
from dataclasses import replace
import os,json,logging,re
from pathlib import Path
from src.application.campaign_interpretation_evaluation import InterpretationEvaluationSettings
from src.application.campaign_planner_runtime import PlannerEvaluationSettings
from src.application.campaign_corpus_preparation import CorpusPreparationSettings,canonical,digest
from src.application.campaign_corpus_embedding import CorpusEmbeddingSettings
from src.application.campaign_corpus_retrieval import CorpusRetrievalSettings
from src.application.campaign_product_image_derivation import ImageDerivationSettings
from src.application.campaign_retrieved_planner_runtime import RetrievedPlannerSettings
from src.application.campaign_artifact_runtime import CampaignArtifactSettings
from src.application.campaign_vertex_runtime import VertexCampaignSettings
from src.application.campaign_visual_request import VisualProfile
from src.application.campaign_evaluator_store import sha
from src.retrieval.upload_corpus_selection import validate_selection
from src.application.campaign_semantic_checks import VERSION as LEGACY_CONTRACT,STRICT_VERSION as STRICT_CONTRACT,APPLICABILITY_VERSION as APPLICABILITY_CONTRACT
from src.application.operation_budget import OperationSettings
from src.application.campaign_operator_host import OperatorHostConfig, ProtectedOperatorHost
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
from src.application.sharing_operation_host import SharingHostSettings
from src.application.workflow_contract import _require
from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings

SESSION_KEY = '_ci_protected_operator_host_v1'


def show_product_image_failure(st,error):
    """Expose fixed categories only; never exception messages or user content."""
    code=getattr(error,'code',None)
    allowed={'ACCESS_DENIED','OPERATION_DEADLINE_EXCEEDED','OPERATION_CANCELLED',
        'SHARING_HOST_RETIRED','SHARING_HOST_NOT_STARTED','SHARING_HOST_PROTOCOL_FAILED',
        'PRODUCT_IMAGE_ORIGINAL_MISSING','PRODUCT_IMAGE_BINDING_NOT_VERIFIED',
        'PRODUCT_IMAGE_BINDING_CHANGED','PRODUCT_IMAGE_ORIGINAL_JOB_UNVERIFIED',
        'PRODUCT_IMAGE_SOURCE_EXPIRED','PRODUCT_IMAGE_SOURCE_OR_ACCESS_CHANGED',
        'PRODUCT_IMAGE_OBJECT_CHANGED','PRODUCT_IMAGE_BYTES_CHANGED',
        'IMAGE_DERIVATION_PARENT_CHANGED','IMAGE_DERIVATION_BYTES_OR_PROFILE_CHANGED',
        'IMAGE_DERIVATION_OUTPUT_CHANGED','IMAGE_DERIVATION_PIXELS_CHANGED'}
    safe=code if type(code) is str and (code in allowed or re.fullmatch(
        r'OPERATOR_FAILURE_(?:TYPE|ATTRIBUTE|KEY|FILE|IMPORT|DEPENDENCY)_(?:L[0-9]+|UNKNOWN)',code)) else 'RUNTIME_UNAVAILABLE'
    logging.getLogger(__name__).warning(json.dumps(dict(event='product_image_delivery_failed',code=safe)))
    st.error('The product-image receipt could not be verified. Reload and recover the saved crop before preparing it again.')
    st.caption('Diagnostic reference: '+safe)


def read_only_config(iap, principal_id, *, now, measurement=None,evaluation=None,planner=None,corpus=None,embedding=None,corpus_retrieval=None,product_image=None,retrieved_planner=None,artifact=None,vertex=None):
    # Publisher assignment is trusted deployment configuration. Session manifests
    # bound read lifetime; reconnect never reconstructs a publication intent.
    manifest = OperatorPublisherManifest('ci-poc-marketing-operator-read', iap.expected_audience,
        (principal_id,), now, now + timedelta(seconds=300))
    return OperatorHostConfig(iap=iap, manifest=manifest, policy=None,
        reason_code='EXPLICIT_PILOT_PUBLISHER_READ', enabled=True, writes_enabled=False,measurement=measurement,evaluation=evaluation,planner=planner,corpus=corpus,embedding=embedding,corpus_retrieval=corpus_retrieval,product_image=product_image,retrieved_planner=retrieved_planner,artifact=artifact,vertex=vertex,operation=OperationSettings(300 if evaluation is not None or planner is not None else 120))


def evaluation_choices(path,measurement):
    path=Path(path)
    _require(path.is_absolute() and path.stat().st_size<=16384,'EVALUATION_CONFIG_PATH')
    value=json.loads(path.read_text())
    if type(value) is dict and {'current','original_recovery'}<=set(value):
        _require(set(value)<={'current','original_recovery','previous_recovery'},'EVALUATION_LANE_CONFIGURATION')
        current=InterpretationEvaluationSettings(**value['current'])
        original=InterpretationEvaluationSettings(**value['original_recovery'])
        _require(not current.recovery_only and original.recovery_only
            and current.run_id!=original.run_id and current.evaluator_sha256!=original.evaluator_sha256
            and current.judge_contract_version in (STRICT_CONTRACT,APPLICABILITY_CONTRACT) and original.judge_contract_version==LEGACY_CONTRACT,
            'EVALUATION_LANE_CONFIGURATION')
        current_label='Stage/source v5 attempt' if current.judge_contract_version==APPLICABILITY_CONTRACT else 'Corrected v4 attempt'
        choices={current_label:current,'Original failed attempt (recovery only)':original}
        if 'previous_recovery' in value:
            previous=InterpretationEvaluationSettings(**value['previous_recovery'])
            _require(previous.recovery_only and previous.judge_contract_version==STRICT_CONTRACT
                and previous.run_id not in {current.run_id,original.run_id}
                and previous.evaluator_sha256 not in {current.evaluator_sha256,original.evaluator_sha256},'EVALUATION_LANE_CONFIGURATION')
            choices['Previous v4 attempt (recovery only)']=previous
    else:
        current=InterpretationEvaluationSettings(**value)
        choices={'Configured evaluation':current}
    _require(measurement is not None and current.evaluator_sha256==measurement.evaluator_sha256,'EVALUATION_MEASUREMENT_BINDING')
    return choices


def render(st, headers, identity, settings, *, planner_scope=None):
    principal = os.environ.get('CI_OPERATOR_PUBLISHER_PRINCIPAL_ID', '')
    _require(identity.principal.principal_id == principal, 'OPERATOR_CONSOLE_ACCESS_DENIED')
    st.subheader('Protected operator status')
    st.caption('Read-only. Policy publication, reviewer assignment and campaign release are disabled.')
    measurement=None
    if os.environ.get('CI_MEASUREMENT_RUNTIME_ENABLED')=='true':
        measurement=MeasurementRuntimeSettings(os.environ.get('CI_MEASUREMENT_WORKSPACE_ID',''),
            os.environ.get('CI_MEASUREMENT_PUBLISHER_PRINCIPAL_ID',''),os.environ.get('CI_MEASUREMENT_REVIEW_SHA256',''),
            os.environ.get('CI_MEASUREMENT_EVALUATOR_SHA256',''))
        _require(measurement.publisher_id==principal,'MEASUREMENT_CONSOLE_ACCESS_DENIED')
    evaluation=None
    if os.environ.get('CI_INTERPRETATION_EVALUATION_CONFIG'):
        choices=evaluation_choices(os.environ['CI_INTERPRETATION_EVALUATION_CONFIG'],measurement)
        choice=st.selectbox('Evaluation attempt',tuple(choices)) if len(choices)>1 else next(iter(choices))
        evaluation=choices[choice]
        measurement=replace(measurement,evaluator_sha256=evaluation.evaluator_sha256)
    planner=None
    if os.environ.get('CI_PLANNER_EVALUATION_CONFIG'):
        path=Path(os.environ['CI_PLANNER_EVALUATION_CONFIG'])
        _require(path.is_absolute() and path.stat().st_size<=32768,'PLANNER_CONFIG_PATH')
        raw=json.loads(path.read_text());_require(type(raw['submission_ids']) is list,'PLANNER_CONFIG_SELECTION')
        raw['submission_ids']=tuple(raw['submission_ids']);planner=PlannerEvaluationSettings(**raw)
        _require(measurement is not None,'PLANNER_MEASUREMENT_REQUIRED')
    if planner_scope is not None:
        _require(os.environ.get('CI_MARKETING_PLANNER_EVALUATION_ENABLED')=='true'
            and type(planner_scope) is tuple and len(planner_scope)==2
            and measurement is not None and planner is not None and evaluation is None
            and planner_scope==(measurement.workspace_id,planner.conversation_id),
            'MARKETING_PLANNER_SCOPE_DENIED')
        if not planner.enabled:
            retained=st.session_state.get(SESSION_KEY)
            if retained is not None:
                _require(type(retained) is ProtectedOperatorHost and retained.close(),
                    'OPERATOR_CONSOLE_CLEANUP_PENDING')
                del st.session_state[SESSION_KEY]
            st.info('The private planner evaluation is configured but execution is disabled.')
            return
    corpus=None
    if os.environ.get('CI_UPLOAD_CORPUS_PREPARATION_ENABLED')=='true':
        _require(planner_scope is not None and planner is not None and planner.enabled,'CORPUS_PRIVATE_SCOPE_REQUIRED')
        path=Path(os.environ['CI_UPLOAD_CORPUS_SELECTION_CONFIG'])
        _require(path.is_absolute() and path.stat().st_size<=16384,'CORPUS_CONFIG_PATH')
        selected=validate_selection(json.loads(path.read_text()));selection_sha=digest(selected)
        corpus=CorpusPreparationSettings('upload-corpus-'+selection_sha[:16],canonical(selected),selection_sha,True)
    embedding=None
    if os.environ.get('CI_UPLOAD_TEXT_EMBEDDING_ENABLED')=='true':
        _require(corpus is not None and corpus.enabled,'EMBEDDING_PRIVATE_CORPUS_REQUIRED')
        path=Path(os.environ['CI_UPLOAD_TEXT_EMBEDDING_CONFIG'])
        _require(path.is_absolute() and path.stat().st_size<=16384,'EMBEDDING_CONFIG_PATH')
        value=json.loads(path.read_text())
        _require(type(value) is dict and set(value)=={'version','run_id','binding_id','corpus_sha256','selection_sha256'}
            and value.pop('version')=='private-text-embedding-operation-v1','EMBEDDING_CONFIG')
        embedding=CorpusEmbeddingSettings(**value,enabled=True)
    corpus_retrieval=None
    if os.environ.get('CI_UPLOAD_RETRIEVAL_MEASUREMENT_ENABLED')=='true':
        _require(embedding is not None,'RETRIEVAL_PRIVATE_INDEX_REQUIRED')
        path=Path(os.environ['CI_UPLOAD_RETRIEVAL_MEASUREMENT_CONFIG'])
        _require(path.is_absolute() and path.stat().st_size<=16384,'RETRIEVAL_CONFIG_PATH')
        value=json.loads(path.read_text())
        _require(type(value) is dict and set(value)=={'version','run_id','query_id','query_text','index_record_id','index_sha256','case'}
            and value.pop('version')=='private-retrieval-operation-v1','RETRIEVAL_CONFIG')
        case=value.pop('case');corpus_retrieval=CorpusRetrievalSettings(**value,case_json=canonical(case),case_sha256=sha(case),enabled=True)
    product_image=None
    if os.environ.get('CI_PRODUCT_IMAGE_DERIVATION_ENABLED')=='true':
        _require(planner is not None and planner.enabled,'PRODUCT_IMAGE_PRIVATE_PLANNER_REQUIRED')
        path=Path(os.environ['CI_PRODUCT_IMAGE_DERIVATION_CONFIG'])
        _require(path.is_absolute() and path.stat().st_size<=16384,'PRODUCT_IMAGE_CONFIG_PATH')
        value=json.loads(path.read_text())
        _require(type(value) is dict and set(value)=={'version','decision','decision_sha256'}
            and value['version']=='private-product-image-derivation-operation-v1','PRODUCT_IMAGE_CONFIG')
        product_image=ImageDerivationSettings(canonical(value['decision']),value['decision_sha256'],True)
    retrieved_planner=artifact=None
    if os.environ.get('CI_CAMPAIGN_ARTIFACT_PREFLIGHT_ENABLED')=='true':
        path=Path(os.environ['CI_CAMPAIGN_ARTIFACT_PREFLIGHT_CONFIG'])
        _require(path.is_absolute() and path.stat().st_size<=16384,'ARTIFACT_PREFLIGHT_CONFIG_PATH')
        value=json.loads(path.read_text())
        _require(set(value)=={'version','retrieved_planner','artifact'}
            and value['version']=='private-campaign-artifact-preflight-v1','ARTIFACT_PREFLIGHT_CONFIG')
        retrieved_planner=RetrievedPlannerSettings(**value['retrieved_planner'])
        artifact=CampaignArtifactSettings(**value['artifact'])
        _require(not retrieved_planner.enabled and not artifact.enabled,'ARTIFACT_PREFLIGHT_REQUIRES_DISABLED_EXECUTION')
    vertex=None
    if os.environ.get('CI_VERTEX_VISUAL_TEST_ENABLED')=='true':
        path=Path(os.environ['CI_VERTEX_VISUAL_TEST_CONFIG'])
        _require(path.is_absolute() and path.stat().st_size<=16384,'VERTEX_CONFIG_PATH')
        value=json.loads(path.read_text())
        _require(set(value)=={'version','settings','endpoint_authorization'} and value['version']=='private-vertex-campaign-test-v1',
            'VERTEX_CONFIG')
        decision=value['endpoint_authorization']
        _require(decision=={'decision':'ALLOW_GLOBAL_FOR_THIS_PRIVATE_IMAGE_TEST','source':'EXPLICIT_USER_CONVERSATION_20261021',
            'storage_location':'asia-south1','outgoing_data':['AUTHORIZED_PRODUCT_ONLY_CROP_PNG','PRODUCT_IDENTITY_NAME','PINNED_PRIVATE_SCENE_PROMPT']},
            'VERTEX_ENDPOINT_DECISION_REQUIRED')
        data=dict(value['settings']);data['profile']=VisualProfile(**data['profile']);vertex=VertexCampaignSettings(**data)
        _require(vertex.enabled and vertex.profile.endpoint_authorized and vertex.profile.location=='global','VERTEX_ENDPOINT_DECISION_REQUIRED')
    host = st.session_state.get(SESSION_KEY)
    reconnect=st.button('Reconnect operator runtime')
    changed=host is not None and type(host) is ProtectedOperatorHost and (host.config.measurement!=measurement or host.config.evaluation!=evaluation or host.config.planner!=planner or host.config.corpus!=corpus or host.config.embedding!=embedding or host.config.corpus_retrieval!=corpus_retrieval or host.config.product_image!=product_image or host.config.retrieved_planner!=retrieved_planner or host.config.artifact!=artifact or host.config.vertex!=vertex)
    if reconnect or changed:
        if host is not None:
            _require(type(host) is ProtectedOperatorHost, 'OPERATOR_CONSOLE_SESSION_INVALID')
            _require(host.close(), 'OPERATOR_CONSOLE_CLEANUP_PENDING')
            del st.session_state[SESSION_KEY]
        host = None
    if host is None:
        config = read_only_config(settings.iap, principal, now=datetime.now(timezone.utc),measurement=measurement,evaluation=evaluation,planner=planner,corpus=corpus,embedding=embedding,corpus_retrieval=corpus_retrieval,product_image=product_image,retrieved_planner=retrieved_planner,artifact=artifact,vertex=vertex)
        host = ProtectedOperatorHost(config, SharingHostSettings(enabled=True))
        try: host.start()
        except BaseException:
            host.close(); raise
        st.session_state[SESSION_KEY] = host
    _require(type(host) is ProtectedOperatorHost and host.config.iap == settings.iap
        and host.config.manifest.principal_ids == (principal,) and not host.config.writes_enabled
        and host.config.policy is None and host.config.measurement==measurement and host.config.evaluation==evaluation and host.config.planner==planner and host.config.corpus==corpus
        and host.config.product_image==product_image and host.config.retrieved_planner==retrieved_planner
        and host.config.artifact==artifact and host.config.vertex==vertex, 'OPERATOR_CONSOLE_SESSION_INVALID')
    st.success('Protected operator worker connected; policy publication and approval writes disabled.')
    if vertex is not None:
        from src.ui.private_vertex_candidate import render
        render(st,host,headers,vertex.run_id)
    if artifact is not None:
        st.caption('Campaign runtime preflight only. No model call, artifact write or generation authorization.')
        if st.button('Verify campaign runtime and saved inputs'):
            try:st.json(host.artifact_preflight(headers))
            except Exception as error:
                code=getattr(error,'code',None)
                allowed={'ACCESS_DENIED','OPERATION_DEADLINE_EXCEEDED','OPERATION_CANCELLED','SHARING_HOST_RETIRED',
                    'SHARING_HOST_PROTOCOL_FAILED','ARTIFACT_PREFLIGHT_CONFIG_CHANGED','ARTIFACT_PREFLIGHT_SOURCE_CHANGED',
                    'ARTIFACT_PREFLIGHT_REQUIRES_DISABLED_EXECUTION','CAMPAIGN_FONT_PIN_CHANGED',
                    'CAMPAIGN_FONT_LICENSE_MISSING','CAMPAIGN_RENDER_PREFLIGHT_FAILED'}
                safe=code if type(code) is str and (code in allowed or re.fullmatch(r'OPERATOR_FAILURE_(?:TYPE|ATTRIBUTE|KEY|FILE|IMPORT|DEPENDENCY)_(?:L[0-9]+|UNKNOWN)',code)) else 'RUNTIME_UNAVAILABLE'
                logging.getLogger(__name__).warning(json.dumps(dict(event='campaign_artifact_preflight_failed',code=safe)))
                st.error('Campaign runtime verification did not complete. No campaign run was started.')
                st.caption('Diagnostic reference: '+safe)
    if product_image is not None:
        st.subheader('Product image preparation')
        st.caption('One-time protected preparation of the authorized product-only crop. Original uploads are preserved. No model call or claim approval.')
        for label,action in (('Prepare approved product-only crop',host.product_image_prepare),
                ('Recover saved product-only crop',host.product_image_recover)):
            if st.button(label):
                try:st.json(action(headers))
                except Exception as error:show_product_image_failure(st,error)
    if planner_scope is None and st.button('Read current operator authority'):
        st.json(host.current(headers))

    if measurement is not None and planner_scope is None:
        st.caption('Measurement and approval runtime installed read-only. Evaluation and approval remain required.')
        if st.button('Verify measurement runtime and authority'):
            result=host.measurement_current(headers)
            st.success('Protected measurement runtime and current publisher authority verified; writes and release disabled.')
            st.json(result)

    if evaluation is not None and evaluation.enabled:
        st.subheader('Single-case interpretation evaluation')
        st.caption('Evaluation records and model calls only. Campaign generation, prompt activation, approvals and release remain disabled.')
        st.text('Case: '+evaluation.case_id)
        st.text('Run: '+evaluation.run_id)
        if evaluation.recovery_only:
            st.info('Original attempt is recovery only. No model dispatch is allowed.')
        if not evaluation.recovery_only and st.button('Run the configured interpretation evaluation'):
            result=host.evaluation_run(headers)
            st.success('Evaluation stage and judge records verified. This is not a qualifying benchmark or release approval.')
            st.json(result)
        if st.button('Recover the original evaluation records'):
            result=host.evaluation_recover(headers)
            st.success('Original evaluation records recovered without model redispatch.')
            st.json(result)

    if planner is not None and planner.enabled:
        st.subheader('Real-image planner evaluation')
        st.caption('Private evaluation only. Full quality, retrieval and artifact checks remain incomplete; this does not count toward the qualifying benchmark.')
        if st.button('Run the configured real-image planner evaluation'):
            st.json(host.planner_run(headers))
        if st.button('Recover the original planner evaluation records'):
            st.json(host.planner_recover(headers))
        if planner_scope is None:
            st.caption('Legacy POC corpus coverage is separate from uploaded-source preparation and does not establish retrieval quality.')
            for label,action in (
                ('Measure retrieval corpus for the saved planner stage',host.retrieval_measure),
                ('Recover the original retrieval corpus measurement',host.retrieval_recover)):
                if st.button(label):
                    try:st.json(action(headers))
                    except Exception as error:
                        from src.ui.marketing_pilot import show_planner_failure
                        show_planner_failure(st,error,retrieval=True)

    if corpus is not None and corpus.enabled:
        st.subheader('Selected retrieval corpus')
        st.caption('Only the configured product image and two text documents are selected. Saved extractions are reused; Prepare extracts only missing selected documents and saves an unverified corpus binding. No model call, claim approval or campaign release occurs.')
        for label,action in (
            ('Prepare selected retrieval corpus',host.corpus_prepare),
            ('Recover saved retrieval corpus',host.corpus_recover)):
            if st.button(label):
                try:
                    st.info('Checking selected originals and saved extractions. The original planner result is preserved.')
                    st.json(action(headers))
                except Exception as error:
                    from src.ui.marketing_pilot import show_planner_failure
                    show_planner_failure(st,error,corpus=True)
    if embedding is not None:
        st.subheader('Selected text embeddings and index')
        st.caption('Private evaluation only: embeds the three selected text chunks once and saves a BigQuery index manifest. This does not generate a campaign, approve claims or score retrieval quality.')
        for label,action in (
            ('Run selected text embeddings and index',host.embedding_run),
            ('Recover original text embeddings and index',host.embedding_recover)):
            if st.button(label):
                try:st.json(action(headers))
                except Exception as error:
                    from src.ui.marketing_pilot import show_planner_failure
                    show_planner_failure(st,error,embedding=True)
    if corpus_retrieval is not None:
        st.subheader('Measured retrieval calibration')
        st.caption('Uses the saved document index. Run makes one query-embedding call and saves retrieval ranks and scores. This calibration does not qualify the full pipeline or approve claims.')
        for label,action in (
            ('Run configured retrieval measurement',host.corpus_retrieval_run),
            ('Recover original retrieval measurement',host.corpus_retrieval_recover)):
            if st.button(label):
                try:st.json(action(headers))
                except Exception as error:
                    from src.ui.marketing_pilot import show_planner_failure
                    show_planner_failure(st,error,measured_retrieval=True)
