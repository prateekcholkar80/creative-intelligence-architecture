"""Fixed protected operator transport; trusted configuration never comes from chat."""
from dataclasses import asdict, dataclass
from datetime import datetime
import re
import json
import math,logging
import sys
from src.application.identity import IapSettings
from src.application.campaign_measurement_runtime import MeasurementRuntimeSettings
from src.application.campaign_interpretation_evaluation import InterpretationEvaluationSettings
from src.application.campaign_planner_runtime import PlannerEvaluationSettings
from src.application.campaign_corpus_preparation import CorpusPreparationSettings,PREFIX as CORPUS_PREFIX
from src.application.campaign_corpus_embedding import CorpusEmbeddingSettings
from src.application.campaign_corpus_retrieval import CorpusRetrievalSettings
from src.application.campaign_product_image_derivation import ImageDerivationSettings,PREFIX as IMAGE_PREFIX
from src.application.campaign_retrieved_planner_runtime import RetrievedPlannerSettings
from src.application.campaign_artifact_runtime import CampaignArtifactSettings
from src.application.campaign_vertex_runtime import VertexCampaignSettings
from src.application.campaign_visual_request import VisualProfile
from src.application.campaign_vertex_receipt import METHODS as VERTEX_METHODS,decode as decode_vertex
from src.retrieval.private_retrieval_metrics import ranking_metrics
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
from src.application.campaign_review_publication_contract import ReviewOperatorPolicy
from src.application.operation_budget import OperationSettings
from src.application.sharing_host_contract import require
from src.application.sharing_operation_host import SharingOperationHost, SharingHostError
from src.application.workflow_control_host import request_values as management_request
from src.application.operation_channel import receive
from src.application.campaign_corpus_retrieval import PHASES


@dataclass(frozen=True, slots=True)
class OperatorHostConfig:
    iap: IapSettings
    manifest: OperatorPublisherManifest
    policy: ReviewOperatorPolicy | None
    reason_code: str
    enabled: bool = False
    writes_enabled: bool = False
    status: str = 'ACTIVE'
    operation: OperationSettings = OperationSettings(120)
    measurement: MeasurementRuntimeSettings | None = None
    evaluation: InterpretationEvaluationSettings | None = None
    planner: PlannerEvaluationSettings | None = None
    corpus: CorpusPreparationSettings | None = None
    embedding: CorpusEmbeddingSettings | None = None
    corpus_retrieval: CorpusRetrievalSettings | None = None
    product_image: ImageDerivationSettings | None = None
    retrieved_planner: RetrievedPlannerSettings | None = None
    artifact: CampaignArtifactSettings | None = None
    vertex: VertexCampaignSettings | None = None

    def __post_init__(self):
        require(type(self.iap) is IapSettings and type(self.manifest) is OperatorPublisherManifest
            and (self.policy is None or type(self.policy) is ReviewOperatorPolicy) and type(self.operation) is OperationSettings
            and type(self.enabled) is bool and type(self.writes_enabled) is bool
            and self.status in {'ACTIVE', 'REVOKED'}
            and type(self.reason_code) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', self.reason_code)
            and (self.measurement is None or (type(self.measurement) is MeasurementRuntimeSettings
                and self.manifest.principal_ids==(self.measurement.publisher_id,)))
            and (self.evaluation is None or (type(self.evaluation) is InterpretationEvaluationSettings and self.measurement is not None
                and self.evaluation.evaluator_sha256==self.measurement.evaluator_sha256))
            and (self.planner is None or (type(self.planner) is PlannerEvaluationSettings and self.measurement is not None))
            and (self.corpus is None or (type(self.corpus) is CorpusPreparationSettings and self.planner is not None
                and self.measurement is not None and self.policy is None and not self.writes_enabled
                and json.loads(self.corpus.selection_json)['image_binding_id']==self.planner.binding_id
                and tuple(s['submission_id'] for s in json.loads(self.corpus.selection_json)['sources']
                    if s['role']=='BOUND_IMAGE')==self.planner.submission_ids
                and all(json.loads(self.corpus.selection_json)['scope'][k]==v for k,v in dict(
                    workspace_id=self.measurement.workspace_id,conversation_id=self.planner.conversation_id,
                    request_id=self.planner.request_id).items())
                and all(json.loads(self.corpus.selection_json)['scope'][k]==json.loads(self.planner.case_input_json)[k]
                    for k in ('sku','country','channel'))))
            and (self.embedding is None or (type(self.embedding) is CorpusEmbeddingSettings
                and self.corpus is not None and self.corpus.enabled
                and self.embedding.selection_sha256==self.corpus.selection_sha256
                and self.policy is None and not self.writes_enabled))
            and (self.corpus_retrieval is None or (type(self.corpus_retrieval) is CorpusRetrievalSettings
                and self.embedding is not None and self.policy is None and not self.writes_enabled
                and json.loads(self.corpus_retrieval.case_json)['corpus_sha256']==self.embedding.corpus_sha256))
            and (self.product_image is None or (type(self.product_image) is ImageDerivationSettings
                and self.planner is not None and self.measurement is not None and self.policy is None and not self.writes_enabled
                and json.loads(self.product_image.decision_json)['source_binding_id']==self.planner.binding_id
                and (json.loads(self.product_image.decision_json)['source_submission_id'],)==self.planner.submission_ids
                and all(json.loads(self.product_image.decision_json)['scope'][k]==json.loads(self.planner.case_input_json)[k]
                    for k in ('sku','country','channel'))))
            and (self.retrieved_planner is None or (type(self.retrieved_planner) is RetrievedPlannerSettings
                and self.planner is not None and self.corpus_retrieval is not None and not self.writes_enabled
                and self.policy is None and self.retrieved_planner.run_id!=self.planner.run_id))
            and (self.artifact is None or (type(self.artifact) is CampaignArtifactSettings
                and self.retrieved_planner is not None and self.product_image is not None
                and not self.artifact.enabled and not self.retrieved_planner.enabled))
            and (self.vertex is None or (type(self.vertex) is VertexCampaignSettings and self.vertex.enabled
                and self.vertex.profile.endpoint_authorized and self.planner is not None
                and self.corpus_retrieval is not None and self.product_image is not None
                and self.policy is None and not self.writes_enabled))
            and self.iap.expected_audience == self.manifest.expected_audience
            and (self.policy is None or self.policy.expected_audience == self.iap.expected_audience))


def config_from_wire(value):
    require(type(value) is dict)
    value=dict(value)
    value.setdefault('planner',None)
    value.setdefault('corpus',None)
    value.setdefault('embedding',None)
    value.setdefault('product_image',None)
    value.setdefault('retrieved_planner',None);value.setdefault('artifact',None)
    value.setdefault('vertex',None)
    require(set(value) == set(OperatorHostConfig.__dataclass_fields__))
    raw = dict(value)
    for key in ('manifest', 'policy'):
        if key == 'policy' and raw[key] is None: continue
        data = dict(raw[key])
        for name in ('effective_at', 'valid_until'): data[name] = datetime.fromisoformat(data[name])
        for name in ('principal_ids', 'grant_operator_ids', 'reviewer_principal_ids', 'review_revoker_ids', 'kinds'):
            if name in data:
                require(type(data[name]) is list); data[name] = tuple(data[name])
        raw[key] = (OperatorPublisherManifest if key == 'manifest' else ReviewOperatorPolicy)(**data)
    if raw['measurement'] is not None:raw['measurement']=MeasurementRuntimeSettings(**raw['measurement'])
    if raw['evaluation'] is not None:raw['evaluation']=InterpretationEvaluationSettings(**raw['evaluation'])
    if raw['planner'] is not None:
        data=dict(raw['planner']);require(type(data['submission_ids']) is list)
        data['submission_ids']=tuple(data['submission_ids']);raw['planner']=PlannerEvaluationSettings(**data)
    if raw['corpus'] is not None:raw['corpus']=CorpusPreparationSettings(**raw['corpus'])
    if raw['embedding'] is not None:raw['embedding']=CorpusEmbeddingSettings(**raw['embedding'])
    if raw['corpus_retrieval'] is not None:raw['corpus_retrieval']=CorpusRetrievalSettings(**raw['corpus_retrieval'])
    if raw['product_image'] is not None:raw['product_image']=ImageDerivationSettings(**raw['product_image'])
    if raw['retrieved_planner'] is not None:raw['retrieved_planner']=RetrievedPlannerSettings(**raw['retrieved_planner'])
    if raw['artifact'] is not None:raw['artifact']=CampaignArtifactSettings(**raw['artifact'])
    if raw['vertex'] is not None:
        data=dict(raw['vertex']);data['profile']=VisualProfile(**data['profile']);raw['vertex']=VertexCampaignSettings(**data)
    raw['iap'] = IapSettings(**raw['iap']); raw['operation'] = OperationSettings(**raw['operation'])
    return OperatorHostConfig(**raw)


def request_values(method, headers, parameters):
    require(method in VERTEX_METHODS|{'artifact_preflight','prepare', 'current', 'publish', 'recover', 'measurement_current','evaluation_run','evaluation_recover','planner_run','planner_recover','retrieval_measure','retrieval_recover','corpus_prepare','corpus_recover','embedding_run','embedding_recover','corpus_retrieval_run','corpus_retrieval_recover','product_image_prepare','product_image_recover'} and type(parameters) is dict)
    # Reuse the bounded IAP-header codec without accepting management parameters.
    management_request('recover_management', headers, {'workspace_id': 'operator', 'command_id': 'operator'})
    require(set(parameters) == ({'confirmed_sha256'} if method in {'publish', 'recover'} else set()))
    if parameters:
        require(type(parameters['confirmed_sha256']) is str
            and re.fullmatch(r'[0-9a-f]{64}', parameters['confirmed_sha256']))
    return dict(method=method, headers=dict(headers), parameters=dict(parameters))


class ProtectedOperatorHost(SharingOperationHost):
    def _receive_reply(self,method,command_id,remaining):
        if method not in {'corpus_retrieval_run','corpus_retrieval_recover'}:
            return super()._receive_reply(method,command_id,remaining)
        for _ in range(9):
            reply=receive(self._channel,remaining)
            if 'progress' not in reply:return reply
            require(set(reply)=={'id','progress'} and reply['id']==command_id)
            event=reply['progress']
            require(type(event) is dict and set(event)=={'phase','state','elapsed_seconds'}
                and event['phase'] in PHASES and event['state'] in {'STARTED','SUCCEEDED','STOPPED'}
                and type(event['elapsed_seconds']) in (int,float) and math.isfinite(event['elapsed_seconds'])
                and 0<=event['elapsed_seconds']<=600)
            # Only this validated projection crosses into serving logs; worker
            # stdout/stderr and arbitrary exception text remain discarded.
            logging.getLogger(__name__).warning(json.dumps(dict(event='private_retrieval_phase',**event)))
        require(False)
    def _valid_config(self, config): return type(config) is OperatorHostConfig
    @property
    def operation_settings(self): return self.config.operation
    def _enabled(self):
        if not self.settings.enabled or not self.config.enabled: raise SharingHostError('OPERATOR_HOST_DISABLED')
        if self.retired: raise SharingHostError('SHARING_HOST_RETIRED')
    def _command(self, fd): return [sys.executable, '-m', 'src.application.campaign_operator_worker', str(fd)]
    def _config_values(self):
        def wire(value):
            if isinstance(value, datetime): return value.isoformat()
            if isinstance(value, dict): return {k: wire(v) for k, v in value.items()}
            if isinstance(value, (tuple, list)): return [wire(v) for v in value]
            return value
        return wire(asdict(self.config))
    def _request_values(self, method, headers, parameters):
        if method in VERTEX_METHODS:require(self.config.vertex is not None and self.config.vertex.enabled)
        if method=='artifact_preflight':require(self.config.artifact is not None and not self.config.artifact.enabled)
        if method in {'product_image_prepare','product_image_recover'}:
            require(self.config.product_image is not None
                and (self.config.product_image.enabled or method=='product_image_recover'))
        if method in {'corpus_retrieval_run','corpus_retrieval_recover'}:
            require(self.config.corpus_retrieval is not None
                and (self.config.corpus_retrieval.enabled or method=='corpus_retrieval_recover'))
        if method in {'corpus_prepare','corpus_recover'}:
            require(self.config.corpus is not None and self.config.corpus.enabled)
        if method == 'publish' and not self.config.writes_enabled: raise SharingHostError('OPERATOR_WRITES_DISABLED')
        if method == 'prepare' and self.config.policy is None: raise SharingHostError('OPERATOR_PROPOSAL_UNCONFIGURED')
        if method in {'evaluation_run','evaluation_recover'}:
            require(self.config.evaluation is not None and self.config.evaluation.enabled)
        if method in {'planner_run','planner_recover','retrieval_measure','retrieval_recover'}:
            require(self.config.planner is not None and self.config.planner.enabled)
        values = request_values(method, headers, parameters)
        if method in {'publish', 'recover'}:
            review = getattr(self, '_review', None)
            require(review is not None and parameters['confirmed_sha256'] == review['command_sha256'])
        return values
    def _decode_result(self, method, value):
        require(type(value) is dict)
        if method in VERTEX_METHODS:return decode_vertex(method,value)
        if method=='artifact_preflight':
            require(set(value)=={'status','run_id','render_config_sha256','font_profile_sha256','crop_binding_id',
                'crop_record_sha256','source_manifest_sha256','retrieval_measurement_record_id','retrieval_outcome_sha256',
                'phase_seconds','model_calls','generation_authorized','qualifying','release_allowed'}
                and value['status']=='PREFLIGHT_PASSED_NOT_GENERATION_AUTHORITY'
                and type(value['model_calls']) is int and value['model_calls']==0
                and all(value[k] is False for k in ('generation_authorized','qualifying','release_allowed'))
                and type(value['crop_binding_id']) is str and re.fullmatch(r'eval_image_derivation_v1_[a-f0-9]{64}',value['crop_binding_id'])
                and type(value['phase_seconds']) is dict and set(value['phase_seconds'])=={'planner_sources','saved_crop','saved_retrieval'}
                and all(type(n) in (int,float) and math.isfinite(n) and 0<=n<=600 for n in value['phase_seconds'].values()))
            for k in ('render_config_sha256','font_profile_sha256','crop_record_sha256','source_manifest_sha256',
                    'retrieval_measurement_record_id','retrieval_outcome_sha256'):
                require(type(value[k]) is str and re.fullmatch('[a-f0-9]{64}',value[k]))
            return value
        if method in {'product_image_prepare','product_image_recover'}:
            require(set(value)=={'binding_id','record_sha256','parent_binding_id','content_sha256','generation',
                'width','height','claim_approval','generation_authorized','qualifying','release_allowed'}
                and all(value[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed'))
                and type(value['binding_id']) is str and re.fullmatch(IMAGE_PREFIX+'[a-f0-9]{64}',value['binding_id'])
                and type(value['parent_binding_id']) is str and re.fullmatch('eval_upload_binding_v1_[a-f0-9]{64}',value['parent_binding_id'])
                and type(value['generation']) is str and re.fullmatch('[1-9][0-9]{0,19}',value['generation']))
            for key in ('record_sha256','content_sha256'):
                require(type(value[key]) is str and re.fullmatch('[a-f0-9]{64}',value[key]))
            for key in ('width','height'):require(type(value[key]) is int and 0<value[key]<=20000)
            return value
        if method in {'corpus_retrieval_run','corpus_retrieval_recover'}:
            require(set(value)=={'run_id','case_id','measurement_record_id','outcome_sha256','query_embedding_record_id',
                'index_record_id','index_sha256','corpus_sha256','native_ranking_job_id','observation_sha256','measurement_sha256',
                'rankings','metrics','label_authority','semantic_reranking','image_embedding_status',
                'claim_approval','generation_authorized','qualifying','release_allowed'})
            require(all(value[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed'))
                and value['semantic_reranking']==value['image_embedding_status']=='NOT_EXECUTED'
                and value['label_authority']=='SOURCE_REVIEW_CALIBRATION_NOT_INDEPENDENT_QUALIFICATION')
            for k in ('measurement_record_id','outcome_sha256','query_embedding_record_id','index_record_id','index_sha256',
                    'corpus_sha256','observation_sha256','measurement_sha256'):
                require(type(value[k]) is str and re.fullmatch('[0-9a-f]{64}',value[k]))
            require(type(value['native_ranking_job_id']) is str and re.fullmatch('private_retrieval_rank_v1_[0-9a-f]{64}',value['native_ranking_job_id']))
            require(self.config.corpus_retrieval is not None)
            case=json.loads(self.config.corpus_retrieval.case_json);grades={x['chunk_id']:x['grade'] for x in case['labels']}
            require(type(value['rankings']) is dict and set(value['rankings'])=={'lexical','vector','rrf'}
                and type(value['metrics']) is dict and set(value['metrics'])==set(value['rankings']))
            for name,ids in value['rankings'].items():
                require(type(ids) is list and len(set(ids))==len(ids) and set(ids)<=set(grades)
                    and (name=='lexical' or set(ids)==set(grades)))
                require(value['metrics'][name]==[ranking_metrics(tuple(ids),grades,k) for k in sorted({1,min(3,len(grades))})])
            return value
        if method in {'embedding_run','embedding_recover'}:
            require(set(value)=={'run_id','corpus_binding_id','corpus_sha256','text_embeddings','index',
                'image_embedding_status','retrieval_quality','claim_approval','generation_authorized','qualifying','release_allowed'})
            require(all(value[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed'))
                and value['image_embedding_status']=='NOT_EXECUTED' and value['retrieval_quality']=='NOT_MEASURED')
            rows=value['text_embeddings'];index=value['index']
            require(type(rows) is list and 0<len(rows)<=24 and type(index) is dict
                and set(index)=={'index_record_id','index_sha256','corpus_sha256','chunk_count','dimension',
                    'claim_approval','generation_authorized','qualifying','release_allowed'}
                and index['corpus_sha256']==value['corpus_sha256']
                and type(index['chunk_count']) is int and index['chunk_count']==len(rows)
                and type(index['dimension']) is int and index['dimension']==768
                and all(index[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed')))
            digests=[value['corpus_sha256'],index['index_record_id'],index['index_sha256']]
            for row in rows:
                require(type(row) is dict and set(row)=={'record_id','outcome_sha256','response_sha256',
                    'dimension','input_tokens','returned_model_id','qualifying','release_allowed'}
                    and row['qualifying'] is False and row['release_allowed'] is False
                    and type(row['dimension']) is int and row['dimension']==768
                    and (row['input_tokens'] is None or type(row['input_tokens']) is int and 0<=row['input_tokens']<2**63)
                    and row['returned_model_id'] in (None,'gemini-embedding-001'))
                digests.extend(row[k] for k in ('record_id','outcome_sha256','response_sha256'))
            require(len({r['record_id'] for r in rows})==len(rows)
                and all(type(d) is str and re.fullmatch(r'[a-f0-9]{64}',d) for d in digests)
                and type(value['corpus_binding_id']) is str
                and re.fullmatch(CORPUS_PREFIX+r'[a-f0-9]{64}',value['corpus_binding_id']))
            return value
        if method in {'corpus_prepare','corpus_recover'}:
            require(set(value)=={'binding_id','corpus_sha256','selection_sha256','run_id','source_count','chunk_count',
                'image_count','text_bytes','claim_approval','generation_authorized','qualifying','release_allowed'})
            require(all(value[k] is False for k in ('claim_approval','generation_authorized','qualifying','release_allowed')))
            for key in ('corpus_sha256','selection_sha256'):
                require(type(value[key]) is str and re.fullmatch(r'[a-f0-9]{64}',value[key]))
            require(type(value['binding_id']) is str and re.fullmatch(CORPUS_PREFIX+r'[a-f0-9]{64}',value['binding_id']))
            for key,limit in (('source_count',16),('chunk_count',256),('image_count',16),('text_bytes',24000)):
                require(type(value[key]) is int and 0<=value[key]<=limit)
            require(value['source_count']>=value['image_count']>0)
            return value
        if method in {'retrieval_measure','retrieval_recover'}:
            require(set(value)=={'record_id','stage_record_id','coverage_sha256','corpus_status','counts',
                'native_job_id','qualifying','release_allowed','run_id','case_id'})
            require(value['qualifying'] is False and value['release_allowed'] is False)
            for k in ('record_id','stage_record_id','coverage_sha256'):
                require(type(value[k]) is str and re.fullmatch(r'[a-f0-9]{64}',value[k]))
            require(value['corpus_status'] in {'CORPUS_EMPTY','TEXT_VECTORS_MISSING',
                'CORPUS_PRESENT_REQUIRES_ELIGIBILITY_AND_RANKING'})
            require(type(value['counts']) is dict and set(value['counts'])=={'knowledge_chunks','text_vectors','image_vectors'}
                and all(type(v) is int and v>=0 for v in value['counts'].values()))
            require((value['corpus_status']=='CORPUS_EMPTY')==(not any(value['counts'].values())))
            require(type(value['native_job_id']) is str and re.fullmatch(r'retrieval_coverage_[a-f0-9]{32}',value['native_job_id']))
            return value
        if method in {'planner_run','planner_recover'}:
            require(set(value)=={'stage_record_id','response_sha256','returned_model_id','usage','scoring_record_id','checks','qualifying','release_allowed','run_id','case_id'})
            require(value['qualifying'] is False and value['release_allowed'] is False)
            for k in ('stage_record_id','response_sha256','scoring_record_id'):
                require(type(value[k]) is str and re.fullmatch(r'[a-f0-9]{64}',value[k]))
            require(type(value['returned_model_id']) is str and value['returned_model_id']=='gemini-3.5-flash')
            usage=value['usage']
            require(type(usage) is dict and set(usage)=={'input_tokens','output_tokens','cached_tokens','thinking_tokens'}
                and all(v is None or type(v) is int and v>=0 for v in usage.values()))
            require(type(value['checks']) is dict and set(value['checks'])=={'schema_and_enum_projection','fresh_source_check',
                'full_47_check_evaluation','retrieval_measurement','rendered_artifact_evaluation'})
            projection=value['checks']['schema_and_enum_projection']
            require(type(projection) is dict and set(projection)=={'status','projection_sha256'}
                and projection['status'] in {'PASS','FAIL'} and (projection['projection_sha256'] is None
                    if projection['status']=='FAIL' else type(projection['projection_sha256']) is str
                    and re.fullmatch(r'[a-f0-9]{64}',projection['projection_sha256'])))
            require(value['checks']['fresh_source_check'] in {'PASS','UNAVAILABLE'})
            require(type(value['checks']) is dict and value['checks'].get('full_47_check_evaluation')=='UNAVAILABLE'
                and value['checks'].get('retrieval_measurement')=='UNAVAILABLE'
                and value['checks'].get('rendered_artifact_evaluation')=='UNAVAILABLE')
            return value
        if method in {'evaluation_run','evaluation_recover'}:
            require(set(value)=={'assessment_record_id','case_id','run_id','stage_record_id','stage_response_sha256','stage_returned_model_id','stage_usage','evaluator_receipts','check_count','observation_sha256','semantic_decisions','qualifying','release_allowed'})
            require(value['qualifying'] is False and value['release_allowed'] is False and value['check_count']==47)
            for key in ('assessment_record_id','stage_record_id','stage_response_sha256','observation_sha256'):
                require(type(value[key]) is str and re.fullmatch(r'[a-f0-9]{64}',value[key]))
            require(type(value['evaluator_receipts']) is list and len(value['evaluator_receipts'])==3)
            require(type(value['semantic_decisions']) is list and len(value['semantic_decisions'])==3 and all(v in {'PASS','BLOCK','CLARIFY','NEEDS_REVIEW'} for v in value['semantic_decisions']))
            return value
        if method=='measurement_current':
            require(set(value)=={'runtime_installed','workspace_revision','membership_revision','publisher_verified',
                'measurement_writes_enabled','approval_writes_enabled','qualifying_evaluation_configured','release_allowed'})
            require(value['runtime_installed'] is True and value['publisher_verified'] is True
                and value['measurement_writes_enabled'] is False and value['approval_writes_enabled'] is False
                and value['release_allowed'] is False and type(value['qualifying_evaluation_configured']) is bool)
            for key in ('workspace_revision','membership_revision'):require(type(value[key]) is int and value[key]>=1)
            return value
        expected = {'command_sha256', 'policy_sha256', 'proposed_record_sha256', 'native_job_id'} if method == 'prepare' else (
            {'policy_sha256', 'record_sha256', 'revision', 'approval_verified'} if method == 'current' else
            {'native_job_id', 'native_status', 'approval_verified'})
        if method in {'publish', 'recover'} and 'current_record_sha256' in value: expected |= {'current_record_sha256'}
        require(set(value) == expected)
        for name, data in value.items():
            if name.endswith('sha256'): require(type(data) is str and re.fullmatch(r'[0-9a-f]{64}', data))
            elif name == 'approval_verified': require(data is False)
            elif name == 'revision': require(type(data) is int and 1 <= data <= 64)
            elif name == 'native_status': require(data in {'COMMITTED', 'NOT_COMMITTED', 'UNKNOWN'})
            else: require(type(data) is str and re.fullmatch(r'[A-Za-z0-9_-]{1,255}', data))
        require('current_record_sha256' not in value or value['native_status'] == 'COMMITTED')
        return value
    def _validate_result_scope(self, method, parameters, result):
        if method in VERTEX_METHODS:
            cfg=self.config.vertex
            require(cfg is not None and result['run_id']==cfg.run_id and result['case_id']==cfg.case_id
                and result['crop_binding_id']==cfg.crop_binding_id)
            if result['artifact'] is not None:
                m=result['artifact']['measurements']['measurements'];render=json.loads(cfg.render_config_json)
                require(m['width']==render['width'] and m['height']==render['height'])
        elif method=='artifact_preflight':
            require(self.config.artifact is not None and result['run_id']==self.config.artifact.run_id
                and result['render_config_sha256']==self.config.artifact.render_config_sha256
                and result['source_manifest_sha256']==self.config.planner.binding_id.removeprefix('eval_upload_binding_v1_'))
        elif method in {'product_image_prepare','product_image_recover'}:
            require(self.config.product_image is not None)
            decision=json.loads(self.config.product_image.decision_json);box=decision['crop_box']
            require(result['parent_binding_id']==decision['source_binding_id']
                and result['content_sha256']==decision['output_sha256']
                and result['width']==box[2]-box[0] and result['height']==box[3]-box[1])
        elif method in {'corpus_retrieval_run','corpus_retrieval_recover'}:
            cfg=self.config.corpus_retrieval
            require(cfg is not None and result['run_id']==cfg.run_id and result['case_id']==cfg.query_id
                and result['index_record_id']==cfg.index_record_id and result['index_sha256']==cfg.index_sha256
                and result['corpus_sha256']==json.loads(cfg.case_json)['corpus_sha256'])
        elif method in {'embedding_run','embedding_recover'}:
            require(self.config.embedding is not None
                and result['run_id']==self.config.embedding.run_id
                and result['corpus_binding_id']==self.config.embedding.binding_id
                and result['corpus_sha256']==self.config.embedding.corpus_sha256)
        elif method in {'corpus_prepare','corpus_recover'}:
            require(self.config.corpus is not None and result['run_id']==self.config.corpus.run_id
                and result['selection_sha256']==self.config.corpus.selection_sha256)
        elif method in {'planner_run','planner_recover','retrieval_measure','retrieval_recover'}:
            require(self.config.planner is not None and result['run_id']==self.config.planner.run_id
                and result['case_id']==json.loads(self.config.planner.case_input_json)['case_id'])
        elif method in {'evaluation_run','evaluation_recover'}:
            require(self.config.evaluation is not None and result['case_id']==self.config.evaluation.case_id and result['run_id']==self.config.evaluation.run_id)
        elif method == 'prepare':
            require(self.config.policy is not None and result['policy_sha256'] == self.config.policy.sha256)
            self._review = dict(result)
        elif method in {'publish', 'recover'}:
            review = getattr(self, '_review', None)
            require(review is not None and parameters['confirmed_sha256'] == review['command_sha256']
                and result['native_job_id'] == review['native_job_id'])
    def prepare(self, headers): return self._invoke('prepare', headers, {}, None)
    def measurement_current(self, headers): return self._invoke('measurement_current', headers, {}, None)
    def current(self, headers): return self._invoke('current', headers, {}, None)
    def publish(self, headers, *, confirmed_sha256): return self._invoke('publish', headers, dict(confirmed_sha256=confirmed_sha256), None)
    def recover(self, headers, *, confirmed_sha256): return self._invoke('recover', headers, dict(confirmed_sha256=confirmed_sha256), None)

    def evaluation_run(self,headers): return self._invoke('evaluation_run',headers,{},None)
    def evaluation_recover(self,headers): return self._invoke('evaluation_recover',headers,{},None)

    def planner_run(self,headers): return self._invoke('planner_run',headers,{},None)
    def planner_recover(self,headers): return self._invoke('planner_recover',headers,{},None)
    def retrieval_measure(self,headers): return self._invoke('retrieval_measure',headers,{},None)
    def retrieval_recover(self,headers): return self._invoke('retrieval_recover',headers,{},None)
    def corpus_prepare(self,headers): return self._invoke('corpus_prepare',headers,{},None)
    def corpus_recover(self,headers): return self._invoke('corpus_recover',headers,{},None)
    def embedding_run(self,headers): return self._invoke('embedding_run',headers,{},None)
    def embedding_recover(self,headers): return self._invoke('embedding_recover',headers,{},None)
    def corpus_retrieval_run(self,headers): return self._invoke('corpus_retrieval_run',headers,{},None)
    def corpus_retrieval_recover(self,headers): return self._invoke('corpus_retrieval_recover',headers,{},None)
    def product_image_prepare(self,headers): return self._invoke('product_image_prepare',headers,{},None)
    def artifact_preflight(self,headers):return self._invoke('artifact_preflight',headers,{},None)
    def vertex_visual_run(self,headers):return self._invoke('vertex_visual_run',headers,{},None)
    def vertex_visual_recover(self,headers):return self._invoke('vertex_visual_recover',headers,{},None)
    def vertex_banner_run(self,headers):return self._invoke('vertex_banner_run',headers,{},None)
    def vertex_banner_recover(self,headers):return self._invoke('vertex_banner_recover',headers,{},None)
    def product_image_recover(self,headers): return self._invoke('product_image_recover',headers,{},None)
