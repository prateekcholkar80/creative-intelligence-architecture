from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone,timedelta
import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock

from src.application.campaign_retrieved_planner import prepare_retrieved_planner_evaluation
from src.application.campaign_planner_evaluation import prepare_planner_evaluation
from src.application.campaign_evaluation_uploads import EvaluationUploadInputs
from src.application.campaign_grounded_planner_definition import GroundedPlannerDefinition
from src.application.campaign_evaluator_store import canonical,sha
from src.application.operation_budget import worker_operation_scope
from src.retrieval.private_semantic_rerank import prepare_rerank,DurableSemanticRerank
from src.retrieval.upload_corpus import UploadCorpusCapture
from tests.test_private_semantic_rerank import fixture,response
from tests.test_private_text_embeddings import ledger
from tests.test_prompt_contract import snapshot
from tests.test_campaign_planner_projection import options


class RetrievedPlannerTests(TestCase):
    def fixture(self):
        old,case,rows=fixture();data=json.loads(old.input_json);now=datetime.now(timezone.utc)
        raw=b'original fixture image';images=(('asset','123','image/webp',raw),)
        pins=[dict(asset_id='asset',generation='123',mime_type='image/webp',byte_count=len(raw),sha256=hashlib.sha256(raw).hexdigest())]
        source=dict(principal_id='owner',context=dict(scope=data['scope'],policy_sha256='a'*64),image_pins=pins)
        uploads=EvaluationUploadInputs(canonical(source),images,now,now,now+timedelta(seconds=120))
        manifest=dict(version='private-upload-retrieval-corpus-v1',scope=data['scope'],principal_id='owner',
            policy_sha256='a'*64,image_manifest_sha256=uploads.sha256,image_binding_id='eval_upload_binding_v1_'+uploads.sha256,
            image_pins=pins,text_chunks=[dict(chunk_id=c['chunk_id'],text=c['text'],provenance=dict(text_sha256=c['text_sha256'])) for c in data['chunks']],
            use='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY',claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)
        capture=UploadCorpusCapture(canonical(manifest),images,now+timedelta(seconds=120));case['corpus_sha256']=capture.sha256
        parent=dict(data['parent'],corpus_sha256=capture.sha256,rankings=dict(rrf=data['baseline_ids']),
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
        request=prepare_rerank(capture,data['corpus_binding_id'],parent,data['query'],case,'rerank-pilot','application')
        store,_,_=ledger();transport=Mock(return_value=response(rows));guard=Mock(return_value=True)
        reranker=DurableSemanticRerank(store,transport,guard,enabled=True)
        base=snapshot(model_id='gemini-3.5-flash')
        snap=GroundedPlannerDefinition(base.output_schema_json).snapshot(base,vertex_prompt_id='999',vertex_version_id='1')
        args=dict(run_id='retrieved-campaign-v1',case_input=dict(case_id='identity-pilot',case_sha256='c'*64,
            stage='GENERATION',sku='CI-SKU-0022',country='US',channel='META',turns=[dict(role='USER',text='Create a product campaign.')]),
            snapshot=snap,registry_record_digest='d'*64,uploads=uploads,binding_id=manifest['image_binding_id'],options_json=options())
        return dict(corpus=capture,rerank_request=request,rerank_case=case,reranker=reranker,checked_at=now,**args),transport,guard,store

    def test_recovered_ranks_and_exact_source_text_reach_registered_planner(self):
        with worker_operation_scope(time.monotonic()+60):
            args,transport,_,_=self.fixture();args['reranker'].execute(args['rerank_request'],args['rerank_case'])
            prepared=prepare_retrieved_planner_evaluation(**args);intent=json.loads(prepared.intent_json)
            evidence=intent['retrieval_evidence'];selected=evidence['selected_chunks']
            self.assertEqual([c['rank'] for c in selected],[1,2,3])
            self.assertEqual(selected[1]['text'],'Apply serum before the face oil.')
            self.assertIn('MEASURED_PRIVATE_CALIBRATION',intent['content'])
            self.assertNotIn('Reviewed relevance.',intent['content'])
            self.assertTrue(all(c['approved_copy_source'] is False for c in selected))
            self.assertFalse(intent['qualifying']);self.assertFalse(intent['release_allowed'])
            cold=prepare_retrieved_planner_evaluation(**args)
            self.assertEqual(cold,prepared);transport.assert_called_once()

    def test_missing_or_revoked_reranking_never_dispatches_during_planner_preparation(self):
        with worker_operation_scope(time.monotonic()+60):
            args,transport,guard,store=self.fixture()
            with self.assertRaises(ValueError):prepare_retrieved_planner_evaluation(**args)
            transport.assert_not_called();store.reserve.assert_not_called()
            args['reranker'].execute(args['rerank_request'],args['rerank_case']);guard.return_value=False
            with self.assertRaises(ValueError):prepare_retrieved_planner_evaluation(**args)
            transport.assert_called_once()

    def test_expired_wrong_scope_image_policy_or_text_cannot_join(self):
        with worker_operation_scope(time.monotonic()+60):
            for fault in ('expired','scope','image','policy','text','owner'):
                args,transport,_,_=self.fixture();capture=args['corpus'];value=json.loads(capture.manifest_json)
                if fault=='expired':args['checked_at']=capture.valid_until
                elif fault=='scope':value['scope']['sku']='CI-SKU-0023'
                elif fault=='image':value['image_manifest_sha256']='0'*64
                elif fault=='policy':value['policy_sha256']='0'*64
                elif fault=='text':value['text_chunks'][0]['text']='Injected replacement'
                else:value['principal_id']='another-owner'
                if fault!='expired':args['corpus']=replace(capture,manifest_json=canonical(value))
                with self.subTest(fault=fault),self.assertRaises(ValueError):prepare_retrieved_planner_evaluation(**args)
                transport.assert_not_called()

    def test_retrieval_changes_content_but_does_not_remint_original_planner_slot(self):
        with worker_operation_scope(time.monotonic()+60):
            args,_,_,_=self.fixture();args['reranker'].execute(args['rerank_request'],args['rerank_case'])
            new=json.loads(prepare_retrieved_planner_evaluation(**args).intent_json)
            base_args={k:v for k,v in args.items() if k not in ('corpus','rerank_request','rerank_case','reranker','checked_at')}
            old=json.loads(prepare_planner_evaluation(**base_args).intent_json)
            self.assertEqual(new['record_id'],old['record_id'])
            self.assertNotEqual(new['content'],old['content'])
            # A distinct new campaign run is required; existing native slots
            # reject changed intent rather than replacing the saved pilot.
            base_args['run_id']='another-campaign-v1'
            self.assertNotEqual(new['record_id'],json.loads(prepare_planner_evaluation(**base_args).intent_json)['record_id'])
