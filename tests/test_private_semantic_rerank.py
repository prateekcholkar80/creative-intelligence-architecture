from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone,timedelta
import hashlib,json,time
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_evaluator_observer import EvaluatorProviderResponse,ModelResultObservation
from src.application.operation_budget import worker_operation_scope
from src.retrieval.private_semantic_rerank import *
from tests.test_private_text_embeddings import ledger


def fixture():
    chunks=[]
    for text in ('Use four drops. Wait three minutes.','Ingredients include botanical oils.','Apply serum before the face oil.'):
        h=hashlib.sha256(text.encode()).hexdigest();chunks.append(dict(chunk_id=sha(['chunk',h]),provenance=dict(text_sha256=h),text=text))
    scope=dict(workspace_id='workspace',conversation_id='conversation',request_id='request',sku='CI-SKU-0022',country='US',channel='META')
    manifest=dict(version=CORPUS_VERSION,use='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY',scope=scope,principal_id='owner',
        text_chunks=chunks,claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)
    capture=UploadCorpusCapture(canonical(manifest),(),datetime.now(timezone.utc)+timedelta(seconds=120))
    query='How many drops should I apply and when should I use the oil?'
    case=dict(version='private-retrieval-relevance-case-v1',case_id='application',
        corpus_sha256=capture.sha256,index_sha256='f'*64,
        query_text_sha256=hashlib.sha256(query.encode()).hexdigest(),
        scope=dict(sku='CI-SKU-0022',country='US',channel='META'),
        label_authority='SOURCE_REVIEW_CALIBRATION_NOT_INDEPENDENT_QUALIFICATION',
        labels=[dict(chunk_id=c['chunk_id'],text_sha256=c['provenance']['text_sha256'],grade=g,rationale='Reviewed relevance.') for c,g in zip(chunks,(3,0,1))])
    parent=dict(measurement_record_id='a'*64,outcome_sha256='b'*64,observation_sha256='c'*64,measurement_sha256='d'*64,
        corpus_sha256=capture.sha256,index_sha256='f'*64,rankings=dict(rrf=[c['chunk_id'] for c in chunks]),
        claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
    request=prepare_rerank(capture,'eval_corpus_binding_v1_'+'e'*64,parent,query,case,'semantic-run','application')
    rows=[dict(chunk_id=c['chunk_id'],grade=g,source_quote=c['text'] if g else '',reason='Source relevance.') for c,g in zip(chunks,(3,0,1))]
    return request,case,rows


def response(rows):
    text=canonical(dict(results=rows));return EvaluatorProviderResponse(text,ModelResultObservation('SUCCEEDED',
        returned_model_id='gemini-3.5-flash',response_sha256=hashlib.sha256(text.encode()).hexdigest(),finish_reason='STOP',safety_outcome='PASSED'))


class SemanticRerankTests(TestCase):
    def work(self,fn):
        with worker_operation_scope(time.monotonic()+60):fn()

    def test_gold_labels_never_reach_model_and_rank_ties_preserve_baseline(self):
        r,case,rows=fixture();self.assertNotIn('scoring_case',r.content);self.assertNotIn('labels',r.content)
        result=project_response(r,response(rows).text);self.assertEqual(result['ranked_ids'],[rows[i]['chunk_id'] for i in (0,2,1)])
        for row in rows:row.update(grade=0,source_quote='')
        self.assertEqual(project_response(r,response(rows).text)['ranked_ids'],json.loads(r.input_json)['baseline_ids'])

    def test_missing_duplicate_foreign_id_invented_quote_and_boolean_grade_deny(self):
        r,_,original=fixture()
        for fault in ('missing','duplicate','foreign','quote','grade'):
            rows=deepcopy(original)
            if fault=='missing':rows.pop()
            elif fault=='duplicate':rows[1]=rows[0]
            elif fault=='foreign':rows[0]['chunk_id']='f'*64
            elif fault=='quote':rows[0]['source_quote']='Fabricated trial result'
            else:rows[0]['grade']=True
            with self.assertRaises(ValueError):project_response(r,response(rows).text)

    def test_source_profile_and_label_drift_do_not_mint_another_slot(self):
        r,_,_=fixture();data=json.loads(r.input_json);data['query']='Changed question'
        changed=replace(r,input_json=canonical(data));self.assertEqual(r.intent['record_id'],changed.intent['record_id'])
        self.assertNotEqual(r.intent['input_sha256'],changed.intent['input_sha256'])
        with self.assertRaises(ValueError):RerankProfile(model_id='latest')
        data=json.loads(r.input_json);data['chunks'][0]['text']='tampered'
        with self.assertRaises(ValueError):replace(r,input_json=canonical(data))

    def test_case_scope_index_and_grade_mismatch_deny_before_dispatch(self):
        r,case,_=fixture()
        data=json.loads(r.input_json)
        capture=UploadCorpusCapture(canonical(dict(version=CORPUS_VERSION,
            use='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY',scope=data['scope'],principal_id=data['principal_id'],
            text_chunks=[dict(chunk_id=c['chunk_id'],text=c['text'],provenance=dict(text_sha256=c['text_sha256']))
                for c in data['chunks']],claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)),
            (),datetime.now(timezone.utc)+timedelta(seconds=120))
        parent=dict(data['parent'],corpus_sha256=capture.sha256,index_sha256=case['index_sha256'],
            rankings=dict(rrf=data['baseline_ids']),claim_approval=False,generation_authorized=False,
            qualifying=False,release_allowed=False)
        for field,value in (('version','unreviewed'),('case_id','other'),('label_authority','APPROVED'),
            ('scope',dict(sku='CI-SKU-0023',country='US',channel='META')),('index_sha256','0'*64)):
            changed=deepcopy(case);changed[field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):
                prepare_rerank(capture,data['corpus_binding_id'],parent,data['query'],changed,r.run_id,r.query_id)
        changed=deepcopy(case);changed['labels'][0]['grade']=True
        with self.assertRaises(ValueError):
            prepare_rerank(capture,data['corpus_binding_id'],parent,data['query'],changed,r.run_id,r.query_id)

    def test_one_real_callback_cold_recovery_and_measured_improvement(self):
        def run():
            r,case,rows=fixture();store,_,_=ledger();transport=Mock(return_value=response(rows));guard=Mock(return_value=True)
            first=DurableSemanticRerank(store,transport,guard,enabled=True).execute(r,case)
            cold=DurableSemanticRerank(store,transport,guard).execute(r,case,recover_only=True)
            self.assertEqual(first,cold);transport.assert_called_once_with(r)
            self.assertEqual(first['metrics']['semantic'][-1]['ndcg'],1)
            self.assertGreater(first['metrics']['semantic'][-1]['ndcg'],first['metrics']['rrf'][-1]['ndcg'])
            self.assertTrue(all(v is None for v in first['usage'].values()));self.assertFalse(first['qualifying'])
        self.work(run)

    def test_uncertain_callback_never_replays(self):
        def run():
            r,case,_=fixture();store,_,_=ledger();transport=Mock(side_effect=TimeoutError('unknown'));guard=Mock(return_value=True)
            for _ in range(2):
                with self.assertRaisesRegex(ValueError,'OUTCOME_UNUSABLE'):
                    DurableSemanticRerank(store,transport,guard,enabled=True).execute(r,case)
            transport.assert_called_once()
        self.work(run)

    def test_lost_commit_and_pending_publication_recover_without_redispatch(self):
        for committed in (False,True):
            def run():
                r,case,rows=fixture();store,_,_=ledger();publish=store.publish.side_effect
                def lost(*a):
                    if committed:publish(*a)
                    raise TimeoutError('publication loss')
                store.publish.side_effect=lost;transport=Mock(return_value=response(rows));guard=Mock(return_value=True)
                engine=DurableSemanticRerank(store,transport,guard,enabled=True)
                with self.assertRaises(TimeoutError):engine.execute(r,case)
                store.publish.side_effect=publish
                engine=DurableSemanticRerank(store,transport,guard) if committed else engine
                self.assertFalse(engine.execute(r,case,recover_only=True)['release_allowed']);transport.assert_called_once()
            self.work(run)

    def test_missing_recovery_stale_access_or_changed_labels_never_dispatch(self):
        def run():
            r,case,_=fixture();store,_,_=ledger();transport=Mock();guard=Mock(return_value=True)
            engine=DurableSemanticRerank(store,transport,guard)
            with self.assertRaisesRegex(ValueError,'NO_DISPATCH'):engine.execute(r,case,recover_only=True)
            guard.return_value=False
            with self.assertRaisesRegex(ValueError,'SOURCE_UNVERIFIED'):engine.execute(r,case)
            changed=deepcopy(case);changed['labels'][0]['grade']=1
            with self.assertRaisesRegex(ValueError,'CASE_CHANGED'):engine.execute(r,changed)
            transport.assert_not_called();store.reserve.assert_not_called()
        self.work(run)

    def test_direct_request_with_changed_index_or_case_denies_before_reservation(self):
        def run():
            r,case,_=fixture();store,_,_=ledger();transport=Mock();guard=Mock(return_value=True)
            for field,value in (('index_sha256','0'*64),('case_id','other'),('label_authority','APPROVED')):
                changed=deepcopy(case);changed[field]=value
                payload=json.loads(r.input_json);payload['scoring_case_sha256']=sha(changed)
                forged=replace(r,input_json=canonical(payload))
                with self.subTest(field=field),self.assertRaises(ValueError):
                    DurableSemanticRerank(store,transport,guard,enabled=True).execute(forged,changed)
            transport.assert_not_called();store.reserve.assert_not_called()
        self.work(run)

    def test_source_revocation_after_provider_preserves_outcome_but_withholds_ranks(self):
        def run():
            r,case,rows=fixture();store,_,outcomes=ledger();guard=Mock(side_effect=[True,True,False,True])
            transport=Mock(return_value=response(rows))
            with self.assertRaisesRegex(ValueError,'OUTCOME_UNUSABLE'):
                DurableSemanticRerank(store,transport,guard,enabled=True).execute(r,case)
            self.assertEqual(outcomes[r.intent['record_id']]['fresh_source_check'],'UNAVAILABLE');transport.assert_called_once()
        self.work(run)
