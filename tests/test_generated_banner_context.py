import copy
import json
import unittest
from unittest.mock import Mock, patch

from src.generators.campaign_brief_generator import (
    assemble_brief, creative_options, generate_grounded_brief, render_brief, select_creative,
)
from src.generators.embeddings_generator import record_identity
from src.generators.generated_banner_embeddings import image_source
from src.retrieval.evidence_packet import evidence_index, retrieve_evidence_packet
from src.retrieval.generated_banner import retrieve_generated_banner
from src.retrieval.generated_banner_context import (
    ROLE, USAGE, build_visual_context, recheck_generated_references, retrieve_generated_references, visual_query,
)
from src.retrieval.generated_banner_search import CONFIG, embedding_config, search_generated_banners, validate_query
from src.retrieval.request import CHANNELS, OBJECTIVES, VISUAL_REFERENCES, RetrievalRequest
from tests import test_evidence_packet, test_register_generated_banner

MODULE = 'src.retrieval.generated_banner_context.'
BRIEF = 'src.generators.campaign_brief_generator.'


class GeneratedBannerContextTests(unittest.TestCase):
    def setUp(self):
        fixture=test_register_generated_banner.RegisterGeneratedBannerTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.bundle=fixture.bundle;self.request=fixture.request;self.draft=fixture.brief['model_selection']
        self.config=json.loads(CONFIG.read_text());self.raw=fixture.preview.png
        source=image_source(fixture.row,self.raw);identity=record_identity(source,embedding_config(self.config))
        self.row=dict(fixture.row,embedding_id=identity['embedding_id'],vector=[0.1]*512,
            model_id=self.config['image_model'],model_location=self.config['location'],dimension=512,task_type='IMAGE',
            embedding_created_at=fixture.row['created_at'],retrieval_status=ROLE,usage_scope=USAGE)
        self.row.update(review_id='REV-test-approved',review_revision=2,review_status='APPROVED_FOR_POC',
                        review_approval_scope='POC_VISUAL_REUSE_ONLY')
        self.client=Mock();self.client.query.return_value.result.return_value=[self.row]
        self.gcs=Mock();blob=self.gcs.bucket.return_value.blob.return_value
        blob.generation=int(self.row['gcs_generation']);blob.content_type='image/png';blob.size=len(self.raw)
        blob.download_as_bytes.return_value=self.raw
        self.metadata,_=retrieve_generated_banner(self.client,self.gcs,self.request.canonical_sku,
                                                   self.row['asset_id'],embedding_config(self.config))
        selection=dict(asset_id=self.row['asset_id'],sku=self.request.canonical_sku,embedding_id=self.row['embedding_id'],
            review_id=self.row['review_id'],review_revision=self.row['review_revision'],
            gcs_uri=self.row['gcs_uri'],gcs_generation=self.row['gcs_generation'],sha256=self.row['sha256'],
            knowledge_snapshot_id=self.row['knowledge_snapshot_id'],distance=0.25)
        with (patch('src.retrieval.generated_banner_search.read_ranked_banners',return_value=[selection]),
              patch('src.retrieval.generated_banner_search.retrieve_generated_banner',return_value=(self.metadata,self.raw))):
            self.packet,self.images=search_generated_banners(Mock(),Mock(),Mock(return_value=[0.1]*512),
                self.request.canonical_sku,visual_query(self.request),self.config)
        self.model_config=dict(model_id='gemini-2.5-flash',location='asia-south1',temperature=0.2,seed=42,max_output_tokens=4096)

    def attach(self):
        section=build_visual_context(self.bundle.context,self.packet,self.images,self.config)
        self.bundle.context['generated_banner_references']=section
        for metadata in section['results']:
            self.bundle.context['image_manifest'].append(copy.deepcopy(metadata))
            self.bundle.image_bytes[metadata['asset_id']]=self.raw
        self.bundle.context['evidence_index']=evidence_index(self.bundle.context)
        return section

    def test_request_query_is_bounded_for_all_preferences_and_does_not_import_audience_claims(self):
        for channel in CHANNELS:
            for objective in OBJECTIVES:
                for style in VISUAL_REFERENCES:
                    request=RetrievalRequest('CI-SKU-0001',objective,'Untrusted 50% wrinkle reduction',channel,style)
                    text=visual_query(request)
                    self.assertEqual(validate_query(request.canonical_sku,text,self.config),text)
                    self.assertNotIn('wrinkle',text);self.assertNotIn('50%',text)
        self.assertIn('off-white',visual_query(self.request))
        request=RetrievalRequest('CI-SKU-0001','AWARENESS','Shoppers','EMAIL','NONE')
        self.assertIn('email',visual_query(request));self.assertNotIn('serif',visual_query(request))

    def test_parent_copy_claims_and_fake_metrics_are_excluded_and_fresh_authority_is_unchanged(self):
        self.images[0][0].update(copy=dict(headline='New benefit'),approved_claim_ids=['fake-approval'],
                                  performance=dict(roas=99),evidence_references=['fake-parent-claim'])
        section=self.attach();metadata=section['results'][0]
        for key in ('copy','approved_claim_ids','performance'):
            self.assertNotIn(key,metadata);self.assertNotIn(key,section)
        self.assertNotIn('fake-parent-claim',json.dumps(section,default=str))
        self.assertEqual(self.bundle.context['generation_constraints']['allowed_identity_wording'],['Source product'])
        self.assertFalse(self.bundle.context['generation_constraints']['benefit_claims_authorized'])
        self.assertIn('generated_campaign_assets',metadata['evidence_references'][0])
        self.assertEqual(metadata['image_role'],ROLE)

    def test_cross_sku_stale_snapshot_and_original_image_or_vector_changes_are_rejected(self):
        for key,value in (('canonical_sku','CI-SKU-0002'),('snapshot_id','stale'),('status','REJECTED'),
                          ('status','PENDING_REVIEW'),('approval_scope','MANUFACTURER_APPROVED'),
                          ('retrieval_status','POC_CONTEXT'),('usage_scope','BENEFIT_EVIDENCE'),('byte_count',0)):
            packet=copy.deepcopy(self.packet);images=copy.deepcopy(self.images)
            images[0][0][key]=value;packet['results']=[images[0][0]]
            with self.subTest(key=key),self.assertRaises(ValueError):
                build_visual_context(self.bundle.context,packet,images,self.config)
        for key,value in (('gcs_generation','999'),('content_sha256','0'*64)):
            packet=copy.deepcopy(self.packet);images=copy.deepcopy(self.images)
            images[0][0]['original_product_image'][key]=value;packet['results']=[images[0][0]]
            with self.assertRaises(ValueError):build_visual_context(self.bundle.context,packet,images,self.config)
        for key,value in (('dimension',768),('model_id','gemini-embedding-001')):
            packet=copy.deepcopy(self.packet);images=copy.deepcopy(self.images)
            images[0][0]['embedding'][key]=value;packet['results']=[images[0][0]]
            with self.assertRaises(ValueError):build_visual_context(self.bundle.context,packet,images,self.config)

    def test_altered_bytes_duplicates_failed_verification_and_wrong_query_cannot_attach(self):
        with self.assertRaises(ValueError):
            build_visual_context(self.bundle.context,self.packet,[(self.images[0][0],b'corrupt')],self.config)
        with self.assertRaises(ValueError):
            build_visual_context(self.bundle.context,{**self.packet,'results':[self.images[0][0]]*2},self.images*2,self.config)
        with self.assertRaises(ValueError):
            build_visual_context(self.bundle.context,{**self.packet,'query':'Unrelated benefit'},self.images,self.config)
        packet=copy.deepcopy(self.packet);images=copy.deepcopy(self.images)
        images[0][0]['verification']['current_eligibility']=False;packet['results']=[images[0][0]]
        with self.assertRaises(ValueError):build_visual_context(self.bundle.context,packet,images,self.config)

    def test_no_eligible_generated_results_add_no_images_claims_or_invented_citations(self):
        packet={**self.packet,'snapshot_id':None,'results':[]}
        section=build_visual_context(self.bundle.context,packet,[],self.config)
        self.bundle.context['generated_banner_references']=section
        brief=assemble_brief(self.bundle.context,self.draft,{})
        self.assertEqual(brief['generated_visual_references']['references'],[])
        self.assertEqual(brief['copy']['claim_ids'],['claim-identity'])
        self.assertEqual(len(self.bundle.image_bytes),2)

    def test_fresh_search_calls_once_and_closes_session_on_success_and_cloud_failure(self):
        with (patch(MODULE+'MultimodalTextQueryEmbedder') as constructor,
              patch(MODULE+'search_generated_banners',return_value=(self.packet,self.images)) as search):
            constructor.return_value.model_calls=1
            packet,images=retrieve_generated_references(Mock(),Mock(),self.request,self.config)
            self.assertEqual(packet['model_calls'],1);constructor.return_value.close.assert_called_once()
            self.assertEqual(search.call_args.args[4],visual_query(self.request))
            search.side_effect=RuntimeError('BigQuery unavailable');constructor.return_value.close.reset_mock()
            with self.assertRaises(RuntimeError):retrieve_generated_references(Mock(),Mock(),self.request,self.config)
            constructor.return_value.close.assert_called_once()

    def test_evidence_orchestrator_attaches_live_images_and_preserves_claim_and_analytics_sections(self):
        fixture=test_evidence_packet.EvidencePacketTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.primary=(self.bundle.context['image_manifest'][0],self.bundle.image_bytes[f'AST-{fixture.sku}-PRIMARY'])
        fixture.readers()
        with patch('src.retrieval.evidence_packet.retrieve_generated_references',return_value=(self.packet,self.images)) as search:
            bundle=retrieve_evidence_packet(Mock(),Mock(),Mock(),fixture.request,fixture.campaign_config,
                                           fixture.sales_config,self.config)
        search.assert_called_once()
        self.assertEqual(len(bundle.image_bytes),3)
        self.assertEqual(bundle.context['brand_context'],fixture.brand)
        self.assertEqual(bundle.context['campaign_performance'],fixture.performance)
        self.assertEqual(bundle.context['sales_analytics'],fixture.sales)
        self.assertEqual(bundle.context['generated_banner_references']['results'][0]['asset_id'],self.row['asset_id'])
        self.assertIn('generated_campaign_assets',json.dumps(bundle.context['evidence_index']))

    def test_generated_search_cloud_failure_aborts_evidence_before_brief_or_export(self):
        fixture=test_evidence_packet.EvidencePacketTests();fixture.setUp();fixture.readers()
        self.addCleanup(fixture.doCleanups)
        with patch('src.retrieval.evidence_packet.retrieve_generated_references',side_effect=RuntimeError('GCS unavailable')):
            with self.assertRaisesRegex(RuntimeError,'GCS unavailable'):
                retrieve_evidence_packet(Mock(),Mock(),Mock(),fixture.request,fixture.campaign_config,
                                         fixture.sales_config,self.config)

    def test_gemini_receives_original_generated_bytes_with_explicit_visual_only_role(self):
        section=self.attach();model=Mock()
        model.models.generate_content.return_value=Mock(text=json.dumps(self.draft),usage_metadata=None,model_version='test',
            prompt_feedback=None,candidates=[Mock(finish_reason='STOP',safety_ratings=[])])
        draft,metadata=select_creative(model,self.bundle,self.model_config)
        args=model.models.generate_content.call_args.kwargs
        data=[part.inline_data.data for part in args['contents'].parts if part.inline_data is not None]
        self.assertEqual(data,list(self.bundle.image_bytes.values()))
        self.assertIn(self.raw,data);self.assertIn(self.row['asset_id'],metadata['input_image_asset_ids'])
        self.assertTrue(any(ROLE in (part.text or '') for part in args['contents'].parts))
        self.assertIn('do not supply product facts',args['config'].system_instruction)
        brief=assemble_brief(self.bundle.context,draft,metadata)
        self.assertEqual(brief['generated_visual_references']['references'][0]['metadata'],section['results'][0])
        self.assertEqual(brief['grounded_evidence']['campaign_performance']['summaries'],
                         self.bundle.context['campaign_performance']['summaries'])
        self.assertEqual(brief['copy']['headline'],'Source product');self.assertEqual(brief['copy']['claim_ids'],['claim-identity'])
        self.assertEqual(creative_options(self.bundle.context)['headline'],['Source product'])
        self.assertIn('Generated POC visual references (composition only',render_brief(brief))

    def test_after_model_revocation_or_metadata_change_prevents_brief_output(self):
        self.attach();model=Mock()
        model.models.generate_content.return_value=Mock(text=json.dumps(self.draft),usage_metadata=None,model_version='test',
            prompt_feedback=None,candidates=[Mock(finish_reason='STOP',safety_ratings=[])])
        with (patch(BRIEF+'retrieve_evidence_packet',return_value=self.bundle),
              patch(BRIEF+'retrieve_brand_context',return_value=self.bundle.context['brand_context']),
              patch(BRIEF+'retrieve_product_knowledge',return_value=self.bundle.context['product_knowledge'])):
            self.client.query.return_value.result.return_value=[]
            with self.assertRaises(ValueError):
                generate_grounded_brief(self.client,self.gcs,model,self.request,{}, {},self.model_config,self.config)
        model.models.generate_content.assert_called_once()
        self.client.query.return_value.result.return_value=[self.row]
        recheck_generated_references(self.client,self.gcs,self.bundle.context,self.bundle.image_bytes,self.config)
        changed={**self.metadata,'visual_description':'changed'}
        with patch(MODULE+'retrieve_generated_banner',return_value=(changed,self.raw)):
            with self.assertRaisesRegex(ValueError,'changed during generation'):
                recheck_generated_references(Mock(),Mock(),self.bundle.context,self.bundle.image_bytes,self.config)


if __name__=='__main__':
    unittest.main()
