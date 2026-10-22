import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from src.generators.creative_experiment_plan import (
    CONFIG, assemble_plan, export_plan, generate_plan, render_plan, validate_config,
)
from src.generators.banner_preview_generator import render_preview
from src.generators.campaign_brief_generator import assemble_brief
from src.ingestion.register_generated_banner import registration_row
from src.retrieval.campaign_performance import json_value
from tests import test_banner_preview_generator

MODULE='src.generators.creative_experiment_plan.'


class ExperimentPlanTests(unittest.TestCase):
    def setUp(self):
        fixture=test_banner_preview_generator.BannerPreviewTests();fixture.setUp()
        fixture.bundle.context['image_manifest'][0]['gcs_generation']='10'
        self.assets=[];self.retrieved=[]
        for index,layout in enumerate(('PRODUCT_RIGHT_COPY_LEFT','PRODUCT_LEFT_COPY_RIGHT')):
            provenance={} if index==0 else dict(selection_method='DETERMINISTIC_LAYOUT_VARIANT',
                                              parent_asset_id=self.assets[0]['asset_id'])
            brief=assemble_brief(fixture.bundle.context,{**fixture.draft,'layout':layout},provenance)
            brief['approval_recheck_passed']=True
            preview=render_preview(brief,fixture.bundle,fixture.config)
            preview.metadata['approval_recheck_passed']=True
            asset=registration_row(preview,brief,fixture.bundle);asset['gcs_generation']=str(100+index)
            self.assets.append(asset)
            metadata=dict(asset_id=asset['asset_id'],canonical_sku=asset['sku'],source_of_truth='BIGQUERY',
                source_system='SYNTHETIC_POC',gcs_uri=asset['gcs_uri'],gcs_generation=asset['gcs_generation'],
                snapshot_id=asset['knowledge_snapshot_id'],content_sha256=asset['sha256'],
                original_product_image=dict(asset_id=asset['source_image_asset_id'],gcs_uri=asset['source_image_uri'],
                    gcs_generation=asset['source_image_generation'],content_sha256=asset['source_image_sha256']),
                review=dict(review_id=f'review-{index}',revision=2,status='APPROVED_FOR_POC',
                            approval_scope='POC_VISUAL_REUSE_ONLY'),
                embedding=dict(embedding_id=f'embedding-{index}',model_id='multimodalembedding@001',dimension=512),
                verification=dict(current_eligibility=True,sha256_match=True,generation_match=True))
            self.retrieved.append((metadata,preview.png))
        self.config=json.loads(CONFIG.read_text())
        self.config.update(control_asset_id=self.assets[0]['asset_id'],variant_asset_id=self.assets[1]['asset_id'])

    def assemble(self):
        return assemble_plan(self.config,self.assets,self.retrieved)

    def pipeline(self):
        reader=patch(MODULE+'read_asset',side_effect=[*self.assets,*self.assets]).start()
        images=patch(MODULE+'retrieve_generated_banner',side_effect=[*self.retrieved,*self.retrieved]).start()
        self.addCleanup(patch.stopall)
        return reader,images

    def generate(self):
        return generate_plan(Mock(),Mock(),self.config,{}, {})

    def test_plan_is_deterministic_proposed_equal_split_and_has_no_fabricated_results(self):
        plan=self.assemble()
        self.assertEqual(plan,self.assemble())
        self.assertEqual(plan['status'],'DRAFT_NOT_LAUNCHED')
        self.assertEqual([arm['allocation_percent'] for arm in plan['arms']],[50,50])
        self.assertEqual(plan['test_variable'],'LAYOUT')
        self.assertEqual(plan['primary_metric']['denominator'],'clicks')
        self.assertIsNone(plan['primary_metric']['zero_denominator_result'])
        self.assertTrue(all(value is None for value in plan['results'].values()))
        for key in ('budget_per_arm','currency','start_date','end_date','sample_size','stopping_rule'):
            self.assertIsNone(plan['delivery_plan'][key])
        self.assertFalse(plan['cloud_writes']);self.assertEqual(plan['model_calls'],0)
        self.assertIn('Results, winner, observed lift and statistical conclusion: unset.',render_plan(plan))

    def test_config_rejects_wrong_scope_allocation_same_asset_and_unknown_fields(self):
        for key,value in (('version','OTHER'),('primary_metric','ROAS'),('allocation_percent',{'A':60,'B':40}),
                          ('canonical_sku','CI-SKU-0021'),('extra',1),('variant_asset_id',self.config['control_asset_id'])):
            with self.subTest(key=key),self.assertRaises(ValueError):validate_config({**self.config,key:value})

    def test_cross_sku_changed_copy_targeting_and_original_image_are_rejected(self):
        for key,value in (('sku','CI-SKU-0002'),('headline','Unsupported benefit'),('audience','Other audience'),
                          ('channel','EMAIL'),('source_image_generation','999'),('approved_claim_ids',['OTHER'])):
            original=self.assets[1]
            self.assets[1]={**original,key:value}
            with self.subTest(key=key),self.assertRaises(ValueError):self.assemble()
            self.assets[1]=original

    def test_pending_rejected_or_wrong_review_scope_cannot_plan(self):
        for key,value in (('status','PENDING_REVIEW'),('status','REJECTED'),('revision',True),
                          ('approval_scope','MANUFACTURER_APPROVED')):
            metadata,raw=self.retrieved[1];changed=copy.deepcopy(metadata);changed['review'][key]=value
            self.retrieved[1]=(changed,raw)
            with self.subTest(key=key),self.assertRaises(ValueError):self.assemble()
            self.retrieved[1]=(metadata,raw)

    def test_altered_original_failed_verification_stale_snapshot_and_source_are_rejected(self):
        metadata,raw=self.retrieved[1]
        for changed,new_raw in ((metadata,b'corrupt'),({**metadata,'source_of_truth':'LOCAL'},raw),
                               ({**metadata,'snapshot_id':'old'},raw),
                               ({**metadata,'verification':{'sha256_match':False}},raw)):
            self.retrieved[1]=(changed,new_raw)
            with self.assertRaises(ValueError):self.assemble()
        self.retrieved[1]=(metadata,raw)

    def test_more_than_layout_changed_or_wrong_parent_is_rejected(self):
        original=self.assets[1]['brief_json']
        for edit in ('palette','selection','parent'):
            brief=json.loads(original)
            if edit=='palette':brief['visual_direction']['background']='#F5F4F0'
            elif edit=='selection':brief['model_selection']['concept']='EDITORIAL_PRODUCT_FRAME'
            else:brief['model']['parent_asset_id']='OTHER'
            self.assets[1]['brief_json']=json.dumps(brief,default=json_value)
            with self.subTest(edit=edit),self.assertRaises(ValueError):self.assemble()
        self.assets[1]['brief_json']=original

    def test_live_pipeline_rechecks_both_arms_without_local_serving_reads_or_writes(self):
        reader,images=self.pipeline()
        client=Mock();gcs=Mock()
        with patch('pathlib.Path.read_text',side_effect=AssertionError('No local serving fallback')):
            plan,raw=generate_plan(client,gcs,self.config,{}, {})
        self.assertEqual(reader.call_count,4);self.assertEqual(images.call_count,4)
        self.assertTrue(plan['approval_recheck_passed']);self.assertEqual(set(raw),{'A','B'})
        client.create_table.assert_not_called();client.load_table_from_json.assert_not_called()
        gcs.bucket.assert_not_called()

    def test_review_change_revocation_or_cloud_failure_propagates_before_export(self):
        reader,images=self.pipeline()
        metadata,raw=self.retrieved[0];changed=copy.deepcopy(metadata);changed['review']['revision']=3
        images.side_effect=[*self.retrieved,(changed,raw),self.retrieved[1]]
        with self.assertRaisesRegex(ValueError,'changed during planning'):self.generate()
        images.side_effect=RuntimeError('BigQuery unavailable')
        reader.side_effect=self.assets
        with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):self.generate()

    def test_review_identity_changes_content_id_and_reference_attribution_is_direct(self):
        first=self.assemble()
        self.retrieved[1][0]['review']['revision']=3
        self.retrieved[1][0]['review']['review_id']='new-review'
        second=self.assemble()
        self.assertNotEqual(first['experiment_id'],second['experiment_id'])
        self.assertTrue(any('#review_id=new-review' in ref for ref in second['evidence_references']))
        self.assertEqual(second['arms'][0]['asset_id'],self.assets[0]['asset_id'])

    def test_unique_export_preserves_images_and_rejects_tampering_before_writes(self):
        self.pipeline();plan,images=self.generate()
        with tempfile.TemporaryDirectory() as tmp:
            first=export_plan(plan,images,Path(tmp));second=export_plan(plan,images,Path(tmp))
            self.assertNotEqual(first,second)
            self.assertEqual((first.parent/'A_banner.png').read_bytes(),images['A'])
            self.assertEqual((first.parent/'B_banner.png').read_bytes(),images['B'])
            self.assertTrue((first.parent/'experiment_plan.txt').exists())
        with tempfile.TemporaryDirectory() as tmp:
            for mutation in ('image','results','recheck','identity'):
                changed=copy.deepcopy(plan);raw=dict(images)
                if mutation=='image':raw['A']=b'changed'
                elif mutation=='results':changed['results']['winner']='B'
                elif mutation=='identity':changed['experiment_id']='OTHER'
                else:changed['approval_recheck_passed']=False
                with self.subTest(mutation=mutation),self.assertRaises(ValueError):export_plan(changed,raw,Path(tmp))
            self.assertEqual(list(Path(tmp).iterdir()),[])


if __name__ == '__main__':
    unittest.main()
