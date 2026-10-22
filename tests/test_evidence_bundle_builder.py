"""Complete planning-evidence assembly checks; no source reads or writes."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import unittest

from src.application.evidence_bundle import PlanningEvidenceBundle, assemble_planning_evidence
from src.application.evidence_contract import validate_evidence_bundle
from src.application.evidence_projection import (
    project_brand_context, project_campaign_creatives, project_campaign_images,
    project_campaign_performance, project_generated_images, project_product_image,
    project_product_knowledge, project_sales_analytics,
    project_visual_reference,
)
from src.application.workflow_contract import ContractError
from tests.test_evidence_projection import (
    captured_brand_read, captured_creative_read, captured_image_reads,
    captured_performance_read, captured_read, captured_sales_read,
    captured_visual_read,
)
from tests.test_execution_contract import execution
from tests.test_workflow_contract import CONVERSATION, TIME, digest


class EvidenceBundleBuilderTests(unittest.TestCase):
    def setUp(self):
        self.attempt,self.request,self.job=execution();self.created=TIME+timedelta(seconds=3)
        kwargs=dict(evidence_snapshot_id='evidence-snapshot-1',request=self.request,job=self.job,
            attempt=self.attempt,conversation=CONVERSATION,created_at=self.created)
        product,campaigns,generated=captured_image_reads()
        creative=captured_creative_read()
        # The ranked creative and verified campaign image must identify identical bytes.
        image_record=campaigns.images[0][0]
        creative_record=creative.packet['creatives'][0]
        creative_record['banner_sha256']=image_record['content_sha256']
        creative_record['gcs_uri']=image_record['gcs_uri']
        creative_record['image_uris']=[creative_record['source_image_uri'],image_record['gcs_uri']]
        self.groups=dict(product_rows=project_product_knowledge(captured_read(),**kwargs),
            brand_rows=project_brand_context(captured_brand_read(),**kwargs),
            sales_rows=project_sales_analytics(captured_sales_read(),**kwargs),
            product_image_rows=project_product_image(product,**kwargs),
            campaign_creative_rows=project_campaign_creatives(creative,**kwargs),
            campaign_image_rows=project_campaign_images(campaigns,**kwargs),
            campaign_performance_rows=project_campaign_performance(captured_performance_read(),**kwargs),
            generated_image_rows=project_generated_images(generated,**kwargs))

    def assemble(self,**changes):
        groups=self.groups|changes
        return assemble_planning_evidence(**groups,request=self.request,job=self.job,
            attempt=self.attempt,conversation=CONVERSATION)

    def test_assembles_complete_bundle_and_reconciles_overlapping_claim(self):
        bundle=self.assemble()
        self.assertIsInstance(bundle,PlanningEvidenceBundle)
        self.assertEqual(bundle.evidence_snapshot_id,'evidence-snapshot-1')
        self.assertEqual(bundle.knowledge_snapshot_id,digest('snapshot'))
        self.assertEqual(bundle.canonical_sku,'CI-SKU-0001')
        claims=[row for row in bundle.rows if row['source_kind']=='POC_CLAIM']
        self.assertEqual(len(claims),1)
        self.assertEqual(claims[0]['source_revision'],'bq-brand-read-1')
        validate_evidence_bundle(list(bundle.rows),request=self.request,job=self.job,
            attempt=self.attempt,conversation=CONVERSATION)

    def test_deterministic_order_does_not_mutate_inputs(self):
        before=deepcopy(self.groups);first=self.assemble();second=self.assemble()
        self.assertEqual(first,second);self.assertEqual(self.groups,before)
        self.assertEqual(list(first.rows),sorted(first.rows,key=lambda row:(row['source_kind'],
            row['rank'] or 0,row['source_record_id'],row['evidence_ref_id'])))

    def test_claim_disagreement_fails_before_bundle_publication(self):
        brand=deepcopy(self.groups['brand_rows'])
        claim=next(row for row in brand if row['source_kind']=='POC_CLAIM')
        claim['content_sha256']=hashlib.sha256(b'different').hexdigest()
        with self.assertRaisesRegex(ContractError,'EVIDENCE_SOURCE_CONFLICT'):
            self.assemble(brand_rows=brand)

    def test_missing_or_misaligned_campaign_group_fails_closed(self):
        with self.assertRaisesRegex(ContractError,'EVIDENCE_CAMPAIGN_MISMATCH'):
            self.assemble(campaign_performance_rows=[])
        creative=deepcopy(self.groups['campaign_creative_rows']);creative[0]['rank']=2
        with self.assertRaises(ContractError):self.assemble(campaign_creative_rows=creative)

    def test_opted_in_visual_reference_requires_all_four_current_sections(self):
        from src.application.workflow_contract import request_fingerprint
        self.request['visual_reference']='NAKED_AND_THRIVING'
        self.request['submitted_preferences']['preferences']['visual_reference']='NAKED_AND_THRIVING'
        self.request['request_sha256']=request_fingerprint(self.request)
        self.job['request_sha256']=self.request['request_sha256']
        visual=project_visual_reference(captured_visual_read(),evidence_snapshot_id='evidence-snapshot-1',
            request=self.request,job=self.job,attempt=self.attempt,conversation=CONVERSATION,
            created_at=self.created)
        bundle=self.assemble(visual_reference_rows=visual)
        self.assertEqual(len([row for row in bundle.rows if row['evidence_payload']['record'].get(
            'chunk_type')=='WEBSITE_VISUAL_REFERENCE']),4)
        with self.assertRaisesRegex(ContractError,'INCOMPLETE_EVIDENCE_STYLE'):
            self.assemble(visual_reference_rows=visual[:-1])


if __name__=='__main__':
    unittest.main()
