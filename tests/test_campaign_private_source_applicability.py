from dataclasses import replace
from datetime import datetime,timezone,timedelta
import json
from unittest import TestCase
from src.application.campaign_private_source_applicability import identity_source_plan,FEATURES
from src.application.campaign_quality_product_reader import ProductFactRead
from src.application.campaign_quality_checks import SourceFact
from src.application.campaign_evaluator_store import canonical
from src.retrieval.upload_corpus import UploadCorpusCapture,VERSION


class PrivateSourceUseTests(TestCase):
    def fixture(self):
        now=datetime.now(timezone.utc);fields=dict(product_name='Rejuvenate Restorative Face Oil')
        fact=SourceFact('native-product','CI-SKU-0022',canonical(fields),now-timedelta(seconds=1),now+timedelta(seconds=60),True)
        capture=UploadCorpusCapture(canonical(dict(version=VERSION,scope=dict(sku=fact.sku,country='US',channel='META'),principal_id='owner',
            use='UNVERIFIED_PRIVATE_RETRIEVAL_EVALUATION_ONLY',image_pins=[dict(sha256='a'*64)],
            text_chunks=[dict(chunk_id='b'*64,provenance=dict(text_sha256='c'*64))],claim_approval=False,
            eligible_for_generation=False,qualifying=False,release_allowed=False)),(),now+timedelta(seconds=60))
        return dict(capture=capture,product=ProductFactRead(fact,'native-product'),copy=dict(headline=fields['product_name'],body='',cta='View product'),features=FEATURES,checked_at=now)

    def finding(self,**changes):
        return dict(image_sha256='a'*64,assessment_record_id='d'*64,assessment_outcome_sha256='e'*64,
            product_identity='MATCH',product_visible=True,contains_people=False,contains_before_after=False,
            contains_clinical_badge=False,embedded_claims=[],**changes)

    def test_identity_copy_does_not_promote_reference_text_or_missing_visual_review(self):
        result=identity_source_plan(**self.fixture())
        self.assertEqual(result['claim_applicability'],'NOT_ASSERTED_BY_EXPLICIT_IDENTITY_ONLY_COPY')
        self.assertEqual(result['image_checks'][0]['decision'],'NEEDS_REVIEW')
        self.assertFalse(result['text_uses'][0]['approved_copy_source']);self.assertFalse(result['generation_authorized'])

    def test_no_badge_or_clean_origin_can_grant_release(self):
        args=self.fixture();r=identity_source_plan(**args,image_findings=(self.finding(),))
        self.assertEqual(r['image_checks'][0]['decision'],'APPLICABLE_AS_UNVERIFIED_PRIVATE_REFERENCE')
        self.assertFalse(r['claim_approval']);self.assertFalse(r['generation_authorized']);self.assertFalse(r['qualifying'])
        image=self.finding();image['contains_clinical_badge']=True
        r=identity_source_plan(**args,image_findings=(image,));self.assertEqual(r['image_checks'][0]['decision'],'NEEDS_REVIEW')

    def test_review_screenshots_people_and_wrong_product_do_not_become_product_assets(self):
        for changes in (dict(product_visible=False),dict(contains_people=True),dict(contains_before_after=True),dict(product_identity='MISMATCH')):
            finding=self.finding();finding.update(changes)
            self.assertEqual(identity_source_plan(**self.fixture(),image_findings=(finding,))['image_checks'][0]['decision'],'BLOCK')

    def test_copy_features_or_current_identity_drift_denied(self):
        for fault in ('benefit','feature','expiry','access','sku'):
            args=self.fixture()
            if fault=='benefit':args['copy']['body']='Reduces wrinkles'
            elif fault=='feature':args['features']=FEATURES+('BENEFIT',)
            else:
                fact=args['product'].fact
                if fault=='expiry':args['checked_at']=fact.valid_until
                elif fault=='access':args['product']=replace(args['product'],fact=replace(fact,access_allowed=False))
                else:args['product']=replace(args['product'],fact=replace(fact,sku='OTHER'))
            with self.assertRaises(ValueError):identity_source_plan(**args)

    def test_wrong_image_hash_or_unbound_findings_denied(self):
        image=self.finding();image['image_sha256']='f'*64
        with self.assertRaises(ValueError):identity_source_plan(**self.fixture(),image_findings=(image,))
        image=self.finding();image['assessment_record_id']='not-native-proof'
        with self.assertRaises(ValueError):identity_source_plan(**self.fixture(),image_findings=(image,))
