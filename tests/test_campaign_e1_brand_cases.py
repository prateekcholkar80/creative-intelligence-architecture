"""Real-brand draft label/provenance integrity, not model evaluation or auth."""
from copy import deepcopy
from pathlib import Path
import json,hashlib
from unittest import TestCase
from src.validation.campaign_e1_brand_cases import DEFAULT,validate
from src.ingestion.add_naked_thriving_products import TARGETS,CORRECTED_JOB_ID,BUCKET
from src.application.workflow_contract import ContractError


def receipt():
    return dict(status='VERIFIED_APPLIED',native_job_id=CORRECTED_JOB_ID,original_full_rows_preserved=True,
        claim_permissions_created=False,serving_enabled=False,total_rows={'product_master':23},
        gcs_pins={sku:dict(generation=1,sha256='a'*64,gcs_uri=f'gs://{BUCKET}/products/{sku}/primary.jpg') for sku in TARGETS})


class E1BrandCasesTests(TestCase):
    def setUp(self):self.package=json.loads(DEFAULT.read_text())

    def deny(self,change):
        package=deepcopy(self.package);change(package)
        with self.assertRaises(ContractError):validate(package,receipt())

    def test_all_three_actual_catalog_ids_and_twenty_six_cases_remain_unreviewed(self):
        result=validate(self.package,receipt())
        self.assertEqual(result['canonical_skus'],sorted(TARGETS));self.assertEqual(result['case_count'],26)
        self.assertEqual(result['source_count'],4)
        for field in ('live_catalog_rechecked','labels_reviewed','model_quality_measured','e1_complete',
            'serving_enabled','formula_versions_verified','study_reports_reviewed'):self.assertIs(result[field],False)

    def test_no_poc_remapping_cross_variant_or_invented_formula(self):
        for changes in ({'canonical_sku':'CI-SKU-0001'},{'source_variant_id':'wrong'},{'formula_version':'v1'},
            {'mapping_status':'APPROVED'},{'brand_product_name':'Other Product'}):
            self.deny(lambda p,v=changes:p['products'][0].update(v))

    def test_verified_catalog_checkpoint_cannot_be_inferred_from_pending_or_failed_import(self):
        for changes in ({'status':'PREPARED_NO_WRITES'},{'native_job_id':'other'},{'original_full_rows_preserved':False},
            {'claim_permissions_created':True},{'total_rows':{'product_master':20}}):
            with self.assertRaises(ContractError):validate(self.package,receipt()|changes)

    def test_missing_generation_wrong_bucket_or_invalid_hash_denies_image_checkpoint(self):
        for changes in ({'generation':True},{'generation':0},{'sha256':'bad'},
            {'gcs_uri':'gs://other-bucket/products/CI-SKU-0021/primary.jpg'}):
            r=receipt();r['gcs_pins']['CI-SKU-0021'].update(changes)
            with self.assertRaises(ContractError):validate(self.package,r)

    def test_observed_sources_never_become_approved_claims_or_verified_formula(self):
        for changes in ({'formula_verified':True},{'claim_approved':True},{'projection_sha256':'b'*64},
            {'url':'https://untrusted.example/product'}):
            self.deny(lambda p,v=changes:p['sources'][0].update(v))

    def test_study_summary_is_not_a_reviewed_report_even_with_a_new_projection_hash(self):
        def change(p):
            source=p['sources'][-1];source['facts']['underlying_reports_reviewed']=True
            source['projection_sha256']=hashlib.sha256(json.dumps(source['facts'],sort_keys=True,separators=(',',':')).encode()).hexdigest()
        self.deny(change)
        self.deny(lambda p:p['sources'][-1].update(source_kind='REVIEWED_STUDY_REPORT'))

    def test_study_context_keeps_participants_and_endpoints_separate_from_effect_size(self):
        facts=self.package['sources'][-1]['facts']
        self.assertEqual(facts['duration_weeks'],4)
        self.assertEqual([facts['products'][sku]['participants'] for sku in TARGETS],[33,34,33])
        self.assertIn('visible_lines_wrinkles_participants_pct',facts['products']['CI-SKU-0021'])
        self.assertNotIn('wrinkle_depth_reduction_pct',facts['products']['CI-SKU-0021'])

    def test_actual_renew_warning_and_source_projections_remain_pinned(self):
        facts=self.package['sources'][0]['facts']
        self.assertIn('NO_SIMULTANEOUS_RETINOL',facts['warnings'])
        self.assertEqual(facts['routine']['start_evenings_per_week'],2)
        self.deny(lambda p:p['sources'][0]['facts'].update(warnings=[]))

    def test_case_references_and_countries_cannot_silently_change(self):
        self.deny(lambda p:p['cases'][0].update(expected_source_ids=['unknown']))
        self.deny(lambda p:p['cases'][0].update(country='FR'))
        self.deny(lambda p:p['cases'][0].update(canonical_sku='CI-SKU-0001'))
        self.deny(lambda p:p['cases'][0].update(rendered_artifact_verified=True))

    def test_prohibited_visuals_and_missing_report_or_claims_never_pass(self):
        for suffix in ('website-before-after','study-report-hold','permission-missing'):
            index=next(i for i,c in enumerate(self.package['cases']) if c['case_id'].endswith(suffix))
            self.deny(lambda p,i=index:p['cases'][i].update(expected_decision='PASS'))

    def test_label_status_and_benchmark_execution_cannot_be_forged(self):
        for changes in ({'status':'APPROVED'},{'approved_by':'manager'},{'serving_enabled':True},
            {'labels_status':'REVIEWED'},{'benchmark_execution_status':'PASSED'}):
            self.deny(lambda p,v=changes:p.update(v))
        self.deny(lambda p:p['cases'][0].update(label_status='APPROVED'))

    def test_case_coverage_and_uniqueness_are_required(self):
        self.deny(lambda p:p['cases'].pop())
        self.deny(lambda p:p['cases'].__setitem__(0,deepcopy(p['cases'][1])))
        self.deny(lambda p:[c.update(stage='GENERATION') for c in p['cases']])

    def test_user_language_baseline_does_not_approve_claims_or_invent_study_availability(self):
        self.assertEqual(self.package['language_baseline']['mode'],'APPEARANCE_FOCUSED')
        self.deny(lambda p:p['language_baseline'].update(baseline_is_claim_permission=True))
        self.deny(lambda p:p['language_baseline'].update(sku_specific_claim_permission_required=False))
        self.deny(lambda p:p.update(evidence_availability='FULL_REPORTS_AVAILABLE'))
