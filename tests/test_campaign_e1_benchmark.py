"""E1 draft integrity and coverage checks, not model-quality evaluation."""
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
import json
from src.validation.campaign_e1_benchmark import load_package,validate_package,MAX_BYTES
from src.application.workflow_contract import ContractError
from src.ingestion.configure_campaign_acceptance import manifest


class E1BenchmarkTests(TestCase):
    def setUp(self):self.package,self.report=load_package()

    def deny(self,mutate):
        changed=deepcopy(self.package);mutate(changed)
        with self.assertRaises(ContractError):validate_package(changed)

    def test_draft_contains_all_rules_and_labels_without_claiming_measured_quality(self):
        r=self.report
        self.assertEqual((r['criteria_count'],r['case_count'],r['multi_turn_case_count']),(47,117,8))
        self.assertEqual((r['synthetic_case_count'],r['playbook_name_case_count']),(105,12))
        self.assertEqual(r['status'],'VALID_DRAFT_NOT_READY')
        for key in ('e1_complete','serving_enabled','labels_reviewed','model_quality_measured','bigquery_updated'):
            self.assertIs(r[key],False)
        self.assertEqual(validate_package(deepcopy(self.package)),r)

    def test_each_rule_has_conforming_and_violation_cases_without_assuming_conforming_means_pass(self):
        c={c['case_id']:c for c in self.package['cases']}
        self.assertEqual(c['e1-g07-conforming']['expected_decision'],'BLOCK')
        self.assertEqual(c['e1-g05-conforming']['expected_decision'],'NEEDS_REVIEW')
        self.assertEqual(c['e1-u01-conforming']['expected_decision'],'CLARIFY')
        self.deny(lambda p:p['cases'].remove(next(c for c in p['cases'] if c['case_id']=='e1-a01-violation')))

    def test_status_or_approval_fields_cannot_turn_a_review_draft_into_authority(self):
        for key,value in (('status','APPROVED'),('serving_enabled',True),('serving_enabled',0),
            ('approved_by','fake-manager'),('approved_at','2026-10-15')):
            self.deny(lambda p,k=key,v=value:p.update({k:v}))

    def test_missing_duplicate_or_reordered_rules_and_changed_source_text_deny(self):
        self.deny(lambda p:p['criteria'].pop())
        self.deny(lambda p:p['criteria'].__setitem__(0,deepcopy(p['criteria'][1])))
        self.deny(lambda p:p['criteria'].reverse())
        self.deny(lambda p:p['criteria'][0].update(acceptance_rule='Invented replacement rule'))
        self.deny(lambda p:p.update(specification_sha256='a'*64))

    def test_no_inferred_canonical_sku_formula_or_approval_from_playbook_names(self):
        for changes in ({'canonical_sku':'CI-SKU-0001'},{'formula_version':'v1'},{'mapping_status':'APPROVED'}):
            self.deny(lambda p,v=changes:p['product_slots'][0].update(v))
        self.deny(lambda p:p['product_slots'][0]['evidence_references'][0].update(slide=27))
        self.assertEqual(self.report['selected_products'][0],'Renew Resurfacing Night Serum')

    def test_playbook_source_hash_slide_name_and_evidence_provenance_must_match(self):
        index=next(i for i,c in enumerate(self.package['cases']) if c['provenance']=='USER_PLAYBOOK_TEXT')
        for changes in ({'source_sha256':'b'*64},{'slide':97},{'exact_text':'Another product'},{'provenance':'BEAUTYFEEDS'}):
            self.deny(lambda p,v=changes:p['cases'][index]['evidence'][0].update(v))
        self.deny(lambda p:p['source_documents'][0].update(sha256='b'*64))
        self.deny(lambda p:p['cases'][0].update(provenance='APPROVED_CLINICAL_STUDY'))

    def test_user_country_selection_and_visual_prohibitions_cannot_silently_widen(self):
        self.deny(lambda p:p['benchmark_countries'].append('FR'))
        self.deny(lambda p:p['brand_directives'].update(ai_people='NEEDS_REVIEW'))
        self.deny(lambda p:p['brand_directives'].update(before_after_imagery='ALLOW_WITH_CONSENT'))
        self.deny(lambda p:p['brand_directives'].update(serving_configuration_updated=0))
        self.deny(lambda p:p['cases'][0].update(country='WW'))
        for case in self.package['cases']:
            if case['case_type']=='USER_BRAND_PROHIBITION':self.assertEqual(case['expected_decision'],'BLOCK')
        index=next(i for i,c in enumerate(self.package['cases']) if c['case_type']=='USER_BRAND_PROHIBITION')
        self.deny(lambda p:p['cases'][index].update(expected_decision='PASS'))

    def test_missing_or_duplicate_expected_evidence_and_cases_deny(self):
        self.deny(lambda p:p['cases'][0].update(expected_evidence_ids=['unknown-source']))
        self.deny(lambda p:p['cases'][0]['evidence'].append(deepcopy(p['cases'][0]['evidence'][0])))
        self.deny(lambda p:p['cases'].append(deepcopy(p['cases'][-1])))
        self.deny(lambda p:p['cases'][0].update(rule_ids=['UNKNOWN']))

    def test_multi_turn_order_and_all_stage_coverage_are_required(self):
        self.deny(lambda p:p['cases'][0]['turns'][0].update(role='ASSISTANT'))
        self.deny(lambda p:p['cases'][0]['turns'].append(dict(role='ASSISTANT',text='trailing answer')))
        self.deny(lambda p:[c.update(stage='GENERATION') for c in p['cases']])
        self.deny(lambda p:[c.update(turns=[dict(role='USER',text='single turn')]) for c in p['cases']])

    def test_text_descriptions_never_claim_real_visual_verification_or_reviewed_labels(self):
        for changes in ({'rendered_artifact_verified':True},{'rendered_artifact_verified':0},
            {'modality':'RENDERED_IMAGE'},{'label_status':'APPROVED'}):
            self.deny(lambda p,v=changes:p['cases'][0].update(v))
        self.deny(lambda p:p['criteria'][0].update(checker_status='IMPLEMENTED'))

    def test_untyped_scopes_missing_fields_extra_fields_and_nonfinite_values_deny(self):
        for key in ('case_type','stage','channel','country','expected_decision','provenance'):
            self.deny(lambda p,k=key:p['cases'][0].update({k:[]}))
        self.deny(lambda p:p['cases'][0].pop('expected_reason'))
        self.deny(lambda p:p.update(extra=True))
        self.deny(lambda p:p['criteria'][0].update(proposed_negative_decision=[]))
        self.deny(lambda p:p.update(extra=float('nan')))

    def test_loader_rejects_duplicate_keys_bad_json_nonfinite_and_oversized_packages(self):
        with TemporaryDirectory() as directory:
            path=Path(directory)/'input.json'
            for text in ('{"status":"DRAFT","status":"APPROVED"}','{','{"value":NaN}','x'*(MAX_BYTES+1)):
                path.write_text(text)
                with self.assertRaises(ContractError):load_package(path)

    def test_original_bigquery_pinned_specification_is_unchanged(self):
        self.assertEqual(manifest()[0]['specification_sha256'],
            '6c3e95a6a33e2a6993736db5407860b0367d9aae1f1be839f63c8a4e38ac6e18')

    def test_package_hash_changes_with_review_content_but_never_becomes_readiness(self):
        changed=deepcopy(self.package);changed['criteria'][0]['pass_assertion']+=' Reviewed example pending.'
        report=validate_package(changed)
        self.assertNotEqual(report['package_sha256'],self.report['package_sha256'])
        self.assertIs(report['e1_complete'],False)
