import json
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from src.application.campaign_evaluation_source_applicability import *
from src.application.campaign_quality_checks import SourceFact
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import canonical_json_bytes
from tests.test_campaign_clarification_prompts import ClarificationPromptTests

class EvaluationSourceApplicabilityTests(TestCase):
    def setUp(self):
        f=ClarificationPromptTests();f.setUp();self.policy=f.policy;self.now=self.policy.observed_at
        self.scope=dict(workspace_id='workspace',principal_id='principal',sku='CI-SKU-0022',country='CA',channel='META',audience='NEW',formula_version=None)
        fact=SourceFact('native-product','CI-SKU-0022','{"product_name":"Product"}',self.now,self.now+timedelta(seconds=60),True)
        self.product=ProductFactRead(fact,'native-product')
    def check(self,features=(),**changes):
        args=dict(response_text=json.dumps(dict(schema_version=INTENT_CONTRACT,features=list(features),formula_version=None,audience='NEW')),
            scope=self.scope,product=self.product,policy=self.policy,checked_at=self.now)
        return interpretation_source_applicability(**(args|changes))
    def test_identity_intent_does_not_require_a_full_study_but_never_authorizes_pipeline(self):
        result=self.check();self.assertEqual(result['downstream_requirements'],[])
        self.assertEqual(len(result['current_stage_sources']),2)
        self.assertFalse(result['qualification_ready']);self.assertFalse(result['generation_authorized'])
        self.assertFalse(result['downstream_source_authority_verified'])
    def test_scientific_feature_preserves_required_report_and_unsupported_scope(self):
        result=self.check(['SCIENTIFIC_ASSERTION'])
        requirement=next(r for r in result['downstream_requirements'] if r['evidence_kind']=='SCIENTIFIC_CLAIM_PERMISSION')
        self.assertIn('study_report_reference',requirement['required_fields'])
        self.assertEqual(requirement['state'],'UNAVAILABLE_LEGACY_PERMISSION_SCOPE');self.assertFalse(requirement['approved'])
    def test_every_policy_feature_retains_all_triggered_requirements(self):
        policy=validate_policy(self.policy.policy_canonical_json)
        for rule in policy['requirements']:
            with self.subTest(requirement=rule['requirement_id']):
                result=self.check(rule['when_any'])
                self.assertIn(rule['requirement_id'],[r['requirement_id'] for r in result['downstream_requirements']])
                self.assertTrue(all(not r['approved'] for r in result['downstream_requirements']))
    def test_stale_denied_wrong_sku_or_unbound_native_product_denies(self):
        for product in (replace(self.product,fact=replace(self.product.fact,sku='CI-SKU-0021')),
            replace(self.product,fact=replace(self.product.fact,access_allowed=False)),replace(self.product,native_job_id='different'),
            replace(self.product,fact=replace(self.product.fact,observed_at=self.now-timedelta(seconds=120),valid_until=self.now-timedelta(seconds=1)))):
            with self.assertRaises(ValueError):self.check(product=product)
        with self.assertRaises(ValueError):self.check(policy=replace(self.policy,observed_at=self.now-timedelta(seconds=120),valid_until=self.now-timedelta(seconds=1)))
    def test_metadata_cannot_be_used_as_claim_or_upload_approval(self):
        for use in ('STUDY_REPORT','UNVERIFIED_UPLOAD','WEBSITE_SUMMARY','CLAIM_APPROVAL'):
            with self.assertRaisesRegex(ValueError,'SOURCE_USE_UNSUPPORTED'):self.check(source_uses=(use,))
        fields='{"product_name":"Product","study_report_reference":"Invented"}'
        with self.assertRaises(ValueError):self.check(product=replace(self.product,fact=replace(self.product.fact,fields_json=canonical_json_bytes(json.loads(fields)).decode())))
    def test_unknown_feature_or_intent_scope_drift_denies(self):
        with self.assertRaises(ValueError):self.check(['UNREGISTERED_FEATURE'])
        with self.assertRaises(ValueError):self.check(scope=self.scope|dict(audience='RETURNING'))
        with self.assertRaises(ValueError):self.check(scope=self.scope|dict(formula_version='different'))

    def test_approved_policy_marker_does_not_substitute_for_verified_review(self):
        value=json.loads(self.policy.policy_canonical_json);value['status']='APPROVED'
        changed=replace(self.policy,policy_canonical_json=canonical_json_bytes(value).decode(),
            request=replace(self.policy.request,policy_sha256=_hash(value)))
        result=self.check(policy=changed)
        self.assertTrue(result['downstream_policy_review_required']);self.assertFalse(result['qualification_ready'])
