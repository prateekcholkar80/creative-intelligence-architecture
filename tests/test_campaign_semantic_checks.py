"""Semantic evaluator boundaries, not evidence of measured judgement quality."""
from dataclasses import replace
from pathlib import Path
import json
from unittest import TestCase
from src.application import campaign_semantic_checks as semantic
from src.application.workflow_contract import canonical_json_bytes


def catalog():
    data=json.loads(Path('config/evaluation/campaign_e1_closeout_v1.json').read_text())
    review=json.loads(Path('config/evaluation/campaign_e1_review_acceptance_v1.json').read_text())
    rows=[{k:c[k] for k in ('rule_id','acceptance_rule','failure_handling','stage_applicability','required_evidence')} for c in data['criteria']]
    return semantic.SemanticCatalog(review['review_sha256'],canonical_json_bytes(rows).decode())


def case():
    evidence=[dict(evidence_id='label-1',kinds=['PRODUCT_FACT','LABEL_INSTRUCTION'],statement='Ingredient X is listed; concentration is unknown.',provenance='SYNTHETIC_FIXTURE')]
    return semantic.SemanticInput('GENERATION','US','META','Ingredient X is listed; its concentration is unknown.',
        '[{"role":"USER","text":"Introduce this product."}]',canonical_json_bytes(evidence).decode(),('A02',),'ISOLATED_RULE_HANDLING')


def result():
    return dict(requested_content_decision='PASS',decision_reason='Only the listed identity is asserted.',checks=[dict(rule_id='A02',
        handling_state='PASS',reason='No concentration is invented.',candidate_quote='Ingredient X is listed',evidence_ids=['label-1'])])


class SemanticCheckerTests(TestCase):
    def test_all_41_missing_rules_have_distinct_reviewed_instructions(self):
        current=catalog();rules=current.rules(semantic.SEMANTIC_RULE_IDS)
        self.assertEqual(len(rules),41)
        self.assertEqual(len({c['acceptance_rule'] for c in rules}),41)
        for rid in semantic.SEMANTIC_RULE_IDS:
            system,prompt=semantic.prompt(current,replace(case(),rule_ids=(rid,)))
            self.assertIn(rid,prompt);self.assertIn('untrusted',system)
        self.assertIn('all ai people',next(c['acceptance_rule'] for c in rules if c['rule_id']=='C07').lower())

    def test_verified_observation_is_never_release_or_source_authority(self):
        observed=semantic.verify_result(catalog(),case(),result())
        self.assertFalse(observed['release_allowed']);self.assertFalse(observed['source_eligibility_verified'])
        self.assertFalse(observed['rendered_pixels_checked'])
        self.assertEqual(observed['input_sha256'],case().sha256)

    def test_fabricated_quotes_or_evidence_ids_deny(self):
        for field,value in (('candidate_quote','An invented efficacy result'),('evidence_ids',['other-product'])):
            output=result();output['checks'][0][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):semantic.verify_result(catalog(),case(),output)

    def test_missing_candidate_cannot_receive_a_handling_pass(self):
        missing=replace(case(),candidate=None,label_scope='REQUEST_ONLY_SCENARIO')
        with self.assertRaises(ValueError):semantic.verify_result(catalog(),missing,result())
        output=result();output['requested_content_decision']='CLARIFY'
        output['checks'][0].update(handling_state='INCONCLUSIVE',candidate_quote='')
        self.assertFalse(semantic.verify_result(catalog(),missing,output)['candidate_observed'])

    def test_failed_or_inconclusive_check_cannot_pass_requested_content(self):
        for state in ('FAIL','INCONCLUSIVE'):
            output=result();output['checks'][0]['handling_state']=state
            with self.subTest(state=state),self.assertRaisesRegex(ValueError,'SEMANTIC_PASS_WITH_UNRESOLVED_CHECK'):
                semantic.verify_result(catalog(),case(),output)
            output['requested_content_decision']='NEEDS_REVIEW'
            self.assertFalse(semantic.verify_result(catalog(),case(),output)['release_allowed'])

    def test_safe_withholding_can_pass_handling_without_passing_campaign(self):
        held=replace(case(),candidate='Benefit copy is withheld pending approved wording.')
        output=result();output.update(requested_content_decision='NEEDS_REVIEW',decision_reason='Requested benefit wording lacks approval.')
        output['checks'][0].update(candidate_quote='Benefit copy is withheld',reason='The response safely withholds unsupported copy.')
        checked=semantic.verify_result(catalog(),held,output)
        self.assertEqual(checked['checks'][0]['handling_state'],'PASS')
        self.assertEqual(checked['requested_content_decision'],'NEEDS_REVIEW')
        self.assertFalse(checked['release_allowed'])

    def test_missing_duplicate_extra_rules_and_unanchored_na_deny(self):
        for mode in ('missing','duplicate','extra','unanchored'):
            output=result()
            if mode=='missing':output['checks']=[]
            elif mode=='duplicate':output['checks']*=2
            elif mode=='extra':output['approved']=True
            else:output['checks'][0].update(handling_state='PASS',candidate_quote='')
            with self.subTest(mode=mode),self.assertRaises(ValueError):semantic.verify_result(catalog(),case(),output)

    def test_rule_content_and_input_changes_change_effective_pins(self):
        original=catalog();rows=json.loads(original.criteria_json);rows[0]['acceptance_rule']='Changed scope.'
        self.assertNotEqual(original.sha256,semantic.SemanticCatalog(original.review_sha256,canonical_json_bytes(rows).decode()).sha256)
        self.assertNotEqual(case().sha256,replace(case(),candidate='Different output.').sha256)

    def test_not_applicable_preserves_explicit_reason_without_fabricated_quote(self):
        output=result();output['checks'][0].update(handling_state='NOT_APPLICABLE',candidate_quote='',reason='No directions are proposed in this response.')
        self.assertEqual(semantic.verify_result(catalog(),case(),output)['checks'][0]['handling_state'],'NOT_APPLICABLE')
        output['checks'][0]['reason']=''
        with self.assertRaises(ValueError):semantic.verify_result(catalog(),case(),output)
