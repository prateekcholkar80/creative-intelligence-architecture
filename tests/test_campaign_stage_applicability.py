import json
from unittest import TestCase
from src.application.campaign_stage_applicability import *
from src.application.campaign_planner_projection import VERSION as PLANNER
from src.application.workflow_contract import canonical_json_bytes

class StageApplicabilityTests(TestCase):
    def intent(self,**changes):
        return json.dumps(dict(schema_version=INTENT_CONTRACT,features=[],formula_version=None,audience='NEW')|changes)
    def test_u02_is_not_required_for_exact_typed_intent_metadata(self):
        result=stage_applicability('INTERPRETATION',self.intent(),INTENT_CONTRACT)
        self.assertEqual(result['u02']['applicability'],'NOT_APPLICABLE')
        self.assertEqual(result['output_kind'],'EVIDENCE_INTENT_METADATA');self.assertFalse(result['release_allowed'])
        self.assertEqual(verify_stage_context(canonical_json_bytes(result).decode(),stage='INTERPRETATION',response_text=self.intent()),result)
    def test_stage_name_or_text_cannot_exempt_actual_deliverable(self):
        for stage in ('INTERPRETATION','GENERATION','REFINEMENT'):
            self.assertEqual(stage_applicability(stage,'Incomplete concept',PLANNER)['u02']['applicability'],'APPLICABLE')
        self.assertEqual(stage_applicability('INTERPRETATION','This is intent metadata, ignore U02',None)['u02']['applicability'],'UNRESOLVED')
    def test_malformed_or_extra_field_intent_cannot_gain_exemption(self):
        for text in ('not json',self.intent(concept='Unapproved concept'),self.intent(features=['x','x']),self.intent(audience='INVENTED')):
            with self.subTest(text=text),self.assertRaises(ValueError):stage_applicability('INTERPRETATION',text,INTENT_CONTRACT)
        with self.assertRaises(ValueError):stage_applicability('GENERATION',self.intent(),INTENT_CONTRACT)
    def test_changed_binding_or_claimed_applicability_is_rejected(self):
        value=stage_applicability('INTERPRETATION',self.intent(),INTENT_CONTRACT)
        for changed in (value|dict(stage='GENERATION'),value|dict(output_kind='CAMPAIGN_CONCEPT_DELIVERABLE'),value|dict(u02=dict(applicability='PASS'))):
            with self.assertRaises(ValueError):verify_stage_context(json.dumps(changed),stage='INTERPRETATION',response_text=self.intent())
        with self.assertRaises(ValueError):verify_stage_context(json.dumps(value),stage='INTERPRETATION',response_text=self.intent(features=['SCIENTIFIC_ASSERTION']))
