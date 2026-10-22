from unittest import TestCase
from src.ingestion.setup_evaluator_model_access import *


class EvaluatorModelAccessTests(TestCase):
    def test_exact_permission_and_unconditional_grant_preserve_unrelated_members(self):
        verified_role(dict(name=ROLE,stage='GA',includedPermissions=['aiplatform.endpoints.predict']),ROLE,['aiplatform.endpoints.predict'])
        policy=dict(etag='original',version=1,bindings=[dict(role='roles/viewer',members=['user:fixture@example.com'])])
        proposed,changed=project_proposal(policy)
        self.assertTrue(changed);self.assertEqual(proposed['etag'],'original');self.assertEqual(proposed['bindings'][0],policy['bindings'][0])
        self.assertNotIn('condition',proposed['bindings'][1]);self.assertEqual(proposed['version'],3)
        again,changed=project_proposal(proposed);self.assertFalse(changed);self.assertEqual(again,proposed)
    def test_broader_role_or_missing_etag_denies(self):
        with self.assertRaises(ValueError):verified_role(dict(name=ROLE,stage='GA',includedPermissions=['aiplatform.endpoints.predict','aiplatform.models.delete']),ROLE,['aiplatform.endpoints.predict'])
        with self.assertRaises(ValueError):project_proposal(dict(bindings=[]))

    def test_migrate_only_exact_legacy_member(self):
        other='user:other@example.com'
        policy=dict(etag='original',version=3,bindings=[dict(role=ROLE,members=[RUNTIME,other],condition=LEGACY_CONDITION),dict(role='roles/viewer',members=[other])])
        proposed,changed=project_proposal(policy)
        self.assertTrue(changed)
        self.assertEqual(policy['bindings'][0]['members'],[RUNTIME,other])
        self.assertEqual(proposed['bindings'][0],dict(role=ROLE,members=[other],condition=LEGACY_CONDITION))
        self.assertEqual(proposed['bindings'][1],policy['bindings'][1])
        self.assertEqual(proposed['bindings'][2],dict(role=ROLE,members=[RUNTIME]))
        self.assertFalse(project_proposal(proposed)[1])
