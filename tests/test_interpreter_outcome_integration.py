from types import SimpleNamespace
from unittest import TestCase
from src.validation.interpreter_outcome_integration import fixture,plan_for,relocate
from src.validation.workflow_writer_integration import Scope
from dataclasses import replace

class InterpreterNativeGuardTests(TestCase):
    def setUp(self):self.probe=SimpleNamespace(scope=Scope('a'*32));self.plan=plan_for(fixture('fixture'))
    def test_relocation_only_owned_tables_and_faults(self):
        for fault in (None,'rollback','postcommit'):
            plan=relocate(self.probe,self.plan,'slot',fault=fault)
            self.assertNotIn('`project-661d501a-e709-4276-a26.ci_application.',plan.sql)
            self.assertIn(self.probe.scope.dataset,plan.sql)
        with self.assertRaises(ValueError):relocate(self.probe,self.plan,'slot',fault='other')
    def test_serving_or_unrelated_refs_are_rejected(self):
        with self.assertRaises((ValueError,RuntimeError)):relocate(self.probe,replace(self.plan,sql='SELECT * FROM `other.dataset.table`'),'bad')
    def test_observations_share_production_slot_but_fixture_race_uses_distinct_slots(self):
        other=plan_for(fixture('fixture'),'UNKNOWN')
        self.assertEqual(self.plan.job_id,other.job_id)
        self.assertNotEqual(relocate(self.probe,self.plan,'a').job_id,relocate(self.probe,other,'b').job_id)
