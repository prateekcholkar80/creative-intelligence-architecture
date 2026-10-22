from dataclasses import replace
from types import SimpleNamespace
from unittest import TestCase
from src.validation.workflow_writer_integration import Scope
from src.validation.control_fixtures import controls
from src.validation.interpreter_submission_integration import plan_for,relocate

class SubmissionProbeTests(TestCase):
    def setUp(self):self.probe=SimpleNamespace(scope=Scope('b'*32));self.rows=controls('fixture');self.plan=plan_for(self.rows)
    def test_relocation_and_fault_markers(self):
        for fault in (None,'rollback','postcommit'):
            p=relocate(self.probe,self.plan,'core',fault)
            self.assertNotIn('.ci_application.',p.sql)
            self.assertIn(self.probe.scope.dataset+'.campaign_transcript_bodies',p.sql)
        with self.assertRaises(ValueError):relocate(self.probe,self.plan,'bad','invalid')
    def test_foreign_and_destructive_sql_denied(self):
        for sql in ('SELECT * FROM `other.dataset.table`',self.plan.sql+'\nDELETE FROM `other.dataset.table` WHERE TRUE'):
            with self.assertRaises(ValueError):relocate(self.probe,replace(self.plan,sql=sql),'bad')
    def test_changed_content_competes_for_same_production_slot(self):
        other=plan_for(self.rows,'Different content')
        self.assertEqual(self.plan.job_id,other.job_id)
        self.assertNotEqual(self.plan.candidate_json,other.candidate_json)
