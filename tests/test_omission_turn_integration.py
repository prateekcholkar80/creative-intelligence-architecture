from unittest import TestCase
from dataclasses import replace
from types import SimpleNamespace
from src.validation.omission_turn_integration import relocate
from tests.test_campaign_omission_turn import fixture
from src.application.grounded_interpreter_submission import prepare_grounded_submission
from src.application.campaign_omission_turn import bind_omission_plan
from src.validation.workflow_writer_integration import Scope


class OmissionNativeScopeTests(TestCase):
    def test_all_production_references_are_relocated_and_fault_stays_in_transaction(self):
        inputs,context,selection,history=fixture()
        plan=bind_omission_plan(prepare_grounded_submission(selection=selection,history=history,**inputs),context)
        probe=SimpleNamespace(scope=Scope('a'*32))
        moved=relocate(probe,plan,'rollback','rollback')
        self.assertNotIn('.ci_application.',moved.sql)
        self.assertNotIn('.ci_brand_memory.',moved.sql)
        self.assertNotIn('.ci_product.',moved.sql)
        self.assertIn(probe.scope.dataset+'.campaign_evidence_turn_links',moved.sql)
        self.assertLess(moved.sql.index("ASSERT FALSE AS 'SUBMIT_FIXTURE_ROLLBACK'"),moved.sql.index('COMMIT TRANSACTION;'))

    def test_unrelated_reference_or_destructive_sql_is_rejected(self):
        inputs,context,selection,history=fixture()
        plan=bind_omission_plan(prepare_grounded_submission(selection=selection,history=history,**inputs),context)
        probe=SimpleNamespace(scope=Scope('a'*32))
        for added in ('; SELECT * FROM `other.dataset.table`', '; DELETE FROM `'+probe.scope.dataset+'.submissions` WHERE TRUE'):
            with self.subTest(added=added),self.assertRaises(ValueError):relocate(probe,replace(plan,sql=plan.sql+added),'bad')
