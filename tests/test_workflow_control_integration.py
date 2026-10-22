"""Native probe ownership guards, tested offline with no ADC access."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from google.api_core.exceptions import NotFound

from src.application.generation_admission import dispatch_sql
from src.application.management_service import bootstrap_sql, management_sql
from src.application.workflow_schema import TABLES
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.validation.workflow_control_integration import Scope, Probe, TTL_MS
from src.validation.control_fixtures import workflow
from src.application.workflow_contract import validate_model_invocation
from src.application.conversation_contract import validate_conversation
from tests.test_management_service import client


class ProbeTests(unittest.TestCase):
    def setUp(self):self.scope=Scope('a'*32)

    def dataset(self,**changes):
        return SimpleNamespace(full_dataset_id=PROJECT+':'+self.scope.dataset.split('.')[1],location=LOCATION,
            labels=self.scope.labels,default_table_expiration_ms=TTL_MS,**changes)

    def test_scope_rejects_arbitrary_run_table_and_serving_sql(self):
        for value in ('bad','../path','b'*31):
            with self.assertRaises(RuntimeError):Scope(value)
        with self.assertRaises(RuntimeError):self.scope.table('product_master')
        with self.assertRaises(RuntimeError):self.scope.sql('SELECT * FROM `foreign.data.table`')

    def test_every_installed_template_relocates_only_allowlisted_tables(self):
        for sql in (bootstrap_sql(),management_sql('MEMBER','memberships',True),management_sql('ARCHIVE','conversations',False),dispatch_sql(False),dispatch_sql(True)):
            relocated=self.scope.sql(sql)
            self.assertNotIn('.ci_application.',relocated);self.assertIn(self.scope.dataset,relocated)
            self.assertNotIn('DELETE ',relocated)

    def test_ownership_requires_exact_labels_region_expiry_and_dataset(self):
        self.scope.own(self.dataset())
        for field,value in (('labels',{}),('location','us'),('default_table_expiration_ms',1),('full_dataset_id','other')):
            record=self.dataset();setattr(record,field,value)
            with self.assertRaises(RuntimeError):self.scope.own(record)

    def test_cleanup_refuses_unowned_dataset(self):
        c=client();self.addCleanup(c.close);probe=Probe(c);probe.owned=True
        c.get_dataset=Mock(return_value=self.dataset());c.delete_dataset=Mock()
        with self.assertRaises(RuntimeError):probe.cleanup()
        c.delete_dataset.assert_not_called()

    def test_cleanup_verifies_absence_and_deletes_only_owned_scope(self):
        c=client();self.addCleanup(c.close);probe=Probe(c);probe.scope=self.scope;probe.owned=True
        c.get_dataset=Mock(side_effect=[self.dataset(),NotFound('absent')]);c.delete_dataset=Mock()
        probe.cleanup();self.assertFalse(probe.owned);self.assertEqual(probe.report['cleanup'],'DELETED_AND_ABSENCE_VERIFIED')
        self.assertEqual(c.delete_dataset.call_args.args,(self.scope.dataset,))

    def test_fixture_workflow_profiles_and_parent_rows_satisfy_real_contracts(self):
        for stage in ('PLANNER','INTERPRETER'):
            rows=workflow('isolated-workspace',stage);conv=rows['conversations'][0]
            resource=validate_conversation(conv,workspace_id=conv['workspace_id'],conversation_id=conv['conversation_id'])
            validate_model_invocation(rows['model_invocations'][0],request=rows['requests'][0],conversation=resource,
                job=rows.get('jobs',[None])[0],attempt=rows.get('job_attempts',[None])[0])

    def test_fixture_control_schema_preserves_partition_filters(self):
        for name in ('requests','job_attempts','model_invocations','audit_events'):
            self.assertTrue(TABLES[name].table(PROJECT).require_partition_filter)


if __name__=='__main__':unittest.main()
