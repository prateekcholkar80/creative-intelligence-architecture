"""Offline containment, fixture identity and native-race evidence guards."""
from copy import deepcopy
from datetime import datetime,timezone
from unittest import TestCase
from unittest.mock import Mock
from google.api_core.exceptions import NotFound
from src.application.campaign_quality_request_reader import partition_predicate
from src.application.campaign_quality_authority_reader import SQL
from src.application.conversation_contract import validate_conversation
from src.application.workflow_contract import validate_request
from src.validation.campaign_authority_fixtures import fixtures,identity
from src.validation.campaign_authority_integration import AuthorityProbe,Scope,TABLES,CASES,TTL_MS,PROJECT,LOCATION
from src.validation.prompt_evaluation_remaining import validate_race
from tests import test_campaign_quality_authority_writer as writer_fixtures


class AuthorityIntegrationTests(TestCase):
    def setUp(self):
        self.scope=Scope('a'*32)
        self.client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)

    def owned(self):
        return Mock(full_dataset_id=PROJECT+':'+self.scope.dataset.split('.')[1],location=LOCATION,
            labels=self.scope.labels,default_table_expiration_ms=TTL_MS)

    def test_fixture_catalog_scope_and_requests_validate_without_remapping(self):
        rows,contexts=fixtures()
        self.assertEqual(len(contexts),9);self.assertEqual(len(TABLES),14);self.assertEqual(len(CASES),12)
        for r in rows['requests']:
            c=next(c for c in rows['conversations'] if c['workspace_id']==r['workspace_id'])
            resource=validate_conversation(c,workspace_id=c['workspace_id'],conversation_id=c['conversation_id'])
            validate_request(r,conversation=resource,catalog=frozenset({'CI-SKU-0021'}))
        self.assertGreater(identity().expires_at,int(datetime.now(timezone.utc).timestamp()))

    def test_scope_relocates_every_reader_source_and_rejects_foreign_or_bare_sql(self):
        self.scope.assert_sql(self.scope.relocate(SQL))
        for sql in (SQL,'SELECT * FROM project.foreign.table',self.scope.relocate(SQL)+'; DROP TABLE x',
                    self.scope.relocate(SQL)+'; SELECT * FROM `other.foreign.table`'):
            with self.assertRaises(RuntimeError):self.scope.assert_sql(sql)

    def test_owned_cleanup_never_deletes_a_foreign_dataset(self):
        probe=AuthorityProbe(self.client);probe.scope=self.scope;probe.owned=True
        self.client.get_dataset.return_value=Mock(full_dataset_id=PROJECT+':ci_application')
        with self.assertRaises(RuntimeError):probe.cleanup()
        self.client.delete_dataset.assert_not_called()

    def test_owned_cleanup_checks_absence(self):
        probe=AuthorityProbe(self.client);probe.scope=self.scope;probe.owned=True
        self.client.get_dataset.side_effect=[self.owned(),NotFound('absent')]
        probe.cleanup();self.assertFalse(probe.owned)
        self.assertEqual(probe.report['cleanup'],'DELETED_AND_ABSENCE_VERIFIED')

    def test_expiry_location_or_labels_cannot_be_adopted(self):
        for key,value in (('labels',{}),('location','us'),('default_table_expiration_ms',None)):
            actual=self.owned();setattr(actual,key,value)
            with self.assertRaises(RuntimeError):self.scope.own(actual)

    def test_faults_preserve_native_plan_settings_and_have_distinct_owned_ids(self):
        fixture=writer_fixtures.AuthorityWriterTests('test_default_denies_before_source_reads')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        plan=fixture.prepare();probe=AuthorityProbe(self.client);probe.scope=self.scope
        for fault,code in (('rollback','FIXTURE_AUTHORITY_ROLLBACK'),('postcommit','FIXTURE_AUTHORITY_POSTCOMMIT')):
            scoped=probe.scoped(plan,fault=fault)
            self.scope.assert_sql(scoped.sql)
            self.assertEqual(scoped.config_json,plan.config_json)
            self.assertEqual(scoped.sql.count(code),1)
            self.assertTrue(scoped.job_id.startswith('authority_it_'+self.scope.run_id+'_'))
        self.assertIn(partition_predicate('requests'),probe.scoped(plan).sql)

    def test_nonoverlapping_or_unproven_race_cannot_pass(self):
        with self.assertRaises(RuntimeError):validate_race([],[],())

    def test_production_metadata_and_native_config_are_not_patched_at_import(self):
        from src.application import campaign_quality_authority_reader as reader
        from src.application.campaign_quality_authority import TABLE
        self.assertEqual(reader.TABLE,TABLE)
        self.assertIn(PROJECT+'.ci_application.',reader.SQL)
