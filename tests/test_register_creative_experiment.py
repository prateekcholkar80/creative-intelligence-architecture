import copy
import json
import unittest
from datetime import datetime,timezone
from unittest.mock import Mock,patch

from google.cloud import bigquery

from src.ingestion.register_creative_experiment import (
    SCHEMA,TARGET,load_draft,register_plan,registration_row,validate_row,verify_evidence_gate,
)
from src.ingestion.generated_banner_reviews import CONFIG
from tests import test_creative_experiment_plan

MODULE='src.ingestion.register_creative_experiment.'


class ExperimentRegistrationTests(unittest.TestCase):
    def setUp(self):
        fixture=test_creative_experiment_plan.ExperimentPlanTests();fixture.setUp()
        self.plan=fixture.assemble()
        self.plan.update(approval_recheck_passed=True,evidence_checked_at=datetime.now(timezone.utc))
        self.row=registration_row(self.plan)
        self.config=json.loads(CONFIG.read_text())

    def client(self):
        client=Mock();client.get_table.return_value=bigquery.Table(TARGET,schema=SCHEMA)
        stored={**self.row,'created_at':datetime(2026,1,1,tzinfo=timezone.utc)}
        stats=dict(experiment_rows=1,invalid_drafts=0,stale_evidence=0,eligible_arms=2)
        client.query.side_effect=[Mock(result=Mock(return_value=[])),Mock(result=Mock(return_value=[stats])),
                                  Mock(result=Mock(return_value=[stored]))]
        return client,stored

    def test_row_preserves_provenance_unlaunched_status_and_unset_delivery(self):
        self.assertEqual(self.row['status'],'DRAFT_NOT_LAUNCHED')
        self.assertEqual(self.row['source_system'],'SYNTHETIC_POC')
        self.assertEqual([a['allocation_percent'] for a in self.row['arms']],[50,50])
        self.assertFalse(self.row['real_business_approved'])
        self.assertEqual(validate_row(self.row)['experiment_id'],self.plan['experiment_id'])
        for name in ('winner','budget_per_arm','start_date','end_date','currency'):
            self.assertIsNone(self.row[name])

    def test_timestamps_do_not_change_immutable_payload_on_rerun(self):
        plan={**self.plan,'evidence_checked_at':datetime(2026,2,1,tzinfo=timezone.utc)}
        row=registration_row(plan)
        self.assertEqual(row['experiment_id'],self.row['experiment_id'])
        self.assertEqual(row['plan_json'],self.row['plan_json'])

    def test_tampered_content_nonpoc_and_mismatched_arm_metadata_rejected(self):
        for key,value in (('plan_sha256','0'*64),('source_system','ACTUAL'),('sku','CI-SKU-0002'),
                          ('winner','B'),('status','LAUNCHED'),('real_business_approved',True)):
            with self.subTest(key=key),self.assertRaises(ValueError):validate_row({**self.row,key:value})
        row=copy.deepcopy(self.row);row['arms'][0]['review_revision']+=1
        with self.assertRaises(ValueError):validate_row(row)
        with self.assertRaises(ValueError):registration_row({**self.plan,'approval_recheck_passed':False})

    def test_explicit_destination_schema_insert_only_transaction_and_timestamp_preservation(self):
        client,stored=self.client()
        with patch(MODULE+'create_view'):
            self.assertEqual(load_draft(client,self.row,self.config),stored)
        job=client.load_table_from_json.call_args.kwargs['job_config']
        self.assertEqual(job.schema,SCHEMA);self.assertFalse(job.autodetect)
        self.assertEqual(job.write_disposition,'WRITE_EMPTY')
        sql=client.query.call_args_list[0].args[0]
        self.assertIn('BEGIN TRANSACTION',sql);self.assertIn('COMMIT TRANSACTION',sql)
        self.assertIn('WHEN NOT MATCHED THEN INSERT',sql)
        self.assertNotIn('UPDATE',sql);self.assertNotIn('DELETE',sql)
        self.assertIn('E.review_revision=arm.review_revision',sql)
        self.assertIn('T.source_system=S.source_system',sql)
        self.assertIn('TO_JSON_STRING(T.arms)',sql)
        client.delete_table.assert_called_once()

    def test_schema_mismatch_refuses_loading_or_altering_existing_table(self):
        client,_=self.client();client.get_table.return_value=bigquery.Table(TARGET,schema=[])
        with self.assertRaisesRegex(ValueError,'schema differs'):load_draft(client,self.row,self.config)
        client.load_table_from_json.assert_not_called();client.update_table.assert_not_called()

    def test_stage_cleanup_on_atomic_source_failure(self):
        client,_=self.client();client.query.side_effect=ValueError('Review revoked in transaction')
        with patch(MODULE+'create_view'),self.assertRaisesRegex(ValueError,'revoked'):
            load_draft(client,self.row,self.config)
        client.delete_table.assert_called_once()

    def test_registration_rebuilds_from_cloud_without_local_factual_fallback(self):
        with (patch(MODULE+'generate_plan',return_value=(self.plan,{})) as generate,
              patch(MODULE+'load_draft',return_value=self.row) as load,
              patch('pathlib.Path.read_text',side_effect=AssertionError('No local factual fallback'))):
            result=register_plan(Mock(),Mock(),{}, {},self.config,self.plan['experiment_id'])
        generate.assert_called_once();load.assert_called_once();self.assertEqual(result,self.row)

    def test_changed_evidence_vs_reviewed_draft_aborts_before_cloud_write(self):
        with (patch(MODULE+'generate_plan',return_value=(self.plan,{})),patch(MODULE+'load_draft') as load):
            with self.assertRaisesRegex(ValueError,'reviewed draft'):
                register_plan(Mock(),Mock(),{}, {},self.config,'OTHER')
        load.assert_not_called()

    def test_source_gate_probes_are_select_only_using_actual_view_sql(self):
        with patch(MODULE+'query',side_effect=lambda client,sql,config,**kwargs:[{
                'n':0 if 'REPLACE' in sql else 1}]) as reader:
            results=verify_evidence_gate(Mock(),self.row['experiment_id'],self.config)
        self.assertEqual(len(results),10);self.assertEqual(results['current_draft'],1)
        for call in reader.call_args_list:
            sql=call.args[1]
            self.assertNotIn(';',sql)
            self.assertIn('poc_generated_banner_embeddings',sql)
            self.assertNotIn('INSERT',sql);self.assertNotIn('DELETE',sql);self.assertNotIn('UPDATE',sql)


if __name__=='__main__':
    unittest.main()
