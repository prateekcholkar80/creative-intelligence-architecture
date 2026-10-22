import copy
import json
import unittest
from unittest.mock import Mock, patch

from google.cloud import bigquery

from src.ingestion.generated_banner_reviews import (
    CONFIG, CURRENT, HISTORY, SCHEMA, STATE, SCOPE, ensure_tables, initialize_review,
    inspect_review, publish_views, record_decision, review_event, store_event, validate_config,
)
from src.generators.generated_banner_embeddings import CONFIG as EMBEDDING_CONFIG, EMBEDDING_VIEW, embedding_view_sql
from src.retrieval.generated_banner import retrieve_generated_banner
from tests import test_register_generated_banner, test_generated_banner_embeddings

MODULE='src.ingestion.generated_banner_reviews.'


class GeneratedBannerReviewTests(unittest.TestCase):
    def setUp(self):
        fixture=test_register_generated_banner.RegisterGeneratedBannerTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.asset=fixture.row;self.raw=fixture.preview.png
        self.config=json.loads(CONFIG.read_text())
        self.initial=review_event(self.asset,None,'PENDING_REVIEW','SYSTEM:REGISTRATION','Awaiting review',
                                  'initial-request',0,self.config,origin='SYSTEM')
        self.approved=review_event(self.asset,self.initial,'APPROVED_FOR_POC','Reviewer fixture','Composition approved',
                                   'approved-request',1,self.config)

    def test_system_only_initializes_pending_and_never_synthesizes_approval(self):
        self.assertEqual(self.initial['revision'],1);self.assertIsNone(self.initial['supersedes_review_id'])
        self.assertEqual(self.initial['review_status'],'PENDING_REVIEW');self.assertEqual(self.initial['approval_scope'],SCOPE)
        for status in ('APPROVED_FOR_POC','REJECTED'):
            with self.assertRaises(ValueError):
                review_event(self.asset,None,status,'SYSTEM:REGISTRATION','Reason','request-key',0,self.config,'SYSTEM')
        with self.assertRaises(ValueError):
            review_event(self.asset,self.initial,'PENDING_REVIEW','SYSTEM','Reason','request-key',0,self.config,'SYSTEM')

    def test_human_decisions_require_initialized_state_reviewer_reason_and_valid_bounds(self):
        values=dict(asset=self.asset,state=self.initial,status='APPROVED_FOR_POC',reviewer='Reviewer fixture',
                    notes='Reason',request_id='request-key',expected_revision=1,config=self.config)
        for field,value in (('reviewer',''),('reviewer','SYSTEM:BOT'),('reviewer','a\nb'),('notes',''),('notes','x'*2001),
                            ('status','REAL_BUSINESS_APPROVED'),('request_id','bad;sql'),('expected_revision',True),
                            ('expected_revision',-1),('state',None)):
            with self.subTest(field=field),self.assertRaises(ValueError):review_event(**{**values,field:value})
        validate_config(self.config)
        with self.assertRaises(ValueError):validate_config({**self.config,'api_key':'secret'})

    def test_identity_retry_key_and_payload_hash_preserve_current_asset_and_revision(self):
        event=self.approved
        self.assertEqual(set(event),{f.name for f in SCHEMA})
        self.assertEqual(event['revision'],2);self.assertEqual(event['supersedes_review_id'],self.initial['review_id'])
        self.assertEqual(event['asset_sha256'],self.asset['sha256']);self.assertEqual(event['gcs_generation'],self.asset['gcs_generation'])
        again=review_event(self.asset,self.initial,'APPROVED_FOR_POC','Reviewer fixture','Composition approved',
                            'approved-request',1,self.config)
        self.assertEqual(again['review_id'],event['review_id']);self.assertEqual(again['request_sha256'],event['request_sha256'])
        changed=review_event(self.asset,self.initial,'REJECTED','Reviewer fixture','Composition approved','approved-request',1,self.config)
        self.assertEqual(changed['review_id'],event['review_id']);self.assertNotEqual(changed['request_sha256'],event['request_sha256'])

    def test_schema_explicit_transaction_appends_history_and_updates_only_its_current_pointer(self):
        client=Mock();client.get_table.return_value=bigquery.Table(HISTORY,schema=SCHEMA)
        client.query.side_effect=[Mock(result=Mock(return_value=[])),Mock(result=Mock(return_value=[])),
                                 Mock(result=Mock(return_value=[self.approved]))]
        self.assertEqual(store_event(client,self.approved,self.config),self.approved)
        settings=client.load_table_from_json.call_args.kwargs['job_config']
        self.assertEqual(settings.schema,SCHEMA);self.assertFalse(settings.autodetect)
        sql=client.query.call_args_list[1].args[0]
        self.assertIn('BEGIN TRANSACTION',sql);self.assertIn('COMMIT TRANSACTION',sql)
        self.assertIn('INSERT INTO `'+HISTORY+'`',sql);self.assertIn('MERGE `'+STATE+'`',sql)
        self.assertIn('C.asset_id=S.asset_id',sql);self.assertIn('C.revision=S.expected_revision',sql)
        self.assertNotIn('DELETE',sql);self.assertNotIn('UPDATE `'+HISTORY+'`',sql)
        self.assertNotIn('MERGE `'+self.asset['sku']+'`',sql)
        client.delete_table.assert_called_once()

    def test_store_replays_original_event_without_writes_or_replacing_a_newer_state(self):
        client=Mock();client.query.return_value.result.return_value=[self.approved]
        self.assertEqual(store_event(client,self.approved,self.config),self.approved)
        client.create_table.assert_not_called();client.load_table_from_json.assert_not_called()
        altered={**self.approved,'request_sha256':'changed'}
        with self.assertRaises(ValueError):store_event(client,altered,self.config)

    def test_failed_transaction_cleans_only_its_stage(self):
        client=Mock();client.get_table.return_value=bigquery.Table(HISTORY,schema=SCHEMA)
        client.query.side_effect=[Mock(result=Mock(return_value=[])),RuntimeError('transaction conflict')]
        with self.assertRaisesRegex(RuntimeError,'conflict'):store_event(client,self.approved,self.config)
        client.delete_table.assert_called_once();self.assertIn('_stage_banner_review_',client.delete_table.call_args.args[0])

    def test_initialize_is_idempotent_and_never_overwrites_a_human_decision(self):
        with (patch(MODULE+'ensure_tables'),patch(MODULE+'read_state',return_value=self.approved),
              patch(MODULE+'store_event') as store):
            self.assertEqual(initialize_review(Mock(),self.asset,self.config),self.approved);store.assert_not_called()
        with (patch(MODULE+'ensure_tables'),patch(MODULE+'read_state',return_value=None),
              patch(MODULE+'store_event',side_effect=lambda client,event,config:event)):
            first=initialize_review(Mock(),self.asset,self.config);second=initialize_review(Mock(),self.asset,self.config)
        self.assertEqual(first['review_id'],second['review_id']);self.assertEqual(first['review_status'],'PENDING_REVIEW')

    def test_stale_human_decision_cannot_write(self):
        with (patch(MODULE+'read_asset',return_value=self.asset),patch(MODULE+'read_state',return_value=self.approved),
              patch(MODULE+'query',return_value=[]),patch(MODULE+'store_event') as store):
            with self.assertRaisesRegex(ValueError,'Stale review'):
                record_decision(Mock(),Mock(),self.asset['sku'],self.asset['asset_id'],'REJECTED',
                                'Reviewer fixture','Reason','new-request',1,self.config)
        store.assert_not_called()

    def test_decision_replay_returns_old_event_while_the_current_rejection_is_preserved(self):
        rejected=review_event(self.asset,self.approved,'REJECTED','Reviewer fixture','Changed decision','reject-request',2,self.config)
        with (patch(MODULE+'read_asset',return_value=self.asset),patch(MODULE+'read_state',return_value=rejected),
              patch(MODULE+'query',return_value=[self.approved]),patch(MODULE+'store_event') as store):
            result=record_decision(Mock(),Mock(),self.asset['sku'],self.asset['asset_id'],'APPROVED_FOR_POC',
                'Reviewer fixture','Composition approved','approved-request',1,self.config)
            self.assertEqual(result,self.approved);store.assert_not_called()

    def test_approval_requires_current_grounding_and_verified_original_but_rejection_remains_available(self):
        with (patch(MODULE+'read_asset',return_value=self.asset),patch(MODULE+'read_state',return_value=self.initial),
              patch(MODULE+'query',side_effect=[[],[]]),patch(MODULE+'download_banner') as download,
              patch(MODULE+'store_event') as store):
            with self.assertRaisesRegex(ValueError,'grounding'):
                record_decision(Mock(),Mock(),self.asset['sku'],self.asset['asset_id'],'APPROVED_FOR_POC',
                                'Reviewer fixture','Reason','new-request',1,self.config)
            download.assert_not_called();store.assert_not_called()
        with (patch(MODULE+'read_asset',return_value=self.asset),patch(MODULE+'read_state',return_value=self.initial),
              patch(MODULE+'query',side_effect=[[],[self.asset]]),patch(MODULE+'download_banner',side_effect=ValueError('image changed')),
              patch(MODULE+'store_event') as store):
            with self.assertRaisesRegex(ValueError,'image changed'):
                record_decision(Mock(),Mock(),self.asset['sku'],self.asset['asset_id'],'APPROVED_FOR_POC',
                                'Reviewer fixture','Reason','new-request',1,self.config)
            store.assert_not_called()
        with (patch(MODULE+'read_asset',return_value=self.asset),patch(MODULE+'read_state',return_value=self.initial),
              patch(MODULE+'query',return_value=[]),patch(MODULE+'download_banner') as download,
              patch(MODULE+'store_event',side_effect=lambda client,event,config:event)):
            event=record_decision(Mock(),Mock(),self.asset['sku'],self.asset['asset_id'],'REJECTED',
                                  'Reviewer fixture','Reason','new-request',1,self.config)
            self.assertEqual(event['review_status'],'REJECTED');download.assert_not_called()

    def test_pending_banner_is_available_for_direct_review_with_history_and_original_png(self):
        with (patch(MODULE+'read_asset',return_value=self.asset),patch(MODULE+'read_state',return_value=self.initial),
              patch(MODULE+'download_banner',return_value=self.raw),patch(MODULE+'query',side_effect=[[self.initial],[{}]])):
            report,raw=inspect_review(Mock(),Mock(),self.asset['sku'],self.asset['asset_id'],self.config)
        self.assertEqual(raw,self.raw);self.assertEqual(report['current_review']['review_status'],'PENDING_REVIEW')
        self.assertEqual(len(report['review_history']),1);self.assertTrue(report['original_png_verified'])

    def test_migration_replaces_only_known_generated_view_and_refuses_unrelated_definitions(self):
        config=json.loads(EMBEDDING_CONFIG.read_text());new=embedding_view_sql(config)
        current=bigquery.Table(CURRENT);current.view_query=(CONFIG.parents[1]/'sql/current_generated_asset_reviews.sql').read_text()
        legacy=(CONFIG.parents[1]/'sql/generated_banner_embeddings_before_reviews.sql').read_text().format(
            model=config['image_model'],dimension=512,location=config['location'],version=config['version'])
        target=bigquery.Table(EMBEDDING_VIEW);target.view_query=legacy
        final=bigquery.Table(EMBEDDING_VIEW);final.view_query=new
        client=Mock();client.get_table.side_effect=[current,target,final]
        publish_views(client,config);client.update_table.assert_called_once_with(target,['view_query'])
        client=Mock();target.view_query='SELECT unrelated';client.get_table.side_effect=[current,target]
        with self.assertRaises(ValueError):publish_views(client,config)
        client.update_table.assert_not_called()

    def test_reader_rejects_unapproved_rows_even_if_a_bad_view_returns_them(self):
        fixture=test_generated_banner_embeddings.GeneratedBannerEmbeddingTests();fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        row=fixture.retrieval_row();gcs,blob=fixture.gcs();client=Mock()
        for status in ('PENDING_REVIEW','REJECTED'):
            client.query.return_value.result.return_value=[{**row,'review_status':status}]
            with self.assertRaises(ValueError):retrieve_generated_banner(client,gcs,fixture.sku,fixture.asset_id,fixture.config)
        blob.download_as_bytes.assert_not_called()

    def test_schema_mismatch_cannot_be_silently_replaced(self):
        client=Mock();client.get_table.return_value=bigquery.Table(HISTORY,schema=[])
        with self.assertRaises(ValueError):ensure_tables(client)


if __name__=='__main__':
    unittest.main()
