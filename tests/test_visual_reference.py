import copy
import hashlib
import unittest
from unittest.mock import Mock, patch

from src.generators.sales_generator import PROJECT
from src.retrieval.visual_reference import (
    SECTIONS, SQL, VisualReferenceRead, build_packet, capture_visual_reference,
    retrieve_visual_reference,
)


class VisualReferenceTests(unittest.TestCase):
    def setUp(self):
        self.reference='NAKED_AND_THRIVING'
        self.rows=[]
        for section in sorted(SECTIONS):
            text=f'Observed visual reference: {section}. Styling inspiration only.'
            self.rows.append(dict(snapshot_id='snapshot-1',chunk_id=section,chunk_type='WEBSITE_VISUAL_REFERENCE',
                brand='Naked & Thriving',sku=None,claim_id=None,campaign_id=None,asset_id=None,
                chunk_text=text,source_system='OFFICIAL_BRAND_WEBSITE',
                source_reference=f'visual-reference#{section}',evidence_references=['https://www.nakedandthriving.com/'],
                image_uris=[],retrieval_status='VISUAL_REFERENCE',approval_scope='VISUAL_INSPIRATION_ONLY',
                content_sha256=hashlib.sha256(text.encode()).hexdigest()))

    def test_complete_style_preserves_evidence_and_order(self):
        original=copy.deepcopy(self.rows)
        packet=build_packet(self.reference,self.rows)
        self.assertEqual(packet,build_packet(self.reference,self.rows[::-1]))
        self.assertEqual(packet['approval_scope'],'VISUAL_INSPIRATION_ONLY')
        self.assertEqual(len(packet['visual_chunks']),4)
        self.assertEqual(self.rows,original)
        self.assertEqual(packet['visual_chunks'][0]['evidence_references'],original[0]['evidence_references'])

    def test_invalid_scope_provenance_and_hash_rejected(self):
        for key,value in (('sku','CI-SKU-0001'),('claim_id','claim-1'),('campaign_id','campaign-1'),
                          ('asset_id','asset-1'),('approval_scope','APPROVED_BRAND_GUIDELINES'),
                          ('retrieval_status','REVIEW_ONLY'),('source_system','SYNTHETIC_POC'),
                          ('brand','Other brand'),('chunk_type','PRODUCT_KNOWLEDGE'),
                          ('content_sha256','invalid'),('evidence_references',[]),
                          ('source_reference','visual-reference#benefit_claims')):
            rows=copy.deepcopy(self.rows);rows[0][key]=value
            with self.subTest(field=key),self.assertRaises(ValueError):build_packet(self.reference,rows)

    def test_missing_duplicate_or_mixed_snapshot_rejected(self):
        for rows in ([],self.rows[:3],self.rows+self.rows):
            with self.assertRaises(ValueError):build_packet(self.reference,rows)
        rows=copy.deepcopy(self.rows);rows[0]['snapshot_id']='older'
        with self.assertRaises(ValueError):build_packet(self.reference,rows)

    def test_none_skips_query_and_unknown_reference_rejected(self):
        client=Mock()
        self.assertEqual(retrieve_visual_reference(client,'NONE')['visual_chunks'],[])
        with self.assertRaises(ValueError):build_packet('NONE',self.rows)
        for reference in ('OTHER',None,[]):
            with self.assertRaises(ValueError):retrieve_visual_reference(client,reference)
        client.query.assert_not_called()

    def test_read_only_parameterized_current_bigquery_view(self):
        client=Mock();query_job=client.query.return_value;query_job.result.return_value=self.rows
        query_job.job_id='bq-visual-read-1';query_job.project=PROJECT;query_job.location='asia-south1'
        packet=retrieve_visual_reference(client,self.reference,maximum_bytes_billed=80_000_000)
        self.assertEqual(len(packet['visual_chunks']),4)
        arguments=client.query.call_args
        self.assertEqual(arguments.kwargs['location'],'asia-south1')
        config=arguments.kwargs['job_config']
        self.assertEqual(config.maximum_bytes_billed,80_000_000)
        self.assertFalse(config.use_query_cache)
        self.assertEqual(config.query_parameters[0].value,'Naked & Thriving')
        self.assertIn('poc_visual_reference_chunks',arguments.args[0])
        self.assertIn('brand=@brand',arguments.args[0])
        for mutation in ('CREATE','MERGE','INSERT','UPDATE','DELETE','DROP'):
            self.assertNotIn(mutation,arguments.args[0])
        client.create_table.assert_not_called()
        client.load_table_from_dataframe.assert_not_called()

    def test_capture_binds_packet_to_native_job_and_none_has_no_fake_read(self):
        client=Mock();query_job=client.query.return_value;query_job.result.return_value=self.rows
        query_job.job_id='bq-visual-read-1';query_job.project=PROJECT;query_job.location='asia-south1'
        captured=capture_visual_reference(client,self.reference)
        self.assertIsInstance(captured,VisualReferenceRead)
        self.assertEqual(captured.query_job_id,'bq-visual-read-1')
        self.assertEqual((captured.project,captured.location),(PROJECT,'asia-south1'))
        self.assertIsNotNone(captured.observed_at.tzinfo)
        with self.assertRaises(ValueError):capture_visual_reference(client,'NONE')

    def test_capture_rejects_wrong_native_job_identity(self):
        client=Mock();query_job=client.query.return_value;query_job.result.return_value=self.rows
        query_job.job_id='bq-visual-read-1';query_job.project=PROJECT;query_job.location='US'
        with self.assertRaises(ValueError):capture_visual_reference(client,self.reference)

    def test_bigquery_failure_has_no_local_json_fallback(self):
        client=Mock();client.query.side_effect=RuntimeError('BigQuery unavailable')
        sql=SQL.read_text()
        def read_sql_only(path,*args,**kwargs):
            self.assertEqual(path,SQL,'Do not read local research or exports for retrieval')
            return sql
        with patch('pathlib.Path.read_text',new=read_sql_only):
            with self.assertRaisesRegex(RuntimeError,'BigQuery unavailable'):
                retrieve_visual_reference(client,self.reference)

    def test_invalid_billing_cap_rejected_before_query(self):
        client=Mock()
        for cap in (0,-1,True,'unlimited'):
            with self.assertRaises(ValueError):retrieve_visual_reference(client,self.reference,cap)
        client.query.assert_not_called()


if __name__=='__main__':unittest.main()
