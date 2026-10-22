from unittest import TestCase
from src.validation.document_extraction_integration import fixture_sql
from src.application.campaign_document_store import PROJECT,SUBMISSIONS


class DocumentFixtureScopeTests(TestCase):
    def test_only_owned_tables_are_allowed(self):
        dataset=PROJECT+'.ci_document_it_123456abcdef'
        sql=f'SELECT * FROM `{dataset}.extractions`'
        self.assertEqual(fixture_sql(sql,dataset),sql)
        for bad in (SUBMISSIONS,dataset+'.turn_links','other.ci_document_it_123456abcdef.extractions'):
            with self.subTest(table=bad),self.assertRaises(ValueError):fixture_sql(f'SELECT * FROM `{bad}`',dataset)
        with self.assertRaises(ValueError):fixture_sql(sql,PROJECT+'.ci_application')
        with self.assertRaises(ValueError):fixture_sql(f'DELETE FROM `{dataset}.extractions` WHERE TRUE',dataset)
