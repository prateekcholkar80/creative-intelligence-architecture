from unittest import TestCase
from src.validation.campaign_evaluator_integration import fixture_sql
from src.application.campaign_evaluator_store import PROJECT


class EvaluatorFixtureScopeTests(TestCase):
    def test_foreign_namespace_table_and_destructive_sql_are_rejected(self):
        dataset=PROJECT+'.ci_evaluator_it_123456abcdef';sql=f'SELECT * FROM `{dataset}.intents`'
        self.assertEqual(fixture_sql(sql,dataset),sql)
        for bad in (PROJECT+'.ci_application.campaign_evaluator_intents',dataset+'.other','other.dataset.intents'):
            with self.assertRaises(ValueError):fixture_sql(f'SELECT * FROM `{bad}`',dataset)
        with self.assertRaises(ValueError):fixture_sql(sql,PROJECT+'.ci_application')
        with self.assertRaises(ValueError):fixture_sql(f'DELETE FROM `{dataset}.intents` WHERE TRUE',dataset)
