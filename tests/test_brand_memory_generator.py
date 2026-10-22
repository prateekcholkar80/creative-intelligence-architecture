import json
import unittest
from unittest.mock import MagicMock

from src.common.bq_merge import merge_statement, upsert
from src.generators.brand_memory_generator import (
    ROOT, RULE_SCHEMA, CLAIM_SCHEMA, build_memory, approved_view_sql,
)


class BrandMemoryTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT/'config/brand_memory.json').read_text())
        self.products = [dict(sku=f'CI-SKU-{i:04d}',brand='Source brand',
            product_name=f'Source product {i}',source_system='BEAUTYFEEDS') for i in range(1,21)]
        self.ingredients = [dict(sku=p['sku'],ingredient_id='ING-1',ingredient_sequence=1,
            inci_name='Aqua',source_system='BEAUTYFEEDS') for p in self.products]

    def test_counts_determinism_and_provenance(self):
        rules,claims = build_memory(self.products,self.ingredients,self.config)
        self.assertEqual((rules,claims),build_memory(self.products[::-1],self.ingredients[::-1],self.config))
        self.assertEqual(len(rules),7)
        self.assertEqual(len(claims),60)
        self.assertEqual(len({c['claim_id'] for c in claims}),60)
        for status in ('APPROVED','REJECTED','PENDING_REVIEW'):
            self.assertEqual(sum(c['approval_status']==status for c in claims),20)
        for rule in rules:
            self.assertEqual(rule['authority'],'PROJECT_POC_POLICY')
            self.assertEqual(rule['source_system'],'SYNTHETIC_POC')
            self.assertEqual(set(rule),{f.name for f in RULE_SCHEMA})
        for claim in claims:
            self.assertEqual(claim['source_system'],'SYNTHETIC_POC')
            self.assertEqual(claim['evidence_source_system'],'BEAUTYFEEDS')
            self.assertEqual(claim['approval_scope'],'SYNTHETIC_POC_ONLY')
            self.assertEqual(set(claim),{f.name for f in CLAIM_SCHEMA})
            if claim['approval_status']=='APPROVED':
                self.assertEqual(claim['claim_type'],'PRODUCT_NAME')
                self.assertEqual(claim['approved_by'],'SYNTHETIC_POC_REVIEWER')
            else:
                self.assertIsNone(claim['approved_by'])
                self.assertIsNone(claim['approved_at'])

    def test_no_synthetic_benefit_approval(self):
        self.config['review_fixture']['statuses_by_type']['INGREDIENT_PRESENCE']='APPROVED'
        with self.assertRaises(ValueError):
            build_memory(self.products,self.ingredients,self.config)

    def test_invalid_identity_missing_evidence_and_false_authority(self):
        with self.assertRaises(ValueError):
            build_memory(self.products[:-1],self.ingredients,self.config)
        with self.assertRaises(ValueError):
            build_memory(self.products,self.ingredients[:-1],self.config)
        self.config['review_fixture']['reviewer']='Manufacturer'
        with self.assertRaises(ValueError):
            build_memory(self.products,self.ingredients,self.config)

    def test_fact_changes_generate_new_claim_ids(self):
        _, before=build_memory(self.products,self.ingredients,self.config)
        self.products[0]['product_name']='Updated product name'
        _, after=build_memory(self.products,self.ingredients,self.config)
        self.assertNotEqual(before[0]['claim_id'],after[0]['claim_id'])

    def test_view_requires_approval_evidence_and_poc_scope(self):
        sql=approved_view_sql()
        for condition in ("approval_status='APPROVED'", "approval_scope='SYNTHETIC_POC_ONLY'",
                          "claim_type='PRODUCT_NAME'", 'approved_at <= CURRENT_TIMESTAMP()',
                          'evidence_excerpt=p.product_name', 'approval_reference'):
            self.assertIn(condition,sql)

    def test_review_decisions_preserved_and_schema_explicit(self):
        _,claims=build_memory(self.products,self.ingredients,self.config)
        sql=merge_statement('project.dataset.claims','project.dataset.stage',list(claims[0]),
                            ['claim_id','source_system'],True)
        self.assertIn("WHEN MATCHED AND T.approval_status='PENDING_REVIEW'",sql)
        self.assertNotIn('T.approved_by=',sql)
        self.assertNotIn('DELETE',sql)
        client=MagicMock()
        client.get_table.return_value.schema=CLAIM_SCHEMA
        client.query.return_value.result.return_value=[]
        upsert(client,'project.dataset.claims',CLAIM_SCHEMA,claims,['claim_id','source_system'],True)
        self.assertEqual(client.load_table_from_dataframe.call_args.kwargs['job_config'].schema,CLAIM_SCHEMA)
        client.delete_table.assert_called_once()

    def test_stage_cleanup_on_failure(self):
        rules,_=build_memory(self.products,self.ingredients,self.config)
        client=MagicMock()
        client.get_table.return_value.schema=RULE_SCHEMA
        client.load_table_from_dataframe.side_effect=RuntimeError('failed')
        with self.assertRaises(RuntimeError):
            upsert(client,'project.dataset.rules',RULE_SCHEMA,rules,['rule_id','source_system'])
        client.delete_table.assert_called_once()


if __name__=='__main__':
    unittest.main()
