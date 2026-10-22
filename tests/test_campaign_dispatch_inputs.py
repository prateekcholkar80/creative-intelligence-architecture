import json
from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_dispatch_inputs as store
from tests import test_campaign_planner_inputs as fixtures

class DispatchInputTests(TestCase):
    def setUp(self):
        self.f=fixtures.PlannerInputTests();self.f.setUp();self.p=self.f.prepare();self.b=self.f.resolved.binding
    def test_manifest_contains_no_raw_text_and_exact_schema(self):
        row=store.validate_prepared(self.p,self.b)
        self.assertEqual(set(row),{f.name for f in store.SPEC.schema})
        self.assertNotIn('hello',row['manifest_canonical_json'])
        self.assertEqual(store.parameter(self.p,self.b).value,json.dumps(row,sort_keys=True,separators=(',',':')))
    def test_mutation_and_scope_rebinding_deny(self):
        for p in (replace(self.p,prompt='changed'),replace(self.p,images=()),replace(self.p,options_json='{}')):
            with self.assertRaises(ValueError):store.validate_prepared(p,self.b)
        with self.assertRaises(ValueError):store.validate_prepared(self.p,replace(self.b,request_id='other'))
    def test_transaction_extension_and_readback_denials(self):
        sql=store.claim_sql('BEGIN TRANSACTION;\n-- PROBE_AFTER_FENCE\nCOMMIT TRANSACTION;')
        self.assertLess(sql.index('INSERT INTO'),sql.index('COMMIT TRANSACTION'))
        io=Mock();io.client.project='project-661d501a-e709-4276-a26'
        expected=store.validate_prepared(self.p,self.b)
        with patch.object(store,'validate_table'):
            io.read.return_value={'manifests':[json.dumps(expected)]}
            self.assertEqual(store.verify_stored(io,self.p,self.b),expected)
            for rows in ([],[json.dumps(expected)]*2,[json.dumps(expected|dict(manifest_sha256='f'*64))]):
                io.read.return_value={'manifests':rows}
                with self.assertRaises(ValueError):store.verify_stored(io,self.p,self.b)
