"""Focused DRAFT configuration safeguards; live setup verified separately."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
import unittest
from google.api_core.exceptions import NotFound
from src.ingestion.configure_campaign_acceptance import (
    manifest, table, write_spec, preflight, verify, configure,
    PROJECT, LOCATION, HEADER, RULES, SOURCE, SOURCES,
)
from src.ingestion.setup_workflow_tables import SchemaMismatch
from tests.test_campaign_acceptance_contract import NOW


class ConfigureAcceptanceTests(unittest.TestCase):
    def test_new_version_preserves_original_and_pins_explicit_prohibitions(self):
        import json
        first=manifest();second=manifest(SOURCES['draft-v0.2'],version='draft-v0.2')
        self.assertEqual(first[0]['specification_sha256'],'6c3e95a6a33e2a6993736db5407860b0367d9aae1f1be839f63c8a4e38ac6e18')
        self.assertNotEqual(first[0]['specification_sha256'],second[0]['specification_sha256'])
        header=json.loads(second[0]['payload_canonical_json'])
        self.assertEqual(header['benchmark_countries'],['AU','CA','GB','US'])
        self.assertEqual(header['brand_directives']['ai_people'],'PROHIBITED')
        self.assertEqual(header['brand_directives']['before_after_imagery'],'PROHIBITED')
        self.assertEqual(header['status'],'DRAFT');self.assertIsNone(header['approved_by'])
        self.assertFalse(header['serving_enabled'])
        a={r['rule_id']:json.loads(r['payload_canonical_json']) for r in first[1:]}
        b={r['rule_id']:json.loads(r['payload_canonical_json']) for r in second[1:]}
        self.assertEqual([key for key in a if a[key]!=b[key]],['C07'])
        self.assertIn('genuine before/after with consent',b['C07']['acceptance_rule'])

    def test_new_sql_is_scoped_to_new_version_and_mixed_or_approved_rows_deny(self):
        rows=manifest(SOURCES['draft-v0.2'],version='draft-v0.2')
        sql,cfg=write_spec(rows,version='draft-v0.2')
        self.assertEqual(cfg.query_parameters[1].value,'draft-v0.2')
        self.assertNotIn('UPDATE',sql);self.assertNotIn('DELETE',sql)
        for altered in (rows[:-1],[dict(rows[0],version='draft-v0.1')]+rows[1:],
            [dict(rows[0],status='APPROVED')]+rows[1:]):
            with self.assertRaises(ValueError):write_spec(altered,version='draft-v0.2')
        with self.assertRaises(ValueError):write_spec(rows)
        with self.assertRaises(ValueError):configure(Mock(),version='approved-v1')

    def test_new_version_verification_reads_only_its_full_authoritative_rows(self):
        rows=manifest(SOURCES['draft-v0.2'],version='draft-v0.2');client=Mock()
        client.query.side_effect=[Mock(result=Mock(return_value=[dict(rows[0],recorded_at=NOW)])),
            Mock(result=Mock(return_value=[dict(r,recorded_at=NOW) for r in rows[1:]]))]
        self.assertEqual(verify(client,rows,version='draft-v0.2')['version'],'draft-v0.2')
        for call in client.query.call_args_list:self.assertEqual(call.kwargs['job_config'].query_parameters[1].value,'draft-v0.2')

    def test_manifest_exact_draft_without_approval_or_activation(self):
        rows = manifest()
        self.assertEqual(len(rows), 48)
        self.assertEqual(len({r['rule_id'] for r in rows[1:]}), 47)
        import json
        head = json.loads(rows[0]['payload_canonical_json'])
        self.assertFalse(head['serving_enabled'])
        self.assertIsNone(head['approved_by'])
        self.assertEqual(head['status'], 'DRAFT')
        self.assertEqual(manifest(), rows)

    def test_duplicate_missing_or_malformed_specification_denied(self):
        raw = SOURCE.read_text(); line = next(l for l in raw.splitlines() if l.startswith('| A01 |'))
        with TemporaryDirectory() as d:
            path = Path(d)/'policy.md'
            for contents in (raw+'\n'+line, raw.replace(line,''), raw.replace(line,'| A01 | invalid |')):
                path.write_text(contents)
                with self.assertRaises(ValueError): manifest(path)

    def test_schema_exact_required_draft_columns(self):
        for name in (HEADER, RULES):
            spec = table(name)
            self.assertEqual(spec.project, PROJECT)
            self.assertEqual(spec.dataset_id, 'ci_brand_memory')
            self.assertTrue(all(f.mode == 'REQUIRED' for f in spec.schema))
            self.assertIsNone(spec.expires)
            self.assertEqual(spec.clustering_fields, ['policy_id','version'])

    def test_sql_atomic_explicit_and_scoped_without_updates_or_deletes(self):
        sql, cfg = write_spec(manifest())
        self.assertIn('BEGIN TRANSACTION', sql)
        self.assertIn('COMMIT TRANSACTION', sql)
        for assertion in ('HEADER_DUPLICATE','RULE_PARTIAL_STATE','POLICY_PARTIAL_STATE',
                          'HEADER_CONFLICT','RULE_CONFLICT','RULE_DUPLICATE'):
            self.assertIn(assertion,sql)
        self.assertNotIn('DELETE',sql); self.assertNotIn('UPDATE',sql)
        self.assertFalse(cfg.use_query_cache)
        self.assertEqual(cfg.job_timeout_ms,'60000')
        self.assertEqual(len(cfg.query_parameters[-1].values),47)

    def test_full_row_verification_rejects_missing_duplicate_changed_payload(self):
        rows=manifest()
        def client_for(items):
            client=Mock()
            head=[dict(r, recorded_at=NOW) for r in items if 'rule_id' not in r]
            rules=[dict(r, recorded_at=NOW) for r in items if 'rule_id' in r]
            client.query.side_effect=[Mock(result=Mock(return_value=head)),
                                      Mock(result=Mock(return_value=rules))]
            return client
        self.assertEqual(verify(client_for(rows),rows)['rule_rows'],47)
        for altered in (rows[:-1], rows+rows[-1:],
                        [dict(rows[0],status='APPROVED')]+rows[1:],
                        [dict(rows[0],payload_canonical_json='{}')]+rows[1:]):
            with self.assertRaises(ValueError): verify(client_for(altered),rows)

    def test_read_only_missing_tables_cannot_write(self):
        client=Mock()
        with patch('src.ingestion.configure_campaign_acceptance.preflight',return_value=[HEADER,RULES]):
            result=configure(client)
        self.assertEqual(result['status'],'MISSING_TABLES')
        client.query.assert_not_called(); client.create_table.assert_not_called()

    def test_preflight_all_tables_before_creation_and_refuses_schema_conflict(self):
        client=Mock(project=PROJECT)
        client.get_dataset.return_value=Mock(project=PROJECT,dataset_id='ci_brand_memory',
            location=LOCATION,default_table_expiration_ms=None,default_partition_expiration_ms=None)
        client.get_table.side_effect=[NotFound('absent'), Mock()]
        with patch('src.ingestion.configure_campaign_acceptance.validate_table',side_effect=SchemaMismatch('bad')):
            with self.assertRaises(SchemaMismatch): preflight(client,apply=True)
        client.create_table.assert_not_called()

    def test_original_job_mismatch_does_not_submit_or_replay(self):
        client=Mock(project=PROJECT)
        client.get_job.return_value=Mock(project=PROJECT,location=LOCATION,job_id='wrong',
                                         to_api_repr=Mock(return_value={}))
        with patch('src.ingestion.configure_campaign_acceptance.preflight',return_value=[]):
            with self.assertRaises(ValueError):configure(client,apply=True)
        client.query.assert_not_called()


if __name__=='__main__': unittest.main()
