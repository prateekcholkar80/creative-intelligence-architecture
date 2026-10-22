"""Binding SQL/schema/config guards; native transaction behavior is unverified."""
from copy import deepcopy
from unittest.mock import Mock
import json
import re
import unittest

from google.api_core.exceptions import NotFound

from src.application.generation_admission import dispatch_sql
from src.application.prompt_runtime_contract import TABLES
from src.application.prompt_runtime_query import parameters,claim_sql,binding_from_parameters,source_rows
from src.application.prompt_runtime_service import ResolvedPrompt
from src.application.workflow_contract import ContractError
from src.ingestion.setup_prompt_runtime_tables import setup
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,SchemaMismatch
from tests.test_prompt_runtime_contract import binding,capture
from tests.test_setup_workflow_tables import dataset


class RuntimeQueryTests(unittest.TestCase):
    def test_complete_source_checks_fit_failed_job_recovery_bound(self):
        from src.application.prompt_runtime_reader import read_sources
        from src.application.prompt_evaluation_query import same_rows, json_row
        from src.application.sharing_jobs import CHILD_LIMIT
        for planner in (False, True):
            sql=claim_sql(dispatch_sql(planner))
            # Conservative statement bound includes unexecuted exception/end
            # statements and one extra injected failure after the transaction.
            self.assertLess(sql.count(';')+1, CHILD_LIMIT)
            self.assertEqual(sql.count("AS 'RUNTIME_PROMPT_CONFLICT'"),1)
            self.assertLess(len(sql.encode('utf-8')),1024**2)
            for name,spec,*_ in read_sources():
                expected=(f'(SELECT {json_row(spec.schema,"item")} '
                    f'FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@runtime_{name}_json))) item)')
                self.assertIn(same_rows('runtime_'+name+'_snapshot',expected),sql)
                self.assertNotIn('CREATE TEMP TABLE runtime_'+name+'_expected',sql)
            self.assertLess(sql.index("AS 'RUNTIME_PROMPT_CONFLICT'"),
                sql.index('INSERT INTO `'+PROJECT+'.ci_application.invocation_prompt_bindings`'))

    def test_atomic_claim_inserts_explicit_binding_and_fences_global_policy(self):
        sql=claim_sql(dispatch_sql(True))
        self.assertIn('UPDATE `'+PROJECT+'.ci_application.prompt_lifecycle_controls` SET control_digest=control_digest',sql)
        self.assertIn('RUNTIME_POLICY_EXPIRED',sql)
        self.assertIn('RUNTIME_PROMPT_CONFLICT',sql)
        self.assertIn('RUNTIME_BINDING_ALREADY_EXISTS',sql)
        self.assertIn('ROLLBACK TRANSACTION',sql)
        self.assertNotIn('DELETE ',sql)
        self.assertEqual(sql.count('INSERT INTO `'+PROJECT+'.ci_application.invocation_prompt_bindings`'),1)
        expected={p.name for p in parameters(ResolvedPrompt(binding(),capture()))}
        found={name for name in re.findall(r'(?<!@)@([a-z_]+)',sql) if name.startswith('runtime_')}
        self.assertEqual(found,expected)

    def test_binding_parameter_roundtrip_and_scope_corruption_deny(self):
        from google.cloud import bigquery
        b=binding();params=parameters(ResolvedPrompt(b,capture()))
        scope={name:getattr(b,name) for name in ('workspace_id','conversation_id','request_id','invocation_id')}
        scope['principal_id']=b.principal_id
        params += [bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in scope.items()]
        self.assertEqual(binding_from_parameters(params),b)
        with self.assertRaises(ContractError):binding_from_parameters(params[:-1])
        with self.assertRaises(ContractError):binding_from_parameters(params+[bigquery.ScalarQueryParameter('runtime_extra','STRING','x')])

    def test_expected_report_retains_original_control_anchor(self):
        c=capture();rows=source_rows(c)
        self.assertEqual(rows['prompt_evaluations'][0]['control_digest'],c.evaluation_control_anchor.digest)
        self.assertEqual(rows['target_heads'][0]['snapshot_sha256'],c.target.head.snapshot.snapshot_sha256)

    def test_setup_check_apply_and_idempotent_metadata_only(self):
        client=Mock(project=PROJECT,location=LOCATION);client.get_dataset.return_value=dataset()
        tables={}
        def get(ref,**kwargs):
            if ref.table_id not in tables:raise NotFound('missing')
            return tables[ref.table_id]
        def create(table,**kwargs):
            table._properties.update(type='TABLE',location=LOCATION);tables[table.table_id]=table;return table
        client.get_table.side_effect=get;client.create_table.side_effect=create
        self.assertEqual(setup(client)['status'],'MISSING_RESOURCES');client.create_table.assert_not_called()
        self.assertEqual(setup(client,apply=True)['created_tables'],['invocation_prompt_bindings'])
        self.assertEqual(setup(client,apply=True)['created_tables'],[])
        client.query.assert_not_called()
        self.assertTrue(TABLES['invocation_prompt_bindings'].append_only)
        table=tables['invocation_prompt_bindings'];table.schema=table.schema[:-1]
        with self.assertRaises(SchemaMismatch):setup(client,apply=True)


if __name__=='__main__':unittest.main()
