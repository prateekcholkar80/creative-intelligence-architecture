"""Exact IAM deltas, unrelated preservation and uncertain responses; offline."""
from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock
from google.cloud import bigquery
from google.api_core.iam import Policy
from src.ingestion import setup_marketing_planner_access as access


class MarketingPlannerAccessTests(TestCase):
    def policy(self):
        p=Policy();p.etag=b'original';p.version=1
        p.bindings=[dict(role='roles/bigquery.dataViewer',members={'user:unrelated@example.com'})]
        return p

    def test_table_proposals_only_add_fixed_runtime_and_preserve_original(self):
        for table,role in access.TABLE_ROLES.items():
            original=self.policy();before=deepcopy(original)
            proposed,changed=access.table_proposal(original,table)
            self.assertTrue(changed);self.assertEqual(original.bindings,before.bindings)
            self.assertEqual(proposed.etag,original.etag)
            self.assertEqual(proposed.bindings[0],original.bindings[0])
            self.assertEqual(proposed.bindings[1],dict(role=role,members={access.MEMBER}))
            again,changed=access.table_proposal(proposed,table);self.assertFalse(changed)
            self.assertEqual(again.bindings,proposed.bindings)
        with self.assertRaises(ValueError):access.table_proposal(self.policy(),'other.table')

    def test_table_conditional_or_missing_etag_denies(self):
        for p in (self.policy(),self.policy()):
            p.etag=None
            with self.assertRaises(ValueError):access.table_proposal(p,access.READ_TABLES[0])
        p=self.policy();p.version=3
        with self.assertRaises(ValueError):access.table_proposal(p,access.READ_TABLES[0])

    def test_project_proposal_preserves_conditions_and_other_principals(self):
        p=dict(etag='original',version=3,bindings=[
            dict(role=access.MODEL,members=['user:other@example.com'],condition={'expression':'false'}),
            dict(role='roles/viewer',members=['user:unrelated@example.com'])])
        before=deepcopy(p);proposed,changed=access.project_proposal(p)
        self.assertTrue(changed);self.assertEqual(p,before)
        self.assertEqual(proposed['bindings'][:2],before['bindings'])
        self.assertEqual(proposed['bindings'][2],dict(role=access.MODEL,members=[access.MEMBER]))
        _,changed=access.project_proposal(proposed);self.assertFalse(changed)
        self.assertNotIn('bigquery.jobs.get',str(proposed))

    def fixture(self):
        client=Mock(project=access.PROJECT,location=access.LOCATION)
        policies={table:self.policy() for table in access.TABLE_ROLES}
        client.get_iam_policy.side_effect=lambda table,**kw:deepcopy(policies[table])
        client.set_iam_policy.side_effect=lambda table,policy,**kw:policies.__setitem__(table,deepcopy(policy))
        session=Mock();project=dict(etag='project-etag',version=3,bindings=[])
        def get(url,**kw):
            role=url.removeprefix('https://iam.googleapis.com/v1/')
            permissions=['aiplatform.endpoints.predict'] if role==access.MODEL else ['bigquery.tables.get','bigquery.tables.getData']
            if role==access.WRITER:permissions.append('bigquery.tables.updateData')
            response=Mock();response.json.return_value=dict(name=role,stage='GA',includedPermissions=permissions)
            return response
        def post(url,**kw):
            if url.endswith(':setIamPolicy'):project.update(deepcopy(kw['json']['policy']))
            response=Mock();response.json.return_value=deepcopy(project);return response
        session.get.side_effect=get;session.post.side_effect=post
        return client,session,policies

    def test_check_mode_never_mutates_and_apply_is_exact_then_idempotent(self):
        c,s,_=self.fixture();result=access.setup(c,s)
        self.assertEqual(result['model_grant'],'MISSING');c.set_iam_policy.assert_not_called()
        self.assertFalse(any(x.args[0].endswith(':setIamPolicy') for x in s.post.call_args_list))
        result=access.setup(c,s,apply=True)
        self.assertTrue(all(t['status']=='APPLIED_VERIFIED' for t in result['tables']))
        self.assertEqual(c.set_iam_policy.call_count,7)
        c.set_iam_policy.reset_mock();s.post.reset_mock()
        result=access.setup(c,s,apply=True)
        self.assertTrue(all(t['status']=='EXISTING_VERIFIED' for t in result['tables']))
        c.set_iam_policy.assert_not_called()
        self.assertFalse(any(x.args[0].endswith(':setIamPolicy') for x in s.post.call_args_list))

    def test_invalid_last_table_blocks_all_mutations(self):
        c,s,policies=self.fixture();policies[access.WRITE_TABLES[-1]].etag=None
        with self.assertRaises(ValueError):access.setup(c,s,apply=True)
        c.set_iam_policy.assert_not_called()
        self.assertFalse(any(x.args[0].endswith(':setIamPolicy') for x in s.post.call_args_list))

    def test_lost_table_response_retains_uncertainty_and_stops_without_retry(self):
        c,s,policies=self.fixture();progress=[]
        def lost(table,policy,**kw):
            policies[table]=deepcopy(policy);raise TimeoutError('response lost')
        c.set_iam_policy.side_effect=lost
        with self.assertRaises(TimeoutError):access.setup(c,s,apply=True,progress=lambda r:progress.append(deepcopy(r)))
        c.set_iam_policy.assert_called_once()
        self.assertEqual(progress[-1]['tables'][0]['status'],'APPLY_ATTEMPTED_UNCERTAIN')
        self.assertEqual(progress[-1]['tables'][1]['status'],'MISSING')
