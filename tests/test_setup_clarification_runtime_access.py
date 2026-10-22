from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock
from google.api_core.iam import Policy
from src.ingestion.setup_clarification_runtime_access import (
    ROLE, MEMBER, PERMISSIONS, TABLES, verify_role, table_grant, role_setup, bindings,
)

class RuntimeAccessTests(TestCase):
    def role(self):
        return dict(name=ROLE,stage='GA',includedPermissions=list(PERMISSIONS))

    def policy(self):
        p=Policy();p.etag='etag';p.bindings.append(dict(role='roles/bigquery.dataViewer',members={'user:existing@example.com'}));return p

    def test_role_conflicts_deny(self):
        verify_role(self.role())
        for change in (dict(deleted=True),dict(stage='DISABLED'),dict(includedPermissions=['bigquery.tables.updateData'])):
            with self.assertRaises(ValueError):verify_role(dict(self.role(),**change))

    def test_default_missing_role_and_grant_never_mutate(self):
        session=Mock();session.get.return_value.status_code=404
        self.assertEqual(role_setup(session,apply=False),'MISSING');session.post.assert_not_called()
        c=Mock();c.get_iam_policy.return_value=self.policy()
        self.assertEqual(table_grant(c,TABLES[0],apply=False)['outcome'],'MISSING');c.set_iam_policy.assert_not_called()

    def test_narrow_grant_preserves_etag_and_unrelated_bindings_then_reuses(self):
        p=self.policy();expected=deepcopy(p);expected.bindings.append(dict(role=ROLE,members={MEMBER}))
        c=Mock();c.get_iam_policy.side_effect=[p,expected,expected]
        self.assertEqual(table_grant(c,TABLES[0],apply=True)['outcome'],'APPLIED_VERIFIED')
        sent=c.set_iam_policy.call_args.args[1]
        self.assertEqual(sent.etag,p.etag);self.assertEqual(bindings(sent),bindings(expected))
        self.assertEqual(table_grant(c,TABLES[0],apply=True)['outcome'],'EXISTING_VERIFIED')
        self.assertEqual(c.set_iam_policy.call_count,1)

    def test_conflict_unknown_and_scope_never_retry(self):
        c=Mock();c.get_iam_policy.return_value=self.policy();c.set_iam_policy.side_effect=RuntimeError('uncertain')
        with self.assertRaises(RuntimeError):table_grant(c,TABLES[0],apply=True)
        self.assertEqual(c.set_iam_policy.call_count,1)
        with self.assertRaises(ValueError):table_grant(c,'unrelated.table',apply=True)
        c=Mock();p=self.policy();p.etag=None;c.get_iam_policy.return_value=p
        with self.assertRaises(ValueError):table_grant(c,TABLES[0],apply=True)
        c.set_iam_policy.assert_not_called()

    def test_changed_readback_denies(self):
        c=Mock();c.get_iam_policy.side_effect=[self.policy(),self.policy()]
        with self.assertRaises(ValueError):table_grant(c,TABLES[1],apply=True)
        self.assertEqual(c.set_iam_policy.call_count,1)

    def test_unconditional_policy_version_normalization_and_conditional_denial(self):
        p=self.policy();expected=deepcopy(p);expected.version=1
        expected.bindings.append(dict(role=ROLE,members={MEMBER}))
        c=Mock();c.get_iam_policy.side_effect=[p,expected]
        self.assertEqual(table_grant(c,TABLES[0],apply=True)['outcome'],'APPLIED_VERIFIED')
        p=self.policy();p.version=3;c=Mock();c.get_iam_policy.return_value=p
        with self.assertRaises(ValueError):table_grant(c,TABLES[0],apply=True)
        c.set_iam_policy.assert_not_called()
