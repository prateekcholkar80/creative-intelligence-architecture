"""Authoritative read/denial fixtures only; never publish fake users or roles."""
from copy import deepcopy
from datetime import datetime, timezone
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Forbidden, NotFound

from src.application.access_policy import Action, AuthorizationError, Reason
from src.application.access_reader import AccessReadError, AccessReadSettings, BigQueryAccessReader
from src.application.identity import ISSUER, IdentityError, VerifiedIdentity, principal_for_subject
from src.application.operation_budget import OperationSettings, OperationStopped, operation_scope
from src.application.workflow_schema import LOCATION, SCHEMA_VERSION
from src.ingestion.setup_workflow_tables import PROJECT

WORKSPACE = 'fixture-marketing'
CONVERSATION = 'fixture-draft'
AUDIENCE = '/projects/123456789/locations/asia-south1/services/test-campaign'
NOW = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
EPOCH = int(NOW.timestamp())
SUBJECT = 'accounts.google.com:test-subject'
IDENTITY = VerifiedIdentity(principal_for_subject(ISSUER, SUBJECT), ISSUER, SUBJECT,
                            AUDIENCE, EPOCH - 10, EPOCH + 590)


def base():
    return dict(schema_version=SCHEMA_VERSION, workspace_id=WORKSPACE, created_at=NOW, updated_at=NOW)


def workspace(**changes):
    return dict(base(), status='ACTIVE', revision=1) | changes


def membership(**changes):
    return dict(base(), principal_id=IDENTITY.principal.principal_id, identity_issuer=ISSUER,
                identity_subject=SUBJECT, roles=['CREATOR'], status='ACTIVE', revision=1,
                assignment_origin='ADMIN', updated_by='fixture-admin') | changes


def conversation(**changes):
    return dict(base(), conversation_id=CONVERSATION, owner_id=IDENTITY.principal.principal_id,
                visibility='PRIVATE', status='ACTIVE', access_revision=1) | changes


class AccessReaderTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock(project=PROJECT)
        self.reader = BigQueryAccessReader(self.client)
        patcher = patch('src.application.identity.time.time', return_value=EPOCH)
        patcher.start()
        self.addCleanup(patcher.stop)

    def rows(self, *results):
        self.client.query.reset_mock()
        jobs = []
        for result in results:
            job = Mock()
            job.result.return_value = deepcopy(result)
            jobs.append(job)
        self.client.query.side_effect = jobs
        return jobs

    def authorize(self, action=Action.GENERATE_DRAFT, resource=None, identity=IDENTITY):
        return self.reader.authorize(identity, action, workspace_id=WORKSPACE, conversation_id=resource)

    def test_creator_generation_uses_current_scoped_parameterized_reads(self):
        jobs = self.rows([workspace()], [membership()])
        result = self.authorize()
        self.assertTrue(result.allowed)
        self.assertEqual(result.membership_revision, 1)
        calls = self.client.query.call_args_list
        self.assertEqual(len(calls), 2)
        for call in calls:
            sql = call.args[0]
            self.assertTrue(sql.startswith('SELECT '))
            self.assertIn('LIMIT 2', sql)
            self.assertNotIn(WORKSPACE, sql)
            self.assertIn('workspace_id=@workspace_id', sql)
            self.assertEqual(call.kwargs['location'], LOCATION)
            self.assertEqual(call.kwargs['timeout'], 20)
            self.assertIsNone(call.kwargs['job_retry'])
            config = call.kwargs['job_config']
            self.assertFalse(config.use_query_cache)
            self.assertEqual(config.maximum_bytes_billed, 64 * 1024 * 1024)
            self.assertEqual(int(config.job_timeout_ms), 20000)  # SDK exposes wire INT64 as a string.
            self.assertEqual({p.name: p.value for p in config.query_parameters}['workspace_id'], WORKSPACE)
        self.assertIn('principal_id=@principal_id', calls[1].args[0])
        self.assertEqual({p.name: p.value for p in calls[1].kwargs['job_config'].query_parameters}['principal_id'],
                         IDENTITY.principal.principal_id)
        for job in jobs:
            self.assertEqual(job.result.call_args.kwargs['timeout'], 20)
            self.assertIsNone(job.result.call_args.kwargs['job_retry'])

    def test_access_queries_and_results_share_remaining_time_without_native_config_changes(self):
        tick=[100.0]
        jobs=self.rows([workspace()],[membership()])
        def query(*args,**kwargs):
            tick[0]+=3
            return jobs.pop(0)
        all_jobs=list(jobs);self.client.query.side_effect=query
        with patch('src.application.operation_budget.time.monotonic',side_effect=lambda:tick[0]),\
                operation_scope(OperationSettings(10)):
            self.assertTrue(self.authorize().allowed)
        self.assertEqual([call.kwargs['timeout'] for call in self.client.query.call_args_list],[10,7])
        self.assertEqual([job.result.call_args.kwargs['timeout'] for job in all_jobs],[7,4])
        for call in self.client.query.call_args_list:
            self.assertIsNone(call.kwargs['retry'])
            self.assertEqual(int(call.kwargs['job_config'].job_timeout_ms),20000)

    def test_expired_workspace_read_never_fetches_membership(self):
        tick=[100.0]
        jobs=self.rows([workspace()],[membership()])
        def slow(*args,**kwargs):tick[0]+=10;return jobs[0]
        self.client.query.side_effect=slow
        with patch('src.application.operation_budget.time.monotonic',side_effect=lambda:tick[0]):
            with self.assertRaises(OperationStopped):
                with operation_scope(OperationSettings(10)):self.authorize()
        self.assertEqual(self.client.query.call_count,1)
        jobs[0].result.assert_not_called()

    def test_lazy_access_page_expiry_is_not_masked_as_store_error(self):
        tick=[100.0]
        jobs=self.rows([])
        def page():
            tick[0]+=10;yield workspace()
        jobs[0].result.return_value=page()
        with patch('src.application.operation_budget.time.monotonic',side_effect=lambda:tick[0]):
            with self.assertRaises(OperationStopped):
                with operation_scope(OperationSettings(10)):self.authorize()
        self.assertEqual(self.client.query.call_count,1)

    def test_opaque_ids_are_values_never_sql_fragments(self):
        unusual = "fixture'OR'1=1"
        self.rows([workspace(workspace_id=unusual)], [membership(workspace_id=unusual)])
        self.reader.authorize(IDENTITY, Action.GENERATE_DRAFT, workspace_id=unusual)
        for call in self.client.query.call_args_list:
            self.assertNotIn(unusual, call.args[0])
            self.assertEqual(call.kwargs['job_config'].query_parameters[0].value, unusual)

    def test_current_conversation_access_revision_and_owner_access(self):
        for action in (Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT, Action.REFINE_DRAFT, Action.SET_VISIBILITY):
            self.rows([workspace()], [membership(revision=3)], [conversation(access_revision=7)])
            result = self.authorize(action, CONVERSATION)
            self.assertEqual(result.reason, Reason.ALLOWED_OWNER)
            self.assertEqual((result.membership_revision, result.resource_revision), (3, 7))
            call = self.client.query.call_args_list[2]
            self.assertIn('workspace_id=@workspace_id AND conversation_id=@conversation_id', call.args[0])

    def test_team_read_and_review_follow_roles(self):
        for role, action in (('VIEWER', Action.READ_DRAFT), ('REVIEWER', Action.REVIEW_FOR_POC)):
            self.rows([workspace()], [membership(roles=[role])],
                      [conversation(owner_id='another-principal', visibility='TEAM')])
            self.assertEqual(self.authorize(action, CONVERSATION).reason, Reason.ALLOWED_TEAM)

    def test_private_other_owner_denies_even_with_all_roles(self):
        self.rows([workspace()], [membership(roles=['VIEWER', 'CREATOR', 'REVIEWER', 'ADMIN'])],
                  [conversation(owner_id='another-principal')])
        with self.assertRaises(AuthorizationError) as caught:
            self.authorize(Action.READ_DRAFT, CONVERSATION)
        self.assertEqual(caught.exception.decision.reason, Reason.PRIVATE_RESOURCE)

    def test_team_sharing_does_not_allow_peer_edit(self):
        self.rows([workspace()], [membership()], [conversation(owner_id='other', visibility='TEAM')])
        with self.assertRaises(AuthorizationError) as caught:
            self.authorize(Action.REFINE_DRAFT, CONVERSATION)
        self.assertEqual(caught.exception.decision.reason, Reason.OWNER_REQUIRED)

    def test_admin_has_no_generation_bypass_and_disabled_actions_remain_disabled(self):
        for roles, action, reason in ((['ADMIN'], Action.GENERATE_DRAFT, Reason.ROLE_REQUIRED),
                                     (['CREATOR', 'ADMIN'], Action.LAUNCH_CAMPAIGN, Reason.ACTION_NOT_ENABLED),
                                     (['REVIEWER'], Action.APPROVE_BENEFIT_CLAIMS, Reason.ACTION_NOT_ENABLED)):
            self.rows([workspace()], [membership(roles=roles)])
            with self.subTest(action=action), self.assertRaises(AuthorizationError) as caught:
                self.authorize(action)
            self.assertEqual(caught.exception.decision.reason, reason)

    def test_archived_conversations_are_read_only(self):
        for action in (Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT):
            self.rows([workspace()], [membership()], [conversation(status='ARCHIVED')])
            self.assertTrue(self.authorize(action, CONVERSATION).allowed)
        for action in (Action.REFINE_DRAFT, Action.SET_VISIBILITY, Action.REVIEW_FOR_POC):
            self.rows([workspace()], [membership(roles=['CREATOR', 'REVIEWER'])], [conversation(status='ARCHIVED')])
            with self.subTest(action=action), self.assertRaises(AccessReadError):
                self.authorize(action, CONVERSATION)

    def test_missing_inactive_memberships_deny_before_resource_read_with_correct_action(self):
        for records, reason in (([], Reason.MEMBERSHIP_REQUIRED),
                                ([membership(status='REVOKED')], Reason.MEMBERSHIP_INACTIVE),
                                ([membership(status='SUSPENDED')], Reason.MEMBERSHIP_INACTIVE)):
            self.rows([workspace()], records)
            with self.subTest(records=records), self.assertRaises(AuthorizationError) as caught:
                self.authorize(Action.READ_DRAFT, CONVERSATION)
            self.assertEqual(caught.exception.decision.reason, reason)
            self.assertEqual(caught.exception.decision.action, Action.READ_DRAFT)
            self.assertEqual(self.client.query.call_count, 2)

    def test_missing_disabled_or_duplicate_workspace_denies_before_membership(self):
        for records in ([], [workspace(status='DISABLED')], [workspace(), workspace()]):
            self.rows(records)
            with self.subTest(records=records), self.assertRaises(AccessReadError):
                self.authorize()
            self.assertEqual(self.client.query.call_count, 1)

    def test_duplicate_memberships_or_conversations_fail_closed(self):
        for results in (([workspace()], [membership(), membership()]),
                        ([workspace()], [membership()], [conversation(), conversation()])):
            self.rows(*results)
            with self.assertRaises(AccessReadError) as caught:
                self.authorize(Action.READ_DRAFT, CONVERSATION)
            self.assertEqual(caught.exception.code, 'DUPLICATE_ACCESS_RECORD')

    def test_missing_resource_and_unexpected_resource_denied(self):
        self.rows([workspace()], [membership()], [])
        with self.assertRaises(AccessReadError):
            self.authorize(Action.READ_DRAFT, CONVERSATION)
        self.rows([workspace()], [membership()], [conversation()])
        with self.assertRaises(AuthorizationError) as caught:
            self.authorize(Action.GENERATE_DRAFT, CONVERSATION)
        self.assertEqual(caught.exception.decision.reason, Reason.UNEXPECTED_RESOURCE)

    def test_malformed_base_records_fail_closed(self):
        for change in ({'schema_version': 'old'}, {'workspace_id': 'different'}, {'revision': True},
                       {'revision': 0}, {'revision': 2**63}, {'roles': ['ADMIN']},
                       {'status': 'UNKNOWN'}, {'created_at': None}, {'updated_at': NOW.replace(tzinfo=None)},
                       {'updated_at': datetime(2026, 10, 9, tzinfo=timezone.utc)}):
            self.rows([workspace(**change)])
            with self.subTest(change=change), self.assertRaises(AccessReadError):
                self.authorize()

    def test_malformed_membership_cannot_supply_roles(self):
        for change in ({'roles': ['UNKNOWN']}, {'roles': ['CREATOR', 'CREATOR']}, {'roles': 'CREATOR'},
                       {'roles': [None]}, {'principal_id': 'different'}, {'identity_subject': 'different'},
                       {'identity_issuer': 'https://accounts.google.com'}, {'status': 'UNKNOWN'},
                       {'revision': True}, {'revision': 2**63},
                       {'updated_by': ''}, {'assignment_origin': 'EMAIL_HEADER'}, {'extra': 'ADMIN'},
                       {'schema_version': 'old'}, {'workspace_id': 'different'}):
            self.rows([workspace()], [membership(**change)])
            with self.subTest(change=change), self.assertRaises(AccessReadError):
                self.authorize()

    def test_malformed_or_cross_workspace_resource_denied(self):
        for change in ({'workspace_id': 'different'}, {'conversation_id': 'different'}, {'owner_id': ''},
                       {'visibility': 'PUBLIC'}, {'access_revision': True}, {'access_revision': 0},
                       {'access_revision': 2**63}, {'status': 'UNKNOWN'}, {'extra': 'ADMIN'}):
            self.rows([workspace()], [membership()], [conversation(**change)])
            with self.subTest(change=change), self.assertRaises(AccessReadError):
                self.authorize(Action.READ_DRAFT, CONVERSATION)

    def test_role_revocation_is_read_again_next_call(self):
        jobs = self.rows([workspace()], [membership()], [workspace()], [membership(status='REVOKED', revision=2)])
        self.assertTrue(self.authorize().allowed)
        with self.assertRaises(AuthorizationError) as caught:
            self.authorize()
        self.assertEqual(caught.exception.decision.membership_revision, 2)
        self.assertEqual(self.client.query.call_count, 4)
        self.assertTrue(all(job.result.call_count == 1 for job in jobs))

    def test_sharing_revocation_is_read_again_next_call(self):
        self.rows([workspace()], [membership(roles=['VIEWER'])], [conversation(owner_id='other', visibility='TEAM')],
                  [workspace()], [membership(roles=['VIEWER'])], [conversation(owner_id='other', access_revision=2)])
        self.assertTrue(self.authorize(Action.READ_DRAFT, CONVERSATION).allowed)
        with self.assertRaises(AuthorizationError) as caught:
            self.authorize(Action.READ_DRAFT, CONVERSATION)
        self.assertEqual(caught.exception.decision.reason, Reason.PRIVATE_RESOURCE)
        self.assertEqual(caught.exception.decision.resource_revision, 2)

    def test_anonymous_plain_principal_and_expired_identity_never_read_database(self):
        for identity in (None, IDENTITY.principal, {'principal': IDENTITY.principal}):
            with self.assertRaises(IdentityError):
                self.authorize(identity=identity)
        with patch('src.application.identity.time.time', return_value=EPOCH + 620), self.assertRaises(IdentityError):
            self.authorize()
        self.client.query.assert_not_called()

    def test_expiry_during_database_reads_denies_before_operation(self):
        self.rows([workspace()], [membership()])
        with patch('src.application.identity.time.time', side_effect=[EPOCH, EPOCH + 620]):
            with self.assertRaises(IdentityError):
                self.authorize()

    def test_bigquery_errors_and_missing_tables_have_no_local_fallback(self):
        for error in (Forbidden('private details'), NotFound('private details'), RuntimeError('private details')):
            self.client.query.side_effect = error
            with self.subTest(error=type(error)), self.assertRaises(AccessReadError) as caught:
                self.authorize()
            self.assertEqual(caught.exception.code, 'ACCESS_STORE_UNAVAILABLE')
            self.assertNotIn('private details', str(caught.exception))
            self.assertIsNone(caught.exception.__cause__)
        jobs = self.rows([workspace()])
        jobs[0].result.side_effect = Forbidden('polling error')
        with self.assertRaises(AccessReadError):
            self.authorize()

    def test_server_configuration_and_invalid_identifiers_fail(self):
        for settings in ({'maximum_bytes_billed': True}, {'maximum_bytes_billed': 1024**3 + 1},
                         {'timeout_seconds': 0}, {'timeout_seconds': 61}):
            with self.assertRaises(ValueError):
                AccessReadSettings(**settings)
        with self.assertRaises(ValueError):
            BigQueryAccessReader(Mock(project='other-project'))
        with self.assertRaises(ValueError):
            BigQueryAccessReader(self.client, settings={'timeout_seconds': 20})
        for value in ('', 'x y', 'x\x00', 'x' * 129, None):
            with self.assertRaises(AccessReadError):
                self.reader.authorize(IDENTITY, Action.GENERATE_DRAFT, workspace_id=value)
        with self.assertRaises(ValueError):
            self.authorize('GENERATE_DRAFT')
        self.client.query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
