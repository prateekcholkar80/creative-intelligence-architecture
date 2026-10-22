"""Offline SQL boundary/result tests; no native transaction or race claims."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import timedelta, timezone
import re
import unittest
from unittest.mock import patch

from src.application.access_policy import AuthorizationError, Visibility
from src.application.sharing_coordinator_contract import (
    SharingCommitObservation, SharingCommitStatus, prepare_sharing,
)
from src.application.sharing_query import (
    SharingQuery, SharingQuerySettings, SharingResultState, validate_sharing_result,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION, TABLES
from src.application.workspace_contract import MAX_REVISION
from tests.test_conversation_contract import LATER, candidate, conversation
from tests.test_membership_contract import ADMIN, TIME, membership, workspace
from tests.test_sharing_coordinator_contract import command


def query(status=SharingCommitStatus.NOT_SUBMITTED, **changes):
    cmd = changes.pop('command', command())
    observation = SharingCommitObservation(cmd.audit_id, status,
        None if status is SharingCommitStatus.NOT_SUBMITTED else 'prior-query-job')
    return SharingQuery(**(dict(command=cmd, expected_workspace_revision=7,
                               commit_observation=observation) | changes))


def result_row(state='APPLIED'):
    q = query()
    proposal = prepare_sharing(q.command, workspace_rows=[workspace()], membership_rows=[membership()],
        conversation_rows=[conversation()], receipt_rows=[], expected_workspace_revision=7,
        observed_at=LATER, commit_observation=q.commit_observation).plan
    return dict(state=state, observed_at=LATER, workspace=dict(proposal.workspace_after),
                membership=membership(), conversation=dict(proposal.conversation_after),
                receipt=dict(proposal.audit_candidate))


class SharingQueryTests(unittest.TestCase):
    def check(self, q=None, row=None):
        return validate_sharing_result(query() if q is None else q,
            [result_row() if row is None else row], query_job_id='current-query-job')

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_values_are_parameters_and_sql_is_independent_of_intent(self):
        first = query()
        other = query(command=command(workspace_id='second-workspace', conversation_id='second-conversation',
                                      trace_id='second-trace', visibility=Visibility.PRIVATE))
        self.assertEqual(first.sql, other.sql)
        for value in ('fixture-workspace', 'second-workspace', first.command.audit_id, 'fixture-retry'):
            self.assertNotIn(value, first.sql)
        params = {p.name: p for p in other.job_config().query_parameters}
        self.assertEqual(params['workspace_id'].value, 'second-workspace')
        self.assertEqual(params['visibility'].value, 'PRIVATE')
        self.assertEqual(params['audit_id'].value, other.command.audit_id)
        self.assertNotIn('retry_key', params)

    def test_parameters_have_exact_scalar_types_and_original_window(self):
        q = query()
        params = {p.name: p for p in q.job_config().query_parameters}
        actual_names = set(re.findall(r'(?<!@)@([a-z_]+)', q.sql))
        self.assertEqual(set(params), actual_names)
        self.assertEqual(params['expected_membership_revision'].type_, 'INT64')
        self.assertEqual(params['expected_conversation_revision'].type_, 'INT64')
        self.assertEqual(params['expected_workspace_revision'].type_, 'INT64')
        self.assertEqual(params['accepted_at'].type_, 'TIMESTAMP')
        self.assertEqual(params['accepted_at'].value, TIME)
        self.assertEqual(params['deadline_at'].value, TIME + timedelta(days=7))
        self.assertEqual(params['trace_id'].value, q.command.trace_id)
        self.assertEqual(params['audit_prefix'].value, q.command.audit_prefix)

    def test_equivalent_timezones_send_utc_parameters(self):
        local = TIME.astimezone(timezone(timedelta(hours=5, minutes=30)))
        params = {p.name: p.value for p in query(command=command(accepted_at=local)).job_config().query_parameters}
        self.assertEqual(params['accepted_at'].tzinfo, timezone.utc)
        self.assertEqual(params['accepted_at'], TIME)

    def test_all_commit_observations_are_explicit_parameters(self):
        for status in SharingCommitStatus:
            params = {p.name: p.value for p in query(status).job_config().query_parameters}
            self.assertEqual(params['commit_status'], status.value)

    def test_sdk_configuration_is_bounded_stateless_and_without_cache(self):
        q = query()
        config = q.job_config()
        self.assertEqual(q.location, LOCATION)
        self.assertFalse(config.use_query_cache)
        self.assertFalse(config.use_legacy_sql)
        self.assertFalse(config.create_session)
        self.assertEqual(config.maximum_bytes_billed, 64 * 1024**2)
        self.assertEqual(config.to_api_repr()['jobTimeoutMs'], '60000')
        self.assertEqual(config.to_api_repr()['query']['scriptOptions']['statementTimeoutMs'], '20000')
        self.assertIsNone(config.destination)
        self.assertIsNone(config.write_disposition)
        self.assertEqual(config.labels, {'workflow': 'sharing-v1'})

    def test_settings_cannot_exceed_bounds_or_use_coercion(self):
        for changes in (dict(maximum_bytes_billed=True), dict(maximum_bytes_billed=0),
                        dict(maximum_bytes_billed=1024**3+1), dict(job_timeout_ms=60001),
                        dict(job_timeout_ms=0), dict(statement_timeout_ms=True),
                        dict(statement_timeout_ms=60001), dict(statement_timeout_ms=0)):
            self.assert_code('INVALID_SHARING_QUERY_SETTINGS', SharingQuerySettings, **changes)
        custom = SharingQuerySettings(100000, 15000, 10000)
        self.assertEqual(query(settings=custom).job_config().to_api_repr()['jobTimeoutMs'], '15000')

    def test_query_contract_is_immutable_but_sdk_objects_are_independent(self):
        q = query()
        with self.assertRaises(FrozenInstanceError):
            q.expected_workspace_revision = 8
        config = q.job_config()
        config.maximum_bytes_billed = 999999
        config.query_parameters[0].value = 'changed'
        fresh = q.job_config()
        self.assertEqual(fresh.maximum_bytes_billed, 64 * 1024**2)
        self.assertEqual(fresh.query_parameters[0].value, q.command.workspace_id)

    def test_query_rejects_untyped_inputs_and_cross_command_observations(self):
        self.assert_code('INVALID_SHARING_COMMAND', SharingQuery, {}, 7, query().commit_observation)
        self.assert_code('INVALID_SHARING_COMMIT_STATUS', query, commit_observation={})
        self.assert_code('SHARING_COMMIT_SCOPE_MISMATCH', query,
            commit_observation=SharingCommitObservation('another-audit', SharingCommitStatus.NOT_SUBMITTED))
        self.assert_code('INVALID_SHARING_QUERY_SETTINGS', query, settings={})
        for value in (True, 0, 7.0, MAX_REVISION+1):
            with self.assertRaises(ContractError):
                query(expected_workspace_revision=value)

    def test_sql_uses_fixed_destinations_and_explicit_full_schema_types(self):
        sql = query().sql
        self.assertNotRegex(sql, r'__[A-Z_]+__')
        self.assertNotIn('SELECT *', sql)
        for name in ('workspaces', 'memberships', 'conversations', 'audit_events'):
            self.assertIn('ci_application.' + name + '`', sql)
            for field in TABLES[name].schema:
                self.assertRegex(sql, rf'\b{field.name}\s+(STRING|INT64|BOOL|TIMESTAMP|ARRAY<STRING>)')
        columns = ', '.join(f.name for f in TABLES['audit_events'].schema)
        self.assertIn('audit_events` (' + columns + ')', sql)
        self.assertIn('roles ARRAY<STRING>', sql)

    def test_transaction_wraps_bounded_reads_fence_updates_and_explicit_audit(self):
        sql = query().sql
        self.assertEqual(sql.count('BEGIN TRANSACTION;'), 1)
        self.assertEqual(sql.count('LIMIT 2'), 4)
        self.assertEqual(sql.count('ASSERT @@row_count = 1'), 3)
        start, commit = sql.index('BEGIN TRANSACTION;'), sql.index('COMMIT TRANSACTION;')
        for operation in ('SET workspace_rows = ARRAY', 'SET receipt_rows = ARRAY',
                          'UPDATE ', 'INSERT INTO '):
            self.assertTrue(start < sql.index(operation) < commit)
        self.assertIn('EXCEPTION WHEN ERROR THEN\n  ROLLBACK TRANSACTION;\n  RAISE;', sql)
        self.assertGreater(sql.index('SELECT result_state AS state'), commit)
        self.assertNotRegex(sql, r'\b(DELETE|MERGE|TRUNCATE|WRITE_TRUNCATE|EXECUTE IMMEDIATE)\b')

    def test_historical_and_unknown_branches_contain_no_mutation(self):
        sql = query().sql
        reuse = sql.split('IF ARRAY_LENGTH(receipt_rows) = 1 THEN', 1)[1].split('ELSEIF', 1)[0]
        uncertain = sql.split("ELSEIF @commit_status IN ('UNKNOWN', 'COMMITTED') THEN", 1)[1].split('ELSE', 1)[0]
        self.assertNotRegex(reuse + uncertain, r'\b(UPDATE|INSERT|DELETE)\b')
        self.assertIn("SET result_state = 'REUSED'", reuse)
        self.assertIn("SET result_state = 'UNKNOWN'", uncertain)
        self.assertIn('STARTS_WITH(audit_id, @audit_prefix)', sql)
        self.assertIn('occurred_at >= @accepted_at AND occurred_at < @deadline_at', sql)
        self.assertLess(sql.index("AND 'CREATOR' IN UNNEST"), sql.index('IF ARRAY_LENGTH(receipt_rows) = 1'))

    def test_sql_defends_identity_roles_owner_revisions_and_causal_receipt(self):
        sql = query().sql
        for guard in ("CONCAT('iap-v1:', LOWER(TO_HEX(SHA256(",
                      "AND membership_row.status = 'ACTIVE'", "COUNT(DISTINCT role)",
                      "AND conversation_row.owner_id = @principal_id", "AND conversation_row.status = 'ACTIVE'",
                      'membership_row.revision = @expected_membership_revision',
                      'workspace_row.revision = @expected_workspace_revision',
                      'conversation_row.access_revision = @expected_conversation_revision',
                      'receipt_row.audit_id = @audit_id',
                      'conversation_row.access_revision > receipt_row.new_revision',
                      "@commit_status != 'NOT_COMMITTED'", 'receipt_row.request_id IS NULL'):
            self.assertIn(guard, sql)

    def test_applied_result_is_minimal_frozen_and_validates_exact_post_state(self):
        row = result_row()
        original = deepcopy(row)
        outcome = self.check(row=row)
        self.assertEqual(outcome.state, SharingResultState.APPLIED)
        self.assertEqual(outcome.resulting_revision, 4)
        self.assertEqual(outcome.audit_id, command().audit_id)
        self.assertEqual(row, original)
        self.assertNotIn('identity_subject', repr(outcome))
        with self.assertRaises(FrozenInstanceError):
            outcome.resulting_revision = 7

    def test_positive_noncommit_can_apply_in_current_submission(self):
        self.assertEqual(self.check(query(SharingCommitStatus.NOT_COMMITTED)).state, SharingResultState.APPLIED)

    def test_applied_result_rejects_unknown_or_previously_committed_input(self):
        for status in (SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED):
            self.assert_code('INVALID_SHARING_APPLIED_RESULT', self.check, query(status))

    def test_applied_result_cannot_rebase_operational_or_business_revisions(self):
        for table, changes in (('workspace', dict(revision=9)), ('membership', dict(revision=4)),
                               ('conversation', dict(access_revision=5))):
            row = result_row()
            row[table].update(changes)
            self.assert_code('INCONSISTENT_SHARING_APPLIED_RESULT', self.check, row=row)

    def test_applied_result_requires_same_observation_times(self):
        row = result_row()
        row['workspace']['updated_at'] = TIME
        self.assert_code('INCONSISTENT_SHARING_APPLIED_RESULT', self.check, row=row)
        row = result_row()
        row['observed_at'] += timedelta(seconds=1)
        self.assert_code('INCONSISTENT_SHARING_APPLIED_RESULT', self.check, row=row)

    def test_reused_result_preserves_later_visibility_and_old_result_revision(self):
        row = result_row('REUSED')
        row['observed_at'] += timedelta(seconds=2)
        row['workspace'].update(revision=10, updated_at=row['observed_at'])
        row['membership'].update(revision=9, updated_at=row['observed_at'])
        row['conversation'].update(visibility='PRIVATE', access_revision=6, updated_at=row['observed_at'])
        before = deepcopy(row)
        outcome = self.check(row=row)
        self.assertEqual(outcome.state, SharingResultState.REUSED)
        self.assertEqual(outcome.resulting_revision, 4)
        self.assertEqual(row, before)

    def test_matching_receipt_resolves_unknown_but_contradicts_positive_noncommit(self):
        row = result_row('REUSED')
        for status in (SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED):
            self.assertEqual(self.check(query(status), row).resulting_revision, 4)
        self.assert_code('SHARING_COMMIT_OBSERVATION_CONFLICT', self.check,
                         query(SharingCommitStatus.NOT_COMMITTED), row)

    def test_missing_receipt_remains_unknown_without_revision_rebasing(self):
        for status in (SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED):
            for conv in (conversation(), candidate()):
                row = result_row('UNKNOWN')
                row.update(receipt=None, conversation=conv)
                outcome = self.check(query(status), row)
                self.assertEqual(outcome.state, SharingResultState.UNKNOWN)
                self.assertIsNone(outcome.resulting_revision)

    def test_unknown_state_rejects_receipt_and_non_uncertain_input(self):
        self.assert_code('INVALID_SHARING_UNKNOWN_RESULT', self.check,
                         query(SharingCommitStatus.UNKNOWN), result_row('UNKNOWN'))
        row = result_row('UNKNOWN') | dict(receipt=None)
        self.assert_code('INVALID_SHARING_UNKNOWN_RESULT', self.check, row=row)

    def test_result_requires_exactly_one_final_row_and_exact_shape(self):
        for rows in ([], [result_row(), result_row()], {}, [None]):
            self.assert_code('INVALID_SHARING_RESULT_ROWS', validate_sharing_result,
                             query(), rows, query_job_id='current-query-job')
        for row in (result_row() | dict(extra='unexpected'), {k: v for k, v in result_row().items() if k != 'receipt'}):
            self.assert_code('INVALID_SHARING_RESULT_SHAPE', self.check, row=row)
        for value in ('PREPARED', '', None, []):
            self.assert_code('INVALID_SHARING_RESULT_STATE', self.check, row=result_row() | dict(state=value))

    def test_result_requires_full_destination_rows_and_receipt(self):
        for table in ('workspace', 'membership', 'conversation', 'receipt'):
            row = result_row()
            row[table]['extra'] = 'unexpected'
            self.assert_code('INVALID_ROW_SHAPE', self.check, row=row)
        self.assert_code('INVALID_SHARING_RESULT_RECEIPT', self.check, row=result_row() | dict(receipt=None))

    def test_current_revocation_and_noncreator_deny_all_result_states(self):
        for state in ('APPLIED', 'REUSED', 'UNKNOWN'):
            for changes in (dict(status='REVOKED'), dict(roles=['ADMIN']), dict(roles=[])):
                row = result_row(state)
                q = query(SharingCommitStatus.UNKNOWN) if state == 'UNKNOWN' else query()
                if state == 'UNKNOWN':
                    row['receipt'] = None
                row['membership'].update(changes)
                with self.assertRaises(AuthorizationError):
                    self.check(q, row)

    def test_current_workspace_and_conversation_lifecycle_deny(self):
        row = result_row('REUSED')
        row['workspace']['status'] = 'DISABLED'
        self.assert_code('WORKSPACE_INACTIVE', self.check, row=row)
        row = result_row('REUSED')
        row['conversation']['status'] = 'ARCHIVED'
        self.assert_code('CONVERSATION_ARCHIVED', self.check, row=row)
        row = result_row('REUSED')
        row['conversation']['owner_id'] = ADMIN.principal_id
        with self.assertRaises(AuthorizationError):
            self.check(row=row)

    def test_returned_identity_cannot_be_substituted_or_cross_scoped(self):
        row = result_row()
        row['membership']['identity_subject'] = 'different-subject'
        self.assert_code('MEMBERSHIP_IDENTITY_MISMATCH', self.check, row=row)
        row = result_row()
        row['conversation']['workspace_id'] = 'another-workspace'
        self.assert_code('CONVERSATION_SCOPE_MISMATCH', self.check, row=row)

    def test_changed_intent_receipt_conflicts_and_corrupted_audit_denies(self):
        row = result_row('REUSED')
        self.assert_code('SHARING_IDEMPOTENCY_CONFLICT', self.check,
                         query(command=command(trace_id='changed')), row)
        for field, value in (('new_revision', 5), ('trace_id', 'changed'), ('request_id', 'unexpected'),
                             ('mutation_outcome', 'UNKNOWN'), ('allowed', False)):
            row = result_row('REUSED')
            row['receipt'][field] = value
            with self.assertRaises(ContractError):
                self.check(row=row)

    def test_receipt_cannot_claim_future_or_inconsistent_conversation_state(self):
        row = result_row('REUSED')
        row['receipt']['occurred_at'] = LATER + timedelta(seconds=1)
        self.assert_code('INVALID_SHARING_RECEIPT_TIME', self.check, row=row)
        row = result_row('REUSED')
        row['conversation']['visibility'] = 'PRIVATE'
        self.assert_code('INCONSISTENT_SHARING_RECEIPT', self.check, row=row)

    def test_result_observation_must_stay_within_original_retry_window(self):
        row = result_row('REUSED')
        row['observed_at'] = command().deadline_at
        self.assert_code('SHARING_RETRY_EXPIRED', self.check, row=row)
        row['observed_at'] = TIME - timedelta(seconds=1)
        self.assert_code('SHARING_INTENT_NOT_YET_VALID', self.check, row=row)

    def test_result_job_id_is_bounded_and_errors_do_not_leak_content(self):
        for value in (None, '', 'user@example.invalid', 'x'*129):
            self.assert_code('INVALID_AUDIT_IDENTIFIER', validate_sharing_result,
                             query(), [result_row()], query_job_id=value)
        row = result_row() | dict(extra='sensitive-secret')
        with self.assertRaises(ContractError) as caught:
            self.check(row=row)
        self.assertNotIn('sensitive-secret', str(caught.exception))

    def test_query_and_result_work_without_cloud_clients_or_identity_calls(self):
        with patch('google.cloud.bigquery.Client', side_effect=AssertionError('Cloud client used')), \
             patch('src.application.identity.IapIdentityAdapter.authenticate', side_effect=AssertionError('IAP used')):
            q = query()
            q.job_config().to_api_repr()
            self.check(q)


if __name__ == '__main__':
    unittest.main()
