"""Offline coordinator observations; never prove a live commit or publish rows."""
import base64
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from src.application.access_policy import AuthorizationError, Reason, Visibility
from src.application.sharing_coordinator_contract import (
    SharingCommand, SharingCommitObservation, SharingCommitStatus, SharingPreparation,
    SharingPreparationState, make_sharing_command, prepare_sharing,
)
from src.application.workflow_contract import ContractError, RETRY_WINDOW
from src.application.workflow_schema import TABLES
from src.application.workspace_contract import MAX_REVISION
from tests.test_conversation_contract import CONVERSATION, LATER, candidate, conversation
from tests.test_membership_contract import ADMIN, TARGET, TIME, WORKSPACE, admin, membership, workspace


def command(**changes):
    return make_sharing_command(**(dict(workspace_id=WORKSPACE, conversation_id=CONVERSATION,
        principal_id=TARGET.principal_id, visibility=Visibility.TEAM, expected_membership_revision=3,
        expected_conversation_revision=3, accepted_at=TIME, trace_id='fixture-intent-trace',
        retry_key='fixture-retry') | changes))


class SharingCoordinatorContractTests(unittest.TestCase):
    def prepare(self, cmd=None, **changes):
        cmd = command() if cmd is None else cmd
        return prepare_sharing(cmd, **(dict(workspace_rows=[workspace()], membership_rows=[membership()],
            conversation_rows=[conversation()], receipt_rows=[], expected_workspace_revision=7,
            observed_at=LATER,
            commit_observation=SharingCommitObservation(cmd.audit_id, SharingCommitStatus.NOT_SUBMITTED)) | changes))

    def receipt(self):
        return dict(self.prepare().plan.audit_candidate)

    def reuse(self, cmd=None, **changes):
        return self.prepare(cmd, **(dict(workspace_rows=[workspace(revision=8, updated_at=LATER)],
            conversation_rows=[candidate()], receipt_rows=[self.receipt()],
            observed_at=LATER+timedelta(seconds=1)) | changes))

    def observed(self, status, cmd=None):
        cmd = command() if cmd is None else cmd
        return SharingCommitObservation(cmd.audit_id, status,
                                       None if status is SharingCommitStatus.NOT_SUBMITTED else 'fixture-query-job')

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_deterministic_scoped_digest_ids_keep_full_hashes(self):
        cmd = command()
        self.assertEqual(cmd, command())
        self.assertEqual(cmd.audit_id, command().audit_id)
        self.assertLessEqual(len(cmd.audit_id), 128)
        prefix, key_hash, payload_hash = cmd.audit_id.split(':')
        self.assertEqual(prefix, 'share-v1')
        self.assertEqual(len(base64.urlsafe_b64decode(key_hash+'=')), 32)
        self.assertEqual(base64.urlsafe_b64decode(payload_hash+'=').hex(), cmd.command_sha256)
        self.assertNotIn('fixture-retry', repr(cmd))

    def test_retry_identity_is_scoped_to_actor_workspace_conversation(self):
        original = command()
        for change in (dict(workspace_id='other'), dict(conversation_id='other'),
                       dict(principal_id=ADMIN.principal_id), dict(retry_key='another-retry')):
            self.assertNotEqual(original.audit_prefix, command(**change).audit_prefix)

    def test_reused_key_changed_intent_retains_lookup_prefix_but_changes_id(self):
        cmd = command()
        for change in (dict(visibility=Visibility.PRIVATE), dict(expected_membership_revision=4),
                       dict(expected_conversation_revision=4), dict(accepted_at=LATER),
                       dict(trace_id='another-intent-trace')):
            changed = command(**change)
            self.assertEqual(changed.audit_prefix, cmd.audit_prefix)
            self.assertNotEqual(changed.audit_id, cmd.audit_id)

    def test_equivalent_timezone_intents_have_identical_hashes(self):
        local = TIME.astimezone(timezone(timedelta(hours=5, minutes=30)))
        self.assertEqual(command().audit_id, command(accepted_at=local).audit_id)

    def test_command_is_frozen_and_versioned(self):
        with self.assertRaises(FrozenInstanceError):
            command().visibility = Visibility.PRIVATE
        self.assert_code('INVALID_SHARING_VERSION', replace, command(), version='old')

    def test_command_inputs_are_bounded_and_typed(self):
        for value in ('', 'has space', 'x'*129, 'jwt.payload.signature', 'user@example.invalid'):
            self.assert_code('INVALID_AUDIT_IDENTIFIER', command, retry_key=value)
        self.assert_code('INVALID_AUDIT_IDENTIFIER', command, conversation_id='has space')
        self.assert_code('INVALID_CONVERSATION_VISIBILITY', command, visibility='TEAM')
        self.assert_code('INVALID_DIGEST', replace, command(), retry_key_sha256='not-a-digest')
        for value in (True, 3.0, '3', 0, MAX_REVISION+1):
            self.assert_code('INVALID_SHARING_REVISION', command, expected_membership_revision=value)
            self.assert_code('INVALID_SHARING_REVISION', command, expected_conversation_revision=value)
        self.assert_code('CONVERSATION_REVISION_EXHAUSTED', command, expected_conversation_revision=MAX_REVISION)

    def test_command_timestamps_and_window_are_representable(self):
        self.assert_code('INVALID_TIMESTAMP', command, accepted_at=TIME.replace(tzinfo=None))
        self.assert_code('INVALID_SHARING_WINDOW', command, accepted_at=datetime.max.replace(tzinfo=timezone.utc))

    def test_plan_contains_only_scoped_fence_change_and_audit(self):
        result = self.prepare()
        self.assertEqual(result.state, SharingPreparationState.PREPARED)
        self.assertIsNone(result.resulting_revision)
        self.assertIsNone(result.receipt)
        plan = result.plan
        self.assertEqual(dict(plan.workspace_before), workspace())
        self.assertEqual(dict(plan.workspace_after), workspace(revision=8, updated_at=LATER))
        self.assertEqual(dict(plan.conversation_after), candidate())
        self.assertEqual(set(plan.audit_candidate), {field.name for field in TABLES['audit_events'].schema})
        self.assertEqual(plan.audit_candidate['audit_id'], command().audit_id)
        self.assertEqual(plan.audit_candidate['mutation_outcome'], 'APPLIED')
        self.assertEqual(plan.audit_candidate['membership_revision'], 3)
        self.assertEqual(plan.audit_candidate['resource_revision'], 3)
        self.assertEqual(plan.audit_candidate['new_revision'], 4)
        self.assertIsNone(plan.audit_candidate['job_id'])

    def test_prepared_applied_audit_is_not_a_commit_result(self):
        result = self.prepare()
        self.assertEqual(result.state, SharingPreparationState.PREPARED)
        self.assertIsNone(result.resulting_revision)

    def test_plan_rows_are_copied_and_immutable(self):
        ws, member, conv = workspace(), membership(), conversation()
        original = deepcopy((ws, member, conv))
        result = self.prepare(workspace_rows=[ws], membership_rows=[member], conversation_rows=[conv])
        self.assertEqual((ws, member, conv), original)
        for row in (result.plan.workspace_before, result.plan.workspace_after,
                    result.plan.conversation_before, result.plan.conversation_after, result.plan.audit_candidate):
            with self.assertRaises(TypeError):
                row['owner_id'] = 'other'
        ws['status'] = 'DISABLED'
        conv['visibility'] = 'TEAM'
        self.assertEqual(result.plan.workspace_before['status'], 'ACTIVE')
        self.assertEqual(result.plan.conversation_before['visibility'], 'PRIVATE')

    def test_plan_constructor_revalidates_modified_proposals(self):
        plan = self.prepare().plan
        self.assert_code('IMMUTABLE_CONVERSATION_OWNER', replace, plan,
                         conversation_after=dict(plan.conversation_after) | dict(owner_id=ADMIN.principal_id))
        self.assert_code('INVALID_WORKSPACE_REVISION_INCREMENT', replace, plan,
                         workspace_after=dict(plan.workspace_after) | dict(revision=9))
        self.assert_code('SHARING_PLAN_AUDIT_MISMATCH', replace, plan,
                         audit_candidate=dict(plan.audit_candidate) | dict(trace_id='different'))
        self.assert_code('SHARING_PLAN_SCOPE_MISMATCH', replace, plan,
                         command=command(visibility=Visibility.PRIVATE))

    def test_missing_control_rows_do_not_bootstrap(self):
        for field, label in (('workspace_rows', 'WORKSPACE'), ('membership_rows', 'MEMBERSHIP'),
                             ('conversation_rows', 'CONVERSATION')):
            self.assert_code('MISSING_SHARING_'+label, self.prepare, **{field: []})

    def test_duplicate_reads_fail_closed_including_receipts(self):
        for field, label, row in (('workspace_rows', 'WORKSPACE', workspace()),
                                 ('membership_rows', 'MEMBERSHIP', membership()),
                                 ('conversation_rows', 'CONVERSATION', conversation()),
                                 ('receipt_rows', 'RECEIPT', self.receipt())):
            self.assert_code('DUPLICATE_SHARING_'+label, self.prepare, **{field: [row, deepcopy(row)]})

    def test_lookup_shapes_and_counts_are_bounded(self):
        for rows in ((workspace(),), [workspace()]*3, 'record'):
            self.assert_code('INVALID_SHARING_LOOKUP', self.prepare, workspace_rows=rows)
        self.assert_code('INVALID_ROW_SHAPE', self.prepare, workspace_rows=[None])

    def test_all_control_rows_are_scope_bound(self):
        self.assert_code('WORKSPACE_SCOPE_MISMATCH', self.prepare,
                         workspace_rows=[workspace(workspace_id='other')])
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.prepare, membership_rows=[admin()])
        self.assert_code('CONVERSATION_SCOPE_MISMATCH', self.prepare,
                         conversation_rows=[conversation(conversation_id='other')])

    def test_inactive_workspace_and_archived_conversation_cannot_prepare(self):
        self.assert_code('WORKSPACE_INACTIVE', self.prepare, workspace_rows=[workspace(status='DISABLED')])
        self.assert_code('CONVERSATION_ARCHIVED', self.prepare,
                         conversation_rows=[conversation(status='ARCHIVED')])

    def test_revoked_and_suspended_creator_cannot_prepare_or_reuse(self):
        for status in ('REVOKED', 'SUSPENDED'):
            for function in (self.prepare, self.reuse):
                with self.assertRaises(AuthorizationError) as caught:
                    function(membership_rows=[membership(status=status)])
                self.assertEqual(caught.exception.decision.reason, Reason.MEMBERSHIP_INACTIVE)

    def test_noncreator_and_nonowner_cannot_prepare_or_reuse(self):
        for function in (self.prepare, self.reuse):
            with self.assertRaises(AuthorizationError) as caught:
                function(membership_rows=[membership(roles=['ADMIN'])])
            self.assertEqual(caught.exception.decision.reason, Reason.ROLE_REQUIRED)
            with self.assertRaises(AuthorizationError) as caught:
                function(conversation_rows=[conversation(owner_id=ADMIN.principal_id)])
            self.assertEqual(caught.exception.decision.reason, Reason.OWNER_REQUIRED)

    def test_stale_workspace_membership_and_conversation_revisions(self):
        self.assert_code('STALE_WORKSPACE_REVISION', self.prepare, expected_workspace_revision=6)
        self.assert_code('STALE_MEMBERSHIP_REVISION', self.prepare, membership_rows=[membership(revision=4)])
        self.assert_code('STALE_CONVERSATION_REVISION', self.prepare,
                         conversation_rows=[conversation(access_revision=4)])

    def test_no_implicit_revision_rebase_or_noop(self):
        self.assert_code('CONVERSATION_VISIBILITY_UNCHANGED', self.prepare,
                         conversation_rows=[conversation(visibility='TEAM')])
        self.assert_code('STALE_CONVERSATION_REVISION', self.prepare,
                         conversation_rows=[candidate()])

    def test_operational_fence_can_refresh_without_changing_intent_hash(self):
        cmd = command()
        original_hash = cmd.command_sha256
        result = self.prepare(cmd, workspace_rows=[workspace(revision=12)], expected_workspace_revision=12,
            commit_observation=self.observed(SharingCommitStatus.NOT_COMMITTED, cmd))
        self.assertEqual(result.plan.workspace_after['revision'], 13)
        self.assertEqual(result.plan.command.command_sha256, original_hash)
        self.assertEqual(result.plan.audit_candidate['audit_id'], cmd.audit_id)

    def test_window_is_original_bounded_and_exclusive_at_deadline(self):
        self.assert_code('SHARING_INTENT_NOT_YET_VALID', self.prepare, observed_at=TIME-timedelta(seconds=1))
        self.assert_code('SHARING_RETRY_EXPIRED', self.prepare, observed_at=TIME+RETRY_WINDOW)
        self.assert_code('SHARING_RETRY_EXPIRED', self.reuse, observed_at=TIME+RETRY_WINDOW)
        self.prepare(observed_at=TIME+RETRY_WINDOW-timedelta(microseconds=1))

    def test_future_snapshots_cannot_authorize(self):
        future = LATER+timedelta(seconds=1)
        self.assert_code('INVALID_SHARING_OBSERVATION_TIME', self.prepare,
                         workspace_rows=[workspace(updated_at=future)])
        self.assert_code('INVALID_SHARING_OBSERVATION_TIME', self.prepare,
                         membership_rows=[membership(updated_at=future)])
        self.assert_code('INVALID_SHARING_OBSERVATION_TIME', self.prepare,
                         conversation_rows=[conversation(updated_at=future)])

    def test_matching_receipt_reuses_original_result_without_proposal(self):
        result = self.reuse()
        self.assertEqual(result.state, SharingPreparationState.REUSED)
        self.assertEqual(result.resulting_revision, 4)
        self.assertIsNone(result.plan)
        self.assertEqual(dict(result.receipt), self.receipt())

    def test_receipt_reuse_checks_current_permission_not_old_member_revision(self):
        result = self.reuse(membership_rows=[membership(revision=8)],
                            workspace_rows=[workspace(revision=20, updated_at=LATER)])
        self.assertEqual(result.state, SharingPreparationState.REUSED)
        self.assertEqual(result.receipt['membership_revision'], 3)

    def test_historical_receipt_does_not_restore_later_visibility(self):
        later = LATER+timedelta(seconds=1)
        row = conversation(visibility='PRIVATE', access_revision=5, updated_at=later)
        original = deepcopy(row)
        result = self.reuse(conversation_rows=[row], observed_at=later+timedelta(seconds=1))
        self.assertEqual(result.resulting_revision, 4)
        self.assertIsNone(result.plan)
        self.assertEqual(row, original)

    def test_same_retry_key_changed_payload_conflicts(self):
        for change in (dict(visibility=Visibility.PRIVATE), dict(expected_membership_revision=4),
                       dict(expected_conversation_revision=4), dict(trace_id='changed')):
            self.assert_code('SHARING_IDEMPOTENCY_CONFLICT', self.reuse, command(**change))

    def test_foreign_receipt_scope_does_not_resolve_submission(self):
        self.assert_code('SHARING_RECEIPT_SCOPE_MISMATCH', self.reuse,
                         receipt_rows=[self.receipt() | dict(workspace_id='other')])
        self.assert_code('SHARING_RECEIPT_SCOPE_MISMATCH', self.reuse,
                         receipt_rows=[self.receipt() | dict(audit_id='unrelated')])

    def test_malformed_or_nonapplied_receipt_is_not_commit_proof(self):
        for change in (dict(allowed=False), dict(mutation_outcome='UNKNOWN', new_revision=None),
                       dict(actor_id=ADMIN.principal_id), dict(resource_revision=2),
                       dict(new_revision=99), dict(trace_id='wrong'), dict(extra='content')):
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.reuse(receipt_rows=[self.receipt() | change])

    def test_receipt_times_must_match_original_window_and_observation(self):
        for instant in (TIME-timedelta(microseconds=1), TIME+RETRY_WINDOW,
                        LATER+timedelta(seconds=2)):
            self.assert_code('INVALID_SHARING_RECEIPT_TIME', self.reuse,
                             receipt_rows=[self.receipt() | dict(occurred_at=instant)])

    def test_receipt_and_current_conversation_must_be_consistent(self):
        for row in (conversation(), candidate(visibility='PRIVATE'), candidate(updated_at=TIME)):
            self.assert_code('INCONSISTENT_SHARING_RECEIPT', self.reuse, conversation_rows=[row])

    def test_unknown_or_committed_without_receipt_stays_unknown(self):
        for status in (SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED):
            for rows in ([conversation()], [candidate()]):
                result = self.prepare(commit_observation=self.observed(status), conversation_rows=rows)
                self.assertEqual(result.state, SharingPreparationState.UNKNOWN)
                self.assertIsNone(result.plan)
                self.assertIsNone(result.resulting_revision)

    def test_unknown_and_committed_can_resolve_from_matching_receipt(self):
        for status in (SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED):
            result = self.reuse(commit_observation=self.observed(status))
            self.assertEqual(result.state, SharingPreparationState.REUSED)

    def test_positive_noncommit_observation_can_prepare_only_with_current_pins(self):
        obs = self.observed(SharingCommitStatus.NOT_COMMITTED)
        self.assertEqual(self.prepare(commit_observation=obs).state, SharingPreparationState.PREPARED)
        self.assert_code('STALE_CONVERSATION_REVISION', self.prepare, commit_observation=obs,
                         conversation_rows=[candidate()])

    def test_receipt_contradicts_noncommit_observation(self):
        self.assert_code('SHARING_COMMIT_OBSERVATION_CONFLICT', self.reuse,
                         commit_observation=self.observed(SharingCommitStatus.NOT_COMMITTED))

    def test_commit_observation_is_typed_scoped_and_has_preallocated_job_id(self):
        self.assert_code('INVALID_SHARING_COMMIT_STATUS', SharingCommitObservation, command().audit_id, 'UNKNOWN')
        self.assert_code('INVALID_AUDIT_IDENTIFIER', SharingCommitObservation, command().audit_id,
                         SharingCommitStatus.UNKNOWN)
        self.assert_code('INVALID_SHARING_QUERY_ID', SharingCommitObservation, command().audit_id,
                         SharingCommitStatus.NOT_SUBMITTED, 'query')
        wrong = SharingCommitObservation(command(retry_key='other').audit_id, SharingCommitStatus.UNKNOWN, 'query')
        self.assert_code('SHARING_COMMIT_SCOPE_MISMATCH', self.prepare, commit_observation=wrong)
        self.assert_code('INVALID_SHARING_COMMIT_STATUS', self.prepare, commit_observation='timeout')

    def test_result_state_cannot_mix_receipt_plan_and_uncertainty(self):
        result = self.prepare()
        self.assert_code('INVALID_SHARING_PREPARATION', SharingPreparation, 'PREPARED')
        self.assert_code('INVALID_SHARING_PREPARATION', SharingPreparation,
                         SharingPreparationState.UNKNOWN, plan=result.plan)
        self.assert_code('INVALID_SHARING_PREPARATION', SharingPreparation,
                         SharingPreparationState.REUSED, plan=result.plan, receipt=self.receipt())
        self.assert_code('INVALID_SHARING_PREPARATION', SharingPreparation,
                         SharingPreparationState.REUSED,
                         receipt=self.receipt() | dict(mutation_outcome='UNKNOWN', new_revision=None))

    def test_reused_receipt_is_copied_and_immutable(self):
        row = self.receipt()
        result = self.reuse(receipt_rows=[row])
        row['new_revision'] = 999
        self.assertEqual(result.resulting_revision, 4)
        with self.assertRaises(TypeError):
            result.receipt['new_revision'] = 999

    def test_failed_preparation_preserves_all_input_rows(self):
        rows = ([workspace()], [membership(status='REVOKED')], [conversation()], [self.receipt()])
        original = deepcopy(rows)
        with self.assertRaises(AuthorizationError):
            self.prepare(workspace_rows=rows[0], membership_rows=rows[1],
                         conversation_rows=rows[2], receipt_rows=rows[3])
        self.assertEqual(rows, original)

    def test_no_authentication_cloud_io_or_model_calls(self):
        with patch('google.cloud.bigquery.Client') as client, patch('google.auth.default') as adc, \
                patch('google.oauth2.id_token.verify_token') as verifier:
            self.prepare()
            self.reuse()
            self.prepare(commit_observation=self.observed(SharingCommitStatus.UNKNOWN))
            client.assert_not_called()
            adc.assert_not_called()
            verifier.assert_not_called()

    def test_safe_errors_never_echo_input_content(self):
        secret = 'private retry content'
        with self.assertRaises(ContractError) as caught:
            command(retry_key=secret)
        self.assertNotIn(secret, str(caught.exception))


if __name__ == '__main__':
    unittest.main()
