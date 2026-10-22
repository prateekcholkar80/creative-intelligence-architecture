"""Offline membership snapshots and proposals; never publish fixture accounts."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from src.application.access_policy import (
    Action, AuthorizationError, MembershipStatus, Reason, Role, authorize,
)
from src.application.identity import ISSUER, principal_for_subject
from src.application.membership_contract import (
    AssignmentOrigin, propose_admin_membership_change, validate_admin_membership_mutation,
    validate_membership, validate_membership_change, validate_membership_creation,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import SCHEMA_VERSION
from src.application.workspace_contract import MAX_REVISION

WORKSPACE = 'fixture-workspace'
TIME = datetime(2026, 10, 10, tzinfo=timezone.utc)
LATER = TIME + timedelta(seconds=1)
SUBJECT = 'accounts.google.com:fixture-member'
ADMIN_SUBJECT = 'accounts.google.com:fixture-admin'
TARGET = principal_for_subject(ISSUER, SUBJECT)
ADMIN = principal_for_subject(ISSUER, ADMIN_SUBJECT)


def workspace(**changes):
    return dict(schema_version=SCHEMA_VERSION, workspace_id=WORKSPACE, status='ACTIVE',
                revision=7, created_at=TIME, updated_at=TIME) | changes


def membership(**changes):
    return dict(schema_version=SCHEMA_VERSION, workspace_id=WORKSPACE,
                principal_id=TARGET.principal_id, identity_issuer=ISSUER, identity_subject=SUBJECT,
                roles=['CREATOR'], status='ACTIVE', revision=3, assignment_origin='ADMIN',
                updated_by=ADMIN.principal_id, created_at=TIME, updated_at=TIME) | changes


def admin(**changes):
    return membership(principal_id=ADMIN.principal_id, identity_subject=ADMIN_SUBJECT,
                      roles=['ADMIN'], revision=5, **changes)


def candidate(**changes):
    return membership(roles=['VIEWER'], revision=4, updated_at=LATER) | changes


class MembershipContractTests(unittest.TestCase):
    def validate(self, row, **changes):
        return validate_membership(row, **(dict(workspace_id=WORKSPACE, principal_id=TARGET.principal_id) | changes))

    def create(self, row=None, **changes):
        return validate_membership_creation(membership(revision=1) if row is None else row,
            **(dict(workspace_id=WORKSPACE, principal_id=TARGET.principal_id, observed_at=TIME,
                    updated_by=ADMIN.principal_id, origin=AssignmentOrigin.ADMIN) | changes))

    def change(self, previous=None, proposed=None, **changes):
        return validate_membership_change(membership() if previous is None else previous,
            candidate() if proposed is None else proposed,
            **(dict(workspace_id=WORKSPACE, principal_id=TARGET.principal_id, expected_revision=3,
                    observed_at=LATER, updated_by=ADMIN.principal_id, origin=AssignmentOrigin.ADMIN) | changes))

    def gate(self, proposed=None, **changes):
        return validate_admin_membership_mutation(candidate() if proposed is None else proposed,
            **(dict(previous=membership(), workspace=workspace(), workspace_id=WORKSPACE,
                    principal_id=TARGET.principal_id, expected_workspace_revision=7, expected_revision=3,
                    actor=ADMIN, actor_membership=admin(), observed_at=LATER) | changes))

    def propose(self, previous=None, **changes):
        return propose_admin_membership_change(membership() if previous is None else previous,
            **(dict(roles=frozenset({Role.VIEWER}), status=MembershipStatus.ACTIVE,
                    workspace=workspace(), workspace_id=WORKSPACE, principal_id=TARGET.principal_id,
                    expected_workspace_revision=7, expected_revision=3, actor=ADMIN,
                    actor_membership=admin(), observed_at=LATER) | changes))

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def assert_denied(self, reason, **changes):
        with self.assertRaises(AuthorizationError) as caught:
            self.gate(**changes)
        self.assertEqual(caught.exception.decision.reason, reason)

    def test_stored_status_origin_and_additive_roles(self):
        for status in MembershipStatus:
            for origin in AssignmentOrigin:
                with self.subTest(status=status, origin=origin):
                    result = self.validate(membership(status=status.value, assignment_origin=origin.value,
                                                      roles=[role.value for role in Role]))
                    self.assertEqual(result.roles, frozenset(Role))
                    self.assertEqual(result.status, status)

    def test_empty_roles_are_valid_but_grant_no_permission(self):
        member = self.validate(membership(roles=[]))
        self.assertFalse(authorize(TARGET, member, Action.GENERATE_DRAFT, workspace_id=WORKSPACE).allowed)

    def test_returns_typed_immutable_snapshot(self):
        row = membership()
        member = self.validate(row)
        row['roles'].append('ADMIN')
        self.assertEqual(member.roles, frozenset({Role.CREATOR}))
        with self.assertRaises(AttributeError):
            member.revision = 99

    def test_exact_schema_no_extra_roles_or_missing_identity(self):
        for name in membership():
            row = membership()
            del row[name]
            with self.subTest(name=name):
                self.assert_code('INVALID_ROW_SHAPE', self.validate, row)
        self.assert_code('INVALID_ROW_SHAPE', self.validate, membership(email='fixture@example.invalid'))

    def test_non_rows_and_required_nulls(self):
        for row in (None, [], 'record'):
            self.assert_code('INVALID_ROW_SHAPE', self.validate, row)
        for name in set(membership()) - {'roles'}:
            self.assert_code('MISSING_REQUIRED_FIELD', self.validate, membership(**{name: None}))

    def test_version_workspace_and_target_scope(self):
        self.assert_code('INVALID_SCHEMA_VERSION', self.validate, membership(schema_version='old'))
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.validate, membership(workspace_id='other'))
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.validate, membership(principal_id=ADMIN.principal_id))

    def test_bounded_identifiers(self):
        for value in ('', 'has space', 'bad\nidentifier', 'x'*257):
            self.assert_code('INVALID_IDENTIFIER', self.validate, membership(), principal_id=value)
        self.assert_code('INVALID_IDENTIFIER', self.validate, membership(), workspace_id='x'*129)

    def test_subject_must_hash_to_principal(self):
        self.assert_code('MEMBERSHIP_IDENTITY_MISMATCH', self.validate,
                         membership(identity_subject=ADMIN_SUBJECT))
        self.assert_code('MEMBERSHIP_IDENTITY_MISMATCH', self.validate,
                         membership(principal_id='fixture-label'), principal_id='fixture-label')

    def test_only_existing_iap_issuer_is_supported(self):
        self.assert_code('INVALID_MEMBERSHIP_IDENTITY', self.validate,
                         membership(identity_issuer='https://accounts.google.com'))

    def test_subject_bounds_controls_and_utf8(self):
        for value in ('x'*257, 'has space', 'bad\x00subject', '\ud800'):
            self.assert_code('INVALID_MEMBERSHIP_IDENTITY', self.validate, membership(identity_subject=value))

    def test_email_does_not_become_principal_or_role(self):
        self.assert_code('MEMBERSHIP_IDENTITY_MISMATCH', self.validate,
                         membership(principal_id='admin@example.invalid'), principal_id='admin@example.invalid')
        self.assert_code('INVALID_MEMBERSHIP_ROLES', self.validate, membership(roles=['example.invalid']))

    def test_roles_are_unique_known_strings_in_a_list(self):
        for value in (['ADMIN', 'ADMIN'], ['UNKNOWN'], ['creator']):
            self.assert_code('INVALID_MEMBERSHIP_ROLES', self.validate, membership(roles=value))
        for value in ('ADMIN', frozenset({'ADMIN'}), [None], [1], None):
            self.assert_code('INVALID_FIELD_TYPE', self.validate, membership(roles=value))

    def test_status_and_origin_are_closed_sets(self):
        self.assert_code('INVALID_MEMBERSHIP_STATUS', self.validate, membership(status='DELETED'))
        self.assert_code('INVALID_ASSIGNMENT_ORIGIN', self.validate, membership(assignment_origin='PROMPT'))

    def test_updater_is_bounded_identifier(self):
        for value in ('has space', 'bad\nupdater', 'x'*257):
            self.assert_code('INVALID_IDENTIFIER', self.validate, membership(updated_by=value))

    def test_positive_int64_revision(self):
        self.validate(membership(revision=MAX_REVISION))
        for value in (0, -1):
            self.assert_code('INVALID_MEMBERSHIP_REVISION', self.validate, membership(revision=value))
        for value in (True, 1.0, '1', MAX_REVISION+1):
            self.assert_code('INVALID_FIELD_TYPE', self.validate, membership(revision=value))

    def test_timestamp_awareness_order_and_utc_range(self):
        for name in ('created_at', 'updated_at'):
            self.assert_code('INVALID_TIMESTAMP', self.validate,
                             membership(**{name: TIME.replace(tzinfo=None)}))
        self.assert_code('INVALID_MEMBERSHIP_TIMELINE', self.validate,
                         membership(updated_at=TIME-timedelta(seconds=1)))
        instant = datetime.min.replace(tzinfo=timezone(timedelta(hours=1)))
        self.assert_code('INVALID_TIMESTAMP', self.validate, membership(created_at=instant, updated_at=instant))

    def test_creation_structurally_accepts_all_recorded_origins_and_statuses(self):
        for origin in AssignmentOrigin:
            for status in MembershipStatus:
                self.create(membership(revision=1, assignment_origin=origin.value, status=status.value), origin=origin)

    def test_creation_requires_revision_one(self):
        self.assert_code('INVALID_INITIAL_MEMBERSHIP_REVISION', self.create, membership(revision=2))

    def test_creation_binds_server_observation_time(self):
        self.assert_code('INVALID_MEMBERSHIP_CREATION_TIME', self.create, observed_at=LATER)
        self.assert_code('INVALID_MEMBERSHIP_CREATION_TIME', self.create,
                         membership(revision=1, updated_at=LATER))

    def test_assignment_binds_updater_and_typed_origin(self):
        self.assert_code('MEMBERSHIP_ASSIGNMENT_MISMATCH', self.create, updated_by=TARGET.principal_id)
        self.assert_code('MEMBERSHIP_ASSIGNMENT_MISMATCH', self.create, origin=AssignmentOrigin.SYNC)
        self.assert_code('INVALID_ASSIGNMENT_ORIGIN', self.create, origin='ADMIN')

    def test_role_update_and_all_explicit_status_transitions(self):
        self.change()
        for before in MembershipStatus:
            for after in MembershipStatus:
                if before != after:
                    self.change(membership(status=before.value),
                                candidate(roles=['CREATOR'], status=after.value))

    def test_stale_expected_revision_is_rejected(self):
        self.assert_code('STALE_MEMBERSHIP_REVISION', self.change, expected_revision=2)
        self.assert_code('STALE_MEMBERSHIP_REVISION', self.propose, expected_revision=2)

    def test_expected_revision_is_strict_int64(self):
        for value in (True, '3', 3.0, 0, -1, MAX_REVISION+1):
            self.assert_code('INVALID_MEMBERSHIP_REVISION', self.change, expected_revision=value)

    def test_revision_must_increment_exactly_once(self):
        for value in (1, 3, 5):
            self.assert_code('INVALID_MEMBERSHIP_REVISION_INCREMENT', self.change,
                             proposed=candidate(revision=value))

    def test_revision_exhaustion_cannot_wrap(self):
        self.assert_code('MEMBERSHIP_REVISION_EXHAUSTED', self.change,
                         membership(revision=MAX_REVISION), candidate(revision=1), expected_revision=MAX_REVISION)
        self.assert_code('MEMBERSHIP_REVISION_EXHAUSTED', self.propose,
                         membership(revision=MAX_REVISION), expected_revision=MAX_REVISION)

    def test_identity_and_creation_time_cannot_change(self):
        self.assert_code('MEMBERSHIP_IDENTITY_MISMATCH', self.change,
                         proposed=candidate(identity_subject=ADMIN_SUBJECT))
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.change,
                         proposed=candidate(principal_id=ADMIN.principal_id, identity_subject=ADMIN_SUBJECT))
        self.assert_code('IMMUTABLE_MEMBERSHIP_IDENTITY', self.change,
                         proposed=candidate(created_at=LATER))

    def test_change_time_is_observed_and_nondecreasing(self):
        for instant in (TIME, LATER+timedelta(seconds=1)):
            self.assert_code('INVALID_MEMBERSHIP_CHANGE_TIME', self.change,
                             proposed=candidate(updated_at=instant))
        self.assert_code('INVALID_MEMBERSHIP_CHANGE_TIME', self.change,
                         membership(updated_at=LATER), candidate(updated_at=TIME), observed_at=TIME)
        self.change(proposed=candidate(updated_at=TIME), observed_at=TIME)

    def test_noop_and_role_reordering_do_not_consume_revision(self):
        self.assert_code('MEMBERSHIP_UNCHANGED', self.change, proposed=candidate(roles=['CREATOR']))
        self.assert_code('MEMBERSHIP_UNCHANGED', self.change,
            membership(roles=['VIEWER', 'CREATOR']), candidate(roles=['CREATOR', 'VIEWER']))
        self.assert_code('MEMBERSHIP_UNCHANGED', self.propose, roles=frozenset({Role.CREATOR}))

    def test_bootstrap_cannot_update_existing_membership(self):
        self.assert_code('BOOTSTRAP_UPDATE_DISABLED', self.change,
                         proposed=candidate(assignment_origin='BOOTSTRAP'), origin=AssignmentOrigin.BOOTSTRAP)
        self.change(proposed=candidate(assignment_origin='SYNC'), origin=AssignmentOrigin.SYNC)

    def test_admin_update_returns_actor_pre_mutation_decision(self):
        decision = self.gate()
        self.assertTrue(decision.allowed)
        self.assertEqual(decision.action, Action.MANAGE_MEMBERS)
        self.assertEqual(decision.principal_id, ADMIN.principal_id)
        self.assertEqual(decision.membership_revision, 5)
        self.assertIsNone(decision.resource_id)

    def test_admin_creation_requires_absent_prior_revision(self):
        row = membership(revision=1, created_at=LATER, updated_at=LATER)
        self.assertTrue(self.gate(row, previous=None, expected_revision=None).allowed)
        self.assert_code('INVALID_INITIAL_MEMBERSHIP_REVISION', self.gate, row,
                         previous=None, expected_revision=1)

    def test_user_admin_path_cannot_claim_sync_or_bootstrap(self):
        for origin in ('SYNC', 'BOOTSTRAP'):
            self.assert_code('MEMBERSHIP_ASSIGNMENT_MISMATCH', self.gate,
                             candidate(assignment_origin=origin))
            self.assert_code('MEMBERSHIP_ASSIGNMENT_MISMATCH', self.gate,
                membership(revision=1, created_at=LATER, updated_at=LATER, assignment_origin=origin),
                previous=None, expected_revision=None)

    def test_admin_candidate_cannot_impersonate_updater(self):
        self.assert_code('MEMBERSHIP_ASSIGNMENT_MISMATCH', self.gate,
                         candidate(updated_by=TARGET.principal_id))

    def test_non_admin_roles_cannot_mutate_memberships(self):
        for roles in ([], ['VIEWER'], ['CREATOR'], ['REVIEWER'], ['VIEWER', 'CREATOR', 'REVIEWER']):
            self.assert_denied(Reason.ROLE_REQUIRED, actor_membership=admin() | dict(roles=roles))

    def test_revoked_or_suspended_admin_cannot_mutate(self):
        for status in ('SUSPENDED', 'REVOKED'):
            self.assert_denied(Reason.MEMBERSHIP_INACTIVE, actor_membership=admin(status=status))

    def test_disabled_workspace_blocks_active_admin(self):
        self.assert_code('WORKSPACE_INACTIVE', self.gate, workspace=workspace(status='DISABLED'))

    def test_workspace_revision_scope_and_exhaustion(self):
        for value in (True, 7.0, '7', 0, MAX_REVISION+1):
            self.assert_code('INVALID_WORKSPACE_REVISION', self.gate, expected_workspace_revision=value)
        self.assert_code('STALE_WORKSPACE_REVISION', self.gate, expected_workspace_revision=6)
        self.assert_code('WORKSPACE_SCOPE_MISMATCH', self.gate, workspace=workspace(workspace_id='other'))
        self.assert_code('WORKSPACE_REVISION_EXHAUSTED', self.gate,
                         workspace=workspace(revision=MAX_REVISION), expected_workspace_revision=MAX_REVISION)

    def test_foreign_or_borrowed_actor_snapshot_cannot_authorize(self):
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.gate, actor_membership=admin(workspace_id='other'))
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.gate, actor_membership=membership())
        self.assert_code('INVALID_MEMBERSHIP_ACTOR', self.gate, actor=ADMIN.principal_id)

    def test_future_authorization_snapshots_are_rejected(self):
        future = LATER+timedelta(seconds=1)
        self.assert_code('INVALID_MEMBERSHIP_OBSERVATION_TIME', self.gate,
                         workspace=workspace(updated_at=future))
        self.assert_code('INVALID_MEMBERSHIP_OBSERVATION_TIME', self.gate,
                         actor_membership=admin(updated_at=future))

    def test_self_revoke_uses_same_pre_mutation_row_and_then_denies(self):
        before = admin()
        proposed = before | dict(revision=6, updated_at=LATER, status='REVOKED')
        decision = self.gate(proposed, previous=before, principal_id=ADMIN.principal_id, expected_revision=5)
        self.assertEqual(decision.membership_revision, 5)
        self.assert_code('INCONSISTENT_ACTOR_MEMBERSHIP', self.gate, proposed,
                         previous=before | dict(revision=4), principal_id=ADMIN.principal_id, expected_revision=4)
        after = validate_membership(proposed, workspace_id=WORKSPACE, principal_id=ADMIN.principal_id)
        self.assertFalse(authorize(ADMIN, after, Action.MANAGE_MEMBERS, workspace_id=WORKSPACE).allowed)

    def test_existing_admin_cannot_be_recreated_as_absent(self):
        proposed = admin() | dict(revision=1, created_at=LATER, updated_at=LATER)
        self.assert_code('INCONSISTENT_ACTOR_MEMBERSHIP', self.gate, proposed,
                         previous=None, expected_revision=None, principal_id=ADMIN.principal_id)

    def test_reactivated_target_needs_admin_and_restores_only_assigned_roles(self):
        row = self.propose(membership(status='REVOKED'), roles=frozenset({Role.VIEWER}))
        member = self.validate(row)
        self.assertEqual(member.status, MembershipStatus.ACTIVE)
        self.assertFalse(authorize(TARGET, member, Action.GENERATE_DRAFT, workspace_id=WORKSPACE).allowed)
        self.assert_denied(Reason.ROLE_REQUIRED, previous=membership(status='REVOKED'),
                           actor_membership=admin() | dict(roles=['CREATOR']))

    def test_proposal_roles_and_status_require_typed_inputs(self):
        for roles in (['VIEWER'], {'VIEWER'}, frozenset({'VIEWER'})):
            self.assert_code('INVALID_MEMBERSHIP_ROLES', self.propose, roles=roles)
        self.assert_code('INVALID_MEMBERSHIP_STATUS', self.propose, status='ACTIVE')

    def test_proposal_is_deterministic_sorted_independent_and_preserves_inputs(self):
        previous, actor_row, control = membership(), admin(), workspace()
        original = deepcopy((previous, actor_row, control))
        kwargs = dict(roles=frozenset({Role.REVIEWER, Role.ADMIN, Role.VIEWER}),
                      actor_membership=actor_row, workspace=control)
        first, second = self.propose(previous, **kwargs), self.propose(previous, **kwargs)
        self.assertEqual(first, second)
        self.assertEqual(first['roles'], ['ADMIN', 'REVIEWER', 'VIEWER'])
        self.assertIsNot(first, previous)
        first['roles'].clear()
        self.assertEqual((previous, actor_row, control), original)

    def test_failed_proposal_and_validations_do_not_mutate_inputs(self):
        before, after = membership(), candidate()
        original = deepcopy((before, after))
        self.change(before, after)
        self.assertEqual((before, after), original)
        self.assert_code('STALE_MEMBERSHIP_REVISION', self.propose, before, expected_revision=1)
        self.assertEqual((before, after), original)

    def test_no_authentication_cloud_calls_or_credentials(self):
        with patch('google.cloud.bigquery.Client') as client, patch('google.auth.default') as adc, \
                patch('google.oauth2.id_token.verify_token') as verifier:
            self.validate(membership())
            self.create()
            self.change()
            self.gate()
            self.propose()
            client.assert_not_called()
            adc.assert_not_called()
            verifier.assert_not_called()

    def test_safe_errors_do_not_echo_subjects_or_roles(self):
        secret = 'private subject content'
        with self.assertRaises(ContractError) as caught:
            self.validate(membership(identity_subject=secret))
        self.assertNotIn(secret, str(caught.exception))


if __name__ == '__main__':
    unittest.main()
