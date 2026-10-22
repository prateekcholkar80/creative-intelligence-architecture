"""Policy tests use fixture identities only; no membership records are published."""
from dataclasses import FrozenInstanceError, replace
import json
import unittest

from src.application.access_policy import (
    Action, AuthorizationError, Membership, MembershipStatus, OwnedResource,
    POLICY_VERSION, Principal, Reason, Role, Visibility, authorize,
    change_visibility, new_draft_access, require_authorized,
)

WORKSPACE = 'test-marketing'
ALICE = Principal('accounts.google.com:test-alice')
BOB = Principal('accounts.google.com:test-bob')


def member(principal=ALICE, *roles, **kwargs):
    return Membership(principal.principal_id, WORKSPACE, frozenset(roles), **kwargs)


def draft(visibility=Visibility.PRIVATE):
    return OwnedResource('test-draft', WORKSPACE, ALICE.principal_id, visibility)


class AccessPolicyTests(unittest.TestCase):
    def evaluate(self, membership, action, resource=None, principal=ALICE):
        return authorize(principal, membership, action, workspace_id=WORKSPACE, resource=resource)

    def test_role_permissions_on_owned_draft(self):
        # An independent expected matrix includes disabled actions and admin separation.
        expected = {
            Role.VIEWER: {Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT},
            Role.CREATOR: {Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT,
                           Action.GENERATE_DRAFT, Action.REFINE_DRAFT, Action.SET_VISIBILITY,Action.ARCHIVE_CONVERSATION},
            Role.REVIEWER: {Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT, Action.REVIEW_FOR_POC},
            Role.ADMIN: {Action.MANAGE_MEMBERS, Action.MANAGE_MODELS, Action.MANAGE_LIMITS,Action.MANAGE_WORKSPACE},
        }
        scoped = {Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT, Action.REFINE_DRAFT,
                  Action.SET_VISIBILITY, Action.REVIEW_FOR_POC,Action.ARCHIVE_CONVERSATION}
        for role in Role:
            for action in Action:
                with self.subTest(role=role, action=action):
                    result = self.evaluate(member(ALICE, role), action,
                                           draft() if action in scoped else None)
                    self.assertEqual(result.allowed, action in expected[role])

    def test_role_assignments_are_additive(self):
        membership = member(ALICE, Role.CREATOR, Role.REVIEWER, Role.ADMIN)
        for action, resource in ((Action.GENERATE_DRAFT, None), (Action.REVIEW_FOR_POC, draft()),
                                 (Action.MANAGE_MODELS, None)):
            self.assertTrue(self.evaluate(membership, action, resource).allowed)

    def test_all_roles_cannot_approve_claims_or_launch(self):
        membership = member(ALICE, *Role)
        for action in (Action.APPROVE_BENEFIT_CLAIMS, Action.LAUNCH_CAMPAIGN):
            result = self.evaluate(membership, action)
            self.assertFalse(result.allowed)
            self.assertEqual(result.reason, Reason.ACTION_NOT_ENABLED)

    def test_team_read_download_and_review_follow_roles(self):
        for role, actions in ((Role.VIEWER, (Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT)),
                              (Role.REVIEWER, (Action.REVIEW_FOR_POC,))):
            for action in actions:
                result = self.evaluate(member(BOB, role), action, draft(Visibility.TEAM), BOB)
                self.assertTrue(result.allowed)
                self.assertEqual(result.reason, Reason.ALLOWED_TEAM)

    def test_private_content_denies_nonowners_even_with_all_roles(self):
        for action in (Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT, Action.REVIEW_FOR_POC):
            result = self.evaluate(member(BOB, *Role), action, draft(), BOB)
            self.assertFalse(result.allowed)
            self.assertEqual(result.reason, Reason.PRIVATE_RESOURCE)

    def test_team_sharing_does_not_grant_editing_or_resharing(self):
        for visibility in Visibility:
            for action in (Action.REFINE_DRAFT, Action.SET_VISIBILITY):
                result = self.evaluate(member(BOB, *Role), action, draft(visibility), BOB)
                self.assertFalse(result.allowed)
                self.assertEqual(result.reason, Reason.OWNER_REQUIRED)

    def test_ownership_does_not_supply_missing_role(self):
        for action in (Action.REFINE_DRAFT, Action.REVIEW_FOR_POC):
            result = self.evaluate(member(ALICE, Role.VIEWER), action, draft())
            self.assertFalse(result.allowed)
            self.assertEqual(result.reason, Reason.ROLE_REQUIRED)

    def test_admin_has_no_implicit_content_or_generation_permission(self):
        for action, resource in ((Action.GENERATE_DRAFT, None), (Action.READ_DRAFT, draft())):
            result = self.evaluate(member(ALICE, Role.ADMIN), action, resource)
            self.assertFalse(result.allowed)
            self.assertEqual(result.reason, Reason.ROLE_REQUIRED)

    def test_anonymous_missing_membership_and_empty_roles_fail_closed(self):
        for principal, membership, reason in (
            (None, member(ALICE, Role.CREATOR), Reason.AUTHENTICATION_REQUIRED),
            (ALICE, None, Reason.MEMBERSHIP_REQUIRED),
            (ALICE, member(ALICE), Reason.ROLE_REQUIRED),
        ):
            result = self.evaluate(membership, Action.GENERATE_DRAFT, principal=principal)
            self.assertFalse(result.allowed)
            self.assertEqual(result.reason, reason)

    def test_membership_cannot_be_borrowed_from_another_principal(self):
        result = self.evaluate(member(BOB, *Role), Action.READ_DRAFT, draft())
        self.assertFalse(result.allowed)
        self.assertEqual(result.reason, Reason.PRINCIPAL_MISMATCH)

    def test_membership_and_resource_must_match_requested_workspace(self):
        for membership, resource in (
            (replace(member(ALICE, *Role), workspace_id='other-team'), draft(Visibility.TEAM)),
            (member(ALICE, *Role), replace(draft(Visibility.TEAM), workspace_id='other-team')),
        ):
            result = self.evaluate(membership, Action.READ_DRAFT, resource)
            self.assertFalse(result.allowed)
            self.assertEqual(result.reason, Reason.WORKSPACE_MISMATCH)

    def test_revoked_and_suspended_members_lose_own_and_team_access(self):
        for status in (MembershipStatus.REVOKED, MembershipStatus.SUSPENDED):
            for resource in (draft(), draft(Visibility.TEAM)):
                result = self.evaluate(member(ALICE, *Role, status=status), Action.READ_DRAFT, resource)
                self.assertFalse(result.allowed)
                self.assertEqual(result.reason, Reason.MEMBERSHIP_INACTIVE)
            self.assertFalse(self.evaluate(member(ALICE, *Role, status=status),
                                           Action.GENERATE_DRAFT).allowed)

    def test_changed_membership_is_reevaluated_and_revision_is_recorded(self):
        active = member(ALICE, Role.CREATOR, revision=3)
        self.assertTrue(self.evaluate(active, Action.GENERATE_DRAFT).allowed)
        revoked = replace(active, status=MembershipStatus.REVOKED, revision=4)
        result = self.evaluate(revoked, Action.GENERATE_DRAFT)
        self.assertFalse(result.allowed)
        self.assertEqual(result.membership_revision, 4)

    def test_resource_actions_require_resource_and_workspace_actions_reject_it(self):
        result = self.evaluate(member(ALICE, *Role), Action.DOWNLOAD_ARTIFACT)
        self.assertEqual(result.reason, Reason.RESOURCE_REQUIRED)
        self.assertFalse(result.allowed)
        result = self.evaluate(member(ALICE, *Role), Action.GENERATE_DRAFT, draft())
        self.assertEqual(result.reason, Reason.UNEXPECTED_RESOURCE)
        self.assertFalse(result.allowed)

    def test_new_draft_derives_owner_and_is_private(self):
        resource = new_draft_access(ALICE, member(ALICE, Role.CREATOR),
                                    workspace_id=WORKSPACE, resource_id='new-test-draft')
        self.assertEqual(resource.owner_id, ALICE.principal_id)
        self.assertEqual(resource.visibility, Visibility.PRIVATE)
        self.assertEqual(resource.access_revision, 1)
        with self.assertRaises(AuthorizationError):
            new_draft_access(ALICE, member(ALICE, Role.VIEWER),
                             workspace_id=WORKSPACE, resource_id='new-test-draft')

    def test_visibility_change_preserves_owner_and_increments_revision(self):
        original = draft()
        shared = change_visibility(ALICE, member(ALICE, Role.CREATOR), original, Visibility.TEAM)
        self.assertEqual(shared.visibility, Visibility.TEAM)
        self.assertEqual(shared.access_revision, 2)
        self.assertEqual(shared.owner_id, original.owner_id)
        self.assertEqual(shared.resource_id, original.resource_id)
        self.assertEqual(original.visibility, Visibility.PRIVATE)
        self.assertIs(change_visibility(ALICE, member(ALICE, Role.CREATOR), shared, Visibility.TEAM), shared)
        private = change_visibility(ALICE, member(ALICE, Role.CREATOR), shared, Visibility.PRIVATE)
        self.assertEqual(private.access_revision, 3)
        self.assertEqual(private.visibility, Visibility.PRIVATE)

    def test_visibility_denies_nonowner_and_revoked_owner(self):
        for principal, membership in ((BOB, member(BOB, *Role)),
                                     (ALICE, member(ALICE, *Role, status=MembershipStatus.REVOKED))):
            with self.assertRaises(AuthorizationError):
                change_visibility(principal, membership, draft(Visibility.TEAM), Visibility.PRIVATE)

    def test_guard_raises_safe_error_and_retains_internal_decision(self):
        with self.assertRaises(AuthorizationError) as caught:
            require_authorized(BOB, member(BOB, Role.VIEWER), Action.READ_DRAFT,
                               workspace_id=WORKSPACE, resource=draft())
        self.assertNotIn(ALICE.principal_id, str(caught.exception))
        self.assertNotIn('test-draft', str(caught.exception))
        self.assertEqual(caught.exception.decision.reason, Reason.PRIVATE_RESOURCE)
        allowed = require_authorized(ALICE, member(ALICE, Role.VIEWER), Action.READ_DRAFT,
                                     workspace_id=WORKSPACE, resource=draft())
        self.assertTrue(allowed.allowed)

    def test_decision_serializes_audit_correlation_without_content(self):
        result = self.evaluate(member(ALICE, Role.VIEWER, revision=8), Action.READ_DRAFT,
                               replace(draft(), access_revision=5))
        data = json.loads(json.dumps(result.to_dict()))
        self.assertEqual(data['policy_version'], POLICY_VERSION)
        self.assertEqual(data['membership_revision'], 8)
        self.assertEqual(data['resource_revision'], 5)
        self.assertEqual(data['reason'], 'ALLOWED_OWNER')
        self.assertNotIn('prompt', data)

    def test_inputs_are_immutable(self):
        for value, field, replacement in ((ALICE, 'principal_id', 'other'),
                                          (member(ALICE, Role.VIEWER), 'status', MembershipStatus.ACTIVE),
                                          (draft(), 'visibility', Visibility.TEAM)):
            with self.assertRaises(FrozenInstanceError):
                setattr(value, field, replacement)

    def test_untyped_and_invalid_policy_inputs_are_rejected(self):
        for factory in (
            lambda: Principal(''), lambda: Principal(' alice'), lambda: Principal('alice\n'),
            lambda: Principal('a'*257),
            lambda: member(ALICE, 'CREATOR'),
            lambda: Membership(ALICE.principal_id, WORKSPACE, {Role.CREATOR}),
            lambda: member(ALICE, Role.CREATOR, status='ACTIVE'),
            lambda: member(ALICE, Role.CREATOR, revision=True),
            lambda: member(ALICE, Role.CREATOR, revision=0),
            lambda: OwnedResource('draft', WORKSPACE, ALICE.principal_id, 'TEAM'),
            lambda: OwnedResource('draft', WORKSPACE, ALICE.principal_id, access_revision=-1),
            lambda: self.evaluate(member(ALICE, Role.CREATOR), 'GENERATE_DRAFT'),
            lambda: self.evaluate(member(ALICE, Role.CREATOR), Action.GENERATE_DRAFT, principal='alice'),
            lambda: self.evaluate({'role': 'ADMIN'}, Action.GENERATE_DRAFT),
            lambda: self.evaluate(member(ALICE, Role.CREATOR), Action.READ_DRAFT, {'owner': 'alice'}),
            lambda: change_visibility(ALICE, member(ALICE, Role.CREATOR), draft(), 'TEAM'),
        ):
            with self.subTest(factory=factory), self.assertRaises(ValueError):
                factory()
