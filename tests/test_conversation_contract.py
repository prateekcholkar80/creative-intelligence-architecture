"""Offline conversation proposals; no fixtures are published or authenticated."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from src.application.access_policy import Action, AuthorizationError, Reason, Role, Visibility
from src.application.audit_contract import (
    AuditTarget, MutationKind, MutationObservation, MutationOutcome, ResourceType, validate_audit_event,
)
from src.application.conversation_contract import (
    ConversationChange, propose_conversation_creation, propose_conversation_visibility,
    validate_conversation, validate_conversation_change, validate_conversation_creation,
    validate_creator_conversation_mutation,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import SCHEMA_VERSION
from src.application.workspace_contract import MAX_REVISION
from tests.test_audit_contract import audit
from tests.test_membership_contract import ADMIN, TARGET, TIME, WORKSPACE, admin, membership, workspace

CONVERSATION = 'fixture-conversation'
LATER = TIME + timedelta(seconds=1)


def conversation(**changes):
    return dict(schema_version=SCHEMA_VERSION, workspace_id=WORKSPACE, conversation_id=CONVERSATION,
                owner_id=TARGET.principal_id, visibility='PRIVATE', status='ACTIVE', access_revision=3,
                created_at=TIME, updated_at=TIME) | changes


def candidate(**changes):
    return conversation(visibility='TEAM', access_revision=4, updated_at=LATER) | changes


class ConversationContractTests(unittest.TestCase):
    def validate(self, row, **changes):
        return validate_conversation(row, **(dict(workspace_id=WORKSPACE, conversation_id=CONVERSATION) | changes))

    def create(self, row=None, **changes):
        return validate_conversation_creation(conversation(access_revision=1) if row is None else row,
            **(dict(workspace_id=WORKSPACE, conversation_id=CONVERSATION,
                    owner_id=TARGET.principal_id, observed_at=TIME) | changes))

    def change(self, previous=None, proposed=None, **changes):
        return validate_conversation_change(conversation() if previous is None else previous,
            candidate() if proposed is None else proposed,
            **(dict(workspace_id=WORKSPACE, conversation_id=CONVERSATION, expected_revision=3,
                    observed_at=LATER, change=ConversationChange.SET_VISIBILITY) | changes))

    def gate(self, proposed=None, **changes):
        return validate_creator_conversation_mutation(candidate() if proposed is None else proposed,
            **(dict(previous=conversation(), workspace=workspace(), workspace_id=WORKSPACE,
                    conversation_id=CONVERSATION, expected_workspace_revision=7, expected_revision=3,
                    actor=TARGET, actor_membership=membership(), observed_at=LATER,
                    change=ConversationChange.SET_VISIBILITY) | changes))

    def new(self, **changes):
        return propose_conversation_creation(**(dict(workspace=workspace(), workspace_id=WORKSPACE,
            conversation_id=CONVERSATION, expected_workspace_revision=7, actor=TARGET,
            actor_membership=membership(), observed_at=LATER) | changes))

    def share(self, previous=None, **changes):
        return propose_conversation_visibility(conversation() if previous is None else previous,
            **(dict(visibility=Visibility.TEAM, workspace=workspace(), workspace_id=WORKSPACE,
                    conversation_id=CONVERSATION, expected_workspace_revision=7, expected_revision=3,
                    actor=TARGET, actor_membership=membership(), observed_at=LATER) | changes))

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_stored_status_visibility_matrix(self):
        for status in ('ACTIVE', 'ARCHIVED'):
            for visibility in ('PRIVATE', 'TEAM'):
                row = conversation(status=status, visibility=visibility)
                resource = self.validate(row)
                self.assertEqual(resource.visibility, Visibility(visibility))
                self.assertEqual(resource.access_revision, 3)

    def test_returned_access_metadata_is_immutable(self):
        resource = self.validate(conversation())
        with self.assertRaises(AttributeError):
            resource.owner_id = ADMIN.principal_id

    def test_exact_schema_no_missing_or_extra_fields(self):
        for name in conversation():
            row = conversation()
            del row[name]
            self.assert_code('INVALID_ROW_SHAPE', self.validate, row)
        self.assert_code('INVALID_ROW_SHAPE', self.validate, conversation(roles=['ADMIN']))

    def test_required_values_and_non_rows(self):
        for row in (None, [], 'record'):
            self.assert_code('INVALID_ROW_SHAPE', self.validate, row)
        for name in conversation():
            self.assert_code('MISSING_REQUIRED_FIELD', self.validate, conversation(**{name: None}))

    def test_schema_and_cross_domain_scope(self):
        self.assert_code('INVALID_SCHEMA_VERSION', self.validate, conversation(schema_version='old'))
        self.assert_code('CONVERSATION_SCOPE_MISMATCH', self.validate, conversation(workspace_id='other'))
        self.assert_code('CONVERSATION_SCOPE_MISMATCH', self.validate, conversation(conversation_id='other'))

    def test_identifiers_and_owner_bounds(self):
        for value in ('', 'has space', 'bad\nidentifier', 'x'*129):
            self.assert_code('INVALID_IDENTIFIER', self.validate, conversation(), conversation_id=value)
        for value in ('has space', 'bad\x00owner', 'x'*257):
            self.assert_code('INVALID_IDENTIFIER', self.validate, conversation(owner_id=value))

    def test_unknown_status_and_public_visibility_are_rejected(self):
        self.assert_code('INVALID_CONVERSATION_STATUS', self.validate, conversation(status='DELETED'))
        self.assert_code('INVALID_CONVERSATION_VISIBILITY', self.validate, conversation(visibility='PUBLIC'))

    def test_revision_is_positive_bigquery_int64(self):
        self.validate(conversation(access_revision=MAX_REVISION))
        for value in (0, -1):
            self.assert_code('INVALID_CONVERSATION_REVISION', self.validate, conversation(access_revision=value))
        for value in (True, 1.0, '1', MAX_REVISION+1):
            self.assert_code('INVALID_FIELD_TYPE', self.validate, conversation(access_revision=value))

    def test_timestamps_are_aware_ordered_and_utc_representable(self):
        for name in ('created_at', 'updated_at'):
            self.assert_code('INVALID_TIMESTAMP', self.validate,
                             conversation(**{name: TIME.replace(tzinfo=None)}))
        self.assert_code('INVALID_CONVERSATION_TIMELINE', self.validate,
                         conversation(updated_at=TIME-timedelta(seconds=1)))
        instant = datetime.min.replace(tzinfo=timezone(timedelta(hours=1)))
        self.assert_code('INVALID_TIMESTAMP', self.validate, conversation(created_at=instant, updated_at=instant))

    def test_creation_is_private_active_revision_one(self):
        self.create()
        self.assertEqual(self.new(), conversation(access_revision=1, created_at=LATER, updated_at=LATER))

    def test_creation_cannot_start_team_or_archived(self):
        self.assert_code('INVALID_INITIAL_CONVERSATION_VISIBILITY', self.create,
                         conversation(access_revision=1, visibility='TEAM'))
        self.assert_code('INVALID_INITIAL_CONVERSATION_STATUS', self.create,
                         conversation(access_revision=1, status='ARCHIVED'))

    def test_creation_binds_owner_revision_and_server_time(self):
        self.assert_code('CONVERSATION_OWNER_MISMATCH', self.create, owner_id=ADMIN.principal_id)
        self.assert_code('INVALID_INITIAL_CONVERSATION_REVISION', self.create, conversation())
        self.assert_code('INVALID_CONVERSATION_CREATION_TIME', self.create, observed_at=LATER)
        self.assert_code('INVALID_CONVERSATION_CREATION_TIME', self.create,
                         conversation(access_revision=1, updated_at=LATER))

    def test_creator_creation_cannot_spoof_an_owner(self):
        self.assert_code('CONVERSATION_OWNER_MISMATCH', self.gate,
            conversation(access_revision=1, owner_id=ADMIN.principal_id, created_at=LATER, updated_at=LATER),
            previous=None, expected_revision=None, change=None)

    def test_creation_requires_no_old_revision_or_change_kind(self):
        row = self.new()
        self.assert_code('INVALID_INITIAL_CONVERSATION_REVISION', self.gate, row,
                         previous=None, expected_revision=1, change=None)
        self.assert_code('INVALID_CONVERSATION_CHANGE', self.gate, row, previous=None,
                         expected_revision=None, change=ConversationChange.ARCHIVE)

    def test_creation_permission_is_creator_only(self):
        for roles in ([], ['VIEWER'], ['REVIEWER'], ['ADMIN']):
            with self.subTest(roles=roles), self.assertRaises(AuthorizationError) as caught:
                self.new(actor_membership=membership(roles=roles))
            self.assertEqual(caught.exception.decision.reason, Reason.ROLE_REQUIRED)

    def test_sharing_both_directions_increments_one_revision(self):
        self.change()
        self.change(conversation(visibility='TEAM'), candidate(visibility='PRIVATE'))
        self.assertEqual(self.share(), candidate())
        self.assertEqual(self.share(conversation(visibility='TEAM'), visibility=Visibility.PRIVATE),
                         candidate(visibility='PRIVATE'))

    def test_stale_revision_rejected_without_rebasing(self):
        self.assert_code('STALE_CONVERSATION_REVISION', self.change, expected_revision=2)
        self.assert_code('STALE_CONVERSATION_REVISION', self.share, expected_revision=2)

    def test_expected_revision_is_strict_int64(self):
        for value in (True, 3.0, '3', 0, -1, MAX_REVISION+1):
            self.assert_code('INVALID_CONVERSATION_REVISION', self.change, expected_revision=value)

    def test_access_revision_cannot_skip_or_reset(self):
        for value in (1, 3, 5):
            self.assert_code('INVALID_CONVERSATION_REVISION_INCREMENT', self.change,
                             proposed=candidate(access_revision=value))

    def test_revision_exhaustion_cannot_wrap(self):
        row = conversation(access_revision=MAX_REVISION)
        self.assert_code('CONVERSATION_REVISION_EXHAUSTED', self.change, row,
                         candidate(access_revision=1), expected_revision=MAX_REVISION)
        self.assert_code('CONVERSATION_REVISION_EXHAUSTED', self.share, row, expected_revision=MAX_REVISION)

    def test_ownership_and_creation_time_are_immutable(self):
        self.assert_code('IMMUTABLE_CONVERSATION_OWNER', self.change,
                         proposed=candidate(owner_id=ADMIN.principal_id))
        self.assert_code('IMMUTABLE_CONVERSATION_CREATION', self.change, proposed=candidate(created_at=LATER))

    def test_scope_cannot_be_remapped_during_changes(self):
        self.assert_code('CONVERSATION_SCOPE_MISMATCH', self.change,
                         proposed=candidate(conversation_id='other'))
        self.assert_code('CONVERSATION_SCOPE_MISMATCH', self.change,
                         proposed=candidate(workspace_id='other'))

    def test_same_clock_timestamp_still_consumes_a_revision(self):
        self.change(proposed=candidate(updated_at=TIME), observed_at=TIME)

    def test_update_time_matches_observation_and_never_moves_backwards(self):
        for instant in (TIME, LATER+timedelta(seconds=1)):
            self.assert_code('INVALID_CONVERSATION_CHANGE_TIME', self.change,
                             proposed=candidate(updated_at=instant))
        self.assert_code('INVALID_CONVERSATION_CHANGE_TIME', self.change,
                         conversation(updated_at=LATER), candidate(updated_at=TIME), observed_at=TIME)

    def test_noop_sharing_cannot_consume_a_revision(self):
        self.assert_code('CONVERSATION_VISIBILITY_UNCHANGED', self.change,
                         proposed=candidate(visibility='PRIVATE'))
        self.assert_code('CONVERSATION_VISIBILITY_UNCHANGED', self.share, visibility=Visibility.PRIVATE)

    def test_sharing_cannot_archive_as_a_side_effect(self):
        self.assert_code('SHARING_CHANGED_CONVERSATION_STATUS', self.change,
                         proposed=candidate(status='ARCHIVED'))

    def test_archive_is_active_to_archived_with_visibility_preserved(self):
        for visibility in ('PRIVATE', 'TEAM'):
            self.change(conversation(visibility=visibility), candidate(status='ARCHIVED', visibility=visibility),
                        change=ConversationChange.ARCHIVE)

    def test_archive_requires_status_change_and_cannot_reshare(self):
        self.assert_code('INVALID_CONVERSATION_ARCHIVE', self.change,
                         proposed=candidate(visibility='PRIVATE'), change=ConversationChange.ARCHIVE)
        self.assert_code('ARCHIVE_CHANGED_CONVERSATION_VISIBILITY', self.change,
                         proposed=candidate(status='ARCHIVED'), change=ConversationChange.ARCHIVE)

    def test_archived_rows_cannot_mutate_or_restore(self):
        for change in ConversationChange:
            self.assert_code('CONVERSATION_ARCHIVED', self.change,
                conversation(status='ARCHIVED'), candidate(), change=change)
        self.assert_code('CONVERSATION_ARCHIVED', self.share, conversation(status='ARCHIVED'))

    def test_change_kind_and_visibility_require_typed_intent(self):
        for value in ('SET_VISIBILITY', 'RESTORE', None):
            self.assert_code('INVALID_CONVERSATION_CHANGE', self.change, change=value)
        self.assert_code('INVALID_CONVERSATION_VISIBILITY', self.share, visibility='TEAM')

    def test_archive_does_not_borrow_sharing_or_admin_permission(self):
        for roles in (['CREATOR'], [role.value for role in Role]):
            self.assert_code('CONVERSATION_LIFECYCLE_WRITE_DISABLED', self.gate,
                candidate(status='ARCHIVED', visibility='PRIVATE'), change=ConversationChange.ARCHIVE,
                actor_membership=membership(roles=roles))

    def test_sharing_returns_pre_mutation_actor_and_resource_revisions(self):
        decision = self.gate()
        self.assertEqual(decision.action, Action.SET_VISIBILITY)
        self.assertEqual(decision.reason, Reason.ALLOWED_OWNER)
        self.assertEqual(decision.membership_revision, 3)
        self.assertEqual(decision.resource_revision, 3)
        self.assertEqual(decision.resource_id, CONVERSATION)

    def test_revoked_or_suspended_creator_cannot_create_or_share(self):
        for status in ('REVOKED', 'SUSPENDED'):
            for function in (self.new, self.share):
                with self.assertRaises(AuthorizationError) as caught:
                    function(actor_membership=membership(status=status))
                self.assertEqual(caught.exception.decision.reason, Reason.MEMBERSHIP_INACTIVE)

    def test_nonowner_cannot_share_private_or_team_even_with_all_roles(self):
        for visibility in ('PRIVATE', 'TEAM'):
            with self.assertRaises(AuthorizationError) as caught:
                self.share(conversation(visibility=visibility), visibility=Visibility('TEAM' if visibility == 'PRIVATE' else 'PRIVATE'),
                           actor=ADMIN, actor_membership=admin() | dict(roles=[r.value for r in Role]))
            self.assertEqual(caught.exception.decision.reason, Reason.OWNER_REQUIRED)

    def test_admin_and_reviewer_owner_cannot_share_without_creator(self):
        for roles in (['ADMIN'], ['REVIEWER'], ['VIEWER'], []):
            with self.assertRaises(AuthorizationError) as caught:
                self.share(actor_membership=membership(roles=roles))
            self.assertEqual(caught.exception.decision.reason, Reason.ROLE_REQUIRED)

    def test_disabled_or_stale_workspace_denies_mutation(self):
        self.assert_code('WORKSPACE_INACTIVE', self.share, workspace=workspace(status='DISABLED'))
        self.assert_code('STALE_WORKSPACE_REVISION', self.share, expected_workspace_revision=6)
        self.assert_code('WORKSPACE_REVISION_EXHAUSTED', self.share,
                         workspace=workspace(revision=MAX_REVISION), expected_workspace_revision=MAX_REVISION)
        for value in (True, 7.0, '7', 0, MAX_REVISION+1):
            self.assert_code('INVALID_WORKSPACE_REVISION', self.share, expected_workspace_revision=value)

    def test_cross_workspace_or_borrowed_membership_fails(self):
        self.assert_code('WORKSPACE_SCOPE_MISMATCH', self.share, workspace=workspace(workspace_id='other'))
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.share, actor_membership=membership(workspace_id='other'))
        self.assert_code('MEMBERSHIP_SCOPE_MISMATCH', self.share, actor_membership=admin())
        self.assert_code('INVALID_CONVERSATION_ACTOR', self.new, actor=TARGET.principal_id)

    def test_future_authorization_snapshots_cannot_be_used(self):
        future = LATER+timedelta(seconds=1)
        self.assert_code('INVALID_CONVERSATION_OBSERVATION_TIME', self.share,
                         workspace=workspace(updated_at=future))
        self.assert_code('INVALID_CONVERSATION_OBSERVATION_TIME', self.share,
                         actor_membership=membership(updated_at=future))

    def test_creation_and_sharing_decisions_fit_audit_contract(self):
        fresh = self.new()
        cases = (
            (fresh, None, None, None, MutationKind.CREATE, 1),
            (candidate(), conversation(), 3, ConversationChange.SET_VISIBILITY, MutationKind.UPDATE, 4),
        )
        for proposed, previous, revision, change, kind, new_revision in cases:
            decision = self.gate(proposed, previous=previous, expected_revision=revision, change=change)
            ctx = dict(observation=decision,
                target=AuditTarget(ResourceType.CONVERSATION, CONVERSATION, WORKSPACE, revision=revision,
                                  conversation_id=CONVERSATION, owner_id=TARGET.principal_id),
                mutation=MutationObservation(kind, MutationOutcome.APPLIED, new_revision),
                observed_at=LATER, recorded_at=LATER+timedelta(seconds=1), trace_id='fixture-trace')
            validate_audit_event(audit(ctx), **ctx)

    def test_successful_proposals_and_validation_preserve_inputs(self):
        previous, member, control = conversation(), membership(), workspace()
        original = deepcopy((previous, member, control))
        first = self.share(previous, workspace=control, actor_membership=member)
        self.assertEqual(first, self.share(previous, workspace=control, actor_membership=member))
        self.assertIsNot(first, previous)
        self.change(previous, first)
        first['owner_id'] = ADMIN.principal_id
        self.assertEqual((previous, member, control), original)

    def test_failed_proposals_preserve_inputs(self):
        previous = conversation(status='ARCHIVED')
        original = deepcopy(previous)
        self.assert_code('CONVERSATION_ARCHIVED', self.share, previous)
        self.assertEqual(previous, original)

    def test_no_cloud_or_authentication_calls(self):
        with patch('google.cloud.bigquery.Client') as client, patch('google.auth.default') as adc, \
                patch('google.oauth2.id_token.verify_token') as verifier:
            self.validate(conversation())
            self.create()
            self.change()
            self.gate()
            self.new()
            self.share()
            client.assert_not_called()
            adc.assert_not_called()
            verifier.assert_not_called()

    def test_errors_do_not_echo_private_content(self):
        secret = 'private owner content'
        with self.assertRaises(ContractError) as caught:
            self.validate(conversation(owner_id=secret))
        self.assertNotIn(secret, str(caught.exception))


if __name__ == '__main__':
    unittest.main()
