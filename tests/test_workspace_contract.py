"""Offline control-row cases; fixtures are never bootstrapped into BigQuery."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import patch

from src.application.workflow_contract import ContractError
from src.application.workflow_schema import SCHEMA_VERSION
from src.application.workspace_contract import (
    MAX_REVISION, WorkspaceChange, propose_workspace_fence, validate_workspace,
    validate_workspace_change, validate_workspace_creation,
)

WORKSPACE = 'fixture-workspace'
TIME = datetime(2026, 10, 10, tzinfo=timezone.utc)
LATER = TIME + timedelta(seconds=1)


def workspace(**changes):
    return dict(schema_version=SCHEMA_VERSION, workspace_id=WORKSPACE,
                status='ACTIVE', revision=1, created_at=TIME, updated_at=TIME) | changes


class WorkspaceContractTests(unittest.TestCase):
    def validate(self, row):
        validate_workspace(row, workspace_id=WORKSPACE)

    def create(self, row, **changes):
        validate_workspace_creation(row, **(dict(workspace_id=WORKSPACE, observed_at=TIME) | changes))

    def fence(self, row=None, **changes):
        return propose_workspace_fence(workspace() if row is None else row,
            **(dict(workspace_id=WORKSPACE, expected_revision=1, observed_at=LATER) | changes))

    def change(self, previous=None, candidate=None, **changes):
        validate_workspace_change(workspace() if previous is None else previous,
            workspace(revision=2, updated_at=LATER) if candidate is None else candidate,
            **(dict(workspace_id=WORKSPACE, expected_revision=1, observed_at=LATER,
                    change=WorkspaceChange.FENCE) | changes))

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_both_stored_statuses_and_revision_boundary(self):
        for status in ('ACTIVE', 'DISABLED'):
            for revision in (1, MAX_REVISION):
                with self.subTest(status=status, revision=revision):
                    self.validate(workspace(status=status, revision=revision))

    def test_exact_schema_rejects_missing_or_extra_columns(self):
        for field in workspace():
            row = workspace()
            del row[field]
            with self.subTest(field=field):
                self.assert_code('INVALID_ROW_SHAPE', self.validate, row)
        self.assert_code('INVALID_ROW_SHAPE', self.validate, workspace(roles=['ADMIN']))

    def test_non_rows_and_nullable_required_fields(self):
        for row in (None, [], 'record'):
            self.assert_code('INVALID_ROW_SHAPE', self.validate, row)
        for field in workspace():
            with self.subTest(field=field):
                self.assert_code('MISSING_REQUIRED_FIELD', self.validate, workspace(**{field: None}))

    def test_version_and_scope(self):
        self.assert_code('INVALID_SCHEMA_VERSION', self.validate, workspace(schema_version='old'))
        self.assert_code('WORKSPACE_SCOPE_MISMATCH', self.validate, workspace(workspace_id='other'))

    def test_invalid_workspace_identifier(self):
        for value in ('', 'has space', 'bad\nidentifier', 'x'*129, 123):
            with self.subTest(value=value):
                self.assert_code('INVALID_IDENTIFIER', validate_workspace,
                                 workspace(workspace_id=value), workspace_id=value)

    def test_unknown_status_is_not_inactive_success(self):
        for value in ('ARCHIVED', 'SUSPENDED', 'active', 'UNKNOWN'):
            self.assert_code('INVALID_WORKSPACE_STATUS', self.validate, workspace(status=value))

    def test_revision_is_positive_int64_not_bool(self):
        for value in (0, -1):
            self.assert_code('INVALID_WORKSPACE_REVISION', self.validate, workspace(revision=value))
        for value in (True, 1.0, '1', MAX_REVISION + 1):
            self.assert_code('INVALID_FIELD_TYPE', self.validate, workspace(revision=value))

    def test_aware_and_ordered_timestamps(self):
        for field in ('created_at', 'updated_at'):
            self.assert_code('INVALID_TIMESTAMP', self.validate,
                             workspace(**{field: TIME.replace(tzinfo=None)}))
        self.assert_code('INVALID_WORKSPACE_TIMELINE', self.validate, workspace(updated_at=TIME-timedelta(seconds=1)))

    def test_out_of_range_utc_timestamp(self):
        instant = datetime.min.replace(tzinfo=timezone(timedelta(hours=1)))
        self.assert_code('INVALID_TIMESTAMP', self.validate, workspace(created_at=instant, updated_at=instant))

    def test_equivalent_timezone_instants(self):
        india = timezone(timedelta(hours=5, minutes=30))
        row = workspace(created_at=TIME.astimezone(india), updated_at=TIME)
        self.validate(row)
        self.create(row)

    def test_initial_active_revision_one(self):
        self.create(workspace())

    def test_creation_rejects_existing_revisions(self):
        self.assert_code('INVALID_INITIAL_WORKSPACE_REVISION', self.create, workspace(revision=2))

    def test_creation_rejects_disabled(self):
        self.assert_code('INVALID_INITIAL_WORKSPACE_STATUS', self.create, workspace(status='DISABLED'))

    def test_creation_binds_supplied_server_time(self):
        self.assert_code('INVALID_WORKSPACE_CREATION_TIME', self.create, workspace(), observed_at=LATER)
        self.assert_code('INVALID_WORKSPACE_CREATION_TIME', self.create, workspace(updated_at=LATER))
        self.assert_code('INVALID_TIMESTAMP', self.create, workspace(), observed_at=TIME.replace(tzinfo=None))

    def test_active_fence_increments_only_revision_and_updated_time(self):
        self.assertEqual(self.fence(), workspace(revision=2, updated_at=LATER))

    def test_equal_clock_timestamp_still_consumes_revision(self):
        self.assertEqual(self.fence(observed_at=TIME), workspace(revision=2))

    def test_fence_returns_independent_copy_without_mutation(self):
        previous = workspace()
        original = deepcopy(previous)
        candidate = self.fence(previous)
        self.assertEqual(previous, original)
        self.assertIsNot(candidate, previous)
        candidate['status'] = 'DISABLED'
        self.assertEqual(previous, original)

    def test_fence_rejects_disabled_workspace(self):
        self.assert_code('WORKSPACE_INACTIVE', self.fence, workspace(status='DISABLED'))

    def test_stale_revision_rejected_even_when_candidate_matches_expected(self):
        self.assert_code('STALE_WORKSPACE_REVISION', self.fence, workspace(revision=2))
        self.assert_code('STALE_WORKSPACE_REVISION', self.change,
                         previous=workspace(revision=2))

    def test_expected_revision_has_strict_int64_bounds(self):
        for value in (True, 1.0, '1', 0, -1, MAX_REVISION+1):
            with self.subTest(value=value):
                self.assert_code('INVALID_WORKSPACE_REVISION', self.fence, expected_revision=value)

    def test_exhausted_revision_cannot_wrap_or_reset(self):
        row = workspace(revision=MAX_REVISION)
        self.assert_code('WORKSPACE_REVISION_EXHAUSTED', self.fence, row, expected_revision=MAX_REVISION)
        self.assert_code('WORKSPACE_REVISION_EXHAUSTED', self.change, previous=row,
                         candidate=workspace(revision=1, updated_at=LATER), expected_revision=MAX_REVISION)

    def test_change_requires_exact_next_revision(self):
        for revision in (1, 3, 10):
            self.assert_code('INVALID_WORKSPACE_REVISION_INCREMENT', self.change,
                             candidate=workspace(revision=revision, updated_at=LATER))

    def test_change_cannot_remap_workspace_or_schema(self):
        self.assert_code('WORKSPACE_SCOPE_MISMATCH', self.change,
                         candidate=workspace(workspace_id='other', revision=2, updated_at=LATER))
        self.assert_code('INVALID_SCHEMA_VERSION', self.change,
                         candidate=workspace(schema_version='old', revision=2, updated_at=LATER))

    def test_creation_timestamp_is_immutable(self):
        self.assert_code('IMMUTABLE_WORKSPACE_CREATION', self.change,
                         candidate=workspace(created_at=LATER, updated_at=LATER, revision=2))

    def test_candidate_time_must_match_observation(self):
        for instant in (TIME, LATER + timedelta(seconds=1)):
            self.assert_code('INVALID_WORKSPACE_CHANGE_TIME', self.change,
                             candidate=workspace(revision=2, updated_at=instant))

    def test_clock_cannot_move_backwards(self):
        self.assert_code('INVALID_WORKSPACE_CHANGE_TIME', self.change,
            previous=workspace(updated_at=LATER), candidate=workspace(revision=2), observed_at=TIME)

    def test_fence_does_not_hide_status_change(self):
        self.assert_code('FENCE_CHANGED_WORKSPACE_STATUS', self.change,
                         candidate=workspace(status='DISABLED', revision=2, updated_at=LATER))

    def test_disable_and_reactivate_are_explicit_status_proposals(self):
        self.change(candidate=workspace(status='DISABLED', revision=2, updated_at=LATER),
                    change=WorkspaceChange.SET_STATUS)
        self.change(previous=workspace(status='DISABLED'), change=WorkspaceChange.SET_STATUS)

    def test_status_change_cannot_be_revision_only(self):
        for status in ('ACTIVE', 'DISABLED'):
            self.assert_code('WORKSPACE_STATUS_UNCHANGED', self.change,
                previous=workspace(status=status),
                candidate=workspace(status=status, revision=2, updated_at=LATER),
                change=WorkspaceChange.SET_STATUS)

    def test_change_kind_requires_typed_intent(self):
        for value in ('FENCE', 'SET_STATUS', None):
            self.assert_code('INVALID_WORKSPACE_CHANGE', self.change, change=value)

    def test_invalid_proposal_leaves_previous_unchanged(self):
        previous = workspace(status='DISABLED')
        original = deepcopy(previous)
        self.assert_code('WORKSPACE_INACTIVE', self.fence, previous)
        self.assertEqual(previous, original)

    def test_change_validation_preserves_both_inputs(self):
        previous, candidate = workspace(), workspace(revision=2, updated_at=LATER)
        original = deepcopy((previous, candidate))
        self.change(previous, candidate)
        self.assertEqual((previous, candidate), original)

    def test_no_cloud_calls_or_credentials(self):
        with patch('google.cloud.bigquery.Client') as client, patch('google.auth.default') as adc:
            self.validate(workspace())
            self.create(workspace())
            self.fence()
            self.change()
            client.assert_not_called()
            adc.assert_not_called()

    def test_errors_do_not_echo_row_content(self):
        secret = 'private row content'
        with self.assertRaises(ContractError) as caught:
            self.validate(workspace(status=secret))
        self.assertNotIn(secret, str(caught.exception))


if __name__ == '__main__':
    unittest.main()
