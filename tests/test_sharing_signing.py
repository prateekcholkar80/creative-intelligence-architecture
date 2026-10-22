"""Offline retention/rotation/handler checks with injected fixture material."""
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta, timezone
import json
import unittest
from unittest.mock import patch

from src.application.access_reader import BigQueryAccessReader
from src.application.sharing_executor import ProtectedSharingExecutor, SharingExecutorSettings
from src.application.sharing_intent import SharingIntentSealer, initial_attempt
from src.application.sharing_query import SharingQuerySettings
from src.application.sharing_retained_sealer import RetainedSharingSealer
from src.application.sharing_retry import SharingRetrySealer
from src.application.sharing_signing_contract import (
    HANDLER_VERSION, SharingSealProfile, SharingSigningKey, SharingSigningPolicy,
    execution_profile_fingerprint, validate_signing_policy_transition,
)
from src.application.workflow_contract import ContractError, RETRY_WINDOW, canonical_json_bytes
from tests.test_membership_contract import TIME
from tests.test_sharing_coordinator_contract import command
from tests.test_sharing_executor import EPOCH, KEY, identity
from tests.test_sharing_query import query
from tests.test_sharing_retry import encode, retry
from tests import test_sharing_retry_executor as executor_fixtures

OLD = SharingSigningKey('old', 'standard', TIME - timedelta(days=1), TIME + timedelta(seconds=5),
                        TIME + timedelta(seconds=5) + RETRY_WINDOW)
NEW = SharingSigningKey('new', 'standard', OLD.issue_until, TIME + timedelta(days=1),
                        TIME + timedelta(days=1) + RETRY_WINDOW)
MATERIAL = {'old': KEY, 'new': b'another-offline-only-key-32-bytes!'}
NOW = TIME + timedelta(seconds=10)


def profile(profile_id='standard', settings=None):
    return SharingSealProfile.from_runtime(profile_id, settings or SharingQuerySettings())


def policy(active='new', keys=(OLD, NEW), profiles=None):
    return SharingSigningPolicy(active, keys, (profile(),) if profiles is None else profiles)


class SharingSigningTests(unittest.TestCase):
    def setUp(self):
        for name in ('sharing_retained_sealer', 'sharing_intent', 'sharing_retry'):
            patcher = patch('src.application.' + name + '._now', return_value=NOW)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch('src.application.identity.time.time', return_value=EPOCH + 10)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.sealer = RetainedSharingSealer(policy=policy(), key_material=MATERIAL)
        self.legacy = SharingRetrySealer(key=KEY, key_id='old')
        self.original = initial_attempt(query())
        self.old_token = self.legacy.seal(query())

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_original_token_resolves_after_rotation_without_identity_change(self):
        self.assertEqual(self.sealer.resolve(self.old_token, identity()), self.original)
        self.assertEqual(self.sealer.key_id, 'new')

    def test_new_initial_token_uses_active_key_and_is_legacy_compatible(self):
        q = query(command=command(accepted_at=NOW))
        token = self.sealer.seal(q)
        legacy = SharingIntentSealer(key=MATERIAL['new'], key_id='new')
        self.assertEqual(token, legacy.seal(q))
        self.assertEqual(legacy.resolve(token, identity()), initial_attempt(q))

    def test_retired_key_cannot_issue_initial_intents(self):
        retired = RetainedSharingSealer(policy=policy('old'), key_material=MATERIAL)
        self.assert_code('SHARING_SIGNING_KEY_NOT_ISSUING', retired.seal, query())

    def test_active_key_cannot_reseal_old_acceptance_cohort(self):
        self.assert_code('SHARING_SIGNING_ACCEPTANCE_MISMATCH', self.sealer.seal, query())

    def test_old_retry_retains_both_original_key_layers_and_job_id(self):
        attempt = retry(self.original)
        token = self.sealer.seal_retry(attempt)
        self.assertEqual(token, self.legacy.seal_retry(attempt))
        self.assertEqual(self.sealer.resolve_retry(token, identity()), attempt)
        for _ in range(2):
            attempt = retry(attempt, expected_workspace_revision=attempt.query.expected_workspace_revision + 1)
            self.assertEqual(self.sealer.resolve_retry(self.sealer.seal_retry(attempt), identity()), attempt)
        self.assertEqual(attempt.query.command.deadline_at, TIME + RETRY_WINDOW)

    def test_replica_and_restart_preserve_original_and_retry_results(self):
        restarted = RetainedSharingSealer(policy=policy(), key_material=dict(MATERIAL))
        attempt = retry(self.original)
        self.assertEqual(restarted.resolve(self.old_token, identity()), self.original)
        self.assertEqual(restarted.seal_retry(attempt), self.sealer.seal_retry(attempt))
        self.assertEqual(restarted.resolve_retry(self.sealer.seal_retry(attempt), identity()), attempt)

    def test_original_deadline_expires_before_retained_key(self):
        token = self.sealer.seal_retry(retry(self.original))
        with patch('src.application.sharing_retained_sealer._now', return_value=TIME + RETRY_WINDOW):
            self.assert_code('SHARING_RETRY_EXPIRED', self.sealer.resolve, self.old_token, identity())
            self.assert_code('SHARING_RETRY_EXPIRED', self.sealer.resolve_retry, token, identity())
            self.assert_code('SHARING_RETRY_EXPIRED', self.sealer.seal_retry, retry(self.original))

    def test_key_retention_boundary_rejects_even_valid_signature(self):
        with patch('src.application.sharing_retained_sealer._now', return_value=OLD.retain_until):
            self.assert_code('SHARING_SIGNING_KEY_EXPIRED', self.sealer.resolve, self.old_token, identity())

    def test_revoked_key_blocks_initial_retry_resolution_and_retry_sealing(self):
        revoked = RetainedSharingSealer(policy=policy(keys=(replace(OLD, revoked=True), NEW)),
                                       key_material={'new': MATERIAL['new']})
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', revoked.resolve, self.old_token, identity())
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', revoked.resolve_retry,
                         self.legacy.seal_retry(retry(self.original)), identity())
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', revoked.seal_retry, retry(self.original))

    def test_revoked_active_key_configuration_fails_closed(self):
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', policy, keys=(OLD, replace(NEW, revoked=True)))

    def test_unknown_key_hint_does_not_try_another_key(self):
        unknown = SharingRetrySealer(key=KEY, key_id='unregistered-key')
        self.assert_code('SHARING_SIGNING_KEY_UNAVAILABLE', self.sealer.resolve,
                         unknown.seal(query()), identity())

    def test_wrong_bytes_same_id_fail_signature_and_do_not_fallback(self):
        wrong = dict(MATERIAL, old=b'x' * 32)
        sealer = RetainedSharingSealer(policy=policy(), key_material=wrong)
        self.assert_code('INVALID_SHARING_SEAL', sealer.resolve, self.old_token, identity())

    def test_missing_extra_mutable_or_reused_key_material_is_rejected(self):
        for material in ({'new': MATERIAL['new']}, dict(MATERIAL, extra=b'x' * 32)):
            self.assert_code('SHARING_SIGNING_MATERIAL_UNAVAILABLE', RetainedSharingSealer,
                             policy=policy(), key_material=material)
        for value in (b'x' * 31, b'x' * 65, bytearray(b'x' * 32), 'x' * 32):
            self.assert_code('INVALID_SHARING_SIGNING_KEY', RetainedSharingSealer,
                             policy=policy(), key_material=dict(MATERIAL, old=value))
        self.assert_code('SHARING_SIGNING_MATERIAL_REUSED', RetainedSharingSealer,
                         policy=policy(), key_material={'old': KEY, 'new': KEY})

    def test_material_mapping_is_copied_and_reprs_do_not_expose_bytes(self):
        material = dict(MATERIAL)
        sealer = RetainedSharingSealer(policy=policy(), key_material=material)
        material['old'] = b'x' * 32
        self.assertEqual(sealer.resolve(self.old_token, identity()), self.original)
        self.assertNotIn(KEY.decode(), repr(sealer))
        self.assertNotIn(KEY.decode(), repr(sealer.policy))

    def test_immutable_policy_and_profiles(self):
        for obj, name, value in ((OLD, 'key_id', 'changed'), (policy(), 'active_key_id', 'old'),
                                 (profile(), 'settings', SharingQuerySettings(maximum_bytes_billed=10))):
            with self.assertRaises(FrozenInstanceError):
                setattr(obj, name, value)

    def test_rotation_allows_retained_settings_and_new_stage_settings(self):
        limited = SharingQuerySettings(maximum_bytes_billed=1024, job_timeout_ms=5000,
                                       statement_timeout_ms=1000)
        changed = policy(keys=(OLD, replace(NEW, profile_id='limited')),
                         profiles=(profile(), profile('limited', limited)))
        sealer = RetainedSharingSealer(policy=changed, key_material=MATERIAL)
        self.assertEqual(sealer.settings, limited)
        self.assertEqual(sealer.resolve(self.old_token, identity()), self.original)
        self.assertEqual(sealer.resolve_retry(sealer.seal_retry(retry(self.original)), identity()),
                         retry(self.original))
        q = query(command=command(accepted_at=NOW), settings=limited)
        self.assertEqual(sealer.resolve(sealer.seal(q), identity()), initial_attempt(q))
        self.assert_code('SHARING_SETTINGS_MISMATCH', sealer.seal,
                         query(command=command(accepted_at=NOW)))

    def test_old_profile_cannot_be_silently_reconstructed_with_new_settings(self):
        sealer = RetainedSharingSealer(policy=policy(profiles=(profile(settings=SharingQuerySettings(
            maximum_bytes_billed=1024)),)), key_material=MATERIAL)
        self.assert_code('SHARING_ATTEMPT_MISMATCH', sealer.resolve, self.old_token, identity())

    def test_installed_sql_drift_fails_at_startup_and_on_existing_instance(self):
        with patch('src.application.sharing_signing_contract._render_sql', return_value='changed SQL'):
            self.assert_code('SHARING_SEAL_PROFILE_UNAVAILABLE', RetainedSharingSealer,
                             policy=self.sealer.policy, key_material=MATERIAL)
            self.assert_code('SHARING_SEAL_PROFILE_UNAVAILABLE', self.sealer.resolve,
                             self.old_token, identity())
            self.assert_code('SHARING_SEAL_PROFILE_UNAVAILABLE', self.sealer.seal_retry, retry(self.original))

    def test_unsupported_handler_and_policy_version_fail_closed(self):
        self.assert_code('UNSUPPORTED_SHARING_SEAL_HANDLER', replace, profile(), handler_version='future-v2')
        self.assert_code('UNSUPPORTED_SHARING_SIGNING_POLICY', replace, policy(), version='future-v2')
        self.assertEqual(profile().handler_version, HANDLER_VERSION)

    def test_profile_fingerprint_changes_with_settings(self):
        self.assertNotEqual(execution_profile_fingerprint(SharingQuerySettings()),
                            execution_profile_fingerprint(SharingQuerySettings(maximum_bytes_billed=1024)))

    def test_key_schedule_requires_aware_times_and_full_original_retention(self):
        self.assert_code('INVALID_SHARING_KEY_RETENTION', replace, OLD,
                         retain_until=OLD.retain_until - timedelta(microseconds=1))
        self.assert_code('INVALID_SHARING_KEY_RETENTION', replace, OLD, issue_from=OLD.issue_until)
        with self.assertRaises(ContractError):
            replace(OLD, issue_from=TIME.replace(tzinfo=None))
        shifted = replace(OLD, issue_from=OLD.issue_from.astimezone(timezone(timedelta(hours=5))))
        self.assertEqual(shifted, OLD)

    def test_key_windows_are_half_open_with_no_ambiguity(self):
        boundary = OLD.issue_until
        with patch('src.application.sharing_retained_sealer._now', return_value=boundary):
            q = query(command=command(accepted_at=boundary))
            token = self.sealer.seal(q)
            self.assertEqual(self.sealer.resolve(token, identity()), initial_attempt(q))
        self.assert_code('OVERLAPPING_SHARING_KEY_WINDOWS', policy,
                         keys=(OLD, replace(NEW, issue_from=TIME)))

    def test_duplicate_unbounded_unknown_profiles_and_keys_reject(self):
        self.assert_code('INVALID_SHARING_SIGNING_KEYS', policy, keys=(OLD, OLD))
        self.assert_code('INVALID_SHARING_SIGNING_KEYS', policy, keys=[OLD, NEW])
        self.assert_code('INVALID_SHARING_SIGNING_KEYS', policy, keys=(OLD,) * 9)
        self.assert_code('INVALID_SHARING_SEAL_PROFILES', policy, profiles=(profile(), profile()))
        self.assert_code('SHARING_SEAL_PROFILE_UNAVAILABLE', policy,
                         keys=(OLD, replace(NEW, profile_id='missing')))
        self.assert_code('SHARING_SIGNING_KEY_UNAVAILABLE', policy, active='missing')

    def test_transition_accepts_planned_rotation_and_emergency_retained_key_revocation(self):
        previous = policy('old')
        validate_signing_policy_transition(previous, policy(), changed_at=OLD.issue_until)
        validate_signing_policy_transition(policy(), policy(keys=(replace(OLD, revoked=True), NEW)),
                                           changed_at=NOW)

    def test_transition_rejects_early_removal_schedule_rebinding_and_unrevocation(self):
        self.assert_code('SHARING_SIGNING_KEY_REMOVED_EARLY', validate_signing_policy_transition,
                         policy(), policy(keys=(NEW,)), changed_at=NOW)
        for key in (replace(OLD, retain_until=OLD.retain_until + timedelta(days=1)),
                    replace(OLD, issue_from=OLD.issue_from - timedelta(seconds=1))):
            self.assert_code('SHARING_SIGNING_KEY_REBOUND', validate_signing_policy_transition,
                             policy(), policy(keys=(key, NEW)), changed_at=NOW)
        self.assert_code('SHARING_SIGNING_KEY_REBOUND', validate_signing_policy_transition,
                         policy(keys=(replace(OLD, revoked=True), NEW)), policy(), changed_at=NOW)

    def test_transition_rejects_existing_profile_rebinding_and_future_activation(self):
        changed = policy(profiles=(profile(settings=SharingQuerySettings(maximum_bytes_billed=1024)),))
        self.assert_code('SHARING_SEAL_PROFILE_REBOUND', validate_signing_policy_transition,
                         policy(), changed, changed_at=NOW)
        self.assert_code('SHARING_SIGNING_KEY_NOT_ISSUING', validate_signing_policy_transition,
                         policy('old'), policy(), changed_at=TIME)

    def test_transition_removes_key_only_after_retention_with_valid_active_key(self):
        third = SharingSigningKey('third', 'standard', NEW.issue_until, NEW.retain_until,
                                  NEW.retain_until + RETRY_WINDOW)
        next_policy = policy('third', keys=(NEW, third))
        previous = policy('third', keys=(OLD, NEW, third))
        validate_signing_policy_transition(previous, next_policy, changed_at=OLD.retain_until)

    def test_transition_cannot_add_backdated_acceptance_cohort(self):
        self.assert_code('SHARING_SIGNING_KEY_BACKDATED', validate_signing_policy_transition,
                         policy('old', keys=(OLD,)), policy(), changed_at=NOW)

    def test_key_ids_are_bounded_opaque_metadata(self):
        for key_id in ('', '_invalid-first', 'x' * 129, 'secret/latest', 'user@example.com', None):
            self.assert_code('INVALID_SHARING_SIGNING_IDENTIFIER', replace, OLD, key_id=key_id)

    def test_token_acceptance_must_belong_to_selected_key_cohort(self):
        wrong = SharingIntentSealer(key=MATERIAL['new'], key_id='new').seal(query())
        self.assert_code('SHARING_SIGNING_ACCEPTANCE_MISMATCH', self.sealer.resolve, wrong, identity())

    def test_nested_retry_cannot_mix_key_layers_even_with_valid_outer_signature(self):
        import hmac
        token = self.legacy.seal_retry(retry(self.original))
        raw = token.split('.')[1]
        import base64
        payload = json.loads(base64.urlsafe_b64decode(raw + '=' * (-len(raw) % 4)))
        payload['key_id'] = 'new'
        raw_bytes = canonical_json_bytes(payload)
        mixed = 'share-retry1.' + encode(raw_bytes) + '.' + encode(hmac.digest(
            MATERIAL['new'], b'ci-sharing-retry-v1\0' + raw_bytes, 'sha256'))
        self.assert_code('INVALID_SHARING_SEAL', self.sealer.resolve_retry, mixed, identity())

    def test_identity_tamper_malformed_tokens_do_not_return_attempts(self):
        from tests.test_membership_contract import ADMIN, ADMIN_SUBJECT
        self.assert_code('SHARING_IDENTITY_MISMATCH', self.sealer.resolve, self.old_token,
                         identity(ADMIN, ADMIN_SUBJECT))
        for token in (None, '', 'share1.a.' + 'x' * 43, self.old_token + 'x', 'x' * 8193):
            with self.assertRaises(ContractError):
                self.sealer.resolve(token, identity())
        parts = self.old_token.split('.')
        parts[-1] = ('a' if parts[-1][0] != 'a' else 'b') + parts[-1][1:]
        self.assert_code('INVALID_SHARING_SEAL', self.sealer.resolve, '.'.join(parts), identity())

    def test_future_original_and_unallocated_acceptance_cohort_cannot_be_sealed(self):
        self.assert_code('SHARING_INTENT_NOT_YET_VALID', self.sealer.seal,
                         query(command=command(accepted_at=NOW + timedelta(seconds=1))))
        older = initial_attempt(query(command=command(accepted_at=OLD.issue_from - timedelta(seconds=1))))
        self.assert_code('SHARING_SIGNING_KEY_UNAVAILABLE', self.sealer.seal_retry, retry(older))


class RetainedSharingExecutorTests(unittest.TestCase):
    """Reuse only fixture setup, not the unrelated legacy test collection."""
    def setUp(self):
        self.fixture = executor_fixtures.SharingRetryExecutorTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        patcher = patch('src.application.sharing_retained_sealer._now', return_value=NOW)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.fixture.sealer = RetainedSharingSealer(policy=policy(), key_material=MATERIAL)
        self.fixture.executor = ProtectedSharingExecutor(client=self.fixture.client, verifier=self.fixture.verifier,
            reader=BigQueryAccessReader(self.fixture.client), sealer=self.fixture.sealer,
            settings=SharingExecutorSettings(execution_enabled=True))
        self.fixture.original_token = SharingRetrySealer(key=KEY, key_id='old').seal(query())

    def test_existing_executor_issues_and_recovers_retry_under_retired_key(self):
        fixture = self.fixture
        fixture.fresh_fence(8)
        issuance = fixture.issue()
        attempt = fixture.sealer.resolve_retry(issuance.token, identity())
        self.assertEqual(attempt.original, fixture.original)
        fixture.failed(attempt)
        outcome = fixture.executor.recover_retry(fixture.headers, issuance.token)
        self.assertEqual(outcome.observation.query_job_id, attempt.job_id)
        self.assertEqual(fixture.client.submissions, [])

    def test_retry_dispatch_preserves_old_settings_when_active_profile_changes(self):
        fixture = self.fixture
        limited = SharingQuerySettings(maximum_bytes_billed=1024)
        fixture.executor.sealer = RetainedSharingSealer(
            policy=policy(keys=(OLD, replace(NEW, profile_id='limited')),
                          profiles=(profile(), profile('limited', limited))), key_material=MATERIAL)
        fixture.fresh_fence(9)
        issued = fixture.issue()
        attempt = fixture.executor.sealer.resolve_retry(issued.token, identity())
        fixture.client.insert_rows = [fixture.applied_row(attempt)]
        outcome = fixture.executor.submit_retry(fixture.headers, issued.token)
        self.assertEqual(outcome.observation.query_job_id, attempt.job_id)
        self.assertEqual(len(fixture.client.submissions), 1)
        config = fixture.client.submissions[0][1]['job_config']
        self.assertEqual(config.maximum_bytes_billed, self.fixture.original.query.settings.maximum_bytes_billed)
        self.assertNotEqual(config.maximum_bytes_billed, limited.maximum_bytes_billed)

    def test_executor_new_intent_selects_active_key_with_server_acceptance(self):
        fixture = self.fixture
        q = fixture.original.query
        token = fixture.executor.issue_intent(fixture.headers,
            workspace_id=q.command.workspace_id, conversation_id=q.command.conversation_id,
            visibility=q.command.visibility, expected_membership_revision=q.command.expected_membership_revision,
            expected_conversation_revision=q.command.expected_conversation_revision,
            expected_workspace_revision=q.expected_workspace_revision, retry_key='new-fixture-intent')
        attempt = fixture.sealer.resolve(token, identity())
        self.assertEqual(attempt.query.command.accepted_at, NOW)
        self.assertEqual(SharingRetrySealer(key=MATERIAL['new'], key_id='new').resolve(token, identity()), attempt)
        self.assertEqual(fixture.client.submissions, [])

    def test_revoked_key_rejects_executor_call_before_bigquery_io(self):
        fixture = self.fixture
        fixture.executor.sealer = RetainedSharingSealer(
            policy=policy(keys=(replace(OLD, revoked=True), NEW)), key_material={'new': MATERIAL['new']})
        with self.assertRaises(ContractError) as caught:
            fixture.executor.recover(fixture.headers, fixture.original_token)
        self.assertEqual(caught.exception.code, 'SHARING_SIGNING_KEY_REVOKED')
        self.assertEqual(fixture.client.lookups, [])
        self.assertEqual(fixture.client.reads, [])
        self.assertEqual(fixture.client.submissions, [])

    def test_default_executor_remains_disabled_with_retained_sealer(self):
        from src.application.sharing_executor import SharingExecutorError
        fixture = self.fixture
        executor = ProtectedSharingExecutor(client=fixture.client, verifier=fixture.verifier,
            reader=BigQueryAccessReader(fixture.client), sealer=fixture.sealer)
        with self.assertRaises(SharingExecutorError) as caught:
            executor.recover(fixture.headers, fixture.original_token)
        self.assertEqual(caught.exception.code, 'SHARING_EXECUTION_DISABLED')
        self.assertEqual(fixture.client.lookups, [])


if __name__ == '__main__':
    unittest.main()
