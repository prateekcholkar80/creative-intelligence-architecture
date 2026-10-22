"""Offline publication/refresh proposals; no authority, secret loading or swap."""
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import unittest

from src.application.sharing_publication_contract import (
    MAX_KEY_HISTORY, SigningKeyHistory, SigningPublication, SigningPublicationStatus,
    initial_publication, propose_publication, secret_config_digest, validate_publication_transition,
)
from src.application.sharing_refresh_contract import (
    MAX_REFRESH_CHAIN, SigningLoadTicket, SigningReadTicket, SigningRefreshSettings,
    SigningReplicaPhase, SigningReplicaState, begin_refresh, complete_refresh,
    fail_refresh, observe_publications, require_signing_ready,
)
from src.application.sharing_secret_loader import SharingSecretVersion
from src.application.sharing_signing_contract import SharingSigningKey
from src.application.workflow_contract import ContractError, RETRY_WINDOW
from src.application.workspace_contract import MAX_REVISION
from tests import test_sharing_secret_loader as secrets
from tests import test_sharing_signing as fixtures

ACTIVE, STOPPED = SigningPublicationStatus.ACTIVE, SigningPublicationStatus.STOPPED
NOW = fixtures.NOW


def initial(config=None, **changes):
    return initial_publication(secrets.config() if config is None else config,
                               **(dict(published_at=NOW) | changes))


def next_publication(parent, **changes):
    return propose_publication(parent, **(dict(config=parent.config, status=ACTIVE,
        published_at=parent.published_at + timedelta(seconds=1)) | changes))


def scheduled_config():
    third = SharingSigningKey('third', 'standard', fixtures.NEW.issue_until,
        fixtures.NEW.issue_until + timedelta(days=10), fixtures.NEW.issue_until + timedelta(days=17))
    versions = secrets.VERSIONS + (SharingSecretVersion('third', 'sharing-intents', 3),)
    return secrets.config(policy=fixtures.policy(keys=(fixtures.OLD, fixtures.NEW, third)), versions=versions), third


def retired_history():
    cfg, third = scheduled_config()
    parent = initial(cfg)
    rotated = next_publication(parent, config=replace(cfg, policy=replace(cfg.policy, active_key_id='third')),
                               published_at=third.issue_from)
    current = replace(rotated.config, policy=replace(rotated.config.policy, keys=(fixtures.NEW, third)),
                      versions=cfg.versions[1:])
    retired = next_publication(rotated, config=current, published_at=fixtures.OLD.retain_until)
    return retired, third


class PublicationTests(unittest.TestCase):
    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_initial_publication_is_deterministic_and_has_no_parent(self):
        first = initial()
        self.assertEqual(first, initial())
        self.assertEqual(first.revision, 1)
        self.assertIsNone(first.parent_digest)
        self.assertEqual(len(first.digest), 64)
        self.assertNotIn(fixtures.KEY.decode(), repr(first))

    def test_config_digest_uses_canonical_order_and_binding_metadata(self):
        cfg = secrets.config()
        reordered = replace(cfg, policy=replace(cfg.policy, keys=tuple(reversed(cfg.policy.keys))),
                            versions=tuple(reversed(cfg.versions)))
        self.assertEqual(secret_config_digest(cfg), secret_config_digest(reordered))
        self.assertEqual(initial(cfg).digest, initial(reordered).digest)
        changed = replace(cfg, versions=(replace(cfg.versions[0], version=3), cfg.versions[1]))
        self.assertNotEqual(secret_config_digest(cfg), secret_config_digest(changed))

    def test_next_revision_pins_parent_and_complete_history(self):
        first = initial()
        second = next_publication(first)
        validate_publication_transition(first, second)
        self.assertEqual((second.revision, second.parent_digest), (2, first.digest))
        self.assertEqual(second.history, first.history)
        self.assertNotEqual(first.digest, second.digest)

    def test_skipped_revision_wrong_parent_and_changed_body_conflict(self):
        first, second = initial(), next_publication(initial())
        for proposed in (replace(second, revision=3), replace(second, parent_digest='f' * 64)):
            self.assert_code('SIGNING_PUBLICATION_CHAIN_MISMATCH', validate_publication_transition, first, proposed)
        alternate = next_publication(first, published_at=second.published_at + timedelta(seconds=1))
        self.assertNotEqual(alternate.digest, second.digest)

    def test_unsupported_names_versions_counters_and_naive_timestamps_reject(self):
        for changes in (dict(version='v2'), dict(policy_id='other-scope')):
            self.assert_code('UNSUPPORTED_SIGNING_PUBLICATION', replace, initial(), **changes)
        for revision in (True, 0, MAX_REVISION + 1, 1.5):
            self.assert_code('INVALID_SIGNING_PUBLICATION_COUNTER', replace, initial(), revision=revision)
        self.assert_code('INVALID_SIGNING_PUBLICATION_PARENT', replace, initial(), parent_digest='f' * 64)
        with self.assertRaises(ContractError):
            initial(published_at=NOW.replace(tzinfo=None))

    def test_revision_exhaustion_cannot_wrap(self):
        last = replace(next_publication(initial()), revision=MAX_REVISION)
        self.assert_code('INVALID_SIGNING_PUBLICATION_COUNTER', next_publication, last)

    def test_active_publication_requires_issuing_key_and_ordered_time(self):
        self.assert_code('SHARING_SIGNING_KEY_NOT_ISSUING', initial,
                         published_at=fixtures.NEW.issue_until)
        self.assert_code('SIGNING_PUBLICATION_TIME_REGRESSION', next_publication,
                         initial(), published_at=NOW - timedelta(seconds=1))

    def test_planned_rotation_and_retired_key_revocation_keep_pins(self):
        cfg = secrets.config(policy=fixtures.policy('old'))
        first = initial(cfg, published_at=fixtures.TIME)
        rotated = next_publication(first, config=secrets.config(), published_at=fixtures.OLD.issue_until)
        validate_publication_transition(first, rotated)
        revoked_config = replace(rotated.config,
            policy=replace(rotated.config.policy, keys=(replace(fixtures.OLD, revoked=True), fixtures.NEW)))
        revoked = next_publication(rotated, config=revoked_config, published_at=NOW)
        self.assertTrue(next(record.key.revoked for record in revoked.history if record.key.key_id == 'old'))
        self.assert_code('SHARING_SIGNING_KEY_REBOUND', next_publication, revoked, config=rotated.config)

    def test_retirement_removes_active_reference_but_preserves_history(self):
        retired, _ = retired_history()
        self.assertNotIn('old', {key.key_id for key in retired.config.policy.keys})
        self.assertIn('old', {record.key.key_id for record in retired.history})

    def test_removed_key_cannot_return_as_new_future_cohort(self):
        retired, third = retired_history()
        revived = SharingSigningKey('old', 'standard', third.issue_until,
            third.issue_until + timedelta(days=1), third.issue_until + timedelta(days=8))
        cfg = replace(retired.config, policy=replace(retired.config.policy, keys=retired.config.policy.keys + (revived,)),
                      versions=retired.config.versions + (SharingSecretVersion('old', 'sharing-intents', 4),))
        self.assert_code('SIGNING_RETIRED_KEY_REINTRODUCED', next_publication, retired, config=cfg)

    def test_removed_secret_version_cannot_be_reused_under_another_id(self):
        retired, third = retired_history()
        fourth = SharingSigningKey('fourth', 'standard', third.issue_until,
            third.issue_until + timedelta(days=1), third.issue_until + timedelta(days=8))
        cfg = replace(retired.config, policy=replace(retired.config.policy, keys=retired.config.policy.keys + (fourth,)),
                      versions=retired.config.versions + (SharingSecretVersion('fourth', 'sharing-intents', 1),))
        self.assert_code('SIGNING_HISTORY_SECRET_REUSED', next_publication, retired, config=cfg)

    def test_profile_id_cannot_be_rebound_after_all_original_keys_retire(self):
        retired, third = retired_history()
        archive_profile = fixtures.profile('archive')
        historical_key = replace(fixtures.OLD, profile_id='archive', key_id='archived',
            issue_from=fixtures.OLD.issue_from-timedelta(days=2),
            issue_until=fixtures.OLD.issue_from-timedelta(days=1),
            retain_until=fixtures.OLD.issue_from-timedelta(days=1)+RETRY_WINDOW)
        historical = SigningKeyHistory(historical_key, SharingSecretVersion('archived', 'archive', 1), archive_profile)
        # A supplied checkpoint can contain previously retired history. Its trust
        # and full predecessor chain belong to the future authoritative reader.
        retired = replace(retired, history=(historical,) + retired.history)
        fourth = SharingSigningKey('fourth', 'archive', third.issue_until,
            third.issue_until + timedelta(days=1), third.issue_until + timedelta(days=8))
        changed_profile = fixtures.profile('archive', fixtures.SharingQuerySettings(maximum_bytes_billed=1024))
        cfg = replace(retired.config, policy=replace(retired.config.policy,
            keys=retired.config.policy.keys + (fourth,), profiles=retired.config.policy.profiles + (changed_profile,)),
            versions=retired.config.versions + (SharingSecretVersion('fourth', 'sharing-intents', 4),))
        self.assert_code('SIGNING_HISTORY_PROFILE_REBOUND', next_publication, retired, config=cfg)

    def test_history_cannot_be_deleted_from_valid_next_publication(self):
        retired, _ = retired_history()
        valid = next_publication(retired)
        trimmed = replace(valid, history=tuple(record for record in valid.history if record.key.key_id != 'old'))
        self.assert_code('SIGNING_PUBLICATION_CHAIN_MISMATCH', validate_publication_transition, retired, trimmed)

    def test_checkpoint_history_cannot_contain_overlapping_acceptance_cohorts(self):
        second = next_publication(initial())
        extra_key = replace(fixtures.OLD, key_id='archived')
        extra = SigningKeyHistory(extra_key, SharingSecretVersion('archived', 'archive', 1), fixtures.profile())
        self.assert_code('OVERLAPPING_SIGNING_HISTORY_WINDOWS', replace, second,
                         history=(extra,) + second.history)

    def test_stop_revokes_active_history_and_never_changes_config(self):
        first = initial()
        stopped = next_publication(first, status=STOPPED)
        self.assertEqual(stopped.config, first.config)
        self.assertTrue(next(r.key.revoked for r in stopped.history if r.key.key_id == 'new'))
        self.assertFalse(stopped.config.policy.key('new').revoked)  # Last valid config, never loadable as STOPPED.
        self.assert_code('SIGNING_HISTORY_KEY_REBOUND', next_publication, stopped)
        changed = replace(first.config, versions=(replace(secrets.VERSIONS[0], version=3), secrets.VERSIONS[1]))
        self.assert_code('SIGNING_STOP_CONFIG_CHANGED', next_publication, first, config=changed, status=STOPPED)

    def test_stop_can_publish_after_issuance_expiry_and_repeat_without_unrevoking(self):
        stopped = next_publication(initial(), status=STOPPED, published_at=fixtures.NEW.issue_until)
        repeated = next_publication(stopped, status=STOPPED)
        self.assertEqual(repeated.history, stopped.history)

    def test_resume_requires_replacement_key_and_retains_emergency_revocation(self):
        cfg, third = scheduled_config()
        stopped = next_publication(initial(cfg), status=STOPPED)
        resumed_cfg = replace(cfg, policy=replace(cfg.policy, active_key_id='third',
            keys=(fixtures.OLD, replace(fixtures.NEW, revoked=True), third)))
        resumed = next_publication(stopped, config=resumed_cfg, published_at=third.issue_from)
        self.assertEqual(resumed.status, ACTIVE)
        self.assertEqual(resumed.config.policy.active_key_id, 'third')
        self.assertTrue(next(r.key.revoked for r in resumed.history if r.key.key_id == 'new'))

    def test_key_schedule_and_version_rebinding_are_rejected(self):
        first = initial()
        changed = replace(first.config, versions=(replace(secrets.VERSIONS[0], version=3), secrets.VERSIONS[1]))
        self.assert_code('SHARING_SECRET_VERSION_REBOUND', next_publication, first, config=changed)
        shifted = replace(first.config, policy=replace(first.config.policy,
            keys=(replace(fixtures.OLD, retain_until=fixtures.OLD.retain_until + timedelta(seconds=1)), fixtures.NEW)))
        self.assert_code('SHARING_SIGNING_KEY_REBOUND', next_publication, first, config=shifted)

    def test_duplicate_mutable_mismatched_and_over_capacity_history_reject(self):
        first = initial()
        for history in (list(first.history), (), first.history * (MAX_KEY_HISTORY + 1)):
            self.assert_code('INVALID_SIGNING_KEY_HISTORY', replace, first, history=history)
        self.assert_code('INVALID_SIGNING_KEY_HISTORY', replace, first, history=(first.history[0], first.history[0]))
        self.assert_code('INVALID_SIGNING_KEY_HISTORY', SigningKeyHistory, fixtures.OLD,
                         secrets.VERSIONS[1], fixtures.profile())
        wrong = replace(first.history[0], key=replace(first.history[0].key, revoked=True))
        self.assert_code('SIGNING_HISTORY_CONFIG_MISMATCH', replace, first, history=(wrong, first.history[1]))

    def test_publication_and_history_are_frozen(self):
        first = initial()
        with self.assertRaises(FrozenInstanceError):
            first.revision = 2
        with self.assertRaises(FrozenInstanceError):
            first.history[0].secret = secrets.VERSIONS[1]


class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.publication = initial()
        self.cold = SigningReplicaState('replica-instance-A')

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def loading(self, state=None, publications=None, observed_at=NOW, **changes):
        reading, ticket = begin_refresh(self.cold if state is None else state)
        loaded, load_ticket = observe_publications(reading, ticket,
            publications=(self.publication,) if publications is None else publications,
            observed_at=observed_at, now=observed_at, **changes)
        return loaded, load_ticket

    def ready(self):
        loading, ticket = self.loading()
        return complete_refresh(loading, ticket, loaded_config=self.publication.config, now=NOW)

    def test_cold_replica_is_not_ready_before_read_and_load(self):
        self.assert_code('SIGNING_REPLICA_NOT_READY', require_signing_ready, self.cold, now=NOW)
        loading, ticket = self.loading()
        self.assert_code('SIGNING_REPLICA_NOT_READY', require_signing_ready, loading, now=NOW)
        ready = complete_refresh(loading, ticket, loaded_config=self.publication.config, now=NOW)
        self.assertEqual(require_signing_ready(ready, now=NOW), self.publication)

    def test_begin_refresh_invalidates_previously_ready_state(self):
        ready = self.ready()
        reading, _ = begin_refresh(ready)
        self.assertEqual(reading.head, ready.head)
        self.assertIsNone(reading.valid_until)
        self.assert_code('SIGNING_REPLICA_NOT_READY', require_signing_ready, reading, now=NOW)

    def test_failed_authority_read_denies_old_keys_but_keeps_revision_floor(self):
        ready = self.ready()
        reading, ticket = begin_refresh(ready)
        failed = fail_refresh(reading, ticket)
        self.assertEqual(failed.head, ready.head)
        self.assert_code('SIGNING_REPLICA_NOT_READY', require_signing_ready, failed, now=NOW)

    def test_new_revision_is_pinned_before_load_and_survives_load_failure(self):
        second = next_publication(self.publication)
        loading, ticket = self.loading(self.ready(), (second,), observed_at=second.published_at)
        failed = fail_refresh(loading, ticket)
        self.assertEqual(failed.head, second)
        reading, read_ticket = begin_refresh(failed)
        self.assert_code('SIGNING_PUBLICATION_ROLLBACK_OR_CONFLICT', observe_publications, reading, read_ticket,
            publications=(self.publication,), observed_at=second.published_at, now=second.published_at)
        self.assert_code('SIGNING_REPLICA_NOT_READY', require_signing_ready, failed, now=second.published_at)

    def test_same_revision_can_retry_fresh_load_after_failure(self):
        loading, ticket = self.loading()
        failed = fail_refresh(loading, ticket)
        loading, ticket = self.loading(failed, observed_at=NOW + timedelta(seconds=2))
        ready = complete_refresh(loading, ticket, loaded_config=self.publication.config,
                                 now=NOW + timedelta(seconds=3))
        self.assertEqual(ready.valid_until, NOW + timedelta(seconds=32))

    def test_same_revision_different_digest_is_conflict(self):
        alternate = initial(published_at=NOW - timedelta(seconds=1))
        reading, ticket = begin_refresh(self.ready())
        self.assert_code('SIGNING_PUBLICATION_ROLLBACK_OR_CONFLICT', observe_publications, reading, ticket,
                         publications=(alternate,), observed_at=NOW, now=NOW)

    def test_skipped_revisions_require_complete_bounded_chain(self):
        second = next_publication(self.publication)
        third = next_publication(second)
        reading, ticket = begin_refresh(self.ready())
        self.assert_code('SIGNING_PUBLICATION_HISTORY_REQUIRED', observe_publications, reading, ticket,
                         publications=(third,), observed_at=third.published_at, now=third.published_at)
        loading, _ = observe_publications(reading, ticket, publications=(second, third),
                                          observed_at=third.published_at, now=third.published_at)
        self.assertEqual(loading.head, third)

    def test_cold_replica_needs_genesis_or_trusted_checkpoint(self):
        second = next_publication(self.publication)
        reading, ticket = begin_refresh(self.cold)
        self.assert_code('SIGNING_PUBLICATION_HISTORY_REQUIRED', observe_publications, reading, ticket,
                         publications=(second,), observed_at=second.published_at, now=second.published_at)
        checkpoint = SigningReplicaState('restart-B', head=second)
        loading, load_ticket = self.loading(checkpoint, (second,), observed_at=second.published_at)
        self.assertEqual(loading.head, second)
        self.assertIsNotNone(load_ticket)

    def test_old_restart_callback_cannot_complete_new_instance_same_generation(self):
        old_loading, old_ticket = self.loading()
        restarted = SigningReplicaState('restart-B', head=self.publication)
        new_loading, _ = self.loading(restarted)
        self.assertEqual(old_loading.generation, new_loading.generation)
        self.assert_code('STALE_SIGNING_REFRESH_TICKET', complete_refresh, new_loading, old_ticket,
                         loaded_config=self.publication.config, now=NOW)

    def test_late_read_and_load_callbacks_cannot_supersede_new_generation(self):
        reading, read_ticket = begin_refresh(self.cold)
        newer_reading, _ = begin_refresh(reading)
        self.assert_code('STALE_SIGNING_REFRESH_TICKET', observe_publications, newer_reading, read_ticket,
                         publications=(self.publication,), observed_at=NOW, now=NOW)
        loading, load_ticket = self.loading()
        newer_reading, _ = begin_refresh(loading)
        self.assert_code('STALE_SIGNING_REFRESH_TICKET', complete_refresh, newer_reading, load_ticket,
                         loaded_config=self.publication.config, now=NOW)
        self.assert_code('STALE_SIGNING_REFRESH_TICKET', fail_refresh, newer_reading, load_ticket)

    def test_stopped_publication_never_creates_load_ticket_and_blocks_late_keys(self):
        previous_loading, previous_ticket = self.loading()
        stopped = next_publication(self.publication, status=STOPPED)
        state, ticket = self.loading(previous_loading, (stopped,), observed_at=stopped.published_at)
        self.assertEqual(state.phase, SigningReplicaPhase.STOPPED)
        self.assertIsNone(ticket)
        self.assert_code('SIGNING_REPLICA_NOT_READY', require_signing_ready, state, now=stopped.published_at)
        self.assert_code('STALE_SIGNING_REFRESH_TICKET', complete_refresh, state, previous_ticket,
                         loaded_config=self.publication.config, now=stopped.published_at)

    def test_fresh_observation_does_not_renew_without_another_key_load(self):
        ready = self.ready()
        loading, _ = self.loading(ready, observed_at=NOW + timedelta(seconds=1))
        self.assertEqual(loading.phase, SigningReplicaPhase.LOADING)
        self.assert_code('SIGNING_REPLICA_NOT_READY', require_signing_ready, loading,
                         now=NOW + timedelta(seconds=1))

    def test_readiness_has_half_open_freshness_and_cannot_extend_at_load_completion(self):
        loading, ticket = self.loading()
        ready = complete_refresh(loading, ticket, loaded_config=self.publication.config,
                                 now=NOW + timedelta(seconds=20))
        self.assertEqual(ready.valid_until, NOW + timedelta(seconds=30))
        require_signing_ready(ready, now=NOW + timedelta(seconds=29))
        self.assert_code('SIGNING_REFRESH_EXPIRED', require_signing_ready, ready,
                         now=NOW + timedelta(seconds=30))
        self.assert_code('SIGNING_REFRESH_EXPIRED', complete_refresh, loading, ticket,
                         loaded_config=self.publication.config, now=NOW + timedelta(seconds=30))

    def test_stale_future_or_prepublication_read_times_are_rejected(self):
        for observed, now in ((NOW, NOW + timedelta(seconds=30)), (NOW + timedelta(seconds=1), NOW)):
            reading, ticket = begin_refresh(self.cold)
            self.assert_code('STALE_SIGNING_PUBLICATION_OBSERVATION', observe_publications, reading, ticket,
                             publications=(self.publication,), observed_at=observed, now=now)
        reading, ticket = begin_refresh(self.cold)
        self.assert_code('INVALID_SIGNING_PUBLICATION_OBSERVATION', observe_publications, reading, ticket,
                         publications=(self.publication,), observed_at=NOW-timedelta(seconds=1), now=NOW)

    def test_ready_cannot_be_used_before_its_observation_time(self):
        self.assert_code('SIGNING_REFRESH_EXPIRED', require_signing_ready, self.ready(),
                         now=NOW-timedelta(seconds=1))

    def test_issuance_end_clips_lease_and_expired_head_remains_high_water(self):
        read_at = fixtures.NEW.issue_until - timedelta(seconds=1)
        loading, ticket = self.loading(observed_at=read_at)
        self.assertEqual(ticket.valid_until, fixtures.NEW.issue_until)
        self.assert_code('SIGNING_REFRESH_EXPIRED', complete_refresh, loading, ticket,
                         loaded_config=self.publication.config, now=fixtures.NEW.issue_until)
        expired, ticket = self.loading(observed_at=fixtures.NEW.issue_until)
        self.assertEqual(expired.phase, SigningReplicaPhase.FAILED)
        self.assertEqual(expired.head, self.publication)
        self.assertIsNone(ticket)

    def test_far_future_expired_policy_fails_closed_without_datetime_overflow(self):
        future = datetime.max.replace(tzinfo=timezone.utc)
        expired, ticket = self.loading(observed_at=future)
        self.assertEqual(expired.phase, SigningReplicaPhase.FAILED)
        self.assertEqual(expired.head, self.publication)
        self.assertIsNone(ticket)

    def test_unobserved_stop_is_not_falsely_claimed_as_instant_replica_revocation(self):
        replica_a = self.ready()
        replica_b = replace(self.cold, instance_id='replica-B')
        stopped = next_publication(self.publication, status=STOPPED)
        state_b, ticket = self.loading(replica_b, (self.publication, stopped), observed_at=stopped.published_at)
        self.assertEqual(state_b.phase, SigningReplicaPhase.STOPPED)
        self.assertIsNone(ticket)
        # Only the supplied state is known. Live refresh/authority checks remain
        # required; the old replica's bounded lease is not synchronously erased.
        require_signing_ready(replica_a, now=stopped.published_at)
        self.assert_code('SIGNING_REFRESH_EXPIRED', require_signing_ready, replica_a,
                         now=replica_a.valid_until)

    def test_loaded_config_must_match_full_pins_not_only_policy(self):
        loading, ticket = self.loading()
        changed = replace(self.publication.config,
            versions=(replace(secrets.VERSIONS[0], version=3), secrets.VERSIONS[1]))
        self.assertEqual(changed.policy, self.publication.config.policy)
        self.assert_code('SIGNING_LOADED_CONFIG_MISMATCH', complete_refresh, loading, ticket,
                         loaded_config=changed, now=NOW)

    def test_empty_duplicate_mutable_and_unbounded_snapshot_rows_are_rejected(self):
        for rows in ((), [self.publication], (self.publication, self.publication),
                     (self.publication,) * (MAX_REFRESH_CHAIN + 1)):
            reading, ticket = begin_refresh(self.cold)
            self.assert_code('INVALID_SIGNING_PUBLICATION_ROWS', observe_publications, reading, ticket,
                             publications=rows, observed_at=NOW, now=NOW)

    def test_ticket_cannot_change_revision_digest_or_deadline(self):
        loading, ticket = self.loading()
        for changed in (replace(ticket, revision=2), replace(ticket, digest='f'*64),
                        replace(ticket, valid_until=ticket.valid_until + timedelta(seconds=1))):
            self.assert_code('STALE_SIGNING_REFRESH_TICKET', complete_refresh, loading, changed,
                             loaded_config=self.publication.config, now=NOW)

    def test_incompatible_parent_chain_is_rejected(self):
        second = next_publication(self.publication)
        wrong = replace(second, parent_digest='f'*64)
        reading, ticket = begin_refresh(self.ready())
        self.assert_code('SIGNING_PUBLICATION_CHAIN_MISMATCH', observe_publications, reading, ticket,
                         publications=(wrong,), observed_at=second.published_at, now=second.published_at)

    def test_refresh_settings_and_state_lease_shapes_are_strict(self):
        for age in (True, 0, 301, 1.5):
            self.assert_code('INVALID_SIGNING_REFRESH_SETTINGS', SigningRefreshSettings, max_age_seconds=age)
        self.assert_code('INVALID_SIGNING_REPLICA_STATE', replace, self.cold, phase='READY')
        self.assert_code('INVALID_SIGNING_REPLICA_LEASE', replace, self.cold,
                         observed_at=NOW, valid_until=NOW+timedelta(seconds=1))
        self.assert_code('INVALID_SIGNING_REPLICA_STATE', replace, self.cold, phase=SigningReplicaPhase.READY)
        loading, _ = self.loading()
        self.assert_code('INVALID_SIGNING_REPLICA_LEASE', replace, loading,
                         valid_until=NOW+timedelta(seconds=301))

    def test_generation_cannot_wrap_and_metadata_is_immutable(self):
        state = SigningReplicaState('replica-A', head=self.publication, generation=MAX_REVISION,
                                   phase=SigningReplicaPhase.READING)
        self.assert_code('INVALID_SIGNING_PUBLICATION_COUNTER', begin_refresh, state)
        reading, ticket = begin_refresh(self.cold)
        with self.assertRaises(FrozenInstanceError):
            reading.phase = SigningReplicaPhase.READY
        with self.assertRaises(FrozenInstanceError):
            ticket.generation = 2


if __name__ == '__main__':
    unittest.main()
