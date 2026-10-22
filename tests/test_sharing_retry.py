"""Offline retry identity/integrity checks; supplied rollback is not native proof."""
import base64
from dataclasses import FrozenInstanceError
from datetime import timedelta, timezone
import hmac
import json
import unittest
from unittest.mock import patch

from src.application.access_policy import Visibility
from src.application.identity import IdentityError
from src.application.sharing_coordinator_contract import SharingCommitObservation, SharingCommitStatus
from src.application.sharing_intent import SharingIntentSealer, initial_attempt
from src.application.sharing_query import SharingQuerySettings, SharingResultState, validate_sharing_result
from src.application.sharing_retry import (
    MAX_RETRIES, SharingRetryAttempt, SharingRetrySealer, SharingRetryStep, prepare_retry,
)
from src.application.workflow_contract import ContractError, canonical_json_bytes
from src.application.workspace_contract import MAX_REVISION
from tests.test_membership_contract import ADMIN, ADMIN_SUBJECT, TIME
from tests.test_sharing_coordinator_contract import command
from tests.test_sharing_executor import EPOCH, KEY, identity
from tests.test_sharing_query import query, result_row

NOW = TIME + timedelta(seconds=10)


def rollback(attempt, **changes):
    return SharingCommitObservation(**(dict(audit_id=attempt.query.command.audit_id,
        status=SharingCommitStatus.NOT_COMMITTED, query_job_id=attempt.job_id) | changes))


def retry(predecessor=None, **changes):
    predecessor = initial_attempt(query()) if predecessor is None else predecessor
    return prepare_retry(predecessor, **(dict(commit_observation=rollback(predecessor),
        expected_workspace_revision=predecessor.query.expected_workspace_revision,
        observed_at=NOW) | changes))


def encode(value):
    return base64.urlsafe_b64encode(value).decode().rstrip('=')


class SharingRetryTests(unittest.TestCase):
    def setUp(self):
        for patcher in (patch('src.application.identity.time.time', return_value=EPOCH + 10),
                        patch('src.application.sharing_intent._now', return_value=NOW),
                        patch('src.application.sharing_retry._now', return_value=NOW)):
            patcher.start()
            self.addCleanup(patcher.stop)
        self.original = initial_attempt(query())
        self.sealer = SharingRetrySealer(key=KEY, key_id='fixture-key')

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def token_payload(self, token):
        value = token.split('.')[1]
        return json.loads(base64.urlsafe_b64decode(value + '=' * (-len(value) % 4)))

    def sign_payload(self, payload, *, raw=None, domain=b'ci-sharing-retry-v1\0'):
        raw = canonical_json_bytes(payload) if raw is None else raw
        return 'share-retry1.' + encode(raw) + '.' + encode(hmac.digest(KEY, domain + raw, 'sha256'))

    def test_same_fence_is_valid_but_has_new_stable_job_id(self):
        first = retry(self.original)
        self.assertEqual(first, retry(initial_attempt(query())))
        self.assertNotEqual(first.job_id, self.original.job_id)
        self.assertEqual(len(first.fingerprint), 64)
        self.assertLessEqual(len(first.job_id), 128)

    def test_replica_observation_times_do_not_remint_job(self):
        first = retry(observed_at=TIME + timedelta(seconds=1))
        second = retry(observed_at=TIME + timedelta(seconds=2))
        self.assertEqual(first.job_id, second.job_id)
        self.assertNotEqual(self.sealer.seal_retry(first), self.sealer.seal_retry(second))

    def test_new_job_is_not_submitted_and_predecessor_proof_is_separate(self):
        attempt = retry()
        self.assertIs(attempt.query.commit_observation.status, SharingCommitStatus.NOT_SUBMITTED)
        self.assertIsNone(attempt.query.commit_observation.query_job_id)
        self.assertEqual(attempt.steps[0].predecessor_observation, rollback(self.original))
        params = {p.name: p.value for p in attempt.query.job_config().query_parameters}
        self.assertEqual(params['commit_status'], 'NOT_SUBMITTED')

    def test_only_workspace_fence_changes_original_envelope(self):
        attempt = retry(self.original, expected_workspace_revision=9)
        self.assertIs(attempt.query.command, self.original.query.command)
        self.assertEqual(attempt.query.settings, self.original.query.settings)
        self.assertEqual(attempt.query.command.audit_id, self.original.query.command.audit_id)
        self.assertEqual(attempt.query.command.deadline_at, TIME + timedelta(days=7))
        self.assertNotEqual(attempt.job_id, retry(expected_workspace_revision=8).job_id)

    def test_settings_and_business_envelope_are_pinned_in_job_identity(self):
        variants = [query(settings=SharingQuerySettings(maximum_bytes_billed=1024)),
                    query(command=command(visibility=Visibility.PRIVATE)),
                    query(command=command(trace_id='another-trace')),
                    query(command=command(workspace_id='another-workspace'))]
        ids = {retry(initial_attempt(q)).job_id for q in variants}
        self.assertEqual(len(ids), len(variants))
        self.assertNotIn(retry().job_id, ids)

    def test_unknown_committed_and_not_submitted_cannot_propose_retry(self):
        for status in (SharingCommitStatus.UNKNOWN, SharingCommitStatus.COMMITTED,
                       SharingCommitStatus.NOT_SUBMITTED):
            observation = SharingCommitObservation(self.original.query.command.audit_id, status,
                None if status is SharingCommitStatus.NOT_SUBMITTED else self.original.job_id)
            with self.subTest(status=status):
                self.assert_code('SHARING_RETRY_REQUIRES_NONCOMMIT', retry,
                                 commit_observation=observation)

    def test_missing_or_untyped_observation_does_not_grant_retry(self):
        for value in (None, 'NOT_COMMITTED', {'status': 'NOT_COMMITTED'}, False):
            self.assert_code('SHARING_RETRY_REQUIRES_NONCOMMIT', retry, commit_observation=value)

    def test_rollback_must_match_exact_job_and_business_audit(self):
        for observation in (rollback(self.original, query_job_id='unrelated-job'),
                            rollback(self.original, audit_id=command(trace_id='another').audit_id)):
            self.assert_code('SHARING_RETRY_PREDECESSOR_MISMATCH', retry,
                             commit_observation=observation)

    def test_retry_chain_binds_immediate_predecessor(self):
        first = retry()
        second = retry(first)
        self.assertNotEqual(second.job_id, first.job_id)
        self.assertEqual(second.steps[-1].predecessor_observation.query_job_id, first.job_id)
        self.assert_code('SHARING_RETRY_PREDECESSOR_MISMATCH', retry, first,
                         commit_observation=rollback(self.original))

    def test_retry_chain_is_bounded_without_extending_window(self):
        attempt = self.original
        for _ in range(MAX_RETRIES):
            attempt = retry(attempt)
            self.assertEqual(attempt.query.command.deadline_at, self.original.query.command.deadline_at)
        self.assert_code('INVALID_SHARING_RETRY_CHAIN', retry, attempt)
        self.assertLessEqual(len(self.sealer.seal_retry(attempt)), 8192)

    def test_chain_rejects_empty_mutable_or_untyped_steps(self):
        for steps in ((), [], [retry().steps[0]], (None,)):
            with self.assertRaises(ContractError):
                SharingRetryAttempt(self.original, steps)

    def test_immutable_original_step_and_derived_attempt(self):
        attempt = retry()
        for obj, name, value in ((attempt, 'job_id', 'changed'),
                                 (attempt.steps[0], 'expected_workspace_revision', 9)):
            with self.assertRaises(FrozenInstanceError):
                setattr(obj, name, value)
        self.assert_code('INVALID_SHARING_RETRY_PREDECESSOR', prepare_retry, object(),
            commit_observation=rollback(self.original), expected_workspace_revision=7, observed_at=NOW)

    def test_fence_cannot_regress_or_overflow(self):
        self.assert_code('SHARING_RETRY_FENCE_REGRESSION', retry, expected_workspace_revision=6)
        self.assert_code('WORKSPACE_REVISION_EXHAUSTED', retry, expected_workspace_revision=MAX_REVISION)
        for value in (True, 0, -1, 1.2, '8', MAX_REVISION + 1):
            self.assert_code('INVALID_SHARING_REVISION', retry, expected_workspace_revision=value)
        self.assert_code('SHARING_RETRY_FENCE_REGRESSION', retry,
                         retry(expected_workspace_revision=9), expected_workspace_revision=8)

    def test_observation_is_inside_original_half_open_window(self):
        self.assert_code('SHARING_INTENT_NOT_YET_VALID', retry, observed_at=TIME-timedelta(seconds=1))
        self.assert_code('SHARING_RETRY_EXPIRED', retry, observed_at=TIME+timedelta(days=7))
        retry(observed_at=TIME+timedelta(days=7)-timedelta(microseconds=1))

    def test_chain_observation_cannot_go_backwards(self):
        first = retry(observed_at=TIME+timedelta(seconds=2))
        self.assert_code('INVALID_SHARING_RETRY_TIME', retry, first,
                         observed_at=TIME+timedelta(seconds=1))

    def test_naive_and_untyped_times_are_rejected(self):
        for value in (TIME.replace(tzinfo=None), None, '2026-10-10'):
            with self.assertRaises(ContractError):
                retry(observed_at=value)

    def test_timezone_equivalent_proposals_have_same_identity_and_seal(self):
        first = retry(observed_at=NOW)
        second = retry(observed_at=NOW.astimezone(timezone(timedelta(hours=5, minutes=30))))
        self.assertEqual(first.job_id, second.job_id)
        self.assertEqual(self.sealer.seal_retry(first), self.sealer.seal_retry(second))

    def test_existing_receipt_reuses_original_revision_after_later_visibility_change(self):
        attempt = retry(expected_workspace_revision=9)
        row = result_row('REUSED')
        row['workspace'].update(revision=10, updated_at=NOW)
        row['conversation'].update(visibility='PRIVATE', access_revision=5, updated_at=NOW)
        row['observed_at'] = NOW
        result = validate_sharing_result(attempt.query, [row], query_job_id=attempt.job_id)
        self.assertIs(result.state, SharingResultState.REUSED)
        self.assertEqual(result.resulting_revision, 4)
        self.assertEqual(row['conversation']['visibility'], 'PRIVATE')

    def test_roundtrip_retains_full_chain_across_server_instances(self):
        attempt = retry(retry(expected_workspace_revision=8), expected_workspace_revision=9)
        token = self.sealer.seal_retry(attempt)
        other = SharingRetrySealer(key=KEY, key_id='fixture-key')
        self.assertEqual(other.resolve_retry(token, identity()), attempt)
        self.assertEqual(other.seal_retry(attempt), token)

    def test_original_v1_seals_and_resolver_remain_compatible(self):
        original_sealer = SharingIntentSealer(key=KEY, key_id='fixture-key')
        token = original_sealer.seal(query())
        self.assertEqual(self.sealer.seal(query()), token)
        self.assertEqual(self.sealer.resolve(token, identity()), self.original)
        self.assert_code('INVALID_SHARING_SEAL', original_sealer.resolve,
                         self.sealer.seal_retry(retry()), identity())
        self.assert_code('INVALID_SHARING_RETRY_SEAL', self.sealer.resolve_retry, token, identity())

    def test_retry_seal_tamper_wrong_key_and_wrong_domain_fail(self):
        token = self.sealer.seal_retry(retry())
        payload = self.token_payload(token)
        payload['steps'][0]['expected_workspace_revision'] = 8
        parts = token.split('.')
        parts[1] = encode(canonical_json_bytes(payload))
        self.assert_code('INVALID_SHARING_RETRY_SEAL', self.sealer.resolve_retry,
                         '.'.join(parts), identity())
        other = SharingRetrySealer(key=b'x'*32, key_id='fixture-key')
        self.assert_code('INVALID_SHARING_RETRY_SEAL', other.resolve_retry, token, identity())
        self.assert_code('INVALID_SHARING_RETRY_SEAL', self.sealer.resolve_retry,
                         self.sign_payload(payload, domain=b'ci-sharing-intent-v1\0'), identity())

    def test_seal_key_id_and_version_are_exact(self):
        for field, value in (('key_id', 'another-key'), ('version', 'sharing-retry-v2')):
            payload = self.token_payload(self.sealer.seal_retry(retry()))
            payload[field] = value
            self.assert_code('INVALID_SHARING_RETRY_SEAL', self.sealer.resolve_retry,
                             self.sign_payload(payload), identity())

    def test_sealed_chain_fingerprint_and_predecessor_are_checked(self):
        for field, value, code in (('fingerprint', '0'*64, 'SHARING_ATTEMPT_MISMATCH'),
                                  ('predecessor_job_id', 'other-job', 'SHARING_RETRY_PREDECESSOR_MISMATCH'),
                                  ('predecessor_status', 'UNKNOWN', 'SHARING_RETRY_REQUIRES_NONCOMMIT')):
            payload = self.token_payload(self.sealer.seal_retry(retry()))
            target = payload if field == 'fingerprint' else payload['steps'][0]
            target[field] = value
            self.assert_code(code, self.sealer.resolve_retry, self.sign_payload(payload), identity())

    def test_original_token_cannot_be_swapped_under_a_retry_chain(self):
        payload = self.token_payload(self.sealer.seal_retry(retry()))
        payload['original_token'] = self.sealer.seal(query(command=command(trace_id='another-trace')))
        self.assert_code('SHARING_RETRY_PREDECESSOR_MISMATCH', self.sealer.resolve_retry,
                         self.sign_payload(payload), identity())

    def test_identity_must_be_fresh_and_match_original_principal(self):
        token = self.sealer.seal_retry(retry())
        self.assert_code('SHARING_IDENTITY_MISMATCH', self.sealer.resolve_retry,
                         token, identity(ADMIN, ADMIN_SUBJECT))
        with patch('src.application.identity.time.time', return_value=EPOCH+1000):
            with self.assertRaises(IdentityError):
                self.sealer.resolve_retry(token, identity())

    def test_resolve_and_seal_do_not_extend_original_deadline(self):
        attempt = retry()
        token = self.sealer.seal_retry(attempt)
        with patch('src.application.sharing_intent._now', return_value=TIME+timedelta(days=7)), \
             patch('src.application.sharing_retry._now', return_value=TIME+timedelta(days=7)):
            self.assert_code('SHARING_RETRY_EXPIRED', self.sealer.resolve_retry, token, identity())
            self.assert_code('SHARING_RETRY_EXPIRED', self.sealer.seal_retry, attempt)

    def test_future_observation_cannot_be_sealed_or_resolved(self):
        self.assert_code('INVALID_SHARING_RETRY_TIME', self.sealer.seal_retry,
                         retry(observed_at=NOW+timedelta(seconds=1)))
        payload = self.token_payload(self.sealer.seal_retry(retry()))
        payload['steps'][0]['observed_at'] = (NOW+timedelta(seconds=1)).isoformat()
        self.assert_code('INVALID_SHARING_RETRY_TIME', self.sealer.resolve_retry,
                         self.sign_payload(payload), identity())

    def test_sql_or_settings_drift_invalidates_existing_retry_token(self):
        token = self.sealer.seal_retry(retry())
        other = SharingRetrySealer(key=KEY, key_id='fixture-key',
                                  settings=SharingQuerySettings(maximum_bytes_billed=1024))
        self.assert_code('SHARING_ATTEMPT_MISMATCH', other.resolve_retry, token, identity())
        with patch('src.application.sharing_query._render_sql', return_value=query().sql+'\n--changed'):
            self.assert_code('SHARING_ATTEMPT_MISMATCH', self.sealer.resolve_retry, token, identity())
        self.assert_code('SHARING_SETTINGS_MISMATCH', other.seal_retry, retry())

    def test_exact_payload_fields_canonical_encoding_and_bounded_chain(self):
        original_payload = self.token_payload(self.sealer.seal_retry(retry()))
        for alter in (lambda p: p.update(extra='hidden'),
                      lambda p: p['steps'][0].update(extra='hidden'),
                      lambda p: p.update(steps=[]),
                      lambda p: p.update(steps=p['steps']*4)):
            payload = json.loads(json.dumps(original_payload))
            alter(payload)
            with self.assertRaises(ContractError):
                self.sealer.resolve_retry(self.sign_payload(payload), identity())
        raw = json.dumps(original_payload, indent=2).encode()
        self.assert_code('INVALID_SHARING_RETRY_SEAL', self.sealer.resolve_retry,
                         self.sign_payload(original_payload, raw=raw), identity())

    def test_malformed_and_oversized_tokens_are_safe_failures(self):
        for token in (None, '', 'share-retry1.x.y', 'share-retry1.'+'x'*8192+'.'+'a'*43,
                      self.sign_payload(None)):
            self.assert_code('INVALID_SHARING_RETRY_SEAL', self.sealer.resolve_retry, token, identity())


if __name__ == '__main__':
    unittest.main()
