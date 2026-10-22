"""Real SDK message/client fixtures; RPCs/credentials are mocked, no cloud I/O."""
from dataclasses import FrozenInstanceError, replace
import traceback
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import FailedPrecondition, NotFound, PermissionDenied, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.cloud import secretmanager_v1 as sm
import google_crc32c

from src.application.sharing_secret_loader import (
    REGIONAL_ENDPOINT, SharingSecretConfig, SharingSecretLoadError,
    SharingSecretLoadSettings, SharingSecretLoader, SharingSecretVersion,
    create_regional_secret_client, validate_secret_config_transition,
)
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT
from tests import test_sharing_signing as fixtures
from tests.test_sharing_executor import EPOCH, identity
from tests.test_sharing_query import query
from tests.test_sharing_retry import retry

PROJECT_NUMBER = '12345678901'  # Offline fixture, not this project's deployed project number.
VERSIONS = (SharingSecretVersion('old', 'sharing-intents', 1),
            SharingSecretVersion('new', 'sharing-intents', 2))


def config(**changes):
    return SharingSecretConfig(**(dict(policy=fixtures.policy(), project_number=PROJECT_NUMBER,
                                     versions=VERSIONS) | changes))


def response(name, data):
    return sm.AccessSecretVersionResponse(name=name,
        payload=sm.SecretPayload(data=data, data_crc32c=google_crc32c.value(data)))


class SharingSecretLoaderTests(unittest.TestCase):
    def setUp(self):
        for module in ('sharing_secret_loader', 'sharing_retained_sealer', 'sharing_intent', 'sharing_retry'):
            patcher = patch('src.application.' + module + '._now', return_value=fixtures.NOW)
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher = patch('src.application.identity.time.time', return_value=EPOCH + 10)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.client = sm.SecretManagerServiceClient(credentials=AnonymousCredentials(),
                                                   client_options={'api_endpoint': REGIONAL_ENDPOINT})
        self.addCleanup(self.client.transport.close)
        self.cfg = config()
        self.fetch = Mock(side_effect=self.good_response)
        self.client.access_secret_version = self.fetch
        self.loader = SharingSecretLoader(client=self.client)

    def good_response(self, *, request, **kwargs):
        binding = next(item for item in self.cfg.versions if self.cfg.name(item) == request['name'])
        return response(request['name'], fixtures.MATERIAL[binding.key_id])

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises((ContractError, SharingSecretLoadError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_fresh_load_resolves_original_and_retry_across_restart(self):
        first = self.loader.load(self.cfg)
        second = self.loader.load(self.cfg)
        token = fixtures.SharingRetrySealer(key=fixtures.KEY, key_id='old').seal(query())
        original = first.resolve(token, identity())
        self.assertEqual(second.resolve(token, identity()), original)
        attempt = retry(original)
        self.assertEqual(second.resolve_retry(first.seal_retry(attempt), identity()), attempt)
        self.assertEqual(self.fetch.call_count, 4)  # Every composition fetches both keys anew.

    def test_calls_use_numeric_regional_names_and_explicit_bounds_without_retries(self):
        self.loader.load(self.cfg)
        self.assertEqual(self.fetch.call_count, 2)
        for call, binding in zip(self.fetch.call_args_list, VERSIONS):
            self.assertEqual(call.kwargs['request'], {'name': self.cfg.name(binding)})
            self.assertIsNone(call.kwargs['retry'])
            self.assertGreater(call.kwargs['timeout'], 0)
            self.assertLessEqual(call.kwargs['timeout'], 5)
            self.assertEqual(set(call.kwargs), {'request', 'retry', 'timeout'})
        self.assertEqual(self.client.transport._host, REGIONAL_ENDPOINT + ':443')

    def test_changed_active_profile_does_not_change_retained_original_settings(self):
        limited = SharingQuerySettings(maximum_bytes_billed=1024)
        changed = fixtures.policy(keys=(fixtures.OLD, replace(fixtures.NEW, profile_id='limited')),
                                  profiles=(fixtures.profile(), fixtures.profile('limited', limited)))
        sealer = self.loader.load(config(policy=changed))
        token = fixtures.SharingRetrySealer(key=fixtures.KEY, key_id='old').seal(query())
        original = sealer.resolve(token, identity())
        self.assertEqual(sealer.settings, limited)
        self.assertEqual(original.query.settings, SharingQuerySettings())
        attempt = retry(original)
        self.assertEqual(sealer.resolve_retry(sealer.seal_retry(attempt), identity()).job_id, attempt.job_id)

    def test_aliases_boolean_and_non_numeric_versions_are_rejected(self):
        for version in ('latest', 'current', '2', True, 0, -1, 2**63, 1.5, None):
            self.assert_code('INVALID_SHARING_SECRET_VERSION', SharingSecretVersion,
                             'old', 'sharing-intents', version)
        self.fetch.assert_not_called()

    def test_invalid_ids_paths_and_unbounded_secret_ids_are_rejected(self):
        for secret_id in ('', 'a/b', 'https://secret', 'has space', 'x' * 256, None):
            self.assert_code('INVALID_SHARING_SECRET_ID', SharingSecretVersion, 'old', secret_id, 1)
        self.assert_code('INVALID_SHARING_SIGNING_IDENTIFIER', SharingSecretVersion, 'key/path', 'secret', 1)
        self.fetch.assert_not_called()

    def test_project_and_region_scope_are_fixed_and_numeric_project_is_required(self):
        for changes in (dict(project_id='other-project'), dict(location='us-central1')):
            self.assert_code('INVALID_SHARING_SECRET_SCOPE', config, **changes)
        for project_number in ('', '00123', PROJECT, '123/456', 123, None, '1' * 21):
            self.assert_code('INVALID_SHARING_SECRET_PROJECT', config, project_number=project_number)
        self.fetch.assert_not_called()

    def test_bindings_must_cover_every_policy_key_uniquely(self):
        for versions in (VERSIONS[:1], VERSIONS + (SharingSecretVersion('extra', 'extra', 3),),
                         list(VERSIONS), (VERSIONS[0], VERSIONS[0]),
                         (VERSIONS[0], SharingSecretVersion('new', VERSIONS[0].secret_id, 1))):
            self.assert_code('INVALID_SHARING_SECRET_BINDINGS', config, versions=versions)
        self.fetch.assert_not_called()

    def test_config_is_immutable_and_name_cannot_reference_unregistered_binding(self):
        with self.assertRaises(FrozenInstanceError):
            self.cfg.project_number = '456'
        with self.assertRaises(FrozenInstanceError):
            VERSIONS[0].version = 3
        self.assert_code('INVALID_SHARING_SECRET_BINDINGS', self.cfg.name,
                         SharingSecretVersion('old', 'foreign', 3))

    def test_wrong_endpoint_is_rejected_before_rpc(self):
        client = sm.SecretManagerServiceClient(credentials=AnonymousCredentials())
        self.addCleanup(client.transport.close)
        self.assert_code('INVALID_SHARING_SECRET_CLIENT', SharingSecretLoader, client=client)
        self.assert_code('INVALID_SHARING_SECRET_CLIENT', SharingSecretLoader, client=object())
        self.fetch.assert_not_called()

    def test_all_profiles_are_preflighted_before_first_fetch(self):
        with patch('src.application.sharing_signing_contract._render_sql', return_value='drift'):
            self.assert_code('SHARING_SEAL_PROFILE_UNAVAILABLE', self.loader.load, self.cfg)
        self.fetch.assert_not_called()

    def test_inactive_issuance_interval_and_invalid_config_do_not_fetch(self):
        cfg = config(policy=fixtures.policy('old'))
        self.assert_code('SHARING_SIGNING_KEY_NOT_ISSUING', self.loader.load, cfg)
        self.assert_code('INVALID_SHARING_SECRET_CONFIG', self.loader.load, {})
        self.fetch.assert_not_called()

    def test_revoked_version_binding_is_retained_without_fetching_its_bytes(self):
        revoked = replace(fixtures.OLD, revoked=True)
        cfg = config(policy=fixtures.policy(keys=(revoked, fixtures.NEW)))
        sealer = self.loader.load(cfg)
        self.assertEqual(self.fetch.call_count, 1)
        self.assertEqual(self.fetch.call_args.kwargs['request']['name'], cfg.name(VERSIONS[1]))
        token = fixtures.SharingRetrySealer(key=fixtures.KEY, key_id='old').seal(query())
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', sealer.resolve, token, identity())

    def test_provider_missing_disabled_denied_unavailable_and_timeout_stop_without_retry(self):
        for error in (NotFound('private secret'), FailedPrecondition('disabled private secret'),
                      PermissionDenied('private identity'), ServiceUnavailable('private transport'),
                      TimeoutError('private timeout')):
            self.fetch.reset_mock()
            self.fetch.side_effect = error
            self.assert_code('SHARING_SECRET_UNAVAILABLE', self.loader.load, self.cfg)
            self.assertEqual(self.fetch.call_count, 1)

    def test_partial_failure_cannot_construct_sealer_or_use_last_success_as_cache(self):
        self.loader.load(self.cfg)
        self.fetch.reset_mock()
        self.fetch.side_effect = [self.good_response(request={'name': self.cfg.name(VERSIONS[0])}),
                                 PermissionDenied('private second key')]
        with patch('src.application.sharing_secret_loader.RetainedSharingSealer') as construct:
            self.assert_code('SHARING_SECRET_UNAVAILABLE', self.loader.load, self.cfg)
            construct.assert_not_called()
        self.assertEqual(self.fetch.call_count, 2)

    def test_response_must_be_actual_sdk_message_with_exact_pinned_name(self):
        valid = self.cfg.name(VERSIONS[0])
        variants = (valid.replace(PROJECT_NUMBER, '999'), valid.replace('asia-south1', 'us-central1'),
                    valid.replace('/secrets/sharing-intents/', '/secrets/other/'),
                    valid[:-1] + '2', valid[:-1] + 'latest', '', valid.replace('/locations/asia-south1', ''))
        for name in variants:
            self.fetch.side_effect = None
            self.fetch.return_value = response(name, fixtures.KEY)
            self.assert_code('SHARING_SECRET_RESPONSE_MISMATCH', self.loader.load, self.cfg)
        self.fetch.return_value = {'name': valid, 'payload': {'data': fixtures.KEY}}
        self.assert_code('SHARING_SECRET_RESPONSE_MISMATCH', self.loader.load, self.cfg)

    def test_missing_corrupt_out_of_range_checksum_and_corrupted_bytes_are_rejected(self):
        name = self.cfg.name(VERSIONS[0])
        payloads = [sm.SecretPayload(data=fixtures.KEY),
                    sm.SecretPayload(data=fixtures.KEY, data_crc32c=google_crc32c.value(fixtures.KEY) ^ 1),
                    sm.SecretPayload(data=fixtures.KEY, data_crc32c=-1),
                    sm.SecretPayload(data=fixtures.KEY, data_crc32c=2**32),
                    sm.SecretPayload(data=b'x' * len(fixtures.KEY), data_crc32c=google_crc32c.value(fixtures.KEY))]
        for payload in payloads:
            self.fetch.side_effect = None
            self.fetch.return_value = sm.AccessSecretVersionResponse(name=name, payload=payload)
            self.assert_code('SHARING_SECRET_INTEGRITY_FAILED', self.loader.load, self.cfg)

    def test_explicit_zero_checksum_is_present_but_must_match(self):
        self.fetch.side_effect = None
        self.fetch.return_value = sm.AccessSecretVersionResponse(name=self.cfg.name(VERSIONS[0]),
            payload=sm.SecretPayload(data=fixtures.KEY, data_crc32c=0))
        self.assertTrue(sm.SecretPayload.pb(self.fetch.return_value.payload).HasField('data_crc32c'))
        self.assert_code('SHARING_SECRET_INTEGRITY_FAILED', self.loader.load, self.cfg)

    def test_payload_length_is_checked_without_text_decoding_or_trimming(self):
        for data in (b'', b'x' * 31, b'x' * 65, fixtures.KEY + b'x' * 100):
            self.fetch.side_effect = None
            self.fetch.return_value = response(self.cfg.name(VERSIONS[0]), data)
            self.assert_code('SHARING_SECRET_PAYLOAD_INVALID', self.loader.load, self.cfg)
        binary = bytes(range(32))
        self.fetch.side_effect = lambda **kwargs: response(kwargs['request']['name'],
            binary if kwargs['request']['name'] == self.cfg.name(VERSIONS[0]) else fixtures.MATERIAL['new'])
        self.loader.load(self.cfg)

    def test_reused_bytes_across_versions_fail_at_composition(self):
        self.fetch.side_effect = lambda **kwargs: response(kwargs['request']['name'], fixtures.KEY)
        self.assert_code('SHARING_SIGNING_MATERIAL_REUSED', self.loader.load, self.cfg)

    def test_deadline_stops_before_fetch_and_refuses_late_complete_load(self):
        with patch('src.application.sharing_secret_loader.time.monotonic', side_effect=[0, 30]):
            self.assert_code('SHARING_SECRET_LOAD_DEADLINE', self.loader.load, self.cfg)
        self.fetch.assert_not_called()
        with patch('src.application.sharing_secret_loader.time.monotonic', side_effect=[0, 0, 1, 30]):
            self.assert_code('SHARING_SECRET_LOAD_DEADLINE', self.loader.load, self.cfg)
        self.assertEqual(self.fetch.call_count, 2)

    def test_deadline_after_partial_fetch_stops_before_next_key(self):
        with patch('src.application.sharing_secret_loader.time.monotonic', side_effect=[0, 0, 30]):
            self.assert_code('SHARING_SECRET_LOAD_DEADLINE', self.loader.load, self.cfg)
        self.assertEqual(self.fetch.call_count, 1)

    def test_deadline_budget_shortens_last_rpc_and_includes_composition_admission(self):
        with patch('src.application.sharing_secret_loader.time.monotonic', side_effect=[0, 0, 29, 29, 29]):
            self.loader.load(self.cfg)
        self.assertEqual(self.fetch.call_args_list[1].kwargs['timeout'], 1)
        self.fetch.reset_mock()
        with patch('src.application.sharing_secret_loader.time.monotonic', side_effect=[0, 0, 29, 29, 30]):
            self.assert_code('SHARING_SECRET_LOAD_DEADLINE', self.loader.load, self.cfg)
        self.assertEqual(self.fetch.call_count, 2)

    def test_issuance_boundary_crossed_during_fetch_cannot_activate(self):
        with patch('src.application.sharing_secret_loader._now',
                   side_effect=[fixtures.NOW, fixtures.NEW.issue_until]):
            self.assert_code('SHARING_SIGNING_KEY_NOT_ISSUING', self.loader.load, self.cfg)
        self.assertEqual(self.fetch.call_count, 2)

    def test_profile_drift_during_fetch_cannot_activate(self):
        def fetch(**kwargs):
            result = self.good_response(**kwargs)
            if self.fetch.call_count == 2:
                patcher = patch('src.application.sharing_signing_contract._render_sql', return_value='drift')
                patcher.start()
                self.addCleanup(patcher.stop)
            return result
        self.fetch.side_effect = fetch
        self.assert_code('SHARING_SEAL_PROFILE_UNAVAILABLE', self.loader.load, self.cfg)

    def test_safe_error_messages_and_tracebacks_do_not_expose_provider_details_or_bytes(self):
        private = fixtures.KEY.decode()
        self.fetch.side_effect = PermissionDenied(private)
        try:
            self.loader.load(self.cfg)
        except SharingSecretLoadError as error:
            rendered = ''.join(traceback.format_exception(error))
            self.assertNotIn(private, rendered)
            self.assertNotIn(PROJECT_NUMBER, rendered)
            self.assertTrue(error.__suppress_context__)
        else:
            self.fail('Expected safe failure')

    def test_loader_settings_reject_unbounded_and_boolean_timeouts(self):
        for changes in (dict(rpc_timeout_seconds=True), dict(total_timeout_seconds=True),
                        dict(rpc_timeout_seconds=0), dict(rpc_timeout_seconds=21),
                        dict(total_timeout_seconds=61), dict(total_timeout_seconds=4)):
            self.assert_code('INVALID_SHARING_SECRET_LOAD_SETTINGS', SharingSecretLoadSettings, **changes)
        self.assert_code('INVALID_SHARING_SECRET_LOAD_SETTINGS', SharingSecretLoader,
                         client=self.client, settings={})

    def test_factory_uses_adc_and_fixed_endpoint_without_credential_files(self):
        with patch('src.application.sharing_secret_loader.secretmanager_v1.SecretManagerServiceClient') as cls:
            self.assertIs(create_regional_secret_client(), cls.return_value)
            cls.assert_called_once_with(client_options={'api_endpoint': REGIONAL_ENDPOINT})

    def test_factory_credential_failure_is_safe_and_has_no_fallback(self):
        with patch('src.application.sharing_secret_loader.secretmanager_v1.SecretManagerServiceClient',
                   side_effect=RuntimeError('private credential path')) as cls:
            self.assert_code('SHARING_SECRET_CLIENT_UNAVAILABLE', create_regional_secret_client)
            cls.assert_called_once()

    def test_bindings_and_policy_transition_retain_original_versions(self):
        previous = config(policy=fixtures.policy('old'))
        validate_secret_config_transition(previous, self.cfg, changed_at=fixtures.OLD.issue_until)
        proposed = config(policy=fixtures.policy(keys=(replace(fixtures.OLD, revoked=True), fixtures.NEW)))
        validate_secret_config_transition(self.cfg, proposed, changed_at=fixtures.NOW)

    def test_transition_rejects_rebinding_retained_and_revoked_secret_versions(self):
        for old in (fixtures.OLD, replace(fixtures.OLD, revoked=True)):
            previous = config(policy=fixtures.policy(keys=(old, fixtures.NEW)))
            proposed = replace(previous, versions=(replace(VERSIONS[0], version=3), VERSIONS[1]))
            self.assert_code('SHARING_SECRET_VERSION_REBOUND', validate_secret_config_transition,
                             previous, proposed, changed_at=fixtures.NOW)
        proposed = replace(self.cfg, versions=(replace(VERSIONS[0], secret_id='other'), VERSIONS[1]))
        self.assert_code('SHARING_SECRET_VERSION_REBOUND', validate_secret_config_transition,
                         self.cfg, proposed, changed_at=fixtures.NOW)

    def test_transition_rejects_project_number_changes_and_early_key_removal(self):
        self.assert_code('SHARING_SECRET_SCOPE_REBOUND', validate_secret_config_transition,
                         self.cfg, replace(self.cfg, project_number='999'), changed_at=fixtures.NOW)
        self.assert_code('SHARING_SIGNING_KEY_REMOVED_EARLY', validate_secret_config_transition,
                         self.cfg, config(policy=fixtures.policy(keys=(fixtures.NEW,)), versions=VERSIONS[1:]),
                         changed_at=fixtures.NOW)


if __name__ == '__main__':
    unittest.main()
