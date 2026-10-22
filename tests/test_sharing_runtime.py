"""4.3 offline SDK composition/concurrency; anonymous clients, no live RPCs."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from threading import Event
import unittest
from unittest.mock import Mock, patch

from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, secretmanager_v1 as sm

from src.application.sharing_publication_read_contract import SigningReadRequest
from src.application.sharing_publication_reader import BigQuerySigningReader
from src.application.sharing_refresh_contract import SigningReadTicket, SigningReplicaPhase
from src.application.sharing_runtime import RuntimeSharingSealer, SigningRuntimeError, SigningRuntimeSettings
from src.application.operation_budget import CancellationSignal, OperationSettings, OperationStopped, operation_scope
from src.application.sharing_secret_loader import REGIONAL_ENDPOINT, SharingSecretLoader
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from tests import test_sharing_publication as publications
from tests import test_sharing_secret_loader as secrets
from tests import test_sharing_signing as signing
from tests import test_signing_storage_io as storage
from tests.test_sharing_executor import EPOCH, identity
from tests.test_sharing_query import query
from tests.test_sharing_retry import retry
from tests.test_sharing_coordinator_contract import command


class SigningRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.now = signing.NOW
        self.tick = 100.0
        for module in ('sharing_runtime', 'sharing_publication_reader', 'sharing_secret_loader',
                       'sharing_retained_sealer', 'sharing_intent', 'sharing_retry'):
            p = patch('src.application.' + module + '._now', side_effect=lambda: self.now)
            p.start()
            self.addCleanup(p.stop)
        p = patch('src.application.sharing_runtime.time.monotonic', side_effect=lambda: self.tick)
        p.start()
        self.addCleanup(p.stop)
        p = patch('src.application.identity.time.time', return_value=EPOCH + 10)
        p.start()
        self.addCleanup(p.stop)
        self.client = bigquery.Client(project=PROJECT, location=LOCATION, credentials=AnonymousCredentials())
        self.addCleanup(self.client.close)
        self.client.get_dataset = Mock(return_value=storage.workflow.dataset())
        self.client.get_table = Mock(side_effect=lambda ref, **kw: storage.table(ref.table_id))
        self.client.query = Mock(side_effect=self.native_query)
        self.secret_client = sm.SecretManagerServiceClient(credentials=AnonymousCredentials(),
            client_options={'api_endpoint': REGIONAL_ENDPOINT})
        self.addCleanup(self.secret_client.transport.close)
        self.secret_client.access_secret_version = Mock(side_effect=self.fetch)
        self.reader = BigQuerySigningReader(client=self.client)
        self.loader = SharingSecretLoader(client=self.secret_client)
        self.pubs = (publications.initial(),)
        self.checkpoint = self.pubs[-1]
        self.material = dict(signing.MATERIAL)
        self.requests = []
        self.runtime = self.make_runtime()
        self.old_token = signing.SharingRetrySealer(key=signing.KEY, key_id='old').seal(query())

    def make_runtime(self, **changes):
        return RuntimeSharingSealer(reader=self.reader, loader=self.loader, instance_id='runtime-process-A',
            settings=SigningRuntimeSettings('pilot-runtime', secrets.PROJECT_NUMBER, **(dict(enabled=True) | changes)))

    def test_runtime_operation_configuration_rejects_untyped_settings(self):
        with self.assertRaises(ContractError):self.make_runtime(operation={'timeout_seconds':10})

    def test_metadata_and_secret_rpc_timeouts_accumulate_across_refresh(self):
        self.runtime=self.make_runtime(operation=OperationSettings(10))
        def dataset(*args,**kwargs):self.tick+=1;return storage.workflow.dataset()
        def table(ref,**kwargs):self.tick+=1;return storage.table(ref.table_id)
        query=self.native_query
        def submit(*args,**kwargs):
            response=query(*args,**kwargs);self.tick+=1;return response
        fetch=self.fetch
        def secret(**kwargs):self.tick+=1;return fetch(**kwargs)
        self.client.get_dataset.side_effect=dataset;self.client.get_table.side_effect=table
        self.client.query.side_effect=submit;self.secret_client.access_secret_version.side_effect=secret
        self.runtime.refresh()
        self.assertEqual(self.client.get_dataset.call_args.kwargs['timeout'],10)
        self.assertEqual([c.kwargs['timeout'] for c in self.client.get_table.call_args_list],[9,8,7,6])
        self.assertEqual(self.client.query.call_args.kwargs['timeout'],5)
        self.assertEqual([c.kwargs['timeout'] for c in self.secret_client.access_secret_version.call_args_list],[4,3])

    def test_stop_during_metadata_preflight_prevents_next_lookup_and_query(self):
        self.runtime=self.make_runtime(operation=OperationSettings(1))
        def table(ref,**kwargs):self.tick+=1;return storage.table(ref.table_id)
        self.client.get_table.side_effect=table
        with self.assertRaises(OperationStopped):self.runtime.refresh()
        self.assertEqual(self.client.get_table.call_count,1)
        self.client.query.assert_not_called();self.secret_client.access_secret_version.assert_not_called()

    def test_expired_provider_response_cannot_establish_higher_floor(self):
        self.advance()
        self.runtime=self.make_runtime(operation=OperationSettings(1))
        query=self.native_query
        def delayed(*args,**kwargs):
            response=query(*args,**kwargs);self.tick+=1;return response
        self.client.query.side_effect=delayed
        with self.assertRaises(OperationStopped):self.runtime.refresh()
        self.assertIsNone(self.runtime.state.head)
        self.assertIsNone(self.runtime._sealer)
        self.secret_client.access_secret_version.assert_not_called()

    def test_expiry_after_validated_observation_keeps_higher_floor(self):
        self.advance()
        self.runtime=self.make_runtime(operation=OperationSettings(1))
        read=self.reader.read
        def delayed(request):
            observation=read(request);self.tick+=1;return observation
        with patch.object(self.reader,'read',side_effect=delayed):
            with self.assertRaises(OperationStopped):self.runtime.refresh()
        self.assertEqual(self.runtime.state.head.digest,self.pubs[-1].digest)
        self.secret_client.access_secret_version.assert_not_called()

    def test_slow_loader_denies_install_without_extending_readiness(self):
        self.runtime=self.make_runtime(operation=OperationSettings(1))
        fetch=self.fetch
        def slow(**kwargs):self.tick+=0.6;return fetch(**kwargs)
        self.secret_client.access_secret_version.side_effect=slow
        with self.assertRaises(OperationStopped):self.runtime.refresh()
        self.assertIsNone(self.runtime._sealer)
        self.assertIsNone(self.runtime._monotonic_deadline)
        self.assertEqual(self.runtime.state.phase,SigningReplicaPhase.FAILED)

    def test_late_loader_failure_reports_stop_instead_of_provider_details(self):
        self.runtime=self.make_runtime(operation=OperationSettings(1))
        def fail(**kwargs):self.tick+=1;raise ValueError('private secret response')
        self.secret_client.access_secret_version.side_effect=fail
        with self.assertRaises(OperationStopped) as caught:self.runtime.refresh()
        self.assertEqual(str(caught.exception),'OPERATION_DEADLINE_EXCEEDED')
        self.assertIsNone(self.runtime._sealer)

    def test_direct_sealer_reuses_server_budget_and_cancel_signal(self):
        signal=CancellationSignal()
        fetch=self.fetch
        def cancel(**kwargs):signal.cancel();return fetch(**kwargs)
        self.secret_client.access_secret_version.side_effect=cancel
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),signal):self.runtime.refresh()
        self.assertIsNone(self.runtime._sealer)
        self.assertEqual(self.runtime.state.phase,SigningReplicaPhase.FAILED)

    def test_cancelled_callback_denies_gate_entry_before_body(self):
        from src.application.sharing_intent import initial_attempt
        signal=CancellationSignal()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),signal):
                with self.runtime.attempt_gate(initial_attempt(query()),identity(),authorize=signal.cancel):
                    self.fail('cancelled dispatch admitted')
        self.assertIsNone(self.runtime._sealer)

    def test_stopped_gate_body_never_refreshes_or_returns_success(self):
        from src.application.sharing_intent import initial_attempt
        signal=CancellationSignal()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),signal):
                with self.runtime.attempt_gate(initial_attempt(query()),identity()):signal.cancel()
        self.assertEqual(len(self.requests),1)
        self.assertIsNone(self.runtime._sealer)

    def test_direct_operation_budget_covers_final_access_callback(self):
        from src.application.sharing_intent import initial_attempt
        self.runtime=self.make_runtime(operation=OperationSettings(1))
        with self.assertRaises(OperationStopped):
            self.runtime.check_attempt(initial_attempt(query()),identity(),
                authorize=lambda:setattr(self,'tick',self.tick+1))
        self.assertIsNone(self.runtime._sealer)

    def test_scope_exit_stop_discards_idle_keys_after_lock_release(self):
        signal=CancellationSignal()
        @contextmanager
        def late_stop(settings):
            with operation_scope(settings,signal) as budget:
                yield budget
                signal.cancel()
        with patch('src.application.sharing_runtime.operation_scope',side_effect=late_stop):
            with self.assertRaises(OperationStopped):self.runtime.refresh()
        self.assertIsNone(self.runtime._sealer)
        self.assertEqual(self.runtime.state.phase,SigningReplicaPhase.FAILED)

    def native_query(self, sql, *, job_config, job_id, **kwargs):
        values = {p.name: p.value for p in job_config.query_parameters}
        ticket = SigningReadTicket(values['instance_id'], values['generation'],
                                   values['base_revision'], values['base_digest'] or None)
        request = SigningReadRequest(values['consumer_id'], values['project_number'], ticket,
                                    self.runtime.state.head)
        self.requests.append(request)
        first = ticket.base_revision or (1 if self.checkpoint is None else self.checkpoint.revision)
        pubs = tuple(p for p in self.pubs if p.revision >= first or p == self.checkpoint)
        row = storage.result(pubs, request=request, checkpoint=self.checkpoint)
        row['observed_at'] = self.now
        job = bigquery.QueryJob(job_id, sql, self.client, job_config=job_config)
        job._properties['jobReference'].update(projectId=PROJECT, location=LOCATION)
        job._properties['status'] = {'state': 'DONE'}
        job.result = Mock(return_value=[row])
        return job

    def fetch(self, *, request, **kwargs):
        cfg = self.pubs[-1].config
        binding = next(v for v in cfg.versions if cfg.name(v) == request['name'])
        return secrets.response(request['name'], self.material[binding.key_id])

    def advance(self, **changes):
        proposed = publications.next_publication(self.pubs[-1], **changes)
        self.pubs += (proposed,)
        self.now = proposed.published_at
        self.checkpoint = proposed
        return proposed

    def assert_code(self, code, function, *args):
        with self.assertRaises(SigningRuntimeError) as caught:
            function(*args)
        self.assertEqual(caught.exception.code, code)

    def assert_denied(self, head=None):
        self.assertEqual(self.runtime.state.phase, SigningReplicaPhase.FAILED)
        self.assertIsNone(self.runtime._sealer)
        self.assertIsNone(self.runtime._monotonic_deadline)
        if head is not None:
            self.assertEqual(self.runtime.state.head.digest, head.digest)

    def test_disabled_default_does_no_io_or_key_loading(self):
        self.runtime = self.make_runtime(enabled=False)
        for call in (self.runtime.refresh, lambda: self.runtime.settings,
                     lambda: self.runtime.resolve(self.old_token, identity())):
            self.assert_code('SIGNING_RUNTIME_DISABLED', call)
        self.client.query.assert_not_called()
        self.secret_client.access_secret_version.assert_not_called()
        self.assertFalse(SigningRuntimeSettings('pilot-runtime', secrets.PROJECT_NUMBER).enabled)

    def test_settings_scope_and_adapters_reject_invalid_inputs(self):
        for changes in (dict(enabled=1), dict(refresh={}), dict(project_number=PROJECT), dict(consumer_id='a/b')):
            args = dict(consumer_id='pilot-runtime', project_number=secrets.PROJECT_NUMBER) | changes
            with self.assertRaises(ContractError):
                SigningRuntimeSettings(**args)
        with self.assertRaises(ContractError):
            RuntimeSharingSealer(reader=object(), loader=self.loader, instance_id='instance',
                settings=SigningRuntimeSettings('pilot-runtime', secrets.PROJECT_NUMBER))

    def test_fresh_native_read_and_all_pinned_loads_before_each_call(self):
        original = self.runtime.resolve(self.old_token, identity())
        self.assertEqual(original, signing.initial_attempt(query()))
        self.assertEqual(self.runtime.resolve(self.old_token, identity()), original)
        self.assertEqual(self.client.query.call_count, 2)
        self.assertEqual(self.client.get_table.call_count, 8)
        self.assertEqual(self.secret_client.access_secret_version.call_count, 4)
        self.assertEqual([r.ticket.generation for r in self.requests], [1, 2])
        self.assertEqual(self.requests[1].known_head.digest, self.pubs[0].digest)
        self.assertEqual(self.runtime.state.phase, SigningReplicaPhase.READY)

    def test_settings_property_is_gated_and_active_token_is_compatible(self):
        self.assertEqual(self.runtime.settings, signing.SharingQuerySettings())
        q = query(command=command(accepted_at=self.now))
        token = self.runtime.seal(q)
        expected = signing.SharingIntentSealer(key=self.material['new'], key_id='new').seal(q)
        self.assertEqual(token, expected)
        self.assertEqual(self.runtime.resolve(token, identity()), signing.initial_attempt(q))
        self.assertEqual(self.client.query.call_count, 3)

    def test_nested_retry_preserves_original_cohort_settings_and_job_identity(self):
        original = self.runtime.resolve(self.old_token, identity())
        attempt = retry(original)
        token = self.runtime.seal_retry(attempt)
        self.assertEqual(self.runtime.resolve_retry(token, identity()), attempt)
        nested = retry(attempt, expected_workspace_revision=attempt.query.expected_workspace_revision + 1)
        self.assertEqual(self.runtime.resolve_retry(self.runtime.seal_retry(nested), identity()), nested)
        self.assertEqual(nested.query.command.deadline_at, original.query.command.deadline_at)

    def test_read_failure_drops_previous_keys_without_cached_fallback(self):
        self.runtime.refresh()
        head = self.runtime.state.head
        loads = self.secret_client.access_secret_version.call_count
        self.client.query.side_effect = RuntimeError('private credential/provider details')
        self.assert_code('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED', self.runtime.refresh)
        self.assert_denied(head)
        self.assertEqual(self.secret_client.access_secret_version.call_count, loads)

    def test_partial_secret_failure_preserves_higher_revision_floor(self):
        self.runtime.refresh()
        head = self.advance()
        good = self.fetch(request={'name': head.config.name(head.config.versions[0])})
        self.secret_client.access_secret_version.side_effect = [good, RuntimeError('private key failure')]
        self.assert_code('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED', self.runtime.refresh)
        self.assert_denied(head)
        self.client.query.side_effect = self.native_query
        self.pubs = self.pubs[:1]
        self.checkpoint = self.pubs[0]
        self.assert_code('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED', self.runtime.refresh)
        self.assert_denied(head)

    def test_same_revision_fresh_retry_recovers_after_load_failure(self):
        self.secret_client.access_secret_version.side_effect = RuntimeError('unavailable')
        self.assert_code('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED', self.runtime.refresh)
        self.secret_client.access_secret_version.side_effect = self.fetch
        self.now += timedelta(seconds=1)
        self.assertEqual(self.runtime.refresh().phase, SigningReplicaPhase.READY)
        self.assertEqual(self.runtime.state.generation, 2)

    def test_missing_checkpoint_never_loads_and_retains_observed_head(self):
        self.checkpoint = None
        self.assert_code('SIGNING_RUNTIME_CHECKPOINT_REQUIRED', self.runtime.refresh)
        self.assert_denied(self.pubs[0])
        self.secret_client.access_secret_version.assert_not_called()

    def test_lagging_checkpoint_denies_until_authorized_floor_advances(self):
        self.runtime.refresh()
        old = self.checkpoint
        head = self.advance()
        self.checkpoint = old
        loads = self.secret_client.access_secret_version.call_count
        self.assert_code('SIGNING_RUNTIME_CHECKPOINT_REQUIRED', self.runtime.refresh)
        self.assert_denied(head)
        self.assertEqual(self.secret_client.access_secret_version.call_count, loads)
        self.checkpoint = head
        self.assertEqual(self.runtime.refresh().head.digest, head.digest)

    def test_restart_uses_authoritative_checkpoint_above_genesis(self):
        self.advance()
        head = self.advance()
        self.runtime = self.make_runtime()
        self.assertEqual(self.runtime.refresh().head.digest, head.digest)
        self.assertIsNone(self.requests[0].known_head)
        self.assertEqual(self.runtime.state.generation, 1)

    def test_checkpoint_does_not_grant_readiness_without_secret_bytes(self):
        head = self.advance()
        self.secret_client.access_secret_version.side_effect = RuntimeError('missing bytes')
        self.assert_code('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED', self.runtime.refresh)
        self.assert_denied(head)

    def test_stop_removes_keys_without_loading_stopped_config(self):
        self.runtime.refresh()
        loads = self.secret_client.access_secret_version.call_count
        stopped = self.advance(status=publications.STOPPED)
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.runtime.resolve, self.old_token, identity())
        self.assertEqual(self.runtime.state.phase, SigningReplicaPhase.STOPPED)
        self.assertEqual(self.runtime.state.head.digest, stopped.digest)
        self.assertIsNone(self.runtime._sealer)
        self.assertEqual(self.secret_client.access_secret_version.call_count, loads)

    def test_revocation_blocks_valid_old_signature_and_skips_revoked_bytes(self):
        self.runtime.resolve(self.old_token, identity())
        parent = self.pubs[-1]
        cfg = replace(parent.config, policy=replace(parent.config.policy,
            keys=(replace(signing.OLD, revoked=True), signing.NEW)))
        head = self.advance(config=cfg)
        self.secret_client.access_secret_version.reset_mock()
        self.assert_code('SHARING_SIGNING_KEY_REVOKED', self.runtime.resolve, self.old_token, identity())
        self.assert_denied(head)
        self.assertEqual(self.secret_client.access_secret_version.call_count, 1)
        self.assertEqual(self.secret_client.access_secret_version.call_args.kwargs['request']['name'],
                         cfg.name(secrets.VERSIONS[1]))

    def test_planned_rotation_keeps_old_retry_tokens_and_uses_new_initial_key(self):
        cfg, third = publications.scheduled_config()
        self.material['third'] = b'third-offline-key-material-32bytes!'
        self.pubs = (publications.initial(cfg),)
        self.checkpoint = self.pubs[0]
        original = self.runtime.resolve(self.old_token, identity())
        # Old original intent expires before third's cohort; rotation verification
        # uses a still-valid second-cohort intent instead.
        q = query(command=command(accepted_at=self.now))
        token = self.runtime.seal(q)
        new_cfg = replace(cfg, policy=replace(cfg.policy, active_key_id='third'))
        self.advance(config=new_cfg, published_at=third.issue_from)
        second_original = self.runtime.resolve(token, identity())
        attempt = retry(second_original)
        self.assertEqual(self.runtime.resolve_retry(self.runtime.seal_retry(attempt), identity()), attempt)
        current = query(command=command(accepted_at=self.now))
        expected = signing.SharingIntentSealer(key=self.material['third'], key_id='third').seal(current)
        self.assertEqual(self.runtime.seal(current), expected)
        self.assertEqual(original.query.settings, second_original.query.settings)

    def test_expired_load_cannot_install_or_extend_lease(self):
        original = self.fetch
        def slow(**kwargs):
            response = original(**kwargs)
            self.now += timedelta(seconds=15)
            return response
        self.secret_client.access_secret_version.side_effect = slow
        self.assert_code('SIGNING_REFRESH_EXPIRED', self.runtime.refresh)
        self.assert_denied(self.pubs[0])

    def test_monotonic_expiry_cannot_be_hidden_by_backward_wall_clock(self):
        original = self.loader.load
        def slow(config):
            loaded = original(config)
            self.now += timedelta(seconds=30)
            self.tick += 30
            self.now -= timedelta(seconds=30)  # Wall time conceals elapsed time.
            return loaded
        with patch.object(self.loader, 'load', side_effect=slow):
            self.assert_code('SIGNING_REFRESH_EXPIRED', self.runtime.refresh)
        self.assert_denied(self.pubs[0])

    def test_result_not_released_if_sealing_crosses_lease(self):
        actual = signing.RetainedSharingSealer.resolve
        def slow(sealer, token, verified):
            result = actual(sealer, token, verified)
            self.tick += 30
            return result
        with patch.object(signing.RetainedSharingSealer, 'resolve', slow):
            self.assert_code('SIGNING_REFRESH_EXPIRED', self.runtime.resolve, self.old_token, identity())
        self.assert_denied(self.pubs[0])

    def test_wrong_loaded_policy_is_rejected_before_installation(self):
        wrong = signing.RetainedSharingSealer(policy=signing.policy('old'), key_material=self.material)
        with patch.object(self.loader, 'load', return_value=wrong):
            self.assert_code('SIGNING_LOADED_CONFIG_MISMATCH', self.runtime.refresh)
        self.assert_denied(self.pubs[0])

    def test_installed_profile_drift_denies_before_key_access(self):
        with patch('src.application.sharing_signing_contract._render_sql', return_value='changed SQL'):
            self.assert_code('SHARING_SEAL_PROFILE_UNAVAILABLE', self.runtime.refresh)
        self.secret_client.access_secret_version.assert_not_called()
        self.assert_denied()

    def test_provider_errors_do_not_expose_tokens_or_secret_values(self):
        private = self.old_token + signing.KEY.decode()
        self.secret_client.access_secret_version.side_effect = RuntimeError(private)
        with self.assertRaises(SigningRuntimeError) as caught:
            self.runtime.refresh()
        self.assertNotIn(private, str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)
        self.assertNotIn(signing.KEY.decode(), repr(self.runtime.state))

    def test_stopped_policy_cannot_be_replaced_by_older_authority(self):
        self.runtime.refresh()
        stopped = self.advance(status=publications.STOPPED)
        self.assert_code('SIGNING_REPLICA_NOT_READY', self.runtime.refresh)
        self.pubs = self.pubs[:1]
        self.checkpoint = self.pubs[0]
        self.assert_code('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED', self.runtime.refresh)
        self.assert_denied(stopped)

    def test_new_instance_id_is_bound_into_every_authoritative_read(self):
        self.runtime = RuntimeSharingSealer(reader=self.reader, loader=self.loader, instance_id='restart-process-B',
            settings=SigningRuntimeSettings('pilot-runtime', secrets.PROJECT_NUMBER, enabled=True))
        self.runtime.refresh()
        self.assertEqual(self.requests[0].ticket.instance_id, 'restart-process-B')
        self.assertEqual(self.runtime.state.instance_id, 'restart-process-B')

    def test_query_profile_change_between_settings_and_seal_fails_closed(self):
        original_settings = self.runtime.settings
        changed = signing.SharingQuerySettings(maximum_bytes_billed=1024)
        cfg, third = publications.scheduled_config()
        self.material['third'] = b'third-offline-key-material-32bytes!'
        third = replace(third, profile_id='limited')
        cfg = replace(cfg, policy=replace(cfg.policy, active_key_id='third',
            keys=(signing.OLD, signing.NEW, third),
            profiles=cfg.policy.profiles + (signing.profile('limited', changed),)))
        # Pin the future profile at genesis in this independent fixture.
        initial_cfg = replace(cfg, policy=replace(cfg.policy, active_key_id='new'))
        self.pubs = (publications.initial(initial_cfg),)
        self.checkpoint = self.pubs[0]
        self.runtime = self.make_runtime()
        self.runtime.refresh()
        self.advance(config=cfg, published_at=third.issue_from)
        stale = query(command=command(accepted_at=self.now), settings=original_settings)
        self.assert_code('SHARING_SETTINGS_MISMATCH', self.runtime.seal, stale)
        self.assert_denied(self.pubs[-1])

    def test_invalid_token_does_not_return_attempt_or_leave_old_keys(self):
        self.assert_code('INVALID_SHARING_SEAL', self.runtime.resolve, 'bad-token', identity())
        self.assert_denied(self.pubs[0])

    def test_close_drops_keys_and_permanently_disables_future_work(self):
        self.runtime.refresh()
        self.runtime.close()
        reads = self.client.query.call_count
        self.assert_code('SIGNING_RUNTIME_DISABLED', self.runtime.refresh)
        self.assertEqual(self.client.query.call_count, reads)
        self.assert_denied(self.pubs[0])
        self.runtime.close()

    def test_close_cold_instance_and_status_do_not_trigger_io(self):
        self.assertEqual(self.runtime.state.phase, SigningReplicaPhase.COLD)
        self.runtime.close()
        self.assert_denied()
        self.client.query.assert_not_called()

    def test_reentrant_callback_cannot_replace_outer_generation(self):
        self.secret_client.access_secret_version.side_effect = lambda **kwargs: self.runtime.refresh()
        self.assert_code('SIGNING_RUNTIME_REFRESH_OR_OPERATION_FAILED', self.runtime.refresh)
        self.assert_denied(self.pubs[0])
        self.assertEqual(self.runtime.state.generation, 1)

    def test_parallel_calls_serialize_load_swap_and_use_new_head(self):
        entered, release, queued = Event(), Event(), Event()
        actual = self.fetch
        def blocked(**kwargs):
            if not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise RuntimeError('test synchronization timeout')
            return actual(**kwargs)
        self.secret_client.access_secret_version.side_effect = blocked
        actual_query = self.native_query
        def newer_on_second_query(*args, **kwargs):
            if self.client.query.call_count == 2:
                self.advance()
            return actual_query(*args, **kwargs)
        self.client.query.side_effect = newer_on_second_query
        def second():
            queued.set()
            return self.runtime.refresh()
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.runtime.refresh)
            self.assertTrue(entered.wait(5))
            second_call = pool.submit(second)
            self.assertTrue(queued.wait(5))
            self.assertEqual(self.client.query.call_count, 1)
            release.set()
            first_state = first.result(timeout=5)
            second_state = second_call.result(timeout=5)
        self.assertEqual((first_state.generation, second_state.generation), (1, 2))
        self.assertEqual((first_state.head.revision, second_state.head.revision), (1, 2))
        self.assertEqual(self.runtime.state, second_state)
        self.assertEqual(self.secret_client.access_secret_version.call_count, 4)


if __name__ == '__main__':
    unittest.main()
