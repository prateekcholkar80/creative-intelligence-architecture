"""Offline E2 stored-request authority guards; native SDK shapes, no cloud calls."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import json
from unittest import TestCase
from unittest.mock import Mock

from google.cloud import bigquery

from src.application import campaign_quality_request_reader as reader
from src.application.workflow_codec import encode_row
from src.application.workflow_contract import ContractError
from src.validation.control_fixtures import SUBJECT, workflow
from tests.test_workflow_history_service import identity

WS = 'quality-request-workspace'
CONV = 'fixture-conversation'
REQUEST = 'fixture-request'


def capture(stage='PLANNER'):
    rows = workflow(WS, stage=stage)
    return dict(observed_at=datetime.now(timezone.utc),
        **{name: [encode_row(row, name) for row in rows[name]]
           for name in ('workspaces', 'memberships', 'conversations', 'requests')}, request_count=1)


def client(row, mutate=None):
    c = Mock(project=reader.PROJECT, location=reader.LOCATION, default_query_job_config=None)
    def query(sql, **kw):
        resource = dict(jobReference=dict(projectId=reader.PROJECT, location=reader.LOCATION, jobId=kw['job_id']),
            configuration=deepcopy(kw['job_config'].to_api_repr()), status=dict(state='DONE'))
        resource['configuration']['query']['query'] = sql
        # These server-generated fields must not redefine requested settings.
        resource['configuration']['query'].update(priority='INTERACTIVE', destinationTable=dict(
            projectId=reader.PROJECT, datasetId='_anonymous', tableId='query_result'))
        job = bigquery.QueryJob.from_api_repr(resource, c)
        job.result = Mock(return_value=[row])
        if mutate is not None:
            mutate(job, kw)
        c.get_job.return_value = job
        return job
    c.query.side_effect = query
    return c


class QualityRequestReaderTests(TestCase):
    def test_native_required_request_partition_filter_covers_array_and_count(self):
        self.assertEqual(reader.SQL.count(reader.partition_predicate('requests')),2)
        self.assertEqual(reader.partition_predicate('workspaces'),'')
    def setUp(self):
        self.row = capture()
        self.at = self.row['observed_at']
        self.identity = identity()

    def read(self, c=None, **changes):
        return reader.read_request_context(c or client(self.row), **(dict(workspace_id=WS,
            conversation_id=CONV, request_id=REQUEST, identity=self.identity, clock=lambda: self.at) | changes))

    def edit(self, table, **changes):
        row = json.loads(self.row[table][0])
        row.update(changes)
        self.row[table][0] = json.dumps(row)

    def test_complete_capture_derives_immutable_scoped_hashes_without_text(self):
        result = self.read()
        pins = json.loads(result.bindings_canonical_json)
        self.assertEqual(result.bindings_sha256, reader._hash(pins))
        self.assertEqual(pins['history_kind'], 'PERSISTED_REQUEST_RECORDS')
        self.assertEqual(pins['history_count'], 1)
        self.assertEqual(pins['operation'], 'GENERATE')
        self.assertIsNone(pins['message_sha256'])
        self.assertEqual(pins['canonical_sku'], 'CI-SKU-0001')
        self.assertEqual(result.valid_until, self.at + timedelta(seconds=60))
        self.assertNotIn('submitted_preferences', pins)
        with self.assertRaises(FrozenInstanceError):
            result.bindings_sha256 = 'a' * 64

    def test_interpretation_preserves_stored_message_hash_without_inventing_raw_text(self):
        self.row = capture('INTERPRETER'); self.at = self.row['observed_at']
        pins = json.loads(self.read().bindings_canonical_json)
        self.assertEqual(pins['operation'], 'INTERPRET')
        self.assertEqual(len(pins['message_sha256']), 64)
        self.assertIsNone(pins['canonical_sku'])

    def test_native_parameters_limits_no_cache_and_no_provider_replay(self):
        c = client(self.row); self.read(c)
        args, kw = c.query.call_args
        self.assertEqual(args, (reader.SQL,))
        self.assertNotIn(WS, args[0])
        self.assertNotIn(SUBJECT, args[0])
        self.assertIn('BEGIN TRANSACTION', args[0]); self.assertIn('COMMIT TRANSACTION', args[0])
        cfg = kw['job_config']
        self.assertEqual(cfg.maximum_bytes_billed, 256 * 1024**2)
        self.assertFalse(cfg.use_query_cache); self.assertFalse(cfg.create_session)
        self.assertIsNone(kw['retry']); self.assertIsNone(kw['job_retry'])
        c.get_job.assert_called_once_with(kw['job_id'], project=reader.PROJECT,
            location=reader.LOCATION, retry=None, timeout=20)
        result_kw = c.get_job.return_value.result.call_args.kwargs
        self.assertIsNone(result_kw['retry']); self.assertIsNone(result_kw['job_retry'])
        self.assertEqual(result_kw['max_results'], 2)

    def test_bad_scope_or_unverified_identity_prevents_query(self):
        for changes in (dict(request_id='../bad id'), dict(workspace_id=''), dict(identity=Mock())):
            c = client(self.row)
            with self.assertRaises(ContractError): self.read(c, **changes)
            c.query.assert_not_called()
        for attr, value in (('project', 'other'), ('location', 'us'), ('default_query_job_config', object())):
            c = client(self.row); setattr(c, attr, value)
            with self.assertRaises(ContractError): self.read(c)
            c.query.assert_not_called()

    def test_expired_identity_denies_before_query(self):
        c = client(self.row)
        expired = replace(self.identity, issued_at=self.identity.issued_at-660,
            expires_at=self.identity.expires_at-660)
        with self.assertRaises(PermissionError): self.read(c, identity=expired)
        c.query.assert_not_called()

    def test_altered_native_sql_parameters_config_parent_state_or_region_deny(self):
        def sql(job, kw): job._properties['configuration']['query']['query'] = 'SELECT 1'
        def parameters(job, kw): job._properties['configuration']['query']['queryParameters'] = []
        def cap(job, kw): job._properties['configuration']['query']['maximumBytesBilled'] = '1'
        def parent(job, kw): job._properties.setdefault('statistics', {})['parentJobId'] = 'other'
        def region(job, kw): job._properties['jobReference']['location'] = 'us'
        def state(job, kw): job._properties['status']['state'] = 'RUNNING'
        def error(job, kw): job._properties['status']['errorResult'] = dict(reason='invalidQuery')
        for mutate in (sql, parameters, cap, parent, region, state, error):
            with self.subTest(mutate=mutate.__name__), self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_NATIVE_MISMATCH'):
                self.read(client(self.row, mutate))

    def test_provider_config_mutation_cannot_redefine_expected_pin(self):
        def mutate(job, kw):
            kw['job_config'].maximum_bytes_billed = 1
            job._properties['configuration']['query']['maximumBytesBilled'] = '1'
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_NATIVE_MISMATCH'):
            self.read(client(self.row, mutate))

    def test_independent_native_fetch_mismatch_or_failure_has_no_fallback(self):
        c = client(self.row); query = c.query.side_effect
        def mismatch(sql, **kw):
            job = query(sql, **kw)
            c.get_job.return_value = Mock()
            return job
        c.query.side_effect = mismatch
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_NATIVE_MISMATCH'): self.read(c)
        c = client(self.row); c.query.side_effect = RuntimeError('provider unavailable')
        with self.assertRaises(RuntimeError): self.read(c)
        self.assertEqual(c.query.call_count, 1)

    def test_missing_duplicate_count_mismatch_and_overflow_deny(self):
        for name in ('workspaces', 'memberships', 'conversations'):
            original = self.row[name]
            for values in ([], original * 2):
                self.row[name] = values
                with self.assertRaises(ContractError): self.read()
            self.row[name] = original
        for count, values in ((0, []), (2, self.row['requests']), (True, self.row['requests']),
                              (51, self.row['requests'] * 51)):
            row = self.row | dict(request_count=count, requests=values)
            with self.assertRaises(ContractError): self.read(client(row))

    def test_missing_target_and_duplicate_history_request_ids_deny(self):
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_NOT_FOUND'):
            self.read(request_id='missing')
        self.row['requests'] *= 2; self.row['request_count'] = 2
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_HISTORY_ORDER'): self.read()

    def test_private_owner_active_status_role_and_identity_binding_deny(self):
        original = deepcopy(self.row)
        cases = (('conversations', dict(status='ARCHIVED')),
                 ('conversations', dict(owner_id='other-owner', visibility='TEAM')),
                 ('memberships', dict(status='REVOKED')),
                 ('memberships', dict(roles=['ADMIN'])),
                 ('memberships', dict(identity_subject='accounts.google.com:other')),
                 ('workspaces', dict(status='DISABLED')))
        for table, changes in cases:
            self.row = deepcopy(original); self.edit(table, **changes)
            with self.subTest(table=table, changes=changes), self.assertRaises((ContractError, PermissionError)):
                self.read()

    def test_changed_request_body_without_valid_fingerprint_and_cross_scope_deny(self):
        original = deepcopy(self.row)
        for changes in (dict(request_sha256='a'*64), dict(workspace_id='other'),
                        dict(conversation_id='other'), dict(owner_id='other'), dict(canonical_sku='CI-SKU-0021')):
            self.row = deepcopy(original); self.edit('requests', **changes)
            with self.assertRaises(ContractError): self.read()

    def test_stale_future_and_backwards_observations_deny(self):
        for observed in (self.at-timedelta(seconds=1), self.at+timedelta(seconds=1)):
            with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_CAPTURE_EXPIRED'):
                self.read(client(self.row | dict(observed_at=observed)))
        times = iter((self.at, self.at+timedelta(seconds=60)))
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_CAPTURE_EXPIRED'):
            self.read(clock=lambda: next(times))
        self.edit('requests', updated_at=(self.at+timedelta(seconds=1)).isoformat())
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_TIMELINE'): self.read()

    def test_null_oversized_extra_and_malformed_payloads_deny(self):
        for value in (None, 'x'*(reader.MAX_ROW_BYTES+1), '{}', '{"x":1,"x":2}'):
            row = self.row | dict(requests=[value])
            with self.assertRaises(ContractError): self.read(client(row))
        with self.assertRaises(ContractError): self.read(client(self.row | dict(extra='x')))

    def test_request_outcome_revision_and_history_changes_change_bindings(self):
        first = json.loads(self.read().bindings_canonical_json)
        original = json.loads(self.row['requests'][0])
        self.edit('requests', revision=2)
        second = json.loads(self.read().bindings_canonical_json)
        self.assertEqual(first['request_sha256'], second['request_sha256'])
        self.assertNotEqual(first['request_record_sha256'], second['request_record_sha256'])
        self.assertNotEqual(first['history_sha256'], second['history_sha256'])
        older = deepcopy(original)
        older['request_id'] = 'earlier-request'
        self.row['requests'].insert(0, json.dumps(older)); self.row['request_count'] = 2
        third = json.loads(self.read().bindings_canonical_json)
        self.assertEqual(third['request_record_sha256'], second['request_record_sha256'])
        self.assertNotEqual(third['history_sha256'], second['history_sha256'])

    def test_unsorted_history_deny_even_when_each_record_is_valid(self):
        later = json.loads(self.row['requests'][0]); later['request_id'] = 'later-request'
        self.row['requests'].insert(0, json.dumps(later)); self.row['request_count'] = 2
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_HISTORY_ORDER'): self.read()

    def test_independent_result_fetch_cannot_return_two_receipts(self):
        c = client(self.row); submit = c.query.side_effect
        def query(sql, **kw):
            job = submit(sql, **kw)
            job.result.return_value = [self.row, self.row]
            return job
        c.query.side_effect = query
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_NATIVE_MISMATCH'): self.read(c)

    def test_fifty_complete_requests_hash_without_truncation(self):
        request = json.loads(self.row['requests'][0])
        values = []
        for index in range(49):
            values.append(json.dumps(request | dict(request_id=f'earlier-request-{index:02}')))
        self.row['requests'] = values + self.row['requests']
        self.row['request_count'] = 50
        pins = json.loads(self.read().bindings_canonical_json)
        self.assertEqual(pins['history_count'], 50)
        self.row['requests'].append(json.dumps(request | dict(request_id='later-request')))
        self.row['request_count'] = 51
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_HISTORY_INCOMPLETE'): self.read()

    def test_total_capture_size_is_bounded_before_decoding(self):
        row = self.row | dict(request_count=50, requests=['x'*reader.MAX_ROW_BYTES]*50)
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_CAPTURE_SIZE'): self.read(client(row))

    def test_metadata_change_during_result_fetch_withholds_bindings(self):
        c = client(self.row); submit = c.query.side_effect
        def query(sql, **kw):
            job = submit(sql, **kw)
            calls = 0
            def result(**unused):
                nonlocal calls
                calls += 1
                if calls == 2:
                    job._properties['configuration']['query']['queryParameters'] = []
                return [self.row]
            job.result.side_effect = result
            return job
        c.query.side_effect = query
        with self.assertRaisesRegex(ContractError, 'QUALITY_REQUEST_NATIVE_MISMATCH'): self.read(c)

    def test_bindings_omit_request_content_and_identity_subject(self):
        text = self.read().bindings_canonical_json
        self.assertNotIn(SUBJECT, text)
        self.assertNotIn('Synthetic POC fixture', text)
        self.assertNotIn('model_profiles', text)
