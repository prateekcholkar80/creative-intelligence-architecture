"""4.2 affected offline setup/codec/SDK-reader checks; no cloud credentials or RPCs."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Conflict, Forbidden, NotFound, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from src.application import sharing_publication_codec as codec
from src.application import sharing_publication_reader as reader
from src.application.sharing_publication_contract import POLICY_ID, SigningPublicationStatus
from src.application.sharing_publication_read_contract import MAX_READ_HISTORY, SigningReadLimits
from src.application.sharing_publication_schema import STORAGE_VERSION, TABLES
from src.application.workflow_contract import ContractError
from src.ingestion import setup_signing_tables as setup
from src.ingestion.setup_workflow_tables import DATASET_ID, LOCATION, PROJECT, SchemaMismatch
from tests import test_sharing_publication_storage as fixtures
from tests import test_setup_workflow_tables as workflow


def table(name):
    value = TABLES[name].table(PROJECT)
    value._properties.update(type='TABLE', location=LOCATION, numRows='0')
    return value


def canonical(pubs):
    return tuple(replace(pub, config=codec.decode_config(codec.encode_config(pub.config))) for pub in pubs)


def result(pubs=None, *, request=None, checkpoint=None):
    pubs = fixtures.sequence() if pubs is None else pubs
    request = fixtures.request() if request is None else request
    head = pubs[-1]
    base = dict(schema_version=STORAGE_VERSION, policy_id=POLICY_ID)
    publications, history = [], []
    for pub in pubs:
        row, records = codec.encode_publication(pub, publisher_id='operator-test', command_id='command-test',
                                                recorded_at=pub.published_at)
        publications.append(row)
        history.extend(records)
    checkpoints = [] if checkpoint is None else [base | dict(consumer_id=request.consumer_id,
        checkpoint_revision=1, publication_revision=checkpoint.revision, publication_digest=checkpoint.digest,
        instance_id='process-test', updated_at=head.published_at, writer_id='runtime-test', command_id='floor-test')]
    ticket = request.ticket
    return dict(observed_at=head.published_at, consumer_id=request.consumer_id,
        project_number=request.project_number, instance_id=ticket.instance_id, generation=ticket.generation,
        base_revision=ticket.base_revision, base_digest=ticket.base_digest,
        head_count=1, checkpoint_count=len(checkpoints), publication_count=len(publications),
        history_count=len(history), unheaded_publication_count=0,
        heads=[base | dict(revision=head.revision, control_revision=1, publication_digest=head.digest,
                          published_at=head.published_at, publisher_id='operator-test', command_id='command-test')],
        checkpoints=checkpoints, publications=publications, history=history)


class SetupTests(unittest.TestCase):
    def client(self, tables=None):
        value = Mock(project=PROJECT)
        value.get_dataset.return_value = workflow.dataset()
        resources = {} if tables is None else tables
        def fetch(reference, **kwargs):
            self.assertEqual(reference.project, PROJECT)
            self.assertEqual(reference.dataset_id, 'ci_application')
            self.assertIn(reference.table_id, TABLES)
            if reference.table_id not in resources:
                raise NotFound('missing')
            return resources[reference.table_id]
        def create(resource, **kwargs):
            resources[resource.table_id] = table(resource.table_id)
        value.get_table.side_effect = fetch
        value.create_table.side_effect = create
        return value

    def test_read_only_missing_tables_and_no_dataset_creation(self):
        client = self.client()
        actual = setup.setup(client)
        self.assertEqual(actual['missing_tables'], list(TABLES))
        self.assertEqual(actual['created_tables'], [])
        client.create_table.assert_not_called()
        client.create_dataset.assert_not_called()

    def test_apply_then_idempotent_rerun(self):
        client = self.client()
        self.assertEqual(setup.setup(client, apply=True)['created_tables'], list(TABLES))
        self.assertEqual(setup.setup(client, apply=True)['created_tables'], [])
        self.assertEqual(client.create_table.call_count, 4)
        client.query.assert_not_called()
        client.insert_rows.assert_not_called()
        client.update_table.assert_not_called()
        client.delete_table.assert_not_called()
        for call in client.create_table.call_args_list:
            self.assertIsNone(call.kwargs['retry'])
            self.assertEqual(call.kwargs['timeout'], 20)

    def test_missing_dataset_requires_existing_workflow_setup(self):
        client = self.client()
        client.get_dataset.side_effect = NotFound('missing dataset')
        with self.assertRaises(NotFound):
            setup.setup(client, apply=True)
        client.create_dataset.assert_not_called()
        client.create_table.assert_not_called()

    def test_preflight_later_incompatible_resource_before_any_creation(self):
        wrong = table('signing_checkpoints')
        wrong.clustering_fields = ['consumer_id']
        client = self.client({'signing_checkpoints': wrong})
        with self.assertRaises(SchemaMismatch):
            setup.setup(client, apply=True)
        client.create_table.assert_not_called()

    def test_existing_resources_and_row_counts_are_preserved(self):
        existing = table('signing_heads')
        existing.description = 'existing operator note'
        existing._properties['numRows'] = '7'
        client = self.client({'signing_heads': existing})
        actual = setup.setup(client, apply=True)
        self.assertNotIn('signing_heads', actual['created_tables'])
        self.assertEqual(actual['table_row_counts']['signing_heads'], 7)
        self.assertEqual(existing.description, 'existing operator note')

    def test_concurrent_creation_is_revalidated(self):
        client = self.client()
        create = client.create_table.side_effect
        def concurrent(resource, **kwargs):
            create(resource, **kwargs)
            raise Conflict('already created')
        client.create_table.side_effect = concurrent
        self.assertEqual(setup.setup(client, apply=True)['status'], 'VERIFIED')

    def test_partial_setup_resumes_and_does_not_recreate(self):
        client = self.client()
        create = client.create_table.side_effect
        def interrupted(resource, **kwargs):
            if resource.table_id == 'signing_key_history':
                raise ServiceUnavailable('fixture outage')
            create(resource, **kwargs)
        client.create_table.side_effect = interrupted
        with self.assertRaises(ServiceUnavailable):
            setup.setup(client, apply=True)
        client.create_table.side_effect = create
        self.assertEqual(setup.setup(client, apply=True)['created_tables'],
                         ['signing_key_history', 'signing_checkpoints'])

    def test_wrong_project_expiry_region_denial_and_nonboolean_apply(self):
        for mutation in ('project', 'expiry', 'region', 'denial', 'apply'):
            with self.subTest(mutation=mutation):
                client = self.client()
                apply = True
                if mutation == 'project':
                    client.project = 'foreign-project'
                if mutation == 'expiry':
                    client.get_dataset.return_value.default_table_expiration_ms = 1000
                if mutation == 'region':
                    client.get_dataset.return_value.location = 'US'
                if mutation == 'denial':
                    client.get_dataset.side_effect = Forbidden('denied')
                if mutation == 'apply':
                    apply = 'yes'
                with self.assertRaises((ValueError, Forbidden)):
                    setup.setup(client, apply=apply)
                client.create_table.assert_not_called()


class CodecTests(unittest.TestCase):
    def check(self, row, request=None):
        return codec.decode_read_result(fixtures.request() if request is None else request,
                                        [row], now=row['observed_at'])

    def test_round_trip_canonical_config_and_full_publication(self):
        row = result()
        observation = self.check(row)
        self.assertEqual(observation.publications, canonical(fixtures.sequence()))
        self.assertEqual(observation.head_control_revision, 1)
        self.assertIsNone(observation.checkpoint_anchor)

    def test_cold_checkpoint_and_running_known_chain(self):
        pubs = fixtures.sequence(5)
        req = fixtures.request(pubs[3])
        row = result((pubs[1], pubs[3], pubs[4]), request=req, checkpoint=pubs[1])
        self.assertEqual(self.check(row, req).publications, canonical(pubs[3:]))
        cold = result(pubs[1:], checkpoint=pubs[1])
        self.assertEqual(self.check(cold).checkpoint_anchor, canonical(pubs)[1])

    def test_valid_stopped_head_preserves_revocation(self):
        first = fixtures.sequence(1)[0]
        stopped = fixtures.fixtures.next_publication(first, status=SigningPublicationStatus.STOPPED)
        observation = self.check(result((first, stopped)))
        self.assertEqual(observation.publications[-1], canonical((stopped,))[0])
        self.assertTrue(next(h for h in stopped.history if h.key.key_id == stopped.config.policy.active_key_id).key.revoked)

    def test_config_rejects_noncanonical_duplicates_unknown_fields_and_constants(self):
        raw = result()['publications'][0]['config_canonical_json']
        data = json.loads(raw)
        bad = [json.dumps(data), raw + '\n', raw.replace('{', '{"active_key_id":"old",', 1),
               codec.canonical_json_bytes(data | {'extra': 'x'}).decode(), raw.replace('false', 'NaN', 1),
               'null', '[]', '[' * 1200 + ']' * 1200, 'x' * 65537, '\ud800']
        for value in bad:
            with self.subTest(raw=value[:40]), self.assertRaises(ContractError):
                codec.decode_config(value)

    def test_config_rejects_scalar_coercions_and_unsorted_lists(self):
        raw = result()['publications'][0]['config_canonical_json']
        data = json.loads(raw)
        candidates = []
        for version in (True, '1', 1.0, 0, 2**63):
            other = deepcopy(data)
            other['versions'][0]['version'] = version
            candidates.append(other)
        other = deepcopy(data)
        other['keys'].reverse()
        candidates.append(other)
        other = deepcopy(data)
        other['profiles'][0]['settings']['job_timeout_ms'] = True
        candidates.append(other)
        for candidate in candidates:
            with self.assertRaises(ContractError):
                codec.decode_config(codec.canonical_json_bytes(candidate).decode())

    def test_config_rejects_noncanonical_utc_and_invalid_schedule(self):
        data = json.loads(result()['publications'][0]['config_canonical_json'])
        for value in ('2026-01-01T00:00:00', data['keys'][0]['issue_from'].replace('+00:00', 'Z'),
                      'not-a-date', 3):
            other = deepcopy(data)
            other['keys'][0]['issue_from'] = value
            with self.assertRaises(ContractError):
                codec.decode_config(codec.canonical_json_bytes(other).decode())

    def test_exact_storage_shapes_and_schema_types(self):
        for group, change in (
                ('heads', {'revision': True}), ('heads', {'control_revision': 1.0}),
                ('heads', {'policy_id': 'foreign'}), ('heads', {'publisher_id': 'email@example.com'}),
                ('publications', {'parent_digest': 1}), ('publications', {'schema_version': 'future'}),
                ('publications', {'config_canonical_json': None}), ('publications', {'extra': 'x'}),
                ('history', {'revoked': 0}), ('history', {'secret_version': '1'}),
                ('history', {'issue_from': '2026-01-01'})):
            with self.subTest(group=group, change=change):
                row = result()
                row[group][0].update(change)
                with self.assertRaises(ContractError):
                    self.check(row)

    def test_publication_hash_config_hash_and_history_count_mismatch(self):
        for change in ({'publication_digest': 'a' * 64}, {'config_digest': 'b' * 64}, {'history_count': 1},
                       {'parent_digest': 'a' * 64}, {'status': 'UNKNOWN'}, {'publication_version': 'future'}):
            row = result()
            row['publications'][-1].update(change)
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.check(row)

    def test_timestamp_provenance_cannot_be_backdated_or_future(self):
        for value in (fixtures.NOW - timedelta(days=1), fixtures.NOW + timedelta(days=1),
                      fixtures.NOW.replace(tzinfo=None)):
            row = result()
            row['publications'][-1]['recorded_at'] = value
            with self.assertRaises(ContractError):
                self.check(row)

    def test_complete_counts_detect_duplicates_truncation_and_overflow_before_decoding(self):
        for name, value in (('head_count', 2), ('checkpoint_count', 2), ('publication_count', 18),
                            ('history_count', MAX_READ_HISTORY + 1), ('history_count', True)):
            row = result()
            row[name] = value
            with patch.object(codec, 'decode_config', side_effect=AssertionError('must reject counts first')):
                with self.assertRaises(ContractError):
                    self.check(row)

    def test_duplicate_missing_or_changed_history_cannot_match_declared_digest(self):
        for mode in ('duplicate', 'missing', 'changed', 'orphan'):
            row = result()
            if mode == 'duplicate':
                row['history'].append(deepcopy(row['history'][0]))
                row['history_count'] += 1
                row['publications'][0]['history_count'] += 1
            if mode == 'missing':
                row['history'].pop()
                row['history_count'] -= 1
            if mode == 'changed':
                row['history'][0]['secret_version'] += 1
            if mode == 'orphan':
                row['history'][0]['publication_revision'] = 99
            with self.subTest(mode=mode), self.assertRaises(ContractError):
                self.check(row)

    def test_duplicate_publication_and_missing_transition_fail(self):
        for mode in ('duplicate', 'missing'):
            row = result()
            if mode == 'duplicate':
                row['publications'].append(deepcopy(row['publications'][0]))
            else:
                row['publications'].pop(1)
                row['history'] = [h for h in row['history'] if h['publication_revision'] != 2]
                row['history_count'] = len(row['history'])
            row['publication_count'] = len(row['publications'])
            with self.assertRaises(ContractError):
                self.check(row)

    def test_ticket_scope_and_project_mismatch_fail(self):
        for key, value in (('generation', True), ('generation', 2), ('instance_id', 'foreign'),
                            ('consumer_id', 'foreign'), ('base_digest', 'a' * 64),
                            ('project_number', '987654321')):
            row = result()
            row[key] = value
            with self.assertRaises(ContractError):
                self.check(row)

    def test_unheaded_and_stale_snapshots_fail(self):
        row = result()
        row['unheaded_publication_count'] = 1
        with self.assertRaises(ContractError):
            self.check(row)
        row = result()
        for now in (row['observed_at'] - timedelta(seconds=1), row['observed_at'] + timedelta(seconds=30)):
            with self.assertRaises(ContractError):
                codec.decode_read_result(fixtures.request(), [row], now=now)

    def test_native_nested_sdk_rows_are_decoded(self):
        def native(value):
            if type(value) is dict:
                return bigquery.Row(tuple(native(v) for v in value.values()),
                                    {k: n for n, k in enumerate(value)})
            if type(value) is list:
                return [native(v) for v in value]
            return value
        row = result()
        self.assertEqual(codec.normalize(native(row)), row)
        self.assertEqual(self.check(codec.normalize(native(row))).publications, canonical(fixtures.sequence()))


class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.client = bigquery.Client(project=PROJECT, location=LOCATION, credentials=AnonymousCredentials())
        self.addCleanup(self.client.close)
        self.client.get_dataset = Mock(return_value=workflow.dataset())
        self.client.get_table = Mock(side_effect=lambda ref, **kw: table(ref.table_id))
        self.row = result()
        self.job = None
        self.client.query = Mock(side_effect=self.query)
        self.clock = patch.object(reader, '_now', return_value=self.row['observed_at'])
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.reader = reader.BigQuerySigningReader(client=self.client)

    def query(self, sql, *, job_config, job_id, **kwargs):
        self.job = bigquery.QueryJob(job_id, sql, self.client, job_config=job_config)
        self.job._properties['jobReference'].update(projectId=PROJECT, location=LOCATION)
        self.job._properties['status'] = {'state': 'DONE'}
        self.job.result = Mock(return_value=[self.row])
        return self.job

    def test_fixed_scoped_fresh_job_and_disabled_retries(self):
        observed = self.reader.read(fixtures.request())
        self.assertEqual(observed.publications, canonical(fixtures.sequence()))
        call = self.client.query.call_args
        self.assertIsNone(call.kwargs['retry'])
        self.assertIsNone(call.kwargs['job_retry'])
        self.assertEqual(call.kwargs['location'], LOCATION)
        self.assertEqual(call.kwargs['timeout'], 20)
        self.assertEqual(self.client.get_table.call_count, 4)
        self.assertEqual(self.job.result.call_args.kwargs,
                         dict(max_results=2, page_size=2, retry=None, job_retry=None, timeout=20))
        first_id = call.kwargs['job_id']
        self.reader.read(fixtures.request())
        self.assertNotEqual(self.client.query.call_args.kwargs['job_id'], first_id)
        self.assertEqual(self.client.get_table.call_count, 8)

    def test_sql_keeps_full_counts_versions_and_bounded_projections(self):
        sql = reader.read_sql()
        self.assertTrue(sql.startswith('BEGIN TRANSACTION;'))
        self.assertIn('COMMIT TRANSACTION;', sql)
        self.assertIn('CURRENT_TIMESTAMP()', sql)
        self.assertNotIn('DISTINCT', sql)
        self.assertNotIn('schema_version=', sql)
        self.assertNotIn('UPDATE ', sql)
        self.assertNotIn('DELETE ', sql)
        self.assertNotIn('INSERT ', sql)
        for name in TABLES:
            self.assertIn(f'`{DATASET_ID}.{name}`', sql)
        config = reader.read_config(fixtures.request(), SigningReadLimits()).to_api_repr()
        self.assertFalse(config['query']['useQueryCache'])
        self.assertFalse(config['query']['createSession'])
        self.assertEqual(config['query']['maximumBytesBilled'], str(64 * 1024**2))
        self.assertIn("NULLIF(@base_digest, '') AS base_digest", sql)
        digest_parameter = next(p for p in config['query']['queryParameters'] if p['name'] == 'base_digest')
        self.assertEqual(digest_parameter['parameterValue'], {'value': ''})

    def test_schema_or_permission_failure_prevents_query_and_is_safe(self):
        for failure in (Forbidden('credential must not leak'), ServiceUnavailable('provider details')):
            self.client.get_table.side_effect = failure
            with self.assertRaises(reader.SigningReadError) as caught:
                self.reader.read(fixtures.request())
            self.assertEqual(caught.exception.code, 'SIGNING_READ_STORE_UNAVAILABLE')
            self.assertNotIn('credential', str(caught.exception))
        self.client.query.assert_not_called()

    def test_missing_resource_prevents_query(self):
        self.client.get_table.side_effect = NotFound('missing')
        with self.assertRaises(reader.SigningReadError) as caught:
            self.reader.read(fixtures.request())
        self.assertEqual(caught.exception.code, 'SIGNING_READ_RESOURCES_MISSING')
        self.client.query.assert_not_called()

    def test_wrong_client_or_defaults_rejected(self):
        foreign = bigquery.Client(project='foreign', location=LOCATION, credentials=AnonymousCredentials())
        self.addCleanup(foreign.close)
        with self.assertRaises(ContractError):
            reader.BigQuerySigningReader(client=foreign)
        self.client.default_query_job_config = bigquery.QueryJobConfig(destination=f'{DATASET_ID}.signing_heads')
        with self.assertRaises(reader.SigningReadError):
            self.reader.read(fixtures.request())
        self.client.query.assert_not_called()

    def test_wrong_native_job_id_query_parameters_region_and_parent_fail(self):
        good_query = self.query
        for mode in ('job_id', 'sql', 'parameters', 'location', 'parent', 'fake'):
            def changed(sql, **kwargs):
                job = good_query(sql, **kwargs)
                if mode == 'job_id':
                    job._properties['jobReference']['jobId'] = 'foreign'
                if mode == 'sql':
                    job._properties['configuration']['query']['query'] = 'SELECT 1'
                if mode == 'parameters':
                    job._properties['configuration']['query']['queryParameters'][0]['parameterValue']['value'] = 'other'
                if mode == 'location':
                    job._properties['jobReference']['location'] = 'US'
                if mode == 'parent':
                    job._properties['statistics'] = {'parentJobId': 'foreign'}
                return Mock() if mode == 'fake' else job
            self.client.query.side_effect = changed
            with self.subTest(mode=mode), self.assertRaises(reader.SigningReadError) as caught:
                self.reader.read(fixtures.request())
            self.assertEqual(caught.exception.code, 'SIGNING_READ_JOB_MISMATCH')

    def test_no_fallback_or_resubmission_after_submission_failure(self):
        self.client.query.side_effect = ServiceUnavailable('lost response')
        with self.assertRaises(reader.SigningReadError):
            self.reader.read(fixtures.request())
        self.assertEqual(self.client.query.call_count, 1)

    def test_terminal_error_timeout_multiple_rows_and_stale_completion_fail(self):
        for mode in ('error', 'running', 'timeout', 'rows', 'stale'):
            def changed(sql, **kwargs):
                job = self.query(sql, **kwargs)
                if mode == 'error':
                    job._properties['status']['errorResult'] = {'reason': 'failure'}
                if mode == 'running':
                    job._properties['status']['state'] = 'RUNNING'
                if mode == 'timeout':
                    job.result.side_effect = TimeoutError('timeout')
                if mode == 'rows':
                    job.result.return_value = [self.row, self.row]
                return job
            self.client.query.side_effect = changed
            if mode == 'stale':
                reader._now.return_value = self.row['observed_at'] + timedelta(seconds=30)
            with self.subTest(mode=mode), self.assertRaises(reader.SigningReadError):
                self.reader.read(fixtures.request())


if __name__ == '__main__':
    unittest.main()
