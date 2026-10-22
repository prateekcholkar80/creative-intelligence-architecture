"""4.1 schema/read-design checks; no cloud, credentials, decoder or SQL calls."""
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import json
import unittest
from unittest.mock import patch

from src.application import sharing_publication_schema as schema
from src.application.sharing_publication_read_contract import (
    MAX_READ_HISTORY, MAX_READ_PUBLICATIONS, SigningCheckpointRecord, SigningHeadRecord,
    SigningHistoryRecord, SigningReadLimits, SigningReadRequest, SigningReadSnapshot,
    validate_publication_read,
)
from src.application.sharing_publication_contract import SigningPublicationStatus
from src.application.sharing_refresh_contract import SigningReadTicket
from src.application.workflow_contract import ContractError
from src.application import workflow_schema
from tests import test_sharing_publication as fixtures
from tests import test_sharing_secret_loader as secrets

NOW = fixtures.NOW


def sequence(length=3):
    first = fixtures.initial()
    pubs = [first]
    for _ in range(length - 1):
        pubs.append(fixtures.next_publication(pubs[-1]))
    return tuple(pubs)


def request(known=None):
    return SigningReadRequest('pilot-runtime', secrets.PROJECT_NUMBER,
        SigningReadTicket('process-epoch', 1, 0 if known is None else known.revision,
                          None if known is None else known.digest), known)


def checkpoint(publication):
    return SigningCheckpointRecord('pilot-runtime', 1, publication.revision,
                                   publication.digest, publication.published_at)


def snapshot(pubs=None, *, req=None, checkpoints=()):
    pubs = sequence() if pubs is None else pubs
    req = request() if req is None else req
    head = max(pubs, key=lambda p: p.revision)
    history = tuple(SigningHistoryRecord(p.revision, p.digest, record) for p in pubs for record in p.history)
    return SigningReadSnapshot(req.consumer_id, req.ticket, head.published_at,
        (SigningHeadRecord(head.revision, head.digest, head.published_at, 1),), checkpoints,
        pubs, history, 1, len(checkpoints), len(pubs), len(history), 0)


class PublicationStorageSchemaTests(unittest.TestCase):
    def columns(self, name):
        return {f.name: f for f in schema.TABLES[name].schema}

    def test_four_control_tables_are_separate_from_existing_eleven(self):
        expected = {
            'signing_publications': ('policy_id', 'revision'),
            'signing_heads': ('policy_id',),
            'signing_key_history': ('policy_id', 'publication_revision', 'key_id'),
            'signing_checkpoints': ('policy_id', 'consumer_id'),
        }
        self.assertEqual(set(schema.TABLES), set(expected))
        self.assertEqual(len(workflow_schema.TABLES), 11)
        self.assertFalse(set(schema.TABLES) & set(workflow_schema.TABLES))
        for name, key in expected.items():
            self.assertEqual(schema.TABLES[name].logical_key, key)
            for column in key:
                self.assertEqual(self.columns(name)[column].mode, 'REQUIRED')

    def test_every_control_is_versioned_policy_scoped_without_business_provenance(self):
        for name, spec in schema.TABLES.items():
            columns = self.columns(name)
            self.assertEqual(columns['schema_version'].mode, 'REQUIRED')
            self.assertEqual(columns['policy_id'].mode, 'REQUIRED')
            self.assertEqual(spec.clustering[0], 'policy_id')
            self.assertNotIn('workspace_id', columns)  # Service control, not app membership.
            self.assertNotIn('source_system', columns)  # Actual control state, not synthetic sales.

    def test_revision_and_version_precision_and_parent_nullability_are_explicit(self):
        for table, names in {
            'signing_publications': ('revision', 'history_count'),
            'signing_heads': ('revision', 'control_revision'),
            'signing_key_history': ('publication_revision', 'secret_version'),
            'signing_checkpoints': ('checkpoint_revision', 'publication_revision'),
        }.items():
            for name in names:
                self.assertEqual((self.columns(table)[name].field_type, self.columns(table)[name].mode),
                                 ('INTEGER', 'REQUIRED'))
        self.assertEqual(self.columns('signing_publications')['parent_digest'].mode, 'NULLABLE')

    def test_canonical_config_is_string_and_history_has_typed_schedule_and_profile(self):
        self.assertEqual(self.columns('signing_publications')['config_canonical_json'].field_type, 'STRING')
        history = self.columns('signing_key_history')
        self.assertEqual(history['revoked'].field_type, 'BOOLEAN')
        for name in ('issue_from', 'issue_until', 'retain_until'):
            self.assertEqual(history[name].field_type, 'TIMESTAMP')
        profile = {f.name: f for f in history['profile'].fields}
        self.assertEqual(set(profile), {'profile_id', 'settings', 'fingerprint', 'handler_version'})
        self.assertEqual({f.name for f in profile['settings'].fields},
                         {'maximum_bytes_billed', 'job_timeout_ms', 'statement_timeout_ms'})
        self.assertTrue(all(f.mode == 'REQUIRED' and f.field_type == 'INTEGER'
                            for f in profile['settings'].fields))

    def test_immutable_history_and_mutable_heads_floors_are_distinct(self):
        self.assertTrue(schema.TABLES['signing_publications'].append_only)
        self.assertTrue(schema.TABLES['signing_key_history'].append_only)
        self.assertFalse(schema.TABLES['signing_heads'].append_only)
        self.assertFalse(schema.TABLES['signing_checkpoints'].append_only)
        for name in ('signing_heads', 'signing_checkpoints'):
            self.assertIn('command_id', self.columns(name))

    def test_checkpoint_stores_floor_not_readiness_or_key_bytes(self):
        columns = self.columns('signing_checkpoints')
        self.assertIn('consumer_id', columns)
        self.assertIn('instance_id', columns)
        self.assertIn('publication_digest', columns)
        self.assertFalse({'ready', 'lease', 'phase', 'material', 'secret_bytes'} & set(columns))
        def check(fields):
            for f in fields:
                self.assertNotIn(f.field_type, {'BYTES', 'JSON', 'FLOAT', 'NUMERIC'})
                check(f.fields)
        for spec in schema.TABLES.values():
            check(spec.schema)

    def test_no_expiry_partitions_unenforced_constraints_or_client_creation(self):
        with patch('google.cloud.bigquery.Client', side_effect=AssertionError('No clients')):
            for spec in schema.TABLES.values():
                table = spec.table('test-project')
                self.assertIsNone(table.expires)
                self.assertIsNone(table.time_partitioning)
                self.assertIsNone(table.range_partitioning)
                self.assertFalse(table.require_partition_filter)
                self.assertIsNone(table.table_constraints)
                self.assertIn(schema.STORAGE_VERSION, table.description)
            self.assertEqual(schema.manifest()['deployment_status'], 'DESIGN_ONLY')

    def test_metadata_and_manifest_are_independent_copies(self):
        name = 'signing_key_history'
        original = self.columns(name)['profile'].fields[0].name
        table = schema.TABLES[name].table('test-project')
        next(f for f in table.schema if f.name == 'profile').fields[0]._properties['name'] = 'changed'
        data = json.loads(json.dumps(schema.manifest()))
        record = next(t for t in data['tables'] if t['name'] == name)
        next(f for f in record['schema'] if f['name'] == 'profile')['fields'][0]['name'] = 'changed'
        self.assertEqual(self.columns(name)['profile'].fields[0].name, original)
        with self.assertRaises(TypeError):
            schema.TABLES[name] = None

    def test_project_injection_and_mutable_spec_are_rejected(self):
        for value in (None, '', 'project`', 'a.b', 'DROP TABLE'):
            with self.assertRaises(ValueError):
                schema.TABLES['signing_heads'].table(value)
        with self.assertRaises(FrozenInstanceError):
            schema.TABLES['signing_heads'].name = 'changed'


class PublicationReadContractTests(unittest.TestCase):
    def assert_code(self, code, req, data, **options):
        with self.assertRaises(ContractError) as caught:
            validate_publication_read(req, data, now=options.pop('now', data.observed_at), **options)
        self.assertEqual(caught.exception.code, code)

    def test_genesis_to_unique_current_head_returns_complete_sorted_chain(self):
        data = snapshot()
        result = validate_publication_read(request(), data, now=data.observed_at)
        self.assertEqual(result.publications, data.publications)
        self.assertIsNone(result.checkpoint_anchor)
        reordered = replace(data, publications=tuple(reversed(data.publications)), history=tuple(reversed(data.history)))
        self.assertEqual(validate_publication_read(request(), reordered, now=data.observed_at), result)

    def test_missing_identical_or_conflicting_heads_never_select_latest(self):
        data = snapshot()
        for heads in ((), data.heads * 2, data.heads + (replace(data.heads[0], digest='f'*64),)):
            self.assert_code('INVALID_SIGNING_READ_COUNTS', request(),
                             replace(data, heads=heads, head_count=len(heads)))

    def test_control_fence_is_independent_of_publication_revision_and_strict(self):
        data = snapshot()
        changed = replace(data, heads=(replace(data.heads[0], control_revision=19),))
        result = validate_publication_read(request(), changed, now=changed.observed_at)
        self.assertEqual(result.publications, data.publications)
        self.assertEqual(result.head_control_revision, 19)
        for value in (True, 0, -1, 1.5, 2**63):
            with self.assertRaises(ContractError):
                replace(data.heads[0], control_revision=value)

    def test_head_digest_time_and_body_must_match(self):
        data = snapshot()
        for head in (replace(data.heads[0], digest='f'*64),
                     replace(data.heads[0], published_at=NOW),
                     replace(data.heads[0], revision=4)):
            self.assert_code('SIGNING_READ_HEAD_MISMATCH', request(), replace(data, heads=(head,)))

    def test_every_count_is_strict_complete_and_bounded(self):
        data = snapshot()
        for field in ('head_count','checkpoint_count','publication_count','history_count'):
            for value in (True, -1, 1.5, '1', getattr(data, field)+1):
                self.assert_code('INVALID_SIGNING_READ_COUNTS', request(), replace(data, **{field:value}))
        self.assert_code('INVALID_SIGNING_READ_COUNTS', request(), replace(data, history_count=MAX_READ_HISTORY+1))
        self.assert_code('INVALID_SIGNING_READ_COUNTS', request(), replace(data, publication_count=MAX_READ_PUBLICATIONS+1))

    def test_omitted_rows_and_wrong_projection_types_fail(self):
        data = snapshot()
        for changes in (dict(publications=list(data.publications)), dict(history=data.history[:-1]),
                        dict(heads=(object(),)), dict(checkpoints=[checkpoint(data.publications[0])])):
            self.assert_code('INVALID_SIGNING_READ_COUNTS', request(), replace(data, **changes))

    def test_duplicate_publication_and_duplicate_key_history_fail_even_if_identical(self):
        data = snapshot()
        self.assert_code('DUPLICATE_SIGNING_PUBLICATION', request(),
                         replace(data, publications=data.publications+(data.publications[0],), publication_count=4))
        self.assert_code('DUPLICATE_SIGNING_READ_HISTORY', request(),
                         replace(data, history=data.history+(data.history[0],), history_count=len(data.history)+1))

    def test_missing_and_extra_intermediate_revisions_fail(self):
        pubs = sequence(4)
        self.assert_code('SIGNING_READ_HISTORY_REQUIRED', request(), snapshot((pubs[0],pubs[2],pubs[3])))
        req = request(pubs[1])
        self.assert_code('SIGNING_READ_HISTORY_REQUIRED', req, snapshot(pubs,req=req))

    def test_parent_conflict_and_known_anchor_conflict_fail(self):
        pubs = sequence()
        corrupted = replace(pubs[-1], parent_digest='f'*64)
        self.assert_code('SIGNING_PUBLICATION_CHAIN_MISMATCH', request(), snapshot(pubs[:-1]+(corrupted,)))
        req = request(pubs[-1])
        alternate = fixtures.next_publication(pubs[-2], published_at=pubs[-1].published_at+timedelta(seconds=1))
        self.assert_code('SIGNING_READ_ANCHOR_CONFLICT', req, snapshot((alternate,),req=req))

    def test_running_replica_cannot_accept_lower_head(self):
        pubs = sequence()
        req = request(pubs[-1])
        self.assert_code('SIGNING_READ_HISTORY_REQUIRED', req, snapshot(pubs[:-1],req=req))

    def test_key_history_digest_missing_changed_and_extra_rows_fail(self):
        data = snapshot()
        row = data.history[0]
        variants = (data.history[1:],
                    (replace(row, publication_digest='f'*64),)+data.history[1:],
                    (replace(row, record=replace(row.record,key=replace(row.record.key,revoked=True))),)+data.history[1:],
                    data.history+(replace(row,publication_revision=4),))
        for history in variants:
            self.assert_code('SIGNING_READ_HISTORY_MISMATCH', request(),
                             replace(data, history=history, history_count=len(history)))

    def test_cold_checkpoint_can_anchor_without_inventing_genesis(self):
        pubs = sequence(18)
        anchor = pubs[9]
        data = snapshot(pubs[9:],checkpoints=(checkpoint(anchor),))
        result = validate_publication_read(request(),data,now=data.observed_at)
        self.assertEqual(result.checkpoint_anchor,anchor)
        self.assertEqual(result.publications,pubs[9:])

    def test_checkpoint_is_not_a_shortcut_for_a_running_replica(self):
        pubs = sequence()
        req = request(pubs[0])
        data = snapshot(pubs[1:],req=req,checkpoints=(checkpoint(pubs[1]),))
        self.assert_code('SIGNING_READ_HISTORY_REQUIRED',req,data)
        full = snapshot(pubs,req=req,checkpoints=(checkpoint(pubs[1]),))
        self.assertEqual(validate_publication_read(req,full,now=full.observed_at).publications,pubs)

    def test_older_checkpoint_adds_one_body_outside_sixteen_revision_window(self):
        pubs = sequence(17)
        req = request(pubs[1])
        data = snapshot(pubs,req=req,checkpoints=(checkpoint(pubs[0]),))
        result = validate_publication_read(req,data,now=data.observed_at)
        self.assertEqual(len(result.publications),16)
        self.assertEqual(result.checkpoint_anchor,pubs[0])

    def test_window_exhaustion_cannot_skip_to_latest_even_with_checkpoint(self):
        pubs = sequence(17)
        self.assert_code('SIGNING_READ_HISTORY_REQUIRED',request(),snapshot(pubs))
        req = request(pubs[0])
        self.assert_code('SIGNING_READ_HISTORY_REQUIRED',req,
                         snapshot(pubs,req=req,checkpoints=(checkpoint(pubs[-1]),)))

    def test_missing_wrong_consumer_digest_future_time_and_ahead_checkpoint_fail(self):
        pubs = sequence()
        good = checkpoint(pubs[0])
        variants = (replace(good,consumer_id='other'), replace(good,publication_digest='f'*64),
                    replace(good,updated_at=pubs[-1].published_at+timedelta(seconds=1)),
                    replace(good,updated_at=NOW-timedelta(seconds=1)),
                    replace(good,publication_revision=4))
        for cp in variants:
            self.assert_code('SIGNING_READ_CHECKPOINT_MISMATCH',request(),snapshot(pubs,checkpoints=(cp,)))
        self.assert_code('INVALID_SIGNING_READ_COUNTS',request(),snapshot(pubs,checkpoints=(good,good)))

    def test_checkpoint_above_head_and_orphan_publications_fail(self):
        pubs = sequence()
        data = snapshot(pubs,checkpoints=(checkpoint(pubs[-1]),))
        self.assert_code('SIGNING_READ_CHECKPOINT_MISMATCH',request(),
                         replace(data,heads=(SigningHeadRecord(2,pubs[1].digest,pubs[1].published_at,1),)))
        for value in (True,1,-1,'0'):
            self.assert_code('SIGNING_READ_UNHEADED_PUBLICATION',request(),
                             replace(snapshot(),unheaded_publication_count=value))

    def test_snapshot_ticket_consumer_and_secret_project_scope_are_pinned(self):
        data = snapshot()
        for changes in (dict(consumer_id='other'),dict(ticket=replace(data.ticket,generation=2)),
                        dict(ticket=replace(data.ticket,instance_id='other'))):
            self.assert_code('SIGNING_READ_SCOPE_MISMATCH',request(),replace(data,**changes))
        other = SigningReadRequest('pilot-runtime','999',request().ticket)
        self.assert_code('INVALID_SIGNING_READ_SCOPE',other,data)

    def test_publication_times_cannot_be_future_relative_to_snapshot(self):
        data = snapshot()
        self.assert_code('INVALID_SIGNING_READ_SCOPE',request(),replace(data,observed_at=NOW),now=NOW)

    def test_freshness_is_half_open_and_does_not_start_on_completion(self):
        data = snapshot()
        for now in (data.observed_at-timedelta(microseconds=1),data.observed_at+timedelta(seconds=30)):
            self.assert_code('STALE_SIGNING_READ_SNAPSHOT',request(),data,now=now)
        validate_publication_read(request(),data,now=data.observed_at+timedelta(seconds=29))

    def test_stopped_and_expired_active_reads_do_not_grant_readiness(self):
        pubs = sequence()
        stop = fixtures.next_publication(pubs[-1],status=SigningPublicationStatus.STOPPED)
        data = snapshot(pubs+(stop,))
        result = validate_publication_read(request(),data,now=data.observed_at)
        self.assertEqual(result.publications[-1].status,SigningPublicationStatus.STOPPED)
        old = snapshot((pubs[0],))
        old = replace(old,observed_at=pubs[0].config.policy.key('new').issue_until)
        result = validate_publication_read(request(),old,now=old.observed_at)
        self.assertFalse(hasattr(result,'ready'))

    def test_request_base_scope_types_and_limits_fail_closed(self):
        req = request()
        with self.assertRaises(ContractError):
            replace(req,known_head=fixtures.initial())
        for value in ('latest', '0', 123, None):
            with self.assertRaises(ContractError):
                replace(req,project_number=value)
        for field in ('maximum_bytes_billed','job_timeout_ms','result_timeout_seconds','max_age_seconds'):
            for value in (True,0,1.5,-1,2**63):
                with self.assertRaises(ContractError):
                    replace(SigningReadLimits(),**{field:value})

    def test_records_are_immutable_and_naive_timestamps_reject(self):
        data = snapshot()
        with self.assertRaises(FrozenInstanceError):
            data.head_count=2
        for record, field in ((data.heads[0],'published_at'),(checkpoint(data.publications[0]),'updated_at')):
            with self.assertRaises(ContractError):
                replace(record,**{field:NOW.replace(tzinfo=None)})

    def test_contract_acceptance_needs_no_sdk_clients(self):
        with patch('google.cloud.bigquery.Client',side_effect=AssertionError('No cloud clients')), \
             patch('google.cloud.secretmanager_v1.SecretManagerServiceClient',side_effect=AssertionError('No secrets')):
            data = snapshot()
            validate_publication_read(request(),data,now=data.observed_at)


if __name__ == '__main__':
    unittest.main()
