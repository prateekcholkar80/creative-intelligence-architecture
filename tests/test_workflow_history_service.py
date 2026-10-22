"""Offline authoritative history/cold-reconnect tests; no cloud calls."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import Mock

from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from src.application.identity import ISSUER, IapIdentityAdapter, IapSettings, VerifiedIdentity, principal_for_subject
from src.application.operation_budget import OperationSettings
from src.application.workflow_codec import encode_row
from src.application.workflow_contract import ContractError, request_fingerprint, retry_key_hash
from src.application.workflow_history_service import HISTORY_SQL, HistorySettings, ProtectedHistoryService
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from src.validation.control_fixtures import ACTOR, SUBJECT, controls, workflow
from tests.test_identity import AUDIENCE

WS = 'history-fixture-workspace'
CONV = 'fixture-conversation'
KEY = b'history-cursor-key-fixture-value!'


class HistorySqlDeclarationTests(unittest.TestCase):
    def test_page_clock_declared_before_transaction(self):
        # BigQuery rejects DECLARE after the transaction statement.
        self.assertTrue(HISTORY_SQL.startswith('DECLARE page_at TIMESTAMP'))
        self.assertLess(HISTORY_SQL.index('DECLARE page_at'), HISTORY_SQL.index('BEGIN TRANSACTION;'))

    def test_request_partition_filter_uses_page_snapshot(self):
        self.assertIn('AND r.created_at<=page_at', HISTORY_SQL)


def identity(subject=SUBJECT):
    epoch = int(datetime.now(timezone.utc).timestamp())
    return VerifiedIdentity(principal_for_subject(ISSUER, subject), ISSUER, subject,
        AUDIENCE, epoch - 10, epoch + 590)


def record(number, created_at):
    rows = workflow(WS)
    request = deepcopy(rows['requests'][0])
    request.update(request_id=f'history-request-{number}', session_id=f'history-session-{number}',
        trace_id=f'history-trace-{number}', generation_intent_id=f'history-intent-{number}',
        created_at=created_at, updated_at=created_at,
        idempotency_key_sha256=retry_key_hash(workspace_id=WS, principal_id=ACTOR,
            conversation_id=CONV, operation='GENERATE', retry_key=f'history-key-{number}'),
        request_sha256='0' * 64)
    request['request_sha256'] = request_fingerprint(request)
    job = deepcopy(rows['jobs'][0])
    for name in ('request_id', 'trace_id', 'generation_intent_id',
                 'idempotency_key_sha256', 'request_sha256'):
        job[name] = request[name]
    job.update(job_id=f'history-job-{number}', active_attempt_id=f'history-attempt-{number}',
        created_at=created_at, updated_at=created_at + timedelta(seconds=1),
        deadline_at=created_at + timedelta(minutes=30))
    return {'job': encode_row(job, 'jobs'), 'requests': [encode_row(request, 'requests')]}


def snapshot(records=(), *, workspace=None, membership=None, conversation=None,
             observed_at=None, page_at=None):
    rows = controls(WS)
    observed_at = observed_at or datetime.now(timezone.utc)
    return dict(observed_at=observed_at, page_at=page_at or observed_at,
        workspaces=[encode_row(rows['workspaces'][0] if workspace is None else workspace, 'workspaces')],
        memberships=[encode_row(rows['memberships'][0] if membership is None else membership, 'memberships')],
        conversations=[encode_row(rows['conversations'][0] if conversation is None else conversation, 'conversations')],
        records=list(records))


def service(enabled=True):
    client = bigquery.Client(project=PROJECT, location=LOCATION,
        credentials=AnonymousCredentials())
    verifier = IapIdentityAdapter(IapSettings(AUDIENCE))
    verifier.authenticate = Mock(side_effect=lambda _headers: identity())
    result = ProtectedHistoryService(client=client, verifier=verifier,
        settings=HistorySettings(enabled=enabled, cursor_key=KEY,
            operation=OperationSettings(10)))
    result.io.read = Mock(return_value=snapshot())
    return result


class WorkflowHistoryServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = service()
        self.addCleanup(self.service.io.client.close)

    def list(self, **changes):
        return self.service.list_jobs({}, workspace_id=WS, conversation_id=CONV, **changes)

    def test_defaults_deny_before_identity_or_bigquery(self):
        disabled = service(False)
        self.addCleanup(disabled.io.client.close)
        with self.assertRaises(ContractError) as caught:
            disabled.list_jobs({}, workspace_id=WS, conversation_id=CONV)
        self.assertEqual(caught.exception.code, 'HISTORY_DISABLED')
        disabled.verifier.authenticate.assert_not_called()
        disabled.io.read.assert_not_called()

    def test_empty_history_returns_current_minimal_conversation_projection(self):
        page = self.list()
        self.assertEqual((page.workspace_id, page.conversation_id), (WS, CONV))
        self.assertEqual((page.visibility, page.access_revision), ('PRIVATE', 1))
        self.assertEqual(page.jobs, ())
        self.assertIsNone(page.next_cursor)
        self.assertNotIn('owner_id', page.__dataclass_fields__)

    def test_keyset_page_and_signed_cursor_preserve_snapshot_and_order(self):
        at = datetime.now(timezone.utc) - timedelta(minutes=1)
        first_records = [record(3, at + timedelta(seconds=3)),
            record(2, at + timedelta(seconds=2)), record(1, at + timedelta(seconds=1))]
        self.service.io.read.side_effect = [snapshot(first_records, page_at=at + timedelta(seconds=5)),
            snapshot([first_records[2]], page_at=at + timedelta(seconds=5))]
        first = self.list(page_size=2)
        self.assertEqual([job.job_id for job in first.jobs], ['history-job-3', 'history-job-2'])
        self.assertIsNotNone(first.next_cursor)
        second = self.list(page_size=2, cursor=first.next_cursor)
        self.assertEqual([job.job_id for job in second.jobs], ['history-job-1'])
        parameters = {item.name: item.value for item in self.service.io.read.call_args.args[1]}
        self.assertEqual(parameters['page_at'], at + timedelta(seconds=5))
        self.assertEqual((parameters['before_at'], parameters['before_id']),
            (at + timedelta(seconds=2), 'history-job-2'))
        self.assertNotIn('OFFSET', HISTORY_SQL)
        self.assertIn('ORDER BY j.created_at DESC, j.job_id DESC LIMIT @fetch_limit', HISTORY_SQL)

    def test_tampered_or_cross_scope_cursor_is_rejected_before_bigquery(self):
        at = datetime.now(timezone.utc) - timedelta(minutes=1)
        records = [record(2, at + timedelta(seconds=2)), record(1, at + timedelta(seconds=1))]
        self.service.io.read.return_value = snapshot(records, page_at=at + timedelta(seconds=3))
        cursor = self.list(page_size=1).next_cursor
        calls = self.service.io.read.call_count
        tampered = cursor[:-1] + ('A' if cursor[-1] != 'A' else 'B')
        malformed = self.service._seal(dict(version=1, workspace_id=WS,
            conversation_id=CONV, principal_id=ACTOR,
            page_at=(at + timedelta(seconds=3)).isoformat(),
            before_at=(at + timedelta(seconds=2)).isoformat(), before_id='bad id'))
        for value, workspace_id in ((tampered, WS), (cursor, 'another-workspace'),
                                    (malformed, WS)):
            with self.subTest(workspace_id=workspace_id), self.assertRaises(ContractError) as caught:
                self.service.list_jobs({}, workspace_id=workspace_id,
                    conversation_id=CONV, page_size=1, cursor=value)
            self.assertEqual(caught.exception.code, 'INVALID_HISTORY_CURSOR')
        self.assertEqual(self.service.io.read.call_count, calls)

    def test_revoked_archived_foreign_owner_and_identity_binding_fail_closed(self):
        rows = controls(WS)
        cases = [dict(membership=rows['memberships'][0] | {'status': 'REVOKED'}),
            dict(conversation=rows['conversations'][0] | {'status': 'ARCHIVED'}),
            dict(conversation=rows['conversations'][0] | {'owner_id': 'foreign-owner'}),
            dict(membership=rows['memberships'][0] | {'identity_subject': 'accounts.google.com:other'})]
        for values in cases:
            with self.subTest(values=values):
                self.service.io.read.return_value = snapshot(**values)
                with self.assertRaises((ContractError, PermissionError)):
                    self.list()

    def test_duplicate_request_invalid_job_and_unordered_page_are_rejected(self):
        at = datetime.now(timezone.utc) - timedelta(minutes=1)
        valid = record(1, at)
        duplicate = deepcopy(valid); duplicate['requests'] *= 2
        malformed = deepcopy(valid); malformed['job'] = malformed['job'].replace(
            '"state":"RUNNING"', '"state":"QUEUED"')
        unordered = [record(1, at), record(2, at + timedelta(seconds=1))]
        for records in ([duplicate], [malformed], unordered):
            self.service.io.read.return_value = snapshot(records)
            with self.subTest(records=len(records)), self.assertRaises(ContractError):
                self.list()

    def test_future_snapshot_and_duplicate_access_rows_fail_closed(self):
        now = datetime.now(timezone.utc)
        future = snapshot(observed_at=now, page_at=now + timedelta(seconds=1))
        duplicate = snapshot(); duplicate['workspaces'] *= 2
        for row in (future, duplicate):
            self.service.io.read.return_value = row
            with self.assertRaises(ContractError):
                self.list()

    def test_page_size_types_and_bounds_are_rejected_before_bigquery(self):
        for value in (True, 0, 51):
            with self.subTest(value=value), self.assertRaises(ContractError):
                self.list(page_size=value)
        self.service.io.read.assert_not_called()


if __name__ == '__main__':
    unittest.main()
