"""Owned history host/worker tests; fake BigQuery and no network/model calls."""
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import signal
import socket
import sys
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import Mock, patch

from google.auth import jwt

from src.application.identity import ASSERTION_HEADER, ISSUER, IapIdentityAdapter, IapSettings
from src.application.operation_budget import OperationSettings, OperationStopped
from src.application.operation_channel import ChannelError
from src.application.sharing_operation_host import SharingHostError, SharingHostSettings
from src.application.workflow_history_host import (
    WorkflowHistoryConfig, WorkflowHistoryHost, config_from_wire, request_values,
)
from src.application.workflow_history_service import (
    ConversationHistoryPage, HistoryJob, HistorySettings,
)
from src.validation.control_fixtures import SUBJECT
from tests.test_identity import AUDIENCE, signer_and_public_key
from tests.test_workflow_history_service import CONV, KEY, WS, record, service, snapshot


def _headers(signer):
    epoch = int(datetime.now(timezone.utc).timestamp())
    assertion = jwt.encode(signer, dict(iss=ISSUER, aud=AUDIENCE, sub=SUBJECT,
        iat=epoch - 1, exp=epoch + 599)).decode()
    return {ASSERTION_HEADER: assertion}


def _fixture_main(fd, mode, public_path):
    from src.application import workflow_history_worker as worker

    channel = socket.socket(fileno=int(fd))
    current = service()
    current.verifier = IapIdentityAdapter(current.verifier.settings)
    fetch = Mock(return_value=type('Response', (), {
        'status': 200, 'data': Path(public_path).read_bytes()})())
    page_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    records = [record(3, page_at - timedelta(seconds=1)),
        record(2, page_at - timedelta(seconds=2)),
        record(1, page_at - timedelta(seconds=3))]

    def read(_sql, parameters):
        values = {item.name: item.value for item in parameters}
        selected = records if values['before_id'] is None else records[2:]
        return snapshot(selected, observed_at=datetime.now(timezone.utc), page_at=page_at)

    current.io.read.side_effect = read
    if mode == 'hang':
        def hang(*_args, **_kwargs):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(60)
        current.io.read.side_effect = hang
    try:
        with patch.object(worker, '_compose', return_value=current), patch(
                'src.application.identity._BoundedKeyRequest', return_value=fetch):
            worker.serve(channel)
    except BaseException:
        pass
    finally:
        channel.close()
        current.io.client.close()


@unittest.skipUnless(os.name == 'posix', 'Owned POSIX history worker')
class WorkflowHistoryHostTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.public_path = str(Path(temp.name) / 'public.json')
        self.signer, public = signer_and_public_key()
        Path(self.public_path).write_text(json.dumps({'fixture-key': public}))
        self.hosts = []

    def tearDown(self):
        for host in self.hosts:
            host.close()

    def host(self, mode='normal', timeout=2):
        history = HistorySettings(enabled=True, cursor_key=KEY,
            operation=OperationSettings(timeout))
        config = WorkflowHistoryConfig(IapSettings(AUDIENCE), history)
        host = WorkflowHistoryHost(config, SharingHostSettings(
            enabled=True, cleanup_seconds=.3))
        command = lambda fd: [sys.executable, '-m', 'tests.test_workflow_history_host',
            'fixture', str(fd), mode, self.public_path]
        with patch.object(host, '_command', side_effect=command):
            host.start()
        self.hosts.append(host)
        return host

    def list(self, host, **changes):
        values = dict(workspace_id=WS, conversation_id=CONV, page_size=2) | changes
        return host.list_jobs(_headers(self.signer), **values)

    def test_config_roundtrip_preserves_secret_bytes_and_defaults_disabled(self):
        disabled = HistorySettings()
        config = WorkflowHistoryConfig(IapSettings(AUDIENCE), disabled)
        host = WorkflowHistoryHost(config)
        self.assertEqual(config_from_wire(host._config_values()), config)
        self.assertNotIn(str(KEY), str(host._config_values()))
        with self.assertRaises(SharingHostError):
            WorkflowHistoryHost(config, SharingHostSettings(enabled=True)).start()

    def test_real_worker_pages_with_same_process_and_typed_timestamps(self):
        host = self.host()
        pid = host.worker_pid
        first = self.list(host)
        self.assertEqual([job.job_id for job in first.jobs],
            ['history-job-3', 'history-job-2'])
        self.assertIsNotNone(first.next_cursor)
        second = self.list(host, cursor=first.next_cursor)
        self.assertEqual([job.job_id for job in second.jobs], ['history-job-1'])
        self.assertEqual(host.worker_pid, pid)
        self.assertIsInstance(first.jobs[0].created_at, datetime)
        self.assertNotIn('owner_id', str(asdict(first)))

    def test_tampered_cursor_is_uniform_denial_and_preserves_worker(self):
        host = self.host()
        cursor = self.list(host).next_cursor
        tampered = cursor[:-1] + ('A' if cursor[-1] != 'A' else 'B')
        with self.assertRaises(SharingHostError) as caught:
            self.list(host, cursor=tampered)
        self.assertEqual(caught.exception.code, 'ACCESS_DENIED')
        self.assertFalse(host.retired)
        self.assertEqual(len(self.list(host).jobs), 2)

    def test_invalid_payload_is_rejected_before_worker(self):
        host = self.host()
        for values in (dict(workspace_id=WS, conversation_id=CONV, page_size=True),
                dict(workspace_id=WS, conversation_id=CONV, page_size=2, owner_id='browser')):
            with self.subTest(values=values), self.assertRaises(ChannelError):
                host.list_jobs(_headers(self.signer), **values)
        self.assertFalse(host.retired)

    def test_malformed_or_cross_scope_result_fails_closed(self):
        host = WorkflowHistoryHost(WorkflowHistoryConfig(
            IapSettings(AUDIENCE), HistorySettings(enabled=True, cursor_key=KEY)),
            SharingHostSettings(enabled=True))
        at = datetime.now(timezone.utc)
        job = HistoryJob('job', 'request', 'CI-SKU-0001', 'RUNNING', 1, None, None, at, at)
        page = ConversationHistoryPage(WS, CONV, 'PRIVATE', 1, at, at, (job,), None)
        value = asdict(page)
        value['jobs'] = list(value['jobs'])
        value['conversation_created_at'] = at.isoformat()
        value['conversation_updated_at'] = at.isoformat()
        value['jobs'][0]['created_at'] = at.isoformat()
        value['jobs'][0]['updated_at'] = at.isoformat()
        decoded = host._decode_result('list_jobs', value)
        with self.assertRaises(ChannelError):
            host._validate_result_scope('list_jobs', {'workspace_id': 'other',
                'conversation_id': CONV, 'page_size': 2}, decoded)
        with self.assertRaises(ChannelError):
            host._decode_result('list_jobs', value | {'principal_id': 'private'})

    def test_hung_read_retires_worker_without_restart(self):
        host = self.host('hang', timeout=1)
        with self.assertRaises(OperationStopped):
            self.list(host)
        self.assertTrue(host.retired)
        self.assertTrue(host.cleanup_complete)
        with self.assertRaises(SharingHostError):
            self.list(host)


if __name__ == '__main__':
    if len(sys.argv) == 5 and sys.argv[1] == 'fixture':
        _fixture_main(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        unittest.main()
