"""Owned submission host/worker tests; fake storage and no model or cloud calls."""
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timezone
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
from src.application.workflow_submission_host import (
    WorkflowSubmissionConfig, WorkflowSubmissionHost, config_from_wire, request_values,
)
from tests.test_identity import AUDIENCE, signer_and_public_key
from tests.test_management_service import CONV, SUBJECT, WS
from tests.test_workflow_submission_service import PREFERENCES, service, settings


def _headers(signer):
    epoch = int(datetime.now(timezone.utc).timestamp())
    token = jwt.encode(signer, dict(iss=ISSUER, aud=AUDIENCE,
        sub=SUBJECT, iat=epoch - 1, exp=epoch + 599)).decode()
    return {ASSERTION_HEADER: token}


def _fixture_main(fd, mode, public_path):
    from src.application import workflow_submission_worker as worker

    channel = socket.socket(fileno=int(fd))
    current = service()
    if mode=='interpret':
        current.io.client.close()
        from tests.test_interpreter_submission import InterpreterSubmissionTests
        from src.application.campaign_quality_authority import TABLE,table_metadata
        from src.application.campaign_transcript import SPEC
        from src.application.control_io import ControlOutcome
        from src.application.sharing_coordinator_contract import SharingCommitStatus
        from google.cloud import bigquery
        fixture=InterpreterSubmissionTests();fixture.setUp();current=fixture.instance();current.enabled=True
        def metadata(reference,**kwargs):
            expected=table_metadata() if str(reference)==TABLE else SPEC.table(current.client.project)
            value=expected.to_api_repr();value.update(location='asia-south1',type='TABLE')
            return bigquery.Table.from_api_repr(value)
        current.client.get_table=Mock(side_effect=metadata)
        current.io.submit_once=Mock(side_effect=lambda p:ControlOutcome(p.job_id,SharingCommitStatus.UNKNOWN))
    current.verifier = IapIdentityAdapter(current.verifier.settings)
    fetch = Mock(return_value=type('Response', (), {
        'status': 200, 'data': Path(public_path).read_bytes()})())
    if mode == 'hang':
        def hang(*_args, **_kwargs):
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            time.sleep(60)
        current.io.read.side_effect = hang
    try:
        with patch.object(worker, '_compose', return_value=current), patch.object(worker,'_compose_interpreter',return_value=current), patch(
                'src.application.identity._BoundedKeyRequest', return_value=fetch):
            worker.serve(channel)
    except BaseException:
        pass
    finally:
        channel.close()
        current.io.client.close()


@unittest.skipUnless(os.name == 'posix', 'Owned POSIX submission worker')
class WorkflowSubmissionHostTests(unittest.TestCase):
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
        config = WorkflowSubmissionConfig(IapSettings(AUDIENCE), replace(
            settings(), operation=OperationSettings(timeout)),interpretation_enabled=(mode=='interpret'))
        host = WorkflowSubmissionHost(config, SharingHostSettings(
            enabled=True, cleanup_seconds=.3))
        command = lambda fd: [sys.executable, '-m', 'tests.test_workflow_submission_host',
            'fixture', str(fd), mode, self.public_path]
        with patch.object(host, '_command', side_effect=command):
            host.start()
        self.hosts.append(host)
        return host

    def submit(self, host, **changes):
        values = dict(workspace_id=WS, conversation_id=CONV,
            preferences=deepcopy(PREFERENCES), retry_key='host-retry') | changes
        return host.submit(_headers(self.signer), **values)

    def test_config_roundtrip_and_defaults_stay_disabled(self):
        config = WorkflowSubmissionConfig(IapSettings(AUDIENCE), settings(False))
        self.assertEqual(config_from_wire(asdict(config)), config)
        with self.assertRaises(SharingHostError):
            WorkflowSubmissionHost(config, SharingHostSettings(enabled=True)).start()

    def test_real_worker_returns_only_validated_minimal_receipt_and_reuses_process(self):
        host = self.host()
        pid = host.worker_pid
        first = self.submit(host)
        second = self.submit(host)
        self.assertEqual(first, second)
        self.assertEqual(host.worker_pid, pid)
        self.assertEqual(set(asdict(first)), {'workspace_id', 'conversation_id', 'request_id',
            'job_id', 'persistence_status', 'job_state', 'submitted'})

    def test_signed_iap_interpretation_uses_owned_worker_and_retains_original_retry(self):
        host=self.host('interpret');pid=host.worker_pid
        values=dict(workspace_id=WS,conversation_id=CONV,message='Create a campaign',retry_key='initial-turn',
            selection=dict(sku='CI-SKU-0001',country='US',channel='META',audience='Shoppers'))
        first=host.interpret(_headers(self.signer),**values)
        second=host.interpret(_headers(self.signer),**values)
        self.assertEqual(first,second);self.assertEqual(first.native_status,'UNKNOWN')
        self.assertEqual(pid,host.worker_pid);self.assertFalse(first.release_allowed)
        with self.assertRaises(SharingHostError):
            host.interpret({ASSERTION_HEADER:'invalid.assertion.signature'},**values)

    def test_invalid_payload_is_rejected_before_worker_and_host_remains_usable(self):
        host = self.host()
        with self.assertRaises(ChannelError):
            self.submit(host, preferences={**PREFERENCES, 'roles': 'ADMIN'})
        self.assertFalse(host.retired)
        self.assertEqual(self.submit(host).job_state, 'QUEUED')

    def test_hung_submission_retires_worker_without_automatic_resubmission(self):
        host = self.host('hang', timeout=1)
        with self.assertRaises(OperationStopped):
            self.submit(host)
        self.assertTrue(host.retired)
        self.assertTrue(host.cleanup_complete)
        with self.assertRaises(SharingHostError):
            self.submit(host)

    def test_scope_mismatch_and_private_result_fields_fail_closed(self):
        host = WorkflowSubmissionHost(WorkflowSubmissionConfig(
            IapSettings(AUDIENCE), settings()), SharingHostSettings(enabled=True))
        valid = dict(workspace_id=WS, conversation_id=CONV, request_id='request-fixture',
            job_id='job-fixture', persistence_status='COMMITTED', job_state='QUEUED',
            submitted=True)
        with self.assertRaises(ChannelError):
            host._decode_result('submit', valid | {'owner_id': 'private'})
        receipt = host._decode_result('submit', valid)
        with self.assertRaises(ChannelError):
            host._validate_result_scope('submit', {'workspace_id': 'other',
                'conversation_id': CONV}, receipt)

    def test_request_contract_rejects_browser_policy_and_oversized_retry_key(self):
        base = dict(workspace_id=WS, conversation_id=CONV,
            preferences=PREFERENCES, retry_key='retry')
        for values in (base | {'owner_id': 'browser-owner'}, base | {'retry_key': 'x' * 129}):
            with self.subTest(values=set(values)), self.assertRaises(ChannelError):
                request_values('submit', {}, values)


if __name__ == '__main__':
    if len(sys.argv) == 5 and sys.argv[1] == 'fixture':
        _fixture_main(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        unittest.main()
