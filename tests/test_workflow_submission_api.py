"""Protected submission HTTP tests; fake storage and no cloud/model calls."""
import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from google.auth import jwt

from src.application.identity import ASSERTION_HEADER, ISSUER, IapSettings
from src.application.operation_budget import OperationSettings
from src.application.sharing_api import SharingApiSettings
from src.application.sharing_operation_host import SharingHostError, SharingHostSettings
from src.application.workflow_submission_api import WorkflowSubmissionApi
from src.application.workflow_submission_host import WorkflowSubmissionConfig, WorkflowSubmissionHost
from tests.test_identity import AUDIENCE, signer_and_public_key
from tests.test_management_service import CONV, SUBJECT, WS
from tests.test_sharing_api import Connection, ORIGIN
from tests.test_workflow_submission_service import PREFERENCES, settings


@unittest.skipUnless(os.name == 'posix', 'Owned POSIX submission worker')
class WorkflowSubmissionApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.public_path = str(Path(temp.name) / 'public.json')
        self.signer, public = signer_and_public_key()
        Path(self.public_path).write_text(json.dumps({'fixture-key': public}))
        self.apis = []

    async def asyncTearDown(self):
        for api in self.apis:
            await api.aclose()

    def api(self, mode='normal', *, enabled=True, timeout=2):
        submission = replace(settings(), operation=OperationSettings(timeout))
        host = WorkflowSubmissionHost(WorkflowSubmissionConfig(
            IapSettings(AUDIENCE), submission), SharingHostSettings(
                enabled=True, cleanup_seconds=.3))
        command = lambda fd: [sys.executable, '-m', 'tests.test_workflow_submission_host',
            'fixture', str(fd), mode, self.public_path]
        with patch.object(host, '_command', side_effect=command):
            host.start()
        api = WorkflowSubmissionApi(host, SharingApiSettings(enabled, (ORIGIN,)))
        self.apis.append(api)
        return api, host

    def scope(self, *, subject=SUBJECT, **changes):
        epoch = int(datetime.now(timezone.utc).timestamp())
        assertion = jwt.encode(self.signer, dict(iss=ISSUER, aud=AUDIENCE, sub=subject,
            iat=epoch - 1, exp=epoch + 599)).decode()
        return dict(type='http', method='POST', path='/v1/submissions/submit',
            query_string=b'', headers=[(b'origin', ORIGIN.encode()),
                (b'content-type', b'application/json'),
                (ASSERTION_HEADER.encode(), assertion.encode())]) | changes

    def body(self, **changes):
        return dict(workspace_id=WS, conversation_id=CONV,
            preferences=deepcopy(PREFERENCES), retry_key='http-retry') | changes

    async def call(self, api, *, body=None, scope=None, connection=None):
        connection = connection or Connection(json.dumps(
            self.body() if body is None else body).encode())
        await api(scope or self.scope(), connection.receive, connection.send)
        return connection

    async def test_success_returns_only_minimal_receipt_and_security_headers(self):
        api, _ = self.api()
        response = await self.call(api)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.payload['version'], 'workflow-submission-api-v1')
        self.assertEqual(response.payload['operation'], 'submit')
        self.assertEqual(set(response.payload['result']), {'workspace_id', 'conversation_id',
            'request_id', 'job_id', 'persistence_status', 'job_state', 'submitted'})
        self.assertIn((b'cache-control', b'no-store'), response.sent[0]['headers'])
        self.assertNotIn('principal', str(response.payload))

    async def test_duplicate_http_retry_reuses_stable_workflow_identity(self):
        api, host = self.api()
        first = await self.call(api)
        second = await self.call(api)
        self.assertEqual(first.payload['result'], second.payload['result'])
        self.assertFalse(host.retired)

    async def test_origin_route_body_size_and_browser_policy_are_rejected_before_host(self):
        api, host = self.api()
        host.submit = Mock()
        bad_origin = self.scope()
        bad_origin['headers'][0] = (b'origin', b'https://evil.example.test')
        self.assertEqual((await self.call(api, scope=bad_origin)).status, 403)
        self.assertEqual((await self.call(api, scope=self.scope(method='GET'))).status, 405)
        self.assertEqual((await self.call(api, body=self.body(owner_id='browser'))).status, 400)
        oversized = Connection(b'x' * 65537)
        self.assertEqual((await self.call(api, connection=oversized)).status, 413)
        host.submit.assert_not_called()

    async def test_access_denials_and_internal_failures_have_uniform_safe_errors(self):
        api, host = self.api()
        host.submit = Mock(side_effect=SharingHostError('PRIVATE_IDENTITY_DETAIL'))
        denied = await self.call(api)
        self.assertEqual((denied.status, denied.payload['error']), (403, 'ACCESS_DENIED'))
        self.assertNotIn('PRIVATE_IDENTITY_DETAIL', str(denied.sent))
        host.submit = Mock(side_effect=SharingHostError('SHARING_HOST_OPERATION_FAILED'))
        failed = await self.call(api)
        self.assertEqual((failed.status, failed.payload['error']), (503, 'SERVICE_UNAVAILABLE'))

    async def test_real_foreign_identity_is_denied_without_role_fallback(self):
        api, host = self.api()
        response = await self.call(api, scope=self.scope(
            subject='accounts.google.com:foreign-submission-user'))
        self.assertEqual((response.status, response.payload['error']), (403, 'ACCESS_DENIED'))
        self.assertFalse(host.retired)

    async def test_disconnect_during_hung_write_retires_and_joins_worker(self):
        api, host = self.api('hang', timeout=1)
        connection = Connection(json.dumps(self.body()).encode())
        task = asyncio.create_task(self.call(api, connection=connection))
        await asyncio.sleep(.2)
        connection.disconnect()
        await task
        self.assertEqual(connection.sent, [])
        self.assertTrue(host.retired)
        self.assertTrue(host.cleanup_complete)
        self.assertEqual(api.observations[-1].code, 'CLIENT_DISCONNECTED')

    async def test_disabled_api_never_invokes_prestarted_worker(self):
        api, host = self.api(enabled=False)
        host.submit = Mock()
        response = await self.call(api)
        self.assertEqual((response.status, response.payload['error']),
            (503, 'SERVICE_UNAVAILABLE'))
        host.submit.assert_not_called()


if __name__ == '__main__':
    unittest.main()
