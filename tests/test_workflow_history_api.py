"""Protected history HTTP tests; fake BigQuery and no cloud/model calls."""
import asyncio
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
from src.application.workflow_history_api import WorkflowHistoryApi
from src.application.workflow_history_host import WorkflowHistoryConfig, WorkflowHistoryHost
from src.application.workflow_history_service import HistorySettings
from src.validation.control_fixtures import SUBJECT
from tests.test_identity import AUDIENCE, signer_and_public_key
from tests.test_sharing_api import Connection, ORIGIN
from tests.test_workflow_history_service import CONV, KEY, WS


@unittest.skipUnless(os.name == 'posix', 'Owned POSIX history worker')
class WorkflowHistoryApiTests(unittest.IsolatedAsyncioTestCase):
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
        history = HistorySettings(enabled=True, cursor_key=KEY,
            operation=OperationSettings(timeout))
        host = WorkflowHistoryHost(WorkflowHistoryConfig(
            IapSettings(AUDIENCE), history), SharingHostSettings(
                enabled=True, cleanup_seconds=.3))
        command = lambda fd: [sys.executable, '-m', 'tests.test_workflow_history_host',
            'fixture', str(fd), mode, self.public_path]
        with patch.object(host, '_command', side_effect=command):
            host.start()
        api = WorkflowHistoryApi(host, SharingApiSettings(enabled, (ORIGIN,)))
        self.apis.append(api)
        return api, host

    def scope(self, *, subject=SUBJECT, **changes):
        epoch = int(datetime.now(timezone.utc).timestamp())
        assertion = jwt.encode(self.signer, dict(iss=ISSUER, aud=AUDIENCE, sub=subject,
            iat=epoch - 1, exp=epoch + 599)).decode()
        return dict(type='http', method='POST', path='/v1/history/list_jobs',
            query_string=b'', headers=[(b'origin', ORIGIN.encode()),
                (b'content-type', b'application/json'),
                (ASSERTION_HEADER.encode(), assertion.encode())]) | changes

    def body(self, **changes):
        return dict(workspace_id=WS, conversation_id=CONV, page_size=2) | changes

    async def call(self, api, *, body=None, scope=None, connection=None):
        connection = connection or Connection(json.dumps(
            self.body() if body is None else body).encode())
        await api(scope or self.scope(), connection.receive, connection.send)
        return connection

    async def test_real_worker_paginates_and_serializes_minimal_history(self):
        api, host = self.api()
        first = await self.call(api)
        self.assertEqual(first.status, 200)
        self.assertEqual(first.payload['version'], 'workflow-history-api-v1')
        self.assertEqual(first.payload['operation'], 'list_jobs')
        result = first.payload['result']
        self.assertEqual([job['job_id'] for job in result['jobs']],
            ['history-job-3', 'history-job-2'])
        self.assertIsInstance(result['jobs'][0]['created_at'], str)
        self.assertNotIn('owner_id', str(result))
        second = await self.call(api, body=self.body(cursor=result['next_cursor']))
        self.assertEqual([job['job_id'] for job in second.payload['result']['jobs']],
            ['history-job-1'])
        self.assertFalse(host.retired)
        self.assertIn((b'cache-control', b'no-store'), first.sent[0]['headers'])

    async def test_origin_route_size_and_browser_policy_reject_before_host(self):
        api, host = self.api()
        host.list_jobs = Mock()
        bad_origin = self.scope()
        bad_origin['headers'][0] = (b'origin', b'https://evil.example.test')
        self.assertEqual((await self.call(api, scope=bad_origin)).status, 403)
        self.assertEqual((await self.call(api, scope=self.scope(method='GET'))).status, 405)
        self.assertEqual((await self.call(api, body=self.body(owner_id='browser'))).status, 400)
        self.assertEqual((await self.call(api, body=self.body(page_size=True))).status, 400)
        oversized = Connection(b'x' * 65537)
        self.assertEqual((await self.call(api, connection=oversized)).status, 413)
        host.list_jobs.assert_not_called()

    async def test_access_denials_and_internal_failures_are_safe(self):
        api, host = self.api()
        host.list_jobs = Mock(side_effect=SharingHostError('PRIVATE_ACCESS_DETAIL'))
        denied = await self.call(api)
        self.assertEqual((denied.status, denied.payload['error']), (403, 'ACCESS_DENIED'))
        self.assertNotIn('PRIVATE_ACCESS_DETAIL', str(denied.sent))
        host.list_jobs = Mock(side_effect=SharingHostError('SHARING_HOST_OPERATION_FAILED'))
        failed = await self.call(api)
        self.assertEqual((failed.status, failed.payload['error']), (503, 'SERVICE_UNAVAILABLE'))

    async def test_real_foreign_identity_has_no_history_fallback(self):
        api, host = self.api()
        response = await self.call(api, scope=self.scope(
            subject='accounts.google.com:foreign-history-user'))
        self.assertEqual((response.status, response.payload['error']), (403, 'ACCESS_DENIED'))
        self.assertFalse(host.retired)

    async def test_disconnect_during_hung_read_retires_and_joins_worker(self):
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

    async def test_disabled_api_does_not_invoke_prestarted_worker(self):
        api, host = self.api(enabled=False)
        host.list_jobs = Mock()
        response = await self.call(api)
        self.assertEqual((response.status, response.payload['error']),
            (503, 'SERVICE_UNAVAILABLE'))
        host.list_jobs.assert_not_called()


if __name__ == '__main__':
    unittest.main()
