"""4.4 offline ASGI tests, including owned workers; no network/cloud writes."""
import asyncio
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from threading import Event
import time
import unittest
from unittest.mock import Mock, patch

from google.auth import jwt

from src.application.identity import ASSERTION_HEADER, ISSUER
from src.application.operation_budget import OperationSettings, OperationStopped, operation_scope
from src.application.sharing_api import SharingApi, SharingApiSettings
from src.application.sharing_coordinator_contract import SharingCommitObservation, SharingCommitStatus
from src.application.sharing_executor import SharingExecutionOutcome, SharingRetryIssuance, SharingRetryIssuanceState
from src.application.sharing_operation_host import SharingHostError, SharingHostSettings, SharingOperationHost
from src.application.sharing_query import SharingQueryResult, SharingResultState
from tests.test_identity import AUDIENCE, signer_and_public_key
from tests.test_membership_contract import SUBJECT
from tests.test_sharing_executor import EPOCH
from tests.test_sharing_operation_host import config
from tests.test_sharing_query import query
from tests.test_sharing_signing import SharingRetrySealer, KEY

ORIGIN = 'https://campaign.example.test'


def scope(operation='recover', **changes):
    return dict(type='http', method='POST', path='/v1/sharing/' + operation,
        query_string=b'', headers=[(b'origin',ORIGIN.encode()),(b'content-type',b'application/json'),
            (ASSERTION_HEADER.encode(),b'offline-assertion')]) | changes


class Connection:
    def __init__(self, raw=b'{"token":"opaque"}', *, events=None, send_hook=None):
        self.incoming=asyncio.Queue()
        for event in events or [dict(type='http.request',body=raw,more_body=False)]:
            self.incoming.put_nowait(event)
        self.sent=[]
        self.send_hook=send_hook

    async def receive(self):
        return await self.incoming.get()

    async def send(self, event):
        if self.send_hook is not None:
            await self.send_hook(event)
        self.sent.append(event)

    def disconnect(self):
        self.incoming.put_nowait({'type':'http.disconnect'})

    @property
    def status(self):
        return self.sent[0]['status'] if self.sent else None

    @property
    def payload(self):
        return json.loads(self.sent[-1]['body'])


class ApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.apis=[]
        self.hosts=[]
        temp=TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.path=str(Path(temp.name)/'observations.jsonl')
        self.token=SharingRetrySealer(key=KEY,key_id='old').seal(query())

    async def asyncTearDown(self):
        for api in self.apis:
            await api.aclose()
        for host in self.hosts:
            host.close()
            if host._process is not None:
                host._process.wait(timeout=3)

    def api(self, *, host=None, enabled=True, capacity=4, timeout=2):
        if host is None:
            host=SharingOperationHost(config(runtime=replace(config().runtime,
                operation=OperationSettings(timeout))),SharingHostSettings(enabled=True,cleanup_seconds=.3))
        self.hosts.append(host)
        api=SharingApi(host,SharingApiSettings(enabled,(ORIGIN,),capacity))
        self.apis.append(api)
        return api,host

    def process_api(self, mode='composed', **options):
        api,host=self.api(**options)
        command=lambda fd:[sys.executable,'-m','tests.sharing_host_worker_fixture',str(fd),mode,self.path]
        with patch('src.application.sharing_operation_host._worker_command',side_effect=command):
            host.start()
        return api,host

    async def call(self, api, request=None, connection=None):
        connection=connection or Connection()
        await api(request or scope(),connection.receive,connection.send)
        return connection

    def unknown(self):
        return SharingExecutionOutcome(SharingCommitObservation(query().command.audit_id,
            SharingCommitStatus.UNKNOWN,'original-job'))

    def rows(self):
        return [json.loads(line) for line in Path(self.path).read_text().splitlines()] if Path(self.path).exists() else []

    async def wait_for(self, predicate):
        until=time.monotonic()+2
        while not predicate():
            if time.monotonic() >= until:self.fail('Expected fixture state did not arrive')
            await asyncio.sleep(.01)

    async def test_disabled_has_no_worker_or_identity_work(self):
        api,host=self.api(enabled=False)
        host.recover=Mock()
        response=await self.call(api)
        self.assertEqual(response.status,503)
        host.recover.assert_not_called();self.assertIsNone(host.worker_pid)

    async def test_explicit_prestart_is_required_and_not_triggered_by_http(self):
        api,host=self.api()
        response=await self.call(api)
        self.assertEqual(response.status,503);self.assertIsNone(host.worker_pid)

    async def test_all_seven_routes_use_fixed_projections(self):
        api,host=self.api()
        for method in ('submit','recover','submit_retry','recover_retry'):
            setattr(host,method,Mock(return_value=self.unknown()))
        host.issue_intent=Mock(return_value='sealed-original')
        host.issue_retry=Mock(return_value=SharingRetryIssuance(SharingRetryIssuanceState.RECOVERY_REQUIRED))
        host.recover_receipt=Mock(return_value=SharingQueryResult(SharingResultState.UNKNOWN,
            query().command.audit_id,query().command.accepted_at,None))
        for method in ('issue_intent','submit','recover','issue_retry','submit_retry','recover_retry','recover_receipt'):
            body={'token':'opaque'}
            if method=='issue_intent':
                body=dict(workspace_id='workspace',conversation_id='conversation',visibility='TEAM',
                    expected_membership_revision=1,expected_conversation_revision=1,
                    expected_workspace_revision=1,retry_key='stable-original-key')
            response=await self.call(api,scope(method),Connection(json.dumps(body).encode()))
            self.assertEqual(response.status,200,method)
            self.assertEqual(response.payload['operation'],method)
            self.assertEqual(response.payload['version'],'sharing-api-v1')
            getattr(host,method).assert_called_once()
        self.assertEqual(len({row.request_id for row in api.observations}),7)

    async def test_only_assertion_is_forwarded_browser_roles_and_deadlines_ignored(self):
        api,host=self.api();host.recover=Mock(return_value=self.unknown())
        request=scope();request['headers'] += [(b'x-goog-authenticated-user-email',b'admin@example.test'),
            (b'x-role',b'ADMIN'),(b'x-deadline',b'999999999'),(b'cookie',b'private-cookie')]
        await self.call(api,request)
        self.assertEqual(host.recover.call_args.args[0],{ASSERTION_HEADER:'offline-assertion'})

    async def test_missing_or_cross_origin_denied_without_worker(self):
        api,host=self.api();host.recover=Mock()
        for name in (b'origin',ASSERTION_HEADER.encode()):
            request=scope();request['headers']=[(k,v) for k,v in request['headers'] if k!=name]
            self.assertEqual((await self.call(api,request)).status,403)
        request=scope();request['headers'][0]=(b'origin',b'https://evil.example.test')
        self.assertEqual((await self.call(api,request)).status,403);host.recover.assert_not_called()

    async def test_duplicate_sensitive_header_case_aliases_rejected(self):
        api,host=self.api();host.recover=Mock()
        for name in (b'Origin',b'Content-Type',b'X-Goog-Iap-Jwt-Assertion'):
            request=scope();request['headers'].append((name,b'other'))
            self.assertEqual((await self.call(api,request)).status,400)
        host.recover.assert_not_called()

    async def test_malformed_headers_rejected(self):
        api,host=self.api();host.recover=Mock()
        for extra in ([(b'bad',b'cr\r\nlf')],[(b'bad',b'x'*16385)],[(b'x',b'y')]*65,[('not-bytes',b'value')]):
            request=scope();request['headers']+=extra
            self.assertEqual((await self.call(api,request)).status,400)
        host.recover.assert_not_called()

    async def test_method_route_query_and_content_type_boundaries(self):
        api,host=self.api();host.recover=Mock()
        for request,expected in ((scope(method='GET'),405),
            (scope(path='/v1/sharing/not-a-method'),404),(scope(query_string=b'token=private'),400)):
            self.assertEqual((await self.call(api,request)).status,expected)
        request=scope();request['headers'][1]=(b'content-type',b'text/plain')
        self.assertEqual((await self.call(api,request)).status,415);host.recover.assert_not_called()

    async def test_json_shape_duplicates_nonfinite_and_browser_configuration_rejected(self):
        api,host=self.api();host.recover=Mock()
        for raw in (b'[]',b'{',b'{"token":"a","token":"b"}',b'{"token":NaN}',
            b'{"token":"a","principal_id":"admin"}',b'{"token":"a","timeout":600}',b'{"token":true}',b'\xff'):
            self.assertEqual((await self.call(api,connection=Connection(raw))).status,400)
        host.recover.assert_not_called()

    async def test_intent_boolean_revisions_rejected_before_host(self):
        api,host=self.api();host.issue_intent=Mock()
        raw=dict(workspace_id='workspace',conversation_id='conversation',visibility='TEAM',
            expected_membership_revision=True,expected_conversation_revision=1,
            expected_workspace_revision=1,retry_key='stable-key')
        response=await self.call(api,scope('issue_intent'),Connection(json.dumps(raw).encode()))
        self.assertEqual(response.status,400);host.issue_intent.assert_not_called()

    async def test_streamed_body_size_length_and_protocol_validation(self):
        api,host=self.api();host.recover=Mock(return_value=self.unknown())
        events=[dict(type='http.request',body=b'{"token":',more_body=True),
                dict(type='http.request',body=b'"opaque"}',more_body=False)]
        self.assertEqual((await self.call(api,connection=Connection(events=events))).status,200)
        self.assertEqual((await self.call(api,connection=Connection(b'x'*65537))).status,413)
        request=scope();request['headers'].append((b'content-length',b'2'))
        self.assertEqual((await self.call(api,request)).status,400)
        bad=Connection(events=[dict(type='http.request',body=b'{}',more_body='false')])
        self.assertEqual((await self.call(api,connection=bad)).status,400)
        self.assertEqual(host.recover.call_count,1)

    async def test_uniform_denials_hide_existence_tokens_and_private_errors(self):
        api,host=self.api()
        for code in ('INVALID_ASSERTION','INVALID_SHARING_SEAL','SHARING_HOST_ACCESS_DENIED',
                     'SIGNING_KEY_REVOKED','ACCESS_UNAVAILABLE'):
            host.recover=Mock(side_effect=SharingHostError(code))
            response=await self.call(api)
            self.assertEqual(response.status,403)
            self.assertEqual(response.payload['error'],'ACCESS_DENIED')
            self.assertNotIn(code,str(response.sent))
        host.recover=Mock(side_effect=RuntimeError('private assertion and secret'))
        response=await self.call(api)
        self.assertEqual(response.status,503)
        self.assertNotIn('private',str(response.sent)+str(api.observations))

    async def test_no_cache_no_credentials_no_internal_records_in_responses(self):
        api,host=self.api();host.recover=Mock(return_value=self.unknown())
        response=await self.call(api)
        self.assertIn((b'cache-control',b'no-store'),response.sent[0]['headers'])
        self.assertIn((b'x-content-type-options',b'nosniff'),response.sent[0]['headers'])
        for value in ('opaque','offline-assertion','principal','secret','cookie'):
            self.assertNotIn(value,str(response.payload)+str(api.observations))
        self.assertTrue(api.observations[-1].response_completed)

    async def test_disconnect_during_body_receipt_never_calls_host(self):
        api,host=self.api();host.recover=Mock()
        connection=Connection(events=[{'type':'http.disconnect'}])
        await self.call(api,connection=connection)
        host.recover.assert_not_called();self.assertEqual(connection.sent,[])
        self.assertEqual(api.observations[-1].code,'CLIENT_DISCONNECTED')

    async def test_body_wait_inherits_parent_budget(self):
        api,host=self.api(timeout=2);host.recover=Mock()
        connection=Connection(events=[dict(type='http.request',body=b'{',more_body=True)])
        with self.assertRaises(OperationStopped), operation_scope(OperationSettings(1)):
            await self.call(api,connection=connection)
        host.recover.assert_not_called()
        self.assertEqual(api.observations[-1].code,'OPERATION_DEADLINE_EXCEEDED')

    async def test_bounded_admission_does_not_call_or_stop_existing_owner(self):
        api,host=self.api(capacity=1)
        entered=Event();release=Event()
        def run(*args,**kwargs):
            entered.set();release.wait(1);return self.unknown()
        host.recover=Mock(side_effect=run)
        first=Connection();task=asyncio.create_task(self.call(api,connection=first))
        await self.wait_for(entered.is_set)
        second=await self.call(api)
        self.assertEqual(second.status,503);self.assertEqual(second.payload['error'],'SERVICE_BUSY')
        release.set();await task
        self.assertEqual(first.status,200);self.assertEqual(host.recover.call_count,1)
        self.assertFalse(host.retired)

    async def test_request_cancel_is_propagated_and_supervisor_is_joined(self):
        api,host=self.api()
        entered=Event();finished=Event()
        def run(*args,cancellation,**kwargs):
            entered.set()
            while not cancellation.cancelled:time.sleep(.005)
            finished.set();return self.unknown()
        host.recover=Mock(side_effect=run)
        connection=Connection();task=asyncio.create_task(self.call(api,connection=connection))
        await self.wait_for(entered.is_set);task.cancel()
        with self.assertRaises(asyncio.CancelledError):await task
        self.assertTrue(finished.is_set());self.assertEqual(api.active_requests,0)
        self.assertEqual(connection.sent,[])

    async def test_server_teardown_cancels_body_wait_and_closes_host(self):
        api,host=self.api()
        connection=Connection(events=[dict(type='http.request',body=b'{',more_body=True)])
        task=asyncio.create_task(self.call(api,connection=connection))
        await self.wait_for(lambda:api.active_requests==1)
        self.assertTrue(await api.aclose());await task
        self.assertEqual(api.active_requests,0);self.assertTrue(host.retired)
        self.assertEqual((await self.call(api)).status,503)
        self.assertTrue(await api.aclose())

    async def test_send_failure_withholds_later_body_and_records_uncertain_delivery(self):
        api,host=self.api();host.submit=Mock(return_value=self.unknown())
        async def fail(event):
            if event['type']=='http.response.body':raise OSError('client went away')
        connection=Connection(send_hook=fail)
        await self.call(api,scope('submit'),connection)
        self.assertLessEqual(len(connection.sent),1);self.assertFalse(api.observations[-1].response_completed)
        self.assertFalse(any(event['type']=='http.response.body' for event in connection.sent))
        self.assertEqual(api.observations[-1].code,'RESPONSE_LOST');host.submit.assert_called_once()

    async def test_disconnect_between_response_start_and_body_withholds_body(self):
        api,host=self.api();host.recover=Mock(return_value=self.unknown())
        connection=Connection()
        async def disconnect(event):
            if event['type']=='http.response.start':
                connection.disconnect();await asyncio.sleep(.04)
        connection.send_hook=disconnect
        await self.call(api,connection=connection)
        self.assertLessEqual(len(connection.sent),1);self.assertFalse(api.observations[-1].response_completed)
        self.assertFalse(any(event['type']=='http.response.body' for event in connection.sent))

    async def test_blocked_response_uses_same_budget_and_no_late_body(self):
        api,host=self.api(timeout=1);host.recover=Mock(return_value=self.unknown())
        async def blocked(event):await asyncio.sleep(60)
        connection=Connection(send_hook=blocked)
        start=time.monotonic();await self.call(api,connection=connection)
        self.assertLess(time.monotonic()-start,1.4);self.assertEqual(connection.sent,[])
        self.assertEqual(api.observations[-1].code,'OPERATION_DEADLINE_EXCEEDED')

    @unittest.skipUnless(os.name=='posix','Owned POSIX workers')
    async def test_cancelled_http_waiter_preserves_active_owners_worker(self):
        api,host=self.process_api('slow-composed')
        body=json.dumps({'token':self.token}).encode()
        first=Connection(body);owner=asyncio.create_task(self.call(api,scope('submit'),first))
        await self.wait_for(lambda:any('working' in row for row in self.rows()))
        second=Connection(body);waiter=asyncio.create_task(self.call(api,scope('recover'),second))
        await self.wait_for(lambda:api.active_requests==2)
        second.disconnect();await waiter;await owner
        self.assertEqual(first.status,200);self.assertEqual(second.sent,[])
        self.assertFalse(host.retired);self.assertEqual(api.active_requests,0)
        self.assertEqual(len([row for row in self.rows() if 'insert' in row]),1)

    @unittest.skipUnless(os.name=='posix','Owned POSIX workers')
    async def test_duplicate_http_submissions_reuse_original_job(self):
        api,host=self.process_api()
        body=json.dumps({'token':self.token}).encode()
        first,second=await asyncio.gather(self.call(api,scope('submit'),Connection(body)),
                                        self.call(api,scope('submit'),Connection(body)))
        self.assertEqual(first.status,200);self.assertEqual(second.status,200)
        self.assertEqual(first.payload['result'],second.payload['result'])
        self.assertEqual(len([row for row in self.rows() if 'insert' in row]),1)
        self.assertFalse(host.retired)

    @unittest.skipUnless(os.name=='posix','Owned POSIX workers')
    async def test_teardown_of_active_real_worker_is_bounded_and_terminal(self):
        api,host=self.process_api('hang')
        connection=Connection(json.dumps({'token':self.token}).encode())
        request=asyncio.create_task(self.call(api,connection=connection))
        await self.wait_for(lambda:bool(self.rows()))
        start=time.monotonic();self.assertTrue(await api.aclose());await request
        self.assertLess(time.monotonic()-start,.8)
        self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)
        self.assertEqual(connection.sent,[]);self.assertEqual(api.active_requests,0)

    @unittest.skipUnless(os.name=='posix','Owned POSIX workers')
    async def test_real_worker_hang_disconnect_retires_and_joins(self):
        api,host=self.process_api('hang')
        connection=Connection(json.dumps({'token':self.token}).encode())
        task=asyncio.create_task(self.call(api,connection=connection))
        await self.wait_for(lambda:bool(self.rows()))
        start=time.monotonic();connection.disconnect();await task
        self.assertLess(time.monotonic()-start,.8)
        self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)
        self.assertEqual(api.active_requests,0);self.assertEqual(connection.sent,[])

    @unittest.skipUnless(os.name=='posix','Owned POSIX workers')
    async def test_lost_committed_response_recovers_original_without_second_submission(self):
        api,host=self.process_api('committed-hang')
        connection=Connection(json.dumps({'token':self.token}).encode())
        task=asyncio.create_task(self.call(api,scope('submit'),connection))
        await self.wait_for(lambda:any('insert' in row for row in self.rows()))
        original=next(row['insert'] for row in self.rows() if 'insert' in row)
        connection.disconnect();await task
        self.assertTrue(host.retired);self.assertEqual(connection.sent,[])
        recovery,_=self.process_api('recover')
        response=await self.call(recovery,scope('recover'),Connection(json.dumps({'token':self.token}).encode()))
        self.assertEqual(response.status,200)
        self.assertEqual(response.payload['result']['observation']['query_job_id'],original)
        self.assertEqual(response.payload['result']['observation']['status'],'COMMITTED')
        self.assertEqual(len([row for row in self.rows() if 'insert' in row]),1)

    @unittest.skipUnless(os.name=='posix','Owned POSIX workers')
    async def test_current_membership_denial_flows_through_http(self):
        api,host=self.process_api('access-denied')
        response=await self.call(api,connection=Connection(json.dumps({'token':self.token}).encode()))
        self.assertEqual(response.status,403);self.assertEqual(response.payload['error'],'ACCESS_DENIED')
        self.assertFalse(host.retired);self.assertFalse(any('insert' in row for row in self.rows()))

    @unittest.skipUnless(os.name=='posix','Owned POSIX workers')
    async def test_real_iap_signature_tampering_rejected_before_bigquery(self):
        signer,public=signer_and_public_key()
        Path(self.path+'.public.json').write_text(json.dumps({'fixture-key':public}))
        api,host=self.process_api('real-iap')
        claims=dict(iss=ISSUER,aud=AUDIENCE,sub=SUBJECT,iat=EPOCH-1,exp=EPOCH+599)
        assertion=jwt.encode(signer,claims).decode('ascii')
        request=scope();request['headers'][-1]=(ASSERTION_HEADER.encode(),(assertion+'x').encode())
        body=json.dumps({'token':self.token}).encode()
        self.assertEqual((await self.call(api,request,Connection(body))).status,403)
        self.assertEqual(self.rows(),[])
        request['headers'][-1]=(ASSERTION_HEADER.encode(),assertion.encode())
        response=await self.call(api,request,Connection(body))
        self.assertEqual(response.status,200);self.assertTrue(any('cloud_read' in row for row in self.rows()))
        self.assertFalse(host.retired)

    async def test_diagnostics_bounded_and_not_a_durable_audit(self):
        api,_=self.api(enabled=False)
        for _ in range(260):await self.call(api)
        self.assertEqual(len(api.observations),256)
        self.assertTrue(all(not row.worker_requested for row in api.observations))


class ApiSettingsTests(unittest.TestCase):
    def test_defaults_disabled_and_explicit_https_origins_required(self):
        self.assertFalse(SharingApiSettings().enabled)
        for origins in ((),('http://local.test',),('https://user@host.test',),('https://host.test/',),
            ('https://host.test?q=1',),('https://host.test#fragment',),('https://host.test:bad',),
            ('https://host.test',)*2,('*',),([],)):
            with self.subTest(origins=origins),self.assertRaises(ValueError):
                SharingApiSettings(True,origins)

    def test_typed_server_configuration_and_capacity_required(self):
        for capacity in (True,0,33,1.5):
            with self.assertRaises(ValueError):SharingApiSettings(max_active_requests=capacity)
        with self.assertRaises(ValueError):SharingApi(Mock())


if __name__=='__main__':unittest.main()
