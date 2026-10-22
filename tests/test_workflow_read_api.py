"""Real IAP signatures + owned read-worker API; cloud methods mocked offline."""
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from google.auth import jwt

from src.application.identity import ASSERTION_HEADER,ISSUER,IapSettings
from src.application.operation_budget import OperationSettings
from src.application.sharing_api import SharingApiSettings
from src.application.sharing_operation_host import SharingHostSettings
from src.application.workflow_read_api import WorkflowReadApi
from src.application.workflow_read_host import WorkflowReadConfig, WorkflowReadHost,config_from_wire,request_values
from src.application.operation_channel import ChannelError
from tests.test_identity import AUDIENCE,signer_and_public_key
from tests.test_protected_service import OWNER,WORKSPACE,EPOCH,SUBJECT
from tests.test_sharing_api import Connection,ORIGIN


@unittest.skipUnless(os.name=='posix','Owned POSIX workflow read worker')
class ReadApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp=TemporaryDirectory();self.addCleanup(temp.cleanup)
        self.path=str(Path(temp.name)/'public.json')
        self.signer,public=signer_and_public_key();Path(self.path).write_text(json.dumps({'fixture-key':public}))
        self.apis=[]

    async def asyncTearDown(self):
        for api in self.apis:await api.aclose()

    def api(self,mode='normal',*,enabled=True):
        host=WorkflowReadHost(WorkflowReadConfig(IapSettings(AUDIENCE),True,OperationSettings(2)),
                              SharingHostSettings(enabled=True,cleanup_seconds=.3))
        command=lambda fd:[sys.executable,'-m','tests.workflow_read_worker_fixture',str(fd),mode,self.path]
        with patch.object(host,'_command',side_effect=command):host.start()
        api=WorkflowReadApi(host,SharingApiSettings(enabled,(ORIGIN,)));self.apis.append(api)
        return api,host

    def request(self,operation,*,subject=SUBJECT):
        token=jwt.encode(self.signer,dict(iss=ISSUER,aud=AUDIENCE,sub=subject,iat=EPOCH-1,exp=EPOCH+599)).decode()
        return dict(type='http',method='POST',path='/v1/workflow/'+operation,query_string=b'',headers=[
            (b'origin',ORIGIN.encode()),(b'content-type',b'application/json'),(ASSERTION_HEADER.encode(),token.encode())])

    async def call(self,api,operation,*,artifact=None,subject=SUBJECT,connection=None):
        params=dict(workspace_id=WORKSPACE,job_id='test-job')
        if artifact is not None:params['artifact_id']=artifact
        connection=Connection(json.dumps(params).encode()) if connection is None else connection
        await api(self.request(operation,subject=subject),connection.receive,connection.send)
        return connection

    async def test_status_and_output_metadata_are_protected_minimal_and_audit_linked(self):
        api,host=self.api()
        for operation in ('get_job_status','get_output_summary'):
            response=await self.call(api,operation)
            self.assertEqual(response.status,200)
            self.assertEqual(response.payload['version'],'workflow-read-api-v1')
            self.assertEqual(response.payload['result']['job_id'],'test-job')
            self.assertNotIn(OWNER,str(response.payload))
            capture=host.captures[-1]
            self.assertEqual(capture.trace_id,response.payload['request_id'])
            self.assertEqual(capture.decision.principal_id,OWNER)
            self.assertTrue(capture.decision.allowed)
            self.assertEqual(capture.target.job_id,'test-job')
            if operation=='get_output_summary':
                downloads=response.payload['result']['downloads']
                self.assertTrue(any(item['kind']=='BANNER_PNG' for item in downloads))
                self.assertTrue(all(set(item)=={'artifact_id','kind','filename',
                    'content_type','size_bytes','sha256'} for item in downloads))
                self.assertNotIn('gcs_uri',str(downloads));self.assertNotIn('gcs_generation',str(downloads))

    async def test_verified_binary_download_has_safe_headers_and_no_gcs_or_actor(self):
        api,host=self.api()
        response=await self.call(api,'download_artifact',artifact='test-banner')
        self.assertEqual(response.status,200)
        headers=dict(response.sent[0]['headers']);raw=response.sent[-1]['body']
        self.assertEqual(headers[b'content-type'],b'image/png')
        self.assertEqual(headers[b'content-disposition'],b'attachment; filename="test-banner.png"')
        self.assertEqual(headers[b'x-content-sha256'].decode(),hashlib.sha256(raw).hexdigest())
        self.assertEqual(headers[b'cache-control'],b'no-store')
        self.assertNotIn(b'gs://',str(headers).encode());self.assertNotIn(OWNER,str(headers))
        self.assertEqual(host.captures[-1].decision.action.value,'DOWNLOAD_ARTIFACT')
        self.assertEqual(host.captures[-1].trace_id,headers[b'x-request-id'].decode())

    async def test_large_download_spans_many_bounded_ipc_frames(self):
        api,_=self.api('large')
        response=await self.call(api,'download_artifact',artifact='test-text')
        self.assertEqual(response.status,200);self.assertEqual(response.sent[-1]['body'],b'POC draft\n'*12000)

    async def test_revocation_during_gcs_denies_byte_release_and_captures_denial(self):
        api,host=self.api('revoked')
        response=await self.call(api,'download_artifact',artifact='test-banner')
        self.assertEqual(response.status,403);self.assertEqual(response.payload['error'],'ACCESS_DENIED')
        self.assertFalse(host.captures[-1].decision.allowed);self.assertFalse(host.retired)

    async def test_valid_different_principal_cannot_read_private_job(self):
        api,host=self.api()
        response=await self.call(api,'get_job_status',subject='accounts.google.com:another')
        self.assertEqual(response.status,403);self.assertNotIn(OWNER,str(response.payload))
        self.assertFalse(host.retired)

    async def test_hung_read_disconnect_cleans_up_owned_worker(self):
        api,host=self.api('hang')
        connection=Connection(json.dumps(dict(workspace_id=WORKSPACE,job_id='test-job')).encode())
        task=asyncio.create_task(self.call(api,'get_job_status',connection=connection))
        await asyncio.sleep(.2);connection.disconnect();await task
        self.assertEqual(connection.sent,[]);self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)

    async def test_disabled_api_does_not_dispatch(self):
        api,host=self.api(enabled=False)
        response=await self.call(api,'get_job_status')
        self.assertEqual(response.status,503);self.assertEqual(host.captures,())

    async def test_invalid_iap_audience_is_anonymous_denial_with_no_resource_leak(self):
        api,host=self.api()
        request=self.request('get_job_status')
        token=jwt.encode(self.signer,dict(iss=ISSUER,aud='foreign-service',sub=SUBJECT,iat=EPOCH-1,exp=EPOCH+599)).decode()
        request['headers'][-1]=(ASSERTION_HEADER.encode(),token.encode())
        connection=Connection(json.dumps(dict(workspace_id=WORKSPACE,job_id='test-job')).encode())
        await api(request,connection.receive,connection.send)
        self.assertEqual(connection.status,403)
        capture=host.captures[-1];self.assertIsNone(capture.decision.principal_id);self.assertIsNone(capture.target)
        self.assertNotIn(SUBJECT,str(connection.payload))


class ReadContractTests(unittest.TestCase):
    def test_disabled_config_roundtrip_and_exact_parameters(self):
        config=WorkflowReadConfig(IapSettings(AUDIENCE))
        self.assertFalse(config.enabled);self.assertEqual(config_from_wire(asdict(config)),config)
        for params in (dict(workspace_id='ws',job_id='job',owner_id='caller'),
                       dict(workspace_id='ws',job_id='../job')):
            with self.assertRaises(ChannelError):request_values('get_job_status',{},params)


if __name__=='__main__':unittest.main()
