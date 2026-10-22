"""Management HTTP projection and owned-process cancellation with real IAP JWTs."""
import asyncio
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from google.auth import jwt

from src.application.identity import ASSERTION_HEADER,ISSUER,IapSettings
from src.application.management_service import ManagementSettings, WorkspaceBootstrapManifest
from src.application.operation_budget import OperationSettings
from src.application.sharing_api import SharingApiSettings
from src.application.sharing_operation_host import SharingHostSettings
from src.application.workflow_control_api import WorkflowControlApi
from src.application.workflow_control_host import WorkflowControlConfig,WorkflowControlHost,config_from_wire,request_values
from src.application.operation_channel import ChannelError
from tests.test_identity import AUDIENCE,signer_and_public_key
from tests.test_management_service import SUBJECT,WS,ACTOR
from tests.test_sharing_api import Connection,ORIGIN


@unittest.skipUnless(os.name=='posix','Owned POSIX control worker')
class ControlApiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp=TemporaryDirectory();self.addCleanup(temp.cleanup);self.path=str(Path(temp.name)/'public.json')
        self.signer,public=signer_and_public_key();Path(self.path).write_text(json.dumps({'fixture-key':public}))
        self.apis=[]
    async def asyncTearDown(self):
        for api in self.apis:await api.aclose()
    def api(self,mode='normal'):
        host=WorkflowControlHost(WorkflowControlConfig(IapSettings(AUDIENCE),ManagementSettings(enabled=True,operation=OperationSettings(2))),
            SharingHostSettings(enabled=True,cleanup_seconds=.3))
        with patch.object(host,'_command',side_effect=lambda fd:[sys.executable,'-m','tests.workflow_control_worker_fixture',str(fd),mode,self.path]):host.start()
        api=WorkflowControlApi(host,SharingApiSettings(True,(ORIGIN,)));self.apis.append(api);return api,host
    async def call(self,api,operation='manage',*,subject=SUBJECT,parameters=None,connection=None):
        epoch=int(datetime.now(timezone.utc).timestamp())
        token=jwt.encode(self.signer,dict(iss=ISSUER,aud=AUDIENCE,sub=subject,iat=epoch-1,exp=epoch+599)).decode()
        scope=dict(type='http',method='POST',path='/v1/control/'+operation,query_string=b'',headers=[
            (b'origin',ORIGIN.encode()),(b'content-type',b'application/json'),(ASSERTION_HEADER.encode(),token.encode())])
        parameters=parameters or dict(kind='MEMBER',workspace_id=WS,command_id='http-fixture',expected_workspace_revision=1,
            target_subject='accounts.google.com:new-fixture',roles=['VIEWER'],status='ACTIVE')
        connection=connection or Connection(json.dumps(parameters).encode());await api(scope,connection.receive,connection.send);return connection
    async def test_protected_management_returns_minimal_scope_not_membership_or_plan(self):
        api,_=self.api();response=await self.call(api)
        self.assertEqual(response.status,200);self.assertEqual(response.payload['version'],'workflow-control-api-v1')
        self.assertEqual(response.payload['result']['state'],'APPLIED')
        self.assertNotIn('config_json',str(response.payload));self.assertNotIn('identity_subject',str(response.payload))
    async def test_duplicate_and_lost_response_recovery_keep_original_native_id(self):
        api,_=self.api('lost');first=await self.call(api)
        self.assertEqual(first.payload['result']['native_status'],'UNKNOWN')
        second=await self.call(api,'recover_management',parameters=dict(workspace_id=WS,command_id='http-fixture'))
        self.assertEqual(second.payload['result']['native_status'],'COMMITTED')
        self.assertEqual(first.payload['result']['query_job_id'],second.payload['result']['query_job_id'])
    async def test_real_different_identity_has_no_admin_fallback(self):
        api,host=self.api();response=await self.call(api,subject='accounts.google.com:foreign')
        self.assertEqual(response.status,403);self.assertFalse(host.retired)
    async def test_browser_owner_or_raw_plan_fields_are_rejected(self):
        api,_=self.api();response=await self.call(api,parameters=dict(kind='MEMBER',workspace_id=WS,command_id='x',owner_id=ACTOR))
        self.assertEqual(response.status,400)
    async def test_hung_management_disconnect_retires_worker_and_joins(self):
        api,host=self.api('hang');connection=Connection(json.dumps(dict(kind='MEMBER',workspace_id=WS,command_id='x',expected_workspace_revision=1,
            target_subject='accounts.google.com:new',roles=['VIEWER'],status='ACTIVE')).encode())
        task=asyncio.create_task(self.call(api,connection=connection));await asyncio.sleep(.2);connection.disconnect();await task
        self.assertEqual(connection.sent,[]);self.assertTrue(host.retired);self.assertTrue(host.cleanup_complete)


class ControlConfigTests(unittest.TestCase):
    def test_manifest_dates_roundtrip_over_trusted_ipc_and_defaults_disabled(self):
        at=datetime.now(timezone.utc)
        config=WorkflowControlConfig(IapSettings(AUDIENCE),ManagementSettings(bootstrap=WorkspaceBootstrapManifest(WS,SUBJECT,('ADMIN',),'manifest',at,at+timedelta(minutes=4))))
        host=WorkflowControlHost(config)
        self.assertEqual(config_from_wire(host._config_values()),config);self.assertFalse(config.management.enabled)
    def test_recovery_does_not_accept_candidate_plan_or_roles(self):
        with self.assertRaises(ChannelError):request_values('recover_management',{},dict(workspace_id=WS,command_id='x',roles=['ADMIN']))


if __name__=='__main__':unittest.main()
