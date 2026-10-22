"""4.4 Critical/Must have/shared pilot: actual SDKs and mocked HTTP, no cloud I/O."""
import base64
import json
import unittest
from unittest.mock import Mock, patch

from google.auth.credentials import AnonymousCredentials, Credentials
from google.cloud import bigquery, secretmanager_v1
import requests

from src.application.operation_budget import CancellationSignal, OperationSettings, OperationStopped, operation_scope
from src.application.operation_transport import (
    OperationSession, OperationRequest, OperationAuthorizedSession, RuntimeClientError,
    create_runtime_cloud_clients,create_workflow_read_clients,
)
from src.application.sharing_secret_loader import REGIONAL_ENDPOINT, SharingSecretLoader
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT


def response(request, payload=None, status=200):
    result=requests.Response()
    result.status_code=status
    result._content=json.dumps({} if payload is None else payload).encode()
    result.headers={'Content-Type':'application/json'}
    result.request=request;result.url=request.url
    return result


class RefreshCredentials(Credentials):
    def __init__(self):
        super().__init__();self.refresh_count=0

    def refresh(self, request):
        self.refresh_count+=1
        request('https://oauth2.googleapis.com/token',method='POST',body=b'fixture')
        self.token='offline-token'


class OperationTransportTests(unittest.TestCase):
    def setUp(self):
        self.tick=100.0
        p=patch('src.application.operation_budget.time.monotonic',side_effect=lambda:self.tick)
        p.start();self.addCleanup(p.stop)
        self.wire=[]
        p=patch('requests.adapters.HTTPAdapter.send',side_effect=self.send)
        self.adapter=p.start();self.addCleanup(p.stop)

    def send(self, request, **kwargs):
        self.wire.append((request,kwargs))
        return response(request)

    def session(self, credentials=None):
        session=OperationAuthorizedSession(AnonymousCredentials() if credentials is None else credentials)
        self.addCleanup(session.close)
        return session

    def clients(self):
        with patch('src.application.operation_transport.google.auth.default',
                   return_value=(AnonymousCredentials(),'ignored-adc-project')):
            clients=create_runtime_cloud_clients()
        self.addCleanup(clients.close)
        return clients

    def test_public_request_has_finite_default_and_zero_adapter_retries(self):
        with OperationSession() as session:
            session.get('https://www.gstatic.com/iap/verify/public_key')
            self.assertEqual(self.wire[-1][1]['timeout'],20)
            self.assertEqual(session.get_adapter('https://example.com').max_retries.total,0)

    def test_auth_request_cap_is_recomputed_at_wire(self):
        with OperationSession() as session,operation_scope(OperationSettings(10)):
            request=OperationRequest(session);self.tick+=8
            request('https://oauth2.googleapis.com/token',timeout=120)
            self.assertEqual(self.wire[-1][1]['timeout'],2)

    def test_adc_refresh_consumes_budget_before_service_send(self):
        credentials=RefreshCredentials();session=self.session(credentials)
        original=self.send
        def send(request,**kwargs):
            result=original(request,**kwargs)
            if request.url.endswith('/token'):self.tick+=4
            return result
        self.adapter.side_effect=send
        with operation_scope(OperationSettings(10)):
            session.get('https://bigquery.googleapis.com/fixture',timeout=15)
        self.assertEqual([call[1]['timeout'] for call in self.wire],[10,6])
        self.assertEqual(credentials.refresh_count,1)

    def test_expired_adc_refresh_never_sends_service_request(self):
        credentials=RefreshCredentials();session=self.session(credentials)
        def slow(request,**kwargs):
            result=self.send(request,**kwargs);self.tick+=10;return result
        self.adapter.side_effect=slow
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):session.get('https://bigquery.googleapis.com/fixture')
        self.assertEqual(len(self.wire),1)
        self.assertTrue(self.wire[0][0].url.endswith('/token'))

    def test_401_never_replays_service_or_refreshes_credentials_again(self):
        credentials=RefreshCredentials();session=self.session(credentials)
        def denied(request,**kwargs):
            self.wire.append((request,kwargs))
            return response(request,status=200 if request.url.endswith('/token') else 401)
        self.adapter.side_effect=denied
        with operation_scope(OperationSettings(10)):
            result=session.post('https://bigquery.googleapis.com/fixture',json={'stable':'job-id'})
        self.assertEqual(result.status_code,401)
        self.assertEqual(len(self.wire),2)
        self.assertEqual(credentials.refresh_count,1)

    def test_redirect_never_replays_mutation(self):
        def redirect(request,**kwargs):
            result=self.send(request,**kwargs);result.status_code=307
            result.headers['Location']='https://bigquery.googleapis.com/second'
            return result
        self.adapter.side_effect=redirect
        session=self.session()
        with operation_scope(OperationSettings(10)):
            result=session.post('https://bigquery.googleapis.com/fixture',json={'stable':'job-id'})
        self.assertEqual(result.status_code,307)
        self.assertEqual(len(self.wire),1)

    def test_connect_read_tuple_is_capped_without_infinite_elements(self):
        session=self.session()
        with operation_scope(OperationSettings(10)):
            self.tick+=7
            session.get('https://bigquery.googleapis.com/fixture',timeout=(1,None))
        self.assertEqual(self.wire[-1][1]['timeout'],(1,3))

    def test_invalid_timeout_never_reaches_network(self):
        session=self.session()
        for timeout in (0,True,float('inf'),(1,0),(1,2,3),'10'):
            with self.subTest(timeout=timeout),self.assertRaises(ValueError):
                session.get('https://bigquery.googleapis.com/fixture',timeout=timeout)
        self.adapter.assert_not_called()

    def test_cancelled_session_denies_before_adc_or_network(self):
        credentials=RefreshCredentials();session=self.session(credentials);signal=CancellationSignal()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10),signal):
                signal.cancel();session.get('https://bigquery.googleapis.com/fixture')
        self.adapter.assert_not_called();self.assertEqual(credentials.refresh_count,0)

    def test_late_network_exception_reports_safe_stop(self):
        def fail(*args,**kwargs):self.tick+=10;raise requests.ConnectionError('private provider body')
        self.adapter.side_effect=fail
        with self.assertRaises(OperationStopped) as caught:
            with operation_scope(OperationSettings(10)):
                self.session().get('https://bigquery.googleapis.com/fixture')
        self.assertEqual(str(caught.exception),'OPERATION_DEADLINE_EXCEEDED')

    def test_factory_uses_explicit_adc_request_and_fixed_scope(self):
        with patch('src.application.operation_transport.google.auth.default',
                   return_value=(AnonymousCredentials(),'wrong-adc-project')) as adc:
            clients=create_runtime_cloud_clients()
        self.addCleanup(clients.close)
        self.assertIsInstance(adc.call_args.kwargs['request'],OperationRequest)
        self.assertIsInstance(clients.bigquery,bigquery.Client)
        self.assertEqual((clients.bigquery.project,clients.bigquery.location),(PROJECT,LOCATION))
        self.assertIsInstance(clients.bigquery._http,OperationAuthorizedSession)
        self.assertIsInstance(clients.secrets,secretmanager_v1.SecretManagerServiceClient)
        self.assertEqual(clients.secrets.api_endpoint,'https://' + REGIONAL_ENDPOINT)
        SharingSecretLoader(client=clients.secrets)
        self.assertIsInstance(clients.secrets.transport._session,OperationAuthorizedSession)
        self.adapter.assert_not_called()

    def test_factory_deadline_in_adc_fails_without_client_fallback(self):
        def adc(**kwargs):self.tick+=10;return AnonymousCredentials(),PROJECT
        with patch('src.application.operation_transport.google.auth.default',side_effect=adc) as discovery:
            with self.assertRaises(OperationStopped):create_runtime_cloud_clients(settings=OperationSettings(10))
        discovery.assert_called_once();self.adapter.assert_not_called()

    def test_factory_adc_failure_is_redacted_and_never_falls_back(self):
        with patch('src.application.operation_transport.google.auth.default',
                   side_effect=ValueError('private ADC file content')) as discovery:
            with self.assertRaises(RuntimeClientError) as caught:create_runtime_cloud_clients()
        self.assertEqual(str(caught.exception),'RUNTIME_CLOUD_CLIENTS_UNAVAILABLE')
        discovery.assert_called_once();self.adapter.assert_not_called()

    def test_partial_factory_failure_closes_already_created_sessions(self):
        with patch('src.application.operation_transport.google.auth.default',
                   return_value=(AnonymousCredentials(),PROJECT)),\
                patch('src.application.operation_transport._OperationSecretRestTransport',side_effect=ValueError()),\
                patch.object(OperationAuthorizedSession,'close',autospec=True) as close:
            with self.assertRaises(RuntimeClientError):create_runtime_cloud_clients()
        self.assertEqual(close.call_count,1)

    def test_secret_rest_wrapper_failure_closes_replacement_session(self):
        from src.application.operation_transport import _OperationSecretRestTransport
        prepare=_OperationSecretRestTransport._prep_wrapped_messages
        count=[0]
        def incompatible(self,info):
            count[0]+=1
            if count[0]==2:raise ValueError('SDK wrapper compatibility failure')
            return prepare(self,info)
        with patch('src.application.operation_transport.google.auth.default',
                   return_value=(AnonymousCredentials(),PROJECT)),\
                patch.object(_OperationSecretRestTransport,'_prep_wrapped_messages',incompatible),\
                patch.object(OperationAuthorizedSession,'close',autospec=True) as close:
            with self.assertRaises(RuntimeClientError):create_runtime_cloud_clients()
        self.assertEqual(close.call_count,2)  # Replacement REST session and prior BQ client.

    def test_authorized_session_constructor_failure_closes_auth_session(self):
        with patch('google.auth.transport.requests.AuthorizedSession.__init__',side_effect=ValueError()),\
                patch.object(OperationSession,'close',autospec=True) as close:
            with self.assertRaises(ValueError):OperationAuthorizedSession(AnonymousCredentials())
        close.assert_called_once()

    def test_bigquery_lazy_pages_recompute_remaining_at_wire(self):
        clients=self.clients();number=0
        def page(request,**kwargs):
            nonlocal number
            number+=1;self.wire.append((request,kwargs));self.tick+=3
            payload={'jobs':[{'jobReference':{'projectId':PROJECT,'jobId':f'fixture_{number}',
                'location':LOCATION},'configuration':{'query':{'query':'SELECT 1'}},'status':{'state':'DONE'}}]}
            if number==1:payload['nextPageToken']='next'
            return response(request,payload)
        self.adapter.side_effect=page
        with operation_scope(OperationSettings(10)):
            values=clients.bigquery.list_jobs(max_results=2,page_size=1,retry=None,timeout=15)
            self.tick+=2
            self.assertEqual(len(list(values)),2)
        self.assertEqual([call[1]['timeout'] for call in self.wire],[8,5])

    def test_expired_lazy_page_never_reaches_network(self):
        clients=self.clients()
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):
                values=clients.bigquery.list_jobs(retry=None,timeout=15)
                self.tick+=10;list(values)
        self.adapter.assert_not_called()

    def test_actual_secret_rest_rpc_uses_regional_route_and_remaining_timeout(self):
        clients=self.clients()
        name=f'projects/123/locations/{LOCATION}/secrets/offline-fixture/versions/1'
        def secret(request,**kwargs):
            self.wire.append((request,kwargs))
            return response(request,{'name':name,'payload':{'data':base64.b64encode(b'fixture').decode(),
                                                          'dataCrc32c':'123'}})
        self.adapter.side_effect=secret
        with operation_scope(OperationSettings(10)):
            self.tick+=8
            result=clients.secrets.access_secret_version(request={'name':name},retry=None,timeout=5)
        self.assertEqual(result.name,name)
        self.assertIn(REGIONAL_ENDPOINT,self.wire[0][0].url)
        self.assertEqual(self.wire[0][1]['timeout'],2)

    def test_actual_query_polling_recomputes_each_rpc_until_stop(self):
        clients=self.clients()
        job=bigquery.QueryJob('offline_poll','SELECT 1',clients.bigquery)
        job._properties['jobReference'].update(projectId=PROJECT,location=LOCATION)
        job._properties['status']={'state':'RUNNING'}
        def poll(request,**kwargs):
            self.wire.append((request,kwargs));self.tick+=3
            if '/queries/' in request.url:
                payload={'jobComplete':False,'schema':{'fields':[]},'totalRows':'0'}
            else:
                payload={'jobReference':{'jobId':job.job_id,'projectId':PROJECT,'location':LOCATION},
                         'configuration':{'query':{'query':'SELECT 1'}},'status':{'state':'RUNNING'}}
            return response(request,payload)
        self.adapter.side_effect=poll
        with self.assertRaises(OperationStopped):
            with operation_scope(OperationSettings(10)):job.result(timeout=10,retry=None,job_retry=None)
        self.assertEqual([call[1]['timeout'] for call in self.wire],[10,7,4,1])
        self.assertTrue(all(call[0].method=='GET' for call in self.wire))

    def test_actual_bigquery_lost_insert_response_has_one_stable_submission(self):
        clients=self.clients()
        def lost(request,**kwargs):
            self.wire.append((request,kwargs));raise requests.ConnectionError('discarded response')
        self.adapter.side_effect=lost
        config=bigquery.QueryJobConfig(job_timeout_ms=60000,use_query_cache=False)
        with operation_scope(OperationSettings(10)),self.assertRaises(requests.ConnectionError):
            self.tick+=7
            clients.bigquery.query('SELECT 1',job_id='offline_stable_job',job_config=config,
                retry=None,job_retry=None,timeout=20,api_method=bigquery.enums.QueryApiMethod.INSERT)
        self.assertEqual(len(self.wire),1)
        request,kwargs=self.wire[0]
        self.assertEqual(kwargs['timeout'],3)
        body=json.loads(request.body)
        self.assertEqual(body['jobReference']['jobId'],'offline_stable_job')
        self.assertEqual(body['configuration']['jobTimeoutMs'],'60000')

    def test_secret_rest_loader_composes_with_owned_adc_transport(self):
        from tests import test_sharing_secret_loader as secrets
        from tests import test_sharing_signing as signing
        clients=self.clients();cfg=secrets.config()
        def secret(request,**kwargs):
            self.wire.append((request,kwargs))
            binding=next(item for item in cfg.versions if cfg.name(item) in request.url)
            payload=secrets.response(cfg.name(binding),signing.MATERIAL[binding.key_id])
            return response(request,{'name':payload.name,'payload':{
                'data':base64.b64encode(payload.payload.data).decode(),
                'dataCrc32c':str(payload.payload.data_crc32c)}})
        self.adapter.side_effect=secret
        with patch('src.application.sharing_secret_loader._now',return_value=signing.NOW),\
                patch('src.application.sharing_retained_sealer._now',return_value=signing.NOW),\
                operation_scope(OperationSettings(10)):
            self.tick+=7
            sealer=SharingSecretLoader(client=clients.secrets).load(cfg)
        self.assertEqual(sealer.policy,cfg.policy)
        self.assertEqual([call[1]['timeout'] for call in self.wire],[3,3])

    def test_client_cleanup_closes_both_service_transports(self):
        clients=self.clients()
        with patch.object(clients.bigquery,'close') as bq,patch.object(clients.secrets.transport,'close') as sm:
            clients.close()
        bq.assert_called_once();sm.assert_called_once()

    def test_read_factory_owns_budget_aware_bigquery_and_gcs_without_secret_access(self):
        with patch('src.application.operation_transport.google.auth.default',return_value=(AnonymousCredentials(),'wrong-adc-project')):
            clients=create_workflow_read_clients()
        self.addCleanup(clients.close)
        self.assertEqual(clients.bigquery.project,PROJECT);self.assertEqual(clients.bigquery.location,LOCATION)
        self.assertEqual(clients.storage.project,PROJECT)
        self.assertIsInstance(clients.storage._http,OperationAuthorizedSession);self.adapter.assert_not_called()

    def test_read_factory_partial_gcs_failure_closes_both_owned_sessions(self):
        with patch('src.application.operation_transport.google.auth.default',return_value=(AnonymousCredentials(),PROJECT)),\
                patch('src.application.operation_transport.storage.Client',side_effect=ValueError('private failure')),\
                patch.object(OperationAuthorizedSession,'close',autospec=True) as close:
            with self.assertRaises(RuntimeClientError):create_workflow_read_clients()
        self.assertEqual(close.call_count,2)

    def test_read_factory_adc_expiry_does_not_construct_clients(self):
        def adc(**kwargs):self.tick+=10;return AnonymousCredentials(),PROJECT
        with patch('src.application.operation_transport.google.auth.default',side_effect=adc),\
                patch('src.application.operation_transport.storage.Client') as gcs:
            with self.assertRaises(OperationStopped):create_workflow_read_clients(settings=OperationSettings(10))
        gcs.assert_not_called()


if __name__=='__main__':unittest.main()
