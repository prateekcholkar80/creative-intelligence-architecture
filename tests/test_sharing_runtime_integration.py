"""4.3 Critical/Must have/shared pilot: native probe containment, no RPCs."""
from datetime import timedelta
import json
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound
from google.auth.credentials import AnonymousCredentials
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery, secretmanager_v1 as sm

from src.validation import sharing_runtime_integration as probe


class RuntimeProbeGuardTests(unittest.TestCase):
    def setUp(self):
        self.client = bigquery.Client(project=probe.PROJECT, location=probe.LOCATION,
                                      credentials=AnonymousCredentials())
        self.secret_client = sm.SecretManagerServiceClient(credentials=AnonymousCredentials(),
            client_options={'api_endpoint': probe.REGIONAL_ENDPOINT})
        self.session = AuthorizedSession(AnonymousCredentials())
        self.addCleanup(self.client.close)
        self.addCleanup(self.secret_client.transport.close)
        self.addCleanup(self.session.close)
        self.runtime = probe.Probe(self.client, self.secret_client, self.session)
        self.runtime.number = '1042059373096'
        self.scope = self.runtime.scope

    def dataset(self):
        value = bigquery.Dataset(self.scope.dataset)
        value.location, value.labels, value.default_table_expiration_ms = (
            probe.LOCATION, self.scope.labels, probe.TTL_MS)
        return value

    def secret(self):
        at = self.runtime.started + timedelta(seconds=1)
        return sm.Secret(name=self.scope.secret(self.runtime.number), labels=self.scope.labels,
                         create_time=at, expire_time=at+timedelta(milliseconds=probe.TTL_MS), etag='owned')

    def binding(self, body=None, status=200):
        response = Mock(status_code=status)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        response.iter_content.return_value = [json.dumps(body or dict(projectId=probe.PROJECT,
            projectNumber=self.runtime.number, lifecycleState='ACTIVE')).encode()]
        self.session.get = Mock(return_value=response)
        return response

    def test_exact_eight_tables_are_owned_and_serving_sql_is_denied(self):
        self.assertEqual(len(probe.TABLES), 8)
        for source in (probe.INSTALLED_READ_SQL(), probe.INSTALLED_SHARING_SQL()):
            relocated = self.scope.relocate(source)
            self.assertNotIn(f'`{probe.DATASET_ID}.', relocated)
            with self.assertRaisesRegex(RuntimeError, 'UNSAFE_PROBE_SQL_SCOPE'):
                self.scope.assert_sql(relocated+f' SELECT * FROM `{probe.DATASET_ID}.workspaces`;')

    def test_scope_injection_and_unknown_tables_rejected(self):
        for run_id in ('', 'a'*31, 'A'*32, 'a'*32+';DROP', None):
            with self.subTest(run_id=run_id), self.assertRaises(RuntimeError):
                probe.ProbeScope(run_id)
        with self.assertRaises(RuntimeError):self.scope.table('product_master')
        for number in ('latest', '0', '01', 123, '../x', ''):
            with self.subTest(number=number), self.assertRaises(RuntimeError):self.scope.secret(number)

    def test_adc_project_number_is_read_from_fixed_bounded_response(self):
        self.binding()
        self.assertEqual(probe.read_project_number(self.session), self.runtime.number)
        self.session.get.assert_called_once_with(probe.PROJECT_URL, timeout=10,
                                                 allow_redirects=False, stream=True)

    def test_project_binding_rejects_foreign_inactive_and_nonnumeric(self):
        base = dict(projectId=probe.PROJECT, projectNumber=self.runtime.number, lifecycleState='ACTIVE')
        for fields in (dict(projectId='other'), dict(lifecycleState='DELETE_REQUESTED'),
                       dict(projectNumber=123), dict(projectNumber='01'), dict(projectNumber='latest')):
            self.binding(base | fields)
            with self.subTest(fields=fields), self.assertRaises(RuntimeError):probe.read_project_number(self.session)

    def test_project_redirect_failure_and_oversized_response_rejected(self):
        for status in (302, 403, 503):
            self.binding(status=status)
            with self.subTest(status=status), self.assertRaises(RuntimeError):probe.read_project_number(self.session)
        response = self.binding()
        response.iter_content.return_value = [b' '*16385]
        with self.assertRaisesRegex(RuntimeError, 'RESPONSE_TOO_LARGE'):probe.read_project_number(self.session)

    def test_project_binding_has_no_arbitrary_session_fallback(self):
        with self.assertRaisesRegex(RuntimeError, 'INVALID_PROJECT_BINDING_SESSION'):
            probe.read_project_number(Mock())

    def test_disabled_service_diagnostic_retains_only_safe_pins(self):
        response=self.binding(status=403)
        response.iter_content.return_value=[json.dumps(dict(error=dict(message='private provider details',
            details=[{'@type':'type.googleapis.com/google.rpc.ErrorInfo', 'reason':'SERVICE_DISABLED',
                      'metadata':dict(service='cloudresourcemanager.googleapis.com',
                                      consumer='projects/'+self.runtime.number, secret='never export')}]))).encode()]
        with self.assertRaises(probe.ProjectBindingError) as caught:probe.read_project_number(self.session)
        error=caught.exception
        self.assertEqual((error.status,error.reason,error.consumer),
                         (403,'SERVICE_DISABLED','projects/'+self.runtime.number))
        self.assertEqual(str(error),'PROJECT_BINDING_UNAVAILABLE')
        self.assertNotIn('private',vars(error).__repr__())
        self.assertNotIn('never export',vars(error).__repr__())

    def test_arbitrary_error_metadata_is_not_retained(self):
        response=self.binding(status=403)
        response.iter_content.return_value=[json.dumps(dict(error=dict(details=[
            {'@type':'type.googleapis.com/google.rpc.ErrorInfo', 'reason':'private text',
             'metadata':dict(service='private.endpoint',consumer='private identity')}]))).encode()]
        with self.assertRaises(probe.ProjectBindingError) as caught:probe.read_project_number(self.session)
        self.assertIsNone(caught.exception.reason)
        self.assertIsNone(caught.exception.consumer)
        self.assertIsNone(caught.exception.service)

    def test_failed_binding_prevents_any_resource_creation(self):
        self.client.create_dataset = Mock()
        self.secret_client.create_secret = Mock()
        self.binding(status=403)
        with self.assertRaises(RuntimeError):self.runtime.setup()
        self.client.create_dataset.assert_not_called()
        self.secret_client.create_secret.assert_not_called()
        self.assertFalse(self.runtime.dataset_attempted or self.runtime.secret_attempted)

    def test_failed_serving_region_preflight_prevents_creation(self):
        self.binding()
        self.client.get_dataset = Mock(side_effect=NotFound('fixture'))
        self.client.create_dataset = Mock()
        with self.assertRaises(NotFound):self.runtime.setup()
        self.client.create_dataset.assert_not_called()
        self.assertFalse(self.runtime.dataset_attempted)

    def test_secret_ownership_requires_labels_region_name_expiry_and_creation(self):
        self.scope.assert_secret(self.secret(), self.runtime.number, self.runtime.started)
        for fields in (dict(labels={}), dict(name='projects/1/locations/us-central1/secrets/other'),
                       dict(expire_time=self.runtime.started+timedelta(hours=1)),
                       dict(create_time=self.runtime.started-timedelta(days=1)),
                       dict(version_aliases={'latest':1}), dict(version_destroy_ttl={'seconds':1})):
            value = self.secret()
            for name, child in fields.items():setattr(value, name, child)
            with self.subTest(fields=fields), self.assertRaises(RuntimeError):
                self.scope.assert_secret(value, self.runtime.number, self.runtime.started)

    def test_dataset_ownership_requires_exact_labels_region_and_expiry(self):
        for name, child in (('labels',{}), ('location','US'), ('default_table_expiration_ms',None)):
            value = self.dataset();setattr(value,name,child)
            with self.subTest(name=name), self.assertRaises(RuntimeError):self.scope.assert_dataset(value)

    def test_server_ttl_creation_stamp_offset_is_bounded(self):
        value=self.secret()
        value.expire_time=value.expire_time-timedelta(microseconds=23987)
        self.scope.assert_secret(value,self.runtime.number,self.runtime.started)
        for offset in (-1,11):
            value=self.secret();value.expire_time=value.expire_time-timedelta(seconds=offset)
            with self.subTest(offset=offset),self.assertRaises(RuntimeError):
                self.scope.assert_secret(value,self.runtime.number,self.runtime.started)

    def test_cleanup_without_creation_never_deletes(self):
        self.client.delete_dataset = Mock()
        self.secret_client.delete_secret = Mock()
        self.runtime.cleanup()
        self.client.delete_dataset.assert_not_called()
        self.secret_client.delete_secret.assert_not_called()

    def test_uncertain_create_reconciles_owned_resource_and_checks_absence(self):
        self.runtime.dataset_attempted = self.runtime.secret_attempted = True
        self.client.get_dataset = Mock(side_effect=[self.dataset(), NotFound('gone')])
        self.secret_client.get_secret = Mock(side_effect=[self.secret(), NotFound('gone')])
        self.client.delete_dataset = Mock()
        self.secret_client.delete_secret = Mock()
        self.runtime.cleanup()
        self.client.delete_dataset.assert_called_once_with(self.scope.dataset, delete_contents=True,
                                                            retry=None, timeout=10)
        self.secret_client.delete_secret.assert_called_once_with(request=dict(
            name=self.scope.secret(self.runtime.number), etag='owned'), retry=None, timeout=10)
        self.assertEqual(self.runtime.report['cleanup_dataset'], 'DELETED_OR_ABSENT')
        self.assertEqual(self.runtime.report['cleanup_secret'], 'DELETED_OR_ABSENT')

    def test_foreign_dataset_cleanup_refused_but_secret_cleanup_still_attempted(self):
        self.runtime.dataset_attempted = self.runtime.secret_attempted = True
        value = self.dataset();value.labels={}
        self.client.get_dataset = Mock(return_value=value)
        self.client.delete_dataset = Mock()
        self.secret_client.get_secret = Mock(side_effect=[self.secret(),NotFound('gone')])
        self.secret_client.delete_secret = Mock()
        with self.assertRaisesRegex(RuntimeError,'PROBE_CLEANUP_FAILED'):self.runtime.cleanup()
        self.client.delete_dataset.assert_not_called()
        self.secret_client.delete_secret.assert_called_once()

    def test_foreign_secret_cleanup_refused_but_dataset_cleanup_still_attempted(self):
        self.runtime.dataset_attempted = self.runtime.secret_attempted = True
        self.client.get_dataset = Mock(side_effect=[self.dataset(),NotFound('gone')])
        self.client.delete_dataset = Mock()
        value=self.secret();value.labels={}
        self.secret_client.get_secret = Mock(return_value=value)
        self.secret_client.delete_secret = Mock()
        with self.assertRaisesRegex(RuntimeError,'PROBE_CLEANUP_FAILED'):self.runtime.cleanup()
        self.client.delete_dataset.assert_called_once()
        self.secret_client.delete_secret.assert_not_called()

    def test_cleanup_absence_is_confirmed_without_deletion(self):
        self.runtime.dataset_attempted = self.runtime.secret_attempted = True
        self.client.get_dataset = Mock(side_effect=NotFound('gone'))
        self.secret_client.get_secret = Mock(side_effect=NotFound('gone'))
        self.client.delete_dataset = Mock();self.secret_client.delete_secret = Mock()
        self.runtime.cleanup()
        self.client.delete_dataset.assert_not_called();self.secret_client.delete_secret.assert_not_called()
        self.assertEqual(self.client.get_dataset.call_count,2)
        self.assertEqual(self.secret_client.get_secret.call_count,2)

    def test_cleanup_requires_confirmed_deletion(self):
        self.runtime.secret_attempted = True
        self.secret_client.get_secret = Mock(return_value=self.secret())
        self.secret_client.delete_secret = Mock()
        with self.assertRaisesRegex(RuntimeError,'PROBE_CLEANUP_FAILED'):self.runtime.cleanup()
        self.assertEqual(self.runtime.report['cleanup_secret'],'REFUSED_OR_FAILED')

    def test_guarded_query_never_submits_foreign_sql(self):
        self.runtime.own=Mock();self.runtime.native_query=Mock()
        with self.assertRaisesRegex(RuntimeError,'UNSAFE_PROBE_SQL_SCOPE'):
            self.runtime.guarded_query(f'SELECT * FROM `{probe.DATASET_ID}.product_master`')
        self.runtime.native_query.assert_not_called()

    def test_schema_mismatch_prevents_json_load_even_in_owned_dataset(self):
        self.runtime.own=Mock()
        self.client.get_table=Mock(return_value=bigquery.Table(self.scope.table('signing_heads'),
            schema=[bigquery.SchemaField('unrelated','STRING')]))
        self.client.load_table_from_json=Mock()
        with self.assertRaisesRegex(RuntimeError,'FIXTURE_SCHEMA_MISMATCH'):
            self.runtime.load('signing_heads',[])
        self.client.load_table_from_json.assert_not_called()

    def test_unrelated_table_never_loads(self):
        self.runtime.own=Mock()
        self.client.load_table_from_json=Mock()
        with self.assertRaisesRegex(RuntimeError,'UNRELATED_PROBE_TABLE'):
            self.runtime.load('product_master',[])
        self.client.load_table_from_json.assert_not_called()

    def test_wrong_returned_table_identity_prevents_load_with_matching_schema(self):
        self.runtime.own=Mock()
        self.client.get_table=Mock(return_value=probe.TABLES['signing_heads'].table(probe.PROJECT))
        self.client.load_table_from_json=Mock()
        with self.assertRaisesRegex(RuntimeError,'FIXTURE_SCHEMA_MISMATCH'):
            self.runtime.load('signing_heads',[])
        self.client.load_table_from_json.assert_not_called()

    def test_guarded_secret_access_rejects_alias_and_foreign_version(self):
        self.runtime.config=Mock()
        self.runtime.config.versions=('v',)
        self.runtime.config.name.return_value=self.scope.secret(self.runtime.number)+'/versions/1'
        self.runtime.secret_access=Mock()
        for name in (self.scope.secret(self.runtime.number)+'/versions/latest',
                     self.scope.secret(self.runtime.number)+'/versions/2', 'projects/other'):
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError,'UNOWNED_SECRET_ACCESS'):
                self.runtime.guarded_access(request={'name':name})
        self.runtime.secret_access.assert_not_called()

    def test_foreign_clients_or_endpoints_rejected(self):
        with self.assertRaisesRegex(RuntimeError,'INVALID_PROBE_CLIENT'):
            probe.Probe(Mock(),self.secret_client,self.session)
        foreign=sm.SecretManagerServiceClient(credentials=AnonymousCredentials())
        self.addCleanup(foreign.transport.close)
        with self.assertRaisesRegex(RuntimeError,'INVALID_SECRET_PROBE_CLIENT'):
            probe.Probe(self.client,foreign,self.session)

    def test_no_resource_cli_runs_without_opt_in_and_output_path(self):
        for args in ([],['--output-dir','unused'],['--run']):
            with patch('sys.argv',['probe']+args), patch.object(probe.google.auth,'default') as adc:
                with self.assertRaises(SystemExit):probe.main()
                adc.assert_not_called()


if __name__ == '__main__':unittest.main()
