"""4.2 Critical/Must have/shared pilot: genesis contracts/SDK/IAP fixtures, no RPCs."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import Conflict, NotFound, ServiceUnavailable
from google.auth import jwt
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from src.application import signing_bootstrap as bootstrap
from src.application.identity import ASSERTION_HEADER, ISSUER, IapIdentityAdapter, IapSettings, IdentityError, principal_for_subject
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.signing_control_writer import SigningWriteError
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from tests import test_identity as identities
from tests import test_sharing_executor as jobs
from tests import test_sharing_publication_storage as publications
from tests import test_signing_control_writer as writes
from tests import test_signing_storage_io as storage

ACTOR = jobs.identity().principal.principal_id
NOW = writes.NOW


def manifest(**changes):
    return replace(bootstrap.SigningBootstrapManifest(ACTOR, publications.sequence()[0], NOW,
        NOW+timedelta(seconds=300)), **changes)


def result(value):
    return dict(observed_at=value.accepted_at, head=writes.unjson(json.loads(value.head_json))[0],
        publication=writes.unjson(json.loads(value.publication_json))[0],
        history=writes.unjson(json.loads(value.history_json)), checkpoint_count=0)


class BootstrapContractTests(unittest.TestCase):
    def test_genesis_manifest_freezes_schema_and_immutable_stamps(self):
        value=manifest()
        receipt=bootstrap.validate_bootstrap_result(value,[result(value)])
        self.assertEqual(receipt.publication_digest,value.publication.digest)
        self.assertEqual(json.loads(value.head_json)[0]['control_revision'],1)
        self.assertEqual(json.loads(value.publication_json)[0]['recorded_at'],NOW.isoformat())

    def test_reserved_slots_ignore_owner_configuration_and_times(self):
        first=manifest();other=manifest(owner_principal_id='iap-v1:'+'a'*64,accepted_at=NOW+timedelta(seconds=1))
        self.assertNotEqual(first.head_json,other.head_json)
        self.assertEqual(len({bootstrap.bootstrap_job_id(i) for i in range(3)}),3)
        self.assertNotIn(ACTOR,bootstrap.bootstrap_job_id(0))
        self.assertNotIn(first.publication.digest,bootstrap.bootstrap_job_id(0))

    def test_invalid_attempts_are_rejected(self):
        for attempt in (-1,3,True,1.0,'0',None):
            with self.subTest(attempt=attempt),self.assertRaises(ContractError):bootstrap.bootstrap_job_id(attempt)

    def test_invalid_owner_genesis_and_windows_are_rejected(self):
        for changes in ({'owner_principal_id':'email@example.invalid'},
                        {'publication':publications.sequence()[-1]},
                        {'deadline_at':NOW},{'deadline_at':NOW+timedelta(seconds=301)},
                        {'accepted_at':publications.sequence()[0].published_at-timedelta(seconds=1)}):
            with self.subTest(changes=changes),self.assertRaises(ContractError):manifest(**changes)

    def test_result_requires_exact_body_history_head_stamps_scope_and_time(self):
        value=manifest()
        for mode in ('head','body','history','checkpoint','time','extra','duplicate'):
            row=result(value)
            if mode=='head':row['head']['publisher_id']='foreign'
            if mode=='body':row['publication']['recorded_at']=NOW-timedelta(seconds=1)
            if mode=='history':row['history'][0]['secret_version']+=1
            if mode=='checkpoint':row['checkpoint_count']=1
            if mode=='time':row['observed_at']=value.deadline_at
            if mode=='extra':row['unknown']='x'
            with self.subTest(mode=mode),self.assertRaises((ContractError,ValueError)):
                bootstrap.validate_bootstrap_result(value,[row,row] if mode=='duplicate' else [row])

    def test_sql_is_fixed_scoped_explicit_atomic_and_cannot_reset_rows(self):
        sql=bootstrap.bootstrap_sql()
        for name in ('signing_heads','signing_publications','signing_key_history','signing_checkpoints'):
            self.assertIn(f'{PROJECT}.ci_application.{name}',sql)
        self.assertNotIn('__',sql);self.assertNotIn('DELETE ',sql);self.assertNotIn('MERGE ',sql)
        self.assertIn('BOOTSTRAP_POLICY_ALREADY_EXISTS',sql)
        self.assertIn('ROLLBACK TRANSACTION',sql)
        cfg=manifest().job_config(0).to_api_repr()
        self.assertFalse(cfg['query']['useQueryCache']);self.assertFalse(cfg['query']['createSession'])
        self.assertEqual(cfg['query']['maximumBytesBilled'],str(512*1024**2))
        self.assertTrue(all(p['parameterValue'].get('value') is not None for p in cfg['query']['queryParameters']))


class BootstrapAdapterTests(unittest.TestCase):
    def setUp(self):
        self.client=bigquery.Client(project=PROJECT,location=LOCATION,credentials=AnonymousCredentials())
        self.addCleanup(self.client.close)
        self.client.get_dataset=Mock(return_value=storage.workflow.dataset())
        self.client.get_table=Mock(side_effect=lambda ref,**kw:storage.table(ref.table_id))
        self.native={}
        self.client.get_job=Mock(side_effect=self.get)
        self.client.query=Mock(side_effect=self.dispatch)
        self.auth=IapIdentityAdapter(IapSettings(identities.AUDIENCE))
        self.auth.authenticate=Mock(return_value=jobs.identity())
        self.value=manifest()
        self.adapter=self.new_adapter()
        for patcher in (patch('src.application.identity.time.time',return_value=jobs.EPOCH+10),
                        patch('src.application.signing_bootstrap._now',return_value=NOW)):
            patcher.start();self.addCleanup(patcher.stop)

    def new_adapter(self, **changes):
        return bootstrap.ProtectedSigningBootstrap(client=self.client,identity=self.auth,
            manifest=replace(self.value,**changes),enabled=True)

    def get(self, job_id, **kwargs):
        if job_id not in self.native:raise NotFound('absent')
        return self.native[job_id]

    def dispatch(self,sql,*,job_config,job_id,**kwargs):
        job=bigquery.QueryJob(job_id,sql,self.client,job_config=deepcopy(job_config))
        job._properties['jobReference'].update(projectId=PROJECT,location=LOCATION)
        job._properties['status']={'state':'DONE'}
        job.result=Mock(return_value=[result(self.value)])
        self.native[job_id]=job
        return job

    def failed(self,attempt=0,*,statement='ROLLBACK_TRANSACTION',state='DONE'):
        job=self.dispatch(bootstrap.bootstrap_sql(),job_config=self.value.job_config(attempt),
                          job_id=bootstrap.bootstrap_job_id(attempt))
        job._properties['status']={'state':state,'errorResult':{'reason':'fixture'}}
        child=bigquery.QueryJob('child-'+str(attempt),statement,self.client)
        child._properties['jobReference'].update(projectId=PROJECT,location=LOCATION)
        child._properties['status']={'state':'DONE'}
        child._properties['statistics']={'parentJobId':job.job_id,'query':{'statementType':statement}}
        self.client.list_jobs=Mock(return_value=[child])
        return job

    def test_default_disable_and_nonowner_deny_before_cloud_reads(self):
        for adapter in (bootstrap.ProtectedSigningBootstrap(client=self.client,identity=self.auth,manifest=self.value),
                        self.new_adapter(owner_principal_id='iap-v1:'+'b'*64)):
            with self.assertRaises(SigningWriteError):adapter.submit({})
        self.client.get_dataset.assert_not_called();self.client.query.assert_not_called()

    def test_invalid_or_expired_identity_denies_dispatch(self):
        self.auth.authenticate.side_effect=IdentityError('INVALID_ASSERTION')
        with self.assertRaises(IdentityError):self.adapter.submit({})
        self.auth.authenticate.side_effect=None
        self.auth.authenticate.return_value=replace(jobs.identity(),issued_at=jobs.EPOCH-100,expires_at=jobs.EPOCH-31)
        with self.assertRaises(IdentityError):self.adapter.submit({})
        self.client.query.assert_not_called()

    def test_same_slot_and_restart_recover_without_reexecution(self):
        first=self.adapter.submit({})
        self.assertEqual(first.native_status,SharingCommitStatus.COMMITTED)
        self.assertEqual(self.adapter.submit({}),first)
        self.assertEqual(self.new_adapter().submit({}),first)
        self.assertEqual(self.client.query.call_count,1)
        kwargs=self.client.query.call_args.kwargs
        self.assertEqual(kwargs['job_id'],bootstrap.bootstrap_job_id(0))
        self.assertIsNone(kwargs['retry']);self.assertIsNone(kwargs['job_retry'])

    def test_read_only_recovery_never_creates_an_absent_job(self):
        self.assertEqual(self.adapter.recover({}).native_status,SharingCommitStatus.UNKNOWN)
        self.client.query.assert_not_called()

    def test_unknown_submission_cannot_advance_or_reissue_in_process(self):
        self.client.query.side_effect=ServiceUnavailable('lost response')
        self.assertEqual(self.adapter.submit({}).native_status,SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.adapter.submit({}).native_status,SharingCommitStatus.UNKNOWN)
        with self.assertRaises(SigningWriteError):self.adapter.submit({},attempt=1)
        self.assertEqual(self.client.query.call_count,1)

    def test_positive_correlated_rollback_admits_next_reserved_slot(self):
        self.failed()
        outcome=self.adapter.submit({},attempt=1)
        self.assertEqual(outcome.native_status,SharingCommitStatus.COMMITTED)
        self.assertEqual(self.client.query.call_args.kwargs['job_id'],bootstrap.bootstrap_job_id(1))

    def test_committed_running_mismatched_or_missing_predecessors_cannot_advance(self):
        for mode in ('commit','running','config','missing','children'):
            self.native.clear()
            job=self.failed(statement='COMMIT_TRANSACTION' if mode=='commit' else 'ROLLBACK_TRANSACTION')
            if mode=='running':job._properties['status']['state']='RUNNING'
            if mode=='config':job._properties['configuration']['query']['query']='SELECT 1'
            if mode=='missing':self.native.clear()
            if mode=='children':self.client.list_jobs.return_value=[]
            with self.subTest(mode=mode),self.assertRaises(SigningWriteError):self.new_adapter().submit({},attempt=1)
        self.client.query.assert_not_called()

    def test_every_ancestor_is_required_not_only_latest(self):
        self.failed(attempt=1)
        with self.assertRaises(SigningWriteError):self.adapter.submit({},attempt=2)
        self.client.query.assert_not_called()

    def test_changed_manifest_cannot_reuse_or_advance_same_slots(self):
        self.adapter.submit({})
        changed=self.new_adapter(accepted_at=NOW+timedelta(seconds=1))
        self.assertEqual(changed.submit({}).native_status,SharingCommitStatus.UNKNOWN)
        with self.assertRaises(SigningWriteError):changed.submit({},attempt=1)
        self.assertEqual(self.client.query.call_count,1)

    def test_conflicting_submission_reads_original_job_without_reminting(self):
        job=self.dispatch(bootstrap.bootstrap_sql(),job_config=self.value.job_config(0),job_id=bootstrap.bootstrap_job_id(0))
        self.client.get_job.side_effect=[NotFound('absent'),job]
        self.client.query.side_effect=Conflict('reserved')
        self.assertEqual(self.adapter.submit({}).native_status,SharingCommitStatus.COMMITTED)
        self.assertEqual(self.client.query.call_count,1)

    def test_missing_or_malformed_result_stays_unknown(self):
        job=self.dispatch(bootstrap.bootstrap_sql(),job_config=self.value.job_config(0),job_id=bootstrap.bootstrap_job_id(0))
        for rows in ([],[{}],[result(self.value),result(self.value)]):
            job.result.return_value=rows
            self.assertEqual(self.adapter.recover({}).native_status,SharingCommitStatus.UNKNOWN)
        self.client.query.assert_not_called()

    def test_disabled_or_expired_bootstrap_can_still_recover_original_job(self):
        first=self.adapter.submit({});self.adapter.enabled=False
        with patch('src.application.signing_bootstrap._now',return_value=self.value.deadline_at):
            self.assertEqual(self.adapter.recover({}),first)
        self.assertEqual(self.client.query.call_count,1)

    def test_expired_or_future_manifest_cannot_dispatch(self):
        for instant in (NOW-timedelta(seconds=1),self.value.deadline_at):
            with patch('src.application.signing_bootstrap._now',return_value=instant),self.assertRaises(SigningWriteError):
                self.adapter.submit({})
        self.client.query.assert_not_called()

    def test_schema_outage_denies_without_query(self):
        self.client.get_table.side_effect=ServiceUnavailable('private details')
        with self.assertRaises(SigningWriteError):self.adapter.submit({})
        self.client.query.assert_not_called()

    def test_settings_client_and_enable_flag_are_strict(self):
        with self.assertRaises(ContractError):
            bootstrap.ProtectedSigningBootstrap(client=self.client,identity=self.auth,manifest=self.value,enabled='yes')
        self.client._default_query_job_config=bigquery.QueryJobConfig()
        with self.assertRaises(ContractError):self.new_adapter()


class BootstrapSignedIdentityTests(unittest.TestCase):
    def test_real_es256_owner_and_negative_identity_boundaries(self):
        signer,public=identities.signer_and_public_key()
        subject='accounts.google.com:bootstrap-owner'
        owner=principal_for_subject(ISSUER,subject).principal_id
        client=bigquery.Client(project=PROJECT,location=LOCATION,credentials=AnonymousCredentials())
        self.addCleanup(client.close)
        auth=IapIdentityAdapter(IapSettings(identities.AUDIENCE))
        adapter=bootstrap.ProtectedSigningBootstrap(client=client,identity=auth,manifest=manifest(owner_principal_id=owner),enabled=True)
        adapter._preflight=Mock(side_effect=SigningWriteError('TEST_STOP_AFTER_AUTH'))
        response=Mock(return_value=SimpleNamespace(status=200,data=json.dumps({'fixture-key':public}).encode()))
        with patch('src.application.identity._BoundedKeyRequest',return_value=response), \
             patch('src.application.identity.time.time',return_value=identities.EPOCH), \
             patch('google.auth._helpers.utcnow',return_value=identities.NOW.replace(tzinfo=None)):
            claims=dict(iss=ISSUER,aud=identities.AUDIENCE,sub=subject,iat=identities.EPOCH-10,exp=identities.EPOCH+590)
            token=jwt.encode(signer,claims).decode('ascii')
            with self.assertRaises(SigningWriteError) as caught:adapter.submit({ASSERTION_HEADER:token})
            self.assertEqual(caught.exception.code,'TEST_STOP_AFTER_AUTH')
            adapter._preflight.reset_mock()
            for headers in ({},{'X-Goog-Authenticated-User-Email':'owner@example.invalid'},
                {ASSERTION_HEADER:jwt.encode(signer,claims|dict(sub='other',roles=['ADMIN'])).decode('ascii')},
                {ASSERTION_HEADER:jwt.encode(signer,claims|dict(aud='other')).decode('ascii')}):
                with self.assertRaises((IdentityError,SigningWriteError)):adapter.submit(headers)
            adapter._preflight.assert_not_called()


if __name__=='__main__':unittest.main()
