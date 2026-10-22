"""4.2 offline write/query/identity/job fixtures; no native concurrency or cloud RPCs."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.auth.credentials import AnonymousCredentials
from google.auth import jwt
from google.cloud import bigquery

from src.application.identity import (
    ASSERTION_HEADER, ISSUER, IapIdentityAdapter, IapSettings, IdentityError, principal_for_subject,
)
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_publication_codec import decode_read_result
from src.application.sharing_publication_reader import SigningCapturedRead
from src.application.signing_write_query import (
    SigningWriteKind, SigningWriteState, prepare_write, render_write_sql, validate_write_result,
)
from src.application.signing_control_writer import (
    ProtectedSigningWriter, SigningCheckpointGrant, SigningWriteError, SigningWriterSettings,
)
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from tests import test_signing_storage_io as storage
from tests import test_identity as identities
from tests import test_sharing_publication_storage as fixtures
from tests import test_sharing_executor as jobs

ACTOR = jobs.identity().principal.principal_id
NOW = fixtures.sequence()[-1].published_at


def capture(checkpoint=False, request=None):
    pubs = fixtures.sequence()
    req = request or fixtures.request()
    row = storage.result(pubs, request=req, checkpoint=pubs[0] if checkpoint else None)
    return SigningCapturedRead(req, decode_read_result(req, [row], now=row['observed_at']), row)


def plan(kind=SigningWriteKind.PUBLICATION, checkpoint=False, command='write-test'):
    cap = capture(checkpoint)
    pub = fixtures.fixtures.next_publication(cap.observation.publications[-1],
                                            published_at=cap.observation.observed_at)
    return prepare_write(cap, kind=kind, actor_id=ACTOR, command_id=command,
                         publication=pub if kind is SigningWriteKind.PUBLICATION else None)


def unjson(value):
    """Match the explicit destination schema's native TIMESTAMP projections."""
    times = {'published_at', 'recorded_at', 'issue_from', 'issue_until', 'retain_until', 'updated_at'}
    if type(value) is list:
        return [unjson(v) for v in value]
    if type(value) is dict:
        return {k: fixtures.NOW.fromisoformat(v) if k in times else unjson(v) for k, v in value.items()}
    return value


def applied(value, state='APPLIED'):
    pub = unjson(json.loads(value.publication_json))[0]
    head = value.capture.rows['heads'][0]
    if state == 'APPLIED':
        head['control_revision'] += 1
        if value.kind is SigningWriteKind.PUBLICATION:
            head.update(revision=pub['revision'], publication_digest=pub['publication_digest'],
                        published_at=pub['published_at'], publisher_id=value.actor_id, command_id=value.command_id)
    cp = unjson(json.loads(value.checkpoint_json)) if value.kind is SigningWriteKind.CHECKPOINT else value.capture.rows['checkpoints']
    return dict(state=state, observed_at=value.capture.observation.observed_at, head=head,
                publication=pub, history=unjson(json.loads(value.history_json)), checkpoints=cp)


class WriteQueryTests(unittest.TestCase):
    def test_publication_plan_validates_transition_and_keeps_immutable_stamps(self):
        value = plan()
        row = json.loads(value.publication_json)[0]
        self.assertEqual(row['publisher_id'], ACTOR)
        self.assertEqual(row['command_id'], value.command_id)
        self.assertEqual(row['parent_digest'], value.capture.observation.publications[-1].digest)
        self.assertEqual(validate_write_result(value, [applied(value)]).state, SigningWriteState.APPLIED)

    def test_checkpoint_creation_and_advancement_use_authoritative_head(self):
        for existing in (False, True):
            value = plan(SigningWriteKind.CHECKPOINT, existing)
            cp = json.loads(value.checkpoint_json)[0]
            self.assertEqual(cp['checkpoint_revision'], 2 if existing else 1)
            self.assertEqual(cp['publication_revision'], 3)
            self.assertEqual(cp['writer_id'], ACTOR)
            self.assertEqual(validate_write_result(value, [applied(value)]).checkpoint_revision, cp['checkpoint_revision'])

    def test_existing_floor_is_preserved_without_restamping(self):
        cap = capture()
        rows = cap.rows
        rows['checkpoints'] = storage.result(checkpoint=fixtures.sequence()[-1])['checkpoints']
        rows['checkpoint_count'] = 1
        rows['publications'] = rows['publications'][-1:]
        rows['publication_count'] = 1
        rows['history'] = [h for h in rows['history'] if h['publication_revision'] == 3]
        rows['history_count'] = len(rows['history'])
        cap = SigningCapturedRead(cap.request, decode_read_result(cap.request,[rows],now=NOW),rows)
        value = prepare_write(cap,kind=SigningWriteKind.CHECKPOINT,actor_id=ACTOR,command_id='later')
        self.assertEqual(unjson(json.loads(value.checkpoint_json)), rows['checkpoints'])
        result = applied(value,'REUSED')
        self.assertEqual(validate_write_result(value,[result]).state,SigningWriteState.REUSED)

    def test_invalid_capture_parent_and_caller_checkpoint_target_fail(self):
        value = plan()
        bad_rows = capture().rows
        bad_rows['heads'][0]['control_revision'] = 0
        invalid = SigningCapturedRead(capture().request,capture().observation,bad_rows)
        for cap, kind, pub in ((invalid,SigningWriteKind.PUBLICATION,fixtures.sequence()[0]),
                              (capture(),SigningWriteKind.PUBLICATION,fixtures.sequence()[0]),
                              (capture(),SigningWriteKind.CHECKPOINT,fixtures.sequence()[0])):
            with self.assertRaises(ContractError):
                prepare_write(cap,kind=kind,actor_id=ACTOR,command_id='test',publication=pub)

    def test_future_publication_is_denied(self):
        cap = capture()
        pub = fixtures.fixtures.next_publication(cap.observation.publications[-1])
        with self.assertRaises(ContractError):
            prepare_write(cap,kind=SigningWriteKind.PUBLICATION,actor_id=ACTOR,command_id='x',publication=pub)

    def test_command_job_id_stays_fixed_when_snapshot_changes(self):
        value = plan()
        changed = replace(value,deadline_at=value.deadline_at-timedelta(seconds=1))
        self.assertEqual(value.job_id, changed.job_id)
        self.assertNotEqual(value.job_config().to_api_repr(), changed.job_config().to_api_repr())
        self.assertNotEqual(value.job_id,replace(value,command_id='different').job_id)

    def test_capture_copies_and_snapshot_json_pin_cannot_change_with_mutated_source(self):
        cap = capture()
        value = prepare_write(cap,kind=SigningWriteKind.CHECKPOINT,actor_id=ACTOR,command_id='x')
        before = value.job_config().to_api_repr()
        cap._rows['heads'][0]['control_revision'] = 99
        self.assertEqual(value.job_config().to_api_repr(),before)
        copy = value.capture.rows
        copy['heads'][0]['control_revision'] = 100
        self.assertNotEqual(value.capture.rows['heads'][0]['control_revision'],100)

    def test_sql_is_explicit_scoped_and_updates_existing_fence_before_first_insert(self):
        sql = render_write_sql()
        self.assertNotIn('__',sql)
        self.assertNotIn('DELETE ',sql)
        self.assertNotIn('MERGE ',sql)
        self.assertIn('SIGNING_HEAD_NOT_UNIQUE',sql)
        self.assertIn('SIGNING_PARENT_HISTORY_CHANGED',sql)
        self.assertLess(sql.index('SET control_revision=control_revision+1'),sql.index('INSERT INTO'))
        self.assertIn('PROBE_AFTER_COMMIT',sql)
        self.assertIn('ROLLBACK TRANSACTION;',sql)
        config = plan().job_config().to_api_repr()
        self.assertFalse(config['query']['useQueryCache'])
        self.assertFalse(config['query']['createSession'])
        self.assertTrue(all(p.get('parameterValue',{}).get('value') is not None for p in config['query']['queryParameters']))

    def test_result_rejects_changed_hash_stamp_fence_time_and_scope(self):
        value = plan()
        for mode in ('hash','stamp','head_actor','head_command','fence','time','scope','rows'):
            row = applied(value)
            if mode=='hash': row['publication']['publication_digest']='a'*64
            if mode=='stamp': row['publication']['command_id']='changed'
            if mode=='head_actor': row['head']['publisher_id']='changed'
            if mode=='head_command': row['head']['command_id']='changed'
            if mode=='fence': row['head']['control_revision']=99
            if mode=='time': row['observed_at']=value.deadline_at
            if mode=='scope': row['head']['policy_id']='foreign'
            with self.subTest(mode=mode), self.assertRaises((ContractError,ValueError)):
                validate_write_result(value,[row,row] if mode=='rows' else [row])

    def test_checkpoint_result_cannot_report_downgraded_or_conflicting_floor(self):
        value = plan(SigningWriteKind.CHECKPOINT)
        for mode in ('digest','floor','consumer','cas'):
            row = applied(value)
            cp = row['checkpoints'][0]
            if mode=='digest': cp['publication_digest']='b'*64
            if mode=='floor': cp['publication_revision']=1
            if mode=='consumer': cp['consumer_id']='foreign'
            if mode=='cas': cp['checkpoint_revision']+=1
            with self.assertRaises(ContractError): validate_write_result(value,[row])

    def test_genesis_creation_is_not_a_supported_plan(self):
        cap = capture()
        first = fixtures.sequence()[0]
        with self.assertRaises(ContractError):
            prepare_write(cap,kind=SigningWriteKind.PUBLICATION,actor_id=ACTOR,command_id='genesis',publication=first)

    def test_checkpoint_application_preserves_all_head_publication_fields(self):
        value=plan(SigningWriteKind.CHECKPOINT)
        for column in ('publisher_id','command_id','publication_digest','published_at'):
            row=applied(value)
            row['head'][column]=NOW-timedelta(seconds=1) if column=='published_at' else 'a'*64
            with self.assertRaises(ContractError):validate_write_result(value,[row])


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.client = bigquery.Client(project=PROJECT,location=LOCATION,credentials=AnonymousCredentials())
        self.addCleanup(self.client.close)
        self.client.get_dataset=Mock(return_value=storage.workflow.dataset())
        self.client.get_table=Mock(side_effect=lambda ref,**kw:storage.table(ref.table_id))
        self.client.get_job=Mock(side_effect=NotFound('absent'))
        self.client.query=Mock(side_effect=self.dispatch)
        self.auth=IapIdentityAdapter(IapSettings(identities.AUDIENCE))
        self.auth.authenticate=Mock(return_value=jobs.identity())
        self.settings=SigningWriterSettings(fixtures.secrets.PROJECT_NUMBER,frozenset({ACTOR}),
            (SigningCheckpointGrant(ACTOR,'pilot-runtime'),),'pilot-runtime',True)
        self.writer=ProtectedSigningWriter(client=self.client,identity=self.auth,settings=self.settings)
        self.writer.reader.capture=Mock(side_effect=lambda request:capture(request=request))
        for name,target,value in (('identity','time.time',jobs.EPOCH+10),
                                  ('signing_control_writer','_now',NOW)):
            patcher=patch('src.application.'+name+'.'+target,return_value=value)
            patcher.start(); self.addCleanup(patcher.stop)
        self.value=self.writer.prepare_publication({},command_id='write-test',publication=
            fixtures.fixtures.next_publication(capture().observation.publications[-1],published_at=NOW))

    def dispatch(self,sql,*,job_config,job_id,**kwargs):
        job=bigquery.QueryJob(job_id,sql,self.client,job_config=job_config)
        job._properties['jobReference'].update(projectId=PROJECT,location=LOCATION)
        job._properties['status']={'state':'DONE'}
        job.result=Mock(return_value=[applied(self.value)])
        self.client.get_job.side_effect=None
        self.client.get_job.return_value=job
        return job

    def test_authorized_submit_and_same_plan_recovery_use_one_native_query(self):
        result=self.writer.submit({},self.value)
        self.assertEqual(result.native_status,SharingCommitStatus.COMMITTED)
        self.assertEqual(result.result.state,SigningWriteState.APPLIED)
        self.assertEqual(self.writer.submit({},self.value),result)
        self.assertEqual(self.client.query.call_count,1)
        call=self.client.query.call_args.kwargs
        self.assertEqual(call['job_id'],self.value.job_id)
        self.assertIsNone(call['retry']);self.assertIsNone(call['job_retry'])

    def test_disabled_by_default_and_no_grant_do_not_query_or_capture(self):
        for settings in (replace(self.settings,enabled=False),replace(self.settings,operator_principals=frozenset())):
            self.writer.settings=settings
            self.writer.reader.capture.reset_mock()
            with self.assertRaises(SigningWriteError):
                self.writer.prepare_publication({},command_id='x',publication=fixtures.sequence()[0])
            self.writer.reader.capture.assert_not_called()
            self.client.query.assert_not_called()

    def test_checkpoint_grant_is_independent_of_operator_and_consumer_scoped(self):
        self.writer.settings=replace(self.settings,checkpoint_grants=())
        with self.assertRaises(SigningWriteError):
            self.writer.prepare_checkpoint({},consumer_id='pilot-runtime',command_id='cp',instance_id='runtime')
        self.writer.settings=self.settings
        with self.assertRaises(SigningWriteError):
            self.writer.prepare_checkpoint({},consumer_id='foreign',command_id='cp',instance_id='runtime')
        checkpoint=self.writer.prepare_checkpoint({},consumer_id='pilot-runtime',command_id='cp',instance_id='runtime')
        self.assertEqual(checkpoint.kind,SigningWriteKind.CHECKPOINT)

    def test_invalid_authentication_prevents_submission(self):
        self.auth.authenticate.side_effect=IdentityError('INVALID_ASSERTION')
        with self.assertRaises(IdentityError):self.writer.submit({},self.value)
        self.client.query.assert_not_called()

    def test_expired_verified_identity_prevents_submission(self):
        self.auth.authenticate.return_value=replace(jobs.identity(),issued_at=jobs.EPOCH-100,expires_at=jobs.EPOCH-31)
        with self.assertRaises(IdentityError):self.writer.submit({},self.value)
        self.client.query.assert_not_called()

    def test_unissued_changed_or_other_process_plan_is_denied(self):
        changed=replace(self.value,command_id='forged')
        with self.assertRaises(SigningWriteError):self.writer.submit({},changed)
        replacement=ProtectedSigningWriter(client=self.client,identity=self.auth,settings=self.settings)
        with self.assertRaises(SigningWriteError):replacement.submit({},self.value)
        self.client.query.assert_not_called()

    def test_uncertain_submission_never_reissues_after_absent_recovery(self):
        self.client.query.side_effect=ServiceUnavailable('lost response')
        self.assertEqual(self.writer.submit({},self.value).native_status,SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.writer.submit({},self.value).native_status,SharingCommitStatus.UNKNOWN)
        self.assertEqual(self.client.query.call_count,1)

    def test_recover_is_read_only_when_missing_running_or_config_mismatched(self):
        self.assertEqual(self.writer.recover({},self.value).native_status,SharingCommitStatus.UNKNOWN)
        native=self.dispatch(self.value.sql,job_config=self.value.job_config(),job_id=self.value.job_id)
        native._properties['status']['state']='RUNNING'
        self.assertEqual(self.writer.recover({},self.value).native_status,SharingCommitStatus.UNKNOWN)
        native._properties['status']['state']='DONE'
        native._properties['configuration']['query']['query']='SELECT 1'
        self.assertEqual(self.writer.recover({},self.value).native_status,SharingCommitStatus.UNKNOWN)
        self.client.query.assert_not_called()

    def test_failed_parent_requires_positive_correlated_child_commit_or_rollback(self):
        native=self.dispatch(self.value.sql,job_config=self.value.job_config(),job_id=self.value.job_id)
        native._properties['status']['errorResult']={'reason':'failed'}
        for statement,status in (('COMMIT_TRANSACTION',SharingCommitStatus.COMMITTED),
                                ('ROLLBACK_TRANSACTION',SharingCommitStatus.NOT_COMMITTED)):
            child=bigquery.QueryJob('child',statement,self.client)
            child._properties['jobReference'].update(projectId=PROJECT,location=LOCATION)
            child._properties['status']={'state':'DONE'}
            child._properties['statistics']={'parentJobId':self.value.job_id,'query':{'statementType':statement}}
            self.client.list_jobs=Mock(return_value=[child])
            self.assertEqual(self.writer.recover({},self.value).native_status,status)
        self.client.list_jobs.return_value=[]
        self.assertEqual(self.writer.recover({},self.value).native_status,SharingCommitStatus.UNKNOWN)

    def test_schema_failure_does_not_dispatch(self):
        self.client.get_table.side_effect=ServiceUnavailable('do not disclose')
        with self.assertRaises(SigningWriteError):self.writer.submit({},self.value)
        self.client.query.assert_not_called()

    def test_expired_write_window_cannot_dispatch_or_remint(self):
        with patch('src.application.signing_control_writer._now',return_value=self.value.deadline_at):
            with self.assertRaises(SigningWriteError) as caught:self.writer.submit({},self.value)
        self.assertEqual(caught.exception.code,'SIGNING_WRITE_WINDOW_CLOSED')
        self.assertEqual(self.writer.submit({},self.value).native_status,SharingCommitStatus.UNKNOWN)
        self.client.query.assert_not_called()

    def test_disabled_writes_still_allow_authorized_original_job_read_only_recovery(self):
        self.writer.settings=replace(self.settings,enabled=False)
        self.assertEqual(self.writer.recover({},self.value).native_status,SharingCommitStatus.UNKNOWN)
        self.client.query.assert_not_called()

    def test_reprepare_same_command_keeps_original_plan_and_window(self):
        pub=fixtures.fixtures.next_publication(capture().observation.publications[-1],published_at=NOW)
        again=self.writer.prepare_publication({},command_id='write-test',publication=pub)
        self.assertIs(again,self.value)
        self.assertEqual(self.writer.reader.capture.call_count,1)
        with self.assertRaises((SigningWriteError,ContractError)):
            self.writer.prepare_publication({},command_id='write-test',publication=fixtures.sequence()[0])

    def test_reprepare_after_later_head_or_store_failure_preserves_original_admission(self):
        self.writer.reader.capture.side_effect=ServiceUnavailable('later outage')
        pub=fixtures.fixtures.next_publication(capture().observation.publications[-1],published_at=NOW)
        self.assertIs(self.writer.prepare_publication({},command_id='write-test',publication=pub),self.value)
        self.assertEqual(self.writer.reader.capture.call_count,1)
        self.client.query.assert_not_called()

    def test_checkpoint_retry_keeps_original_target_and_rejects_changed_instance(self):
        cp=self.writer.prepare_checkpoint({},consumer_id='pilot-runtime',command_id='cp',instance_id='runtime')
        self.writer.reader.capture.side_effect=ServiceUnavailable('later outage')
        self.assertIs(self.writer.prepare_checkpoint({},consumer_id='pilot-runtime',command_id='cp',
                                                     instance_id='runtime'),cp)
        with self.assertRaises(SigningWriteError):
            self.writer.prepare_checkpoint({},consumer_id='pilot-runtime',command_id='cp',instance_id='replacement')
        self.client.query.assert_not_called()

    def test_mismatched_capture_cannot_issue_new_admission(self):
        self.writer.reader.capture.side_effect=None
        self.writer.reader.capture.return_value=capture()
        with self.assertRaises(ContractError):
            self.writer.prepare_checkpoint({},consumer_id='pilot-runtime',command_id='new',instance_id='runtime')
        self.client.query.assert_not_called()

    def test_invalid_grants_boolean_limits_and_wrong_project_are_denied(self):
        for change in ({'enabled':'true'},{'operator_principals':{ACTOR}},{'rpc_timeout_seconds':True},
                       {'checkpoint_grants':(SigningCheckpointGrant(ACTOR,'pilot-runtime'),)*2}):
            with self.assertRaises(ContractError):replace(self.settings,**change)
        with self.assertRaises(ContractError):SigningCheckpointGrant('email@example.com','pilot-runtime')


class SignedWriterIdentityTests(unittest.TestCase):
    """Verify real ES256 authentication reaches the explicit global grant boundary."""
    def setUp(self):
        self.signer, public=identities.signer_and_public_key()
        self.client=bigquery.Client(project=PROJECT,location=LOCATION,credentials=AnonymousCredentials())
        self.addCleanup(self.client.close)
        self.client.query=Mock()
        self.subject='accounts.google.com:signing-operator'
        principal=principal_for_subject(ISSUER,self.subject).principal_id
        self.writer=ProtectedSigningWriter(client=self.client,identity=IapIdentityAdapter(IapSettings(identities.AUDIENCE)),
            settings=SigningWriterSettings(fixtures.secrets.PROJECT_NUMBER,frozenset({principal}),enabled=True))
        self.writer.reader.capture=Mock(side_effect=lambda request:capture(request=request))
        response=Mock(return_value=SimpleNamespace(status=200,data=json.dumps({'fixture-key':public}).encode()))
        for patcher in (patch('src.application.identity._BoundedKeyRequest',return_value=response),
                        patch('src.application.identity.time.time',return_value=identities.EPOCH),
                        patch('google.auth._helpers.utcnow',return_value=identities.NOW.replace(tzinfo=None))):
            patcher.start();self.addCleanup(patcher.stop)

    def headers(self, **changes):
        claims=dict(iss=ISSUER,aud=identities.AUDIENCE,sub=self.subject,
                    iat=identities.EPOCH-10,exp=identities.EPOCH+590)
        claims.update(changes)
        return {ASSERTION_HEADER:jwt.encode(self.signer,claims).decode('ascii')}

    def prepare(self, headers):
        pub=fixtures.fixtures.next_publication(capture().observation.publications[-1],published_at=NOW)
        return self.writer.prepare_publication(headers,command_id='signed',publication=pub)

    def test_valid_es256_operator_signature_issues_actor_bound_plan(self):
        issued=self.prepare(self.headers())
        self.assertEqual(issued.actor_id,principal_for_subject(ISSUER,self.subject).principal_id)
        self.writer.reader.capture.assert_called_once()
        self.client.query.assert_not_called()

    def test_missing_wrong_audience_expired_and_other_subject_cannot_capture(self):
        for headers in ({},{'X-Goog-Authenticated-User-Email':'operator@example.invalid'},
                        self.headers(aud=identities.AUDIENCE+'-other'),
                        self.headers(iat=identities.EPOCH-100,exp=identities.EPOCH-31),
                        self.headers(sub='other-subject',roles=['ADMIN'],email='operator@example.invalid')):
            with self.subTest(headers_type=type(headers)),self.assertRaises((IdentityError,SigningWriteError)):
                self.prepare(headers)
        self.writer.reader.capture.assert_not_called()
        self.client.query.assert_not_called()


if __name__=='__main__':unittest.main()
