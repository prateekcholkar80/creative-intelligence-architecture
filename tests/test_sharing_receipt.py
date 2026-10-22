"""Impacted receipt/query/access flows; cloud SQL is checked by the opt-in probe."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import timedelta, timezone
import re
import unittest
from unittest.mock import Mock, patch

from google.cloud import bigquery

from src.application.access_policy import AuthorizationError
from src.application.access_reader import BigQueryAccessReader
from src.application.identity import IapIdentityAdapter, IdentityError
from src.application.sharing_executor import ProtectedSharingExecutor, SharingExecutorError, SharingExecutorSettings
from src.application.sharing_intent import SharingIntentSealer, initial_attempt
from src.application.sharing_jobs import matches_sharing_job
from src.application.sharing_query import SharingResultState
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_receipt import SharingReceiptQuery, validate_receipt_result
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import LOCATION, TABLES
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_membership_contract import ADMIN, TIME, membership
from tests.test_sharing_executor import ClientFixture, EPOCH, KEY, identity
from tests.test_sharing_query import query, result_row


def receipt_row():
    source = result_row('REUSED')
    return dict(observed_at=source['observed_at'], workspaces=[source['workspace']],
        memberships=[source['membership']], conversations=[source['conversation']], audit_events=[source['receipt']])


class SharingReceiptContractTests(unittest.TestCase):
    def check(self, row=None, q=None):
        return validate_receipt_result(SharingReceiptQuery(q or query()), [row or receipt_row()],
                                       original_job_id=initial_attempt(q or query()).job_id)

    def test_single_select_uses_four_bounded_explicit_projections_and_full_prefix(self):
        sql = SharingReceiptQuery(query()).sql
        self.assertTrue(sql.startswith('SELECT'))
        self.assertNotRegex(sql, r'\b(UPDATE|INSERT|DELETE|MERGE|TRUNCATE|CREATE|BEGIN|COMMIT|ROLLBACK)\b')
        self.assertNotIn('SELECT *', sql)
        self.assertEqual(sql.count('LIMIT 2'), 4)
        self.assertEqual(sql.count('CURRENT_TIMESTAMP()'), 1)
        self.assertIn('STARTS_WITH(audit_id, @audit_prefix)', sql)
        self.assertIn('occurred_at >= @accepted_at AND occurred_at < @deadline_at', sql)
        self.assertNotIn('audit_id=@audit_id', sql)
        for table in ('workspaces','memberships','conversations','audit_events'):
            self.assertIn(f'`{PROJECT}.ci_application.{table}`', sql)
            self.assertIn(', '.join(f.name for f in TABLES[table].schema), sql)

    def test_parameters_preserve_original_window_scope_and_scalar_types(self):
        q = SharingReceiptQuery(query())
        parameters = {p.name:p for p in q.job_config().query_parameters}
        self.assertEqual(set(parameters), set(re.findall(r'@([a-z_]+)', q.sql)))
        self.assertEqual(parameters['accepted_at'].value, TIME)
        self.assertEqual(parameters['deadline_at'].value, TIME+timedelta(days=7))
        self.assertEqual(parameters['audit_prefix'].value, q.command.audit_prefix)
        for name in ('accepted_at','deadline_at'):
            self.assertEqual(parameters[name].type_, 'TIMESTAMP')
            self.assertEqual(parameters[name].value.tzinfo, timezone.utc)
        for name in ('workspace_id','conversation_id','principal_id','audit_prefix'):
            self.assertEqual(parameters[name].type_, 'STRING')
        self.assertNotIn(q.command.audit_id, q.sql)

    def test_read_settings_are_stateless_bounded_and_independent(self):
        q = SharingReceiptQuery(query())
        config = q.job_config()
        self.assertFalse(config.use_legacy_sql or config.use_query_cache or config.create_session)
        self.assertEqual(config.maximum_bytes_billed, 64*1024**2)
        self.assertEqual(config.to_api_repr()['jobTimeoutMs'], '60000')
        self.assertEqual(q.location, LOCATION)
        self.assertIsNone(config.destination)
        self.assertIsNone(config.write_disposition)
        config.query_parameters[0].value='changed'
        self.assertEqual(q.job_config().query_parameters[0].value, q.command.workspace_id)
        with self.assertRaises(FrozenInstanceError):
            q.intent=query()

    def test_only_original_intent_can_construct_receipt_query(self):
        for value in ({}, query(SharingCommitStatus.UNKNOWN), query(SharingCommitStatus.NOT_COMMITTED)):
            with self.assertRaises(ContractError):
                SharingReceiptQuery(value)

    def test_receipt_returns_original_revision_without_mutating_input_or_exposing_identity(self):
        row=receipt_row(); before=deepcopy(row)
        result=self.check(row)
        self.assertEqual(result.state, SharingResultState.REUSED)
        self.assertEqual(result.resulting_revision, 4)
        self.assertEqual(row,before)
        self.assertNotIn('identity_subject',repr(result))
        self.assertFalse(hasattr(result,'observation'))  # No mutation-job commit attribution.

    def test_missing_receipt_is_unknown_even_with_stale_business_and_fence_pins(self):
        row=receipt_row(); row['audit_events']=[]
        row['workspaces'][0]['revision']=20
        row['memberships'][0]['revision']=19
        row['conversations'][0]['access_revision']=18
        result=self.check(row)
        self.assertEqual(result.state,SharingResultState.UNKNOWN)
        self.assertIsNone(result.resulting_revision)

    def test_later_visibility_change_and_current_revisions_preserve_historical_result(self):
        row=receipt_row()
        row['conversations'][0].update(visibility='PRIVATE',access_revision=6)
        row['workspaces'][0]['revision']=12
        row['memberships'][0]['revision']=9
        self.assertEqual(self.check(row).resulting_revision,4)
        self.assertEqual(row['conversations'][0]['visibility'],'PRIVATE')

    def test_changed_envelope_under_same_prefix_conflicts(self):
        from dataclasses import replace
        q=query(); changed=replace(q.command,trace_id='changed')
        with self.assertRaises(ContractError) as caught:
            self.check(q=query(command=changed))
        self.assertEqual(caught.exception.code,'SHARING_IDEMPOTENCY_CONFLICT')

    def test_duplicate_missing_or_malformed_controls_and_receipts_fail_closed(self):
        for key in ('workspaces','memberships','conversations','audit_events'):
            row=receipt_row(); row[key]*=2
            with self.assertRaises(ContractError): self.check(row)
            row=receipt_row(); row[key][0]['extra']='unexpected'
            with self.assertRaises(ContractError): self.check(row)
        for key in ('workspaces','memberships','conversations'):
            row=receipt_row(); row[key]=[]
            with self.assertRaises(ContractError): self.check(row)

    def test_receipt_scope_fields_time_and_causality_are_checked(self):
        for key,value in (('workspace_id','other'),('trace_id','other'),('new_revision',5),
                          ('allowed',False),('request_id','unexpected'),('mutation_outcome','UNKNOWN'),
                          ('occurred_at',TIME+timedelta(days=8))):
            row=receipt_row(); row['audit_events'][0][key]=value
            with self.assertRaises(ContractError): self.check(row)
        row=receipt_row(); row['conversations'][0]['visibility']='PRIVATE'
        with self.assertRaises(ContractError): self.check(row)

    def test_current_membership_owner_and_lifecycle_are_required(self):
        for changes in (dict(status='REVOKED'),dict(roles=['ADMIN']),dict(roles=[])):
            row=receipt_row(); row['memberships'][0].update(changes)
            with self.assertRaises(AuthorizationError): self.check(row)
        row=receipt_row(); row['conversations'][0]['owner_id']=ADMIN.principal_id
        with self.assertRaises(AuthorizationError): self.check(row)
        for key,changes in (('workspaces',dict(status='DISABLED')),('conversations',dict(status='ARCHIVED'))):
            row=receipt_row(); row[key][0].update(changes)
            with self.assertRaises(ContractError): self.check(row)

    def test_observation_window_and_result_shape_are_strict(self):
        for when in (TIME-timedelta(seconds=1),TIME+timedelta(days=7)):
            row=receipt_row(); row['observed_at']=when
            with self.assertRaises(ContractError): self.check(row)
        for rows in ([],[receipt_row(),receipt_row()],[None],[receipt_row()|dict(extra='private')]):
            with self.assertRaises(ContractError) as caught:
                validate_receipt_result(SharingReceiptQuery(query()),rows,original_job_id='original_job')
            self.assertNotIn('private',str(caught.exception))


class SharingReceiptExecutorTests(unittest.TestCase):
    def setUp(self):
        for patcher in (patch('src.application.identity.time.time',return_value=EPOCH),
                        patch('src.application.sharing_intent._now',return_value=TIME),
                        patch('src.application.sharing_executor._now',return_value=TIME)):
            patcher.start(); self.addCleanup(patcher.stop)
        self.client=ClientFixture()
        self.verifier=Mock(spec=IapIdentityAdapter); self.verifier.authenticate.return_value=identity()
        self.sealer=SharingIntentSealer(key=KEY,key_id='fixture-key')
        self.executor=ProtectedSharingExecutor(client=self.client,verifier=self.verifier,
            reader=BigQueryAccessReader(self.client),sealer=self.sealer,
            settings=SharingExecutorSettings(execution_enabled=True))
        self.token=self.sealer.seal(query()); self.headers={'x-goog-iap-jwt-assertion':'fixture'}
        self.reads=[]; self.row=receipt_row(); self.read_job=None; self.after_read=None
        original=self.client.query
        def dispatch(sql,**kwargs):
            if sql.startswith('SELECT\n  CURRENT_TIMESTAMP() AS observed_at,'):
                self.reads.append((sql,kwargs))
                job=bigquery.QueryJob(kwargs['job_id'],sql,client=self.client,job_config=kwargs['job_config'])
                job._properties['jobReference'].update(projectId=PROJECT,location=LOCATION)
                job._properties['status']=dict(state='DONE')
                def result(**options):
                    if self.after_read: self.after_read(job)
                    return [self.row]
                job.result=Mock(side_effect=result); self.read_job=job
                return job
            return original(sql,**kwargs)
        self.client.query=dispatch

    def recover(self):
        return self.executor.recover_receipt(self.headers,self.token)

    def test_recovery_uses_no_original_metadata_or_mutation_and_checks_access_again(self):
        result=self.recover()
        self.assertEqual(result.state,SharingResultState.REUSED)
        self.assertEqual(self.client.lookups+self.client.submissions,[])
        self.assertEqual(len(self.reads),1)
        self.verifier.authenticate.assert_called_once_with(self.headers)
        args=self.reads[0][1]
        self.assertTrue(args['job_id'].startswith('share_receipt_v1_'))
        self.assertNotEqual(args['job_id'],initial_attempt(query()).job_id)
        self.assertEqual((args['project'],args['location']),(PROJECT,LOCATION))
        self.assertIsNone(args['retry']); self.assertIsNone(args['job_retry'])
        self.assertEqual(args['api_method'],bigquery.enums.QueryApiMethod.INSERT)
        self.assertEqual(self.read_job.result.call_args.kwargs['max_results'],2)
        self.assertIsNone(self.read_job.result.call_args.kwargs['job_retry'])

    def test_missing_receipt_never_remints_a_mutation_job(self):
        self.row['audit_events']=[]
        self.assertEqual(self.recover().state,SharingResultState.UNKNOWN)
        self.assertEqual(self.client.lookups+self.client.submissions,[])

    def test_default_disablement_authentication_and_seal_reject_before_io(self):
        self.executor.settings=SharingExecutorSettings()
        with self.assertRaises(SharingExecutorError): self.recover()
        self.assertEqual(self.reads+self.client.schemas,[])
        self.verifier.authenticate.assert_not_called()
        self.executor.settings=SharingExecutorSettings(execution_enabled=True)
        self.token+='x'
        with self.assertRaises(ContractError): self.recover()
        self.assertEqual(self.reads+self.client.schemas,[])
        self.verifier.authenticate.side_effect=IdentityError('INVALID_ASSERTION')
        with self.assertRaises(IdentityError): self.recover()

    def test_current_denial_stops_receipt_lookup(self):
        for changes in (dict(status='REVOKED'),dict(roles=['ADMIN'])):
            self.client.member.update(changes)
            with self.assertRaises(AuthorizationError): self.recover()
            self.client.member=membership()
        self.client.conversation['owner_id']=ADMIN.principal_id
        with self.assertRaises(AuthorizationError): self.recover()
        self.assertEqual(self.reads,[])

    def test_revocation_during_read_denies_release(self):
        self.after_read=lambda job:self.client.member.update(status='REVOKED')
        with self.assertRaises(AuthorizationError): self.recover()
        self.assertEqual(len(self.reads),1)

    def test_identity_or_intent_expiring_during_read_denies_release(self):
        for target,value,error in (('src.application.identity.time.time',EPOCH+1000,IdentityError),
            ('src.application.sharing_executor._now',TIME+timedelta(days=7),ContractError)):
            patcher=patch(target,return_value=value)
            def expire(job):
                patcher.start()
            self.after_read=expire
            try:
                with self.assertRaises(error): self.recover()
            finally:
                patcher.stop()

    def test_unavailable_or_mismatched_job_is_safe_and_never_retried(self):
        for change in (lambda job:job._properties['jobReference'].update(location='us'),
                       lambda job:job._properties['status'].update(state='RUNNING'),
                       lambda job:job._properties['status'].update(errorResult={'reason':'backendError'})):
            self.after_read=change
            with self.assertRaises(SharingExecutorError) as caught: self.recover()
            self.assertEqual(caught.exception.code,'SHARING_RECEIPT_UNAVAILABLE')
        self.assertEqual(len(self.reads),3)
        self.assertEqual(self.client.submissions+self.client.lookups,[])

    def test_schema_drift_blocks_the_select(self):
        self.client.tables['audit_events'].schema=[bigquery.SchemaField('wrong','STRING')]
        with self.assertRaises(SharingExecutorError): self.recover()
        self.assertEqual(self.reads,[])

    def test_lost_read_result_is_safe_and_never_repeated(self):
        def fail(job):
            raise RuntimeError('sensitive transport content')
        self.after_read=fail
        with self.assertRaises(SharingExecutorError) as caught: self.recover()
        self.assertNotIn('sensitive transport content',str(caught.exception))
        self.assertEqual(len(self.reads),1)
        self.assertEqual(self.client.submissions+self.client.lookups,[])

    def test_nested_sdk_struct_rows_normalize_before_validation(self):
        self.row={key: [bigquery.Row(tuple(row.values()),{k:i for i,k in enumerate(row)}) for row in value]
                  if isinstance(value,list) else value for key,value in self.row.items()}
        self.assertEqual(self.recover().state,SharingResultState.REUSED)

    def test_job_match_rejects_unexpected_script_options_on_receipt_select(self):
        self.recover()
        sql,args=self.reads[0]
        expected=SharingReceiptQuery(query()).job_config()
        self.assertTrue(matches_sharing_job(self.read_job,job_id=args['job_id'],sql=sql,config=expected))
        self.read_job._properties['configuration']['query']['scriptOptions']={'statementTimeoutMs':'1'}
        self.assertFalse(matches_sharing_job(self.read_job,job_id=args['job_id'],sql=sql,config=expected))


if __name__=='__main__':
    unittest.main()
