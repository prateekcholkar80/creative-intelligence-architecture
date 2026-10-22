"""Offline authoritative-capture/native-job adapter guards, no cloud calls."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound,Conflict
from google.cloud import bigquery
from src.application import campaign_bootstrap_reader as reader
from src.application.campaign_bootstrap_contract import TABLES,CONTROL_HEAD,CONTROL_HISTORY,bootstrap_job_id
from src.application.campaign_bootstrap_query import bootstrap_spec
from src.application.campaign_bootstrap_executor import ProtectedCampaignBootstrap
from src.application.campaign_review_storage import row_json
from src.application.identity import IapIdentityAdapter,IapSettings
from src.application.workflow_contract import ContractError
from src.application.operation_budget import OperationStopped
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION
from tests.test_campaign_bootstrap import manifest,snapshot,genesis
from tests.test_campaign_permission_lifecycle import identity,NOW,AUDIENCE


def wire(value=None):
    value=snapshot() if value is None else value
    return tuple(dict(table_name=name,row_count=len(rows),records=list(rows) if rows else None) for name,rows in value.tables)


def capture(value=None):
    return reader.decode_capture(wire(value),native_job_id='fixture-capture',checked_at=NOW)


class BootstrapReaderTests(TestCase):
    def test_complete_empty_and_exact_genesis_captures(self):
        for value in (snapshot(),genesis()):
            result=reader.decode_capture(wire(value),native_job_id='fixture',checked_at=NOW)
            self.assertEqual(result.snapshot,value)
            self.assertEqual(result.checked_at,NOW)

    def test_omitted_duplicate_unknown_and_overflow_markers_deny(self):
        rows=wire()
        for supplied in ((),rows[:-1],rows+rows[:1],(rows[0],)+rows[:-1],
            (rows[0]|{'table_name':'unrelated'},)+rows[1:]):
            with self.assertRaises(ContractError):reader.decode_capture(supplied,native_job_id='fixture',checked_at=NOW)
        for changes in ({'row_count':3},{'row_count':True},{'row_count':-1},{'row_count':1},
            {'records':['{}']},{'records':'[]'},{'extra':True}):
            with self.assertRaises(ContractError):reader.decode_capture((rows[0]|changes,)+rows[1:],native_job_id='fixture',checked_at=NOW)

    def test_oversized_ignored_or_truncated_record_cannot_look_empty(self):
        rows=list(wire());index=TABLES.index(CONTROL_HEAD)
        for changes in ({'row_count':1,'records':[]},{'row_count':1,'records':None},
            {'row_count':2,'records':[row_json(manifest().control.row())]},
            {'row_count':1,'records':['x'*65537]}):
            changed=deepcopy(rows);changed[index].update(changes)
            with self.assertRaises(ContractError):reader.decode_capture(tuple(changed),native_job_id='fixture',checked_at=NOW)

    def test_native_json_timestamp_normalization_and_future_denial(self):
        rows=list(wire(genesis()));index=TABLES.index(CONTROL_HEAD)
        text=rows[index]['records'][0].replace('+00:00','Z')
        rows[index]['records']=[text]
        self.assertEqual(reader.decode_capture(tuple(rows),native_job_id='fixture',checked_at=NOW).snapshot,genesis())
        with self.assertRaises(ContractError):reader.decode_capture(tuple(rows),native_job_id='fixture',checked_at=NOW-timedelta(seconds=1))

    def test_query_has_thirteen_markers_positive_counts_and_bounded_cells(self):
        sql,cfg=reader.query_spec()
        self.assertEqual(sql.count('COUNT(*)'),13);self.assertEqual(sql.count('IGNORE NULLS LIMIT 3'),13)
        self.assertEqual(sql.count('UNION ALL'),12);self.assertTrue(sql.endswith('LIMIT 14'))
        self.assertNotIn('INSERT',sql);self.assertNotIn('UPDATE',sql);self.assertFalse(cfg.use_query_cache)

    def native_client(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        job=Mock(state='DONE',error_result=None);job.result.return_value=wire()
        client.query.return_value=job;client.get_job.return_value=job
        return client,job

    def preflight(self):
        return (patch.object(reader,'inspect_reviews',return_value={'status':'VERIFIED'}),
            patch.object(reader,'inspect_permissions',return_value={'status':'VERIFIED'}),
            patch.object(reader,'matches_sharing_job',return_value=True))

    def test_reader_uses_native_original_and_no_query_retries(self):
        client,job=self.native_client();a,b,c=self.preflight()
        with a,b,c:
            result=reader.read_capture(client,manifest(),identity(),expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        self.assertEqual(result.snapshot,snapshot());client.query.assert_called_once();client.get_job.assert_called_once()
        self.assertIsNone(client.query.call_args.kwargs['retry']);self.assertIsNone(client.query.call_args.kwargs['job_retry'])
        self.assertEqual(client.get_job.call_args.args,(result.native_job_id,))

    def test_wrong_actor_denies_before_cloud_reads(self):
        client,_=self.native_client()
        with self.assertRaises(ContractError):reader.read_capture(client,manifest(),identity('other'),expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        client.query.assert_not_called();client.get_dataset.assert_not_called()

    def test_native_mismatch_failed_job_and_missing_schema_deny(self):
        client,job=self.native_client();a,b,c=self.preflight()
        with a,b,c,patch.object(reader,'matches_sharing_job',return_value=False):
            with self.assertRaises(ContractError):reader.read_capture(client,manifest(),identity(),expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        job.error_result={'reason':'invalidQuery'};a,b,c=self.preflight()
        with a,b,c:
            with self.assertRaises(ContractError):reader.read_capture(client,manifest(),identity(),expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        a,b,c=self.preflight();job.error_result=None
        with a,b,c,patch.object(reader,'inspect_reviews',return_value={'status':'MISSING'}):
            with self.assertRaises(ContractError):reader.read_capture(client,manifest(),identity(),expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)

    def test_expiry_during_preflight_prevents_query_and_during_result_withholds_capture(self):
        client,_=self.native_client();a,b,c=self.preflight();expired=NOW+timedelta(seconds=301)
        with a,b,c:
            with self.assertRaises(ContractError):reader.read_capture(client,manifest(),identity(),expected_audience=IapSettings(AUDIENCE),clock=Mock(side_effect=[NOW,expired]))
        client.query.assert_not_called();a,b,c=self.preflight()
        with a,b,c:
            with self.assertRaises(ContractError):reader.read_capture(client,manifest(),identity(),expected_audience=IapSettings(AUDIENCE),clock=Mock(side_effect=[NOW,NOW,NOW,expired]))


class BootstrapExecutorTests(TestCase):
    def setUp(self):
        self.client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        self.adapter=IapIdentityAdapter(IapSettings(AUDIENCE));self.adapter.authenticate=Mock(return_value=identity())
        self.executor=ProtectedCampaignBootstrap(self.client,self.adapter,manifest(),execution_enabled=True,clock=lambda:NOW)
        self.plan=bootstrap_spec(manifest())
        self.job=Mock(state='DONE',error_result=None)
        self.job.result.return_value=[dict(state='APPLIED',manifest_sha256=manifest().sha256,
            control_sha256=manifest().control.sha256,observed_at=NOW)]
        self.client.get_job.return_value=self.job
        self.read=patch('src.application.campaign_bootstrap_executor.read_capture',return_value=capture(genesis())).start()
        self.match_patch=patch('src.application.campaign_bootstrap_executor.matches_sharing_job',return_value=True)
        self.match=self.match_patch.start()
        self.addCleanup(patch.stopall)

    def test_default_disabled_and_wrong_owner_deny_before_cloud(self):
        default=ProtectedCampaignBootstrap(self.client,self.adapter,manifest(),clock=lambda:NOW)
        with self.assertRaises(ContractError):default.submit({})
        self.adapter.authenticate.assert_not_called()
        self.adapter.authenticate.return_value=identity('other')
        with self.assertRaises(ContractError):self.executor.recover({})
        self.client.get_job.assert_not_called();self.client.query.assert_not_called()

    def test_single_insert_only_after_notfound_fresh_empty_capture(self):
        self.client.get_job.side_effect=NotFound('absent');self.client.query.return_value=self.job
        self.read.side_effect=[capture(),capture(genesis())]
        outcome=self.executor.submit({})
        self.assertEqual(outcome.native_status,Status.COMMITTED);self.assertEqual(outcome.result_state,'APPLIED');self.assertTrue(outcome.submitted)
        self.client.query.assert_called_once();kwargs=self.client.query.call_args.kwargs
        self.assertEqual(kwargs['job_id'],bootstrap_job_id());self.assertIsNone(kwargs['job_retry']);self.assertIsNone(kwargs['retry'])
        self.assertEqual(self.read.call_count,2)

    def test_known_job_and_duplicate_submission_never_insert(self):
        for operation in (self.executor.submit,self.executor.recover):
            self.assertEqual(operation({}).result_state,'APPLIED')
        self.client.query.assert_not_called()

    def test_lost_response_and_conflict_recover_original_without_second_insert(self):
        for error in (RuntimeError('discarded'),Conflict('competing submission')):
            self.client.reset_mock();self.client.get_job.side_effect=[NotFound('absent'),self.job]
            self.client.query.side_effect=error;self.read.side_effect=[capture(),capture(genesis())]
            result=self.executor.submit({})
            self.assertEqual(result.native_status,Status.COMMITTED);self.assertTrue(result.submitted)
            self.client.query.assert_called_once()
            for call in self.client.get_job.call_args_list:self.assertEqual(call.args,(bootstrap_job_id(),))

    def test_absent_recovery_unknown_lookup_and_mismatch_never_insert(self):
        self.client.get_job.side_effect=NotFound('absent')
        self.assertEqual(self.executor.recover({}).native_status,Status.UNKNOWN)
        self.client.get_job.side_effect=RuntimeError('unavailable')
        self.assertEqual(self.executor.submit({}).native_status,Status.UNKNOWN)
        self.client.get_job.side_effect=None;self.match.return_value=False
        self.assertEqual(self.executor.submit({}).native_status,Status.UNKNOWN)
        self.client.query.assert_not_called()

    def test_genesis_present_without_original_and_partial_snapshot_deny_dispatch(self):
        self.client.get_job.side_effect=NotFound('absent')
        for value in (genesis(),snapshot({CONTROL_HEAD:(row_json(manifest().control.row()),)})):
            self.read.return_value=capture(value)
            with self.assertRaises(ContractError):self.executor.submit({})
        self.client.query.assert_not_called()

    def test_positive_rollback_postcommit_and_unknown_failure_do_not_remint(self):
        self.job.error_result={'reason':'invalidQuery'}
        for status in (Status.NOT_COMMITTED,Status.COMMITTED,Status.UNKNOWN):
            with patch('src.application.campaign_bootstrap_executor.failed_sharing_job_status',return_value=status):
                result=self.executor.submit({})
            self.assertEqual(result.native_status,status);self.assertIsNone(result.result_state)
        self.client.query.assert_not_called()

    def test_malformed_result_and_timeout_remain_unknown(self):
        self.job.result.return_value=[{}]
        self.assertEqual(self.executor.recover({}).native_status,Status.UNKNOWN)
        self.job.state='RUNNING';self.job.result.side_effect=TimeoutError('bounded wait')
        self.assertEqual(self.executor.recover({}).native_status,Status.UNKNOWN)
        self.client.query.assert_not_called()

    def test_expired_manifest_recovers_old_job_with_fresh_owner_but_cannot_dispatch(self):
        later=NOW+timedelta(hours=1);self.executor.clock=lambda:later
        self.adapter.authenticate.return_value=replace(identity(),issued_at=int(later.timestamp())-30,expires_at=int(later.timestamp())+300)
        self.assertEqual(self.executor.recover({}).native_status,Status.COMMITTED)
        self.client.get_job.side_effect=NotFound('absent')
        with self.assertRaises(ContractError):self.executor.submit({})
        self.client.query.assert_not_called()

    def test_dispatch_window_expiry_after_capture_denies_insert(self):
        self.client.get_job.side_effect=NotFound('absent');self.read.return_value=capture()
        now=Mock(return_value=NOW);self.executor.clock=now
        def delayed(*args,**kwargs):
            later=NOW+timedelta(seconds=301);now.return_value=later
            self.adapter.authenticate.return_value=replace(identity(),issued_at=int(later.timestamp())-30,expires_at=int(later.timestamp())+300)
            return capture()
        self.read.side_effect=delayed
        with self.assertRaises(ContractError):self.executor.submit({})
        self.client.query.assert_not_called()

    def test_release_expiry_or_changed_storage_withholds_successful_receipt(self):
        self.adapter.authenticate.side_effect=[identity(),identity('other')]
        with self.assertRaises(ContractError):self.executor.recover({})
        self.adapter.authenticate.side_effect=None;self.adapter.authenticate.return_value=identity()
        self.read.return_value=capture()
        with self.assertRaises(ContractError):self.executor.recover({})
        self.client.query.assert_not_called()

    def test_operation_stop_after_insert_propagates_without_recovery_or_replay(self):
        self.client.get_job.side_effect=NotFound('absent');self.read.return_value=capture()
        self.client.query.side_effect=OperationStopped('OPERATION_CANCELLED')
        with self.assertRaises(OperationStopped):self.executor.submit({})
        self.client.query.assert_called_once();self.client.get_job.assert_called_once()

    def test_conflicting_manifest_same_slot_real_sdk_metadata_denies_without_result_read(self):
        self.match_patch.stop()
        config=self.plan.config().to_api_repr();config['query']['query']=self.plan.sql
        native=bigquery.QueryJob.from_api_repr(dict(jobReference=dict(projectId=PROJECT,
            location=LOCATION,jobId=bootstrap_job_id()),configuration=config,status=dict(state='DONE')),self.client)
        native.result=Mock(side_effect=AssertionError('Conflicting result must not be read'))
        self.client.get_job.return_value=native
        conflicting=ProtectedCampaignBootstrap(self.client,self.adapter,replace(manifest(),reason_code='OTHER_INTENT'),
            execution_enabled=True,clock=lambda:NOW)
        self.assertEqual(conflicting.recover({}).native_status,Status.UNKNOWN)
        self.assertEqual(conflicting.submit({}).native_status,Status.UNKNOWN)
        native.result.assert_not_called();self.client.query.assert_not_called();self.read.assert_not_called()

    def test_wrong_project_region_default_config_or_audience_rejected(self):
        for values in (dict(project='other'),dict(location='us-central1'),dict(default_query_job_config=bigquery.QueryJobConfig())):
            client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
            for name,value in values.items():setattr(client,name,value)
            with self.assertRaises(ContractError):ProtectedCampaignBootstrap(client,self.adapter,manifest())
        other_adapter=IapIdentityAdapter(IapSettings(AUDIENCE+'-other'))
        with self.assertRaises(ContractError):ProtectedCampaignBootstrap(self.client,other_adapter,manifest())
