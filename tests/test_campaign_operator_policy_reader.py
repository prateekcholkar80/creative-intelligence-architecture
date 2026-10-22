"""Operator-policy read/setup guards; all cloud calls are mocked."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound,Conflict
from google.cloud import bigquery
from src.application import campaign_operator_policy_reader as reader
from src.ingestion import setup_campaign_operator_policy as setup_module
from src.application.campaign_operator_policy_storage import POLICY_ID,SCHEMAS,HEAD_TABLE,HISTORY_TABLE,encode_record,head_row,table_metadata
from src.application.campaign_permission_reader import Checkpoint
from src.application.campaign_review_storage import row_json
from src.application.identity import IapSettings
from src.application.workflow_contract import ContractError
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION
from src.ingestion.setup_workflow_tables import SchemaMismatch
from tests.test_campaign_operator_policy_storage import record
from tests.test_campaign_review_publication_writer import OPERATOR
from tests.test_campaign_permission_lifecycle import identity,NOW,AUDIENCE


def wire(*records):
    records=records or (record(),);history=[encode_record(r) for r in records]
    return tuple(dict(row_role=role,policy_id=POLICY_ID,revision=row['revision'],record_json=row_json(row),scoped_row_count=len(rows))
        for role,rows in (('HEAD',[head_row(history[-1])]),('HISTORY',history)) for row in rows)


def decode(values=None,checkpoint=None,actor=None):
    return reader.decode_snapshot(wire() if values is None else values,checkpoint or Checkpoint(POLICY_ID),actor or OPERATOR,
        expected_audience=IapSettings(AUDIENCE),native_job_id='fixture-policy-read',checked_at=NOW)


class OperatorReaderTests(TestCase):
    def test_current_policy_and_all_explicit_roles_read_without_escalation(self):
        for actor in (OPERATOR,identity()):
            result=decode(actor=actor)
            self.assertEqual(result.state.current,record());self.assertEqual(result.checked_at,NOW)
            self.assertEqual(len(result.history_json),1)
        with self.assertRaises(ContractError):decode(actor=identity('bootstrap-owner'))

    def test_fixed_scope_missing_duplicate_partial_and_overflow_deny(self):
        rows=wire()
        for values in ((),rows[:1],rows[1:],rows+rows[:1],rows[:1]*reader.MAX_ROWS):
            with self.assertRaises(ContractError):decode(values)
        for changes in ({'row_role':'OTHER'},{'policy_id':'other'},{'revision':True},{'revision':2},
            {'scoped_row_count':2},{'record_json':None},{'extra':True}):
            with self.assertRaises(ContractError):decode((rows[0]|changes,)+rows[1:])
        with self.assertRaises(ContractError):decode(checkpoint=Checkpoint('other'))

    def test_checkpoint_revocation_expiry_and_wrong_audience_or_actor_deny(self):
        first=record();second=record(2,first)
        self.assertEqual(decode(wire(first,second),Checkpoint(POLICY_ID,1,first.sha256)).state.current,second)
        for checkpoint in (Checkpoint(POLICY_ID,3),Checkpoint(POLICY_ID,1,'a'*64)):
            with self.assertRaises(ContractError):decode(wire(first,second),checkpoint)
        with self.assertRaises(ContractError):decode(wire(first,replace(second,status='REVOKED')))
        with self.assertRaises(ContractError):decode(wire(replace(first,policy=replace(first.policy,valid_until=NOW))))
        with self.assertRaises(ContractError):decode(actor=replace(OPERATOR,audience=AUDIENCE+'-other'))
        with self.assertRaises(ContractError):decode(actor=replace(OPERATOR,expires_at=int(NOW.timestamp())))

    def test_json_projection_mismatch_and_noncanonical_stored_payload_deny(self):
        rows=list(wire());changed=deepcopy(rows)
        import json
        data=json.loads(changed[0]['record_json']);data['policy_id']='other'
        changed[0]['record_json']=json.dumps(data)
        with self.assertRaises(ContractError):decode(tuple(changed))
        data=json.loads(rows[1]['record_json']);data['payload_canonical_json']+=' '
        rows[1]['record_json']=json.dumps(data)
        with self.assertRaises(ContractError):decode(tuple(rows))

    def test_query_is_fixed_id_parameterized_single_snapshot_no_mutations(self):
        sql,cfg=reader.query_spec()
        self.assertNotIn(POLICY_ID,sql);self.assertEqual(sql.count('UNION ALL'),1)
        self.assertEqual(sql.count('COUNT(*) OVER()'),2);self.assertTrue(sql.endswith('LIMIT 66'))
        self.assertNotIn('INSERT',sql);self.assertFalse(cfg.use_query_cache)
        self.assertEqual(cfg.query_parameters[0].value,POLICY_ID)

    def client(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        job=Mock(state='DONE',error_result=None);job.result.return_value=wire()
        client.query.return_value=job;client.get_job.return_value=job
        return client,job

    def test_native_correlation_retry_disabled_and_no_local_fallback(self):
        client,job=self.client()
        with patch.object(reader,'inspect',return_value={'status':'VERIFIED'}),patch.object(reader,'matches_sharing_job',return_value=True):
            result=reader.read_policy(client,Checkpoint(POLICY_ID),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        self.assertEqual(result.state.current,record());client.query.assert_called_once()
        self.assertIsNone(client.query.call_args.kwargs['retry']);self.assertIsNone(client.query.call_args.kwargs['job_retry'])
        self.assertEqual(client.get_job.call_args.args,(result.native_job_id,))
        with patch.object(reader,'inspect',return_value={'status':'MISSING_RESOURCES'}):
            with self.assertRaises(ContractError):reader.read_policy(client,Checkpoint(POLICY_ID),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)

    def test_expiry_after_schema_preflight_prevents_query(self):
        client,_=self.client()
        with patch.object(reader,'inspect',return_value={'status':'VERIFIED'}):
            with self.assertRaises(ContractError):reader.read_policy(client,Checkpoint(POLICY_ID),OPERATOR,
                expected_audience=IapSettings(AUDIENCE),clock=Mock(side_effect=[NOW,NOW+timedelta(seconds=301)]))
        client.query.assert_not_called()

    def test_wrong_native_job_denies_and_empty_native_read_retains_diagnostic_id(self):
        client,job=self.client()
        with patch.object(reader,'inspect',return_value={'status':'VERIFIED'}),patch.object(reader,'matches_sharing_job',return_value=False):
            with self.assertRaises(ContractError):reader.read_policy(client,Checkpoint(POLICY_ID),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        job.result.return_value=[]
        with patch.object(reader,'inspect',return_value={'status':'VERIFIED'}),patch.object(reader,'matches_sharing_job',return_value=True):
            with self.assertRaises(ContractError) as caught:reader.read_policy(client,Checkpoint(POLICY_ID),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        self.assertTrue(caught.exception.native_job_id.startswith('campaign_operator_policy_read_'))


def actual_metadata(name):
    return bigquery.Table.from_api_repr(table_metadata(name,PROJECT).to_api_repr()|dict(location=LOCATION,type='TABLE',numRows='0'))


class OperatorSetupTests(TestCase):
    def client(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        dataset=bigquery.Dataset(f'{PROJECT}.{DATASET}');dataset.location=LOCATION
        client.get_dataset.return_value=dataset
        client.get_table.side_effect=lambda reference,**kwargs:actual_metadata(reference.table_id)
        return client

    def test_check_and_idempotent_apply_never_create_existing_tables(self):
        client=self.client()
        for apply in (False,True):
            result=setup_module.setup(client,apply=apply)
            self.assertEqual(result['status'],'VERIFIED');self.assertEqual(result['created_tables'],[])
            self.assertEqual(result['policy_rows_written'],0);self.assertFalse(result['serving_enabled'])
        client.create_table.assert_not_called();client.query.assert_not_called()

    def test_missing_creation_and_conflict_are_revalidated(self):
        client=self.client();present=set()
        def get(reference,**kwargs):
            if reference.table_id not in present:raise NotFound('missing')
            return actual_metadata(reference.table_id)
        def create(table,**kwargs):present.add(table.table_id);return table
        client.get_table.side_effect=get;client.create_table.side_effect=create
        result=setup_module.setup(client,apply=True)
        self.assertEqual(set(result['created_tables']),set(SCHEMAS));self.assertEqual(client.create_table.call_count,2)
        client=self.client();present=set();client.get_table.side_effect=get
        def conflict(table,**kwargs):present.add(table.table_id);raise Conflict('race')
        client.create_table.side_effect=conflict
        self.assertEqual(setup_module.setup(client,apply=True)['status'],'VERIFIED')

    def test_incompatible_schema_or_dataset_blocks_all_creation(self):
        client=self.client()
        def invalid(reference,**kwargs):
            table=actual_metadata(reference.table_id);table.clustering_fields=[];return table
        client.get_table.side_effect=invalid
        with self.assertRaises(SchemaMismatch):setup_module.setup(client,apply=True)
        client.create_table.assert_not_called()
        client=self.client();client.get_dataset.return_value.location='US'
        with self.assertRaises(SchemaMismatch):setup_module.setup(client,apply=True)
        client.create_table.assert_not_called()

    def test_empty_snapshot_requires_exact_zero_markers_and_correlated_native_job(self):
        client=self.client();job=Mock(state='DONE',error_result=None)
        values=[dict(table_name=name,row_count=0) for name in SCHEMAS]
        job.result.return_value=values;client.query.return_value=job;client.get_job.return_value=job
        with patch.object(setup_module,'matches_sharing_job',return_value=True):
            result=setup_module.setup(client,require_empty=True)
            self.assertEqual(result['authoritative_row_counts'],{name:0 for name in SCHEMAS})
            for changed in (values[:1],values+values[:1],[values[0]|{'row_count':True},values[1]],
                [values[0]|{'row_count':1},values[1]]):
                job.result.return_value=changed
                with self.assertRaises(ValueError):setup_module.setup(client,require_empty=True)
        job.result.return_value=values
        with patch.object(setup_module,'matches_sharing_job',return_value=False):
            with self.assertRaises(ValueError):setup_module.setup(client,require_empty=True)
