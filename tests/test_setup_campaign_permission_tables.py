"""P6 schema resource setup guards. No native cloud calls."""
from unittest.mock import Mock
import unittest
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery
from src.ingestion.setup_campaign_permission_tables import inspect,setup,verify_empty
from src.application.campaign_permission_storage import TABLE_SCHEMAS,table_metadata
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION
from src.ingestion.setup_workflow_tables import SchemaMismatch


def actual(name):
    spec=table_metadata(name,PROJECT)
    result=bigquery.Table.from_api_repr(spec.to_api_repr()|dict(location=LOCATION,type='TABLE',numRows='0'))
    return result


def client():
    c=Mock(project=PROJECT)
    c.get_dataset.return_value=bigquery.Dataset.from_api_repr(dict(
        datasetReference=dict(projectId=PROJECT,datasetId=DATASET),location=LOCATION))
    c.get_table.side_effect=lambda ref,**kwargs:actual(ref.table_id)
    return c


class SetupPermissionTests(unittest.TestCase):
    def test_exact_present_resources_read_only(self):
        c=client();result=setup(c)
        self.assertEqual(result['status'],'VERIFIED')
        self.assertEqual(len(result['verified_tables']),6)
        self.assertEqual(result['created_tables'],[])
        c.create_table.assert_not_called();c.query.assert_not_called()

    def test_check_reports_missing_without_creation(self):
        c=client();c.get_table.side_effect=NotFound('fixture')
        result=setup(c)
        self.assertEqual(result['status'],'MISSING_RESOURCES')
        self.assertEqual(len(result['missing_tables']),6)
        c.create_table.assert_not_called()

    def test_apply_creates_only_missing_after_full_preflight_and_is_idempotent(self):
        c=client();names=list(TABLE_SCHEMAS);events=[];present=set(names[1:])
        def get(ref,**kwargs):
            events.append(('GET',ref.table_id))
            if ref.table_id not in present:raise NotFound('fixture')
            return actual(ref.table_id)
        def create(table,**kwargs):events.append(('CREATE',table.table_id));present.add(table.table_id)
        c.get_table.side_effect=get;c.create_table.side_effect=create
        first=setup(c,apply=True);second=setup(c,apply=True)
        self.assertEqual(first['created_tables'],names[:1]);self.assertEqual(second['created_tables'],[])
        self.assertEqual([name for event,name in events[:6] if event=='GET'],names)
        self.assertEqual(c.create_table.call_count,1);c.query.assert_not_called()

    def test_incompatible_later_resource_prevents_any_creation(self):
        c=client();names=list(TABLE_SCHEMAS)
        def get(ref,**kwargs):
            if ref.table_id==names[0]:raise NotFound('fixture')
            obj=actual(ref.table_id)
            if ref.table_id==names[-1]:obj.schema=obj.schema[:-1]
            return obj
        c.get_table.side_effect=get
        with self.assertRaises(SchemaMismatch):setup(c,apply=True)
        c.create_table.assert_not_called()

    def test_wrong_project_dataset_region_expiration_and_flags_denied(self):
        for attribute,value in [('project','wrong')]:
            c=client();setattr(c,attribute,value)
            with self.assertRaises(ValueError):setup(c,apply=True)
            c.create_table.assert_not_called()
        for changes in (dict(location='US'),dict(defaultTableExpirationMs='1000'),
                        dict(datasetReference=dict(projectId=PROJECT,datasetId='wrong'))):
            c=client();payload=c.get_dataset.return_value.to_api_repr();payload.update(changes)
            c.get_dataset.return_value=bigquery.Dataset.from_api_repr(payload)
            with self.assertRaises(SchemaMismatch):setup(c,apply=True)
            c.create_table.assert_not_called()
        for flags in (dict(apply=1),dict(require_empty='yes')):
            c=client()
            with self.assertRaises(ValueError):setup(c,**flags)
            c.get_dataset.assert_not_called()

    def test_concurrent_creator_must_still_match_exact_schema(self):
        c=client();names=list(TABLE_SCHEMAS);first=True
        def get(ref,**kwargs):
            nonlocal first
            if ref.table_id==names[0] and first:first=False;raise NotFound('fixture')
            return actual(ref.table_id)
        c.get_table.side_effect=get;c.create_table.side_effect=Conflict('fixture')
        result=setup(c,apply=True)
        self.assertEqual(result['created_tables'],[])
        self.assertEqual(result['status'],'VERIFIED')

    def empty_client(self, *, rows=None, mutate=None):
        c=client()
        def query(sql,**kwargs):
            native=Mock(project=PROJECT,location=LOCATION,job_id=kwargs['job_id'],query=sql,
                        state='DONE',error_result=None)
            config=kwargs['job_config'].to_api_repr();config['query']['query']=sql
            native.to_api_repr.return_value={'configuration':config}
            if mutate:mutate(native,config)
            c.get_job.return_value=native
            values=[dict(table_name=name,row_count=0) for name in TABLE_SCHEMAS] if rows is None else rows
            return Mock(result=Mock(return_value=values))
        c.query.side_effect=query
        return c

    def test_authoritative_empty_snapshot_all_six_counts_and_native_identity(self):
        c=self.empty_client();result=setup(c,require_empty=True)
        self.assertEqual(len(result['authoritative_row_counts']),6)
        self.assertEqual(result['permission_rows_written'],0)
        self.assertFalse(result['serving_enabled'])
        sql=c.query.call_args.args[0]
        self.assertNotIn('INSERT',sql);self.assertNotIn('UPDATE',sql);self.assertNotIn('DELETE',sql)
        self.assertIsNone(c.query.call_args.kwargs['job_retry'])
        self.assertNotIn('queryParameters',c.query.call_args.kwargs['job_config'].to_api_repr()['query'])
        c.create_table.assert_not_called()

    def test_missing_duplicate_nonempty_or_invalid_count_denied(self):
        baseline=[dict(table_name=name,row_count=0) for name in TABLE_SCHEMAS]
        for rows in (baseline[:-1],baseline+baseline[:1],
                     [baseline[0]|dict(row_count=1)]+baseline[1:],
                     [baseline[0]|dict(row_count=False)]+baseline[1:],
                     [baseline[0]|dict(table_name='unknown')]+baseline[1:]):
            with self.assertRaises(ValueError):verify_empty(self.empty_client(rows=rows))

    def test_native_mismatch_or_provider_failure_does_not_retry_or_create(self):
        for mutate in (lambda j,c:setattr(j,'query','SELECT 1'),lambda j,c:setattr(j,'state','RUNNING'),
                       lambda j,c:c.update(jobTimeoutMs='1'),lambda j,c:c['query'].update(useQueryCache=True)):
            c=self.empty_client(mutate=mutate)
            with self.assertRaises(ValueError):verify_empty(c)
            self.assertEqual(c.query.call_count,1);c.create_table.assert_not_called()
        c=self.empty_client();c.query.side_effect=TimeoutError('fixture')
        with self.assertRaises(TimeoutError):verify_empty(c)
        self.assertEqual(c.query.call_count,1)

    def test_missing_tables_deny_required_empty_check_without_query(self):
        c=client();c.get_table.side_effect=NotFound('fixture')
        with self.assertRaises(ValueError):setup(c,require_empty=True)
        c.query.assert_not_called();c.create_table.assert_not_called()


if __name__=='__main__':unittest.main()
