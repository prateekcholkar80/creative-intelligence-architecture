"""Probe guards and synthetic fixtures; no native calls."""
from copy import deepcopy
from unittest.mock import Mock,create_autospec
from google.cloud import bigquery
import unittest
from src.application.campaign_permission_reader import query_spec,decode_snapshot
from src.application.campaign_permission_storage import PREFIX
from src.application.workflow_contract import ContractError
from src.validation.campaign_permission_fixtures import fixtures,CASES
from src.validation.campaign_permission_integration import Scope,validate_report,TTL_MS,load_rows,fixture_bytes
from tests.test_campaign_acceptance_contract import NOW


class PermissionProbeTests(unittest.TestCase):
    def test_oversized_negative_cell_is_preserved_by_bounded_fixture_serializer(self):
        data={'payload_canonical_json':'x'*262145}
        self.assertGreater(len(fixture_bytes(data)),262145)
        self.assertEqual(fixture_bytes({'b':1,'a':2}),fixture_bytes({'a':2,'b':1}))
        with self.assertRaises(ValueError):fixture_bytes({'oversized':'x'*(8*1024*1024)})
    def test_loader_uses_actual_sdk_signature_zero_upload_retries_and_owned_destination(self):
        client=create_autospec(bigquery.Client,instance=True)
        client.load_table_from_json.return_value.job_id='fixture-native-load'
        scope=Scope('a'*32);rows,_=fixtures(NOW);name=PREFIX['CLAIM']+'_versions'
        self.assertEqual(load_rows(client,scope,name,rows[name]),'fixture-native-load')
        call=client.load_table_from_json.call_args
        self.assertEqual(call.args[1],scope.table(name))
        self.assertEqual(call.kwargs['num_retries'],0)
        self.assertEqual(call.kwargs['job_config'].write_disposition,'WRITE_APPEND')
        self.assertNotIn('retry',call.kwargs)
    def test_scope_only_exact_owned_relocation_no_serving_sql(self):
        scope=Scope('a'*32);_,contexts=fixtures(NOW)
        for context in contexts.values():
            sql,cfg=query_spec(context['request']);relocated=scope.relocate(sql)
            self.assertNotIn('.ci_brand_memory.',relocated)
            self.assertIn(scope.dataset,relocated)
            self.assertNotEqual(sql,relocated)
        for sql in ('SELECT 1','SELECT * FROM `other.ds.table`',
                    'SELECT * FROM project.dataset.table',
                    f'DELETE FROM `{scope.table(PREFIX["CLAIM"]+"_heads")}` WHERE TRUE'):
            with self.assertRaises(ValueError):scope.assert_sql(sql)
        with self.assertRaises(ValueError):Scope('bad')
        with self.assertRaises(ValueError):scope.table('unrelated')

    def test_cleanup_ownership_requires_exact_creation_labels_region_and_ttl(self):
        scope=Scope('a'*32);scope.created=NOW
        client=Mock();ds=Mock(project='project-661d501a-e709-4276-a26',dataset_id=scope.dataset_id,
            location='asia-south1',labels=scope.labels,default_table_expiration_ms=TTL_MS,created=NOW)
        client.get_dataset.return_value=ds
        self.assertIs(scope.own(client),ds)
        for attribute,value in [('labels',{}),('created',None),('location','US'),
                                ('default_table_expiration_ms',1),('dataset_id','other')]:
            old=getattr(ds,attribute);setattr(ds,attribute,value)
            with self.assertRaises(ValueError):scope.own(client)
            setattr(ds,attribute,old)
        client.delete_dataset.assert_not_called()

    def test_expiring_fixture_metadata_adapter_checks_real_expiry_without_altering_it(self):
        scope=Scope('a'*32);scope.created=NOW
        client=Mock()
        client.get_dataset.return_value=Mock(project='project-661d501a-e709-4276-a26',dataset_id=scope.dataset_id,
            location='asia-south1',labels=scope.labels,default_table_expiration_ms=TTL_MS,created=NOW)
        actuals={}
        for name in PREFIX.values():
            for suffix in ('_heads','_versions'):
                key=name+suffix;metadata=scope.expected(key).to_api_repr()
                metadata.update(location='asia-south1',type='TABLE',
                    creationTime=str(int(NOW.timestamp()*1000)),
                    expirationTime=str(int(NOW.timestamp()*1000)+TTL_MS))
                actuals[key]=bigquery.Table.from_api_repr(metadata)
        client.get_table.side_effect=lambda key,**kwargs:actuals[key.split('.')[-1]]
        before={name:table.expires for name,table in actuals.items()}
        self.assertEqual(scope.inspect(client),{'status':'VERIFIED'})
        self.assertEqual(before,{name:table.expires for name,table in actuals.items()})
        next(iter(actuals.values())).expires=None
        with self.assertRaises(ValueError):scope.inspect(client)

    def test_fixture_seeds_match_every_declared_case_and_read_decoder_outcome(self):
        tables,contexts=fixtures(NOW)
        self.assertEqual(set(contexts),{case for case,_ in CASES})
        for case,expected in CASES:
            req=contexts[case]['request'];kind=req.kind;record_id=req.checkpoint.record_id
            scoped=[]
            def collect(role,kind,key):
                prefix=PREFIX[kind]
                heads=[r for r in tables[prefix+'_heads'] if r['record_id']==key]
                history=[r for r in tables[prefix+'_versions'] if r['record_id']==key]
                for part,values in [('HEAD',heads),('HISTORY',history)]:
                    for original in values:
                        row=deepcopy(original)
                        if part=='HEAD':row['payload_canonical_json']=None
                        elif case=='oversized':row['payload_canonical_json']=None
                        row.update(scope_role=role,row_role=part,scoped_row_count=len(values))
                        scoped.append(row)
            collect('TARGET',kind,record_id)
            if kind=='CLAIM':
                for e in contexts[case]['expected_record'].evidence:collect('EVIDENCE','EVIDENCE',e.evidence_id)
            with self.subTest(case=case):
                if expected is None:
                    result=decode_snapshot(tuple(scoped),req,native_job_id='fixture-native',checked_at=NOW)
                    self.assertEqual(result.state.current.sha256,contexts[case]['expected_record'].sha256)
                else:
                    with self.assertRaises(ContractError) as caught:
                        decode_snapshot(tuple(scoped),req,native_job_id='fixture-native',checked_at=NOW)
                    self.assertEqual(caught.exception.code,expected)

    def report(self):
        return dict(cases=[dict(case=name,status='PASSED',reason_code=reason,native_job_id='fixture-job-'+str(i))
                    for i,(name,reason) in enumerate(CASES)],fixture_rows_preserved=True,
                    cleanup_verified=True,serving_writes=0,model_calls=0)

    def test_report_requires_exact_cases_outcomes_preservation_and_cleanup(self):
        report=self.report();validate_report(report)
        for change in ({'cases':report['cases'][:-1]}, {'cleanup_verified':False},
                       {'fixture_rows_preserved':False},{'serving_writes':1},{'model_calls':1}):
            with self.assertRaises(ValueError):validate_report(report|change)
        for changes in ({'status':'FAILED'},{'reason_code':'WRONG'},{'native_job_id':None}):
            bad=deepcopy(report);bad['cases'][0].update(changes)
            with self.assertRaises(ValueError):validate_report(bad)


if __name__=='__main__':unittest.main()
