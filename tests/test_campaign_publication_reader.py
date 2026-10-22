"""P6 publication capture and review-resource guards, no cloud calls."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
import unittest
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound,Conflict
from google.cloud import bigquery
from src.application.campaign_publication_reader import (
    PublicationReadRequest,decode_capture,query_spec,read_capture,MAX_ROWS,
)
from src.application.campaign_permission_reader import Checkpoint
from src.application.campaign_permission_lifecycle import Action
from src.application.campaign_review_storage import (
    SCHEMAS,ReviewVersion,encode_review,review_head,row_json,table_metadata,
)
from src.application.campaign_permission_storage import encode_record,head_row
from src.application.identity import IapSettings
from src.application.workflow_contract import ContractError
from src.ingestion import setup_campaign_review_tables as setup_module
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION,DATASET
from src.ingestion.setup_workflow_tables import SchemaMismatch
from tests.test_campaign_review_publication import bundle
from tests.test_campaign_permission_lifecycle import identity,grant,capture,draft,NOW,AUDIENCE


def request(action=Action.APPROVE):
    return PublicationReadRequest(action,'CLAIM',draft().claim_id,Checkpoint(grant().grant_id),
        Checkpoint(capture(draft()).review_id) if action is Action.APPROVE else None,
        evidence=(Checkpoint('fixture-study'),) if action is Action.APPROVE else ())


def wire_rows(action=Action.APPROVE):
    sources=bundle(action);result=[]
    selected=[sources['control'],sources['grant'],sources['permission']]
    if sources['review']:selected.append(sources['review'])
    selected.extend(sources['evidence'])
    for source in selected:
        kind='CLAIM_EVIDENCE' if source in sources['evidence'] else source.kind
        for part,items in [('HEAD',[source.head_json] if source.head_json else []),('HISTORY',source.history_json)]:
            for text in items:
                parsed=json.loads(text)
                # BigQuery's outer JSON order and Z timestamps differ from the
                # canonical storage wire; exact values are preserved on parsing.
                parsed['recorded_at']=parsed['recorded_at'].replace('+00:00','Z')
                native=json.dumps(parsed)
                result.append(dict(capture_kind=kind,row_role=part,record_id=source.record_id,
                    revision=parsed['revision'],record_json=native,scoped_row_count=len(items)))
    return tuple(result)


def decode(rows=None,req=None,actor=None):
    return decode_capture(wire_rows() if rows is None else rows,req or request(),actor or identity(),
        expected_audience=IapSettings(AUDIENCE),native_job_id='fixture-native',checked_at=NOW)


class PublicationReaderTests(unittest.TestCase):
    def test_single_snapshot_capture_builds_exact_disabled_plan(self):
        value=decode();plan=value.plan(candidate=draft(),reason_code='FIXTURE_REVIEW')
        expected=bundle(Action.APPROVE)
        self.assertEqual(value.control,expected['control'])
        self.assertEqual(value.grant,expected['grant'])
        self.assertEqual(value.evidence,expected['evidence'])
        self.assertFalse(plan.execution_enabled)

    def test_create_absence_only_after_valid_authority_and_control(self):
        req=request(Action.CREATE_DRAFT);value=decode(wire_rows(Action.CREATE_DRAFT),req)
        self.assertIsNone(value.permission.head_json)
        self.assertFalse(value.plan(candidate=draft(),reason_code='FIXTURE_REVIEW').execution_enabled)
        with self.assertRaises(ContractError):decode((),req)
        with self.assertRaises(ContractError):decode(wire_rows(Action.APPROVE),req)

    def test_missing_partial_duplicate_overflow_and_shape_denied(self):
        rows=wire_rows()
        for values in ((),rows[1:],rows+rows[:1],rows[:1]*MAX_ROWS):
            with self.assertRaises(ContractError):decode(values)
        for changes in ({'scoped_row_count':2},{'scoped_row_count':True},
                        {'capture_kind':'OTHER'},{'record_json':None},{'revision':True},{'extra':True}):
            with self.assertRaises(ContractError):decode((rows[0]|changes,)+rows[1:])

    def test_unauthorized_identity_wrong_audience_or_expiry_never_releases_capture(self):
        for actor in (identity('other'),replace(identity(),expires_at=int(NOW.timestamp())),
                      replace(identity(),audience=AUDIENCE+'-other')):
            with self.assertRaises(ContractError):decode(actor=actor)
        with self.assertRaises(ContractError):decode_capture(wire_rows(),request(),None,
            expected_audience=IapSettings(AUDIENCE),native_job_id='fixture',checked_at=NOW)

    def test_control_grant_permission_evidence_and_review_checkpoint_pins(self):
        req=request()
        for values in ({'control':Checkpoint('campaign-permissions',2)},
                       {'grant':Checkpoint(grant().grant_id,1,'0'*64)},
                       {'permission':Checkpoint(req.record_id,2)},
                       {'review':Checkpoint(capture(draft()).review_id,2)},
                       {'evidence':(Checkpoint('fixture-study',2),)}):
            with self.assertRaises(ContractError):decode(req=replace(req,**values))

    def test_mutated_review_grant_scope_and_evidence_blocks_plan_or_capture(self):
        rows=list(wire_rows())
        for i,row in enumerate(rows):
            if row['capture_kind']=='REVIEW' and row['row_role']=='HISTORY':
                body=json.loads(row['record_json']);body['payload_sha256']='0'*64
                rows[i]=row|dict(record_json=json.dumps(body))
        with self.assertRaises(ContractError):decode(tuple(rows))
        value=decode();changed=replace(draft(),wording=('Changed wording.',))
        with self.assertRaises(ContractError):value.plan(candidate=changed,reason_code='FIXTURE_REVIEW')

    def test_historical_grant_checkpoint_may_be_expired_when_current_grant_is_eligible(self):
        rows=[r for r in wire_rows() if r['capture_kind']!='GRANT']
        old=ReviewVersion(replace(grant(),valid_until=NOW-timedelta(seconds=1)),1,None,'ACTIVE','operator',NOW-timedelta(minutes=2))
        new=ReviewVersion(grant(),2,old.sha256,'ACTIVE','operator',NOW)
        for part,items in [('HEAD',[review_head(encode_review(new))]),('HISTORY',[encode_review(old),encode_review(new)])]:
            for item in items:rows.append(dict(capture_kind='GRANT',row_role=part,record_id=old.record_id,
                revision=item['revision'],record_json=row_json(item),scoped_row_count=len(items)))
        req=replace(request(),grant=Checkpoint(old.record_id,1,old.sha256))
        self.assertEqual(decode(tuple(rows),req).grant.history_json[-1],row_json(encode_review(new)))

    def test_parameterized_all_scopes_one_query_and_server_bounds(self):
        sql,cfg=query_spec(request())
        self.assertNotIn(draft().claim_id,sql);self.assertIn('COUNT(*) OVER',sql)
        self.assertIn('BYTE_LENGTH',sql);self.assertIn('TO_JSON_STRING',sql)
        self.assertNotIn('INSERT',sql);self.assertFalse(cfg.use_query_cache)
        for action in (Action.CREATE_DRAFT,Action.REVOKE,Action.REPLACE_DRAFT):
            query,config=query_spec(request(action))
            self.assertIn('FROM `'+PROJECT,query)
            self.assertNotIn('review_id',[p.name for p in config.query_parameters])

    def client(self,mutate=None,rows=None):
        client=Mock(project=PROJECT)
        def query(sql,**kw):
            original=Mock(project=PROJECT,location=LOCATION,job_id=kw['job_id'],query=sql,state='DONE',error_result=None)
            config=deepcopy(kw['job_config'].to_api_repr());config['query']['query']=sql
            original.to_api_repr.return_value={'configuration':config}
            if mutate:mutate(original,config)
            client.get_job.return_value=original
            return Mock(result=Mock(return_value=wire_rows() if rows is None else rows))
        client.query.side_effect=query;return client

    def read(self,client):
        with patch('src.application.campaign_publication_reader.inspect_reviews',return_value={'status':'VERIFIED'}),patch(
            'src.application.campaign_publication_reader.inspect_permissions',return_value={'status':'VERIFIED'}):
            return read_capture(client,request(),identity(),expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)

    def test_fresh_native_reads_no_mutation_and_exact_metadata_correlated(self):
        client=self.client();first=self.read(client);second=self.read(client)
        self.assertNotEqual(first.native_job_id,second.native_job_id)
        self.assertEqual(first.control,second.control)
        client.create_table.assert_not_called()
        self.assertIsNone(client.query.call_args.kwargs['job_retry'])
        for change in (lambda j,c:setattr(j,'state','RUNNING'),lambda j,c:setattr(j,'query','SELECT 1'),
                       lambda j,c:c['query'].update(useQueryCache=True),lambda j,c:c.update(jobTimeoutMs='1')):
            with self.assertRaises(ContractError):self.read(self.client(change))

    def test_missing_resources_bad_identity_and_provider_failure_deny_without_replay(self):
        client=self.client()
        with self.assertRaises(ContractError):read_capture(client,request(),None,expected_audience=IapSettings(AUDIENCE))
        client.query.assert_not_called()
        with patch('src.application.campaign_publication_reader.inspect_reviews',return_value={'status':'MISSING_RESOURCES'}):
            with self.assertRaises(ContractError):read_capture(client,request(),identity(),expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        client.query.assert_not_called()
        client.query.side_effect=TimeoutError('fixture')
        with self.assertRaises(TimeoutError):self.read(client)
        self.assertEqual(client.query.call_count,1)

    def test_identity_is_rechecked_after_provider_response(self):
        client=self.client();stamps=iter((NOW,NOW+timedelta(seconds=301)))
        with patch('src.application.campaign_publication_reader.inspect_reviews',return_value={'status':'VERIFIED'}),patch(
            'src.application.campaign_publication_reader.inspect_permissions',return_value={'status':'VERIFIED'}):
            with self.assertRaises(ContractError) as caught:
                read_capture(client,request(),identity(),expected_audience=IapSettings(AUDIENCE),clock=lambda:next(stamps))
        self.assertEqual(caught.exception.code,'PUBLICATION_READER_IDENTITY_DENIED')
        self.assertEqual(client.query.call_count,1)

    def test_checkpoint_on_absent_target_cannot_be_treated_as_genesis(self):
        req=replace(request(Action.CREATE_DRAFT),permission=Checkpoint(draft().claim_id,2))
        with self.assertRaises(ContractError):decode(wire_rows(Action.CREATE_DRAFT),req)

    def test_invalid_review_and_evidence_scopes_are_rejected_before_query(self):
        req=request()
        for change in ({'review':None},{'permission':Checkpoint('other')},
                       {'control':Checkpoint('other')},{'evidence':(Checkpoint('x'),Checkpoint('x'))}):
            with self.assertRaises(ContractError):replace(req,**change)


class ReviewSetupTests(unittest.TestCase):
    def client(self):
        c=Mock(project=PROJECT)
        c.get_dataset.return_value=bigquery.Dataset.from_api_repr(dict(datasetReference=dict(projectId=PROJECT,datasetId=DATASET),location=LOCATION))
        def actual(ref,**kwargs):
            payload=table_metadata(ref.table_id,PROJECT).to_api_repr()
            return bigquery.Table.from_api_repr(payload|dict(location=LOCATION,type='TABLE',numRows='0'))
        c.get_table.side_effect=actual;return c

    def test_seven_existing_exact_resources_check_does_not_write(self):
        c=self.client();result=setup_module.setup(c)
        self.assertEqual(len(result['verified_tables']),7)
        self.assertEqual(result['created_tables'],[]);c.query.assert_not_called();c.create_table.assert_not_called()

    def test_absent_resources_full_preflight_and_idempotent_apply(self):
        c=self.client();original=c.get_table.side_effect;present=set();events=[]
        def get(ref,**kwargs):
            events.append(('GET',ref.table_id))
            if ref.table_id not in present:raise NotFound('fixture')
            return original(ref)
        def create(t,**kwargs):events.append(('CREATE',t.table_id));present.add(t.table_id)
        c.get_table.side_effect=get;c.create_table.side_effect=create
        result=setup_module.setup(c,apply=True)
        self.assertEqual(len(result['created_tables']),7)
        self.assertTrue(all(kind=='GET' for kind,_ in events[:7]))
        self.assertEqual(setup_module.setup(c,apply=True)['created_tables'],[])

    def test_later_schema_conflict_blocks_all_creation(self):
        c=self.client();original=c.get_table.side_effect;names=list(SCHEMAS)
        def get(ref,**kw):
            if ref.table_id==names[0]:raise NotFound('fixture')
            table=original(ref)
            if ref.table_id==names[-1]:table.schema=table.schema[:-1]
            return table
        c.get_table.side_effect=get
        with self.assertRaises(SchemaMismatch):setup_module.setup(c,apply=True)
        c.create_table.assert_not_called()

    def test_empty_snapshot_requires_all_seven_zero_counts(self):
        c=self.client()
        def query(sql,**kw):
            job=Mock(project=PROJECT,location=LOCATION,job_id=kw['job_id'],query=sql,state='DONE',error_result=None)
            config=kw['job_config'].to_api_repr();config['query']['query']=sql
            job.to_api_repr.return_value={'configuration':config};c.get_job.return_value=job
            return Mock(result=Mock(return_value=[dict(table_name=n,row_count=0) for n in SCHEMAS]))
        c.query.side_effect=query
        result=setup_module.setup(c,require_empty=True)
        self.assertEqual(len(result['authoritative_row_counts']),7)
        self.assertNotIn('queryParameters',c.query.call_args.kwargs['job_config'].to_api_repr()['query'])
        c.create_table.assert_not_called()


if __name__=='__main__':unittest.main()
