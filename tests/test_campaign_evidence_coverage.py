"""Synthetic permission captures; no live BigQuery or business approvals."""
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_evidence_coverage as coverage
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_acceptance_contract import _hash,PolicyStatus
from src.application.campaign_permission_storage import encode_record,head_row
from tests.test_campaign_permission_contract import offer,claim,evidence,NOW

class CoverageTests(TestCase):
    def setUp(self):
        self.scope=dict(workspace_id='workspace',conversation_id='conversation',request_id='request',
            sku='CI-SKU-0001',country='CA',channel='META',request_sha256='a'*64,intent_sha256='b'*64)
        self.policy=json.loads(Path('config/clarification/evidence_requirements_v1.json').read_text())
        self.rules={r['requirement_id']:r for r in self.policy['requirements']}
    def row(self,records=()):
        row=dict(observed_at=NOW,scope_sha256=_hash(self.scope))
        for name in coverage.NAMES:row[name]=[];row[name+'_count']=0
        for kind,record in records:
            value=encode_record(record,recorded_at=NOW);prefix=coverage.PREFIX[kind]
            def text(v):return json.dumps(v,default=lambda x:x.isoformat())
            for suffix,item in (('_heads',head_row(value)),('_versions',value)):
                row[prefix+suffix].append(text(item));row[prefix+suffix+'_count']+=1
        return row
    def project(self,records=(),rule='scientific-assertion',**changes):
        states,_=coverage.decode_catalog(self.row(records))
        args=dict(formula_version='fixture-formula-v1',audience='NEW',now=NOW)
        return coverage.project_coverage(states,self.rules[rule],self.scope,**(args|changes))
    def test_complete_empty_read_is_absent_only_for_supported_schema(self):
        self.assertEqual(self.project(),('ABSENT',()))
        self.assertEqual(self.project(rule='current-price'),('UNAVAILABLE',()))
        self.assertEqual(self.project(rule='label-instructions'),('UNAVAILABLE',()))
        self.scope['sku']='CI-SKU-0021';self.assertEqual(self.project(),('UNAVAILABLE',()))
    def test_current_science_covers_metadata_not_full_report_bytes(self):
        state,fields=self.project((('CLAIM',claim()),('EVIDENCE',evidence())))
        self.assertEqual(state,'CURRENT_APPROVED');self.assertIn('approved_wording',fields)
        self.assertNotIn('study_report_reference',fields)
    def test_unreviewed_expired_wrong_formula_and_missing_evidence_are_not_absent(self):
        draft=replace(claim(),approval=replace(claim().approval,status=PolicyStatus.DRAFT,approved_by=None,approved_at=None))
        self.assertEqual(self.project((('CLAIM',draft),))[0],'PENDING_REVIEW')
        records=(('CLAIM',claim()),('EVIDENCE',evidence()))
        self.assertEqual(self.project(records,now=NOW+timedelta(days=2))[0],'STALE')
        self.assertEqual(self.project(records,formula_version='changed')[0],'CONFLICT')
        self.assertEqual(self.project((('CLAIM',claim()),))[0],'CONFLICT')
    def test_offer_scope_and_multiple_records_deny_implicit_selection(self):
        self.scope['channel']='KLAVIYO_EMAIL'
        self.assertEqual(self.project((('OFFER',offer()),),rule='commercial-offer',audience='RETURNING')[0],'CURRENT_APPROVED')
        second=replace(offer(),offer_id='other-offer')
        self.assertEqual(self.project((('OFFER',offer()),('OFFER',second)),rule='commercial-offer',audience='RETURNING')[0],'CONFLICT')
        self.scope['country']='US'
        self.assertEqual(self.project((('OFFER',offer()),),rule='commercial-offer',audience='RETURNING')[0],'ABSENT')
    def test_orphan_duplicate_partial_and_corrupt_rows_deny(self):
        row=self.row((('CLAIM',claim()),));name=coverage.PREFIX['CLAIM']+'_heads'
        variants=[]
        for mutate in (lambda r:r.update({name:[],name+'_count':0}),
            lambda r:r.update({name:r[name]*2,name+'_count':2}),
            lambda r:r.update({name+'_count':2}),lambda r:r.update({name:[None]})):
            r=json.loads(json.dumps(row,default=lambda x:x.isoformat()));mutate(r);variants.append(r)
        for r in variants:
            with self.assertRaises(ValueError):coverage.decode_catalog(r)
    def native(self,row=None):
        client=Mock(project=coverage.PROJECT,location=coverage.LOCATION,default_query_job_config=None)
        job=Mock(state='DONE',error_result=None);job.result.return_value=[row or self.row()]
        client.query.return_value=job;client.get_job.return_value=job
        return client
    def read(self,client):
        return coverage.read_coverage(client,canonical(self.policy),scope=self.scope,features=('SCIENTIFIC_ASSERTION',),
            formula_version='fixture-formula-v1',audience='NEW',identity=Mock(),access_reader=Mock(),clock=lambda:NOW)
    def test_native_correlation_query_failure_and_revocation(self):
        with patch.object(coverage,'_access',return_value='access'),patch.object(coverage,'validate_table'),\
            patch.object(coverage,'matches_sharing_job',return_value=True):
            client=self.native();read=self.read(client)
            self.assertEqual(read.captures[0].state,'ABSENT');client.get_job.assert_called_once()
            client=self.native();client.query.side_effect=TimeoutError('fixture')
            with self.assertRaises(TimeoutError):self.read(client)
            with patch.object(coverage,'matches_sharing_job',return_value=False):
                with self.assertRaises(ValueError):self.read(self.native())
            with patch.object(coverage,'_access',side_effect=['access','revoked']):
                with self.assertRaises(ValueError):self.read(self.native())

    def test_structured_price_source_is_used_and_failure_is_not_absence(self):
        from src.application.campaign_structured_evidence import StructuredRead
        source=StructuredRead('ABSENT',(),'a'*64,'structured-read',NOW,NOW+timedelta(seconds=60))
        with patch.object(coverage,'_access',return_value='access'),patch.object(coverage,'validate_table'),\
            patch.object(coverage,'matches_sharing_job',return_value=True),\
            patch.object(coverage,'read_structured',return_value=source) as structured:
            def read():
                return coverage.read_coverage(self.native(),canonical(self.policy),scope=self.scope,features=('PRICE',),
                    formula_version=None,audience='NEW',identity=Mock(),access_reader=Mock(),clock=lambda:NOW)
            result=read();self.assertEqual(result.captures[0].state,'ABSENT')
            self.assertEqual(result.captures[0].native_job_id,'structured-read');structured.assert_called_once()
            structured.side_effect=TimeoutError('source outage')
            with self.assertRaises(TimeoutError):read()

    def test_two_label_requirements_reuse_one_scoped_read(self):
        from src.application.campaign_structured_evidence import StructuredRead
        source=StructuredRead('CURRENT_APPROVED',('formula_version','label_reference','directions','warnings'),
            'a'*64,'structured-read',NOW,NOW+timedelta(seconds=60))
        with patch.object(coverage,'_access',return_value='access'),patch.object(coverage,'validate_table'),\
            patch.object(coverage,'matches_sharing_job',return_value=True),\
            patch.object(coverage,'read_structured',return_value=source) as structured:
            result=coverage.read_coverage(self.native(),canonical(self.policy),scope=self.scope,features=('DIRECTIONS','WARNINGS'),
                formula_version='fixture-formula-v1',audience='NEW',identity=Mock(),access_reader=Mock(),clock=lambda:NOW)
            self.assertEqual(len(result.captures),2);structured.assert_called_once()
            self.assertNotIn('warnings',result.captures[0].available_fields)
