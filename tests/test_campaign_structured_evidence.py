from datetime import timedelta
from dataclasses import replace
import json
from unittest import TestCase
from unittest.mock import Mock,patch
from google.api_core.exceptions import NotFound
from src.application import campaign_structured_evidence as store
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_permission_storage import encode_record,head_row,decode_state
from src.application.campaign_permission_reader import PermissionSnapshot
from src.application.campaign_permission_contract import EvidenceKind
from tests.test_campaign_permission_contract import evidence,NOW
from src.ingestion import setup_structured_evidence as setup

class StructuredEvidenceTests(TestCase):
    def record(self,**changes):
        return dict(schema_version=store.VERSION,record_id='label-record',sku='CI-SKU-0001',evidence_kind='CURRENT_LABEL',
            formula_version='fixture-formula-v1',permission_record_id=None,countries=['CA'],channels=['META'],audiences=['NEW'],
            fields=dict(formula_version='fixture-formula-v1',label_reference='official-label',directions='Apply as directed.',warnings='Avoid eyes.'),
            source_reference='official-label',source_sha256='b'*64,status='UNVERIFIED',review_id=None,
            effective_at=(NOW-timedelta(days=1)).isoformat(),valid_until=(NOW+timedelta(days=1)).isoformat())|changes
    def rows(self,value=None):
        value=value or self.record();item=dict(schema_version=store.VERSION,record_id=value['record_id'],record_sha256=_hash(value),
            sku=value['sku'],evidence_kind=value['evidence_kind'],record_canonical_json=canonical(value),recorded_at=NOW.isoformat())
        return dict(observed_at=NOW,records=[json.dumps(item)],record_count=1,revocations=[],revocation_count=0)
    def project(self,value,*,review=None,formula='fixture-formula-v1',country='CA',revoked=()):
        return store.project_records({value['record_id']:(value,_hash(value))},set(revoked),scope=dict(country=country,channel='META'),
            formula_version=formula,audience='NEW',required_fields=('formula_version','label_reference','directions','warnings'),
            now=NOW,review_reader=review or Mock(side_effect=ValueError('no review')))
    def test_user_or_approved_marker_alone_never_establishes_coverage(self):
        self.assertEqual(self.project(self.record()),('PENDING_REVIEW',()))
        value=self.record(status='APPROVED',review_id='review')
        self.assertEqual(self.project(value),('CONFLICT',()))
    def test_current_independent_review_must_pin_exact_structured_content(self):
        value=self.record(status='APPROVED',review_id='fixture-study')
        pin=replace(evidence(),kind=EvidenceKind.PRODUCT_FACT,content_sha256=_hash(value),endpoint=None,sample_size=None,
            duration_days=None,usage_conditions=None,population=None,statistic=None,statistic_meaning=None)
        version=encode_record(pin,recorded_at=NOW);state=decode_state(head_row(version),(version,),kind='EVIDENCE',record_id=pin.evidence_id)
        snapshot=PermissionSnapshot(state,(),'review-job',NOW)
        status,fields=self.project(value,review=lambda _:snapshot)
        self.assertEqual(status,'CURRENT_APPROVED');self.assertIn('directions',fields)
        self.assertEqual(self.project(value|dict(source_sha256='c'*64),review=lambda _:snapshot)[0],'CONFLICT')
    def test_formula_scope_expiry_revocation_and_empty_are_distinct(self):
        value=self.record(status='APPROVED',review_id='review')
        self.assertEqual(self.project(value,formula='other')[0],'CONFLICT')
        self.assertEqual(self.project(value,country='US')[0],'ABSENT')
        self.assertEqual(self.project(value,revoked=(value['record_id'],))[0],'REJECTED')
        self.assertEqual(self.project(value|dict(valid_until=NOW.isoformat()))[0],'STALE')
        self.assertEqual(store.project_records({},set(),scope={},formula_version=None,audience='NEW',required_fields=(),now=NOW,review_reader=Mock()),('ABSENT',()))
    def test_wrong_fields_prices_and_approval_shape_deny(self):
        for value in (self.record(fields={'invented_benefit':'cures acne'}),self.record(status='APPROVED'),
            self.record(evidence_kind='CURRENT_PRICE',fields={'price':'NaN'}),
            self.record(evidence_kind='CURRENT_PRICE',fields={'price':'-1'})):
            with self.assertRaises(ValueError):store.validate_record(value)
    def test_partial_duplicate_corrupt_and_unsupported_rows_deny(self):
        row=self.rows();self.assertEqual(len(store.decode_rows(row,sku='CI-SKU-0001',kind='CURRENT_LABEL',now=NOW)[0]),1)
        for changed in (row|dict(record_count=2),row|dict(records=row['records']*2,record_count=2),row|dict(records=[None])):
            with self.assertRaises(ValueError):store.decode_rows(changed,sku='CI-SKU-0001',kind='CURRENT_LABEL',now=NOW)
        self.assertEqual(store.validate_record(self.record(sku='CI-SKU-0021'))['sku'],'CI-SKU-0021')
    def test_empty_schema_setup_and_reapply_never_insert_business_rows(self):
        client=Mock(project=store.PROJECT,location=store.LOCATION)
        client.get_dataset.return_value=Mock(location=store.LOCATION,default_table_expiration_ms=None,default_partition_expiration_ms=None)
        tables={}
        def get(ref,**kw):
            if ref.table_id not in tables:raise NotFound('missing')
            return tables[ref.table_id]
        def create(table,**kw):table._properties.update(type='TABLE',location=store.LOCATION);tables[table.table_id]=table;return table
        client.get_table.side_effect=get;client.create_table.side_effect=create
        self.assertEqual(len(setup.setup(client,apply=True)['created_tables']),2)
        self.assertEqual(setup.setup(client,apply=True)['created_tables'],[]);client.query.assert_not_called()

    def test_native_read_error_correlation_and_revoked_access_never_become_absence(self):
        client=Mock(project=store.PROJECT,location=store.LOCATION,default_query_job_config=None)
        native=Mock(state='DONE',error_result=None)
        native.result.return_value=[dict(observed_at=NOW,records=[],record_count=0,revocations=[],revocation_count=0)]
        client.query.return_value=native;client.get_job.return_value=native
        def read():
            return store.read_structured(client,scope=dict(workspace_id='w',conversation_id='c',sku='CI-SKU-0021',country='CA',channel='META'),
                kind='CURRENT_LABEL',formula_version=None,audience='NEW',required_fields=('directions',),
                identity=Mock(),access_reader=Mock(),clock=lambda:NOW)
        with patch.object(store,'validate_table'),patch.object(store,'matches_sharing_job',return_value=True),\
            patch.object(store,'_access',return_value='access'):
            self.assertEqual(read().state,'ABSENT')
            with patch.object(store,'matches_sharing_job',return_value=False):
                with self.assertRaises(ValueError):read()
            with patch.object(store,'_access',side_effect=['access','revoked']):
                with self.assertRaises(ValueError):read()
            client.query.side_effect=TimeoutError('source failed')
            with self.assertRaises(TimeoutError):read()

    def test_exclusion_record_cannot_be_reused_for_another_offer(self):
        value=self.record(evidence_kind='OFFER_EXCLUSIONS',fields={'exclusions':'No stacking.'},permission_record_id='offer-1')
        store.validate_record(value)
        status,_=store.project_records({value['record_id']:(value,_hash(value))},set(),scope=dict(country='CA',channel='META'),
            formula_version='fixture-formula-v1',audience='NEW',required_fields=('exclusions',),now=NOW,
            review_reader=Mock(),permission_record_id='offer-2',expected_exclusions='No stacking.')
        self.assertEqual(status,'ABSENT')

    def test_current_record_read_expires_with_review_or_record_window(self):
        value=self.record(status='APPROVED',review_id='fixture-study',valid_until=(NOW+timedelta(seconds=5)).isoformat())
        pin=replace(evidence(),kind=EvidenceKind.PRODUCT_FACT,content_sha256=_hash(value),endpoint=None,sample_size=None,
            duration_days=None,usage_conditions=None,population=None,statistic=None,statistic_meaning=None)
        version=encode_record(pin,recorded_at=NOW);state=decode_state(head_row(version),(version,),kind='EVIDENCE',record_id=pin.evidence_id)
        snapshot=PermissionSnapshot(state,(),'review-job',NOW)
        native=Mock(state='DONE',error_result=None);native.result.return_value=[self.rows(value)]
        client=Mock(project=store.PROJECT,location=store.LOCATION,default_query_job_config=None)
        client.query.return_value=native;client.get_job.return_value=native
        with patch.object(store,'validate_table'),patch.object(store,'matches_sharing_job',return_value=True),\
            patch.object(store,'_access',return_value='access'),patch.object(store,'read_permission',return_value=snapshot):
            read=store.read_structured(client,scope=dict(workspace_id='w',conversation_id='c',sku='CI-SKU-0001',country='CA',channel='META'),
                kind='CURRENT_LABEL',formula_version='fixture-formula-v1',audience='NEW',required_fields=('directions',),
                identity=Mock(),access_reader=Mock(),clock=lambda:NOW)
            self.assertEqual(read.valid_until,NOW+timedelta(seconds=5))

    def test_price_availability_shipping_and_linked_exclusions_have_reviewed_projections(self):
        start=(NOW-timedelta(days=1)).isoformat()
        examples={
            'CURRENT_PRICE':dict(price='19.95',currency='CAD',effective_at=start),
            'CURRENT_FULFILMENT':dict(availability='IN_STOCK',effective_at=start),
            'CURRENT_SHIPPING':dict(shipping_terms='Standard shipping.',effective_at=start),
            'OFFER_EXCLUSIONS':dict(exclusions='No stacking.'),
        }
        for kind,fields in examples.items():
            permission='offer-1' if kind=='OFFER_EXCLUSIONS' else None
            value=self.record(evidence_kind=kind,fields=fields,status='APPROVED',review_id='fixture-study',permission_record_id=permission)
            store.validate_record(value)
            pin=replace(evidence(),kind=EvidenceKind.PRODUCT_FACT,content_sha256=_hash(value),endpoint=None,sample_size=None,
                duration_days=None,usage_conditions=None,population=None,statistic=None,statistic_meaning=None)
            version=encode_record(pin,recorded_at=NOW);state=decode_state(head_row(version),(version,),kind='EVIDENCE',record_id=pin.evidence_id)
            snapshot=PermissionSnapshot(state,(),'review-job',NOW)
            status,available=store.project_records({value['record_id']:(value,_hash(value))},set(),
                scope=dict(country='CA',channel='META'),formula_version=None,audience='NEW',required_fields=tuple(sorted(fields)),
                now=NOW,review_reader=lambda _:snapshot,permission_record_id=permission,expected_exclusions=fields.get('exclusions'))
            self.assertEqual(status,'CURRENT_APPROVED');self.assertEqual(set(available),set(fields))
