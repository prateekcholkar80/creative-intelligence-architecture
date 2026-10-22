"""Historical receipt integrity and authorization, offline fixtures only."""
from copy import deepcopy
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_publication_receipt import decode_receipt,query_spec,read_receipt,LIMIT
from src.application.campaign_publication_executor import PublicationIntent,PublicationExecutor
from src.application.campaign_review_storage import parse_row,row_json,PublicationControl
from src.application.campaign_permission_storage import encode_record,head_row,decode_state
from src.application.campaign_permission_lifecycle import Action,propose_permission
from src.application.identity import IapIdentityAdapter,IapSettings
from src.application.workflow_contract import ContractError
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION
from tests.test_campaign_publication_reader import decode
from tests.test_campaign_permission_lifecycle import NOW,AUDIENCE,draft,identity,grant


def fixture():
    intent=PublicationIntent(decode(),draft(),'FIXTURE_REVIEW');plan=intent.plan()
    arrays={p['name']:[parse_row(v['value']) for v in p['parameterValue'].get('arrayValues',[])]
        for p in plan.config().to_api_repr()['query']['queryParameters'] if 'arrayValues' in p['parameterValue']}
    groups=dict(AUDIT=[parse_row(plan.audit_json)],CONTROL_HEAD=arrays['new_control'],
        CONTROL_HISTORY=arrays['old_control_history']+arrays['new_control'],
        PERMISSION_HEAD=arrays['new_head'],PERMISSION_HISTORY=arrays['old_history']+arrays['new_history'])
    return intent,groups


def wire(groups):
    return tuple(dict(capture_kind=kind,record_json=row_json(row)) for kind,rows in groups.items() for row in rows)


class ReceiptTests(TestCase):
    def setUp(self):self.intent,self.groups=fixture()
    def decode(self,groups=None,now=NOW):
        return decode_receipt(wire(self.groups if groups is None else groups),self.intent,
            read_job_id='fixture-read',checked_at=now)

    def test_exact_receipt_and_expired_original_are_historical_only(self):
        for now in (NOW,NOW+timedelta(days=90)):
            receipt=self.decode(now=now)
            self.assertEqual(receipt.state,'VERIFIED')
            self.assertEqual(receipt.record_sha256,self.intent.plan().proposed_record_sha256)

    def test_later_revocation_and_control_updates_preserve_receipt(self):
        groups=deepcopy(self.groups)
        later=propose_permission(Action.REVOKE,identity=identity(),grant=grant(),current=
            decode_state(
                groups['PERMISSION_HEAD'][0],tuple(groups['PERMISSION_HISTORY']),kind='CLAIM',record_id=draft().claim_id),
            checked_at=NOW,reason_code='FIXTURE_REVOKE').proposed_record
        row=encode_record(later,recorded_at=NOW);groups['PERMISSION_HISTORY'].append(row);groups['PERMISSION_HEAD']=[head_row(row)]
        ctrl=groups['CONTROL_HISTORY'][-1]
        next_control=PublicationControl(ctrl['revision']+1,ctrl['control_sha256'],'b'*64,ctrl['changed_by'],NOW)
        groups['CONTROL_HISTORY'].append(next_control.row());groups['CONTROL_HEAD']=[next_control.row()]
        self.assertEqual(self.decode(groups).state,'VERIFIED')

    def test_absent_audit_unknown_and_duplicate_audit_denied(self):
        groups=deepcopy(self.groups);groups['AUDIT']=[]
        self.assertEqual(self.decode(groups).state,'UNKNOWN')
        groups['AUDIT']=self.groups['AUDIT']*2
        with self.assertRaises(ContractError):self.decode(groups)

    def test_conflicting_audit_missing_revision_partial_state_and_changed_head_deny(self):
        for change in ('audit','permission','control','head'):
            groups=deepcopy(self.groups)
            if change=='audit':groups['AUDIT'][0]['reason_code']='OTHER'
            if change=='permission':groups['PERMISSION_HISTORY']=[]
            if change=='control':groups['CONTROL_HISTORY']=groups['CONTROL_HISTORY'][:-1]
            if change=='head':groups['PERMISSION_HEAD'][0]['record_sha256']='0'*64
            with self.assertRaises(ContractError):self.decode(groups)

    def test_bad_shape_and_overflow_deny(self):
        for rows in (({'capture_kind':'OTHER','record_json':'{}'},),wire(self.groups)*LIMIT):
            with self.assertRaises(ContractError):decode_receipt(rows,self.intent,read_job_id='fixture',checked_at=NOW)

    def test_snapshot_sql_is_read_only_scoped_and_parameterized(self):
        sql,cfg=query_spec(self.intent.plan())
        self.assertNotIn('INSERT',sql);self.assertNotIn('UPDATE',sql)
        self.assertEqual(sql.count('UNION ALL'),4)
        self.assertNotIn(draft().claim_id,sql)
        self.assertFalse(cfg.use_query_cache)

    def test_adapter_uses_current_grant_without_historical_review_or_evidence(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        adapter=IapIdentityAdapter(IapSettings(AUDIENCE));adapter.authenticate=Mock(return_value=identity())
        executor=PublicationExecutor(client,adapter,execution_enabled=True,clock=lambda:NOW)
        with patch('src.application.campaign_publication_executor.read_capture',return_value=decode()) as auth, \
             patch('src.application.campaign_publication_receipt.read_receipt',return_value=self.decode()) as read:
            self.assertEqual(executor.recover_receipt({},self.intent).state,'VERIFIED')
            self.assertEqual(auth.call_count,2);read.assert_called_once()
            for call in auth.call_args_list:
                request=call.args[1];self.assertIsNone(request.review);self.assertEqual(request.evidence,())
        client.query.assert_not_called();client.get_job.assert_not_called()

    def test_current_revocation_withholds_historical_receipt(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        adapter=IapIdentityAdapter(IapSettings(AUDIENCE));adapter.authenticate=Mock(return_value=identity())
        executor=PublicationExecutor(client,adapter,execution_enabled=True,clock=lambda:NOW)
        with patch('src.application.campaign_publication_executor.read_capture',side_effect=[decode(),ContractError('PUBLICATION_REVIEWER_DENIED')]), \
             patch('src.application.campaign_publication_receipt.read_receipt',return_value=self.decode()):
            with self.assertRaises(ContractError):executor.recover_receipt({},self.intent)

    def test_reader_exact_job_correlation_and_zero_mutations(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        job=Mock(state='DONE',error_result=None);job.result.return_value=wire(self.groups)
        client.query.return_value=job;client.get_job.return_value=job
        with patch('src.application.campaign_publication_receipt.inspect_reviews',return_value={'status':'VERIFIED'}), \
             patch('src.application.campaign_publication_receipt.inspect_permissions',return_value={'status':'VERIFIED'}), \
             patch('src.application.campaign_publication_receipt.matches_sharing_job',return_value=True):
            self.assertEqual(read_receipt(client,self.intent,clock=lambda:NOW).state,'VERIFIED')
        client.query.assert_called_once();self.assertNotIn('INSERT',client.query.call_args.args[0])
        self.assertIsNone(client.query.call_args.kwargs['job_retry'])

    def test_receipt_reader_rejects_wrong_original_native_job(self):
        client=Mock(project=PROJECT,location=LOCATION,default_query_job_config=None)
        job=Mock(state='DONE',error_result=None);job.result.return_value=wire(self.groups)
        client.query.return_value=job;client.get_job.return_value=job
        with patch('src.application.campaign_publication_receipt.inspect_reviews',return_value={'status':'VERIFIED'}), \
             patch('src.application.campaign_publication_receipt.inspect_permissions',return_value={'status':'VERIFIED'}), \
             patch('src.application.campaign_publication_receipt.matches_sharing_job',return_value=False):
            with self.assertRaises(ContractError):read_receipt(client,self.intent,clock=lambda:NOW)

    def test_unknown_before_creation_requires_complete_control(self):
        groups=deepcopy(self.groups);groups['AUDIT']=[]
        groups['PERMISSION_HEAD']=[];groups['PERMISSION_HISTORY']=[]
        self.assertEqual(self.decode(groups).state,'UNKNOWN')
        groups['CONTROL_HEAD']=[]
        with self.assertRaises(ContractError):self.decode(groups)
