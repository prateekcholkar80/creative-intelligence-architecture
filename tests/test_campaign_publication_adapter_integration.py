"""Isolated native adapter probe guard tests; no cloud calls."""
from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock
from src.validation.campaign_publication_adapter_integration import TrackedClient,CASES,validate_report
from src.validation.campaign_publication_integration import Scope


def complete_report():
    cases=[dict(case=name,status='PASSED',mutation_submissions=0,snapshot_unchanged=True) for name in CASES]
    by_case={c['case']:c for c in cases}
    by_case['postcommit_receipt'].update(result_state=None,receipt_state='VERIFIED')
    by_case['rollback_no_receipt'].update(receipt_state='UNKNOWN')
    by_case['lost_response_recovery'].update(discarded_response=True)
    run_id='a'*32
    jobs=[dict(native_job_id=f'publication_it_{run_id}_adapter_{i:064x}',
        status='COMMITTED' if i<4 else 'NOT_COMMITTED',initial_adapter_status='UNKNOWN',
        recovered_adapter_status='COMMITTED' if i<4 else 'NOT_COMMITTED',
        result_state='APPLIED' if i<3 else None,error=None if i<3 else
            dict(message='FIXTURE_PUBLICATION_POSTCOMMIT' if i==3 else 'FIXTURE_PUBLICATION_ROLLBACK')) for i in range(5)]
    return dict(status='FAILED',run_id=run_id,cases=cases,cleanup_verified=True,errors=[],serving_writes=0,model_calls=0,
        mutation_submissions=5,jobs=jobs,submitted_job_ids=[j['native_job_id'] for j in jobs],
        read_job_ids=['campaign_publication_read_fixture','campaign_publication_receipt_fixture'])


class AdapterProbeTests(TestCase):
    def setUp(self):
        self.scope=Scope('a'*32);self.client=Mock();self.report={}
        self.tracked=TrackedClient(self.client,self.scope,self.report)
        self.sql='SELECT * FROM `'+self.scope.table('campaign_permission_controls')+'`'

    def test_owned_scope_and_namespace_enforced_before_native_call(self):
        for sql,native in [('SELECT * FROM `other.ds.table`','campaign_publication_read_fixture'),
                           (self.sql,'publication_it_'+'b'*32+'_fixture'),(self.sql,'unexpected')]:
            with self.assertRaises(ValueError):self.tracked.query(sql,job_id=native,retry=None,job_retry=None)
        self.client.query.assert_not_called()

    def test_single_insert_lost_response_is_observed_not_replayed(self):
        native='publication_it_'+self.scope.run_id+'_fixture'
        self.tracked.lose_response=True
        with self.assertRaises(RuntimeError):self.tracked.query(self.sql,job_id=native,retry=None,job_retry=None)
        self.client.query.assert_called_once();self.assertFalse(self.tracked.lose_response)
        self.assertEqual(self.report['mutation_submissions'],1)
        self.assertEqual(self.report['submitted_job_ids'],[native])
        self.client.query.return_value.result.assert_called_once()

    def test_read_query_tracking_and_retry_guard(self):
        self.tracked.query(self.sql,job_id='campaign_publication_receipt_fixture',retry=None,job_retry=None)
        self.assertEqual(self.report['read_job_ids'],['campaign_publication_receipt_fixture'])
        self.assertEqual(self.tracked.publications,[])
        with self.assertRaises(ValueError):self.tracked.query(self.sql,job_id='campaign_publication_read_fixture',retry=True,job_retry=None)

    def test_report_requires_cases_cleanup_commits_and_preservation(self):
        report=complete_report();validate_report(report)
        for field,value in [('cleanup_verified',False),('serving_writes',1),('mutation_submissions',6),('errors',['failure']),('submitted_job_ids',[]),('read_job_ids',[])]:
            changed=deepcopy(report);changed[field]=value
            with self.assertRaises(ValueError):validate_report(changed)
        for mutation in ('unknown','missing','duplicate','changed','false_receipt','fault_code','recovery_mismatch'):
            changed=deepcopy(report)
            if mutation=='unknown':changed['jobs'][0]['status']='UNKNOWN'
            if mutation=='missing':changed['cases'].pop()
            if mutation=='duplicate':changed['jobs'][1]['native_job_id']=changed['jobs'][0]['native_job_id']
            if mutation=='changed':changed['cases'][1]['snapshot_unchanged']=False
            if mutation=='fault_code':changed['jobs'][3]['error']['message']='OTHER_FAILURE'
            if mutation=='recovery_mismatch':changed['jobs'][0]['recovered_adapter_status']='UNKNOWN'
            if mutation=='false_receipt':next(c for c in changed['cases'] if c['case']=='postcommit_receipt')['result_state']='APPLIED'
            with self.assertRaises(ValueError):validate_report(changed)

    def test_revoke_fixture_is_seeded_as_approval_then_review_is_removed(self):
        from datetime import datetime,timezone
        from dataclasses import replace
        from src.validation.campaign_publication_fixtures import seed
        from src.application.campaign_permission_lifecycle import Action
        from src.application.campaign_publication_reader import PublicationReadRequest
        _,contexts=seed(datetime.now(timezone.utc))
        request=contexts['approve']['request']
        self.assertEqual(request.action,Action.APPROVE)
        revoke=replace(request,action=Action.REVOKE,review=None,evidence=())
        self.assertIsInstance(revoke,PublicationReadRequest)
        self.assertIsNone(revoke.review);self.assertEqual(revoke.evidence,())
