"""Owned native bootstrap probe guards, no cloud calls."""
from copy import deepcopy
from unittest import TestCase
from unittest.mock import Mock
from src.validation.campaign_bootstrap_integration import (
    Scope,fixture_job_id,scoped_plan,TrackedClient,GROUPS,validate_report,expected_genesis,
)
from src.application.campaign_bootstrap_query import bootstrap_spec
from src.application.campaign_bootstrap_contract import bootstrap_job_id
from src.validation.campaign_publication_integration import TABLES
from tests.test_campaign_bootstrap import manifest


def complete_report(group):
    resources=[dict(dataset='project.fixture_'+str(i)*32,cleanup_verified=True) for i in range({'CORE':1,'FAULTS':3,'DENIALS':4}[group])]
    ids=['campaign_bootstrap_it_'+r['dataset'].rsplit('_',1)[-1]+'_0' for r in resources]
    cases=[dict(case=name,status='PASSED',insert_attempts=0,snapshot_unchanged=True) for name in GROUPS[group]]
    by_case={c['case']:c for c in cases};jobs=[];attempts=[]
    if group=='CORE':
        by_case['competing_manifests'].update(insert_attempts=2,unique_native_jobs=1,loser_status='UNKNOWN',atomic_projection_verified=True)
        jobs=[dict(native_job_id=ids[0],status='COMMITTED',error=None)];attempts=ids[:1]*2
    if group=='FAULTS':
        codes=('FIXTURE_BOOTSTRAP_ROLLBACK','FIXTURE_BOOTSTRAP_POSTCOMMIT',None)
        jobs=[dict(native_job_id=ids[i],status='NOT_COMMITTED' if i==0 else 'COMMITTED',error={'message':code} if code else None) for i,code in enumerate(codes)]
        attempts=ids
        for i,name in enumerate(('rollback','postcommit','lost_response')):
            by_case[name].update(insert_attempts=1,atomic_projection_verified=True,result_state=None if i<2 else 'APPLIED')
    return dict(group=group,cases=cases,resources=resources,insert_attempts=attempts,jobs=jobs,
        read_jobs=['campaign_bootstrap_read_fixture'],errors=[],serving_writes=0,model_calls=0)


class BootstrapProbeTests(TestCase):
    def setUp(self):self.scope=Scope('a'*32)
    def test_fixture_slot_shared_by_conflicting_manifests_and_not_production_slot(self):
        from dataclasses import replace
        first=scoped_plan(bootstrap_spec(manifest()),self.scope)
        second=scoped_plan(bootstrap_spec(replace(manifest(),reason_code='CONFLICT')),self.scope)
        self.assertEqual(first.native_job_id,second.native_job_id)
        self.assertNotEqual(first.native_job_id,bootstrap_job_id())
        self.assertNotEqual(first.config_json,second.config_json)
        self.assertNotIn('.ci_brand_memory.',first.sql)
        self.assertEqual(first.config_json,bootstrap_spec(manifest()).config_json)

    def test_scope_rejects_other_tables_production_sql_and_resets(self):
        for sql in ('SELECT * FROM `other.ds.table`','DELETE FROM `'+self.scope.table('campaign_permission_controls')+'` WHERE TRUE'):
            with self.assertRaises(ValueError):self.scope.relocate(sql)
        with self.assertRaises(ValueError):self.scope.table('unexpected')

    def test_fault_markers_preserve_child_bound_and_correct_commit_side(self):
        plan=bootstrap_spec(manifest())
        rollback=scoped_plan(plan,self.scope,'rollback');post=scoped_plan(plan,self.scope,'postcommit')
        self.assertLess(rollback.sql.index('FIXTURE_BOOTSTRAP_ROLLBACK'),rollback.sql.index('COMMIT TRANSACTION;'))
        self.assertGreater(post.sql.index('FIXTURE_BOOTSTRAP_POSTCOMMIT'),post.sql.index('COMMIT TRANSACTION;'))
        self.assertLess(rollback.sql.count(';'),64)
        with self.assertRaises(ValueError):scoped_plan(plan,self.scope,'unknown')

    def test_tracked_client_forbids_serving_slot_and_nonread_read_ids(self):
        client=Mock();report=dict(insert_attempts=[],read_jobs=[]);tracked=TrackedClient(client,self.scope,report)
        plan=scoped_plan(bootstrap_spec(manifest()),self.scope)
        with self.assertRaises(ValueError):tracked.query(plan.sql,job_id=bootstrap_job_id(),retry=None,job_retry=None)
        with self.assertRaises(ValueError):tracked.get_job(bootstrap_job_id())
        with self.assertRaises(ValueError):tracked.query(plan.sql,job_id='campaign_bootstrap_read_fixture',retry=None,job_retry=None)
        client.query.assert_not_called();client.get_job.assert_not_called()

    def test_lost_response_is_one_insert_and_one_native_completion_wait(self):
        client=Mock();report=dict(insert_attempts=[],read_jobs=[]);tracked=TrackedClient(client,self.scope,report)
        tracked.lose=True;plan=scoped_plan(bootstrap_spec(manifest()),self.scope)
        with self.assertRaises(RuntimeError):tracked.query(plan.sql,job_id=plan.native_job_id,retry=None,job_retry=None)
        self.assertEqual(report['insert_attempts'],[fixture_job_id(self.scope)])
        self.assertFalse(tracked.lose);client.query.assert_called_once();client.query.return_value.result.assert_called_once()

    def test_expected_projection_has_only_control_and_history(self):
        before={name:[] for name in TABLES};after=expected_genesis(before,manifest())
        self.assertEqual({name for name in before if before[name]!=after[name]},
            {'campaign_permission_controls','campaign_permission_control_history'})
        self.assertFalse(any(before.values()))
        with self.assertRaises(ValueError):expected_genesis(after,manifest())

    def test_report_checks_cases_cleanup_submissions_native_outcomes_and_namespace(self):
        for group in GROUPS:
            report=complete_report(group);validate_report(report)
            for field,value in (('cases',[]),('errors',['failure']),('read_jobs',[]),('serving_writes',1)):
                changed=deepcopy(report);changed[field]=value
                with self.assertRaises(ValueError):validate_report(changed)
            changed=deepcopy(report);changed['resources'][0]['cleanup_verified']=False
            with self.assertRaises(ValueError):validate_report(changed)
        report=complete_report('CORE')
        for field,value in (('status','UNKNOWN'),('native_job_id',bootstrap_job_id())):
            changed=deepcopy(report);changed['jobs'][0][field]=value
            with self.assertRaises(ValueError):validate_report(changed)
        report=complete_report('FAULTS');report['jobs'][0]['error']['message']='WRONG_CODE'
        with self.assertRaises(ValueError):validate_report(report)
