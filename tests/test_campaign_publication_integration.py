"""Publication probe ownership, fixture and report guards; no native calls."""
from copy import deepcopy
from dataclasses import replace
from unittest.mock import Mock,create_autospec
import unittest
from google.cloud import bigquery
from src.validation.campaign_publication_integration import Scope,TABLES,GROUPS,scoped_plan,expected_mutation,validate_report,append_rows
from src.validation.campaign_publication_fixtures import seed,actor
from src.application.campaign_permission_publication import build_publication
from src.application.campaign_permission_lifecycle import Action
from tests.test_campaign_review_publication import bundle
from tests.test_campaign_permission_contract import NOW


class PublicationProbeTests(unittest.TestCase):
    def test_scope_only_thirteen_owned_tables_and_no_destructive_sql(self):
        scope=Scope('a'*32);plan=build_publication(Action.APPROVE,**bundle(Action.APPROVE))
        modified=scoped_plan(plan,scope,'fixture')
        self.assertNotIn('.ci_brand_memory.',modified.sql)
        self.assertEqual(len(TABLES),13)
        self.assertEqual(modified.config_json,plan.config_json)
        for sql in ('UPDATE `other.ds.table` SET a=1','DELETE FROM `'+scope.table('campaign_permission_controls')+'` WHERE TRUE',
                    'SELECT * FROM project.dataset.table'):
            with self.assertRaises(ValueError):scope.assert_sql(sql)
        with self.assertRaises(ValueError):scope.table('unexpected')

    def test_faults_have_one_exact_marker_and_under_child_bound(self):
        scope=Scope('a'*32);plan=build_publication(Action.CREATE_DRAFT,**bundle())
        rollback=scoped_plan(plan,scope,'rollback','rollback')
        post=scoped_plan(plan,scope,'post','postcommit')
        self.assertLess(rollback.sql.index('FIXTURE_PUBLICATION_ROLLBACK'),rollback.sql.index('COMMIT TRANSACTION;'))
        self.assertGreater(post.sql.index('FIXTURE_PUBLICATION_POSTCOMMIT'),post.sql.index('COMMIT TRANSACTION;'))
        self.assertLess(rollback.sql.count(';'),64)
        with self.assertRaises(ValueError):scoped_plan(plan,scope,'bad','other')

    def test_synthetic_seeds_are_deterministic_owned_and_complete(self):
        tables,contexts=seed(NOW)
        self.assertEqual(seed(NOW)[0],tables)
        self.assertEqual(set(tables),set(TABLES));self.assertEqual(len(contexts),12)
        self.assertEqual(len(tables['campaign_permission_controls']),1)
        self.assertEqual(actor(NOW).expires_at-actor(NOW).issued_at,605)
        self.assertTrue(any(r['record_id']=='unrelated-fixture-claim' for r in tables['campaign_claim_permissions_heads']))

    def test_expected_atomic_projection_preserves_every_other_table_row(self):
        plan=build_publication(Action.CREATE_DRAFT,**bundle())
        before={name:[] for name in TABLES}
        before['campaign_claim_permissions_heads']=['{"record_id":"unrelated"}']
        before['campaign_permission_controls']=['{"control_id":"campaign-permissions"}']
        after=expected_mutation(before,plan)
        self.assertIn('{"record_id":"unrelated"}',after['campaign_claim_permissions_heads'])
        changed={name for name in TABLES if before[name]!=after[name]}
        self.assertEqual(changed,{'campaign_claim_permissions_heads','campaign_claim_permissions_versions',
            'campaign_permission_controls','campaign_permission_control_history','campaign_permission_publication_audits'})
        self.assertEqual(before['campaign_permission_controls'],['{"control_id":"campaign-permissions"}'])

    def test_append_uses_distinct_stable_call_ids_exact_schema_zero_upload_retries(self):
        scope=Scope('a'*32);client=create_autospec(bigquery.Client,instance=True)
        scope.own=Mock();rows,_=seed(NOW);name='campaign_reviewer_grants_versions'
        append_rows(client,scope,name,rows[name][:1]);append_rows(client,scope,name,rows[name][:1])
        a,b=client.load_table_from_json.call_args_list
        self.assertNotEqual(a.kwargs['job_id'],b.kwargs['job_id'])
        self.assertEqual(a.kwargs['num_retries'],0)
        self.assertEqual(a.args[1],scope.table(name))
        self.assertEqual(a.kwargs['job_config'].write_disposition,'WRITE_APPEND')

    def test_reports_reject_incomplete_cases_unknown_outcomes_and_cleanup_failure(self):
        jobs=[dict(native_job_id='fixture-'+str(i),status='NOT_COMMITTED' if i==6 else 'COMMITTED',
            receipt_state=None if i in (6,7) else 'REUSED' if i==1 else 'APPLIED',
            submission_result_discarded=i==5,error={'message':'FIXTURE_PUBLICATION_ROLLBACK' if i==6 else 'FIXTURE_PUBLICATION_POSTCOMMIT'} if i in (6,7) else None)
            for i in range(8)]
        report=dict(group='CORE',cases=[dict(case=name,status='PASSED') for name in GROUPS['CORE']],
            jobs=jobs,mutation_submissions=8,cleanup_verified=True,errors=[],serving_writes=0,model_calls=0)
        validate_report(report)
        for changes in ({'cases':report['cases'][:-1]},{'jobs':[dict(status='UNKNOWN')]},
                        {'cleanup_verified':False},{'errors':['failure']},{'serving_writes':1}):
            with self.assertRaises(ValueError):validate_report(report|changes)

    def test_null_unused_review_parameter_is_not_declared(self):
        plan=build_publication(Action.CREATE_DRAFT,**bundle())
        self.assertNotIn('review_id',[p.name for p in plan.config().query_parameters])
        for empty in ('old_head','old_history'):
            self.assertNotIn(empty,[p.name for p in plan.config().query_parameters])
            self.assertIn('CREATE TEMP TABLE '+empty+' (',plan.sql)

    def test_race_report_requires_overlap_one_winner_and_positive_loser(self):
        from datetime import timedelta
        jobs=[dict(native_job_id='winner',status='COMMITTED',receipt_state='APPLIED',error=None),
              dict(native_job_id='loser',status='NOT_COMMITTED',receipt_state=None,
                   error=dict(reason='concurrentUpdate',message='Transaction is aborted due to concurrent update'))]
        race=dict(case='overlapping_competing_genesis',status='PASSED',winner=0,jobs=['winner','loser'],
            transaction_intervals=[[NOW.isoformat(),(NOW+timedelta(seconds=2)).isoformat()],
                                  [(NOW+timedelta(seconds=1)).isoformat(),(NOW+timedelta(seconds=3)).isoformat()]])
        report=dict(group='RACE',cases=[race],jobs=jobs,mutation_submissions=2,cleanup_verified=True,
                    errors=[],serving_writes=0,model_calls=0)
        validate_report(report)
        for change in ('no_overlap','two_winners','unknown_loser'):
            candidate=deepcopy(report)
            if change=='no_overlap':candidate['cases'][0]['transaction_intervals'][1][0]=(NOW+timedelta(seconds=2)).isoformat()
            if change=='two_winners':candidate['jobs'][1].update(status='COMMITTED',receipt_state='APPLIED',error=None)
            if change=='unknown_loser':candidate['jobs'][1]['status']='UNKNOWN'
            with self.assertRaises((ValueError,RuntimeError)):validate_report(candidate)


if __name__=='__main__':unittest.main()
