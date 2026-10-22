"""Publication plans use synthetic observations; no measured model-quality claims."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from src.application import campaign_quality_publication as publication
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
from src.application.campaign_permission_publication import Rows
from src.application.campaign_review_storage import PublicationControl,row_json
from src.application.workflow_contract import canonical_json_bytes
from tests.test_campaign_quality_qualification import qualification_fixture
from tests.test_campaign_permission_lifecycle import identity,AUDIENCE


def publication_fixture():
    benchmark,report,args=qualification_fixture();now=args['observed_at'];actor=identity()
    actor=replace(actor,issued_at=int(now.timestamp())-1,expires_at=int(now.timestamp())+299)
    report['recorded_by']=actor.principal.principal_id
    from src.application.campaign_acceptance_contract import _hash
    pins=replace(args['pins'],evaluation_sha256=_hash(report))
    control=PublicationControl(1,None,'0'*64,actor.principal.principal_id,now-timedelta(seconds=5))
    capability=OperatorPublisherManifest('explicit-measurement-fixture',AUDIENCE,(actor.principal.principal_id,),
        now-timedelta(seconds=1),now+timedelta(seconds=299))
    return dict(identity=actor,manifest=publication.MeasurementPublisherManifest(capability,pins.workspace_id,
        args['requirements'].review_sha256,args['requirements'].evaluator_sha256),
        control=Rows('CONTROL','campaign-permissions',row_json(control.row()),(row_json(control.row()),)),
        capture=dict(benchmark_json=canonical_json_bytes(benchmark).decode(),report_json=canonical_json_bytes(report).decode(),
            existing_benchmarks=(),invocations=args['invocations'],requests=args['requests'],
            bindings=tuple(canonical_json_bytes(dict(v,bound_at=v['bound_at'].isoformat())).decode() for v in args['bindings'])),
        pins=pins,requirements=args['requirements'],checked_at=now)


class MeasurementPublicationTests(TestCase):
    def test_atomic_fenced_bounded_disabled_plan(self):
        args=publication_fixture();plan=publication.build_publication(**args)
        self.assertFalse(plan.execution_enabled)
        for name in (publication.CONTROL,publication.HISTORY,publication.AUDIT,publication.REPORTS,publication.BENCHMARKS):
            self.assertIn(name,plan.sql)
        self.assertIn('MEASUREMENT_INVOCATIONS_CHANGED',plan.sql)
        self.assertIn('MEASUREMENT_BINDINGS_CHANGED',plan.sql)
        self.assertIn('MEASUREMENT_REQUESTS_CHANGED',plan.sql)
        self.assertIn('ROLLBACK TRANSACTION',plan.sql)
        self.assertEqual(plan.config().maximum_bytes_billed,768*1024*1024)

    def test_capability_scope_recorder_and_expiry_are_not_inferred(self):
        args=publication_fixture()
        for changes in (dict(manifest=None),dict(manifest=replace(args['manifest'],workspace_id='other')),
            dict(checked_at=args['checked_at']+timedelta(seconds=300))):
            with self.subTest(changes=changes),self.assertRaises(ValueError):publication.build_publication(**(args|changes))
        bad=dict(args['capture'],existing_benchmarks=('wrong',))
        with self.assertRaisesRegex(ValueError,'BENCHMARK_CONFLICT'):publication.build_publication(**(args|dict(capture=bad)))

    def test_changed_sources_and_unmeasured_results_deny_before_plan(self):
        args=publication_fixture()
        for group in ('invocations','bindings','requests'):
            with self.subTest(group=group),self.assertRaises(ValueError):
                publication.build_publication(**(args|dict(capture=args['capture']|{group:()})))

    def test_same_target_retains_native_slot_when_capture_or_actor_window_changes(self):
        args=publication_fixture();first=publication.build_publication(**args)
        later=publication.build_publication(**(args|dict(checked_at=args['checked_at']+timedelta(seconds=1))))
        self.assertEqual(first.native_job_id,later.native_job_id)
        self.assertNotEqual(first.command_sha256,later.command_sha256)

    def test_no_empty_array_parameters_can_break_native_job_correlation(self):
        plan=publication.build_publication(**publication_fixture())
        params=plan.config().to_api_repr()['query']['queryParameters']
        for parameter in params:
            if parameter['parameterType']['type']=='ARRAY':
                self.assertTrue(parameter['parameterValue']['arrayValues'])
        self.assertIn('@existing_benchmark_count',plan.sql)
