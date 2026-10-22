import json,time
from dataclasses import replace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application.campaign_planner_runtime import *
from src.application.operation_budget import worker_operation_scope
from tests.test_campaign_planner_evaluation import PlannerEvaluationTests


class PlannerRuntimeTests(TestCase):
    def test_expiring_session_denies_before_source_queries_or_reservation(self):
        from datetime import datetime,timezone
        from types import SimpleNamespace
        now=datetime.now(timezone.utc)
        service=object.__new__(ProtectedPlannerEvaluation);service.settings=self.settings()
        service.measurement=Mock();service.measurement.clock.return_value=now
        service.measurement._actor.return_value=SimpleNamespace(expires_at=now.timestamp()+299)
        service.uploads=Mock();service.registry=Mock()
        with worker_operation_scope(time.monotonic()+300):
            with self.assertRaisesRegex(ValueError,'PLANNER_SESSION_REFRESH_REQUIRED'):
                service._capture({},require_session_lifetime=True)
        service.measurement.io.read.assert_not_called();service.uploads.capture.assert_not_called()
        service.registry.read_exact.assert_not_called()

    def test_recovery_keeps_fresh_source_checks_without_starting_session_budget(self):
        from datetime import datetime,timezone
        from types import SimpleNamespace
        now=datetime.now(timezone.utc)
        service=object.__new__(ProtectedPlannerEvaluation);service.settings=self.settings()
        m=Mock();m.clock.return_value=now;m._actor.return_value=SimpleNamespace(
            expires_at=now.timestamp()+299,principal=SimpleNamespace(principal_id='owner'))
        service.measurement=m;service.uploads=Mock();service.registry=Mock()
        service.uploads.capture.return_value=Mock(manifest_json=canonical(dict(context=dict(scope=dict(sku='CI-SKU-0001',country='US',channel='META')))))
        with patch('src.application.campaign_planner_runtime.verify_authority',return_value=(4,2)):
            with worker_operation_scope(time.monotonic()+300):
                with self.assertRaisesRegex(ValueError,'PLANNER_RUNTIME_CASE_SOURCE_SCOPE'):
                    service._capture({},require_session_lifetime=False)
        m._actor.assert_called_once_with({});service.registry.read_exact.assert_not_called()

    def settings(self):
        case=dict(case_id='real-image-private-pilot',stage='GENERATION',sku='CI-SKU-0022',country='US',channel='META',
            turns=[dict(role='USER',text='Prepare a private identity-only layout proposal for evaluation.')])
        return PlannerEvaluationSettings('real-planner-pilot-v1',canonical(case),sha(case),'eval_upload_binding_v1_'+'a'*64,
            'conversation','request',('b'*64,),'999','1','c'*64,True)

    def test_case_pins_reject_gold_and_changed_case(self):
        s=self.settings()
        with self.assertRaises(ValueError):replace(s,case_sha256='f'*64)
        case=json.loads(s.case_input_json);case['expected_decision']='PASS'
        with self.assertRaises(ValueError):replace(s,case_input_json=canonical(case),case_sha256=sha(case))
        with self.assertRaises(ValueError):replace(s,submission_ids=['b'*64])

    def test_identity_options_have_no_benefit_or_approved_reference(self):
        opts=json.loads(identity_options(dict(product_name='Rejuvenate Face Oil')))
        self.assertEqual(opts['headline'],['Rejuvenate Face Oil']);self.assertEqual(opts['body'],[''])
        self.assertEqual(opts['reference_campaign_id'],['NONE_NO_APPLICABLE_REFERENCE'])

    def test_guard_rebuilds_exact_input_and_denies_changed_source(self):
        # Verify the real guard rather than replacing it with a passing callback.
        p=PlannerEvaluationTests().prepared();intent=json.loads(p.intent_json)
        service=object.__new__(ProtectedPlannerEvaluation);service._headers={'iap':'private'}
        service._capture=Mock(return_value=('inputs','snapshot','pins',{'product_native_job_id':'native'}))
        service._prepare=Mock(return_value=p)
        self.assertEqual(service._guard(intent),{'product_native_job_id':'native'})
        altered=dict(intent,registry_record_digest='e'*64)
        with self.assertRaises(ValueError):service._guard(altered)
        service._headers=None
        with self.assertRaises(ValueError):service._guard(intent)

    def test_action_clears_headers_after_denial_and_recovery_is_explicit(self):
        service=object.__new__(ProtectedPlannerEvaluation);service.settings=self.settings();service._lock=RLock()
        service._capture=Mock(side_effect=ValueError('source revoked'));service.engine=Mock()
        with worker_operation_scope(time.monotonic()+30):
            with self.assertRaises(ValueError):service.execute({'iap':'private'})
        self.assertIsNone(service._headers);service.engine.execute.assert_not_called()

    def test_real_capture_rejects_wrong_saved_sku_before_binding_registry_or_provider(self):
        from datetime import datetime,timezone
        service=object.__new__(ProtectedPlannerEvaluation);service.settings=self.settings()
        m=Mock();m.clock.return_value=datetime.now(timezone.utc);service.measurement=m
        service.uploads=Mock();service.registry=Mock()
        service.uploads.capture.return_value=Mock(manifest_json=canonical(dict(context=dict(scope=dict(sku='CI-SKU-0001',country='US',channel='META')))))
        with patch('src.application.campaign_planner_runtime.verify_authority',return_value=(4,2)):
            with self.assertRaisesRegex(ValueError,'PLANNER_RUNTIME_CASE_SOURCE_SCOPE'):service._capture({})
        service.uploads.verify_binding.assert_not_called();service.registry.read_exact.assert_not_called()
