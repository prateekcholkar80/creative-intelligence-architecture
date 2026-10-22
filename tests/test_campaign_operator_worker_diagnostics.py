from unittest import TestCase
from src.application.campaign_operator_worker import operation_failure_code,contract_failure_code
from src.application.workflow_contract import ContractError

class WorkerDiagnosticsTests(TestCase):
    def test_image_diagnostics_are_fixed_and_operation_scoped(self):
        for method in ('product_image_prepare','product_image_recover'):
            for code in ('PRODUCT_IMAGE_ORIGINAL_MISSING','PRODUCT_IMAGE_OBJECT_CHANGED','IMAGE_DERIVATION_OUTPUT_CHANGED'):
                self.assertEqual(contract_failure_code(ContractError(code),method),code)
                self.assertEqual(contract_failure_code(ContractError(code),'current'),'ACCESS_DENIED')
            self.assertEqual(contract_failure_code(ContractError('PRODUCT_IMAGE_private@example.com'),method),'ACCESS_DENIED')
    def test_embedding_codes_are_fixed_and_operation_scoped(self):
        for method in ('embedding_run','embedding_recover'):
            self.assertEqual(contract_failure_code(ContractError('CORPUS_EXTRACTION_CHANGED'),method),'CORPUS_EXTRACTION_CHANGED')
            self.assertEqual(contract_failure_code(ContractError('PRIVATE_INDEX_RECOVERY_NO_WRITE'),method),'PRIVATE_INDEX_RECOVERY_NO_WRITE')
            self.assertEqual(contract_failure_code(ContractError('private@example.com'),method),'ACCESS_DENIED')
        self.assertEqual(contract_failure_code(ContractError('CORPUS_EXTRACTION_CHANGED'),'current'),'ACCESS_DENIED')
    def test_session_refresh_code_is_exposed_only_for_planner_run(self):
        error=ContractError('PLANNER_SESSION_REFRESH_REQUIRED')
        self.assertEqual(contract_failure_code(error,'planner_run'),'PLANNER_SESSION_REFRESH_REQUIRED')
        for method in ('planner_recover','current','evaluation_run'):
            self.assertEqual(contract_failure_code(error,method),'ACCESS_DENIED')
        self.assertEqual(contract_failure_code(ContractError('secret'),'planner_run'),'ACCESS_DENIED')

    def test_messages_and_values_never_leave_worker(self):
        for kind,category in ((TypeError,'TYPE'),(AttributeError,'ATTRIBUTE'),(FileNotFoundError,'FILE'),(RuntimeError,'DEPENDENCY')):
            try:raise kind('secret-token private/path identity@example.com')
            except Exception as error:code=operation_failure_code(error)
            self.assertEqual(code,'OPERATOR_FAILURE_'+category+'_UNKNOWN')
            self.assertNotIn('secret',code)

    def test_evaluation_coverage_is_not_misreported_as_access_denial(self):
        error=ContractError('SEMANTIC_RESULT_COVERAGE')
        for method in ('evaluation_run','evaluation_recover'):
            self.assertEqual(contract_failure_code(error,method),'SEMANTIC_RESULT_COVERAGE')
        self.assertEqual(contract_failure_code(error,'current'),'ACCESS_DENIED')
        self.assertEqual(contract_failure_code(ContractError('PRIVATE_AUTHORITY_DETAIL'),'evaluation_run'),'ACCESS_DENIED')
        self.assertEqual(contract_failure_code(ValueError('secret'),'evaluation_run'),'ACCESS_DENIED')

    def test_applicability_failures_are_safe_fixed_codes_only_in_evaluation_lane(self):
        for code in ('SEMANTIC_STAGE_APPLICABILITY','SEMANTIC_SOURCE_APPLICABILITY_BINDING','INTERPRETATION_EVALUATION_SOURCE_APPLICABILITY_UNAVAILABLE'):
            self.assertEqual(contract_failure_code(ContractError(code),'evaluation_run'),code)
            self.assertEqual(contract_failure_code(ContractError(code),'evaluation_recover'),code)
            self.assertEqual(contract_failure_code(ContractError(code),'measurement_current'),'ACCESS_DENIED')

    def action_fixture(self):
        from datetime import timedelta
        from types import SimpleNamespace
        from unittest.mock import Mock
        from tests.test_campaign_measurement_runtime import authority_fixture
        from src.application.campaign_measurement_runtime import ProtectedMeasurementRuntime
        from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
        from src.application.identity import IapSettings
        from src.application.operation_budget import OperationSettings
        row,args,_=authority_fixture();now=args['ended'];actor=args['identity']
        old=OperatorPublisherManifest('explicit-old-session',actor.audience,(actor.principal.principal_id,),now-timedelta(seconds=600),now-timedelta(seconds=300))
        measurement=object.__new__(ProtectedMeasurementRuntime);measurement.settings=args['settings']
        measurement.verifier=Mock(settings=IapSettings(actor.audience));measurement.verifier.authenticate.return_value=actor
        measurement.manifest=old;measurement.clock=lambda:now
        measurement.publisher=SimpleNamespace(enabled=False);measurement.approval_publisher=SimpleNamespace(enabled=False)
        config=SimpleNamespace(measurement=measurement.settings,writes_enabled=False,policy=None,manifest=old,
            iap=IapSettings(actor.audience),operation=OperationSettings(300))
        return config,measurement,actor,now

    def test_fresh_action_survives_expired_connection_manifest_without_restart(self):
        from unittest.mock import patch
        from src.application.campaign_operator_worker import refresh_measurement_action
        config,m,actor,now=self.action_fixture();original=m.manifest
        with patch('src.application.campaign_measurement_runtime.require_owned_worker'),self.assertRaisesRegex(ValueError,'OPERATOR_PUBLISHER_DENIED'):m._actor({})
        pending={'original':'retained'};m.retained_fixture=pending;publisher=m.publisher
        with patch('src.application.campaign_operator_worker.require_owned_worker'):
            refresh_measurement_action(config,m,{'x-fixture':'assertion'})
        self.assertIsNot(m.manifest,original);self.assertEqual(m.manifest.effective_at,now)
        self.assertEqual((m.manifest.valid_until-now).total_seconds(),300)
        self.assertIs(m.retained_fixture,pending);self.assertIs(m.publisher,publisher)
        with patch('src.application.campaign_measurement_runtime.require_owned_worker'):self.assertEqual(m._actor({}),actor)

    def test_wrong_principal_audience_or_expired_assertion_cannot_renew(self):
        from dataclasses import replace
        from unittest.mock import patch
        from src.application.campaign_operator_worker import refresh_measurement_action
        from src.application.identity import principal_for_subject,ISSUER
        for fault in ('principal','audience','expired'):
            config,m,actor,now=self.action_fixture();old=m.manifest
            if fault=='principal':actor=replace(actor,principal=principal_for_subject(ISSUER,'different-subject'),subject='different-subject')
            if fault=='audience':actor=replace(actor,audience=actor.audience+'-other')
            if fault=='expired':actor=replace(actor,issued_at=int(now.timestamp())-100,expires_at=int(now.timestamp())-1)
            m.verifier.authenticate.return_value=actor
            with patch('src.application.campaign_operator_worker.require_owned_worker'),self.assertRaises(ValueError):refresh_measurement_action(config,m,{})
            self.assertIs(m.manifest,old)

    def test_write_enabled_runtime_cannot_use_read_action_refresh(self):
        from unittest.mock import patch
        from src.application.campaign_operator_worker import refresh_measurement_action
        for fault in ('host','measurement','approval'):
            config,m,_,_=self.action_fixture();old=m.manifest
            if fault=='host':config.writes_enabled=True
            if fault=='measurement':m.publisher.enabled=True
            if fault=='approval':m.approval_publisher.enabled=True
            with patch('src.application.campaign_operator_worker.require_owned_worker'),self.assertRaises(ValueError):refresh_measurement_action(config,m,{})
            self.assertIs(m.manifest,old);m.verifier.authenticate.assert_not_called()

    def test_recovery_integrity_errors_are_distinguished_without_values(self):
        for code in ('EVALUATOR_RECOVERY_INTENT_CHANGED','EVALUATOR_RECOVERY_JOB_UNVERIFIED','INTERPRETATION_EVALUATION_SOURCE_CHANGED'):
            self.assertEqual(contract_failure_code(ContractError(code),'evaluation_recover'),code)
            self.assertEqual(contract_failure_code(ContractError(code),'current'),'ACCESS_DENIED')
        self.assertEqual(contract_failure_code(ContractError('PRIVATE_RECORD_CONTENT'),'evaluation_recover'),'ACCESS_DENIED')
