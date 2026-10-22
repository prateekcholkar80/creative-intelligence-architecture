from unittest import TestCase
from src.application.operation_channel import ChannelError
from unittest.mock import Mock
from src.application.campaign_operator_host import request_values
from src.application.campaign_operator_worker import execute


class EvaluationBoundaryTests(TestCase):
    def test_browser_cannot_supply_case_profile_scope_or_response(self):
        for method in ('evaluation_run','evaluation_recover'):
            self.assertEqual(request_values(method,{},{} )['parameters'],{})
            for key in ('case_id','run_id','model','workspace_id','response','profile','principal'):
                with self.subTest(method=method,key=key),self.assertRaises(ChannelError):request_values(method,{},dict.fromkeys([key],'untrusted'))
    def test_recovery_routes_to_read_only_execution_mode(self):
        evaluator=Mock();service=Mock()
        execute(service,Mock(),'evaluation_recover',{}, {},evaluation=evaluator)
        evaluator.execute.assert_called_once_with({},recover_only=True);service.execute.assert_not_called()
    def test_missing_evaluation_runtime_denies(self):
        with self.assertRaises(ChannelError):execute(Mock(),Mock(),'evaluation_run',{}, {})
