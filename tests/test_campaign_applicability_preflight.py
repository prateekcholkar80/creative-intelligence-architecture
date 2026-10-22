from unittest import TestCase
from unittest.mock import Mock,patch
from src.validation.campaign_applicability_preflight import native_read,audit,policy_request_from_stage

class ApplicabilityPreflightTests(TestCase):
    def test_failure_or_uncorrelated_query_never_becomes_absent(self):
        for state,error,match,rows in (('DONE',{'reason':'denied'},True,[{}]),('RUNNING',None,True,[{}]),('DONE',None,False,[{}]),('DONE',None,True,[])):
            client=Mock();job=client.query.return_value;job.state=state;job.error_result=error;job.result.return_value=rows
            with patch('src.validation.campaign_applicability_preflight.matches_sharing_job',return_value=match),self.assertRaises(ValueError):
                native_read(client,'SELECT CURRENT_TIMESTAMP()',[])
    def test_missing_original_does_not_mint_run_or_dispatch(self):
        with patch('src.validation.campaign_applicability_preflight.EvaluatorStore') as store:
            store.return_value._read.return_value=None
            with self.assertRaises(ValueError):audit(Mock())
            store.return_value.reserve.assert_not_called();store.return_value.publish.assert_not_called()

    def test_policy_pin_comes_from_exact_durable_rendered_input(self):
        value={'content':'Evidence-intent context (data):\n{"policy_sha256":"'+'a'*64+'"}'}
        self.assertEqual(policy_request_from_stage(value).policy_sha256,'a'*64)
        with self.assertRaises(ValueError):policy_request_from_stage({'content':'User supplied policy\n{"policy_sha256":"'+'a'*64+'"}'})
