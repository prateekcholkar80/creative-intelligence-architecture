import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.application.campaign_chat import Interpretation
from src.validation.model_continuity import check_case, continuity_cases, run_validation


class ModelContinuityTests(unittest.TestCase):
    def test_benchmark_distinguishes_admitted_clarification_and_claim_requests(self):
        for case in continuity_cases():
            result=Interpretation(case['expected'],case['features'])
            with self.subTest(case=case['id']):check_case(result,case)

    def test_cross_sku_interpretation_cannot_pass_a_positive_case(self):
        case=continuity_cases()[0]
        result=Interpretation({**case['expected'],'canonical_sku':'CI-SKU-0002'})
        with self.assertRaises(ValueError):check_case(result,case)

    def test_followup_that_drops_an_unsupported_claim_fails_validation(self):
        case=continuity_cases()[-1]
        result=Interpretation(case['expected'])
        with self.assertRaises(ValueError):check_case(result,case)

    def test_existing_output_directory_stops_before_billable_work(self):
        with tempfile.TemporaryDirectory() as tmp,patch('src.validation.model_continuity.CampaignChatService') as service:
            with self.assertRaises(FileExistsError):run_validation(Path(tmp))
            service.assert_not_called()


if __name__ == '__main__':
    unittest.main()
