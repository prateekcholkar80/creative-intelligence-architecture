from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import create_autospec,Mock
from google.cloud import bigquery
from src.validation.operator_policy_publication_integration import relocate,NAMES,run,fixture_identity,load_fixture_control,PROJECT,settle_original
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from tests.test_campaign_review_publication_writer import control_rows
from src.application.campaign_review_storage import parse_row
from tests.test_campaign_permission_lifecycle import NOW
from tests.test_campaign_operator_policy_publication import plan


class OperatorProbeTests(TestCase):
    def test_fixture_load_uses_actual_sdk_signature_and_no_upload_retry(self):
        client=create_autospec(bigquery.Client,instance=True)
        row=parse_row(control_rows().head_json)
        load_fixture_control(client,PROJECT+'.ci_operator_policy_it_fixture.campaign_permission_controls',row)
        self.assertEqual(client.load_table_from_json.call_args.kwargs['num_retries'],0)
        self.assertNotIn('retry',client.load_table_from_json.call_args.kwargs)
        client.load_table_from_json.return_value.result.assert_called_once_with(timeout=45,retry=None)
        with self.assertRaises(ValueError):load_fixture_control(client,PROJECT+'.ci_brand_memory.campaign_permission_controls',row)

    def test_fixture_identity_respects_bounded_verifier_contract(self):
        value=fixture_identity(NOW)
        self.assertEqual(value.expires_at-value.issued_at,600)
        self.assertIn('fixture',value.subject)

    def test_relocation_is_complete_and_keeps_exact_configuration(self):
        original=plan();mapped=relocate(original,'ci_operator_policy_it_fixture')
        self.assertNotIn('.ci_brand_memory.',mapped.sql)
        for name in NAMES:self.assertIn('.ci_operator_policy_it_fixture.'+name,mapped.sql)
        self.assertEqual(mapped.config_json,original.config_json)
        self.assertNotEqual(mapped.job_id,original.native_job_id)
        self.assertTrue(mapped.job_id.startswith('ci_operator_fixture_'))
        self.assertNotIn('DELETE ',mapped.sql)

    def test_fixture_slots_are_dataset_owned_and_stable_within_revision(self):
        original=plan()
        self.assertEqual(relocate(original,'ci_operator_policy_it_a').job_id,relocate(original,'ci_operator_policy_it_a').job_id)
        self.assertNotEqual(relocate(original,'ci_operator_policy_it_a').job_id,relocate(original,'ci_operator_policy_it_b').job_id)

    def test_unknown_recovers_only_original_with_bounded_observations(self):
        io=Mock();original=relocate(plan(),'ci_operator_policy_it_fixture')
        io.recover.side_effect=[ControlOutcome(original.job_id,Status.UNKNOWN),ControlOutcome(original.job_id,Status.NOT_COMMITTED)]
        result,observations=settle_original(io,original,ControlOutcome(original.job_id,Status.UNKNOWN))
        self.assertIs(result.native_status,Status.NOT_COMMITTED)
        self.assertEqual(observations,['UNKNOWN','UNKNOWN','NOT_COMMITTED'])
        self.assertEqual(io.recover.call_count,2);io.submit_once.assert_not_called()

    def test_unowned_or_injected_dataset_names_are_rejected(self):
        for name in ('ci_brand_memory','other',"ci_operator_policy_it_`; DROP TABLE x",'ci_operator_policy_it_foo.bar'):
            with self.assertRaises(ValueError):relocate(plan(),name)

    def test_existing_report_prevents_native_replay(self):
        with TemporaryDirectory() as directory:
            output=Path(directory)/'report.json';output.write_text('original')
            with self.assertRaisesRegex(ValueError,'OPERATOR_FIXTURE_REPORT_EXISTS'):run(output)
            self.assertEqual(output.read_text(),'original')
