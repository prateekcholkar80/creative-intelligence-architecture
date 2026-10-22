from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
import json
from tests import test_interpreter_submission as fixtures

class IntakeOptionsTests(TestCase):
    def test_options_come_from_exact_registry_schema_without_dispatch(self):
        fixture=fixtures.InterpreterSubmissionTests();fixture.setUp();service=fixture.instance();self.addCleanup(service.client.close)
        profile=service.profile;at=fixture.inputs['observed_at']
        snapshot=SimpleNamespace(task=SimpleNamespace(value='INTERPRETER'),output_schema_version=profile['output_schema_version'],
            model_id=profile['model_id'],output_schema_json=json.dumps(dict(properties=dict(audience=dict(type='STRING',enum=['NEW','RETURNING'])))),snapshot_sha256='a'*64)
        head=SimpleNamespace(snapshot=snapshot);registry=Mock();registry.read_exact.return_value=SimpleNamespace(state=SimpleNamespace(head=head),observed_at=at)
        with patch('src.application.prompt_registry_reader.BigQueryPromptRegistryReader',return_value=registry):
            options=service.intake_options({},workspace_id=fixture.rows['workspaces']['workspace_id'],conversation_id=fixture.resource.resource_id)
        self.assertEqual(options.audiences,('NEW','RETURNING'));self.assertEqual(registry.read_exact.call_count,2)
        service.io.preflight.assert_not_called()
    def test_unknown_or_free_schema_never_guesses_allowed_values(self):
        fixture=fixtures.InterpreterSubmissionTests();fixture.setUp();service=fixture.instance();self.addCleanup(service.client.close)
        profile=service.profile;snapshot=SimpleNamespace(task=SimpleNamespace(value='INTERPRETER'),output_schema_version=profile['output_schema_version'],model_id=profile['model_id'],output_schema_json='{"properties":{"audience":{"type":"STRING"}}}')
        registry=Mock();registry.read_exact.return_value=SimpleNamespace(state=SimpleNamespace(head=SimpleNamespace(snapshot=snapshot)))
        with patch('src.application.prompt_registry_reader.BigQueryPromptRegistryReader',return_value=registry):
            with self.assertRaises(ValueError):service.intake_options({},workspace_id='workspace',conversation_id='conversation')
