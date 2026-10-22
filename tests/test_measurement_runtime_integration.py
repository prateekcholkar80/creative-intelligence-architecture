from unittest import TestCase
from unittest.mock import patch
from tempfile import TemporaryDirectory
from pathlib import Path
from src.validation import measurement_runtime_integration as probe


class RuntimeAuthorityProbeTests(TestCase):
    def test_exact_scope_relocation_and_production_scope_denial(self):
        sql=probe.relocate(probe.AUTHORITY_SQL,probe.PREFIX+'a'*32)
        self.assertNotIn('.ci_application.',sql)
        with self.assertRaises(ValueError):probe.relocate(probe.AUTHORITY_SQL,'ci_application')
        with self.assertRaises(ValueError):probe.relocate('SELECT * FROM `'+probe.PROJECT+'.ci_application.other`',probe.PREFIX+'a'*32)

    def test_existing_report_cannot_repeat_cloud_work(self):
        with TemporaryDirectory() as d:
            output=Path(d)/'evidence.json';output.write_text('{}')
            with patch.object(probe.bigquery,'Client') as client,self.assertRaisesRegex(ValueError,'REPORT_EXISTS'):probe.run(output)
            client.assert_not_called()
