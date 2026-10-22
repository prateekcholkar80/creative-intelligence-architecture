"""Offline guards for isolated native measurement verification."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch
from src.validation import quality_measurement_integration as probe
from src.validation.quality_measurement_fixtures import fixture
from src.application.campaign_quality_publication import build_publication
from src.application.campaign_quality_qualification_reader import SQL


class MeasurementProbeTests(TestCase):
    def test_complete_exact_relocation_never_targets_serving_tables(self):
        args,_=fixture();plan=build_publication(**args);dataset=probe.PREFIX+'a'*32
        for sql in (plan.sql,SQL):
            relocated=probe.relocate(sql,dataset)
            self.assertNotIn('.ci_brand_memory.',relocated)
            self.assertNotIn('.ci_application.',relocated)
            self.assertIn(dataset,relocated)
        for wrong in ('ci_brand_memory',probe.PREFIX+'bad'):
            with self.assertRaises(ValueError):probe.relocate(SQL,wrong)
        with self.assertRaises(ValueError):probe.relocate('SELECT * FROM `'+probe.PROJECT+'.ci_application.unknown`',dataset)

    def test_existing_report_refuses_cloud_work(self):
        with TemporaryDirectory() as directory:
            output=Path(directory)/'evidence.json';output.write_text('{}')
            with patch.object(probe.bigquery,'Client') as client,self.assertRaisesRegex(ValueError,'REPORT_EXISTS'):
                probe.run(output)
            client.assert_not_called()

    def test_synthetic_fixture_is_qualifiable_storage_shape_not_actual_benchmark(self):
        args,rows=fixture();plan=build_publication(**args)
        self.assertEqual(len(rows['model_invocations']),1)
        self.assertFalse(plan.execution_enabled)
        self.assertIn('SYNTHETIC_STORAGE_FIXTURE',args['capture']['report_json'])
        self.assertIn('synthetic-storage-benchmark',args['capture']['benchmark_json'])
