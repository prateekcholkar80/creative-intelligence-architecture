import hashlib
import io
import json
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from google.api_core.exceptions import PreconditionFailed
from PIL import Image, ImageDraw, ImageFont

from src.generators.campaign_assets_generator import (
    BUCKET, ROOT, SCHEMA, asset_merge_sql, asset_row, load_metadata,
    render_banner, upload_asset, wrap_text,
)


class CampaignAssetTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'config/campaign_assets_generator.json').read_text())
        name = 'A source product name with a very long description and 60ml Duo'
        self.campaign = dict(
            campaign_id='SYN-CMP-20250101-CI-SKU-0001-01', sku='CI-SKU-0001',
            source_system='SYNTHETIC_POC', objective='AWARENESS', product_name=name,
            headline=name, body_copy=f'Explore {name}. View product details before choosing.',
            call_to_action='View product',
            product_image_uri=f'gs://{BUCKET}/products/CI-SKU-0001/primary.jpg',
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        image = Image.new('RGB', (200, 400), 'red')
        ImageDraw.Draw(image).rectangle((0, 200, 199, 399), fill='blue')
        buffer = io.BytesIO()
        image.save(buffer, 'PNG')
        self.original = buffer.getvalue()

    def test_deterministic_render_and_complete_source_image(self):
        png = render_banner(self.campaign, self.original, self.config)
        self.assertEqual(png, render_banner(self.campaign, self.original, self.config))
        image = Image.open(io.BytesIO(png))
        self.assertEqual(image.size, (1200, 628))
        # 200x400 image is proportionally contained in 440x480 -> 240x480.
        self.assertEqual(image.getpixel((927, 100)), (255, 0, 0))
        self.assertEqual(image.getpixel((927, 530)), (0, 0, 255))
        self.assertEqual(image.getpixel((790, 100)), (255, 255, 255))

    def test_wrapping_keeps_all_characters(self):
        text = 'Long source name ' + 'X' * 100
        draw = ImageDraw.Draw(Image.new('RGB', (100, 100)))
        font = ImageFont.truetype(self.config['font_path'], 20)
        lines = wrap_text(text, draw, font, 150)
        self.assertEqual(''.join(lines).replace(' ', ''), text.replace(' ', ''))
        self.assertTrue(all(draw.textlength(line, font=font) <= 150 for line in lines))

    def test_grounding_rejection(self):
        for changes in (dict(headline='Invented claim'), dict(body_copy='Removes wrinkles'),
                        dict(product_image_uri='gs://other/image.jpg'),
                        dict(source_system='BEAUTYFEEDS')):
            with self.assertRaises(ValueError):
                render_banner(dict(self.campaign, **changes), self.original, self.config)

    def test_metadata_and_content_addressing(self):
        png = render_banner(self.campaign, self.original, self.config)
        row = asset_row(self.campaign, png, self.config)
        self.assertEqual(set(row), {f.name for f in SCHEMA})
        self.assertEqual(row['source_system'], 'SYNTHETIC_POC')
        self.assertIn(hashlib.sha256(png).hexdigest(), row['gcs_uri'])
        self.assertEqual(row['source_image_uri'], self.campaign['product_image_uri'])
        self.assertEqual(row['description_method'], 'TEMPLATE_METADATA')

    def test_gcs_retries_preserve_existing_content(self):
        png = render_banner(self.campaign, self.original, self.config)
        row = asset_row(self.campaign, png, self.config)
        bucket = MagicMock()
        blob = bucket.blob.return_value
        blob.upload_from_string.side_effect = PreconditionFailed('exists')
        blob.download_as_bytes.return_value = png
        upload_asset(bucket, row, png)
        self.assertEqual(blob.upload_from_string.call_args.kwargs['if_generation_match'], 0)
        blob.download_as_bytes.return_value = b'other object'
        with self.assertRaises(ValueError):
            upload_asset(bucket, row, png)

    def test_scoped_merge_and_cleanup(self):
        row = asset_row(self.campaign, b'png', self.config)
        sql = asset_merge_sql('project.dataset.stage', list(row))
        self.assertIn('T.source_system=S.source_system', sql)
        self.assertNotIn('DELETE', sql)
        self.assertNotIn('T.created_at=', sql)
        client = MagicMock()
        client.get_table.return_value.schema = SCHEMA
        client.load_table_from_dataframe.side_effect = RuntimeError('failed')
        with self.assertRaises(RuntimeError):
            load_metadata(client, [row])
        self.assertEqual(client.load_table_from_dataframe.call_args.kwargs['job_config'].schema, SCHEMA)
        client.delete_table.assert_called_once()


if __name__ == '__main__':
    unittest.main()
