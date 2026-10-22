import json
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path

from src.retrieval.request import CANONICAL_SKUS, RetrievalRequest


class RetrievalRequestTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1]
        self.payload = json.loads((root/'config/retrieval_request.example.json').read_text())

    def test_example_round_trip_and_visual_scope(self):
        request = RetrievalRequest.from_dict(self.payload)
        self.assertEqual(request.to_dict(), self.payload)
        self.assertEqual(request.visual_reference_scope, 'VISUAL_INSPIRATION_ONLY')
        self.assertEqual(RetrievalRequest.from_dict(json.loads(json.dumps(request.to_dict()))), request)

    def test_canonical_range_and_reject_remapping(self):
        for sku in CANONICAL_SKUS:
            self.assertEqual(RetrievalRequest.from_dict({**self.payload, 'canonical_sku': sku}).canonical_sku, sku)
        for sku in ('CI-SKU-0000', 'CI-SKU-0021', 'ci-sku-0001', 'CI-SKU-1', ' CI-SKU-0001', 'OTHER-1'):
            with self.subTest(sku=sku), self.assertRaises(ValueError):
                RetrievalRequest.from_dict({**self.payload, 'canonical_sku': sku})

    def test_preferences_use_existing_campaign_vocabulary(self):
        for objective in ('AWARENESS', 'CONSIDERATION', 'CONVERSION'):
            for channel in ('PAID_SOCIAL', 'PAID_SEARCH', 'EMAIL'):
                request = RetrievalRequest.from_dict({**self.payload, 'objective': objective, 'channel': channel})
                self.assertEqual((request.objective, request.channel), (objective, channel))
        for field, value in (('objective', 'UNKNOWN'), ('channel', 'UNKNOWN'), ('visual_reference', 'OTHER_BRAND')):
            with self.subTest(field=field), self.assertRaises(ValueError):
                RetrievalRequest.from_dict({**self.payload, field: value})

    def test_no_visual_reference_is_explicit(self):
        request = RetrievalRequest.from_dict({**self.payload, 'visual_reference': 'NONE'})
        self.assertIsNone(request.visual_reference_scope)
        self.assertEqual(request.canonical_sku, self.payload['canonical_sku'])

    def test_missing_unknown_and_evidence_fields_rejected(self):
        for field in self.payload:
            with self.subTest(missing=field), self.assertRaisesRegex(ValueError, 'Missing request fields'):
                RetrievalRequest.from_dict({k:v for k,v in self.payload.items() if k != field})
        for field in ('sku', 'brand', 'approved_claims', 'source_system', 'approval_scope'):
            with self.subTest(extra=field), self.assertRaisesRegex(ValueError, 'Unsupported request fields'):
                RetrievalRequest.from_dict({**self.payload, field: 'User supplied evidence'})

    def test_types_blank_values_and_direct_construction(self):
        for field in self.payload:
            for value in (None, 1, True, [], {}, '', '   '):
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    RetrievalRequest.from_dict({**self.payload, field: value})
        for payload in (None, [], 'request'):
            with self.assertRaises(ValueError):
                RetrievalRequest.from_dict(payload)
        with self.assertRaises(ValueError):
            RetrievalRequest(**{**self.payload, 'canonical_sku': 'UNRELATED'})

    def test_trimmed_audience_and_immutable_validated_request(self):
        request = RetrievalRequest.from_dict({**self.payload, 'audience': '  Skincare shoppers  '})
        self.assertEqual(request.audience, 'Skincare shoppers')
        with self.assertRaises(FrozenInstanceError):
            request.canonical_sku = 'UNRELATED'
        output = request.to_dict()
        output['canonical_sku'] = 'UNRELATED'
        self.assertEqual(request.canonical_sku, 'CI-SKU-0001')


if __name__ == '__main__':
    unittest.main()
