"""An explicitly supplied catalog is structural input, never source authority."""
from copy import deepcopy
from unittest import TestCase
from src.application.workflow_contract import validate_request, request_fingerprint, ContractError
from tests.test_workflow_contract import request, CONVERSATION


class QualityCatalogContractTests(TestCase):
    def test_new_catalog_validates_exact_identity_without_remapping_or_changing_legacy_default(self):
        row=request();row['canonical_sku']='CI-SKU-0021'
        row['submitted_preferences']['preferences']['canonical_sku']='CI-SKU-0021'
        catalog=frozenset({'CI-SKU-0021'})
        row['request_sha256']=request_fingerprint(row,catalog=catalog)
        original=deepcopy(row)
        validate_request(row,conversation=CONVERSATION,catalog=catalog)
        self.assertEqual(row,original)
        with self.assertRaises(ContractError):validate_request(row,conversation=CONVERSATION)

    def test_catalog_is_not_part_of_input_and_legacy_fingerprints_remain_identical(self):
        row=request()
        self.assertEqual(request_fingerprint(row),request_fingerprint(row,catalog=frozenset({'CI-SKU-0001'})))
        validate_request(row,conversation=CONVERSATION)

    def test_missing_invalid_mutable_or_changed_identity_catalog_denies(self):
        for catalog in (frozenset(),set({'CI-SKU-0001'}),frozenset({'other'}),frozenset({'CI-SKU-0021'})):
            with self.assertRaises(ContractError):
                validate_request(request(),conversation=CONVERSATION,catalog=catalog)

    def test_extended_invocation_parent_validation_is_opt_in_only(self):
        from tests.test_invocation_contract import invocation
        from src.application.workflow_contract import validate_model_invocation
        row,context=invocation('SUCCEEDED')
        r=context['request'];r['canonical_sku']='CI-SKU-0021'
        r['submitted_preferences']['preferences']['canonical_sku']='CI-SKU-0021'
        catalog=frozenset({'CI-SKU-0021'})
        r['request_sha256']=request_fingerprint(r,catalog=catalog)
        context['job'].update(canonical_sku=r['canonical_sku'],request_sha256=r['request_sha256'])
        validate_model_invocation(row,**context,catalog=catalog)
        with self.assertRaises(ContractError):validate_model_invocation(row,**context)
