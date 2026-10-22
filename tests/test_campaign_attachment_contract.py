import json
from unittest import TestCase
from src.application.campaign_evidence_gaps import GapPlan, canonical
from src.application.campaign_attachment_contract import attachment_requests, collection_choice,ReferenceCollectionRead


def plan(kind='SCIENTIFIC_CLAIM_PERMISSION', decision='CLARIFY', alternative='FACTUAL_PRODUCT_COPY'):
    return GapPlan(canonical(dict(decision=decision, generation_authorized=False,policy_sha256='f'*64,
        scope=dict(workspace_id='workspace', conversation_id='conversation', request_id='request',
            sku='CI-SKU-0001', country='US', channel='META', request_sha256='a'*64, intent_sha256='b'*64),
        questions=[dict(requirement_id='science', evidence_kind=kind, owner='BRAND_PRODUCT',
            fields=['study_report_reference'], alternative=alternative)])))


class AttachmentContractTests(TestCase):
    def test_voluntary_reference_is_not_a_missing_evidence_or_omission_claim(self):
        data=json.loads(plan().canonical_json)
        reference=ReferenceCollectionRead(canonical(dict(version='campaign-reference-collection-v1',scope=data['scope'],
            policy_sha256='f'*64,generation_authorized=False,checked_at='2026-10-17T00:00:00+00:00')))
        request,=attachment_requests(reference);value=json.loads(request.canonical_json)
        self.assertEqual(value['collection_mode'],'VOLUNTARY_REFERENCE')
        self.assertEqual(value['missing_fields'],[])
        self.assertEqual(len(value['categories']),4)
        with self.assertRaises(ValueError):collection_choice(request,'OMIT_UNSUPPORTED_CONTENT')
    def test_relevant_categories_missing_fields_and_exact_scope(self):
        request, = attachment_requests(plan())
        data=json.loads(request.canonical_json)
        self.assertEqual(data['categories'], ['REFERENCE_DATA','PLAYBOOK','SUPPORTING_DOCUMENT'])
        self.assertEqual(data['missing_fields'], ['study_report_reference'])
        self.assertEqual(data['scope']['sku'], 'CI-SKU-0001')
        self.assertEqual(data['submission_status'], 'UNVERIFIED')
        self.assertFalse(data['generation_authorized'])

    def test_image_playbook_and_unknown_evidence_kinds(self):
        for kind, category in [('PRODUCT_IMAGE','PRODUCT_IMAGE'), ('BRAND_PLAYBOOK','PLAYBOOK'),
                               ('VISUAL_REFERENCE','REFERENCE_DATA'), ('FUTURE_KIND','SUPPORTING_DOCUMENT')]:
            request,=attachment_requests(plan(kind))
            self.assertFalse(collection_choice(request,'PROVIDE_EVIDENCE',category=category)['generation_authorized'])

    def test_omission_is_explicit_request_change_not_approval(self):
        request,=attachment_requests(plan())
        choice=collection_choice(request,'OMIT_UNSUPPORTED_CONTENT')
        self.assertEqual(choice['alternative'],'FACTUAL_PRODUCT_COPY')
        self.assertEqual(choice['status'],'REQUEST_CHANGE_PENDING')
        self.assertFalse(choice['generation_authorized'])
        request,=attachment_requests(plan(alternative=''))
        with self.assertRaises(ValueError): collection_choice(request,'OMIT_UNSUPPORTED_CONTENT')

    def test_no_questions_for_current_evidence_outages_or_review_holds(self):
        for decision in ('NO_EVIDENCE_GAPS','RETRY_RETRIEVAL','NEEDS_REVIEW'):
            self.assertEqual(attachment_requests(plan(decision=decision)),())

    def test_wrong_category_or_implicit_omission_denied(self):
        request,=attachment_requests(plan())
        for choice,category in [('PROVIDE_EVIDENCE','PRODUCT_IMAGE'), ('OMIT_UNSUPPORTED_CONTENT','PLAYBOOK'), ('PASS',None)]:
            with self.assertRaises(ValueError): collection_choice(request,choice,category=category)
