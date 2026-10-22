"""Structured evidence collection, separate from model questions and approval."""
from dataclasses import dataclass
import json

from src.application.campaign_evidence_gaps import GapPlan, canonical
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import _require, _digest

VERSION = 'campaign-attachment-request-v1'
CATEGORIES = ('PRODUCT_IMAGE', 'REFERENCE_DATA', 'PLAYBOOK', 'SUPPORTING_DOCUMENT')
# UI capabilities, not claim policy or evidence eligibility. Unknown kinds remain
# representable; the current authoritative gap determines what is requested.
COLLECTION_TYPES = {
    'CURRENT_LABEL': ('PRODUCT_IMAGE', 'SUPPORTING_DOCUMENT'),
    'SCIENTIFIC_CLAIM_PERMISSION': ('REFERENCE_DATA', 'PLAYBOOK', 'SUPPORTING_DOCUMENT'),
    'CURRENT_OFFER': ('REFERENCE_DATA', 'SUPPORTING_DOCUMENT'),
    'CURRENT_PRICE': ('REFERENCE_DATA', 'SUPPORTING_DOCUMENT'),
    'CURRENT_FULFILMENT': ('REFERENCE_DATA', 'SUPPORTING_DOCUMENT'),
    'CURRENT_SHIPPING': ('REFERENCE_DATA', 'SUPPORTING_DOCUMENT'),
    'PRODUCT_IMAGE': ('PRODUCT_IMAGE',),
    'VISUAL_REFERENCE': ('PRODUCT_IMAGE', 'REFERENCE_DATA', 'PLAYBOOK'),
    'BRAND_PLAYBOOK': ('PLAYBOOK', 'SUPPORTING_DOCUMENT'),
}


@dataclass(frozen=True, slots=True)
class AttachmentRequest:
    canonical_json: str

    @property
    def sha256(self):
        data=json.loads(self.canonical_json)
        # Fresh native reads change plan provenance, not the user's collection
        # intent. Scope, policy, missing fields, categories and choices stay bound.
        data.pop('plan_sha256')
        return _hash(data)


@dataclass(frozen=True,slots=True)
class ReferenceCollectionRead:
    """Explicit optional user references, not a model-detected evidence gap."""
    canonical_json: str

    @property
    def sha256(self):return _hash(json.loads(self.canonical_json))


def attachment_requests(plan):
    """Server-owned plan only. This projection does not authorize question release."""
    _require(type(plan) in (GapPlan,ReferenceCollectionRead), 'ATTACHMENT_PLAN_REQUIRED')
    data = json.loads(plan.canonical_json)
    _require(data['generation_authorized'] is False, 'ATTACHMENT_PLAN_INVALID')
    _digest(data['policy_sha256'])
    if type(plan) is ReferenceCollectionRead:
        _require(data['version']=='campaign-reference-collection-v1','ATTACHMENT_REFERENCE_VERSION')
        return (AttachmentRequest(canonical(dict(version=VERSION,plan_sha256=plan.sha256,
            policy_sha256=data['policy_sha256'],scope=data['scope'],collection_mode='VOLUNTARY_REFERENCE',
            requirement_id='user-supporting-material',evidence_kind='USER_SUBMITTED_REFERENCE',
            missing_fields=[],owner='USER',categories=list(CATEGORIES),choices=['PROVIDE_EVIDENCE'],
            alternative='',submission_status='UNVERIFIED',generation_authorized=False))),)
    if data['decision'] != 'CLARIFY':
        return ()  # Never invite a replacement for an outage or rejected approval.
    return tuple(AttachmentRequest(canonical(dict(
        version=VERSION, plan_sha256=plan.sha256, policy_sha256=data['policy_sha256'], scope=data['scope'],
        requirement_id=g['requirement_id'], evidence_kind=g['evidence_kind'],
        missing_fields=g['fields'], owner=g['owner'],
        categories=list(COLLECTION_TYPES.get(g['evidence_kind'], CATEGORIES)),
        choices=['PROVIDE_EVIDENCE'] + (['OMIT_UNSUPPORTED_CONTENT'] if g['alternative'] else []),
        alternative=g['alternative'], submission_status='UNVERIFIED',
        generation_authorized=False))) for g in data['questions'])


def collection_choice(request, choice, *, category=None):
    _require(type(request) is AttachmentRequest, 'ATTACHMENT_REQUEST_REQUIRED')
    data = json.loads(request.canonical_json)
    _require(choice in data['choices'], 'ATTACHMENT_CHOICE_INVALID')
    _require((choice == 'PROVIDE_EVIDENCE' and category in data['categories'])
        or (choice == 'OMIT_UNSUPPORTED_CONTENT' and category is None
            and bool(data['alternative'])), 'ATTACHMENT_CATEGORY_INVALID')
    return dict(request_sha256=request.sha256, choice=choice, category=category,
        alternative=data['alternative'] if choice == 'OMIT_UNSUPPORTED_CONTENT' else None,
        status='UNVERIFIED' if category else 'REQUEST_CHANGE_PENDING',
        generation_authorized=False)
