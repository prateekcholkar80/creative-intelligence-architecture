"""Synthetic native publication seeds, not authenticated business approvals."""
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from src.application.identity import VerifiedIdentity,principal_for_subject,ISSUER,IapSettings
from src.application.campaign_permission_lifecycle import ReviewerGrant,ReviewCapture,reviewed_content_sha256,Action
from src.application.campaign_permission_contract import Approval,EvidencePin,EvidenceKind,ClaimPermission,ClaimKind
from src.application.campaign_acceptance_contract import PolicyStatus
from src.application.campaign_review_storage import SCHEMAS,PublicationControl,ReviewVersion,encode_review,review_head
from src.application.campaign_permission_storage import TABLE_SCHEMAS,encode_record,head_row,PREFIX
from src.application.campaign_publication_reader import PublicationReadRequest
from src.application.campaign_permission_reader import Checkpoint

AUDIENCE='/projects/1042059373096/locations/asia-south1/services/ci-review-fixture'
CASES=('create','replace','approve','revoke','stale','grant_revoked','review_revoked','evidence_changed',
       'rollback','postcommit','lost','race')


def actor(now):
    subject='publication-synthetic-manager'
    return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,
        int(now.timestamp())-5,int(now.timestamp())+600)


def seed(now):
    tables={name:[] for name in dict(TABLE_SCHEMAS)|dict(SCHEMAS)};contexts={}
    identity=actor(now)
    control=PublicationControl(1,None,'a'*64,'synthetic-bootstrap',now)
    tables['campaign_permission_controls'].append(control.row())
    tables['campaign_permission_control_history'].append(control.row())
    for case in CASES:
        grant=ReviewerGrant('fixture-grant-'+case,'v1',AUDIENCE,(identity.principal.principal_id,),
            ('CLAIM','EVIDENCE','OFFER'),now-timedelta(hours=1),now+timedelta(days=1))
        gv=ReviewVersion(grant,1,None,'ACTIVE','synthetic-operator',now)
        grow=encode_review(gv)
        tables['campaign_reviewer_grants_versions'].append(grow)
        tables['campaign_reviewer_grants_heads'].append(review_head(grow))
        approval=Approval('fixture-v1',1,None,PolicyStatus.DRAFT,None,None,
            now+timedelta(hours=1),now+timedelta(days=1))
        support=Approval('synthetic-study-v1',1,None,PolicyStatus.APPROVED,'synthetic-scientific-reviewer',
            now-timedelta(days=2),now-timedelta(days=1),now+timedelta(days=1))
        evidence=EvidencePin('fixture-evidence-'+case,'CI-SKU-0001','synthetic-formula-v1',
            EvidenceKind.FINISHED_PRODUCT_CLINICAL,'c'*64,support,endpoint='Appearance of fine lines',
            sample_size=33,duration_days=28,usage_conditions='Synthetic instructions',population='Synthetic adults',
            statistic=Decimal('97'),statistic_meaning='PARTICIPANTS_PERCENT')
        candidate=ClaimPermission('fixture-claim-'+case,'CI-SKU-0001','synthetic-formula-v1',
            ClaimKind.CLINICAL_RESULT,approval,(evidence,),('Synthetic clinical copy, not approved brand wording.',),
            ('Synthetic study; results vary.',),('CA',),('META',),('NEW','RETURNING'))
        current=None
        action=Action.CREATE_DRAFT
        if case=='replace':action=Action.REPLACE_DRAFT;current=candidate
        if case in {'approve','review_revoked','evidence_changed'}:action=Action.APPROVE;current=candidate
        if case=='revoke':
            action=Action.REVOKE
            current=replace(candidate,approval=replace(support,version='fixture-v1'))
        if current:
            h=encode_record(current,recorded_at=now)
            tables[PREFIX['CLAIM']+'_versions'].append(h);tables[PREFIX['CLAIM']+'_heads'].append(head_row(h))
        review=None;rv=None
        if action is Action.APPROVE:
            e=encode_record(evidence,recorded_at=now)
            tables[PREFIX['EVIDENCE']+'_versions'].append(e);tables[PREFIX['EVIDENCE']+'_heads'].append(head_row(e))
            review=ReviewCapture('fixture-review-'+case,grant.sha256,identity.principal.principal_id,
                'CLAIM',candidate.claim_id,reviewed_content_sha256(candidate),'e'*64,
                now,now+timedelta(days=1))
            rv=ReviewVersion(review,1,None,'ACTIVE',identity.principal.principal_id,now)
            r=encode_review(rv);tables['campaign_review_captures_versions'].append(r)
            tables['campaign_review_captures_heads'].append(review_head(r))
        request=PublicationReadRequest(action,'CLAIM',candidate.claim_id,Checkpoint(grant.grant_id),
            Checkpoint(review.review_id) if review else None,
            evidence=(Checkpoint(evidence.evidence_id),) if review else ())
        contexts[case]=dict(candidate=None if action is Action.REVOKE else candidate,request=request,
            grant=gv,review=rv,evidence=evidence)
    # Complete unrelated rows are left outside every publication record ID.
    extra=replace(contexts['replace']['candidate'],claim_id='unrelated-fixture-claim')
    r=encode_record(extra,recorded_at=now)
    tables[PREFIX['CLAIM']+'_versions'].append(r);tables[PREFIX['CLAIM']+'_heads'].append(head_row(r))
    return tables,contexts
