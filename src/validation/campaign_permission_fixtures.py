"""Synthetic P6 native seeds; no actual brand evidence or approval."""
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from src.application.campaign_acceptance_contract import PolicyStatus
from src.application.campaign_permission_contract import Approval,EvidencePin,EvidenceKind,ClaimPermission,ClaimKind,OfferPermission
from src.application.campaign_permission_reader import PermissionRequest,Checkpoint
from src.application.campaign_permission_storage import encode_record,head_row,PREFIX,TABLE_SCHEMAS

CASES=(('valid_claim',None),('valid_offer',None),('valid_evidence',None),
 ('missing_head','PERMISSION_READ_INCOMPLETE'),('missing_history','PERMISSION_READ_INCOMPLETE'),
 ('duplicate_head','PERMISSION_READ_PARTIAL_OR_DUPLICATE'),
 ('duplicate_history','PERMISSION_HISTORY_GAP_OR_DUPLICATE'),
 ('corrupt_hash','PERMISSION_ROW_HASH_OR_PROJECTION_MISMATCH'),
 ('expired','PERMISSION_NOT_CURRENT'),('draft','PERMISSION_NOT_CURRENT'),
 ('revoked_claim','PERMISSION_NOT_CURRENT'),('revoked_evidence','PERMISSION_NOT_CURRENT'),
 ('changed_evidence','PERMISSION_EVIDENCE_HEAD_CHANGED'),('missing_evidence','PERMISSION_EVIDENCE_HEAD_MISSING'),
 ('checkpoint','PERMISSION_HEAD_OR_CHECKPOINT_MISMATCH'),('broken_parent','PERMISSION_PARENT_CONFLICT'),
 ('history_gap','PERMISSION_HISTORY_GAP_OR_DUPLICATE'),('oversized','INVALID_PERMISSION_ROW_TYPE'))


def fixtures(now):
    rows={name:[] for name in TABLE_SCHEMAS};contexts={}
    a=Approval('fixture-v1',1,None,PolicyStatus.APPROVED,'synthetic-manager',
               now-timedelta(days=2),now-timedelta(days=1),now+timedelta(days=1))
    for case,denial in CASES:
        e=EvidencePin(case+'-evidence','CI-SKU-0001','synthetic-formula-v1',
            EvidenceKind.FINISHED_PRODUCT_CLINICAL,'a'*64,a,
            endpoint='Appearance of fine lines',sample_size=33,duration_days=28,
            usage_conditions='Synthetic protocol',population='Synthetic adult study cohort',
            statistic=Decimal('97'),statistic_meaning='PARTICIPANTS_PERCENT')
        c=ClaimPermission(case+'-claim','CI-SKU-0001','synthetic-formula-v1',ClaimKind.CLINICAL_RESULT,
            a,(e,),('Synthetic participant result; not approved brand copy.',),('Synthetic study; results vary.',),
            ('CA',),('META',),('NEW','RETURNING'))
        o=OfferPermission(case+'-offer',a,('CA',),('KLAVIYO_EMAIL',),('RETURNING',),('CI-SKU-0001',),
            'CAD',Decimal('250'),Decimal('25'),('CI-SKU-0002',),'FIXTURE',False,False,False,
            'STANDARD',('Synthetic expired-independent offer terms.',))
        kind='OFFER' if case=='valid_offer' else 'EVIDENCE' if case=='valid_evidence' else 'CLAIM'
        target={'CLAIM':c,'OFFER':o,'EVIDENCE':e}[kind];history=None
        if case=='expired':target=replace(c,approval=replace(a,valid_until=now-timedelta(seconds=1)))
        if case=='draft':target=replace(c,approval=replace(a,status=PolicyStatus.DRAFT,approved_by=None,approved_at=None))
        if case in {'revoked_claim','broken_parent','history_gap'}:
            next_a=replace(a,revision=2,parent_sha256='0'*64 if case=='broken_parent' else c.sha256,
                           status=PolicyStatus.REVOKED if case=='revoked_claim' else PolicyStatus.APPROVED)
            target=replace(c,approval=next_a)
            history=[encode_record(c,recorded_at=now),encode_record(target,recorded_at=now)]
            if case=='history_gap':history=history[1:]
        def append(record,record_kind,values=None):
            prefix=PREFIX[record_kind]
            past=values or [encode_record(record,recorded_at=now)]
            head=head_row(past[-1])
            rows[prefix+'_versions'].extend(past);rows[prefix+'_heads'].append(head)
            return head,past
        head,past=append(target,kind,history)
        if kind=='CLAIM' and case!='missing_evidence':
            if case=='revoked_evidence':
                changed=replace(e,review=replace(a,revision=2,parent_sha256=e.sha256,status=PolicyStatus.REVOKED))
                append(changed,'EVIDENCE',[encode_record(e,recorded_at=now),encode_record(changed,recorded_at=now)])
            elif case=='changed_evidence':append(replace(e,content_sha256='b'*64),'EVIDENCE')
            else:append(e,'EVIDENCE')
        prefix=PREFIX[kind]
        if case=='missing_head':rows[prefix+'_heads'].remove(head)
        if case=='missing_history':rows[prefix+'_versions'].remove(past[-1])
        if case=='duplicate_head':rows[prefix+'_heads'].append(dict(head))
        if case=='duplicate_history':rows[prefix+'_versions'].append(dict(past[-1]))
        if case=='corrupt_hash':past[-1]['payload_sha256']='0'*64
        if case=='oversized':past[-1]['payload_canonical_json']='x'*262145
        record_id=getattr(target,{'CLAIM':'claim_id','OFFER':'offer_id','EVIDENCE':'evidence_id'}[kind])
        request=PermissionRequest(kind,Checkpoint(record_id,2 if case=='checkpoint' else 1))
        contexts[case]=dict(request=request,expected_record=target,expected_denial=denial)
    # Outside every requested ID, for exact preservation checks.
    unrelated=replace(o,offer_id='unrelated-fixture-offer')
    append(unrelated,'OFFER')
    return rows,contexts
