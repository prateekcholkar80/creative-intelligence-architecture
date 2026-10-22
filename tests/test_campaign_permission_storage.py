"""Pure permission storage fixtures; no real approvals or cloud calls."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import json
import unittest
from src.application.campaign_permission_storage import (
    TABLE_SCHEMAS, table_metadata, encode_record, decode_record, head_row,
    decode_state, reconcile_claim_evidence, MAX_PAYLOAD_BYTES,
)
from src.application.campaign_acceptance_contract import PolicyStatus
from src.application.workflow_contract import ContractError, canonical_json_bytes
from src.ingestion.configure_campaign_acceptance import digest, PROJECT
from tests.test_campaign_permission_contract import claim,offer,evidence,NOW


def state(record,kind):
    row=encode_record(record,recorded_at=NOW)
    key={'CLAIM':'claim_id','OFFER':'offer_id','EVIDENCE':'evidence_id'}[kind]
    return decode_state(head_row(row),(row,),kind=kind,record_id=getattr(record,key))


def chain(record,kind,status=PolicyStatus.REVOKED):
    field='review' if kind=='EVIDENCE' else 'approval'
    old=getattr(record,field)
    new=replace(record,**{field:replace(old,revision=2,parent_sha256=record.sha256,status=status)})
    return (encode_record(record,recorded_at=NOW),encode_record(new,recorded_at=NOW+timedelta(seconds=1)))


class PermissionStorageTests(unittest.TestCase):
    def test_all_six_proposed_schemas_exact_metadata_without_cloud(self):
        self.assertEqual(len(TABLE_SCHEMAS),6)
        for name in TABLE_SCHEMAS:
            first=table_metadata(name,PROJECT);second=table_metadata(name,PROJECT)
            self.assertEqual(first.dataset_id,'ci_brand_memory')
            self.assertEqual(first.clustering_fields,['record_id','revision'])
            self.assertIsNone(first.expires)
            self.assertIsNot(first,second)
            self.assertEqual(len(first.schema),11 if name.endswith('_versions') else 10)

    def test_claim_offer_evidence_roundtrip_preserve_decimal_and_identity(self):
        for kind,record in [('CLAIM',claim()),('OFFER',offer()),('EVIDENCE',evidence())]:
            row=encode_record(record,recorded_at=NOW)
            decoded=decode_record(row,kind=kind,record_id=row['record_id'])
            self.assertEqual(decoded,record);self.assertEqual(decoded.sha256,record.sha256)
            self.assertEqual(state(record,kind).require_current(NOW),record)
        self.assertIsInstance(state(offer(),'OFFER').current.threshold,Decimal)

    def test_projection_hash_shape_scope_and_bool_revision_denied(self):
        row=encode_record(claim(),recorded_at=NOW)
        for changes in ({'record_sha256':'0'*64},{'payload_sha256':'0'*64},
                        {'record_id':'other'},{'record_kind':'OFFER'},{'revision':True},
                        {'status':'DRAFT'},{'extra':'field'},{'recorded_at':NOW.replace(tzinfo=None)}):
            with self.assertRaises(ContractError):
                decode_record(row|changes,kind='CLAIM',record_id=claim().claim_id)

    def test_json_canonical_shape_types_and_size_denied_even_when_rehashed(self):
        row=encode_record(offer(),recorded_at=NOW)
        original=json.loads(row['payload_canonical_json'])
        altered=[]
        for transform in (lambda p:p['record'].update(threshold=250.0),
                          lambda p:p['record'].update(automatic=0),
                          lambda p:p['record']['approval'].update(status='UNKNOWN'),
                          lambda p:p['record'].update(countries='CA'),
                          lambda p:p['record'].update(extra='field')):
            payload=deepcopy(original);transform(payload);altered.append(canonical_json_bytes(payload).decode())
        altered.extend([row['payload_canonical_json']+' ','null','[1]',
                        '{"x":1,"x":2}', '"'+'x'*MAX_PAYLOAD_BYTES+'"'])
        for text in altered:
            with self.assertRaises(ContractError):decode_record(row|dict(payload_canonical_json=text,
                payload_sha256=digest(text.encode())),kind='OFFER',record_id=offer().offer_id)

    def test_complete_revocation_chain_denies_current_use_and_old_head(self):
        for kind,record in [('CLAIM',claim()),('OFFER',offer()),('EVIDENCE',evidence())]:
            history=chain(record,kind)
            decoded=decode_state(head_row(history[-1]),history,kind=kind,record_id=history[0]['record_id'])
            with self.assertRaises(ContractError):decoded.require_current(NOW+timedelta(seconds=2))
            with self.assertRaises(ContractError):decode_state(head_row(history[0]),history,
                kind=kind,record_id=history[0]['record_id'])

    def test_missing_gap_duplicate_reordered_or_parent_conflict_denied(self):
        history=chain(claim(),'CLAIM')
        bad_parent=replace(claim(),approval=replace(claim().approval,revision=2,parent_sha256='0'*64))
        bad_history=(history[0],encode_record(bad_parent,recorded_at=NOW))
        for rows in ((),history[1:],history[::-1],history+history[-1:],bad_history):
            with self.assertRaises(ContractError):decode_state(head_row(history[-1]),rows,
                kind='CLAIM',record_id=claim().claim_id)

    def test_revocation_is_terminal_cannot_resurrect_same_identity(self):
        rows=chain(claim(),'CLAIM');revoked=decode_record(rows[-1],kind='CLAIM',record_id=claim().claim_id)
        restored=replace(revoked,approval=replace(revoked.approval,revision=3,
                        parent_sha256=revoked.sha256,status=PolicyStatus.APPROVED))
        last=encode_record(restored,recorded_at=NOW+timedelta(seconds=2))
        with self.assertRaises(ContractError):decode_state(head_row(last),rows+(last,),
            kind='CLAIM',record_id=claim().claim_id)

    def test_identity_change_and_history_time_rollback_denied(self):
        first=encode_record(claim(),recorded_at=NOW)
        changed=replace(claim(),sku='CI-SKU-0002',evidence=(replace(evidence(),sku='CI-SKU-0002'),),
            approval=replace(claim().approval,revision=2,parent_sha256=claim().sha256))
        last=encode_record(changed,recorded_at=NOW)
        with self.assertRaises(ContractError):decode_state(head_row(last),(first,last),kind='CLAIM',record_id=claim().claim_id)
        rows=chain(claim(),'CLAIM');last=rows[-1]|dict(recorded_at=NOW-timedelta(seconds=1))
        with self.assertRaises(ContractError):decode_state(head_row(last),(rows[0],last),kind='CLAIM',record_id=claim().claim_id)

    def test_checkpoint_floor_and_hash_deny_stale_or_conflicting_state(self):
        row=encode_record(claim(),recorded_at=NOW)
        for options in ({'minimum_revision':2},{'minimum_revision':True},
                        {'checkpoint_sha256':'0'*64},{'minimum_revision':65}):
            with self.assertRaises(ContractError):decode_state(head_row(row),(row,),kind='CLAIM',
                record_id=claim().claim_id,**options)
        self.assertEqual(decode_state(head_row(row),(row,),kind='CLAIM',record_id=claim().claim_id,
            checkpoint_sha256=claim().sha256).current,claim())

    def test_independent_evidence_revocation_blocks_old_embedded_pin(self):
        c=state(claim(),'CLAIM');e=state(evidence(),'EVIDENCE')
        self.assertEqual(reconcile_claim_evidence(c,(e,),checked_at=NOW),claim())
        rows=chain(evidence(),'EVIDENCE')
        revoked=decode_state(head_row(rows[-1]),rows,kind='EVIDENCE',record_id=evidence().evidence_id)
        with self.assertRaises(ContractError):reconcile_claim_evidence(c,(revoked,),checked_at=NOW+timedelta(seconds=2))

    def test_missing_extra_or_changed_current_evidence_denied(self):
        c=state(claim(),'CLAIM');e=state(evidence(),'EVIDENCE')
        changed=state(replace(evidence(),content_sha256='b'*64),'EVIDENCE')
        for supplied in ((),(e,e),(changed,),[e]):
            with self.assertRaises(ContractError):reconcile_claim_evidence(c,supplied,checked_at=NOW)

    def test_draft_expiry_and_history_limit(self):
        c=claim();draft=replace(c,approval=replace(c.approval,status=PolicyStatus.DRAFT,
                                               approved_by=None,approved_at=None))
        with self.assertRaises(ContractError):state(draft,'CLAIM').require_current(NOW)
        with self.assertRaises(ContractError):state(c,'CLAIM').require_current(c.approval.valid_until)
        too_late=replace(c,approval=replace(c.approval,revision=65,parent_sha256='a'*64))
        with self.assertRaises(ContractError):encode_record(too_late,recorded_at=NOW)

    def test_future_record_timestamp_cannot_establish_current_permission(self):
        row=encode_record(claim(),recorded_at=NOW+timedelta(seconds=1))
        current=decode_state(head_row(row),(row,),kind='CLAIM',record_id=claim().claim_id)
        with self.assertRaises(ContractError):current.require_current(NOW)


if __name__=='__main__':unittest.main()
