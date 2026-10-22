from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import hashlib,json
from unittest import TestCase
from src.application.campaign_quality_authority import (
    VERSION,validate_payload,decode_authority,validate_transcript,bind_provider,bind_permissions,table_metadata,
)
from src.application.campaign_acceptance_contract import Stage,_hash
from src.application.workflow_contract import ContractError,canonical_json_bytes
from tests.test_workflow_contract import request,TIME
from tests.test_invocation_contract import invocation
from tests.test_campaign_permission_reader import decode


def payload():
    r=request('INTERPRET')
    return dict(version=VERSION,workspace_id=r['workspace_id'],conversation_id=r['conversation_id'],
        request_id=r['request_id'],principal_id=r['principal_id'],request_sha256=r['request_sha256'],
        sku='CI-SKU-0001',country='US',channel='META',stage=Stage.INTERPRETATION.value,formula_version=None,
        audience='Skincare shoppers',transcript=[dict(request_id=r['request_id'],role='USER',
        content_sha256=r['message_sha256'],invocation_id=None)],claims=[])


class QualityAuthorityTests(TestCase):
    def test_hash_only_manifest_has_no_raw_content_and_unsupported_fields_deny(self):
        p=payload();self.assertEqual(validate_payload(p),p)
        for changes in (dict(country='FR'),dict(sku='CI-SKU-0024'),dict(channel='PAID_SOCIAL'),dict(raw_text='secret')):
            with self.assertRaises(ContractError):validate_payload(p|changes)
        self.assertEqual(len(table_metadata().schema),7)

    def test_manifest_duplicates_missing_hash_and_assistant_without_invocation_deny(self):
        p=payload()
        for turns in (p['transcript']*2,[p['transcript'][0]|dict(content_sha256='bad')],
                      [p['transcript'][0]|dict(role='ASSISTANT')]):
            with self.assertRaises(ContractError):validate_payload(p|dict(transcript=turns))

    def test_exact_manifest_matches_stored_message_hash_never_generation_fingerprint(self):
        p=payload();r=request('INTERPRET')
        self.assertEqual(len(validate_transcript(p,[r],[])),64)
        for changed in (p|dict(transcript=[]),p|dict(transcript=[p['transcript'][0]|dict(content_sha256='c'*64)])):
            with self.assertRaises(ContractError):validate_transcript(changed,[r],[])
        with self.assertRaises(ContractError):validate_transcript(p,[request()],[])
        # A first structured request has no stored chat utterances; an empty
        # manifest is explicit, never fabricated from the input fingerprint.
        self.assertEqual(len(validate_transcript(p|dict(transcript=[]),[request()],[])),64)

    def test_authority_row_selection_hash_owner_time_and_channel_are_bound(self):
        from types import SimpleNamespace
        p=payload();r=request('INTERPRET')|dict(canonical_sku=p['sku'],audience=p['audience'],channel='PAID_SOCIAL')
        row={k:p[k] for k in ('workspace_id','conversation_id','request_id','principal_id')}
        row.update(payload_canonical_json=canonical_json_bytes(p).decode(),payload_sha256=_hash(p),recorded_at=TIME)
        identity=SimpleNamespace(principal=SimpleNamespace(principal_id=r['principal_id']))
        self.assertEqual(decode_authority(row,request=r,identity=identity,observed_at=TIME),p)
        for changed in (row|dict(payload_sha256='a'*64),row|dict(principal_id='other'),
                        row|dict(recorded_at=TIME+timedelta(seconds=1))):
            with self.assertRaises(ContractError):decode_authority(changed,request=r,identity=identity,observed_at=TIME)
        # A pre-dispatch interpretation may bind explicit stored preferences;
        # free text alone never supplies an inferred SKU/audience/channel.
        from tests.test_workflow_contract import preferences
        from src.application.workflow_contract import request_fingerprint
        pending=request('INTERPRET');pending['submitted_preferences']['preferences']=preferences()
        pending['request_sha256']=request_fingerprint(pending)
        selected=p|dict(request_sha256=pending['request_sha256'])
        early=row|dict(payload_canonical_json=canonical_json_bytes(selected).decode(),payload_sha256=_hash(selected))
        self.assertEqual(decode_authority(early,request=pending,identity=identity,observed_at=TIME),selected)

    def test_provider_exact_scope_model_bytes_and_projection_required(self):
        call,context=invocation('SUCCEEDED');profile=call['profile']
        call['returned_model_id']=profile['model_id'];text='observed response'
        self.assertEqual(len(bind_provider(call,request=context['request'],profile=profile,
            response_text=text,candidate_text=text)),64)
        for change in (dict(state='UNKNOWN'),dict(returned_model_id='other'),dict(response_sha256='a'*64),
            dict(request_id='other'),dict(safety_outcome='BLOCKED'),dict(dispatch_receipt_id=None)):
            with self.assertRaises(ContractError):bind_provider(call|change,request=context['request'],
                profile=profile,response_text=text,candidate_text=text)
        with self.assertRaises(ContractError):bind_provider(call,request=context['request'],profile=profile,
            response_text=text,candidate_text='rewritten response')

    def test_claim_use_requires_current_independent_evidence_and_exact_scope(self):
        snapshot=decode();claim=snapshot.state.current
        p=payload()|dict(sku=claim.sku,formula_version=claim.formula_version,country=claim.countries[0],
            channel=claim.channels[0],audience=claim.audiences[0],claims=[dict(claim_id=claim.claim_id,
                wording=claim.wording[0],qualifications=list(claim.qualifications))])
        self.assertEqual(len(bind_permissions(p,(snapshot,),checked_at=snapshot.checked_at)),64)
        for changed in (p|dict(country='XX'),p|dict(sku='CI-SKU-0023'),p|dict(formula_version='other')):
            with self.assertRaises(ContractError):bind_permissions(changed,(snapshot,),checked_at=snapshot.checked_at)
        with self.assertRaises(ContractError):bind_permissions(p,(snapshot,),checked_at=claim.approval.valid_until)
        with self.assertRaises(ContractError):bind_permissions(p,(),checked_at=snapshot.checked_at)
        with self.assertRaises(ContractError):bind_permissions(p,(replace(snapshot,evidence_states=()),),checked_at=snapshot.checked_at)
