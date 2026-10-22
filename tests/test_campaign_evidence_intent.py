from dataclasses import replace
from datetime import timedelta
import hashlib,json
from pathlib import Path
from unittest import TestCase
from src.application.campaign_evidence_intent import bind_evidence_intent,VERSION
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_acceptance_contract import _hash
from tests.test_campaign_permission_contract import NOW

class EvidenceIntentTests(TestCase):
    def setUp(self):
        self.policy=canonical(json.loads(Path('config/clarification/evidence_requirements_v1.json').read_text()))
        self.value=dict(schema_version=VERSION,features=['OFFER'],formula_version=None,audience='NEW')
        self.raw=canonical(self.value)
        request=dict(operation='INTERPRET',request_status='READY',workspace_id='w',conversation_id='c',request_id='r',
            principal_id='p',owner_id='p',trace_id='t',request_sha256='a'*64)
        authority={k:request[k] for k in ('workspace_id','conversation_id','request_id','principal_id')}
        authority.update(sku='CI-SKU-0001',country='US',channel='META',formula_version=None,audience='NEW')
        self.call=dict(request,state='SUCCEEDED',profile=dict(output_schema_version=VERSION,model_id='fixture'),stage='INTERPRETER',
            invocation_id='inv',revision=3,dispatch_receipt_id='receipt',worker_id='worker',returned_model_id='fixture',
            finish_reason='STOP',safety_outcome='PASSED',completed_at=NOW,response_sha256=hashlib.sha256(self.raw.encode()).hexdigest())
        binding={k:request[k] for k in ('workspace_id','conversation_id','request_id','principal_id')}
        binding.update(invocation_id='inv',prompt_key='campaign-evidence-intent',stage='INTERPRETER',
            binding_canonical_json=canonical(dict(profile=self.call['profile'],snapshot=dict(
                prompt_key='campaign-evidence-intent',output_schema_version=VERSION))))
        self.content=dict(authority=authority,request_record=request,calls=[self.call],bindings=[binding])
    def capture(self):
        c=json.loads(json.dumps(self.content,default=lambda v:v.isoformat()))
        return ExecutionAuthorityRead(canonical(c),_hash(c),'native-read',NOW,NOW+timedelta(seconds=60))
    def bind(self,raw=None):return bind_evidence_intent(self.capture(),raw or self.raw,self.policy,checked_at=NOW)
    def test_durable_raw_hash_and_scope_are_bound(self):
        r=self.bind();self.assertEqual(r.features,('OFFER',))
        self.assertEqual(json.loads(r.scope_json)['intent_sha256'],self.call['response_sha256'])
    def test_changed_response_legacy_schema_or_failed_outcome_denies(self):
        with self.assertRaises(ValueError):self.bind(self.raw+' ')
        for changes in (dict(state='FAILED'),dict(profile=dict(output_schema_version='legacy',model_id='fixture'))):
            previous=dict(self.call);self.call.update(changes)
            with self.assertRaises(ValueError):self.bind()
            self.call.clear();self.call.update(previous)
    def test_invented_features_and_selection_conflicts_deny_even_with_matching_hash(self):
        for values in (self.value|dict(features=['invented']),self.value|dict(audience='RETURNING'),
            self.value|dict(formula_version='invented'),self.value|dict(features=['OFFER','OFFER'])):
            raw=canonical(values);self.call['response_sha256']=hashlib.sha256(raw.encode()).hexdigest()
            with self.assertRaises(ValueError):self.bind(raw)
    def test_stale_or_supplied_authority_digest_denies(self):
        for c in (replace(self.capture(),valid_until=NOW),replace(self.capture(),content_sha256='b'*64)):
            with self.assertRaises(ValueError):bind_evidence_intent(c,self.raw,self.policy,checked_at=NOW)

    def test_wrong_or_missing_registered_intent_binding_denies(self):
        self.content['bindings'][0]['prompt_key']='other'
        with self.assertRaises(ValueError):self.bind()
        self.content['bindings']=[]
        with self.assertRaises(ValueError):self.bind()
