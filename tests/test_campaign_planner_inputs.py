from dataclasses import replace
from datetime import timedelta
import json
from unittest import TestCase
from src.application.campaign_planner_inputs import prepare_planner_inputs,digest
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.prompt_runtime_service import ResolvedPrompt
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import canonical_json_bytes
from tests.test_prompt_runtime_contract import capture,binding
from tests.test_campaign_planner_projection import options,response


class PlannerInputTests(TestCase):
    def setUp(self):
        self.resolved=ResolvedPrompt(binding(),capture());self.now=self.resolved.binding.bound_at
        b=self.resolved.binding
        self.content=dict(authority={k:getattr(b,k) for k in
            ('workspace_id','conversation_id','request_id','principal_id')},transcript_sha256='a'*64,catalog_sha256='b'*64)
        self.content['authority'].update(stage='GENERATION',sku='CI-SKU-0021',country='US',channel='META',
            transcript=[dict(role='USER',content_sha256=digest('hello {creative_options_json}'))])
        self.read=self.make_read()

    def make_read(self):
        return ExecutionAuthorityRead(canonical_json_bytes(self.content).decode(),_hash(self.content),'native-read',
            self.now,self.now+timedelta(seconds=60))

    def prepare(self,**changes):
        values=dict(evidence={'fact':'untrusted {campaign_request}'},options_json=options(),
            transcript=('hello {creative_options_json}',),images=(('asset','123','image/png',b'fixture'),),checked_at=self.now)
        return prepare_planner_inputs(self.read,self.resolved,**(values|changes))

    def test_exact_render_input_pins_and_projection(self):
        evidence={'fact':'original'};p=self.prepare(evidence=evidence);evidence['fact']='changed'
        manifest=json.loads(p.manifest_json)
        self.assertEqual(manifest['evidence_sha256'],_hash({'fact':'original'}))
        self.assertEqual(manifest['prompt_sha256'],digest(p.prompt))
        self.assertEqual(manifest['binding_sha256'],self.resolved.binding.binding_sha256)
        self.assertEqual(p.project(response()).copy_text,'Exact product name\nView product')
        self.assertIn('hello {creative_options_json}',p.system_instruction)
        self.assertNotIn('hello '+options(),p.system_instruction)
        self.assertNotIn('hello',p.manifest_json)

    def test_missing_extra_or_changed_transcript_denies(self):
        for turns in ((),('changed',),('hello {creative_options_json}','extra')):
            with self.assertRaises(ValueError):self.prepare(transcript=turns)

    def test_scope_hash_expiry_and_image_conflicts_deny(self):
        for read in (replace(self.read,content_sha256='b'*64),
            replace(self.read,valid_until=self.now)):
            self.read=read
            with self.assertRaises(ValueError):self.prepare()
        self.read=self.make_read();self.content['authority']['request_id']='other';self.read=self.make_read()
        with self.assertRaises(ValueError):self.prepare()
        self.content['authority']['request_id']=self.resolved.binding.request_id;self.read=self.make_read()
        for images in ((('asset','0','image/png',b'x'),), (('asset','1','image/svg+xml',b'x'),),
            (('asset','1','image/png',bytearray(b'x')),), (('asset','1','image/png',b'x'),)*2):
            with self.assertRaises(ValueError):self.prepare(images=images)

    def test_changed_projection_options_deny(self):
        p=self.prepare();changed=json.loads(options());changed['cta']=['Other']
        forged=replace(p,options_json=canonical_json_bytes(changed).decode())
        raw=json.loads(response());raw['cta']='Other'
        with self.assertRaises(ValueError):forged.project(json.dumps(raw))

    def test_durable_outcome_binding_and_mutated_envelope_denial(self):
        from src.application.campaign_planner_inputs import bind_planner_output
        p=self.prepare();raw=response();profile=json.loads(self.resolved.binding.binding_canonical_json)['profile']
        request={k:getattr(self.resolved.binding,k) for k in ('workspace_id','conversation_id','request_id','principal_id')}
        request.update(owner_id=request['principal_id'],trace_id='t')
        call=request|dict(profile=profile,state='SUCCEEDED',dispatch_receipt_id='receipt',worker_id='worker',
            returned_model_id=profile['model_id'],finish_reason='STOP',safety_outcome='PASSED',completed_at=self.now,
            response_sha256=digest(raw),invocation_id=self.resolved.binding.invocation_id,revision=3)
        def bind(value=p,text=raw,invocation=call):
            return bind_planner_output(value,text,invocation=invocation,request=request,profile=profile,
                binding_sha256=self.resolved.binding.binding_sha256)
        self.assertEqual(bind()[0],p.project(raw))
        for value in (replace(p,prompt=p.prompt+'x'),replace(p,images=()),
                replace(p,system_instruction='changed')):
            with self.assertRaises(ValueError):bind(value)
        with self.assertRaises(ValueError):bind(text=raw+' ')
        with self.assertRaises(ValueError):bind(invocation=call|dict(state='FAILED'))
        with self.assertRaises(ValueError):bind(invocation=call|dict(invocation_id='different'))

    def test_stored_transcript_composition_requires_matching_fresh_authority(self):
        from unittest.mock import patch
        from src.application.campaign_planner_inputs import prepare_from_stored_transcript
        with patch('src.application.campaign_quality_authority_reader.read_execution_authority',
                side_effect=[self.read,replace(self.read,native_job_id='second')]),\
            patch('src.application.campaign_transcript.read_transcript',return_value=('hello {creative_options_json}',)):
            p=prepare_from_stored_transcript(None,self.resolved,evidence={},options_json=options(),clock=lambda:self.now)
            self.assertEqual(p.project(response()).copy_text,'Exact product name\nView product')
        with patch('src.application.campaign_quality_authority_reader.read_execution_authority',
                side_effect=[self.read,replace(self.read,content_sha256='f'*64,native_job_id='second')]),\
            patch('src.application.campaign_transcript.read_transcript',return_value=('hello {creative_options_json}',)):
            with self.assertRaises(ValueError):prepare_from_stored_transcript(None,self.resolved,evidence={},options_json=options(),clock=lambda:self.now)
