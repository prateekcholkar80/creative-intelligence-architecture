"""Full-authority composition observations are synthetic, never live approvals."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import hashlib,json
from unittest import TestCase
from unittest.mock import Mock,patch

from src.application import campaign_quality_bound_execution as service
from src.application.campaign_quality_authority import VERSION
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import canonical_json_bytes
from tests import test_campaign_quality_execution as fixtures
from tests.test_prompt_runtime_contract import binding


class BoundExecutionTests(TestCase):
    def setUp(self):
        self.fixture=fixtures.QualityExecutionTests('test_fresh_rechecks_bind_every_context_and_never_release')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.r=fixtures.request();self.s=fixtures.settings();self.now=self.fixture.current.observed_at
        self.pins=binding().row();self.pins['bound_at']=self.pins['bound_at'].isoformat()
        p=dict(version=VERSION,**{k:getattr(self.r,k) for k in ('workspace_id','conversation_id','request_id','sku','country','channel')},
            principal_id=self.fixture.current.request.identity.principal.principal_id,request_sha256='a'*64,
            stage=self.r.stage.value,formula_version=None,audience='Synthetic fixture',
            transcript=[dict(request_id='prior-request',role='USER',content_sha256='e'*64,invocation_id=None)],claims=[])
        req=dict(workspace_id=self.r.workspace_id,conversation_id=self.r.conversation_id,request_id=self.r.request_id,
            owner_id=p['principal_id'],principal_id=p['principal_id'],trace_id='trace',request_sha256='a'*64,message_sha256=None)
        call=dict(workspace_id=self.r.workspace_id,conversation_id=self.r.conversation_id,request_id=self.r.request_id,
            owner_id=p['principal_id'],trace_id='trace',profile=json.loads(self.s.model_profile_json),stage='PLANNER',
            invocation_id=self.pins['invocation_id'],revision=3,state='SUCCEEDED',dispatch_receipt_id='receipt',worker_id='worker',
            returned_model_id=json.loads(self.s.model_profile_json)['model_id'],finish_reason='STOP',safety_outcome='PASSED',
            completed_at=self.now.isoformat(),started_at=self.now.isoformat(),response_sha256=hashlib.sha256(
                b'Renew Resurfacing Night Serum').hexdigest())
        self.content=dict(version='campaign-execution-authority-capture-v1',authority=p,authority_sha256=_hash(p),
            authority_recorded_at=self.now.isoformat(),
            request_record=req,request_bindings=dict(request_record_sha256=_hash(req)),transcript_sha256='b'*64,
            catalog_sha256='f'*64,calls=[call],bindings=[self.pins],jobs=[],attempts=[])
        self.counter=0
        def read(*a,**kw):
            self.counter+=1
            return self.capture(self.content,f'read-{self.counter}')
        self.reader=patch.object(service,'read_execution_authority',side_effect=read).start()
        self.addCleanup(patch.stopall)

    def capture(self,content,native_id='read'):
        return ExecutionAuthorityRead(canonical_json_bytes(content).decode(),_hash(content),native_id,
            self.now,self.now+timedelta(seconds=60))

    def run_check(self,r=None,build=None):
        def builder(fact):
            candidate=fixtures.builder(fact)
            return candidate,candidate.text
        return service.run_authoritative_quality(r or self.r,build or builder,settings=self.s,client=self.fixture.client,
            runtime_reader=self.fixture.runtime,access_reader=self.fixture.access,
            identity=self.fixture.current.request.identity,clock=lambda:self.now)

    def test_exact_selection_transcript_provider_prompt_and_permission_bindings_never_release(self):
        result=self.run_check();context=json.loads(result.context_canonical_json)
        self.assertFalse(result.release_allowed)
        self.assertEqual(context['authority_sha256'],_hash(self.content))
        self.assertEqual(context['authority_native_job_ids'],['read-1','read-2'])
        self.assertEqual(context['invocation_binding_sha256'],self.pins['binding_sha256'])
        self.assertEqual(context['claim_evidence_sha256'],_hash([]))

    def test_supplied_market_platform_stage_and_hash_drift_denies_before_annotation(self):
        for changes in (dict(country='CA'),dict(channel='PINTEREST'),dict(user_input_sha256='c'*64),dict(conversation_sha256='c'*64)):
            b=Mock()
            with self.assertRaises(ValueError):self.run_check(r=replace(self.r,**changes),build=b)
            b.assert_not_called()

    def test_changed_authoritative_context_after_checker_withholds(self):
        changed=deepcopy(self.content);changed['catalog_sha256']='c'*64
        self.reader.side_effect=[self.capture(self.content,'first'),self.capture(changed,'second')]
        with self.assertRaisesRegex(ValueError,'QUALITY_AUTHORITY_CONTEXT_CHANGED'):self.run_check()

    def test_missing_or_mismatched_durable_provider_binding_denies(self):
        original=deepcopy(self.content)
        for changes in (dict(calls=[]),dict(bindings=[])):
            self.content=original|changes
            with self.assertRaises(ValueError):self.run_check()
        self.content=deepcopy(original);self.content['bindings'][0]['principal_id']='other'
        with self.assertRaises(ValueError):self.run_check()

    def test_planned_creation_precedes_later_binding_and_completion(self):
        # Immutable started_at belongs to PLANNED creation, not provider start.
        self.content['calls'][0]['started_at']=(self.now-timedelta(seconds=20)).isoformat()
        self.content['authority_recorded_at']=(self.now-timedelta(seconds=20)).isoformat()
        self.content['bindings'][0]['bound_at']=(self.now-timedelta(seconds=10)).isoformat()
        pins={'prompt':json.loads(self.pins['binding_canonical_json'])['eligibility']}
        call,_,_=service._lineage(self.content,self.s,pins)
        self.assertEqual(call['invocation_id'],self.pins['invocation_id'])

    def test_binding_outside_authority_planning_completion_window_denies(self):
        pins={'prompt':json.loads(self.pins['binding_canonical_json'])['eligibility']}
        original=deepcopy(self.content)
        for field,delta in (('authority_recorded_at',1),('started_at',1),('completed_at',-1)):
            self.content=deepcopy(original)
            if field=='authority_recorded_at':self.content[field]=(self.now+timedelta(seconds=delta)).isoformat()
            else:self.content['calls'][0][field]=(self.now+timedelta(seconds=delta)).isoformat()
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'QUALITY_PROVIDER_BINDING_MISMATCH'):
                service._lineage(self.content,self.s,pins)

    def test_unknown_provider_model_bytes_and_candidate_rewrites_denied(self):
        original=deepcopy(self.content)
        for change in (dict(state='UNKNOWN'),dict(returned_model_id='other'),dict(response_sha256='c'*64)):
            self.content=deepcopy(original);self.content['calls'][0].update(change)
            with self.assertRaises(ValueError):self.run_check()
        self.content=original
        with self.assertRaises(ValueError):self.run_check(build=lambda f:(fixtures.builder(f),'different bytes'))

    def test_missing_source_or_stale_authority_has_no_local_fallback(self):
        self.reader.side_effect=RuntimeError('missing authority')
        with self.assertRaises(RuntimeError):self.run_check()
        self.reader.side_effect=None
        self.reader.return_value=replace(self.capture(self.content),valid_until=self.now)
        with self.assertRaises(ValueError):self.run_check()
