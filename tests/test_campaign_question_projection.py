"""Observed durable question output projects exactly; no release grant."""
from copy import deepcopy
from dataclasses import replace
import hashlib,json
from unittest import TestCase
from src.application.campaign_question_projection import bind_question_output
from src.application.campaign_acceptance_contract import _hash
from tests import test_campaign_evidence_conversation as gap_fixture
from tests import test_campaign_question_binding as binding_fixture


class QuestionProjectionTests(TestCase):
    def setUp(self):
        q=gap_fixture.EvidenceConversationTests();q.setUp()
        b=binding_fixture.QuestionBindingTests();b.setUp()
        self.plan=b.gaps.plan
        # The requirement/fields are unchanged; use the actual pinned plan hash.
        value=json.loads(q.raw);value['plan_sha256']=self.plan.sha256
        self.raw=json.dumps(value)
        self.authority=b.bind();self.profile=b.profile
        self.request=dict(workspace_id='w',conversation_id='c',request_id='question-child',owner_id='p',principal_id='p',trace_id='trace',request_sha256=self.authority['request_sha256'])
        self.call=dict(self.request,invocation_id='question-call',stage='INTERPRETER',state='SUCCEEDED',revision=3,
            profile=self.profile,dispatch_receipt_id='receipt',worker_id='worker',returned_model_id=self.profile['model_id'],
            finish_reason='STOP',safety_outcome='PASSED',completed_at='2026-10-16T00:00:00+00:00',
            response_sha256=hashlib.sha256(self.raw.encode()).hexdigest())
    def bind(self):return bind_question_output(self.plan,self.raw,authority=self.authority,
        request=self.request,invocation=self.call,profile=self.profile,binding_sha256='a'*64)
    def test_exact_provider_json_projects_display_copy_and_binds_hashes(self):
        projection,provider=self.bind()
        self.assertEqual(projection.text,json.loads(self.raw)['questions'][0]['question'])
        self.assertEqual(projection.response_sha256,self.call['response_sha256'])
        self.assertEqual(len(projection.sha256),64);self.assertEqual(len(provider),64)
        self.assertFalse(projection.release_allowed)
    def test_changed_parent_plan_profile_or_provider_hash_denies(self):
        for key,value in [('gap_plan_sha256','e'*64),('response_sha256','f'*64),('request_id','other-parent')]:
            previous=deepcopy(self.authority);self.authority['question_parent'][key]=value
            with self.assertRaises(ValueError):self.bind()
            self.authority=previous
        self.call['response_sha256']='f'*64
        with self.assertRaises(ValueError):self.bind()
    def test_forged_fields_extra_copy_and_incomplete_question_coverage_deny(self):
        original=self.raw
        for change in ('fields','extra','empty'):
            value=json.loads(original)
            if change=='fields':value['questions'][0]['fields']=['unapproved-field']
            elif change=='extra':value['additional_copy']='Unreviewed text'
            else:value['questions']=[]
            self.raw=json.dumps(value);self.call['response_sha256']=hashlib.sha256(self.raw.encode()).hexdigest()
            with self.subTest(change=change),self.assertRaises(ValueError):self.bind()
    def test_question_edit_changes_projection_and_requires_new_provider_hash(self):
        first,_=self.bind();value=json.loads(self.raw);value['questions'][0]['question']='What are the offer exclusions?'
        self.raw=json.dumps(value)
        with self.assertRaises(ValueError):self.bind()
        self.call['response_sha256']=hashlib.sha256(self.raw.encode()).hexdigest()
        second,_=self.bind();self.assertNotEqual(first.sha256,second.sha256)

    def test_authoritative_quality_bridge_binds_projected_copy_and_keeps_hold(self):
        from datetime import datetime,timedelta,timezone
        from types import SimpleNamespace
        from unittest.mock import Mock,patch
        from src.application import campaign_quality_bound_execution as service
        from src.application.campaign_quality_execution import QualityRequest
        from src.application.campaign_quality_checks import QualityCandidate,check_candidate
        from src.application.campaign_acceptance_contract import Stage
        from tests.test_campaign_quality_execution import settings
        now=datetime.now(timezone.utc);until=now+timedelta(seconds=60)
        # Interface-only N&T fixture for the product quality reader. This does
        # not establish workflow-catalog admission or real product provenance.
        plan_value=json.loads(self.plan.canonical_json);plan_value['scope']['sku']='CI-SKU-0021'
        self.plan=replace(self.plan,canonical_json=json.dumps(plan_value,sort_keys=True,separators=(',',':')))
        self.authority['sku']='CI-SKU-0021';self.authority['question_parent']['gap_plan_sha256']=self.plan.sha256
        raw=json.loads(self.raw);raw['plan_sha256']=self.plan.sha256;self.raw=json.dumps(raw)
        self.call['response_sha256']=hashlib.sha256(self.raw.encode()).hexdigest()
        projection,_=self.bind()
        request=QualityRequest('w','c','question-child','CI-SKU-0021',Stage.CLARIFICATION,'US','META','d'*64,'e'*64)
        candidate=QualityCandidate('question-child',Stage.CLARIFICATION,'CI-SKU-0021','US','META',projection.text,())
        report=check_candidate(candidate,(),checked_at=now,not_before=now)
        access=SimpleNamespace(to_dict=lambda:{'fixture_only':True})
        content=dict(authority=self.authority,request_record=self.request,catalog_sha256='a'*64,transcript_sha256='b'*64)
        def run(value):
            # Reader/approval fixtures isolate this projection bridge only; real
            # native/source checks are covered by their respective reader tests.
            reads=[(SimpleNamespace(content_sha256='c'*64,native_job_id=n,valid_until=until),content)
                for n in ('first','second')]
            with patch.object(service,'_capture',side_effect=reads), \
                patch.object(service,'_access',return_value=access), \
                patch.object(service,'_context',return_value=({'prompt':{'eligible_until':until.isoformat()}},now)), \
                patch.object(service,'_lineage',return_value=(self.call,self.profile,'a'*64)), \
                patch.object(service,'_permissions',return_value='b'*64), \
                patch.object(service,'read_product_fact',return_value=SimpleNamespace(fact=SimpleNamespace(valid_until=until),native_job_id='product')), \
                patch.object(service,'check_candidate',return_value=report):
                return service.run_authoritative_quality(request,lambda _:(value,self.raw),settings=settings(),
                    client=Mock(),runtime_reader=Mock(),access_reader=Mock(),identity=Mock(),clock=lambda:now,question_plan=self.plan)
        result=run(candidate)
        self.assertFalse(result.release_allowed)
        self.assertEqual(json.loads(result.context_canonical_json)['projection_sha256'],projection.sha256)
        with self.assertRaisesRegex(ValueError,'QUALITY_QUESTION_PROJECTION_COPY_CHANGED'):
            run(replace(candidate,text='Different display copy'))
