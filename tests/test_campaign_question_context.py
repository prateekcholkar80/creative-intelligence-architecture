"""Only exact trusted-plan mutations may change a question execution context."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timedelta,timezone
import json,time
from unittest import TestCase
from unittest.mock import Mock,patch
from types import SimpleNamespace
from google.cloud import bigquery
from src.application.campaign_question_context import NAMES,QuestionContextRead,canonical_records,verify_transition
from src.application.campaign_question_context_reader import read_question_context,sql
from src.application.control_io import ControlPlan,query_config
from src.application.identity import IapSettings,VerifiedIdentity,principal_for_subject,ISSUER
from src.validation.control_fixtures import ACTOR,SUBJECT
from src.validation.question_child_fixtures import child_plan
from src.application.workflow_codec import encode_row
from tests import test_question_child_integration as fixture
from tests.test_identity import AUDIENCE


class QuestionContextTests(TestCase):
    def setUp(self):
        f=fixture.QuestionChildProbeTests();rows,extra,history=f.capture()
        self.plan=child_plan(rows,extra,history,'context-child')
        self.p={p.name:p.value for p in self.plan.config().query_parameters}
        self.before={n:[] for n in NAMES}
        for n,values in rows.items():self.before[n]=[encode_row(v,n) for v in values]
        self.before.update({n:[json.dumps(v) for v in values] for n,values in extra.items()})
        self.after=deepcopy(self.before)
        self.after['workspaces']=[self.p['workspace_candidate']]
        for n,key in [('requests','request_candidate'),('model_invocations','invocation_candidate'),
                ('campaign_transcript_bodies','transcript_candidate'),('audit_events','request_audit'),('audit_events','invocation_audit')]:
            self.after[n].append(self.p[key])
        req=json.loads(self.p['request_candidate'])
        self.after['campaign_request_authorities'].append(json.dumps(dict(
            workspace_id=req['workspace_id'],conversation_id=req['conversation_id'],request_id=req['request_id'],
            principal_id=req['principal_id'],payload_canonical_json=self.p['authority_payload'],
            payload_sha256=self.p['authority_digest'],recorded_at=req['created_at'])))
        self.now=datetime.now(timezone.utc)
    def capture(self,records,read_id):
        return QuestionContextRead('question-offline','fixture-conversation',ACTOR,
            canonical_records(records),read_id,self.now,self.now+timedelta(seconds=60))
    def verify(self,after=None,before=None):
        return verify_transition(before or self.capture(self.before,'before'),
            after or self.capture(self.after,'after'),self.plan,phase='ADMITTED',checked_at=self.now)
    def test_exact_child_admission_does_not_grant_release(self):
        result=self.verify();self.assertFalse(result.release_allowed)
        self.assertNotEqual(result.before_sha256,result.after_sha256)
    def test_extra_message_membership_and_raw_transcript_changes_deny(self):
        for table,key,value in [('memberships','roles',['ADMIN']),('campaign_transcript_bodies','batch_canonical_json','changed'),
                ('requests','message_sha256','f'*64),('conversations','visibility','TEAM')]:
            altered=deepcopy(self.after);row=json.loads(altered[table][0]);row[key]=value
            altered[table][0]=json.dumps(row)
            with self.subTest(table=table),self.assertRaises(ValueError):self.verify(self.capture(altered,'changed'))
        altered=deepcopy(self.after);row=json.loads(altered['requests'][0]);row['request_id']='extra-turn'
        altered['requests'].append(json.dumps(row))
        with self.assertRaises(ValueError):self.verify(self.capture(altered,'extra'))
    def test_missing_body_audit_fence_or_authority_deny_partial_context(self):
        for name in ('campaign_transcript_bodies','audit_events','campaign_request_authorities','workspaces'):
            altered=deepcopy(self.after);altered[name]=deepcopy(self.before[name])
            with self.subTest(name=name),self.assertRaises(ValueError):self.verify(self.capture(altered,'partial'))
    def test_reused_expired_changed_scope_and_duplicate_key_deny(self):
        before=self.capture(self.before,'before');after=self.capture(self.after,'after')
        for changed in (replace(after,read_id='before'),replace(after,valid_until=self.now),
                replace(after,principal_id='another-principal')):
            with self.assertRaises(ValueError):self.verify(changed,before)
        records=deepcopy(self.before);row=json.loads(records['requests'][0]);row['message_sha256']='f'*64
        records['requests'].append(json.dumps(row))
        with self.assertRaisesRegex(ValueError,'DUPLICATE_KEY'):self.capture(records,'duplicate')
    def test_registered_question_dispatch_allows_only_its_recorded_mutations(self):
        p=self.p;old=json.loads(p['invocation_candidate']);candidate=old|dict(state='DISPATCHED',revision=2,
            worker_id='fixture-worker',dispatch_receipt_id='fixture-receipt')
        ws=json.loads(p['workspace_candidate']);new_ws=ws|dict(revision=ws['revision']+1)
        binding=json.loads(self.after['invocation_prompt_bindings'][0])
        binding.update(invocation_id=old['invocation_id'],request_id=old['request_id'],prompt_key='campaign-evidence-questions')
        audit=json.loads(p['invocation_audit']);audit['audit_id']='dispatch-fixture-audit'
        params={'memberships_pin':p['membership_pin'],'conversations_pin':p['conversation_pin'],'requests_pin':p['request_candidate'],'model_invocations_pin':p['invocation_candidate'],
            'candidate':json.dumps(candidate),'workspaces_pin':p['workspace_candidate'],'workspace':json.dumps(new_ws),
            'runtime_binding':json.dumps(binding),'audit':json.dumps(audit)}
        config=query_config([bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in params.items()])
        plan=ControlPlan(ACTOR,'question-offline','dispatch','dispatch-fixture','fixture SQL',json.dumps(config.to_api_repr()),
            'DISPATCH',json.dumps(candidate),json.dumps(audit),'model_invocations')
        after=deepcopy(self.after);after['workspaces']=[json.dumps(new_ws)]
        after['model_invocations'][-1]=json.dumps(candidate);after['invocation_prompt_bindings'].append(json.dumps(binding))
        after['audit_events'].append(json.dumps(audit))
        result=verify_transition(self.capture(self.after,'admitted'),self.capture(after,'dispatched'),plan,
            phase='DISPATCHED',checked_at=self.now)
        self.assertFalse(result.release_allowed)
        after['model_invocations'][0]=json.dumps(json.loads(after['model_invocations'][0])|dict(response_sha256='f'*64))
        with self.assertRaises(ValueError):verify_transition(self.capture(self.after,'admitted'),self.capture(after,'changed'),plan,
            phase='DISPATCHED',checked_at=self.now)
    def test_context_reader_bounds_counts_and_rechecks_access(self):
        epoch=int(time.time());identity=VerifiedIdentity(principal_for_subject(ISSUER,SUBJECT),ISSUER,SUBJECT,AUDIENCE,epoch-1,epoch+300)
        def read(query,params):
            values={p.name:p.value for p in params};row=dict(observed_at=self.now,read_id=values['read_id'])
            for n in NAMES:row[n]=self.before[n];row[n+'_count']=len(self.before[n])
            return row
        io=Mock();io.read.side_effect=read
        with patch('src.application.campaign_question_context_reader.ControlIO',return_value=io), \
                patch('src.application.campaign_question_context_reader._access',return_value=('current',)):
            result=read_question_context(Mock(),identity=identity,access_reader=Mock(),workspace_id='question-offline',
                conversation_id='fixture-conversation',clock=lambda:self.now)
            self.assertEqual(result.records(),json.loads(canonical_records(self.before)))
        with patch('src.application.campaign_question_context_reader.ControlIO',return_value=io), \
                patch('src.application.campaign_question_context_reader._access',side_effect=[('before',),('after',)]):
            with self.assertRaisesRegex(ValueError,'ACCESS_CHANGED'):read_question_context(Mock(),identity=identity,
                access_reader=Mock(),workspace_id='question-offline',conversation_id='fixture-conversation',clock=lambda:self.now)
        self.assertIn('LIMIT 257',sql());self.assertIn('recorded_at BETWEEN',sql())

    def test_bounded_large_context_and_native_plan_hashing(self):
        records=deepcopy(self.before)
        body=json.loads(records['campaign_transcript_bodies'][0]);body['batch_canonical_json']='x'*70000
        records['campaign_transcript_bodies'][0]=json.dumps(body)
        capture=self.capture(records,'large-bounded')
        self.assertGreater(len(capture.records_canonical_json),65536)
        self.assertEqual(len(capture.sha256),64)
        capture.records()
        config=self.plan.config();config.query_parameters=list(config.query_parameters)+[
            bigquery.ScalarQueryParameter('fixture_large_parameter','STRING','x'*70000)]
        plan=replace(self.plan,config_json=json.dumps(config.to_api_repr()))
        result=verify_transition(self.capture(self.before,'before'),self.capture(self.after,'after'),plan,
            phase='ADMITTED',checked_at=self.now)
        self.assertEqual(len(result.plan_sha256),64)
