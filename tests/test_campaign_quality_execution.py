from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import Mock, patch

from src.application import campaign_quality_execution as execution
from src.application.access_policy import AccessDecision, Action, Reason
from src.application.campaign_acceptance_contract import Stage, Decision, _hash
from src.application.prompt_runtime_contract import canonical
from src.application.workflow_contract import ContractError
from src.application.campaign_quality_request_reader import RequestContextRead
from tests.test_campaign_policy_reader import decode
from tests.test_campaign_quality_checks import candidate
from tests.test_campaign_quality_product_reader import client
from tests.test_prompt_runtime_contract import capture, profile, behavior
from tests.test_prompt_lifecycle_contract import snapshot


def request(**changes):
    c=capture()
    return execution.QualityRequest(**(dict(workspace_id=c.request.workspace_id,
        conversation_id='conversation-1',request_id='request-1',sku='CI-SKU-0021',stage=Stage.GENERATION,
        country='US',channel='META',user_input_sha256='a'*64,conversation_sha256='b'*64)|changes))


def settings(**changes):
    p=decode()
    return execution.QualityExecutionSettings(**(dict(policy_id=p.policy_id,policy_version=p.version,
        policy_specification_sha256=p.specification_sha256,policy_header_sha256=p.header_sha256,
        behavior=behavior(),model_profile_json=canonical(profile()),approved_profiles=frozenset({_hash(profile())}))|changes))


def builder(fact):return candidate(fact,stage=Stage.GENERATION)


def request_capture(current, native_id='request-read-1', **changes):
    """Synthetic reader-interface fixture only, not a valid stored N&T request.

    The real workflow-v1 reader still rejects this SKU. These observations isolate
    execution composition; separate reader/native-boundary tests prove denial.
    """
    r=request()
    pins=dict(version='campaign-quality-request-bindings-v1',workspace_id=r.workspace_id,
        conversation_id=r.conversation_id,request_id=r.request_id,
        principal_id=current.request.identity.principal.principal_id,access_sha256='e'*64,
        request_sha256=r.user_input_sha256,message_sha256=None,request_record_sha256='d'*64,
        request_revision=1,operation='GENERATE',request_status='READY',canonical_sku=r.sku,
        channel='PAID_SOCIAL',history_kind='PERSISTED_REQUEST_RECORDS',history_count=1,
        history_sha256=r.conversation_sha256)|changes
    return RequestContextRead(canonical(pins),_hash(pins),native_id,current.observed_at,
        current.observed_at+timedelta(seconds=60))


class QualityExecutionTests(TestCase):
    def setUp(self):
        self.current=capture();self.clock=lambda:self.current.observed_at
        self.client=client();self.runtime=Mock(read=Mock(return_value=self.current))
        r=request();self.decision=AccessDecision(True,Action.READ_DRAFT,Reason.ALLOWED_OWNER,r.workspace_id,
            self.current.request.identity.principal.principal_id,self.current.request.membership_revision,
            r.conversation_id,1)
        self.access=Mock(authorize=Mock(return_value=self.decision))
        self.policy=patch.object(execution,'read_policy',return_value=decode()).start()
        self.capture_counter=0
        def current_request(*args,**kwargs):
            self.capture_counter+=1
            return request_capture(self.current,native_id=f'request-read-{self.capture_counter}')
        self.requests=patch.object(execution,'read_request_context',side_effect=current_request).start()
        self.addCleanup(patch.stopall)
        patch('src.application.campaign_quality_product_reader.ensure_fresh').start()

    def run_check(self,r=None,s=None,b=builder):
        return execution.run_current_quality(r or request(),b,settings=s or settings(),client=self.client,
            runtime_reader=self.runtime,access_reader=self.access,identity=self.current.request.identity,clock=self.clock)

    def test_fresh_rechecks_bind_every_context_and_never_release(self):
        result=self.run_check();context=json.loads(result.context_canonical_json)
        self.assertEqual(result.context_sha256,_hash(context))
        self.assertFalse(result.release_allowed);self.assertIs(result.report.decision,Decision.NEEDS_REVIEW)
        self.assertEqual(context['request'],request().payload())
        self.assertEqual(context['sources']['model_profile_sha256'],_hash(profile()))
        self.assertEqual(context['sources']['prompt']['registry_digest'],self.current.target.head.digest)
        self.assertEqual(context['sources']['policy']['snapshot_sha256'],decode().snapshot_sha256)
        self.assertEqual(self.policy.call_count,2);self.assertEqual(self.runtime.read.call_count,2)
        self.assertEqual(self.access.authorize.call_count,3)
        self.assertEqual(self.requests.call_count,2)
        self.assertEqual(context['request_bindings_sha256'],request_capture(self.current).bindings_sha256)
        self.assertFalse(context['request_coverage']['transcript_authoritative'])
        self.assertFalse(context['request_coverage']['market_and_exact_platform_authoritative'])
        self.assertEqual(context['request_coverage']['workflow_catalog'],'ORIGINAL_20_SKUS')
        self.assertEqual(context['evidence_sha256'],result.report.evidence_sha256)
        with self.assertRaises(FrozenInstanceError):result.context_sha256='c'*64

    def test_supplied_input_and_history_drift_deny_before_policy_product_or_annotation(self):
        for r,code in ((request(user_input_sha256='c'*64),'QUALITY_REQUEST_INPUT_MISMATCH'),
                       (request(conversation_sha256='c'*64),'QUALITY_REQUEST_HISTORY_MISMATCH')):
            b=Mock()
            with self.assertRaisesRegex(ContractError,code):self.run_check(r,b=b)
            b.assert_not_called()
        self.policy.assert_not_called();self.client.query.assert_not_called()

    def test_candidate_request_stage_market_channel_and_sku_cannot_drift(self):
        for changes in (dict(request_id='other'),dict(stage=Stage.REFINEMENT),dict(country='CA'),
            dict(channel='PINTEREST'),dict(sku='CI-SKU-0022')):
            with self.subTest(changes=changes),self.assertRaises(ContractError):
                self.run_check(b=lambda fact:replace(builder(fact),**changes))

    def test_initial_access_denial_prevents_all_source_reads(self):
        self.access.authorize.return_value=replace(self.decision,allowed=False)
        with self.assertRaises(ContractError):self.run_check()
        self.requests.assert_not_called()
        self.policy.assert_not_called();self.runtime.read.assert_not_called();self.client.query.assert_not_called()

    def test_access_revocation_after_annotation_or_final_context_reread_denies(self):
        for decisions in ([self.decision,replace(self.decision,allowed=False)],
            [self.decision,self.decision,replace(self.decision,resource_revision=2)]):
            self.access.authorize.side_effect=decisions
            with self.assertRaises(ContractError):self.run_check()

    def test_policy_change_during_execution_is_not_silently_rebased(self):
        self.policy.side_effect=[decode(),replace(decode(),snapshot_sha256='c'*64)]
        with self.assertRaisesRegex(ContractError,'QUALITY_EXECUTION_CONTEXT_CHANGED'):self.run_check()

    def test_policy_capture_must_match_installed_pins(self):
        for changes in (dict(policy_id='other'),dict(version='other'),dict(header_sha256='c'*64),
            dict(specification_sha256='c'*64)):
            self.policy.return_value=replace(decode(),**changes)
            with self.assertRaises(ContractError):self.run_check()

    def test_draft_or_unevaluated_prompt_stops_before_annotation(self):
        self.runtime.read.return_value=snapshot();b=Mock()
        with self.assertRaises(ContractError):self.run_check(b=b)
        self.client.query.assert_not_called();b.assert_not_called()

    def test_prompt_control_revision_change_during_check_withholds_result(self):
        ec=self.current.evaluation.control
        newer=replace(ec,revision=2,parent_digest=ec.digest,updated_at=ec.updated_at+timedelta(seconds=1))
        changed=replace(self.current,evaluation=replace(self.current.evaluation,control=newer,control_history=(ec,newer)))
        self.runtime.read.side_effect=[self.current,changed]
        with self.assertRaisesRegex(ContractError,'QUALITY_EXECUTION_CONTEXT_CHANGED'):self.run_check()

    def test_wrong_task_or_unapproved_model_profile_denies(self):
        with self.assertRaises(ContractError):self.run_check(request(stage=Stage.INTERPRETATION))
        with self.assertRaises(ContractError):self.run_check(s=settings(approved_profiles=frozenset({'c'*64})))
        edited=profile()|dict(model_id='other')
        with self.assertRaises(ContractError):self.run_check(s=settings(model_profile_json=canonical(edited),
            approved_profiles=frozenset({_hash(edited)})))

    def test_runtime_capture_access_scope_is_checked(self):
        self.access.authorize.return_value=replace(self.decision,membership_revision=self.decision.membership_revision+1)
        with self.assertRaises(ContractError):self.run_check()

    def test_stale_capture_and_final_deadline_deny(self):
        self.clock=lambda:self.current.observed_at+timedelta(seconds=61)
        with self.assertRaises(ContractError):self.run_check()
        elapsed=0
        def access_with_delay(*args,**kwargs):
            nonlocal elapsed
            if self.access.authorize.call_count%3==0:elapsed=60
            return self.decision
        self.access.authorize.reset_mock()
        self.access.authorize.side_effect=access_with_delay
        self.clock=lambda:self.current.observed_at+timedelta(seconds=elapsed)
        with self.assertRaises(ContractError):self.run_check()

    def test_cloud_failures_do_not_fallback_or_dispatch(self):
        self.policy.side_effect=RuntimeError('unavailable');b=Mock()
        with self.assertRaises(RuntimeError):self.run_check(b=b)
        self.client.query.assert_not_called();b.assert_not_called()

    def test_request_and_settings_reject_malformed_pins_and_mutable_json(self):
        for changes in (dict(country='FR'),dict(sku='CI-SKU-0024'),dict(user_input_sha256='bad')):
            with self.assertRaises(ContractError):request(**changes)
        with self.assertRaises(ContractError):settings(model_profile_json=json.dumps(profile()))
        with self.assertRaises(ContractError):settings(approved_profiles=set())
