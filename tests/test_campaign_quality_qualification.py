"""Synthetic measured-result records exercise verification, not measured quality."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
from unittest import TestCase
from src.application import campaign_quality_qualification as quality
from src.application.campaign_policy_approval_receipt import PolicyApprovalPins
from src.application.campaign_acceptance_contract import RULE_IDS,_hash
from src.application.workflow_codec import encode_row
from src.application.workflow_contract import request_fingerprint,canonical_json_bytes
from tests.test_prompt_runtime_contract import binding
from tests.test_invocation_contract import invocation


def qualification_fixture():
    bound=binding();payload=json.loads(bound.binding_canonical_json);profile=payload['profile'];now=bound.bound_at
    call,context=invocation('SUCCEEDED');request=deepcopy(context['request'])
    request.update(workspace_id=bound.workspace_id,conversation_id=bound.conversation_id,request_id=bound.request_id,
        principal_id=bound.principal_id,owner_id=bound.principal_id)
    request['model_profiles']=[profile if p['stage']==profile['stage'] else p for p in request['model_profiles']]
    request['request_sha256']=request_fingerprint(request)
    for key in ('workspace_id','conversation_id','request_id','owner_id','trace_id'):call[key]=request[key]
    call.update(invocation_id=bound.invocation_id,profile=profile,returned_model_id=profile['model_id'],
        started_at=now-timedelta(seconds=1),completed_at=now+timedelta(seconds=1))
    index=[dict(case_id='reviewed-e2e',stage='GENERATION',case_sha256='c'*64,label_scope='STAGE_END_TO_END',
        rule_ids=list(RULE_IDS),expected_decision='PASS',requires_provider=True,request_sha256=request['request_sha256'])]
    requirements=quality.QualificationRequirements('a'*64,_hash(index),'b'*64,('GENERATION',))
    benchmark=dict(schema_version='campaign-quality-benchmark-v1',benchmark_id='reviewed-benchmark',version='v1',
        review_sha256=requirements.review_sha256,criterion_ids=list(RULE_IDS),evaluator_sha256=requirements.evaluator_sha256,
        required_stages=list(requirements.required_stages),case_index=index)
    profiles={'GENERATION':dict(prompt_snapshot_sha256=payload['eligibility']['snapshot_sha256'],
        model_profile_sha256=_hash(profile),model_id=profile['model_id'])}
    policies={kind:dict(policy_id='acceptance' if kind=='ACCEPTANCE' else 'clarification',policy_version='v1',policy_sha256='d'*64)
        for kind in ('ACCEPTANCE','CLARIFICATION')}
    report=dict(schema_version='campaign-quality-measurement-v1',evaluation_id='measured-evaluation',workspace_id=bound.workspace_id,
        benchmark_sha256=_hash(benchmark),evaluator_sha256=requirements.evaluator_sha256,policy_bindings=policies,profiles=profiles,
        started_at=(now-timedelta(seconds=2)).isoformat(),completed_at=(now+timedelta(seconds=2)).isoformat(),
        valid_until=(now+timedelta(hours=1)).isoformat(),recorded_by='iap-v1:'+'e'*64,
        cases=[dict(case_id='reviewed-e2e',case_sha256='c'*64,observed_decision='PASS',checks=[dict(rule_id=rid,state='PASS',
            reason_code='MEASURED_SYNTHETIC_FIXTURE',evidence_sha256='f'*64) for rid in RULE_IDS],
            provider=dict(invocation_id=bound.invocation_id,response_sha256=call['response_sha256'],binding_sha256=bound.binding_sha256))])
    pins=PolicyApprovalPins(bound.workspace_id,'ACCEPTANCE','GENERATION','acceptance','v1','d'*64,
        profiles['GENERATION']['prompt_snapshot_sha256'],_hash(profile),_hash(benchmark),_hash(report))
    args=dict(pins=pins,requirements=requirements,invocations=(encode_row(call,'model_invocations'),),bindings=(bound.row(),),
        requests=(encode_row(request,'requests'),),native_job_id='real-read-fixture-label',observed_at=now+timedelta(seconds=3),valid_until=now+timedelta(seconds=60))
    return benchmark,report,args


def run(benchmark,report,args):
    return quality.qualify_documents(canonical_json_bytes(benchmark).decode(),canonical_json_bytes(report).decode(),**args)


class QualityQualificationTests(TestCase):
    def test_complete_matching_source_and_provider_bindings_return_scoped_observation(self):
        benchmark,report,args=qualification_fixture()
        result=run(benchmark,report,args)
        self.assertEqual(result.pins,args['pins']);self.assertEqual(result.native_job_id,args['native_job_id'])
        self.assertEqual(len(report['cases'][0]['checks']),47)

    def test_partial_failed_unavailable_or_skipped_checks_cannot_qualify(self):
        for mode in ('partial','unavailable','unimplemented','skip','wrong_decision'):
            benchmark,report,args=qualification_fixture()
            if mode=='partial':report['cases'][0]['checks'].pop()
            elif mode=='unavailable':report['cases'][0]['checks'][0]['state']='UNAVAILABLE'
            elif mode=='unimplemented':report['cases'][0]['checks'][0]['reason_code']='CHECKER_NOT_IMPLEMENTED'
            elif mode=='skip':report['cases'][0]['checks'][0]['state']='NOT_APPLICABLE'
            else:report['cases'][0]['observed_decision']='BLOCK'
            args['pins']=replace(args['pins'],evaluation_sha256=_hash(report))
            with self.subTest(mode=mode),self.assertRaises(ValueError):run(benchmark,report,args)

    def test_changed_review_case_set_model_or_policy_pins_deny(self):
        benchmark,report,args=qualification_fixture()
        for field in ('model_profile_sha256','policy_sha256','benchmark_sha256','evaluation_sha256'):
            with self.subTest(field=field),self.assertRaises(ValueError):
                run(benchmark,report,args|dict(pins=replace(args['pins'],**{field:'0'*64})))
        with self.assertRaises(ValueError):run(benchmark,report,args|dict(requirements=replace(args['requirements'],review_sha256='0'*64)))
        with self.assertRaises(ValueError):run(benchmark,report,args|dict(requirements=replace(args['requirements'],case_index_sha256='0'*64)))

    def test_missing_or_foreign_durable_provider_sources_deny(self):
        benchmark,report,args=qualification_fixture()
        for changes in (dict(invocations=()),dict(bindings=()),dict(requests=())):
            with self.subTest(changes=changes),self.assertRaises(ValueError):run(benchmark,report,args|changes)
        changed=json.loads(args['invocations'][0]);changed['response_sha256']='0'*64
        with self.assertRaises(ValueError):run(benchmark,report,args|dict(invocations=(json.dumps(changed),)))

    def test_checker_only_or_p5_smoke_document_cannot_qualify(self):
        benchmark,report,args=qualification_fixture()
        benchmark['case_index'][0]['label_scope']='ISOLATED_RULE_HANDLING'
        args['requirements']=replace(args['requirements'],case_index_sha256=_hash(benchmark['case_index']))
        args['pins']=replace(args['pins'],benchmark_sha256=_hash(benchmark))
        with self.assertRaisesRegex(ValueError,'END_TO_END_COVERAGE'):run(benchmark,report,args)
        with self.assertRaises(ValueError):quality.qualify_documents('{}','{}',**args)

    def test_pass_decision_cannot_hide_failed_or_inconclusive_checks(self):
        for state in ('FAIL','INCONCLUSIVE'):
            benchmark,report,args=qualification_fixture()
            report['cases'][0]['checks'][0]['state']=state
            args['pins']=replace(args['pins'],evaluation_sha256=_hash(report))
            with self.subTest(state=state),self.assertRaisesRegex(ValueError,'DECISION_CONFLICT'):
                run(benchmark,report,args)
