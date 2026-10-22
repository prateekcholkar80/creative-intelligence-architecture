"""Synthetic native question-child fixtures; never approvals or real authentication."""
from datetime import datetime, timezone, timedelta
import hashlib,json
from pathlib import Path
from src.validation.control_fixtures import controls,profile,ACTOR
from src.application.conversation_contract import validate_conversation
from src.application.grounded_interpreter_submission import prepare_grounded_submission
from src.application.interpreter_outcome import prepare_interpreter_outcome
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.workflow_codec import decode_row,encode_row
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_evidence_gaps import plan_gaps,EvidenceCoverage,canonical
from src.application.campaign_evidence_conversation import BoundGapRead
from src.application.campaign_quality_authority_reader import ExecutionAuthorityRead
from src.application.campaign_question_submission import prepare_question_submission
from src.application.prompt_runtime_contract import STORAGE_VERSION


def parent_fixture(workspace):
    rows=controls(workspace);ws=rows['workspaces'][0];conv=rows['conversations'][0]
    resource=validate_conversation(conv,workspace_id=workspace,conversation_id=conv['conversation_id'])
    p=profile('INTERPRETER');p['output_schema_version']='campaign-evidence-intent-v1'
    selection=dict(sku='CI-SKU-0001',country='US',channel='META',audience='NEW')
    first=prepare_grounded_submission(selection=selection,workspace=ws,membership=rows['memberships'][0],
        conversation=conv,resource=resource,principal_id=ACTOR,message='Consider an offer campaign',
        retry_key='parent',profile=p,observed_at=datetime.now(timezone.utc),worker_id='fixture-worker')
    v={p.name:p.value for p in first.config().query_parameters}
    req=decode_row(v['request_candidate'],'requests');inv=decode_row(v['invocation_candidate'],'model_invocations')
    inv.update(state='DISPATCHED',revision=2,worker_id='fixture-worker',dispatch_receipt_id='fixture-dispatch')
    raw=canonical(dict(schema_version='campaign-evidence-intent-v1',features=['OFFER'],formula_version=None,audience='NEW'))
    result=prepare_interpreter_outcome(workspace=ws,membership=rows['memberships'][0],conversation=conv,
        request=req,invocation=inv,response_text=raw,observed_at=datetime.now(timezone.utc),
        observation=ModelResultObservation('SUCCEEDED',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),
            returned_model_id=p['model_id'],finish_reason='STOP',safety_outcome='PASSED'))
    r={p.name:p.value for p in result.config().query_parameters}
    rows.update(workspaces=[decode_row(r['workspace_candidate'],'workspaces')],
        requests=[decode_row(r['request_candidate'],'requests')],model_invocations=[decode_row(r['invocation_candidate'],'model_invocations')])
    binding=dict(schema_version=STORAGE_VERSION,workspace_id=workspace,conversation_id=conv['conversation_id'],
        request_id=req['request_id'],invocation_id=inv['invocation_id'],principal_id=ACTOR,prompt_key='campaign-evidence-intent',
        stage='INTERPRETER',binding_sha256='a'*64,binding_canonical_json=canonical({'fixture_only':True}),bound_at=inv['started_at'].isoformat())
    extra={'campaign_transcript_bodies':[json.loads(v['transcript_candidate'])],
        'campaign_transcript_responses':[json.loads(r['response_candidate'])],
        'campaign_request_authorities':[dict(workspace_id=workspace,conversation_id=conv['conversation_id'],
            request_id=req['request_id'],principal_id=ACTOR,payload_canonical_json=v['authority_payload'],
            payload_sha256=v['authority_digest'],recorded_at=req['created_at'].isoformat())],
        'invocation_prompt_bindings':[binding]}
    return rows,extra


def child_plan(rows,extra,history,key):
    """Fresh fixture captures from independently read fixture rows."""
    now=datetime.now(timezone.utc);req=rows['requests'][0];call=rows['model_invocations'][0]
    authority=json.loads(extra['campaign_request_authorities'][0]['payload_canonical_json'])
    content=dict(authority=authority,request_record=json.loads(encode_row(req,'requests')),
        calls=[json.loads(encode_row(call,'model_invocations'))],
        bindings=extra['invocation_prompt_bindings'])
    parent=ExecutionAuthorityRead(canonical(content),_hash(content),'fixture-native-parent-read',now,now+timedelta(seconds=60))
    scope={k:authority[k] for k in ('workspace_id','conversation_id','request_id','sku','country','channel')}
    scope.update(request_sha256=req['request_sha256'],intent_sha256=call['response_sha256'])
    policy=canonical(json.loads(Path('config/clarification/evidence_requirements_v1.json').read_text()))
    coverage=EvidenceCoverage('commercial-offer',_hash(scope),'ABSENT',(),now,now+timedelta(seconds=60),'fixture-coverage')
    gap=plan_gaps(policy,scope=scope,features=('OFFER',),captures=(coverage,),checked_at=now)
    gaps=BoundGapRead(gap,'b'*64,ACTOR,now+timedelta(seconds=60),False)
    question=profile('INTERPRETER');question['output_schema_version']='campaign-evidence-questions-v1'
    conv=rows['conversations'][0]
    return prepare_question_submission(parent=parent,gaps=gaps,history=history,
        selection={k:authority[k] for k in ('sku','country','channel','audience')},workspace=rows['workspaces'][0],
        membership=rows['memberships'][0],conversation=conv,resource=validate_conversation(conv,
            workspace_id=authority['workspace_id'],conversation_id=conv['conversation_id']),principal_id=ACTOR,
        message='Consider an offer campaign',retry_key=key,profile=question,observed_at=now,worker_id='fixture-worker')
