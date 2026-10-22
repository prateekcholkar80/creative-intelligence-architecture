"""Versioned request-selection/transcript authority and outcome binding contracts.

Rows must be captured by an authenticated server writer in BigQuery. Constructing
one locally does not establish selection, transcript authority or approval.
"""
import hashlib
import json
from google.cloud import bigquery

from src.application.campaign_acceptance_contract import Stage, _hash, _utc
from src.application.workflow_contract import _require, _id, _digest, canonical_json_bytes
from datetime import timedelta
from src.ingestion.setup_workflow_tables import PROJECT

VERSION='campaign-request-authority-v1'
TABLE=f'{PROJECT}.ci_application.campaign_request_authorities'
COUNTRIES=frozenset({'US','CA','GB','AU'})
CHANNELS=frozenset({'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'})
CATALOG=frozenset(f'CI-SKU-{i:04d}' for i in range(1,24))
KEYS={'version','workspace_id','conversation_id','request_id','principal_id','request_sha256',
      'sku','country','channel','stage','formula_version','audience','transcript','claims'}


def table_metadata():
    table=bigquery.Table(TABLE,schema=[bigquery.SchemaField(n,'STRING',mode='REQUIRED')
        for n in ('workspace_id','conversation_id','request_id','principal_id',
                  'payload_canonical_json','payload_sha256')]+[
        bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')])
    table.clustering_fields=['workspace_id','conversation_id','request_id']
    table.description=VERSION+': immutable server-captured selection and hash-only transcript; not approval'
    return table


def validate_payload(value):
    if type(value) is dict and value.get('version') == 'campaign-question-authority-v1':
        from src.application.campaign_question_binding import validate_parent
        _require(set(value) == KEYS | {'question_parent'} and value['stage'] == 'CLARIFICATION',
            'INVALID_QUESTION_AUTHORITY')
        validate_parent(value['question_parent'], child_request_id=value['request_id'])
        base = {k:v for k,v in value.items() if k != 'question_parent'}
        validate_payload(dict(base, version=VERSION))
        return value
    _require(type(value) is dict and set(value)==KEYS and value['version']==VERSION,'INVALID_REQUEST_AUTHORITY')
    for key in ('workspace_id','conversation_id','request_id'):_id(value[key])
    _id(value['principal_id'],maximum=256);_digest(value['request_sha256'])
    _require(value['sku'] in CATALOG and value['country'] in COUNTRIES and value['channel'] in CHANNELS
        and value['stage'] in {s.value for s in Stage},'INVALID_REQUEST_AUTHORITY_SCOPE')
    if value['formula_version'] is not None:_id(value['formula_version'])
    _require(type(value['audience']) is str and 0<len(value['audience'])<=160,'INVALID_REQUEST_AUTHORITY_AUDIENCE')
    turns=value['transcript']
    _require(type(turns) is list and 0<=len(turns)<=100,'INVALID_TRANSCRIPT_COVERAGE')
    seen=set()
    for turn in turns:
        _require(type(turn) is dict and set(turn)=={'request_id','role','content_sha256','invocation_id'},
            'INVALID_TRANSCRIPT_TURN')
        _id(turn['request_id']);_digest(turn['content_sha256'])
        _require(turn['role'] in {'USER','ASSISTANT'},'INVALID_TRANSCRIPT_TURN')
        if turn['role']=='ASSISTANT':_id(turn['invocation_id'])
        else:_require(turn['invocation_id'] is None,'INVALID_TRANSCRIPT_TURN')
        key=(turn['request_id'],turn['role'])
        _require(key not in seen,'DUPLICATE_TRANSCRIPT_TURN');seen.add(key)
    claims=value['claims']
    _require(type(claims) is list and len(claims)<=8,'INVALID_AUTHORITY_CLAIMS')
    ids=[]
    for claim in claims:
        _require(type(claim) is dict and set(claim)=={'claim_id','wording','qualifications'},'INVALID_AUTHORITY_CLAIMS')
        _id(claim['claim_id']);ids.append(claim['claim_id'])
        _require(type(claim['wording']) is str and 0<len(claim['wording'])<=4096
            and type(claim['qualifications']) is list and len(claim['qualifications'])<=16
            and all(type(t) is str and 0<len(t)<=4096 for t in claim['qualifications']), 'INVALID_AUTHORITY_CLAIMS')
    _require(ids==sorted(set(ids)) and (not claims or value['formula_version'] is not None),'INVALID_AUTHORITY_CLAIMS')
    canonical_json_bytes(value)
    return value


def decode_authority(row, *, request, identity, observed_at):
    expected={f.name for f in table_metadata().schema}
    _require(type(row) is dict and set(row)==expected and type(row['payload_canonical_json']) is str
        and len(row['payload_canonical_json'].encode())<=65536,'INVALID_REQUEST_AUTHORITY_ROW')
    value=validate_payload(json.loads(row['payload_canonical_json']))
    _require(canonical_json_bytes(value).decode()==row['payload_canonical_json']
        and _hash(value)==row['payload_sha256'],'REQUEST_AUTHORITY_HASH_MISMATCH')
    for key in ('workspace_id','conversation_id','request_id','principal_id'):
        _require(row[key]==value[key]==request[key],'REQUEST_AUTHORITY_SCOPE_MISMATCH')
    preferences=request['submitted_preferences']['preferences']
    selected=lambda key:request[key] if request[key] is not None else preferences[key]
    _require(value['principal_id']==identity.principal.principal_id
        and value['request_sha256']==request['request_sha256']
        and value['sku']==selected('canonical_sku') and value['audience']==selected('audience'),
        'REQUEST_AUTHORITY_INPUT_MISMATCH')
    family='EMAIL' if value['channel']=='KLAVIYO_EMAIL' else 'PAID_SOCIAL'
    _require(selected('channel')==family,'REQUEST_AUTHORITY_CHANNEL_MISMATCH')
    _require(request['created_at']<=_utc(row['recorded_at'])<=_utc(observed_at),'REQUEST_AUTHORITY_TIME_MISMATCH')
    operation='GENERATE' if value['stage']==Stage.GENERATION.value else 'INTERPRET'
    _require(value['stage']!=Stage.REFINEMENT.value and request['operation']==operation,'REQUEST_AUTHORITY_STAGE_MISMATCH')
    return value


def validate_transcript(authority, requests, invocations):
    """Complete ordered prior turns plus current user input; no raw-text inference.

    GENERATE has structured input, not a stored user utterance. It cannot be
    certified as transcript USER text. A separate INTERPRET request must capture
    that utterance; missing message hashes deny rather than fabricate coverage.
    """
    by_id={r['request_id']:r for r in requests}
    _require(len(by_id)==len(requests),'DUPLICATE_TRANSCRIPT_REQUEST')
    calls={i['invocation_id']:i for i in invocations}
    _require(len(calls)==len(invocations),'DUPLICATE_TRANSCRIPT_INVOCATION')
    current=by_id[authority['request_id']]
    expected=[]
    for request in sorted(requests,key=lambda r:(r['created_at'],r['request_id'])):
        if (request['created_at'],request['request_id'])>(current['created_at'],current['request_id']):continue
        if request['message_sha256'] is not None:
            expected.append(dict(request_id=request['request_id'],role='USER',
                content_sha256=request['message_sha256'],invocation_id=None))
        _require(all(i['state'] in {'SUCCEEDED','FAILED','BLOCKED'} for i in invocations
            if i['request_id']==request['request_id'] and request['request_id']!=current['request_id']),
            'TRANSCRIPT_OUTCOME_UNKNOWN')
        relevant=[i for i in invocations if i['request_id']==request['request_id'] and i['state']=='SUCCEEDED'
            and request['request_id']!=current['request_id']]
        _require(len(relevant)<=1,'AMBIGUOUS_TRANSCRIPT_RESPONSE')
        for call in relevant:
            _require(call['response_sha256'] is not None and call['finish_reason']=='STOP'
                and call['safety_outcome']=='PASSED','INVALID_TRANSCRIPT_RESPONSE')
            expected.append(dict(request_id=request['request_id'],role='ASSISTANT',
                content_sha256=call['response_sha256'],invocation_id=call['invocation_id']))
    _require(authority['transcript']==expected,'TRANSCRIPT_AUTHORITY_MISMATCH')
    return _hash(dict(version='stored-transcript-hashes-v1',turns=expected))


def bind_provider(invocation, *, request, profile, response_text, candidate_text):
    for key in ('workspace_id','conversation_id','request_id','owner_id','trace_id'):
        _require(invocation[key]==request[key],'QUALITY_PROVIDER_SCOPE_MISMATCH')
    _require(invocation['profile']==profile and invocation['state']=='SUCCEEDED'
        and invocation['dispatch_receipt_id'] is not None and invocation['worker_id'] is not None
        and invocation['returned_model_id']==profile['model_id'] and invocation['finish_reason']=='STOP'
        and invocation['safety_outcome']=='PASSED' and invocation['completed_at'] is not None,
        'QUALITY_PROVIDER_OUTCOME_UNVERIFIED')
    _require(type(response_text) is str and len(response_text.encode())<=65536
        and hashlib.sha256(response_text.encode()).hexdigest()==invocation['response_sha256'],
        'QUALITY_PROVIDER_RESPONSE_MISMATCH')
    # This adapter certifies exact text only. Structured planner projection needs
    # an independently versioned projector, never a guessed JSON field.
    _require(candidate_text==response_text,'QUALITY_PROVIDER_PROJECTION_UNSUPPORTED')
    return _hash(dict(invocation_id=invocation['invocation_id'],revision=invocation['revision'],
        profile=profile,response_sha256=invocation['response_sha256'],returned_model_id=invocation['returned_model_id']))


def bind_permissions(authority,snapshots,*,checked_at):
    from src.application.campaign_permission_reader import PermissionSnapshot
    from src.application.campaign_permission_storage import reconcile_claim_evidence
    _require(type(snapshots) is tuple and len(snapshots)==len(authority['claims']),'QUALITY_CLAIM_COVERAGE')
    pins=[]
    for use,snapshot in zip(authority['claims'],snapshots):
        _require(type(snapshot) is PermissionSnapshot and snapshot.state.kind=='CLAIM'
            and snapshot.state.record_id==use['claim_id']
            and _utc(checked_at)-timedelta(seconds=60)<=snapshot.checked_at<=_utc(checked_at),'QUALITY_CLAIM_SCOPE')
        claim=reconcile_claim_evidence(snapshot.state,snapshot.evidence_states,checked_at=checked_at)
        # Reconciliation returns no use grant; enforce exact wording and target scope.
        claim=snapshot.state.current
        claim.validate_use(sku=authority['sku'],formula_version=authority['formula_version'],wording=use['wording'],
            qualifications=tuple(use['qualifications']),country=authority['country'],channel=authority['channel'],
            audience=authority['audience'],checked_at=checked_at)
        pins.append(dict(claim_id=claim.claim_id,claim_sha256=claim.sha256,
            evidence=[dict(evidence_id=s.record_id,sha256=s.current.sha256) for s in snapshot.evidence_states]))
    return _hash(pins)
