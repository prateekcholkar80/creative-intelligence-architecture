"""Atomic initial-turn selection authority; no inferred market, claims or history."""
from dataclasses import replace
import hashlib,json
from google.cloud import bigquery
from src.application.interpreter_submission import prepare_interpreter_submission
from src.application.campaign_quality_authority import VERSION,TABLE,CATALOG,COUNTRIES,CHANNELS,validate_payload
from src.application.workflow_contract import _require,request_fingerprint,canonical_json_bytes,validate_request
from src.application.workflow_codec import decode_row,encode_row
from src.ingestion.setup_workflow_tables import PROJECT,DATASET_ID

SELECTION_KEYS=frozenset({'sku','country','channel','audience'})


def validate_selection(selection):
    _require(type(selection) is dict and set(selection)==SELECTION_KEYS,'CHAT_SELECTION_REQUIRED')
    _require(selection['sku'] in CATALOG and selection['country'] in COUNTRIES and selection['channel'] in CHANNELS
        and type(selection['audience']) is str and 0<len(selection['audience'].strip())<=160
        and selection['audience']==selection['audience'].strip(),'CHAT_SELECTION_INVALID')
    return dict(selection)


def prepare_grounded_submission(*,selection,history=None,**inputs):
    selection=validate_selection(selection);plan=prepare_interpreter_submission(**inputs)
    config=plan.config();params={p.name:p.value for p in config.query_parameters}
    request=decode_row(params['request_candidate'],'requests')
    request['submitted_preferences']['preferences'].update(canonical_sku=selection['sku'],audience=selection['audience'],
        channel='EMAIL' if selection['channel']=='KLAVIYO_EMAIL' else 'PAID_SOCIAL')
    request['request_sha256']=request_fingerprint(request,catalog=CATALOG)
    validate_request(request,conversation=inputs['resource'],catalog=CATALOG)
    authority=dict(version=VERSION,**{k:request[k] for k in ('workspace_id','conversation_id','request_id','principal_id','request_sha256')},
        **selection,stage='INTERPRETATION',formula_version=None,claims=[],transcript=[dict(request_id=request['request_id'],
        role='USER',content_sha256=request['message_sha256'],invocation_id=None)])
    if history is not None:
        from src.application.campaign_transcript_history import TranscriptHistory
        from datetime import timedelta
        _require(type(history) is TranscriptHistory and history.observed_at<=inputs['observed_at']<history.observed_at+timedelta(seconds=60),'CHAT_HISTORY_EXPIRED')
        authority['transcript']=list(history.turns)+authority['transcript']
        from src.application.campaign_transcript import encode_turns
        # Validate complete coverage but store only the new utterance. Copying old
        # text into a new partition would extend its 30-day table retention.
        encode_turns(authority,tuple(history.texts)+(inputs['message'],))
    validate_payload(authority)
    payload=canonical_json_bytes(authority).decode();digest=hashlib.sha256(payload.encode()).hexdigest()
    params['request_candidate']=encode_row(request,'requests')
    config.query_parameters=[bigquery.ScalarQueryParameter(p.name,p.type_,params[p.name]) for p in config.query_parameters]
    config.query_parameters=list(config.query_parameters)+[
        bigquery.ScalarQueryParameter('authority_payload','STRING',payload),bigquery.ScalarQueryParameter('authority_digest','STRING',digest),
        bigquery.ScalarQueryParameter('selected_sku','STRING',selection['sku']),
        bigquery.ScalarQueryParameter('selected_source','STRING','NAKED_AND_THRIVING_OFFICIAL' if int(selection['sku'][-4:])>20 else 'BEAUTYFEEDS')]
    # Capture within the same fenced transaction as request/invocation/body.
    # This initial-turn adapter explicitly refuses an existing conversation history.
    # It cannot certify unseen prior user text or provider responses.
    empty_history=[]
    for name,partition in (('model_invocations','started_at'),('jobs',None),('job_attempts','started_at'),('campaign_transcript_bodies','recorded_at')):
        predicate=(f" AND {partition} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')") if partition else ''
        empty_history.append(f"(SELECT COUNT(*) FROM `{DATASET_ID}.{name}` WHERE workspace_id=@workspace_id AND conversation_id=conversation_pin.conversation_id{predicate})=0")
    history_guard=' AND '.join(empty_history)
    marker=" ASSERT CURRENT_TIMESTAMP()<@expires_at"
    sql=plan.sql.replace(marker,f''' ASSERT (SELECT COUNT(*) FROM `{DATASET_ID}.requests` t
 WHERE workspace_id=@workspace_id AND conversation_id=conversation_pin.conversation_id
 AND created_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'))=0 AS 'CHAT_PRIOR_TRANSCRIPT_CAPTURE_REQUIRED';
 ASSERT {history_guard} AS 'CHAT_PRIOR_TRANSCRIPT_CAPTURE_REQUIRED';
 ASSERT (SELECT COUNT(*) FROM `{TABLE}` WHERE workspace_id=@workspace_id AND (conversation_id=conversation_pin.conversation_id OR request_id=request_candidate.request_id))=0 AS 'CHAT_AUTHORITY_CONFLICT';
 ASSERT (SELECT COUNT(*) FROM `{PROJECT}.ci_product.product_master` WHERE sku=@selected_sku)=1
 AND (SELECT COUNT(*) FROM `{PROJECT}.ci_product.product_master` WHERE sku=@selected_sku
 AND source_system=@selected_source AND product_status='ACTIVE')=1 AS 'CHAT_PRODUCT_UNAVAILABLE';
'''+marker)
    if history is not None:
        from src.application.campaign_transcript_history import TABLES as HISTORY_TABLES
        source=json.loads(history.source_json);guards=[];extra=[]
        for key,(name,partition) in HISTORY_TABLES.items():
            extra.append(bigquery.ScalarQueryParameter('history_'+key,'STRING',json.dumps(source[key],separators=(',',':'))))
            predicate=f" AND {partition} BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31')" if partition else ''
            guards.append(f"ASSERT NOT EXISTS(SELECT value FROM (SELECT TO_JSON_STRING(t) value,'CURRENT' origin FROM `{DATASET_ID}.{name}` t WHERE workspace_id=@workspace_id AND conversation_id=conversation_pin.conversation_id{predicate} UNION ALL SELECT e,'EXPECTED' FROM UNNEST(JSON_VALUE_ARRAY(PARSE_JSON(@history_{key}))) e) GROUP BY value HAVING COUNTIF(origin='CURRENT')!=COUNTIF(origin='EXPECTED')) AS 'CHAT_TRANSCRIPT_SOURCE_CHANGED';")
        config.query_parameters=list(config.query_parameters)+extra
        first=sql.index(' ASSERT (SELECT COUNT(*) FROM `'+DATASET_ID+'.requests` t')
        last=sql.index(' ASSERT (SELECT COUNT(*) FROM `'+TABLE+'`',first)
        sql=sql[:first]+'\n'.join(guards)+'\n'+sql[last:]
        # Prior authority is immutable and remains stored; only this request's key must be absent.
        sql=sql.replace('(conversation_id=conversation_pin.conversation_id OR request_id=request_candidate.request_id)','request_id=request_candidate.request_id')
    replay=" AS 'INTERPRETER_SUBMIT_REPLAY_CONFLICT';"
    sql=sql.replace(replay,f''' AND (SELECT COUNT(*) FROM `{TABLE}` WHERE workspace_id=@workspace_id
 AND request_id=request_candidate.request_id)=1 AND (SELECT COUNT(*) FROM `{TABLE}` WHERE workspace_id=@workspace_id
 AND conversation_id=conversation_pin.conversation_id AND request_id=request_candidate.request_id
 AND principal_id=request_candidate.principal_id AND payload_canonical_json=@authority_payload
 AND payload_sha256=@authority_digest)=1'''+replay)
    sql=sql.replace(" SET result_state='APPLIED';",f''' INSERT INTO `{TABLE}`
 (workspace_id,conversation_id,request_id,principal_id,payload_canonical_json,payload_sha256,recorded_at)
 VALUES(@workspace_id,conversation_pin.conversation_id,request_candidate.request_id,request_candidate.principal_id,
 @authority_payload,@authority_digest,request_candidate.created_at);
 SET result_state='APPLIED';''')
    sql=sql.replace(' AS transcript_row;',' AS transcript_row,@authority_digest AS authority_sha256;')
    _require(sql!=plan.sql and (sql.count('CHAT_TRANSCRIPT_SOURCE_CHANGED')==6 if history is not None else sql.count('CHAT_PRIOR_TRANSCRIPT_CAPTURE_REQUIRED')==2),'CHAT_CAPTURE_SQL_SHAPE')
    return replace(plan,sql=sql,config_json=json.dumps(config.to_api_repr(),sort_keys=True),
        candidate_json=params['request_candidate'],kind='GROUNDED_INTERPRETER_SUBMISSION')
