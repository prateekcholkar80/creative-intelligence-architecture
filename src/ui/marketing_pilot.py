"""IAP-protected release-disabled marketing entry point; no anonymous fallback."""
import base64,json,os,logging,re,traceback
from pathlib import Path
import streamlit as st
from google.cloud import bigquery
from src.application.marketing_pilot import MarketingPilot,DEFAULT_CONVERSATION,EVALUATION_CONVERSATION
from src.ui.workflow_runtime import iap_headers
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION


def show_planner_failure(st,error,*,retrieval=False,corpus=False,embedding=False,measured_retrieval=False):
    """Safe failure category only; never exception text, assertions or content."""
    code=getattr(error,'code',None)
    allowed={'PLANNER_SESSION_REFRESH_REQUIRED','OPERATION_DEADLINE_EXCEEDED',
        'OPERATION_CANCELLED','SHARING_HOST_RETIRED','SHARING_HOST_PROTOCOL_FAILED',
        'ACCESS_DENIED','SHARING_HOST_NOT_STARTED','CORPUS_EXTRACTION_REQUIRED','CORPUS_EXTRACTION_UNAVAILABLE',
        'CORPUS_BINDING_NOT_VERIFIED','CORPUS_BINDING_NATIVE_UNVERIFIED','CORPUS_SELECTED_SOURCE_CHANGED',
        'CORPUS_PREPARATION_SOURCE_OR_ACCESS_CHANGED','CORPUS_PREPARATION_CAPTURE_EXPIRED'}
    if embedding is True or measured_retrieval is True:
        from src.application.campaign_operator_worker import EMBEDDING_VALIDATION_CODES
        allowed.update(EMBEDDING_VALIDATION_CODES)
    if measured_retrieval is True and type(code) is str and re.fullmatch(
            r'(?:PRIVATE_RETRIEVAL|PRIVATE_MEASUREMENT|CORPUS_RETRIEVAL)_[A-Z_]{1,80}',code):allowed.add(code)
    safe=code if type(code) is str and (code in allowed or re.fullmatch(
        r'OPERATOR_FAILURE_(?:TYPE|ATTRIBUTE|KEY|FILE|IMPORT|DEPENDENCY)_(?:L[0-9]+|UNKNOWN)',code)) else 'RUNTIME_UNAVAILABLE'
    frames=traceback.extract_tb(error.__traceback__)
    frame=next((f for f in reversed(frames) if '/src/' in f.filename),None)
    logging.getLogger(__name__).warning(json.dumps(dict(event='marketing_planner_delivery_failed',
        action='measured_retrieval' if measured_retrieval is True else 'embedding' if embedding is True else 'corpus' if corpus is True else 'retrieval' if retrieval is True else 'planner',
        code=safe,error_type=type(error).__name__,source_line=frame.lineno if frame else None)))
    if measured_retrieval is True:
        st.error('The retrieval measurement could not be verified. Reload this page to refresh your signed-in session, then choose Recover original retrieval measurement. Do not repeat Run after an uncertain response. Recovery makes no model calls.')
    elif embedding is True:
        st.error('The original embedding/index operation could not be verified. Reload this page to refresh your signed-in session, then choose Recover original text embeddings and index. Do not repeat Run after an uncertain response.')
    elif corpus is True:
        if safe in {'CORPUS_EXTRACTION_REQUIRED','CORPUS_BINDING_NOT_VERIFIED'}:
            st.error('A complete saved corpus binding could not be recovered. Prepare selected retrieval corpus can resume the selected files and reuse saved extractions. Reload this page before resuming. No model call is made by this action.')
        else:
            st.error('Corpus preparation could not be verified. Reload this page, then click Recover saved retrieval corpus to check the original saved binding. No model call is made by this action.')
    elif retrieval is True:
        st.error('The corpus measurement receipt could not be verified. Reload this page, then click Recover the original retrieval corpus measurement. Do not repeat Measure. No model call is made by this action.')
    elif safe=='PLANNER_SESSION_REFRESH_REQUIRED':
        st.error('Reload this page to refresh your signed-in session before starting the evaluation. No model was dispatched by this denied attempt.')
    else:
        st.error('The evaluation receipt could not be delivered. Records may already be saved. Reload this page, then recover the original planner evaluation records. Do not click Run again.')
    st.caption('Diagnostic reference: '+safe)

@st.cache_resource
def pilot():
    path=Path(os.environ['CI_CAMPAIGN_SERVING_CONFIG'])
    if not path.is_absolute() or path.stat().st_size>65536:raise ValueError('MARKETING_CONFIG_INVALID')
    manifest=json.loads(path.read_text());key=base64.b64decode(os.environ['CI_CAMPAIGN_HISTORY_CURSOR_KEY_B64'],validate=True)
    return MarketingPilot(client=bigquery.Client(project=PROJECT,location=LOCATION),manifest=manifest,cursor_key=key)


def main():
    try:
        context=st.query_params.get('campaign',DEFAULT_CONVERSATION)
        if context not in (DEFAULT_CONVERSATION,EVALUATION_CONVERSATION):raise ValueError('MARKETING_CONVERSATION_CONTEXT')
        # Never reinterpret a retained submission under another conversation.
        marker='_ci_marketing_conversation_context'
        if marker not in st.session_state:
            st.session_state[marker]=(DEFAULT_CONVERSATION if st.session_state.get('protected_pending_turn') is not None else context)
        if st.session_state[marker]!=context:
            st.info('Open the evaluation conversation in a new tab. This tab retains its original request.')
            st.link_button('Open requested conversation in a new tab','/?campaign='+context)
            return
        current=pilot();headers=iap_headers(st.context.headers)
        actor,_,member,conversation,owned_conversation=current.inspect(headers,context=context)
    except Exception:
        st.title('Creative Intelligence')
        st.error('Protected marketing access is unavailable. Sign in with an authorized pilot account.')
        st.chat_input('Authorized access required',disabled=True);return
    if 'CREATOR' not in member['roles'] or conversation is None:
        st.title('Creative Intelligence')
        st.success('Google identity and workspace membership verified')
        st.info('Generation and generated-output release are disabled during pilot verification.')
        if 'CREATOR' not in member['roles']:
            st.write('Campaign creation requires CREATOR access. ADMIN access is separate.')
            if 'ADMIN' in member['roles'] and st.button('Enable my campaign creator access'):
                try:st.write(current.setup(headers,'CREATOR',context=context));st.rerun()
                except Exception:st.error('Creator access could not be confirmed. Refresh to check stored state.')
        elif st.button('Create my private Rejuvenate evaluation conversation' if context==EVALUATION_CONVERSATION else 'Create my private campaign conversation'):
            try:st.write(current.setup(headers,'CONVERSATION',context=context));st.rerun()
            except Exception:st.error('Conversation setup could not be confirmed. Refresh to check stored state.')
        return
    if (context==EVALUATION_CONVERSATION
            and os.environ.get('CI_MARKETING_PLANNER_EVALUATION_ENABLED')=='true'
            and st.sidebar.checkbox('Private planner evaluation',value=False)):
        try:
            from types import SimpleNamespace
            from src.ui.campaign_operator_console import render
            render(st,headers,actor,SimpleNamespace(iap=current.verifier.settings),
                planner_scope=(current.manifest['workspace_id'],owned_conversation))
        except Exception as error:
            show_planner_failure(st,error)
        return
    try:
        if st.sidebar.button('Reconnect protected services'):
            current.reconnect(headers,context=context)
        runtime=current.runtime(headers,context=context)
    except Exception:
        st.error('Protected services are unavailable. Reconnect after restoring access.');return
    if context==DEFAULT_CONVERSATION:
        st.sidebar.link_button('Open Rejuvenate evaluation conversation','/?campaign='+EVALUATION_CONVERSATION)
    else:
        st.sidebar.info('Separate Rejuvenate evaluation conversation. Original requests and uploads remain in the original conversation.')
        st.sidebar.link_button('Open original campaign conversation','/')
    from src.ui.campaign_chat import main as chat
    chat(runtime_override=runtime)
