"""Thin Streamlit chat UI; all factual generation is delegated to cloud readers."""
from __future__ import annotations

import json
import logging
import uuid

import streamlit as st

from src.application.campaign_chat import (
    FIELDS, CampaignChatService, CampaignResult, Interpretation, load_settings, safe_error,
)
from src.retrieval.campaign_performance import json_value
from src.retrieval.request import CHANNELS, OBJECTIVES, VISUAL_REFERENCES
from src.ui.workflow_session import (
    WorkflowSessionState, durable_ui_enabled, empty_workflow_session, workflow_rows,
)
from src.ui.workflow_runtime import (
    WorkflowUiError, WorkflowUiJobDetails, get_workflow_runtime,
)

LABELS = dict(canonical_sku='Product', objective='Objective', audience='Audience',
              channel='Channel', visual_reference='Visual inspiration')
NAMES = dict(AWARENESS='Awareness', CONSIDERATION='Consideration', CONVERSION='Conversion',
    PAID_SOCIAL='Paid social', PAID_SEARCH='Paid search', EMAIL='Email',
    NONE='No website reference', NAKED_AND_THRIVING='Naked & Thriving styling')
FEATURES = dict(BENEFIT_CLAIMS='benefit claims or outcome promises', DISCOUNTS='offers or discounts',
    CUSTOM_COPY='custom campaign copy', CUSTOM_LAYOUT='specific layout, color or font instructions',
    CUSTOM_SIZE='custom banner dimensions', OTHER_TASK='tasks outside campaign preview generation',
    OTHER_CONSTRAINT='other unsupported requirements')
STAGE_LABELS = dict(RETRIEVING_EVIDENCE='Retrieving product, rules, analytics and original images',
    PLANNING='Planning a grounded campaign with Gemini', RENDERING='Composing the banner',
    VERIFYING='Checking current evidence and approvals')


def initialize() -> None:
    defaults = dict(session_id=uuid.uuid4().hex, messages=[], catalog=None, catalog_error=None,
                    draft=dict.fromkeys(FIELDS, ''), pending=None, turns=0, results={}, busy=False,
                    unsupported_features=(), workflow_state=empty_workflow_session(),
                    workflow_error=None, workflow_refresh=False, workflow_details={})
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def show_durable_workflow_state(runtime=None) -> None:
    if not durable_ui_enabled():
        return
    state = st.session_state.workflow_state
    if type(state) is not WorkflowSessionState:
        st.error('Durable workflow state is unavailable. Start a new conversation.')
        return
    st.caption('Durable workflow mode · protected runtime')
    if st.session_state.workflow_error:
        st.error('Protected workflow access is unavailable. Refresh after the service connection is restored.')
        return
    rows = workflow_rows(state)
    if state.workspace_id is None:
        st.info('Protected workflow runtime composition is not active in this UI process.')
        return
    if runtime is None:
        st.info('Protected workflow runtime composition is not active in this UI process.')
        return
    with st.expander(f'{len(rows)} durable workflow jobs'):
        for row in rows:
            details = st.session_state.workflow_details.get(row['job_id'])
            if details is not None and type(details) is not WorkflowUiJobDetails:
                st.session_state.workflow_error = 'INVALID_UI_WORKFLOW_STATE'
                st.error('Protected workflow state is unavailable. Start a new conversation.')
                return
            status = details.status.state if details else row['state'] or 'COMMIT STATUS UNKNOWN'
            st.markdown(f'**{row["job_id"]}** · {status}')
            if st.button('Refresh job status', key=f'status_{row["job_id"]}',
                    disabled=st.session_state.busy):
                try:
                    st.session_state.workflow_details[row['job_id']] = runtime.job_details(
                        st.context.headers, job_id=row['job_id'])
                except WorkflowUiError as error:
                    st.session_state.workflow_error = error.code
                st.rerun()
            if details and details.output:
                output = details.output
                st.caption(f'{output.canonical_sku} · Synthetic POC draft · {output.initial_review_status}')
                labels = dict(BANNER_PNG='Download banner', BRIEF_JSON='Download metadata',
                              BRIEF_TEXT='Download campaign brief')
                for artifact in output.downloads:
                    try:
                        fetch = runtime.deferred_download(st.context.headers,
                            job_id=row['job_id'], artifact=artifact)
                        st.download_button(labels[artifact.kind], fetch,
                            file_name=artifact.filename, mime=artifact.content_type,
                            key=f'download_{row["job_id"]}_{artifact.artifact_id}',
                            on_click='ignore')
                    except WorkflowUiError as error:
                        st.session_state.workflow_error = error.code
                        st.error('Protected download access is unavailable.')
                        return
    if state.next_cursor and runtime is not None and st.button(
            'Load older workflow jobs', disabled=st.session_state.busy):
        try:
            st.session_state.workflow_state = runtime.load_history(
                state, st.context.headers, cursor=state.next_cursor)
        except WorkflowUiError as error:
            st.session_state.workflow_error = error.code
        st.rerun()


def show_result(result: CampaignResult, turn: str) -> None:
    envelope = result.envelope
    if envelope['status'] != 'SUCCEEDED':
        st.error(envelope['error']['message'])
        st.caption(f'Request {envelope["request_id"][:8]} · {envelope["error"]["code"]}')
        return
    st.markdown('**Your campaign preview is ready**')
    st.image(result.png, caption='1200 × 628 · original product image preserved', width='stretch')
    st.caption('Synthetic POC draft · awaiting human review')
    brief = envelope['brief']
    st.text(brief['copy']['headline'])
    if brief['copy']['body']:
        st.text(brief['copy']['body'])
    st.caption(f'CTA: {brief["copy"]["cta"]} · {NAMES[brief["request"]["channel"]]}')
    with st.expander('Campaign brief and evidence'):
        st.text(result.brief_text)
    with st.expander('Generation details'):
        st.json(dict(request_id=envelope['request_id'], model=brief['model'],
            snapshot_id=brief['snapshot_id'], guardrails=envelope['guardrails'],
            duration_seconds=envelope['duration_seconds'], usage=envelope['usage'], cost=envelope['cost']))
        st.caption('Available usage is shown for request interpretation and planning. Embedding usage and billed cost are not yet tracked.')
    left, right = st.columns(2)
    left.download_button('Download banner', result.png, f'{envelope["canonical_sku"]}-preview.png',
                         'image/png', key=f'png_{turn}', on_click='ignore', width='stretch')
    right.download_button('Download brief', result.brief_text, f'{envelope["canonical_sku"]}-brief.txt',
                          'text/plain', key=f'brief_{turn}', on_click='ignore', width='stretch')
    st.download_button('Download evidence and metadata', json.dumps(envelope, indent=2, default=json_value),
                       f'{envelope["canonical_sku"]}-metadata.json', 'application/json',
                       key=f'json_{turn}', on_click='ignore')


def request_editor(service: CampaignChatService, catalog: list[dict], runtime=None) -> None:
    pending = st.session_state.pending
    if not pending:
        return
    interpretation = pending['interpretation']
    if interpretation.unsupported_features:
        st.warning('This preview supports product identity and neutral shopping copy. Please revise your request to remove: '
                   +', '.join(FEATURES[v] for v in interpretation.unsupported_features)+'.')
        return
    if interpretation.missing:
        st.info('Complete these campaign details: '+', '.join(LABELS[k].lower() for k in interpretation.missing)+'.')
    else:
        st.caption('Review the campaign details, then generate your preview.')
    draft = interpretation.preferences
    product_names = {r['sku']: f'{r["sku"]} · {r["product_name"]}' for r in catalog}
    options = dict(canonical_sku=['', *product_names], objective=['', *sorted(OBJECTIVES)],
                   channel=['', *sorted(CHANNELS)], visual_reference=['', *sorted(VISUAL_REFERENCES)])
    values = {}
    with st.form(f'campaign_details_{pending["turn"]}'):
        values['canonical_sku'] = st.selectbox('Product', options['canonical_sku'],
            index=options['canonical_sku'].index(draft['canonical_sku']),
            format_func=lambda value: product_names.get(value, 'Choose a product'))
        left, right = st.columns(2)
        for container, key in ((left, 'objective'), (right, 'channel')):
            values[key] = container.selectbox(LABELS[key], options[key], index=options[key].index(draft[key]),
                format_func=lambda value: NAMES.get(value, 'Choose '+LABELS[key].lower()))
        values['audience'] = st.text_input('Audience', draft['audience'], max_chars=load_settings()['max_audience_characters'],
                                         placeholder='Who is this campaign for?')
        values['visual_reference'] = st.selectbox('Visual inspiration', options['visual_reference'],
            index=options['visual_reference'].index(draft['visual_reference']),
            format_func=lambda value: NAMES.get(value, 'Choose visual inspiration'))
        generate = st.form_submit_button('Generate preview', type='primary', disabled=st.session_state.busy,
                                         width='stretch')
    if not generate:
        return
    adjusted = Interpretation(values, model=interpretation.model)
    if adjusted.missing:
        st.warning('Choose '+', '.join(LABELS[k].lower() for k in adjusted.missing)+' before generating.')
        return
    request = adjusted.request()
    request_id = pending['request_id']
    # Guard the expensive action by request identity, including automatic UI reruns.
    if request_id in st.session_state.results:
        st.session_state.pending = None
        st.rerun()
    st.session_state.busy = True
    status = st.status('Submitting campaign workflow' if runtime else 'Generating campaign preview',
                       expanded=True)
    try:
        st.session_state.draft = request.to_dict()
        if runtime:
            submitted = runtime.submit(
                st.session_state.workflow_state, st.context.headers,
                preferences=request.to_dict(), retry_key=request_id)
            st.session_state.workflow_state = submitted.state
            status_text = submitted.receipt.job_state or 'COMMIT STATUS UNKNOWN'
            st.session_state.messages.append(dict(role='assistant',
                content=f'Campaign workflow {submitted.receipt.job_id} · {status_text}'))
            status.update(label='Campaign workflow receipt recorded', state='complete', expanded=False)
        else:
            result = service.generate(request, st.session_state.session_id, request_id,
                on_event=lambda event: status.write(STAGE_LABELS[event['stage']]))
            if result.envelope['status'] == 'SUCCEEDED':
                result.envelope['usage']['interpreter'] = interpretation.model.get('usage')
                result.envelope['interpretation_model'] = interpretation.model
            st.session_state.results[request_id] = result
            st.session_state.messages.append(dict(role='assistant', result=result, turn=request_id))
            status.update(label='Preview ready' if result.envelope['status'] == 'SUCCEEDED' else 'Generation stopped',
                          state='complete' if result.envelope['status'] == 'SUCCEEDED' else 'error', expanded=False)
        st.session_state.pending = None
    except WorkflowUiError as error:
        st.session_state.workflow_error = error.code
        st.session_state.messages.append(dict(role='assistant',
            content='The protected workflow could not be completed. Refresh after the service connection is restored.'))
        st.session_state.pending = None
        status.update(label='Workflow unavailable', state='error', expanded=False)
    except Exception as error:
        st.session_state.messages.append(dict(role='assistant', content=safe_error(error).message))
        st.session_state.pending = None
        status.update(label='Generation stopped', state='error', expanded=False)
    finally:
        st.session_state.busy = False
    st.rerun()


def protected_intake(runtime,service):
    """Persist explicit initial context; never call a local model or release questions."""
    from src.application.campaign_quality_authority import COUNTRIES,CHANNELS
    from src.ui.campaign_attachments import show_protected_attachments
    show_protected_attachments(runtime)
    if runtime.interpretation_available is not True:
        st.chat_input('Campaign service verification required',disabled=True)
        return
    st.info('Requests can be saved. Generated questions and campaigns await evaluation and policy approval.')
    try:
        catalog=service.reference_catalog()
    except Exception:
        st.error('Current product catalog unavailable. Refresh to try again.')
        return
    names={r['sku']:r['product_name'] for r in catalog}
    sku=st.selectbox('Product for this request',list(names),index=None,
        format_func=lambda k:f'{k} · {names[k]}')
    country=st.selectbox('Campaign market',sorted(COUNTRIES),index=None)
    channel=st.selectbox('Campaign platform',sorted(CHANNELS),index=None)
    from src.application.interpreter_submission import InterpreterIntakeOptions
    try:
        options=runtime.intake_options(st.context.headers)
        if type(options) is not InterpreterIntakeOptions:raise WorkflowUiError('INTAKE_OPTIONS_UNAVAILABLE')
    except WorkflowUiError:
        st.info('Current audience options could not be verified. Refresh to try again.')
        return
    audience=st.selectbox('Customer audience',list(options.audiences),index=None)
    st.caption('Use the message to describe additional audience details. Customer types follow the registered prompt.')
    if 'protected_pending_turn' not in st.session_state:
        st.session_state.protected_pending_turn=None
    pending=st.session_state.protected_pending_turn
    if pending:
        receipt=st.session_state.get('protected_submission_receipt')
        if receipt is not None and receipt.native_status=='COMMITTED':
            st.success('Request and campaign context saved. Generated output remains under review.')
            st.caption('Request '+receipt.request_id)
            if st.button('Continue conversation'):
                st.session_state.protected_pending_turn=None
                st.session_state.protected_submission_receipt=None
                st.rerun()
        st.caption('A submission is retained for exact retry. Its message and selections cannot change.')
        if st.button('Check original submission'):
            _submit_protected_turn(runtime,pending)
        return
    message=st.chat_input('Describe your campaign…',max_chars=2000,
        disabled=not(sku and country and channel and audience))
    if message:
        pending=dict(message=message,selection=dict(sku=sku,country=country,channel=channel,audience=audience),retry_key=uuid.uuid4().hex)
        st.session_state.protected_pending_turn=pending
        _submit_protected_turn(runtime,pending)
        st.rerun()


def _submit_protected_turn(runtime,pending):
    try:
        receipt=runtime.interpret(st.context.headers,**pending)
        st.session_state.protected_submission_receipt=receipt
        if receipt.native_status=='COMMITTED':
            st.success('Request and campaign context saved. Generated output remains under review.')
            st.caption('Request '+receipt.request_id)
        else:
            st.warning('Submission is not confirmed. Check the original submission before sending another request.')
    except WorkflowUiError:
        st.error('Submission could not be verified. Check the original submission after restoring access.')


def main(runtime_override=None) -> None:
    st.set_page_config(page_title='Creative Intelligence', page_icon='✨', layout='centered')
    initialize()
    service = CampaignChatService()
    with st.sidebar:
        st.markdown('### Creative Intelligence')
        st.caption('Campaign studio · local POC')
        if st.button('New conversation', disabled=st.session_state.busy, width='stretch'):
            for key in list(st.session_state):
                del st.session_state[key]
            st.rerun()
        if st.button('Refresh product catalog', disabled=st.session_state.busy, width='stretch'):
            st.session_state.catalog = None
            st.session_state.catalog_error = None
        if durable_ui_enabled() and st.button('Refresh workflow history',
                disabled=st.session_state.busy, width='stretch'):
            st.session_state.workflow_refresh = True
            st.session_state.workflow_error = None
            st.session_state.workflow_details = {}
        st.divider()
        st.markdown('**Create a campaign**')
        st.write('Choose a product and describe the campaign you want to create.')
        st.caption('Benefit claims and discounts are unavailable in this POC.')
        if durable_ui_enabled():
            st.caption('Durable history requires the protected workflow runtime. Drafts are not automatically approved or published.')
        else:
            st.caption('Conversation history lasts for this session. Drafts are not automatically approved or published.')
    st.caption('CREATIVE INTELLIGENCE / CAMPAIGN STUDIO')
    st.title('From product to campaign')
    st.write('Describe your campaign. We’ll ground the preview in your product, brand rules and creative references.')
    st.caption('Synthetic POC · campaign performance is simulated · visual references are inspiration only')
    runtime = None
    if durable_ui_enabled():
        runtime = runtime_override if runtime_override is not None else get_workflow_runtime()
        if runtime is not None and runtime.campaign_creation_available is True:
            from src.ui.campaign_creation_chat import render_campaign_creation
            render_campaign_creation(st,runtime)
            return
        if runtime is not None and (not st.session_state.workflow_state.history_loaded
                or st.session_state.workflow_refresh):
            try:
                st.session_state.workflow_state = runtime.load_history(
                    st.session_state.workflow_state, st.context.headers)
                st.session_state.workflow_error = None
            except WorkflowUiError as error:
                st.session_state.workflow_error = error.code
            finally:
                st.session_state.workflow_refresh = False
        show_durable_workflow_state(runtime)
        if runtime is None or st.session_state.workflow_error:
            st.chat_input('Protected workflow runtime connection required', disabled=True)
            return
        from src.ui.question_chat import render_questions
        try:
            turn = runtime.question_turn(st.context.headers)
            if turn is not None:
                render_questions(st, turn, workspace_id=runtime.settings.workspace_id,
                    conversation_id=runtime.settings.conversation_id)
        except WorkflowUiError:
            st.info('Evidence questions are unavailable. Refresh after service verification.')
        try:
            readiness = runtime.evidence_readiness(st.context.headers)
        except WorkflowUiError:
            st.info('Campaign chat is awaiting protected evidence and eligibility verification.')
            protected_intake(runtime,service)
            return
        if not readiness.release_allowed:
            st.info('Campaign chat is awaiting prompt evaluation, policy review and response validation.')
            protected_intake(runtime,service)
            return
        protected_intake(runtime,service)
        return
    if st.session_state.catalog is None and st.session_state.catalog_error is None:
        try:
            with st.spinner('Connecting to your product catalog…'):
                st.session_state.catalog = service.catalog()
        except Exception as error:
            st.session_state.catalog_error = safe_error(error).message
            logging.getLogger(__name__).warning('Catalog unavailable: %s', type(error).__name__)
    catalog = st.session_state.catalog
    if not catalog:
        st.error(st.session_state.catalog_error or 'Product catalog unavailable.')
        st.info('Restore the cloud connection, then refresh the product catalog.')
        st.chat_input('Connect your product catalog to begin', disabled=True)
        return
    with st.sidebar.expander(f'{len(catalog)} products connected'):
        for row in catalog:
            st.text(f'{row["sku"]} · {row["product_name"]}')
    if not st.session_state.messages:
        with st.chat_message('assistant', avatar='✨'):
            st.write('Tell me which product you’d like to feature and who the campaign is for.')
            st.caption('Try: Create a conversion campaign for CI-SKU-0001 on paid social for skincare shoppers, '
                       'using Naked & Thriving styling.')
    for message in st.session_state.messages:
        with st.chat_message(message['role'], avatar='✨' if message['role'] == 'assistant' else None):
            if 'result' in message:
                show_result(message['result'], message['turn'])
            else:
                st.text(message['content'])
    request_editor(service, catalog, runtime)
    prompt = st.chat_input('Describe your campaign or update its preferences…',
                           max_chars=load_settings()['max_message_characters'], disabled=st.session_state.busy)
    if prompt:
        st.session_state.messages.append(dict(role='user', content=prompt))
        st.session_state.pending = None
        st.session_state.turns += 1
        with st.spinner('Understanding your campaign request…'):
            try:
                interpretation = service.interpret(prompt, st.session_state.draft, st.session_state.unsupported_features)
                st.session_state.catalog = service.catalog()
                st.session_state.draft = dict(interpretation.preferences)
                st.session_state.unsupported_features = interpretation.unsupported_features
                st.session_state.pending = dict(interpretation=interpretation,
                    turn=st.session_state.turns, request_id=uuid.uuid4().hex)
            except Exception as error:
                st.session_state.messages.append(dict(role='assistant', content=safe_error(error).message))
        st.rerun()


if __name__ == '__main__':
    main()
