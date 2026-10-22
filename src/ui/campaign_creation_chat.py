"""Product selection and campaign conversation using a protected backend only."""
import hashlib
from uuid import uuid4
from src.application.campaign_creation import CampaignProduct,CampaignJob,CampaignImage
from src.ui.workflow_runtime import WorkflowUiError

KEY='protected_campaign_creation_v1'
REASON_TEXT={
    'SOURCE_PREPARATION_REQUIRED':'The saved source material is still being prepared.',
    'PRODUCT_IMAGE_REQUIRED':'A usable product image is needed.',
    'IMAGE_BINDING_REQUIRED':'The product image still needs protected storage and binding.',
    'IMAGE_USE_REVIEW_REQUIRED':'Product-image use needs review.',
    'CLAIM_REVIEW_REQUIRED':'The requested claims need scoped approval.',
    'BRAND_REVIEW_REQUIRED':'Applicable brand guidance needs review.',
    'PROMPT_POLICY_REVIEW_REQUIRED':'The campaign prompt and policy need evaluation and approval.',
    'PIPELINE_NOT_INSTALLED':'The campaign execution service is not installed yet.',
    'ARTIFACT_CHECKS_REQUIRED':'The banner is awaiting validation.',
    'SOURCE_UNAVAILABLE':'Current source evidence could not be verified.',
    'UNSUPPORTED_PRODUCT_SCOPE':'This product does not yet support the selected campaign scope.',
}
STAGE_TEXT=dict(QUEUED='Campaign accepted',CHECKING_EVIDENCE='Checking product evidence',
    RETRIEVING='Selecting relevant grounding',PLANNING='Planning the campaign',
    RENDERING='Rendering the banner',VALIDATING='Validating the banner',COMPLETE='Campaign execution complete',STOPPED='Campaign stopped')


def _retain_brief(state,job,pending):
    messages=state.setdefault('messages',{})
    messages[job.job_id]=dict(sku=pending['sku'],message=pending['message'])
    while len(messages)>50:del messages[next(iter(messages))]


def _reasons(st,reasons):
    for reason in reasons:st.info(REASON_TEXT[reason])


def _recover(st,runtime,state):
    pending=state['pending']
    if pending is None:return
    st.warning('The original campaign submission is retained. Check it before sending another request.')
    st.text(pending['message'])
    if st.button('Check original campaign request',key='campaign_recover_original'):
        try:
            job=runtime.campaign_recover(st.context.headers,**pending)
            if job is None or type(job) is not CampaignJob or job.state=='UNKNOWN':
                st.warning('The original submission is not confirmed yet. No new generation was requested.')
            elif job.sku!=pending['sku'] or job.message_sha256!=hashlib.sha256(pending['message'].encode()).hexdigest():
                raise WorkflowUiError('CAMPAIGN_ADMISSION_UNVERIFIED')
            else:
                _retain_brief(state,job,pending)
                state['pending']=None;st.rerun()
        except WorkflowUiError:
            st.error('The original request could not be verified. Reconnect and check it again.')


def _jobs(st,runtime,state=None):
    try:
        jobs=runtime.campaign_jobs(st.context.headers)
        if type(jobs) is not tuple or len(jobs)>50 or any(type(j) is not CampaignJob for j in jobs):
            raise WorkflowUiError('CAMPAIGN_HISTORY_UNVERIFIED')
        if len({j.job_id for j in jobs})!=len(jobs):raise WorkflowUiError('CAMPAIGN_HISTORY_UNVERIFIED')
    except WorkflowUiError:
        st.info('Saved campaign progress could not be verified. Reconnect to check it.')
        return
    if not jobs:return
    st.button('Refresh campaign progress',key='campaign_refresh_progress')
    for job in jobs:
        brief=(state or {}).get('messages',{}).get(job.job_id)
        if (brief and brief['sku']==job.sku
                and hashlib.sha256(brief['message'].encode()).hexdigest()==job.message_sha256):
            with st.chat_message('user'):st.text(brief['message'])
        with st.chat_message('assistant'):
            st.text(STAGE_TEXT[job.stage]);st.caption(f'{job.sku} · {job.state} · {job.job_id}')
            _reasons(st,job.reasons)
            if job.state!='SUCCEEDED':continue
            try:
                artifact=runtime.campaign_artifact(st.context.headers,job_id=job.job_id)
                if type(artifact) is not CampaignImage or artifact.job_id!=job.job_id or artifact.sku!=job.sku:
                    raise WorkflowUiError('CAMPAIGN_ARTIFACT_UNVERIFIED')
                st.text('Your campaign draft is ready')
                st.image(artifact.png,caption=f'{artifact.width} × {artifact.height} · campaign draft',width='stretch')
                with st.expander('Grounding and validation'):st.text(artifact.evidence_summary)
                download=runtime.campaign_deferred_download(st.context.headers,job_id=job.job_id,
                    sku=job.sku,content_sha256=artifact.content_sha256)
                st.download_button('Download banner',download,file_name=f'{job.sku}-campaign.png',
                    mime='image/png',key='campaign_download_'+job.job_id,on_click='ignore')
            except WorkflowUiError:
                st.warning('The saved campaign image is withheld until current access and artifact checks can be verified.')


def render_campaign_creation(st,runtime):
    scope=(runtime.settings.workspace_id,runtime.settings.conversation_id)
    state=st.session_state.get(KEY)
    if state is None:
        state=dict(scope=scope,pending=None,messages={});st.session_state[KEY]=state
    if type(state) is not dict or state.get('scope')!=scope:
        st.info('Open this campaign conversation in a separate tab to preserve the original request.')
        st.chat_input('Campaign conversation changed',disabled=True);return
    _recover(st,runtime,state)
    _jobs(st,runtime,state)
    try:
        products=runtime.campaign_products(st.context.headers)
        if (type(products) is not tuple or len(products)>1000
                or any(type(p) is not CampaignProduct for p in products)
                or len({p.sku for p in products})!=len(products)):
            raise WorkflowUiError('CAMPAIGN_CATALOG_UNVERIFIED')
    except WorkflowUiError:
        st.info('Current product readiness could not be verified. Reconnect to try again.')
        st.chat_input('Product verification required',disabled=True);return
    if not products:
        st.info('No products are currently configured for this campaign workspace.')
        st.chat_input('Product grounding required',disabled=True);return
    by_id={p.sku:p for p in products}
    sku=st.selectbox('Product for your campaign',list(by_id),index=0 if len(products)==1 else None,
        format_func=lambda value:f'{value} · {by_id[value].product_name}',
        key='campaign_creation_product',disabled=state['pending'] is not None)
    product=by_id.get(sku)
    if product:
        st.caption(f'{product.country} · {product.channel} · {product.audience} · {product.state}')
        _reasons(st,product.reasons)
    ready=product is not None and product.state=='READY' and state['pending'] is None
    message=st.chat_input('Describe the campaign you want to create…',max_chars=2000,disabled=not ready)
    if not message:return
    pending=dict(sku=product.sku,message=message,retry_key=uuid4().hex)
    state['pending']=pending
    with st.chat_message('user'):st.text(message)
    try:
        job=runtime.campaign_submit(st.context.headers,**pending)
        if (type(job) is not CampaignJob or job.sku!=product.sku
                or job.message_sha256!=hashlib.sha256(message.encode()).hexdigest()):
            raise WorkflowUiError('CAMPAIGN_ADMISSION_UNVERIFIED')
        if job.state!='UNKNOWN':
            _retain_brief(state,job,pending)
            state['pending']=None
    except WorkflowUiError:
        st.error('Submission could not be confirmed. Check the retained original request before trying again.')
    st.rerun()
