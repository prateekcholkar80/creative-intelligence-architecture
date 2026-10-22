"""Evidence collection controls; no local fallback, parsing or claim approval."""
import json
import streamlit as st
from src.application.campaign_attachment_contract import AttachmentRequest
from src.application.campaign_attachment_service import AttachmentReceipt,MAX_BYTES,inspect_file
from src.ui.workflow_runtime import WorkflowUiError

LABELS=dict(PRODUCT_IMAGE='Product image',REFERENCE_DATA='Reference data',
    PLAYBOOK='Playbook',SUPPORTING_DOCUMENT='Supporting document')


@st.fragment
def show_protected_attachments(runtime):
    """Rerun upload controls alone; service calls still recheck current authority."""
    show_attachment_readiness(runtime)
    show_saved_attachment_requests(runtime)


def show_attachment_readiness(runtime):
    if runtime.attachments_available is not True:return
    st.caption('Protected supporting-material intake configured · originals remain unverified')
    if st.button('Verify protected upload intake'):
        try:
            if runtime.verify_attachment_intake(st.context.headers) is not True:
                raise WorkflowUiError('ATTACHMENT_STORAGE_UNAVAILABLE')
            st.success('Protected upload storage and current creator access verified. Generation remains disabled.')
        except WorkflowUiError:
            st.error('Protected upload storage could not be verified. Reconnect before retrying.')


def show_saved_attachment_requests(runtime):
    if runtime.attachments_available is not True:return
    try:
        rows=runtime.attachment_saved_requests(st.context.headers)
        if type(rows) is not tuple or len(rows)>20:raise WorkflowUiError('ATTACHMENT_HISTORY_UNAVAILABLE')
        if not rows:return
        by_id={r['request_id']:r for r in rows}
        selected=st.selectbox('Saved request for supporting material',list(by_id),index=None,
            format_func=lambda key:by_id[key]['sku']+' · '+by_id[key]['created_at'],key='attachment_saved_request')
        if not selected:return
        summaries=runtime.attachment_summaries(st.context.headers,request_id=selected)
        if type(summaries) is not tuple or len(summaries)>20:raise WorkflowUiError('ATTACHMENT_HISTORY_UNAVAILABLE')
        with st.expander(f'{len(summaries)} recent supporting-material submissions'):
            for row in summaries:
                if row['generation_authorized'] is not False:raise WorkflowUiError('ATTACHMENT_HISTORY_UNAVAILABLE')
                st.text((row['filename'] or 'Omission request')+' · '+row['status'])
                st.caption('Submission '+row['submission_id'])
                from src.ui.campaign_document_controls import show_processing_controls
                show_processing_controls(runtime,row)
        show_attachment_requests(runtime,selected)
    except WorkflowUiError:
        st.info('Saved supporting-material history is unavailable. Reconnect to refresh access.')


def show_attachment_batches(runtime,request_id):
    """Recover progress from BigQuery, never from a browser-only file queue."""
    selected=None
    try:
        batches=runtime.attachment_batches(st.context.headers,request_id=request_id)
        if type(batches) is not tuple or len(batches)>20:raise WorkflowUiError('ATTACHMENT_BATCH_UNVERIFIED')
        if not batches:return False
        by_id={b['batch_id']:b for b in batches}
        selector_key='attachment_batch_'+request_id
        focus=st.session_state.pop('attachment_batch_focus_'+request_id,None)
        if focus in by_id:st.session_state[selector_key]=focus
        selected=st.selectbox('Upload batch to inspect or resume',list(by_id),
            format_func=lambda bid:f"{by_id[bid]['count']} files · {bid[-12:]}",key=selector_key)
        if selected not in by_id:return False
        state=runtime.attachment_batch_state(st.context.headers,batch_id=selected)
        if type(state) is not tuple or not state or any(type(r['saved']) is not bool for r in state):
            raise WorkflowUiError('ATTACHMENT_BATCH_UNVERIFIED')
        saved=sum(r['saved'] for r in state)
        st.caption(f'Upload batch: {saved} of {len(state)} files saved. Pending originals are stored privately.')
        for row in state:st.text(row['filename']+' · '+('Saved' if row['saved'] else 'Pending'))
        running_key='attachment_batch_running_'+selected
        if saved==len(state):
            st.session_state[running_key]=False
            st.success('All files in this batch are saved.')
            images=[r for r in state if r['filename'].lower().endswith(('.png','.jpg','.jpeg','.webp'))]
            image_by_id={r['submission_id']:r for r in images}
            image_id=None
            if images:
                image_id=st.selectbox('Product image for pipeline evaluation',list(image_by_id),
                    format_func=lambda sid:image_by_id[sid]['filename'],index=None,
                    placeholder='Choose the product photo to evaluate',key='eval_image_'+selected)
                st.caption('Choose a photo showing the product. Ingredient, texture and review screenshots are supporting references.')
            if images and st.button('Bind selected image for pipeline evaluation',key='eval_bind_'+selected,
                    disabled=image_id not in image_by_id) and image_id in image_by_id:
                try:
                    # Start with one bounded source; no approval or model execution.
                    receipt=runtime.bind_evaluation_uploads(st.context.headers,request_id=request_id,
                        submission_ids=(image_id,))
                    if type(receipt) is not dict or receipt.get('generation_authorized') is not False:
                        raise WorkflowUiError('EVALUATION_UPLOAD_BINDING_UNVERIFIED')
                    st.json(receipt)
                    st.caption('Evaluation input binding saved. Claims remain subject to review; no model was called.')
                except WorkflowUiError:
                    st.error('Evaluation binding could not be verified. Reconnect before checking the same source again.')
            return False
        if st.button('Resume batch',key='resume_'+selected):st.session_state[running_key]=True
        if st.button('Pause batch',key='pause_'+selected):st.session_state[running_key]=False
        if st.session_state.get(running_key) is True:
            st.caption('Saving the next pending file. You can reconnect and resume this batch if interrupted.')
            runtime.advance_attachment_batch(st.context.headers,batch_id=selected)
            st.rerun()
            return True
        return False
    except WorkflowUiError:
        if selected is not None:st.session_state['attachment_batch_running_'+selected]=False
        st.error('Batch progress could not be verified. Reconnect and inspect this batch before resuming. No automatic retry was made.')
        return True


def show_attachment_requests(runtime,request_id):
    if runtime.attachments_available is not True:return
    try:
        requests=runtime.attachment_requests(st.context.headers,request_id=request_id)
        if type(requests) is not tuple or any(type(r) is not AttachmentRequest for r in requests):
            raise WorkflowUiError('ATTACHMENT_REQUESTS_UNAVAILABLE')
    except WorkflowUiError:
        st.info('Supporting-material requests are not available for this saved turn yet. No upload was submitted.')
        return
    if show_attachment_batches(runtime,request_id):return
    for request in requests:
        data=json.loads(request.canonical_json);key='evidence_'+request.sha256
        voluntary=data.get('collection_mode')=='VOLUNTARY_REFERENCE'
        with st.expander('Supporting material (optional)' if voluntary else 'Supporting material: '+data['evidence_kind'].replace('_',' ').lower()):
            if voluntary:
                st.write('You can add a product image, reference, playbook or document to this saved request. This does not mean evidence is missing or approved.')
            else:st.write('Missing: '+', '.join(f.replace('_',' ') for f in data['missing_fields']))
            st.caption('Share relevant evidence for this product and campaign. Files need validation and review; uploading does not approve a claim.')
            choice=st.radio('How would you like to continue?',data['choices'],
                format_func=lambda c:'Provide supporting material' if c=='PROVIDE_EVIDENCE' else 'Omit unsupported content',key=key+'_choice')
            if choice=='OMIT_UNSUPPORTED_CONTENT':
                st.caption('Requested alternative: '+data['alternative'].replace('_',' ').lower()+'. The revised request still needs validation.')
            with st.form(key):
                category=None;uploaded=None
                if choice=='PROVIDE_EVIDENCE':
                    category=st.selectbox('Material type',data['categories'],format_func=LABELS.get,key=key+'_category')
                    uploaded=st.file_uploader('Choose supporting material',type=['png','jpg','jpeg','webp','pdf','txt','json','csv'],
                        accept_multiple_files=True,max_upload_size=5,key=key+'_file')
                    st.caption('PNG/JPEG/WebP, PDF, UTF-8 text, JSON or CSV · maximum 5 MiB per file. Select multiple files together. Use Reference data or Supporting document for mixed images and documents. Export other documents as PDF or text. Files are stored privately and are not used for generation.')
                send=st.form_submit_button('Save supporting material' if category else 'Save omission request')
            if not send:continue
            files=uploaded if category else [None]
            if category:
                if not files:
                    st.warning('Choose at least one supported file, each no larger than 5 MiB.');continue
                invalid=False
                for file in files:
                    try:
                        if file.size>MAX_BYTES:raise ValueError('ATTACHMENT_FILE_SIZE')
                        inspect_file(file.name,file.getvalue(),category)
                    except (ValueError,OSError):
                        st.error('One or more files are invalid for this material type or exceed 5 MiB. No files were submitted.');invalid=True;break
                if invalid:continue
            if category:
                try:
                    batch_id=runtime.start_attachment_batch(st.context.headers,request_id=request_id,
                        requirement_id=data['requirement_id'],request_sha256=request.sha256,category=category,
                        files=tuple((f.name,f.getvalue()) for f in files))
                    if type(batch_id) is not str or not batch_id.startswith('batch_v1_'):
                        raise WorkflowUiError('ATTACHMENT_BATCH_UNVERIFIED')
                    st.session_state['attachment_batch_running_'+batch_id]=True
                    st.session_state['attachment_batch_focus_'+request_id]=batch_id
                    st.success('All selected originals are staged privately. Batch progress can be recovered after reconnect.')
                    st.rerun()
                except WorkflowUiError:
                    st.error('Batch staging could not be verified. Reconnect and inspect upload batches before resubmitting.')
                continue
            try:
                receipt=runtime.submit_attachment(st.context.headers,request_id=request_id,
                    requirement_id=data['requirement_id'],request_sha256=request.sha256,choice=choice,
                    category=None,filename=None,content=None)
                if type(receipt) is not AttachmentReceipt or receipt.generation_authorized is not False:
                    raise WorkflowUiError('ATTACHMENT_SUBMISSION_UNVERIFIED')
                st.success('Omission request saved; campaign details still need validation.')
                st.caption('Submission '+receipt.submission_id)
            except WorkflowUiError:
                st.error('Submission could not be verified. Reconnect and inspect saved receipts before retrying.')
