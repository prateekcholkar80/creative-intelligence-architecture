"""Explicit processing actions; no automatic parsing/model calls on rerun."""
import streamlit as st
from src.application.campaign_document_service import DocumentReceipt
from src.ui.workflow_runtime import WorkflowUiError


def show_processing_controls(runtime,row):
    submission_id=row['submission_id']
    if row['filename'] is None:
        if row['status']=='REQUEST_CHANGE_PENDING' and st.button('Continue with this omission',key='omit_'+submission_id):
            try:
                receipt=runtime.apply_omission(st.context.headers,submission_id=submission_id)
                if receipt.native_status=='COMMITTED':
                    st.success('New campaign turn saved. Reevaluation is required before generation.')
                    st.caption('Request '+receipt.request_id)
                else:st.warning('The original operation is not confirmed. Check again without creating another omission.')
            except WorkflowUiError:
                st.error('The omission could not be applied. Current evidence, policy, completed history and access must be verified. No new operation was substituted.')
        return
    left,right=st.columns(2)
    extract=left.button('Extract document text',key='extract_'+submission_id)
    check=right.button('Check extraction and eligibility',key='extract_status_'+submission_id)
    if not (extract or check):return
    try:
        receipt=runtime.process_document(st.context.headers,submission_id=submission_id,extract=bool(extract))
        if receipt is None:
            st.info('This file has not been extracted.');return
        if type(receipt) is not DocumentReceipt or receipt.generation_authorized is not False:
            raise WorkflowUiError('DOCUMENT_PROCESSING_UNAVAILABLE')
        st.text(f'{receipt.status} · {receipt.text_bytes} text bytes · {receipt.segment_count} source segments')
        st.caption('Extraction '+receipt.extraction_id)
        messages=dict(OCR_REQUIRED='This image or scanned document needs OCR. Provide a text-based PDF or UTF-8 text for now.',
            ENCRYPTED_DOCUMENT='Provide an unlocked export of this document.',NO_TEXT='No readable text was found.')
        if receipt.status in messages:st.info(messages[receipt.status])
        st.info('Source review and applicable campaign evaluation remain required. Extracted text is not approved wording and is not used for generation.')
    except WorkflowUiError:
        st.error('Document processing could not be verified. Check format, limits and current access before retrying. No partial text was released.')
