"""Single private-candidate action with two original-job bounded worker phases."""
import hashlib,json,logging,re


def failure(st,error):
    code=getattr(error,'code',None)
    allowed={'ACCESS_DENIED','OPERATION_DEADLINE_EXCEEDED','OPERATION_CANCELLED','SHARING_HOST_RETIRED',
        'SHARING_HOST_PROTOCOL_FAILED','VERTEX_CAMPAIGN_PLANNER_ORIGINAL_UNAVAILABLE','VERTEX_CAMPAIGN_CROP_CHANGED',
        'VERTEX_CAMPAIGN_PROMPT_CHANGED','VERTEX_CAMPAIGN_SOURCE_CHANGED','VERTEX_CAMPAIGN_SOURCE_EXPIRED',
        'VERTEX_CAMPAIGN_OWNER_CHANGED','VISUAL_COMPLETION_BUDGET_REQUIRED','VISUAL_ENDPOINT_AUTHORIZATION_REQUIRED',
        'VISUAL_ORIGINAL_MISSING','VISUAL_ORIGINAL_UNCERTAIN','VISUAL_ORIGINAL_UNAVAILABLE_OR_UNKNOWN',
        'VISUAL_OBJECT_GENERATION_CHANGED','VISUAL_OBJECT_BYTES_CHANGED','ARTIFACT_ORIGINAL_MISSING',
        'ARTIFACT_ORIGINAL_UNCERTAIN','ARTIFACT_SCORE_ORIGINAL_MISSING','ARTIFACT_SCORE_ORIGINAL_UNCERTAIN','ARTIFACT_GENERATION_CHANGED'}
    safe=code if type(code) is str and (code in allowed or re.fullmatch(
        r'OPERATOR_FAILURE_(?:TYPE|ATTRIBUTE|KEY|FILE|IMPORT|DEPENDENCY)_(?:L[0-9]+|UNKNOWN)',code)) else 'RUNTIME_UNAVAILABLE'
    logging.getLogger(__name__).warning(json.dumps(dict(event='vertex_candidate_failed',code=safe)))
    st.error('The original Vertex candidate could not be verified. Reload and recover the original candidate before running again.')
    st.caption('Diagnostic reference: '+safe)


def render(st,host,headers,run_id):
    key='_ci_vertex_candidate_pending_'+hashlib.sha256(run_id.encode()).hexdigest()
    pending=st.session_state.get(key,False)
    st.subheader('Private Vertex campaign candidate')
    st.caption('One image-model attempt on Vertex global. The saved product crop is preserved. Claims and release remain unapproved.')
    if st.button('Generate private Vertex candidate',disabled=pending is True):
        st.session_state[key]=True
        try:
            with st.spinner('Generating and saving the Vertex scene…'):
                visual=host.vertex_visual_run(headers)
            st.json(visual)
            with st.spinner('Composing and checking the private banner…'):
                banner=host.vertex_banner_run(headers)
            st.json(banner);st.session_state[key]=False
        except Exception as error:failure(st,error)
    if st.button('Recover original Vertex candidate'):
        try:
            with st.spinner('Checking the original scene and banner records…'):
                visual=host.vertex_visual_recover(headers);st.json(visual)
                try:banner=host.vertex_banner_recover(headers)
                except Exception as error:
                    if getattr(error,'code',None) not in {'ARTIFACT_ORIGINAL_MISSING','ARTIFACT_SCORE_ORIGINAL_MISSING'}:raise
                    st.session_state[key]=False
                    st.info('The saved Vertex scene is verified. Generate private Vertex candidate can resume the missing banner work and reuse that scene.')
                else:st.json(banner);st.session_state[key]=False
        except Exception as error:
            st.session_state[key]=True;failure(st,error)
