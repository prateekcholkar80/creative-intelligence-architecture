"""Protected omission application/recovery; never dispatches a model."""
import hashlib,json
from google.api_core.exceptions import NotFound
from src.application.campaign_document_store import DocumentStore
from src.application.campaign_omission_turn import read_omission,omission_key,link_id,OmissionContext
from src.application.campaign_clarification_policy import read_policy
from src.application.campaign_evidence_gaps import canonical
from src.application.control_io import query_config
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import _require,_digest
from src.application.interpreter_submission import InterpreterSubmissionReceipt
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION


class OmissionTurnService:
    def __init__(self,*,submission,intake):
        _require(submission.capture_history is True,'OMISSION_HISTORY_REQUIRED')
        self.submission,self.intake=submission,intake
        self.store=DocumentStore(submission.client)

    def apply(self,headers,*,workspace_id,conversation_id,submission_id):
        _digest(submission_id)
        actor,access=self.intake._actor(headers,workspace_id,conversation_id)
        source=self.store.submission(submission_id,workspace_id=workspace_id,conversation_id=conversation_id,
            principal_id=actor.principal.principal_id)
        _require(source['selection']['choice']=='OMIT_UNSUPPORTED_CONTENT'
            and source['selection']['status']=='REQUEST_CHANGE_PENDING' and source['file'] is None,
            'OMISSION_EXPLICIT_CHOICE_REQUIRED')
        rid=link_id(workspace_id,conversation_id,actor.principal.principal_id,submission_id)
        existing=self.store.read('turn_links',rid)
        if existing is not None:
            result=self._recover(source,existing,actor.principal.principal_id,submission_id)
        else:
            key=omission_key(workspace_id,conversation_id,actor.principal.principal_id,submission_id)
            try:self.submission.io.original_job('interpreter_submit_v1_'+key)
            except NotFound:
                context=read_omission(self.intake,self.store,headers,workspace_id=workspace_id,
                    conversation_id=conversation_id,submission_id=submission_id)
                result=self.submission.submit(headers,workspace_id=workspace_id,conversation_id=conversation_id,
                    message=context.message,retry_key='omission-v1-'+submission_id,
                    selection=context.value['selection'],omission_context=context)
            else:
                # A missing link is not proof that an in-flight/failed original
                # rolled back. Keep its slot; never rebuild or resubmit it.
                result=InterpreterSubmissionReceipt(workspace_id,conversation_id,'interpret-v1-'+key,
                    'interpreter-v1-'+key,'interpreter_submit_v1_'+key,'UNKNOWN')
        read_policy(self.submission.client,self.intake.plan_reader.policy_request,clock=self.intake.plan_reader.clock)
        after,current=self.intake._actor(headers,workspace_id,conversation_id)
        _require(after.principal==actor.principal and current==access,'OMISSION_ACCESS_CHANGED')
        return result

    def _recover(self,source,link,principal_id,submission_id):
        scope=source['scope'];key=omission_key(scope['workspace_id'],scope['conversation_id'],principal_id,submission_id)
        _require(link['version']=='campaign-omission-turn-link-v1' and link['generation_authorized'] is False
            and link['status']=='REEVALUATION_REQUIRED' and link['source_submission_id']==submission_id
            and link['source_record_sha256']==hashlib.sha256(canonical(source).encode()).hexdigest()
            and link['principal_id']==principal_id and link['workspace_id']==scope['workspace_id']
            and link['conversation_id']==scope['conversation_id'] and link['parent_request_id']==scope['request_id']
            and link['new_request_id']=='interpret-v1-'+key and link['new_invocation_id']=='interpreter-v1-'+key
            and link['native_job_id']=='interpreter_submit_v1_'+key,'OMISSION_LINK_SCOPE')
        job=self.submission.client.get_job(link['native_job_id'],location=LOCATION,retry=None,timeout=20)
        _require(hashlib.sha256(job.query.encode()).hexdigest()==link['sql_sha256']
            and job.user_email=='ci-marketing@'+PROJECT+'.iam.gserviceaccount.com'
            and matches_sharing_job(job,job_id=link['native_job_id'],sql=job.query,config=query_config(job.query_parameters)),
            'OMISSION_ORIGINAL_JOB_UNVERIFIED')
        rows=list(job.result(timeout=45,max_results=2,retry=None,job_retry=None))
        _require(job.state=='DONE' and job.error_result is None and len(rows)==1
            and rows[0].get('omission_link_row')==canonical(link),'OMISSION_ORIGINAL_RECEIPT_UNVERIFIED')
        row=dict(rows[0]);request=decode_row(row['request_row'],'requests')
        invocation=decode_row(row['invocation_row'],'model_invocations')
        body=json.loads(row['transcript_row']);batch=json.loads(body['batch_canonical_json'])
        expected=OmissionContext(canonical(dict(source=source,submission_id=submission_id))).message
        _require(request['request_id']==link['new_request_id'] and request['request_sha256']==link['new_request_sha256']
            and request['message_sha256']==link['message_sha256']==hashlib.sha256(expected.encode()).hexdigest()
            and invocation['invocation_id']==link['new_invocation_id'] and invocation['request_id']==request['request_id']
            and body['request_id']==request['request_id'] and len(batch)==1 and batch[0]['text']==expected
            and row['authority_sha256']==link['authority_sha256'],'OMISSION_STORED_TURN_CHANGED')
        return InterpreterSubmissionReceipt(scope['workspace_id'],scope['conversation_id'],link['new_request_id'],
            link['new_invocation_id'],link['native_job_id'],'COMMITTED')
