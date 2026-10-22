"""Opt-in server composition for scoped chat uploads; no deployment/approval."""
import json
import os
from pathlib import Path
from datetime import datetime,timezone
import hashlib
from concurrent.futures import ThreadPoolExecutor

from google.cloud import storage
from src.application.access_reader import BigQueryAccessReader
from src.application.campaign_attachment_service import AttachmentStore,ProtectedAttachmentService
from src.application.campaign_clarification_policy import ClarificationPolicyRequest,read_policy
from src.application.campaign_quality_authority_reader import read_execution_authority
from src.application.campaign_attachment_contract import ReferenceCollectionRead
from src.application.campaign_evidence_gaps import canonical
from src.application.campaign_evidence_conversation import read_bound_gaps
from src.application.campaign_question_context_reader import read_question_context
from src.application.campaign_response_transcript import verify_stored_response
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import _require

CONFIG_ENV='CI_CAMPAIGN_ATTACHMENTS_CONFIG'


class StoredAttachmentPlanReader:
    """Resolve the persisted intent response, never a browser-provided answer."""
    def __init__(self,*,client,verifier,access_reader,policy_request,clock=lambda:datetime.now(timezone.utc)):
        self.client,self.verifier,self.access_reader=client,verifier,access_reader
        self.policy_request,self.clock=policy_request,clock

    def saved_requests(self,headers,*,workspace_id,conversation_id):
        identity=self.verifier.authenticate(headers)
        capture=read_question_context(self.client,identity=identity,access_reader=self.access_reader,
            workspace_id=workspace_id,conversation_id=conversation_id,clock=self.clock)
        rows=[decode_row(row,'requests') for row in capture.records()['requests']]
        rows=[r for r in rows if r['operation']=='INTERPRET' and r['principal_id']==identity.principal.principal_id]
        _require(self.clock()<capture.valid_until,'ATTACHMENT_SOURCE_EXPIRED')
        return tuple(dict(request_id=r['request_id'],sku=r['canonical_sku'] or r['submitted_preferences']['preferences']['canonical_sku'],
            created_at=r['created_at'].isoformat()) for r in sorted(rows,key=lambda r:(r['created_at'],r['request_id']),reverse=True)[:20])

    def __call__(self,headers,*,workspace_id,conversation_id,request_id):
        identity=self.verifier.authenticate(headers)
        capture=read_question_context(self.client,identity=identity,access_reader=self.access_reader,
            workspace_id=workspace_id,conversation_id=conversation_id,clock=self.clock)
        records=capture.records()
        calls=[decode_row(row,'model_invocations') for row in records['model_invocations']]
        calls=[call for call in calls if call['request_id']==request_id
            and call['stage']=='INTERPRETER' and call['state']=='SUCCEEDED']
        if not calls:
            return self._optional_reference(identity,workspace_id,conversation_id,request_id)
        _require(len(calls)==1,'ATTACHMENT_STORED_INTENT_REQUIRED')
        expected=verify_stored_response(self.client,calls[0],clock=self.clock)
        bodies=[json.loads(row) for row in records['campaign_transcript_responses']]
        bodies=[b for b in bodies if b['request_id']==request_id
            and b['invocation_id']==calls[0]['invocation_id'] and b['content_sha256']==expected]
        _require(len(bodies)==1 and hashlib.sha256(bodies[0]['response_text'].encode()).hexdigest()==expected,
            'ATTACHMENT_STORED_RESPONSE_REQUIRED')
        result=read_bound_gaps(self.client,self.policy_request,identity=identity,access_reader=self.access_reader,
            workspace_id=workspace_id,conversation_id=conversation_id,request_id=request_id,
            observed_interpreter_response=bodies[0]['response_text'],clock=self.clock)
        _require(self.clock()<min(capture.valid_until,result.valid_until),'ATTACHMENT_SOURCE_EXPIRED')
        return result.plan

    def _optional_reference(self,identity,workspace_id,conversation_id,request_id):
        """Collection precedes model execution; no fabricated intent or gap state."""
        args=dict(identity=identity,workspace_id=workspace_id,conversation_id=conversation_id,
            request_id=request_id,clock=self.clock)
        # Independent read-only sources overlap within each pass. Complete the
        # initial pass before starting the final pass; never cache authority.
        with ThreadPoolExecutor(max_workers=2,thread_name_prefix='attachment-source') as pool:
            def capture():
                authority=pool.submit(read_execution_authority,self.client,**args)
                policy=pool.submit(read_policy,self.client,self.policy_request,clock=self.clock)
                return authority.result(),policy.result()
            first,policy=capture()
            final,final_policy=capture()
        _require(first.content_sha256==final.content_sha256 and first.native_job_id!=final.native_job_id
            and policy.record_sha256==final_policy.record_sha256 and policy.native_job_id!=final_policy.native_job_id
            and self.clock()<min(first.valid_until,final.valid_until,policy.valid_until,final_policy.valid_until),
            'ATTACHMENT_REFERENCE_CONTEXT_CHANGED')
        content=json.loads(first.content_canonical_json);authority=content['authority']
        _require(authority['principal_id']==identity.principal.principal_id,'ATTACHMENT_REFERENCE_PRINCIPAL')
        scope={k:authority[k] for k in ('workspace_id','conversation_id','request_id','sku','country','channel','audience','request_sha256')}
        scope.update(authority_sha256=first.content_sha256,transcript_sha256=content['transcript_sha256'])
        return ReferenceCollectionRead(canonical(dict(version='campaign-reference-collection-v1',scope=scope,
            checked_at=self.clock().isoformat(),policy_sha256=policy.request.policy_sha256,generation_authorized=False)))


def compose_attachments(*,client,verifier,environ=None):
    """Only a deployment-owned absolute config path enables this optional port."""
    env=os.environ if environ is None else environ
    if not env.get(CONFIG_ENV):return None
    path=Path(env[CONFIG_ENV])
    _require(path.is_absolute() and path.stat().st_size<=8192,'ATTACHMENT_CONFIG_PATH')
    config=json.loads(path.read_text())
    _require(type(config) is dict and set(config)=={'enabled','bucket','policy'}
        and type(config['enabled']) is bool,'ATTACHMENT_CONFIG_INVALID')
    if not config['enabled']:return None
    policy=ClarificationPolicyRequest(**config['policy'])
    access=BigQueryAccessReader(client)
    reader=StoredAttachmentPlanReader(client=client,verifier=verifier,access_reader=access,policy_request=policy)
    return ProtectedAttachmentService(verifier=verifier,access_reader=access,plan_reader=reader,
        store=AttachmentStore(client=client,storage=storage.Client(project=client.project),bucket=config['bucket']),enabled=True)
