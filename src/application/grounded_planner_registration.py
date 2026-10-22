"""Exact-target IAP/ADMIN DRAFT import; no activation or serving authorization."""
from dataclasses import dataclass
from datetime import datetime,timezone
from threading import RLock
from src.application.planner_prompt_registration import PlannerPromptRegistrationOperator
from src.application.prompt_import_preview import PromptImportPreviewRequest
from src.application.prompt_contract import PromptTask
from src.application.campaign_grounded_planner_definition import GroundedPlannerDefinition,KEY
from src.application.vertex_prompt_reader import VertexPromptReadRequest
from src.application.workflow_contract import _require,_digest


@dataclass(frozen=True,slots=True)
class GroundedPlannerTarget:
    prompt_id:str
    version_id:str
    snapshot_sha256:str
    settings_prompt_id:str
    settings_version_id:str
    settings_snapshot_sha256:str

    def __post_init__(self):
        for h in (self.snapshot_sha256,self.settings_snapshot_sha256):_digest(h)
        for pid,vid in ((self.prompt_id,self.version_id),(self.settings_prompt_id,self.settings_version_id)):
            VertexPromptReadRequest(KEY,PromptTask.PLANNER,pid,vid,'campaign-draft-v1','target-validation')


class GroundedReviewStore:
    def __init__(self):self.issued={};self.lock=RLock()


class GroundedPlannerRegistrationOperator(PlannerPromptRegistrationOperator):
    def __init__(self,*,target,settings_snapshot,review_store,**base):
        super().__init__(config={},**base)
        _require(type(target) is GroundedPlannerTarget and type(review_store) is GroundedReviewStore,
            'GROUNDED_REGISTRATION_CONFIG')
        _require(settings_snapshot.snapshot_sha256==target.settings_snapshot_sha256
            and settings_snapshot.vertex_prompt_id==target.settings_prompt_id
            and settings_snapshot.vertex_version_id==target.settings_version_id,'GROUNDED_REGISTRATION_SETTINGS_CHANGED')
        self.target,self.settings,self.reviews=target,settings_snapshot,review_store
        self.definition=GroundedPlannerDefinition(settings_snapshot.output_schema_json)

    def _candidate(self,candidate):
        expected=self.definition.snapshot(self.settings,vertex_prompt_id=self.target.prompt_id,vertex_version_id=self.target.version_id)
        _require(candidate==expected and candidate.snapshot_sha256==self.target.snapshot_sha256,'GROUNDED_REGISTRATION_PARITY')

    def _preview(self,request,proposed_by):
        _require(request.prompt_key==KEY and request.task is PromptTask.PLANNER
            and request.prompt_id==self.target.prompt_id and request.version_id==self.target.version_id,
            'GROUNDED_REGISTRATION_TARGET')
        value=self.preview_service.preview(PromptImportPreviewRequest(request,datetime.now(timezone.utc),proposed_by))
        self._candidate(value.candidate);return value

    def prepare(self,headers,**values):
        with self.reviews.lock:
            _require(len(self.reviews.issued)<32,'GROUNDED_REGISTRATION_CAPACITY')
            command=super().prepare(headers,**values)
            self.reviews.issued[command.approved_preview_sha256]=command
            return command

    def register(self,headers,command,*,confirmed_sha256):
        with self.reviews.lock:
            _require(self.reviews.issued.get(confirmed_sha256) is command,'GROUNDED_REVIEW_NOT_ISSUED')
            self._candidate(command.approved_preview.candidate)
            return super().register(headers,command,confirmed_sha256=confirmed_sha256)
