"""Protected read-only measurement/approval dependency installation.

Installation is distinct from qualifying a report, appointing a reviewer or
allowing a model dispatch. No browser request can supply authority or report pins.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
import json
from src.application.campaign_quality_publication import MeasurementPublisherManifest
from src.application.campaign_quality_publisher import MeasurementPublisher
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
from src.application.campaign_policy_approval_publisher import PolicyApprovalPublisher
from src.application.campaign_quality_qualification_reader import QualityQualificationReader
from src.application.campaign_policy_approval_reader import PolicyApprovalReader
from src.application.campaign_policy_approval_runtime import PolicyApprovalRuntimeGuard
from src.application.campaign_quality_qualification import SCHEMAS as QUALITY,table_metadata as quality_table
from src.application.campaign_policy_approval_receipt import SCHEMAS as APPROVAL,table_metadata as approval_table,_principal
from src.application.campaign_review_storage import table_metadata as control_table
from src.application.campaign_publication_reader import _identity
from src.application.control_io import ControlIO
from src.application.operation_budget import require_owned_worker,operation_call
from src.application.workflow_schema import TABLES
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import _require,_id,_digest
from src.ingestion.setup_workflow_tables import PROJECT,validate_table
from google.cloud import bigquery


@dataclass(frozen=True,slots=True)
class MeasurementRuntimeSettings:
    workspace_id: str
    publisher_id: str
    review_sha256: str
    evaluator_sha256: str

    def __post_init__(self):
        _id(self.workspace_id);_principal(self.publisher_id)
        _digest(self.review_sha256);_digest(self.evaluator_sha256)


AUTHORITY_SQL=f'''SELECT CURRENT_TIMESTAMP() observed_at,
 ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.ci_application.workspaces` t WHERE workspace_id=@workspace_id LIMIT 2) workspaces,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_application.workspaces` WHERE workspace_id=@workspace_id) workspace_count,
 ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.ci_application.memberships` t WHERE workspace_id=@workspace_id AND principal_id=@principal_id LIMIT 2) memberships,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_application.memberships` WHERE workspace_id=@workspace_id AND principal_id=@principal_id) membership_count'''


def verify_authority(row,*,settings,identity,started,ended):
    _require(type(row) is dict and set(row)=={'observed_at','workspaces','workspace_count','memberships','membership_count'}
        and started<=row['observed_at']<=ended and (ended-started).total_seconds()<=60,'MEASUREMENT_RUNTIME_CAPTURE')
    for group,count in (('workspaces','workspace_count'),('memberships','membership_count')):
        _require(type(row[count]) is int and row[count]==1 and type(row[group]) is list and len(row[group])==1,
            'MEASUREMENT_RUNTIME_AUTHORITY_MISSING')
    workspace=decode_row(row['workspaces'][0],'workspaces');member=decode_row(row['memberships'][0],'memberships')
    _require(workspace['workspace_id']==member['workspace_id']==settings.workspace_id
        and workspace['status']==member['status']=='ACTIVE'
        and member['principal_id']==identity.principal.principal_id==settings.publisher_id
        and member['identity_issuer']==identity.issuer and member['identity_subject']==identity.subject
        and workspace['updated_at']<=row['observed_at'] and member['updated_at']<=row['observed_at'],
        'MEASUREMENT_RUNTIME_AUTHORITY_REVOKED')
    return workspace['revision'],member['revision']


class ProtectedMeasurementRuntime:
    def __init__(self,*,client,verifier,settings,session_manifest,clock=lambda:datetime.now(timezone.utc)):
        require_owned_worker()
        _require(type(settings) is MeasurementRuntimeSettings and type(session_manifest) is OperatorPublisherManifest
            and session_manifest.principal_ids==(settings.publisher_id,),'MEASUREMENT_RUNTIME_CONFIGURATION')
        self.client,self.verifier,self.settings,self.manifest,self.clock=client,verifier,settings,session_manifest,clock
        self.io=ControlIO(client,result_timeout_seconds=90)
        # Measurement and approval writes have independent, disabled adapters.
        manifest=MeasurementPublisherManifest(session_manifest,settings.workspace_id,settings.review_sha256,settings.evaluator_sha256)
        self.publisher=MeasurementPublisher(client=client,identity_adapter=verifier,manifest=manifest,enabled=False,clock=clock)
        self.approval_publisher=PolicyApprovalPublisher(client=client,identity_adapter=verifier,enabled=False,
            qualification_reader=self.qualify,clock=clock)
        self._qualification=None

    def configure_qualification(self,reader):
        """Server-only future evaluated-version binding; never a transport method."""
        require_owned_worker()
        _require(type(reader) is QualityQualificationReader and reader.client is self.client
            and reader.verifier is self.verifier and reader.pins.workspace_id==self.settings.workspace_id
            and reader.requirements.review_sha256==self.settings.review_sha256
            and reader.requirements.evaluator_sha256==self.settings.evaluator_sha256,
            'MEASUREMENT_RUNTIME_QUALIFICATION_SCOPE')
        self._qualification=reader

    def qualify(self,headers,pins):
        self._actor(headers)
        _require(self._qualification is not None,'POLICY_QUALIFYING_EVALUATION_UNAVAILABLE')
        result=self._qualification.read(headers,pins)
        self._actor(headers)
        return result

    def approval_guard(self,acceptance_reader,clarification_reader):
        """Assemble only exact server readers after a qualifying report exists."""
        require_owned_worker()
        _require(self._qualification is not None,'POLICY_QUALIFYING_EVALUATION_UNAVAILABLE')
        for reader in (acceptance_reader,clarification_reader):
            _require(type(reader) is PolicyApprovalReader and reader.client is self.client
                and reader.verifier is self.verifier and reader.pins.workspace_id==self.settings.workspace_id,
                'MEASUREMENT_RUNTIME_APPROVAL_SCOPE')
        return PolicyApprovalRuntimeGuard(acceptance_reader=acceptance_reader,clarification_reader=clarification_reader,clock=self.clock)

    def _actor(self,headers):
        require_owned_worker();actor=self.verifier.authenticate(headers)
        _identity(actor,self.verifier.settings,self.clock());self.manifest.require_actor(actor,self.clock())
        _require(actor.principal.principal_id==self.settings.publisher_id,'MEASUREMENT_RUNTIME_PUBLISHER_DENIED')
        return actor

    def current(self,headers):
        actor=self._actor(headers)
        expected=[TABLES[name].table(PROJECT) for name in ('workspaces','memberships','requests','model_invocations')]
        from src.application.prompt_runtime_contract import TABLES as BINDINGS
        expected += [BINDINGS['invocation_prompt_bindings'].table(PROJECT)]
        expected += [quality_table(name) for name in QUALITY]+[approval_table(name) for name in APPROVAL]
        expected += [control_table(name,PROJECT) for name in ('campaign_permission_controls','campaign_permission_control_history','campaign_permission_publication_audits')]
        for table in expected:validate_table(operation_call(self.client.get_table,table.reference,retry=None,timeout=10),table)
        started=self.clock()
        row=self.io.read(AUTHORITY_SQL,[bigquery.ScalarQueryParameter('workspace_id','STRING',self.settings.workspace_id),
            bigquery.ScalarQueryParameter('principal_id','STRING',actor.principal.principal_id)])
        workspace_revision,membership_revision=verify_authority(row,settings=self.settings,identity=actor,started=started,ended=self.clock())
        current=self._actor(headers)
        _require(current.principal==actor.principal,'MEASUREMENT_RUNTIME_ACTOR_CHANGED')
        return dict(runtime_installed=True,workspace_revision=workspace_revision,membership_revision=membership_revision,
            publisher_verified=True,measurement_writes_enabled=False,approval_writes_enabled=False,
            qualifying_evaluation_configured=self._qualification is not None,release_allowed=False)
