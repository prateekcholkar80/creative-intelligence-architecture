"""Default-disabled protected campaign submission for Priority 4.7.

The service authenticates IAP headers, reads fresh BigQuery control rows, builds
the existing typed workflow-start plan and submits it through the 4.6 executor.
It exposes neither mutable rows nor writer plans to an HTTP or browser caller.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock

from google.cloud import bigquery

from src.application.access_capture import capture_access
from src.application.access_policy import Action, POLICY_VERSION, require_authorized
from src.application.audit_contract import (
    AuditTarget, MutationKind, MutationObservation, MutationOutcome, ResourceType,
    WorkflowAction, WorkflowObservation,
)
from src.application.audit_producer import produce_access_audit, produce_service_audit
from src.application.control_io import ControlIO
from src.application.conversation_contract import validate_conversation
from src.application.identity import IapIdentityAdapter, ensure_fresh
from src.application.management_service import digest, parameter, require, table
from src.application.membership_contract import validate_membership
from src.application.operation_budget import OperationSettings, operation_lock, operation_scope
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import (
    PREFERENCE_FIELDS, _id, request_fingerprint, retry_key_hash, validate_job,
    validate_profile, validate_request,
)
from src.application.workflow_schema import SCHEMA_VERSION, TABLES
from src.application.workflow_start_query import prepare_workflow_start
from src.application.workflow_writer_executor import (
    WorkflowWriteOutcome, WorkflowWriterExecutor, WorkflowWriterSettings,
)
from src.application.workspace_contract import validate_workspace


SNAPSHOT_SQL = f'''BEGIN TRANSACTION;
CREATE TEMP TABLE snapshot AS SELECT CURRENT_TIMESTAMP() AS observed_at,
  ARRAY(SELECT TO_JSON_STRING(w) FROM {table('workspaces')} w
    WHERE workspace_id=@workspace_id LIMIT 2) AS workspaces,
  ARRAY(SELECT TO_JSON_STRING(m) FROM {table('memberships')} m
    WHERE workspace_id=@workspace_id AND principal_id=@principal_id LIMIT 2) AS memberships,
  ARRAY(SELECT TO_JSON_STRING(c) FROM {table('conversations')} c
    WHERE workspace_id=@workspace_id AND conversation_id=@conversation_id LIMIT 2) AS conversations;
COMMIT TRANSACTION;
SELECT * FROM snapshot;'''


@dataclass(frozen=True, slots=True)
class SubmissionSettings:
    enabled: bool = False
    worker_id: str = 'workflow-submission'
    model_profiles: tuple[dict, ...] = ()
    renderer_version: str = ''
    renderer_settings_sha256: str = ''
    job_deadline_seconds: int = 1800
    maximum_plans: int = 1024
    operation: OperationSettings = OperationSettings()
    query: SharingQuerySettings = SharingQuerySettings(maximum_bytes_billed=512 * 1024**2)

    def __post_init__(self):
        require(type(self.enabled) is bool and type(self.operation) is OperationSettings
            and type(self.query) is SharingQuerySettings
            and type(self.job_deadline_seconds) is int and 60 <= self.job_deadline_seconds <= 3600
            and type(self.maximum_plans) is int and 1 <= self.maximum_plans <= 4096,
            'INVALID_SUBMISSION_SETTINGS')
        _id(self.worker_id)
        profiles = tuple(deepcopy(value) for value in self.model_profiles)
        if self.enabled:
            require(len(profiles) == 2 and {value.get('stage') for value in profiles}
                == {'INTERPRETER', 'PLANNER'}, 'INVALID_SUBMISSION_SETTINGS')
            _id(self.renderer_version)
            require(len(self.renderer_settings_sha256) == 64
                and all(c in '0123456789abcdef' for c in self.renderer_settings_sha256),
                'INVALID_SUBMISSION_SETTINGS')
        for profile in profiles:
            validate_profile(profile)
        object.__setattr__(self, 'model_profiles', profiles)


@dataclass(frozen=True, slots=True)
class SubmissionReceipt:
    workspace_id: str
    conversation_id: str
    request_id: str
    job_id: str
    persistence_status: str
    job_state: str | None
    submitted: bool

    def __post_init__(self):
        for value in (self.workspace_id, self.conversation_id, self.request_id, self.job_id):
            _id(value)
        require(self.persistence_status in {value.value for value in SharingCommitStatus}
            and type(self.submitted) is bool
            and self.job_state == ('QUEUED' if self.persistence_status == 'COMMITTED' else None),
            'INVALID_SUBMISSION_RECEIPT')


class ProtectedSubmissionService:
    """Prepare and submit one authenticated generation request without model I/O."""

    def __init__(self, *, client: bigquery.Client, verifier: IapIdentityAdapter,
                 settings: SubmissionSettings = SubmissionSettings()):
        require(isinstance(verifier, IapIdentityAdapter) and type(settings) is SubmissionSettings,
            'INVALID_SUBMISSION_SERVICE')
        self.verifier, self.settings = verifier, settings
        self.io = ControlIO(client)
        self.writer = WorkflowWriterExecutor(client, WorkflowWriterSettings(
            execution_enabled=settings.enabled, result_timeout_seconds=60))
        self._lock = RLock()
        self._plans: dict[str, object] = {}
        self._business: dict[str, str] = {}

    def _identity(self, headers, *, write: bool):
        if write:
            require(self.settings.enabled, 'SUBMISSION_DISABLED')
        identity = self.verifier.authenticate(headers)
        ensure_fresh(identity)
        return identity

    def _snapshot(self, identity, workspace_id: str, conversation_id: str):
        row = self.io.read(SNAPSHOT_SQL, [parameter('workspace_id', workspace_id),
            parameter('principal_id', identity.principal.principal_id),
            parameter('conversation_id', conversation_id)])
        require(set(row) == {'observed_at', 'workspaces', 'memberships', 'conversations'}
            and len(row['workspaces']) == len(row['memberships']) == len(row['conversations']) == 1
            and isinstance(row['observed_at'], datetime) and row['observed_at'].tzinfo is not None,
            'SUBMISSION_ACCESS_UNAVAILABLE')
        workspace = decode_row(row['workspaces'][0], 'workspaces')
        membership = decode_row(row['memberships'][0], 'memberships')
        conversation = decode_row(row['conversations'][0], 'conversations')
        validate_workspace(workspace, workspace_id=workspace_id)
        member = validate_membership(membership, workspace_id=workspace_id,
            principal_id=identity.principal.principal_id)
        resource = validate_conversation(conversation, workspace_id=workspace_id,
            conversation_id=conversation_id)
        require(workspace['status'] == 'ACTIVE' and conversation['status'] == 'ACTIVE'
            and membership['identity_issuer'] == identity.issuer
            and membership['identity_subject'] == identity.subject
            and max(workspace['updated_at'], membership['updated_at'], conversation['updated_at'])
                <= row['observed_at'], 'SUBMISSION_ACCESS_UNAVAILABLE')
        decision = require_authorized(identity.principal, member, Action.GENERATE_DRAFT,
            workspace_id=workspace_id)
        require(resource.owner_id == identity.principal.principal_id, 'SUBMISSION_OWNER_REQUIRED')
        ensure_fresh(identity)
        return row['observed_at'], workspace, membership, conversation, resource, decision

    def _plan(self, identity, *, workspace_id: str, conversation_id: str,
              preferences: dict, retry_key: str):
        _id(workspace_id); _id(conversation_id); _id(retry_key)
        require(type(preferences) is dict and set(preferences) == set(PREFERENCE_FIELDS),
            'INVALID_SUBMISSION_PREFERENCES')
        observed_at, workspace, membership, conversation, resource, decision = self._snapshot(
            identity, workspace_id, conversation_id)
        principal_id = identity.principal.principal_id
        retry_digest = retry_key_hash(workspace_id=workspace_id, principal_id=principal_id,
            conversation_id=conversation_id, operation='GENERATE', retry_key=retry_key)
        request_id = 'request-v1-' + retry_digest
        workflow_job_id = 'job-v1-' + retry_digest
        trace_id = digest(['workflow-submit-v1', workspace_id, principal_id,
            conversation_id, retry_digest])[:32]
        request = dict.fromkeys(field.name for field in TABLES['requests'].schema)
        request.update(schema_version=SCHEMA_VERSION, workspace_id=workspace_id,
            conversation_id=conversation_id, owner_id=principal_id, principal_id=principal_id,
            request_id=request_id, session_id='session-v1-' + retry_digest,
            trace_id=trace_id, operation='GENERATE', request_status='READY', revision=1,
            submitted_preferences={'preferences': deepcopy(preferences), 'renderer': {
                'version': self.settings.renderer_version,
                'settings_sha256': self.settings.renderer_settings_sha256}},
            message_sha256=None, **deepcopy(preferences), unsupported_features=[],
            idempotency_key_sha256=retry_digest, request_sha256='0' * 64,
            parent_output_id=None, generation_intent_id='intent-v1-' + retry_digest,
            model_profiles=[deepcopy(value) for value in self.settings.model_profiles],
            policy_version=POLICY_VERSION, created_at=observed_at, updated_at=observed_at)
        request['request_sha256'] = request_fingerprint(request)
        validate_request(request, conversation=resource)
        job = dict.fromkeys(field.name for field in TABLES['jobs'].schema)
        for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id',
                    'request_id', 'principal_id', 'trace_id', 'canonical_sku',
                    'idempotency_key_sha256', 'request_sha256', 'generation_intent_id',
                    'policy_version'):
            job[key] = request[key]
        job.update(job_id=workflow_job_id, state='QUEUED', revision=1, attempt_count=0,
            active_attempt_id=None, output_id=None, safe_error_code=None,
            created_at=observed_at, updated_at=observed_at,
            deadline_at=observed_at + timedelta(seconds=self.settings.job_deadline_seconds),
            completed_at=None)
        validate_job(job, request=request, conversation=resource)
        request_target = AuditTarget(ResourceType.REQUEST, request_id, workspace_id,
            conversation_id=conversation_id, owner_id=principal_id, request_id=request_id)
        access_audit = produce_access_audit(capture_access(decision, target=request_target,
            trace_id=trace_id, observed_at=observed_at), recorded_at=observed_at)
        request_audit = produce_service_audit(WorkflowObservation(self.settings.worker_id,
            workspace_id, WorkflowAction.CREATE_REQUEST), target=request_target,
            mutation=MutationObservation(MutationKind.CREATE, MutationOutcome.APPLIED, 1),
            trace_id=trace_id, observed_at=observed_at, recorded_at=observed_at)
        job_audit = produce_service_audit(WorkflowObservation(self.settings.worker_id,
            workspace_id, WorkflowAction.CREATE_JOB), target=AuditTarget(ResourceType.JOB,
            workflow_job_id, workspace_id, conversation_id=conversation_id,
            owner_id=principal_id, request_id=request_id, job_id=workflow_job_id),
            mutation=MutationObservation(MutationKind.CREATE, MutationOutcome.APPLIED, 1),
            trace_id=trace_id, observed_at=observed_at, recorded_at=observed_at)
        business = digest([request['request_sha256'], retry_digest,
            self.settings.renderer_version, self.settings.renderer_settings_sha256])
        existing = self._plans.get(workflow_job_id)
        if existing is not None:
            require(self._business[workflow_job_id] == business, 'SUBMISSION_IDEMPOTENCY_CONFLICT')
            return existing
        require(len(self._plans) < self.settings.maximum_plans, 'SUBMISSION_ADMISSION_FULL')
        plan = prepare_workflow_start(workspace=workspace, membership=membership,
            conversation=conversation, request=request, job=job, request_audit=request_audit,
            job_audit=job_audit, access_audit=access_audit, observed_at=observed_at,
            settings=self.settings.query)
        self._plans[workflow_job_id] = plan
        self._business[workflow_job_id] = business
        return plan

    def submit(self, headers, *, workspace_id: str, conversation_id: str,
               preferences: dict, retry_key: str) -> SubmissionReceipt:
        with operation_scope(self.settings.operation), operation_lock(self._lock):
            identity = self._identity(headers, write=True)
            plan = self._plan(identity, workspace_id=workspace_id,
                conversation_id=conversation_id, preferences=preferences, retry_key=retry_key)
            outcome: WorkflowWriteOutcome = self.writer.submit(plan)
            current = self._identity(headers, write=False)
            require(current.principal == identity.principal and current.issuer == identity.issuer
                and current.subject == identity.subject, 'SUBMISSION_IDENTITY_CHANGED')
            self._snapshot(current, workspace_id, conversation_id)
            committed = outcome.native_status is SharingCommitStatus.COMMITTED
            return SubmissionReceipt(workspace_id, conversation_id, plan.request_id,
                plan.workflow_job_id, outcome.native_status.value,
                'QUEUED' if committed else None, outcome.submitted)

