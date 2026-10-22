"""Produce validated access and workflow audit envelopes before durable writes.

Inputs are already captured server observations. This module bounds observation
age, creates exact-schema rows, and retains the original context for immutable
replay validation. It performs no authentication, authorization, cloud I/O, or
commit classification.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime,timedelta

from src.application.access_capture import AccessCapture
from src.application.audit_contract import (
    AuditTarget, MutationObservation, WorkflowObservation, validate_audit_event,
    validate_audit_replay,
)
from src.application.workflow_codec import decode_row,encode_row
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import SCHEMA_VERSION,TABLES

DEFAULT_MAX_OBSERVATION_AGE=timedelta(seconds=30)


def _fresh(observed_at: datetime, recorded_at: datetime, maximum_age: timedelta) -> None:
    if (not isinstance(observed_at,datetime) or observed_at.tzinfo is None or observed_at.utcoffset() is None
            or not isinstance(recorded_at,datetime) or recorded_at.tzinfo is None or recorded_at.utcoffset() is None
            or not isinstance(maximum_age,timedelta) or maximum_age<=timedelta(0)):
        raise ContractError('STALE_AUDIT_OBSERVATION')
    age=recorded_at-observed_at
    if age<timedelta(0) or age>maximum_age:
        raise ContractError('STALE_AUDIT_OBSERVATION')


@dataclass(frozen=True,slots=True)
class AuditEnvelope:
    audit_json: str
    observation: object
    target: AuditTarget | None
    mutation: MutationObservation | None
    observed_at: datetime
    recorded_at: datetime
    trace_id: str

    def __post_init__(self):
        validate_audit_event(self.row(),observation=self.observation,target=self.target,
            mutation=self.mutation,observed_at=self.observed_at,recorded_at=self.recorded_at,
            trace_id=self.trace_id)

    def row(self) -> dict:
        return decode_row(self.audit_json,'audit_events')

    def validate_replay(self, existing: dict) -> None:
        validate_audit_replay(existing,self.row(),observation=self.observation,target=self.target,
            mutation=self.mutation,observed_at=self.observed_at,recorded_at=self.recorded_at,
            trace_id=self.trace_id)


def produce_access_audit(capture: AccessCapture, *, recorded_at: datetime,
                         maximum_age: timedelta = DEFAULT_MAX_OBSERVATION_AGE) -> AuditEnvelope:
    """Validate a fresh access capture and preserve its exact row for replay."""
    if not isinstance(capture,AccessCapture):
        raise ContractError('INVALID_AUDIT_OBSERVATION')
    _fresh(capture.observed_at,recorded_at,maximum_age)
    return AuditEnvelope(capture.audit_json,capture.decision,capture.target,None,
        capture.observed_at,recorded_at,capture.trace_id)


def produce_service_audit(observation: WorkflowObservation, *, target: AuditTarget,
                          mutation: MutationObservation, trace_id: str,
                          observed_at: datetime, recorded_at: datetime,
                          maximum_age: timedelta = DEFAULT_MAX_OBSERVATION_AGE) -> AuditEnvelope:
    """Create an exact service audit row from a fresh typed workflow observation."""
    if (not isinstance(observation,WorkflowObservation) or not isinstance(target,AuditTarget)
            or not isinstance(mutation,MutationObservation)):
        raise ContractError('INVALID_AUDIT_OBSERVATION')
    _fresh(observed_at,recorded_at,maximum_age)
    row=dict.fromkeys(field.name for field in TABLES['audit_events'].schema)
    row.update(schema_version=SCHEMA_VERSION,workspace_id=observation.workspace_id,
        audit_id=f'workflow-v1:{observation.action.value}:{trace_id}',actor_id=observation.worker_id,
        actor_kind='SERVICE',action=observation.action.value,
        allowed=observation.reason.value!='TRANSITION_REJECTED',reason_code=observation.reason.value,
        policy_version=observation.policy_version,resource_type=target.resource_type.value,
        resource_id=target.resource_id,resource_revision=target.revision,
        conversation_id=target.conversation_id,owner_id=target.owner_id,
        request_id=target.request_id,job_id=target.job_id,trace_id=trace_id,
        membership_revision=None,mutation_outcome=mutation.outcome.value,
        new_revision=mutation.new_revision,occurred_at=observed_at)
    return AuditEnvelope(encode_row(row,'audit_events'),observation,target,mutation,
        observed_at,recorded_at,trace_id)
