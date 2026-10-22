"""Server-captured access/audit handoff; no persistence or grant from HTTP status."""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from contextlib import contextmanager
from contextvars import ContextVar
import re

from src.application.access_policy import AccessDecision, Action, Reason
from src.application.audit_contract import AuditTarget, ResourceType, validate_audit_event
from src.application.workflow_codec import decode_row, encode_row
from src.application.workflow_schema import SCHEMA_VERSION

_DELIVERY_TRACE: ContextVar[str | None] = ContextVar('creative_delivery_trace',default=None)


@contextmanager
def delivery_trace(trace_id: str):
    """Server-created HTTP correlation only, never a browser identity or grant."""
    if type(trace_id) is not str or re.fullmatch(r'[0-9a-f]{32}',trace_id) is None:
        raise ValueError('A server-created delivery trace is required')
    token=_DELIVERY_TRACE.set(trace_id)
    try:yield
    finally:_DELIVERY_TRACE.reset(token)


def current_delivery_trace():
    return _DELIVERY_TRACE.get()


@dataclass(frozen=True, slots=True)
class AccessCapture:
    decision: AccessDecision
    target: AuditTarget | None
    observed_at: datetime
    trace_id: str
    audit_json: str

    def __post_init__(self):
        validate_audit_event(decode_row(self.audit_json,'audit_events'), observation=self.decision,
            target=self.target,observed_at=self.observed_at,recorded_at=self.observed_at,trace_id=self.trace_id)

    def wire(self):
        return dict(decision=asdict(self.decision),target=None if self.target is None else asdict(self.target),
            observed_at=self.observed_at.isoformat(),trace_id=self.trace_id,audit_json=self.audit_json)


def capture_access(decision: AccessDecision, *, target: AuditTarget | None, trace_id: str,
                   observed_at: datetime | None = None) -> AccessCapture:
    now=datetime.now(timezone.utc) if observed_at is None else observed_at
    row=dict(schema_version=SCHEMA_VERSION,workspace_id=decision.workspace_id,
        audit_id='access-v1:'+trace_id,actor_id=decision.principal_id,
        actor_kind='USER' if decision.principal_id is not None else 'ANONYMOUS',action=decision.action.value,
        allowed=decision.allowed,reason_code=decision.reason.value,policy_version=decision.policy_version,
        membership_revision=decision.membership_revision,trace_id=trace_id,occurred_at=now,
        mutation_outcome=None,new_revision=None,resource_type=None if target is None else target.resource_type.value,
        resource_id=None if target is None else target.resource_id,resource_revision=None if target is None else target.revision,
        **{key:None if target is None else getattr(target,key) for key in ('conversation_id','owner_id','request_id','job_id')})
    return AccessCapture(decision,target,now,trace_id,encode_row(row,'audit_events'))


def capture_from_wire(value) -> AccessCapture:
    if type(value) is not dict or set(value)!={'decision','target','observed_at','trace_id','audit_json'}:
        raise ValueError('Invalid captured access projection')
    if type(value['decision']) is not dict or set(value['decision'])!=set(AccessDecision.__dataclass_fields__):
        raise ValueError('Invalid captured decision projection')
    decision=dict(value['decision']);decision['action']=Action(decision['action']);decision['reason']=Reason(decision['reason'])
    target=None
    if value['target'] is not None:
        if type(value['target']) is not dict or set(value['target'])!=set(AuditTarget.__dataclass_fields__):
            raise ValueError('Invalid captured target projection')
        raw=dict(value['target']);raw['resource_type']=ResourceType(raw['resource_type']);target=AuditTarget(**raw)
    return AccessCapture(AccessDecision(**decision),target,datetime.fromisoformat(value['observed_at']),
                         value['trace_id'],value['audit_json'])
