"""Pure marketing-team permissions and ownership policy (A1 foundation).

Inputs must come from a verified identity adapter and current authoritative
membership/resource records. These classes do not verify JWTs or read BigQuery.
Never construct them from a campaign prompt, browser role field or session ID.
The local chat does not yet call this module; A2/A3 integrate that boundary.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType

POLICY_VERSION = 'marketing-access-v1'


class Role(str, Enum):
    VIEWER = 'VIEWER'
    CREATOR = 'CREATOR'
    REVIEWER = 'REVIEWER'
    ADMIN = 'ADMIN'


class MembershipStatus(str, Enum):
    ACTIVE = 'ACTIVE'
    SUSPENDED = 'SUSPENDED'
    REVOKED = 'REVOKED'


class Visibility(str, Enum):
    PRIVATE = 'PRIVATE'
    TEAM = 'TEAM'


class Action(str, Enum):
    READ_DRAFT = 'READ_DRAFT'
    DOWNLOAD_ARTIFACT = 'DOWNLOAD_ARTIFACT'
    GENERATE_DRAFT = 'GENERATE_DRAFT'
    REFINE_DRAFT = 'REFINE_DRAFT'
    SET_VISIBILITY = 'SET_VISIBILITY'
    REVIEW_FOR_POC = 'REVIEW_FOR_POC'
    MANAGE_MEMBERS = 'MANAGE_MEMBERS'
    MANAGE_MODELS = 'MANAGE_MODELS'
    MANAGE_LIMITS = 'MANAGE_LIMITS'
    MANAGE_WORKSPACE = 'MANAGE_WORKSPACE'
    ARCHIVE_CONVERSATION = 'ARCHIVE_CONVERSATION'
    APPROVE_BENEFIT_CLAIMS = 'APPROVE_BENEFIT_CLAIMS'
    LAUNCH_CAMPAIGN = 'LAUNCH_CAMPAIGN'


class Reason(str, Enum):
    AUTHENTICATION_REQUIRED = 'AUTHENTICATION_REQUIRED'
    MEMBERSHIP_REQUIRED = 'MEMBERSHIP_REQUIRED'
    MEMBERSHIP_INACTIVE = 'MEMBERSHIP_INACTIVE'
    PRINCIPAL_MISMATCH = 'PRINCIPAL_MISMATCH'
    WORKSPACE_MISMATCH = 'WORKSPACE_MISMATCH'
    ACTION_NOT_ENABLED = 'ACTION_NOT_ENABLED'
    ROLE_REQUIRED = 'ROLE_REQUIRED'
    RESOURCE_REQUIRED = 'RESOURCE_REQUIRED'
    UNEXPECTED_RESOURCE = 'UNEXPECTED_RESOURCE'
    PRIVATE_RESOURCE = 'PRIVATE_RESOURCE'
    OWNER_REQUIRED = 'OWNER_REQUIRED'
    ALLOWED_WORKSPACE_ACTION = 'ALLOWED_WORKSPACE_ACTION'
    ALLOWED_OWNER = 'ALLOWED_OWNER'
    ALLOWED_TEAM = 'ALLOWED_TEAM'


_READ = frozenset({Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT})
ROLE_ACTIONS = MappingProxyType({
    Role.VIEWER: _READ,
    Role.CREATOR: _READ | {Action.GENERATE_DRAFT, Action.REFINE_DRAFT, Action.SET_VISIBILITY, Action.ARCHIVE_CONVERSATION},
    Role.REVIEWER: _READ | {Action.REVIEW_FOR_POC},
    Role.ADMIN: frozenset({Action.MANAGE_MEMBERS, Action.MANAGE_MODELS, Action.MANAGE_LIMITS, Action.MANAGE_WORKSPACE}),
})
_RESOURCE_ACTIONS = _READ | {Action.REFINE_DRAFT, Action.SET_VISIBILITY, Action.REVIEW_FOR_POC, Action.ARCHIVE_CONVERSATION}
_OWNER_ACTIONS = frozenset({Action.REFINE_DRAFT, Action.SET_VISIBILITY, Action.ARCHIVE_CONVERSATION})
_DISABLED_ACTIONS = frozenset({Action.APPROVE_BENEFIT_CLAIMS, Action.LAUNCH_CAMPAIGN})


def _identifier(value: str, name: str, maximum: int = 256) -> None:
    if (not isinstance(value, str) or not value or len(value) > maximum
            or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)):
        raise ValueError(f'{name} must be a bounded opaque identifier without whitespace/control characters')


def _revision(value: int) -> None:
    if type(value) is not int or value < 1:
        raise ValueError('Authorization revisions must be positive integers')


@dataclass(frozen=True, slots=True)
class Principal:
    """Normalized subject from the server identity verifier, not authentication proof."""
    principal_id: str

    def __post_init__(self) -> None:
        _identifier(self.principal_id, 'principal_id')


@dataclass(frozen=True, slots=True)
class Membership:
    principal_id: str
    workspace_id: str
    roles: frozenset[Role]
    status: MembershipStatus = MembershipStatus.ACTIVE
    revision: int = 1

    def __post_init__(self) -> None:
        _identifier(self.principal_id, 'principal_id')
        _identifier(self.workspace_id, 'workspace_id', 128)
        if (not isinstance(self.roles, frozenset) or any(not isinstance(r, Role) for r in self.roles)
                or not isinstance(self.status, MembershipStatus)):
            raise ValueError('Membership requires typed, immutable roles and status')
        _revision(self.revision)


@dataclass(frozen=True, slots=True)
class OwnedResource:
    """Access metadata for a server-loaded conversation/job/output or its artifact."""
    resource_id: str
    workspace_id: str
    owner_id: str
    visibility: Visibility = Visibility.PRIVATE
    access_revision: int = 1

    def __post_init__(self) -> None:
        _identifier(self.resource_id, 'resource_id', 128)
        _identifier(self.workspace_id, 'workspace_id', 128)
        _identifier(self.owner_id, 'owner_id')
        if not isinstance(self.visibility, Visibility):
            raise ValueError('Resource visibility must be explicitly PRIVATE or TEAM')
        _revision(self.access_revision)


@dataclass(frozen=True, slots=True)
class AccessDecision:
    allowed: bool
    action: Action
    reason: Reason
    workspace_id: str
    principal_id: str | None
    membership_revision: int | None
    resource_id: str | None
    resource_revision: int | None
    policy_version: str = POLICY_VERSION

    def to_dict(self) -> dict:
        """Audit-ready decision; persistence/redaction are the service's responsibility."""
        return dict(allowed=self.allowed, action=self.action.value, reason=self.reason.value,
            workspace_id=self.workspace_id, principal_id=self.principal_id,
            membership_revision=self.membership_revision, resource_id=self.resource_id,
            resource_revision=self.resource_revision, policy_version=self.policy_version)


class AuthorizationError(PermissionError):
    def __init__(self, decision: AccessDecision):
        self.decision = decision
        super().__init__('The current identity and permissions do not allow this action.')


def authorize(principal: Principal | None, membership: Membership | None, action: Action,
              *, workspace_id: str, resource: OwnedResource | None = None) -> AccessDecision:
    """Evaluate current server-owned inputs; roles are additive, without admin bypass."""
    _identifier(workspace_id, 'workspace_id', 128)
    if (not isinstance(action, Action) or (principal is not None and not isinstance(principal, Principal))
            or (membership is not None and not isinstance(membership, Membership))
            or (resource is not None and not isinstance(resource, OwnedResource))):
        raise ValueError('Access policy requires typed server-side inputs')

    def decision(allowed: bool, reason: Reason) -> AccessDecision:
        return AccessDecision(allowed, action, reason, workspace_id,
            principal.principal_id if principal else None, membership.revision if membership else None,
            resource.resource_id if resource else None, resource.access_revision if resource else None)

    if principal is None:
        return decision(False, Reason.AUTHENTICATION_REQUIRED)
    if membership is None:
        return decision(False, Reason.MEMBERSHIP_REQUIRED)
    if membership.principal_id != principal.principal_id:
        return decision(False, Reason.PRINCIPAL_MISMATCH)
    if (membership.workspace_id != workspace_id
            or (resource is not None and resource.workspace_id != workspace_id)):
        return decision(False, Reason.WORKSPACE_MISMATCH)
    if membership.status != MembershipStatus.ACTIVE:
        return decision(False, Reason.MEMBERSHIP_INACTIVE)
    if action in _DISABLED_ACTIONS:
        return decision(False, Reason.ACTION_NOT_ENABLED)
    permissions = frozenset(a for role in membership.roles for a in ROLE_ACTIONS[role])
    if action not in permissions:
        return decision(False, Reason.ROLE_REQUIRED)
    if action not in _RESOURCE_ACTIONS:
        if resource is not None:
            return decision(False, Reason.UNEXPECTED_RESOURCE)
        return decision(True, Reason.ALLOWED_WORKSPACE_ACTION)
    if resource is None:
        return decision(False, Reason.RESOURCE_REQUIRED)
    owned = resource.owner_id == principal.principal_id
    if action in _OWNER_ACTIONS and not owned:
        return decision(False, Reason.OWNER_REQUIRED)
    if resource.visibility == Visibility.PRIVATE and not owned:
        return decision(False, Reason.PRIVATE_RESOURCE)
    return decision(True, Reason.ALLOWED_OWNER if owned else Reason.ALLOWED_TEAM)


def require_authorized(principal: Principal | None, membership: Membership | None, action: Action,
                       *, workspace_id: str, resource: OwnedResource | None = None) -> AccessDecision:
    result = authorize(principal, membership, action, workspace_id=workspace_id, resource=resource)
    if not result.allowed:
        raise AuthorizationError(result)
    return result


def new_draft_access(principal: Principal, membership: Membership, *, workspace_id: str,
                     resource_id: str) -> OwnedResource:
    """Propose access for a new ID; the persistence layer must prevent replacing an existing owner."""
    require_authorized(principal, membership, Action.GENERATE_DRAFT, workspace_id=workspace_id)
    return OwnedResource(resource_id, workspace_id, principal.principal_id)


def change_visibility(principal: Principal, membership: Membership, resource: OwnedResource,
                      visibility: Visibility) -> OwnedResource:
    """Propose a visibility change only; later persistence must compare revisions and audit it."""
    if not isinstance(visibility, Visibility):
        raise ValueError('Visibility must be explicitly PRIVATE or TEAM')
    require_authorized(principal, membership, Action.SET_VISIBILITY,
                       workspace_id=resource.workspace_id, resource=resource)
    if visibility == resource.visibility:
        return resource
    return replace(resource, visibility=visibility, access_revision=resource.access_revision + 1)
