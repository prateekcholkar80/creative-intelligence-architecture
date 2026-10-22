"""Offline audit observations; no real identities, role assignments or writes."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.access_policy import (
    Action, Membership, MembershipStatus, OwnedResource, Principal, Reason, Role, Visibility, authorize,
)
from src.application.audit_contract import (
    AUDIT_POLICY_VERSION, ActorKind, AuditTarget, MutationKind, MutationObservation, MutationOutcome,
    ResourceType, WorkflowAction, WorkflowObservation, WorkflowReason, validate_audit_event, validate_audit_replay,
)
from src.application.workflow_contract import ContractError
from src.application.workflow_schema import SCHEMA_VERSION, TABLES
from tests.test_workflow_contract import TIME, digest

WORKSPACE = 'fixture-workspace'
ACTOR = Principal('iap-v1:'+digest('fixture subject'))
OTHER = Principal('iap-v1:'+digest('other fixture subject'))
RESOURCE = OwnedResource('fixture-conversation', WORKSPACE, ACTOR.principal_id, access_revision=3)
MEMBER = Membership(ACTOR.principal_id, WORKSPACE, frozenset({Role.CREATOR}), revision=5)
TARGET = AuditTarget(ResourceType.CONVERSATION, RESOURCE.resource_id, WORKSPACE, revision=3,
    conversation_id=RESOURCE.resource_id, owner_id=RESOURCE.owner_id)


def context(action=Action.READ_DRAFT, *, member=MEMBER, principal=ACTOR, resource=RESOURCE, target=TARGET, mutation=None):
    return dict(observation=authorize(principal, member, action, workspace_id=WORKSPACE, resource=resource),
        target=target, mutation=mutation, observed_at=TIME, recorded_at=TIME+timedelta(seconds=1), trace_id='fixture-trace')


def audit(ctx, audit_id='fixture-audit'):
    observation, target, mutation = ctx['observation'], ctx['target'], ctx['mutation']
    service = isinstance(observation, WorkflowObservation)
    row = dict.fromkeys(f.name for f in TABLES['audit_events'].schema)
    row.update(schema_version=SCHEMA_VERSION, workspace_id=observation.workspace_id, audit_id=audit_id,
        actor_id=observation.worker_id if service else observation.principal_id,
        actor_kind='SERVICE' if service else 'USER' if observation.principal_id is not None else 'ANONYMOUS',
        action=observation.action.value, allowed=observation.reason != WorkflowReason.TRANSITION_REJECTED if service else observation.allowed,
        reason_code=observation.reason.value, policy_version=observation.policy_version,
        membership_revision=None if service else observation.membership_revision,
        resource_type=target.resource_type.value if target else None, resource_id=target.resource_id if target else None,
        resource_revision=target.revision if target else None, new_revision=mutation.new_revision if mutation else None,
        mutation_outcome=mutation.outcome.value if mutation else None, trace_id=ctx['trace_id'], occurred_at=ctx['observed_at'])
    for key in ('conversation_id', 'owner_id', 'request_id', 'job_id'):
        row[key] = getattr(target, key) if target else None
    return row


def service_context(action=WorkflowAction.UPDATE_JOB, *, reason=WorkflowReason.TRANSITION_VALIDATED,
                    outcome=MutationOutcome.APPLIED, kind=MutationKind.UPDATE, target_type=ResourceType.JOB):
    target = AuditTarget(target_type, 'fixture-job' if target_type == ResourceType.JOB else 'fixture-record', WORKSPACE,
        revision=7 if kind == MutationKind.UPDATE else None, conversation_id=RESOURCE.resource_id,
        owner_id=ACTOR.principal_id, request_id='fixture-record' if target_type == ResourceType.REQUEST else 'fixture-request',
        job_id=None if target_type == ResourceType.REQUEST else 'fixture-job')
    new_revision = (8 if kind == MutationKind.UPDATE else 1 if kind == MutationKind.CREATE else None) if outcome == MutationOutcome.APPLIED else (
        target.revision if outcome == MutationOutcome.NO_CHANGE else None)
    return dict(observation=WorkflowObservation('fixture-worker', WORKSPACE, action, reason), target=target,
        mutation=MutationObservation(kind, outcome, new_revision), observed_at=TIME,
        recorded_at=TIME+timedelta(seconds=1), trace_id='fixture-trace')


class AuditContractTests(unittest.TestCase):
    def validate(self, ctx=None, candidate=None):
        ctx = context() if ctx is None else ctx
        validate_audit_event(audit(ctx) if candidate is None else candidate, **ctx)

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(ContractError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_owner_read_and_download_capture_access_revisions(self):
        for action in (Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT):
            ctx = context(action); self.validate(ctx)
            self.assertEqual(audit(ctx)['resource_revision'], 3)
            self.assertEqual(audit(ctx)['membership_revision'], 5)

    def test_team_reader_and_reviewer_preserve_owner_separation(self):
        resource = replace(RESOURCE, visibility=Visibility.TEAM)
        for role, action in ((Role.VIEWER, Action.READ_DRAFT), (Role.REVIEWER, Action.REVIEW_FOR_POC)):
            member = Membership(OTHER.principal_id, WORKSPACE, frozenset({role}), revision=2)
            ctx = context(action, member=member, principal=OTHER, resource=resource); self.validate(ctx)
            self.assertEqual(audit(ctx)['reason_code'], 'ALLOWED_TEAM')

    def test_anonymous_denial_has_no_invented_actor_or_membership(self):
        ctx = context(principal=None, member=None, resource=None, target=None); self.validate(ctx)
        row = audit(ctx)
        self.assertEqual(row['actor_kind'], 'ANONYMOUS')
        self.assertIsNone(row['actor_id']); self.assertIsNone(row['membership_revision'])
        for change in (dict(actor_id='invented'), dict(actor_kind='USER'), dict(allowed=True), dict(membership_revision=1)):
            with self.assertRaises(ContractError): self.validate(ctx, row | change)

    def test_missing_suspended_and_revoked_memberships_record_denial(self):
        for member in (None, replace(MEMBER, status=MembershipStatus.SUSPENDED), replace(MEMBER, status=MembershipStatus.REVOKED)):
            ctx = context(member=member); self.validate(ctx)
            self.assertFalse(audit(ctx)['allowed'])

    def test_borrowed_membership_and_foreign_resource_redact_untrusted_target(self):
        for member, resource in ((replace(MEMBER, principal_id=OTHER.principal_id), RESOURCE),
                                  (MEMBER, replace(RESOURCE, workspace_id='other-workspace'))):
            ctx = context(member=member, resource=resource, target=None); self.validate(ctx)
            self.assertIsNone(audit(ctx)['resource_id']); self.assertIsNone(audit(ctx)['owner_id'])

    def test_private_peer_and_owner_only_denials(self):
        member = replace(MEMBER, principal_id=OTHER.principal_id)
        for action in (Action.READ_DRAFT, Action.SET_VISIBILITY, Action.REFINE_DRAFT):
            self.validate(context(action, principal=OTHER, member=member))

    def test_disabled_claim_approval_and_launch_stay_denied(self):
        admin = replace(MEMBER, roles=frozenset(Role))
        target = AuditTarget(ResourceType.WORKSPACE, WORKSPACE, WORKSPACE, revision=2)
        for action in (Action.APPROVE_BENEFIT_CLAIMS, Action.LAUNCH_CAMPAIGN):
            ctx = context(action, member=admin, resource=None, target=target); self.validate(ctx)
            with self.assertRaises(ContractError): self.validate(ctx | dict(observation=replace(ctx['observation'], allowed=True, reason=Reason.ALLOWED_WORKSPACE_ACTION)))

    def test_workspace_generation_and_admin_decisions(self):
        target = AuditTarget(ResourceType.WORKSPACE, WORKSPACE, WORKSPACE, revision=4)
        for action, member in ((Action.GENERATE_DRAFT, MEMBER), *[(a, replace(MEMBER, roles=frozenset({Role.ADMIN})))
                for a in (Action.MANAGE_MEMBERS, Action.MANAGE_MODELS, Action.MANAGE_LIMITS)]):
            self.validate(context(action, member=member, resource=None, target=target))

    def test_all_access_denial_reasons_from_policy_are_supported(self):
        cases = [
            context(principal=None, member=None, resource=None, target=None), context(member=None),
            context(member=replace(MEMBER, status=MembershipStatus.REVOKED)),
            context(member=replace(MEMBER, principal_id=OTHER.principal_id), target=None),
            context(member=replace(MEMBER, workspace_id='other-workspace'), target=None),
            context(Action.LAUNCH_CAMPAIGN, member=replace(MEMBER, roles=frozenset(Role)), resource=None, target=None),
            context(member=replace(MEMBER, roles=frozenset())), context(resource=None, target=None),
            context(Action.GENERATE_DRAFT, target=AuditTarget(ResourceType.CONVERSATION, RESOURCE.resource_id,
                WORKSPACE, revision=3, conversation_id=RESOURCE.resource_id, owner_id=RESOURCE.owner_id)),
            context(principal=OTHER, member=replace(MEMBER, principal_id=OTHER.principal_id)),
            context(Action.SET_VISIBILITY, principal=OTHER, member=replace(MEMBER, principal_id=OTHER.principal_id)),
        ]
        expected = set(Reason) - {Reason.ALLOWED_OWNER, Reason.ALLOWED_TEAM, Reason.ALLOWED_WORKSPACE_ACTION}
        self.assertEqual({c['observation'].reason for c in cases}, expected)
        for ctx in cases:
            with self.subTest(reason=ctx['observation'].reason): self.validate(ctx)

    def test_exact_schema_no_sensitive_or_free_metadata_fields(self):
        ctx = context(); row = audit(ctx)
        for key in ('jwt', 'email', 'prompt', 'response', 'notes', 'exception', 'metadata', 'cost'):
            with self.subTest(key=key), self.assertRaises(ContractError): self.validate(ctx, row | {key: 'secret'})
        for key in row:
            candidate = deepcopy(row); del candidate[key]
            with self.subTest(key=key), self.assertRaises(ContractError): self.validate(ctx, candidate)

    def test_row_binds_every_observed_field_and_identity(self):
        ctx = context(); row = audit(ctx)
        for key in ('workspace_id', 'actor_id', 'actor_kind', 'action', 'reason_code', 'policy_version',
                    'resource_type', 'resource_id', 'conversation_id', 'owner_id', 'request_id', 'job_id', 'trace_id'):
            with self.subTest(key=key), self.assertRaises(ContractError): self.validate(ctx, row | {key: 'other'})
        for change in (dict(schema_version='v2'), dict(membership_revision=6), dict(resource_revision=4),
                       dict(new_revision=4), dict(mutation_outcome='APPLIED'), dict(allowed=False)):
            with self.assertRaises(ContractError): self.validate(ctx, row | change)

    def test_strict_types_int64_and_timestamps(self):
        ctx = context(); row = audit(ctx)
        for change in (dict(allowed=1), dict(allowed='true'), dict(resource_revision=True), dict(resource_revision=2**63),
                       dict(occurred_at=TIME.isoformat()), dict(occurred_at=TIME.replace(tzinfo=None))):
            with self.assertRaises(ContractError): self.validate(ctx, row | change)
        with self.assertRaises(ContractError): self.validate(ctx | dict(observed_at=TIME.replace(tzinfo=None)))
        with self.assertRaises(ContractError): self.validate(ctx | dict(recorded_at=TIME.replace(tzinfo=None)))
        with self.assertRaises(ContractError): self.validate(ctx | dict(recorded_at=TIME-timedelta(seconds=1)))
        with self.assertRaises(ContractError): self.validate(ctx, row | dict(occurred_at=TIME+timedelta(seconds=1)))

    def test_email_jwt_uri_unicode_and_free_text_identifiers_rejected(self):
        for value in ('user@example.com', 'eyJ.payload.signature', 'gs://private/image', 'natural language',
                      'worker\nsecret', 'éclair', 'x'*257, '', '_bad'):
            with self.subTest(value=value), self.assertRaises(ContractError):
                self.validate(context() | dict(observation=replace(context()['observation'], principal_id=value)))
            with self.subTest(value=value), self.assertRaises(ContractError): WorkflowObservation(value, WORKSPACE, WorkflowAction.UPDATE_JOB)
        for value in ('audit@example.com', 'jwt.payload.signature', 'x'*129):
            with self.assertRaises(ContractError): self.validate(candidate=audit(context()) | dict(audit_id=value))

    def test_decision_requires_typed_enums_bool_and_current_policy(self):
        for change in (dict(action='READ_DRAFT'), dict(reason='ALLOWED_OWNER'), dict(allowed=1), dict(policy_version='future'),
                       dict(membership_revision=0), dict(resource_revision=True), dict(principal_id=None)):
            with self.assertRaises(ContractError): self.validate(context() | dict(observation=replace(context()['observation'], **change)), audit(context()))
        with self.assertRaises(ContractError): self.validate(context() | dict(observation={'allowed': True}), audit(context()))

    def test_reason_and_allow_flag_cannot_contradict(self):
        for change in (dict(allowed=False), dict(reason=Reason.PRIVATE_RESOURCE),
                       dict(reason=Reason.ALLOWED_WORKSPACE_ACTION), dict(resource_id=None), dict(resource_revision=None)):
            with self.assertRaises(ContractError): self.validate(context() | dict(observation=replace(context()['observation'], **change)))

    def test_unknown_target_allowed_only_for_denial(self):
        with self.assertRaises(ContractError): self.validate(context(target=None))
        self.validate(context(member=None, target=None))
        with self.assertRaises(ContractError): self.validate(context(target={'resource_id': 'fake'}), audit(context()))

    def test_access_resource_is_parent_conversation_and_revision(self):
        for target in (replace(TARGET, revision=4), replace(TARGET, workspace_id='other-workspace'),
                       AuditTarget(ResourceType.CONVERSATION, 'other', WORKSPACE, revision=3,
                                   conversation_id='other', owner_id=ACTOR.principal_id)):
            with self.assertRaises(ContractError): self.validate(context(target=target))
        child = AuditTarget(ResourceType.OUTPUT, 'fixture-output', WORKSPACE, conversation_id=RESOURCE.resource_id,
                            owner_id=ACTOR.principal_id, request_id='fixture-request', job_id='fixture-job')
        with self.assertRaises(ContractError): self.validate(context(target=child))

    def test_conversation_access_can_capture_known_request_job_links(self):
        ctx = context(target=replace(TARGET, request_id='fixture-request', job_id='fixture-job')); self.validate(ctx)
        self.assertEqual(audit(ctx)['job_id'], 'fixture-job')

    def test_owner_team_reason_must_match_captured_target_owner(self):
        with self.assertRaises(ContractError): self.validate(context(target=replace(TARGET, owner_id=OTHER.principal_id)))
        ctx = context(member=replace(MEMBER, principal_id=OTHER.principal_id), principal=OTHER,
                      resource=replace(RESOURCE, visibility=Visibility.TEAM), target=replace(TARGET, owner_id=OTHER.principal_id))
        with self.assertRaises(ContractError): self.validate(ctx)

    def test_visibility_outcomes_preserve_authorization_and_revision_semantics(self):
        for outcome, revision in ((MutationOutcome.APPLIED, 4), (MutationOutcome.NO_CHANGE, 3),
                                  (MutationOutcome.REJECTED, None), (MutationOutcome.UNKNOWN, None)):
            ctx = context(Action.SET_VISIBILITY, mutation=MutationObservation(MutationKind.UPDATE, outcome, revision))
            self.validate(ctx); self.assertTrue(audit(ctx)['allowed'])

    def test_applied_update_requires_exact_increment_no_change_keeps_revision(self):
        for outcome, revisions in ((MutationOutcome.APPLIED, (None, 3, 5)), (MutationOutcome.NO_CHANGE, (None, 4)),
                                   (MutationOutcome.UNKNOWN, (3, 4)), (MutationOutcome.REJECTED, (3, 4))):
            for revision in revisions:
                ctx = context(Action.SET_VISIBILITY, mutation=MutationObservation(MutationKind.UPDATE, outcome, revision))
                with self.assertRaises(ContractError): self.validate(ctx)

    def test_denied_mutation_can_only_record_rejected(self):
        viewer = replace(MEMBER, roles=frozenset({Role.VIEWER}))
        self.validate(context(Action.SET_VISIBILITY, member=viewer,
            mutation=MutationObservation(MutationKind.UPDATE, MutationOutcome.REJECTED)))
        for outcome, revision in ((MutationOutcome.APPLIED, 4), (MutationOutcome.NO_CHANGE, 3), (MutationOutcome.UNKNOWN, None)):
            with self.assertRaises(ContractError): self.validate(context(Action.SET_VISIBILITY, member=viewer,
                mutation=MutationObservation(MutationKind.UPDATE, outcome, revision)))

    def test_reads_and_policy_only_review_refinement_cannot_claim_mutations(self):
        mutation = MutationObservation(MutationKind.UPDATE, MutationOutcome.APPLIED, 4)
        for action in (Action.READ_DRAFT, Action.DOWNLOAD_ARTIFACT, Action.REFINE_DRAFT, Action.REVIEW_FOR_POC):
            with self.assertRaises(ContractError): self.validate(context(action, mutation=mutation))

    def test_admin_membership_mutation_can_target_other_member(self):
        admin = replace(MEMBER, roles=frozenset({Role.ADMIN}))
        target = AuditTarget(ResourceType.MEMBERSHIP, OTHER.principal_id, WORKSPACE, revision=9)
        ctx = context(Action.MANAGE_MEMBERS, member=admin, resource=None, target=target,
            mutation=MutationObservation(MutationKind.UPDATE, MutationOutcome.APPLIED, 10))
        self.validate(ctx)
        self.assertNotEqual(audit(ctx)['actor_id'], audit(ctx)['resource_id'])

    def test_creating_generation_request_has_no_old_revision_and_new_one(self):
        target = AuditTarget(ResourceType.REQUEST, 'fixture-request', WORKSPACE, conversation_id=RESOURCE.resource_id,
                            owner_id=ACTOR.principal_id, request_id='fixture-request')
        ctx = context(Action.GENERATE_DRAFT, resource=None, target=target,
            mutation=MutationObservation(MutationKind.CREATE, MutationOutcome.APPLIED, 1)); self.validate(ctx)
        for change in (dict(new_revision=None), dict(new_revision=2)):
            with self.assertRaises(ContractError): self.validate(ctx | dict(mutation=replace(ctx['mutation'], **change)))
        with self.assertRaises(ContractError): self.validate(ctx | dict(target=replace(target, owner_id=OTHER.principal_id)))

    def test_user_generation_cannot_create_workspace_or_update_job(self):
        target = AuditTarget(ResourceType.WORKSPACE, WORKSPACE, WORKSPACE)
        with self.assertRaises(ContractError): self.validate(context(Action.GENERATE_DRAFT, resource=None, target=target,
            mutation=MutationObservation(MutationKind.CREATE, MutationOutcome.APPLIED, 1)))
        with self.assertRaises(ContractError): self.validate(context(Action.GENERATE_DRAFT, resource=None,
            mutation=MutationObservation(MutationKind.UPDATE, MutationOutcome.APPLIED, 4)))

    def test_each_supported_service_action_has_scoped_target_and_mutation_kind(self):
        cases = [
            (WorkflowAction.CREATE_REQUEST, ResourceType.REQUEST, MutationKind.CREATE),
            (WorkflowAction.CREATE_JOB, ResourceType.JOB, MutationKind.CREATE),
            (WorkflowAction.CLAIM_ATTEMPT, ResourceType.JOB_ATTEMPT, MutationKind.CREATE),
            (WorkflowAction.UPDATE_JOB, ResourceType.JOB, MutationKind.UPDATE),
            (WorkflowAction.UPDATE_ATTEMPT, ResourceType.JOB_ATTEMPT, MutationKind.UPDATE),
            *[(a, ResourceType.MODEL_INVOCATION, MutationKind.CREATE if a == WorkflowAction.CREATE_MODEL_INVOCATION else MutationKind.UPDATE)
              for a in (WorkflowAction.CREATE_MODEL_INVOCATION, WorkflowAction.CLAIM_MODEL_DISPATCH,
                        WorkflowAction.RECORD_MODEL_RESULT, WorkflowAction.RECORD_MODEL_ACCOUNTING)],
            (WorkflowAction.APPEND_STAGE_EVENT, ResourceType.STAGE_EVENT, MutationKind.APPEND),
            (WorkflowAction.APPEND_EVIDENCE, ResourceType.EVIDENCE_SNAPSHOT, MutationKind.APPEND),
            (WorkflowAction.APPEND_OUTPUT, ResourceType.OUTPUT, MutationKind.APPEND),
        ]
        self.assertEqual({a for a, _, _ in cases}, set(WorkflowAction))
        for action, target_type, kind in cases:
            with self.subTest(action=action): self.validate(service_context(action, target_type=target_type, kind=kind))

    def test_service_records_use_worker_not_user_membership(self):
        ctx = service_context(); self.validate(ctx)
        row = audit(ctx)
        self.assertEqual(row['actor_kind'], ActorKind.SERVICE.value)
        self.assertIsNone(row['membership_revision'])
        for change in (dict(actor_kind='USER'), dict(actor_id=ACTOR.principal_id), dict(membership_revision=5), dict(policy_version='marketing-access-v1')):
            with self.assertRaises(ContractError): self.validate(ctx, row | change)

    def test_service_reason_outcome_pairs_are_explicit(self):
        for reason, outcome in ((WorkflowReason.TRANSITION_VALIDATED, MutationOutcome.NO_CHANGE),
                               (WorkflowReason.TRANSITION_REJECTED, MutationOutcome.REJECTED),
                               (WorkflowReason.OUTCOME_UNKNOWN, MutationOutcome.UNKNOWN)):
            ctx = service_context(reason=reason, outcome=outcome); self.validate(ctx)
            if outcome == MutationOutcome.UNKNOWN: self.assertIsNone(audit(ctx)['new_revision'])
        for reason, outcome in ((WorkflowReason.TRANSITION_VALIDATED, MutationOutcome.UNKNOWN),
                               (WorkflowReason.TRANSITION_REJECTED, MutationOutcome.APPLIED),
                               (WorkflowReason.OUTCOME_UNKNOWN, MutationOutcome.APPLIED)):
            with self.assertRaises(ContractError): self.validate(service_context(reason=reason, outcome=outcome))

    def test_service_action_cannot_target_wrong_resource_kind_workspace_or_operation(self):
        ctx = service_context()
        for change in (dict(target=replace(ctx['target'], resource_type=ResourceType.JOB_ATTEMPT)),
                       dict(target=replace(ctx['target'], workspace_id='other-workspace')),
                       dict(mutation=MutationObservation(MutationKind.CREATE, MutationOutcome.APPLIED, 1)),
                       dict(target=None), dict(mutation=None)):
            with self.assertRaises(ContractError): self.validate(ctx | change)

    def test_interpreter_invocation_can_have_request_without_job(self):
        ctx = service_context(WorkflowAction.CREATE_MODEL_INVOCATION, target_type=ResourceType.MODEL_INVOCATION, kind=MutationKind.CREATE)
        ctx['target'] = replace(ctx['target'], job_id=None); self.validate(ctx)

    def test_append_only_records_do_not_invent_revisions(self):
        for outcome in (MutationOutcome.APPLIED, MutationOutcome.NO_CHANGE, MutationOutcome.REJECTED, MutationOutcome.UNKNOWN):
            reason = WorkflowReason.TRANSITION_REJECTED if outcome == MutationOutcome.REJECTED else (
                WorkflowReason.OUTCOME_UNKNOWN if outcome == MutationOutcome.UNKNOWN else WorkflowReason.TRANSITION_VALIDATED)
            ctx = service_context(WorkflowAction.APPEND_OUTPUT, target_type=ResourceType.OUTPUT,
                                  kind=MutationKind.APPEND, outcome=outcome, reason=reason)
            self.validate(ctx)
            with self.assertRaises(ContractError): self.validate(ctx | dict(mutation=replace(ctx['mutation'], new_revision=1)))

    def test_context_dataclasses_are_frozen_and_require_typed_values(self):
        with self.assertRaises(FrozenInstanceError): TARGET.revision = 4
        for function, args in ((AuditTarget, ('CONVERSATION', 'x', WORKSPACE)),
                               (MutationObservation, ('UPDATE', MutationOutcome.APPLIED)),
                               (MutationObservation, (MutationKind.UPDATE, 'APPLIED')),
                               (WorkflowObservation, ('worker', WORKSPACE, 'UPDATE_JOB'))):
            with self.assertRaises(ContractError): function(*args)
        with self.assertRaises(ContractError): WorkflowObservation('worker', WORKSPACE, WorkflowAction.UPDATE_JOB, policy_version='future')

    def test_target_requires_complete_scope_without_unrelated_links(self):
        for values in (dict(resource_type=ResourceType.WORKSPACE, resource_id='wrong', workspace_id=WORKSPACE),
                       dict(resource_type=ResourceType.MEMBERSHIP, resource_id=OTHER.principal_id, workspace_id=WORKSPACE, owner_id=OTHER.principal_id),
                       dict(resource_type=ResourceType.CONVERSATION, resource_id='conversation', workspace_id=WORKSPACE),
                       dict(resource_type=ResourceType.JOB, resource_id='job', workspace_id=WORKSPACE, conversation_id='conversation', owner_id=ACTOR.principal_id, request_id='request'),
                       dict(resource_type=ResourceType.CONVERSATION, resource_id='conversation', workspace_id=WORKSPACE, conversation_id='conversation', owner_id=ACTOR.principal_id, job_id='job')):
            with self.assertRaises(ContractError): AuditTarget(**values)

    def test_revision_bounds_and_overflow_rejected(self):
        for value in (0, -1, True, 1.0, 2**63):
            with self.assertRaises(ContractError): replace(TARGET, revision=value)
            with self.assertRaises(ContractError): MutationObservation(MutationKind.UPDATE, MutationOutcome.APPLIED, value)
        target = replace(TARGET, revision=2**63-1)
        ctx = context(Action.SET_VISIBILITY, target=target,
            mutation=MutationObservation(MutationKind.UPDATE, MutationOutcome.APPLIED, 2**63-1))
        ctx['observation'] = replace(ctx['observation'], resource_revision=target.revision)
        with self.assertRaises(ContractError): self.validate(ctx)

    def test_replay_requires_identical_logical_key_and_content(self):
        ctx = context(); row = audit(ctx)
        validate_audit_replay(row, deepcopy(row), **ctx)
        self.assert_code('AUDIT_REPLAY_KEY_MISMATCH', validate_audit_replay, row, row | dict(audit_id='other-audit'), **ctx)
        self.assert_code('AUDIT_IDEMPOTENCY_CONFLICT', validate_audit_replay, row, row | dict(allowed=False), **ctx)

    def test_replay_does_not_relabel_actor_or_rewrite_outcome(self):
        ctx = service_context(); row = audit(ctx)
        for change in (dict(actor_id='other-worker'), dict(mutation_outcome='UNKNOWN', new_revision=None),
                       dict(reason_code='TRANSITION_REJECTED'), dict(occurred_at=TIME+timedelta(seconds=1))):
            self.assert_code('AUDIT_IDEMPOTENCY_CONFLICT', validate_audit_replay, row, row | change, **ctx)

    def test_create_and_update_cannot_swap_revision_semantics(self):
        ctx = service_context(WorkflowAction.CREATE_JOB, kind=MutationKind.CREATE)
        with self.assertRaises(ContractError): self.validate(ctx | dict(target=replace(ctx['target'], revision=1)))
        ctx = service_context()
        with self.assertRaises(ContractError): self.validate(ctx | dict(target=replace(ctx['target'], revision=None)))

    def test_create_no_change_and_append_revisions_are_invalid(self):
        ctx = service_context(WorkflowAction.CREATE_JOB, kind=MutationKind.CREATE, outcome=MutationOutcome.NO_CHANGE)
        with self.assertRaises(ContractError): self.validate(ctx)
        with self.assertRaises(ContractError): AuditTarget(ResourceType.OUTPUT, 'output', WORKSPACE, revision=1,
            conversation_id=RESOURCE.resource_id, owner_id=ACTOR.principal_id, request_id='request', job_id='job')

    def test_authenticated_decision_cannot_use_anonymous_reason_or_email_target(self):
        ctx = context()
        with self.assertRaises(ContractError): self.validate(ctx | dict(observation=replace(ctx['observation'],
            allowed=False, reason=Reason.AUTHENTICATION_REQUIRED)), audit(ctx))
        with self.assertRaises(ContractError): AuditTarget(ResourceType.MEMBERSHIP, 'user@example.com', WORKSPACE)

    def test_replay_uses_original_observation_not_a_later_membership_decision(self):
        ctx = context(); row = audit(ctx)
        later = context(member=replace(MEMBER, status=MembershipStatus.REVOKED, revision=6))
        with self.assertRaises(ContractError): validate_audit_replay(row, row, **later)
        validate_audit_replay(row, row, **ctx)  # Historical consistency is not current permission.

    def test_validation_never_authenticates_calls_cloud_or_mutates_inputs(self):
        ctx = context(); row = audit(ctx); before = deepcopy(row)
        with (patch('google.cloud.bigquery.Client', side_effect=AssertionError('no BQ')),
              patch('google.cloud.storage.Client', side_effect=AssertionError('no GCS')),
              patch('src.application.identity.IapIdentityAdapter.authenticate', side_effect=AssertionError('no authentication'))):
            self.validate(ctx, row); validate_audit_replay(row, deepcopy(row), **ctx)
        self.assertEqual(row, before)

    def test_errors_never_echo_sensitive_content(self):
        ctx = context(); row = audit(ctx) | dict(reason_code='private exception user@example.com')
        with self.assertRaises(ContractError) as caught: self.validate(ctx, row)
        self.assertNotIn('private', str(caught.exception)); self.assertNotIn('@', str(caught.exception))


if __name__ == '__main__':
    unittest.main()
