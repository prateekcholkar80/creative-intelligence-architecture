"""Synthetic lifecycle fixtures: SQL observations, never campaign quality/approval."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from src.application.prompt_contract import PromptLifecycle, initial_prompt_record, propose_prompt_transition
from src.application.prompt_evaluation_storage import encode_control, encode_evaluation
from src.application.prompt_registry_codec import PromptRegistryState, encode_registry_state
from src.validation.control_fixtures import controls
from src.validation.prompt_evaluation_fixtures import UNRELATED, fixtures

SCENARIOS = ('flow', 'recovery', 'retry', 'cap', 'postcommit', 'unknown',
             'expiry', 'assertion', 'policy', 'revocation', 'race', 'reservation')


def lifecycle_fixtures(now=None):
    now = datetime.now(timezone.utc) if now is None else now
    _, base, control_plan, report_plan = fixtures(now)
    rows = {name: [] for name in ('workspaces', 'memberships', 'prompt_registry',
        'prompt_registry_history', 'prompt_evaluation_controls',
        'prompt_evaluation_control_history', 'prompt_evaluations', 'prompt_evaluation_cases')}
    contexts = {}
    for index, case in enumerate(SCENARIOS):
        key, workspace = 'fixture-lifecycle-' + case, 'fixture-lifecycle-' + case
        other = workspace + '-other'
        for scope in (workspace, other):
            seed = controls(scope)
            for name in ('workspaces', 'memberships'):
                rows[name].extend(seed[name])
            control = replace(control_plan.control, workspace_id=scope, prompt_key=key)
            rows['prompt_evaluation_controls'].append(encode_control(control))
            rows['prompt_evaluation_control_history'].append(encode_control(control))
        states, reports = [], []
        for version in ('41', '42'):
            snapshot = replace(base.head.snapshot, prompt_key=key, vertex_version_id=version,
                vertex_prompt_id=str(901234567890 + index))
            draft = initial_prompt_record(snapshot, imported_at=base.history[0].changed_at,
                imported_by=control_plan.identity.principal.principal_id)
            registered = propose_prompt_transition(draft, status=PromptLifecycle.REGISTERED,
                changed_at=base.head.changed_at, changed_by=draft.changed_by, reason_code='FIXTURE_REVIEWED')
            state = PromptRegistryState(registered, (draft, registered))
            record = replace(report_plan.record, evaluation_id=key + '-' + version,
                registry_record=registered)
            if case == 'expiry':
                record = replace(record, valid_until=record.completed_at + timedelta(seconds=1))
            # Flow tests actual DRAFT -> REGISTERED; other scopes start REGISTERED.
            stored = PromptRegistryState(draft, (draft,)) if case == 'flow' else state
            head, history = encode_registry_state(stored)
            rows['prompt_registry'].append(head)
            rows['prompt_registry_history'].extend(history)
            for scope in (() if case == 'flow' else (workspace, other)):
                control = replace(control_plan.control, workspace_id=scope, prompt_key=key)
                summary, cases = encode_evaluation(control, record)
                rows['prompt_evaluations'].append(summary)
                rows['prompt_evaluation_cases'].extend(cases)
            states.append(state)
            reports.append(record)
        contexts[case] = dict(workspace=workspace, other=other, key=key,
            states=tuple(states), reports=tuple(reports), identity=control_plan.identity,
            control=replace(control_plan.control, workspace_id=workspace, prompt_key=key))
    seed = controls(UNRELATED)
    for name in ('workspaces', 'memberships'):
        rows[name].extend(seed[name])
    return rows, contexts
