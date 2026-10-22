"""Synthetic Batch 16 runtime seeds; no identity, quality or model-call proof.

All canonical rows are built through production contracts. Callers must relocate
SQL into an owned expiring dataset; this module never creates a cloud client.
"""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from src.application.prompt_contract import PromptLifecycle, propose_prompt_transition
from src.application.prompt_evaluation_contract import _hash
from src.application.prompt_evaluation_storage import encode_control as encode_evaluation_control, encode_evaluation
from src.application.prompt_evaluation_store import EvaluationReadRequest, EvaluationSnapshot
from src.application.prompt_lifecycle_contract import LifecycleControl, LifecycleReadRequest, LifecycleSnapshot, encode_control
from src.application.prompt_registry_codec import PromptRegistryState, encode_registry_state
from src.application.prompt_runtime_contract import RuntimeBehavior, bind_invocation
from src.application.prompt_runtime_reader import RuntimeReadRequest
from src.application.prompt_runtime_service import ResolvedPrompt
from src.application.workflow_contract import ContractError, request_fingerprint
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES
from src.validation.control_fixtures import controls, workflow
from src.validation.prompt_evaluation_fixtures import UNRELATED
from src.validation.prompt_lifecycle_fixtures import lifecycle_fixtures

SCENARIOS = ('core', 'rollback', 'postcommit', 'lost', 'unknown', 'expiry',
             'policy', 'revocation', 'missing', 'duplicate', 'race')


def runtime_fixtures(now=None, *, scenarios=SCENARIOS):
    """Return exact rows and isolated contexts, including a promotion candidate.

    The candidate is EVALUATED and eligible for a lifecycle ACTIVE replacement.
    Every scenario has a second workspace with the same evaluation/control pins
    to exercise cross-workspace promotion without sharing the workspace fence.
    """
    now = datetime.now(timezone.utc) if now is None else now
    if (type(scenarios) is not tuple or not scenarios or len(set(scenarios)) != len(scenarios)
            or any(type(case) is not str or not case.isidentifier() for case in scenarios)):
        raise ContractError('INVALID_RUNTIME_FIXTURE_SCENARIOS')
    _, lifecycle = lifecycle_fixtures(now)
    base = lifecycle['recovery']
    rows = {name: [] for name in WORKFLOW_TABLES}
    rows.update({name: [] for name in ('prompt_registry', 'prompt_registry_history',
        'prompt_evaluation_controls', 'prompt_evaluation_control_history',
        'prompt_evaluations', 'prompt_evaluation_cases', 'prompt_lifecycle_controls',
        'prompt_lifecycle_history', 'invocation_prompt_bindings')})
    contexts = {}
    for index, case in enumerate(scenarios):
        workspace = 'fixture-runtime-' + case
        other, key = workspace + '-other', workspace + '-planner'
        identity = base['identity']
        states, records = [], []
        for version, original, report in zip(('41', '42'), base['states'], base['reports']):
            snapshot = replace(original.head.snapshot, prompt_key=key,
                vertex_prompt_id=str(902234567890 + index), vertex_version_id=version)
            # Rebuild hashes and the REGISTERED evaluation anchor after scope edits.
            draft = replace(original.history[0], snapshot=snapshot)
            registered = propose_prompt_transition(draft, status=PromptLifecycle.REGISTERED,
                changed_at=original.head.changed_at, changed_by=identity.principal.principal_id,
                reason_code='FIXTURE_REVIEWED')
            record = replace(report, evaluation_id=workspace + '-evaluation-' + version,
                registry_record=registered)
            evaluated = propose_prompt_transition(registered, status=PromptLifecycle.EVALUATED,
                changed_at=now - timedelta(seconds=4), changed_by=identity.principal.principal_id,
                reason_code='FIXTURE_EVALUATED', evaluation_id=record.evaluation_id)
            history = (draft, registered, evaluated)
            if version == '41':
                active = propose_prompt_transition(evaluated, status=PromptLifecycle.ACTIVE,
                    changed_at=now - timedelta(seconds=3), changed_by=identity.principal.principal_id,
                    reason_code='FIXTURE_ACTIVE', evaluation_id=record.evaluation_id)
                history += (active,)
            state = PromptRegistryState(history[-1], history)
            head, past = encode_registry_state(state)
            rows['prompt_registry'].append(head)
            rows['prompt_registry_history'].extend(past)
            states.append(state)
            records.append(record)
        active, candidate = states
        control = LifecycleControl(key, active.head.snapshot.task, 1, None,
            active.head.snapshot.snapshot_sha256, _hash([workspace, 'fixture-activation']),
            (active.head.digest,), workspace, now - timedelta(seconds=3),
            identity.principal.principal_id)
        for name in ('prompt_lifecycle_controls', 'prompt_lifecycle_history'):
            rows[name].append(encode_control(control))
        evaluation_controls = {}
        for scope in (workspace, other):
            seed = workflow(scope)
            if scope == other:
                seed = controls(scope)
            for name, values in seed.items():
                rows[name].extend(values)
            ec = replace(base['control'], workspace_id=scope, prompt_key=key)
            evaluation_controls[scope] = ec
            for name in ('prompt_evaluation_controls', 'prompt_evaluation_control_history'):
                rows[name].append(encode_evaluation_control(ec))
            for record in records:
                summary, cases = encode_evaluation(ec, record)
                rows['prompt_evaluations'].append(summary)
                rows['prompt_evaluation_cases'].extend(cases)
        snapshot = active.head.snapshot
        request = next(row for row in rows['requests'] if row['workspace_id'] == workspace)
        invocation = next(row for row in rows['model_invocations'] if row['workspace_id'] == workspace)
        profile = deepcopy(invocation['profile']) | dict(model_id=snapshot.model_id,
            prompt_id=snapshot.vertex_prompt_id, prompt_version=snapshot.vertex_version_id,
            output_schema_version=snapshot.output_schema_version,
            safety_policy_version=snapshot.safety_policy_version, max_output_tokens=4096)
        invocation['profile'] = deepcopy(profile)
        request['model_profiles'] = [deepcopy(profile) if p['stage'] == 'PLANNER' else p
                                     for p in request['model_profiles']]
        request['request_sha256'] = request_fingerprint(request)
        next(row for row in rows['jobs'] if row['workspace_id'] == workspace)['request_sha256'] = request['request_sha256']
        ec = evaluation_controls[workspace]
        read_request = RuntimeReadRequest(workspace, invocation['conversation_id'], key,
            snapshot.task, identity, 1)
        lr = LifecycleReadRequest(workspace, key, snapshot.task, snapshot.vertex_prompt_id,
            snapshot.vertex_version_id, identity, 1, records[0].evaluation_id)
        ev = EvaluationSnapshot(EvaluationReadRequest(workspace, key, identity, 1,
            records[0].evaluation_id), ec, (ec,), records[0], active, now)
        capture = LifecycleSnapshot(lr, control, (control,), active, active, ev, now, ec)
        behavior = RuntimeBehavior(key, snapshot.task, snapshot.required_variables,
            snapshot.output_schema_json, snapshot.safety_settings_json,
            snapshot.output_schema_version, snapshot.safety_policy_version)
        binding = bind_invocation(capture, snapshot, profile, behavior, frozenset({_hash(profile)}),
            conversation_id=invocation['conversation_id'], request_id=invocation['request_id'],
            invocation_id=invocation['invocation_id'], checked_at=now)
        contexts[case] = dict(workspace=workspace, other=other, key=key, identity=identity,
            states=tuple(states), reports=tuple(records), control=control,
            evaluation_control=ec, evaluation_controls=evaluation_controls,
            profile=profile, behavior=behavior, capture=capture,
            resolved=ResolvedPrompt(binding, capture), read_request=read_request,
            request=request, invocation=invocation)
    for name, values in controls(UNRELATED).items():
        rows[name].extend(values)
    return rows, contexts
