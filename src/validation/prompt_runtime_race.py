"""Owned native promotion-versus-claim verification; never calls a model."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from threading import Barrier

from src.application.control_io import ControlIO
from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_lifecycle_contract import LifecycleReadRequest
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import PROJECT
from src.validation import prompt_lifecycle_integration as lifecycle
from src.validation.prompt_evaluation_remaining import transaction_interval, validate_race
from src.validation.signing_read_integration import require


def run_race(probe, context):
    raw, resolved = probe.prepare(context)
    claim = probe.scoped_plan(raw)
    store = lifecycle.FixtureLifecycleStore(probe)
    store._preflight = probe.own

    def request(version, evaluation=False):
        snapshot = context['states'][version].head.snapshot
        return LifecycleReadRequest(context['other'], context['key'], snapshot.task,
            snapshot.vertex_prompt_id, snapshot.vertex_version_id,
            probe.request(context).identity, 1,
            context['reports'][version].evaluation_id if evaluation else None)

    with lifecycle.owned_sql(probe.scope):
        capture = store.read(request(1, True))
        promotion = lifecycle.ScopedLifecycleWrite(capture, PromptLifecycle.ACTIVE,
            datetime.now(timezone.utc), 'FIXTURE_REVIEWED', fixture_scope=probe.scope,
            fixture_slot='race_promotion')
        require(claim.job_id != promotion.job_id
            and claim.workspace_id != promotion.capture.request.workspace_id,
            'RACE_SCOPE_NOT_DISTINCT')
        before = probe.snapshot()
        barrier = Barrier(2)

        def claim_work():
            probe.own()
            barrier.wait(timeout=30)
            io = ControlIO(probe.client)
            result = io.submit_once(claim)
            # Reconcile this exact original native job, never submit a second one.
            job = probe.client.get_job(claim.job_id, project=PROJECT, location=LOCATION,
                retry=None, timeout=30)
            try:
                job.result(timeout=120, retry=None, job_retry=None)
            except Exception:
                pass
            return io.recover(claim)

        def promotion_work():
            probe.own()
            barrier.wait(timeout=30)
            result = store.submit(promotion)
            return store.recover(promotion) if result.native_status is lifecycle.Status.UNKNOWN else result

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(claim_work), pool.submit(promotion_work)]
            outcomes = [future.result(timeout=180) for future in futures]

        infos, intervals = [], []
        from src.application.sharing_jobs import matches_sharing_job
        job = probe.client.get_job(claim.job_id, project=PROJECT, location=LOCATION,
            retry=None, timeout=30)
        require(matches_sharing_job(job, job_id=claim.job_id, sql=claim.sql,
            config=claim.config()), 'NATIVE_JOB_MISMATCH')
        claim_info = dict(job_id=claim.job_id, status=outcomes[0].native_status.value,
            receipt_state=outcomes[0].rows[0].get('state') if outcomes[0].rows else None,
            error=job.error_result, bytes_processed=job.total_bytes_processed)
        probe.report['jobs'].append(claim_info)
        promotion_info = lifecycle.LifecycleProbe.job_info(probe, promotion, outcomes[1])
        for plan, info in zip((claim, promotion), (claim_info, promotion_info)):
            children = list(probe.client.list_jobs(project=PROJECT, parent_job=plan.job_id,
                all_users=False, max_results=64, page_size=64, retry=None, timeout=30))
            interval = transaction_interval(children, plan.job_id)
            intervals.append(interval)
            info['transaction_interval'] = [at.isoformat() for at in interval]
            infos.append(info)
        winner = validate_race(infos, intervals,
            ('RUNTIME_PROMPT_CONFLICT', 'LIFECYCLE_CONTROL_STALE'))
        loser_message = infos[1-winner]['error'].get('message', '')
        conflict_table = ('workspaces' if 'workspaces' in loser_message else
            'prompt_lifecycle_controls' if 'prompt_lifecycle_controls' in loser_message else 'UNSPECIFIED')
        probe.report['races'].append(dict(case='promotion_versus_claim', winner=winner,
            winner_kind=('dispatch_claim', 'promotion')[winner], conflict_table=conflict_table,
            overlap_seconds=(min(i[1] for i in intervals)-max(i[0] for i in intervals)).total_seconds(),
            jobs=infos))
        probe.record('overlapping_promotion_versus_claim')
        after = probe.snapshot()
        if winner == 0:
            probe.atomic(before, after, claim, resolved)
            require(store.read(request(0)).target == context['states'][0]
                and store.read(request(1)).target == context['states'][1],
                'LOSING_PROMOTION_CHANGED_REGISTRY')
        else:
            fresh = store.read(request(1))
            require(fresh.control == promotion.control
                and fresh.control_history == promotion.capture.control_history + (promotion.control,),
                'LIFECYCLE_CONTROL_PARTIAL')
            by_version = {state.head.snapshot.vertex_version_id: state for state in promotion.after_states}
            for version in (0, 1):
                expected = by_version.get(context['states'][version].head.snapshot.vertex_version_id,
                    context['states'][version])
                require(store.read(request(version)).target == expected, 'LIFECYCLE_REGISTRY_PARTIAL')
            mutable = {'prompt_registry', 'prompt_registry_history',
                'prompt_lifecycle_controls', 'prompt_lifecycle_history', 'workspaces'}
            for name in before:
                if name not in mutable:
                    require(before[name] == after[name], 'LOSING_CLAIM_CHANGED_ROWS')
                field = 'workspace_id' if name == 'workspaces' else 'prompt_key'
                allowed = context['other'] if name == 'workspaces' else context['key']
                outside = lambda values: [row for row in values if json.loads(row).get(field) != allowed]
                require(outside(before[name]) == outside(after[name]), 'RACE_UNRELATED_ROWS_CHANGED')
            old = next(json.loads(row) for row in before['workspaces']
                if json.loads(row)['workspace_id'] == context['other'])
            new = next(json.loads(row) for row in after['workspaces']
                if json.loads(row)['workspace_id'] == context['other'])
            require(new['revision'] == old['revision']+1
                and datetime.fromisoformat(new['updated_at']) >= datetime.fromisoformat(old['updated_at'])
                and {k:v for k,v in new.items() if k not in {'revision', 'updated_at'}}
                    == {k:v for k,v in old.items() if k not in {'revision', 'updated_at'}},
                'RACE_WORKSPACE_FENCE_MISMATCH')
        probe.record('race_atomic_winner_preserves_other_scope')
