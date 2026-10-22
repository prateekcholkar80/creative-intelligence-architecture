"""Batch 14 planned native overlap and partial-state gates; owned fixtures only."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from threading import Barrier

from google.cloud import bigquery

from src.application.prompt_evaluation_query import EvaluationWriteKind, PromptEvaluationWrite
from src.application.prompt_evaluation_storage import encode_control, encode_evaluation
from src.application.prompt_evaluation_store import EvaluationReadRequest, decode_snapshot, read_config, read_sql
from src.application.prompt_registry_codec import encode_registry_state
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_contract import ContractError
from src.validation.control_fixtures import controls
from src.validation.prompt_evaluation_fixtures import UNRELATED, fixtures
from src.validation.prompt_evaluation_integration import FixtureStore, Probe, ScopedWrite, Status, TABLES
from src.validation.signing_read_integration import require

PARTIAL_CASES = {
    'head_without_history': ('INVALID_EVALUATION_CONTROL_HISTORY', 'CONTROL_HISTORY_STALE', 'CONTROL_HISTORY_STALE'),
    'history_without_head': ('ORPHAN_EVALUATION_STORAGE', 'CONTROL_HEAD_MISSING', 'EVALUATION_CONTROL_STALE'),
    'history_gap': ('EVALUATION_CONTROL_HISTORY_MISMATCH', 'CONTROL_HISTORY_STALE', 'CONTROL_HISTORY_STALE'),
    'report_without_cases': ('INVALID_EVALUATION_CASE_COUNT', None, 'EVALUATION_CASE_REPLAY_CONFLICT'),
    'report_missing_case': ('INVALID_EVALUATION_CASE_COUNT', None, 'EVALUATION_CASE_REPLAY_CONFLICT'),
    'orphan_case': ('ORPHAN_EVALUATION_CASES', None, 'ORPHAN_EVALUATION_CASES'),
}


def remaining_fixtures(now=None):
    """Seed independent corrupt scopes once; no fixture resets or row deletion."""
    _, registry, control, report = fixtures(now)
    rows = {name: [] for name in TABLES}
    contexts = {}
    for case in ('control_race', 'report_race', *PARTIAL_CASES):
        workspace = 'fixture-evaluation-' + case.replace('_', '-')
        head = replace(control.control, workspace_id=workspace)
        history = (head,)
        if case == 'history_gap':
            initial = head
            head = replace(head, revision=2, parent_digest=initial.digest,
                updated_at=initial.updated_at + timedelta(seconds=1))
            history = (initial, head)
        write_control = PromptEvaluationWrite(EvaluationWriteKind.CONTROL, head, control.identity, 1,
            previous=None if head.revision == 1 else history[-2], control_history=history[:-1])
        write_report = replace(report, control=head, control_history=history,
            record=replace(report.record, evaluation_id='fixture-' + case.replace('_', '-')))
        contexts[case] = (write_control, write_report)
        seed = controls(workspace)
        for name in ('workspaces', 'memberships'):
            rows[name].extend(seed[name])
        if case != 'history_without_head':
            rows['prompt_evaluation_controls'].append(encode_control(head))
        if case != 'head_without_history':
            rows['prompt_evaluation_control_history'].append(encode_control(head))
        summary, cases = encode_evaluation(head, write_report.record)
        if case in {'report_without_cases', 'report_missing_case'}:
            rows['prompt_evaluations'].append(summary)
        if case == 'report_missing_case':
            rows['prompt_evaluation_cases'].extend(cases[:-1])
        if case == 'orphan_case':
            rows['prompt_evaluation_cases'].append(cases[0])
    sentinel = controls(UNRELATED)
    for name in ('workspaces', 'memberships'):
        rows[name].extend(sentinel[name])
    current, history = encode_registry_state(registry)
    rows['prompt_registry'], rows['prompt_registry_history'] = [current], history
    return rows, contexts


def transaction_interval(children, job_id):
    require(0 < len(children) < 64 and all(isinstance(child, bigquery.QueryJob)
        and child.project == 'project-661d501a-e709-4276-a26' and child.location == 'asia-south1'
        and child.parent_job_id == job_id and child.state == 'DONE' for child in children),
        'INVALID_RACE_CHILD_CORRELATION')
    begins = [child.ended for child in children if child.statement_type == 'BEGIN_TRANSACTION'
              and child.error_result is None]
    ends = [child.started for child in children if child.statement_type in
            {'COMMIT_TRANSACTION', 'ROLLBACK_TRANSACTION'} and child.error_result is None]
    require(len(begins) == len(ends) == 1 and all(begins + ends)
            and begins[0] < ends[0], 'INVALID_TRANSACTION_INTERVAL')
    return begins[0], ends[0]


def validate_race(jobs, intervals, allowed_assertions):
    require(len(jobs) == len(intervals) == 2 and all(intervals)
            and max(interval[0] for interval in intervals) < min(interval[1] for interval in intervals),
            'RACE_DID_NOT_OVERLAP')
    winners = [i for i, job in enumerate(jobs) if job['status'] == 'COMMITTED'
               and job['receipt_state'] == 'APPLIED' and job['error'] is None]
    require(len(winners) == 1, 'RACE_WINNER_NOT_VERIFIED')
    loser = jobs[1 - winners[0]]
    error = loser['error']
    require(loser['status'] == 'NOT_COMMITTED' and loser['receipt_state'] is None
            and type(error) is dict and error.get('reason') in {'invalidQuery', 'concurrentUpdate'}
            and ('Transaction is aborted due to concurrent update' in error.get('message', '')
                 or any(code in error.get('message', '') for code in allowed_assertions)),
            'RACE_LOSER_NOT_VERIFIED')
    return winners[0]


def assert_scope_preserved(before, after, workspace, mutable):
    """Exact rows outside the permitted winner scope; exactly one workspace fence."""
    for name in TABLES:
        left, right = before[name], after[name]
        if name not in mutable | {'workspaces'}:
            require(left == right, 'RACE_UNRELATED_ROWS_CHANGED')
        else:
            outside = lambda rows: [row for row in rows if json.loads(row).get('workspace_id') != workspace]
            require(outside(left) == outside(right), 'RACE_UNRELATED_ROWS_CHANGED')
    left = [json.loads(row) for row in before['workspaces'] if json.loads(row)['workspace_id'] == workspace]
    right = [json.loads(row) for row in after['workspaces'] if json.loads(row)['workspace_id'] == workspace]
    require(len(left) == len(right) == 1 and right[0]['revision'] == left[0]['revision'] + 1
            and datetime.fromisoformat(right[0]['updated_at']) >= datetime.fromisoformat(left[0]['updated_at'])
            and {k: v for k, v in right[0].items() if k not in {'revision', 'updated_at'}}
                == {k: v for k, v in left[0].items() if k not in {'revision', 'updated_at'}},
            'RACE_WORKSPACE_FENCE_MISMATCH')


class RemainingProbe(Probe):
    def __init__(self, client):
        super().__init__(client)
        self.report.update(scope='ISOLATED_NATIVE_EVALUATION_REMAINING_GATES',
            races=[], denied_reads=[], verified_native_error_codes={})

    @staticmethod
    def fresh(plan):
        # Supplied fixture identity only; does not authenticate or mint a JWT.
        now = int(datetime.now(timezone.utc).timestamp())
        return replace(plan, identity=replace(plan.identity, issued_at=now - 1, expires_at=now + 599))

    def read(self, plan, evaluation_id=None):
        request = EvaluationReadRequest(plan.control.workspace_id, plan.control.prompt_key,
            plan.identity, plan.membership_revision, evaluation_id)
        return decode_snapshot(request, self.query(self.scope.relocate(read_sql()), read_config(request)))

    def snapshot(self):
        fields = []
        for name in TABLES:
            predicate = (" WHERE changed_at BETWEEN TIMESTAMP('0001-01-01') "
                "AND TIMESTAMP('9999-12-31 23:59:59.999999+00')" if name == 'prompt_registry_history' else '')
            source = f'`{self.scope.table(name)}` t{predicate}'
            fields.extend([f'(SELECT COUNT(*) FROM {source}) AS {name}_count',
                f'ARRAY(SELECT TO_JSON_STRING(t) FROM {source} ORDER BY TO_JSON_STRING(t) LIMIT 1025) AS {name}'])
        row = self.query('SELECT ' + ', '.join(fields))[0]
        require(all(row[name + '_count'] == len(row[name]) <= 1024 for name in TABLES),
            'FIXTURE_SNAPSHOT_OVERFLOW')
        return {name: row[name] for name in TABLES}

    def race(self, bases, case, allowed_assertions):
        plans = [ScopedWrite(self.fresh(base), self.scope, case + '_' + side)
                 for base, side in zip(bases, ('left', 'right'))]
        barrier = Barrier(2)
        def work(plan):
            store = FixtureStore(self)
            def preflight():
                self.preflight()
                barrier.wait(timeout=20)
            store._preflight = preflight
            outcome = store.submit(plan)
            return store.recover(plan) if outcome.native_status is Status.UNKNOWN else outcome
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(work, plan) for plan in plans]
            outcomes = [future.result(timeout=120) for future in futures]
        jobs, intervals = [], []
        for plan, outcome in zip(plans, outcomes):
            job = self.client.get_job(plan.job_id, project=self.client.project,
                location=self.client.location, retry=None, timeout=10)
            require(matches_sharing_job(job, job_id=plan.job_id, sql=plan.sql, config=plan.job_config()),
                'RACE_JOB_MISMATCH')
            children = list(self.client.list_jobs(project=self.client.project, parent_job=plan.job_id,
                all_users=False, max_results=64, page_size=64, retry=None, timeout=10))
            interval = transaction_interval(children, plan.job_id)
            intervals.append(interval)
            info = dict(job_id=plan.job_id, status=outcome.native_status.value,
                receipt_state=None if outcome.receipt is None else outcome.receipt['state'],
                error=job.error_result, transaction_interval=[at.isoformat() for at in interval],
                children=[dict(statement_type=child.statement_type, error=child.error_result)
                          for child in children])
            jobs.append(info)
            self.report['jobs'].append(info)
        winner = validate_race(jobs, intervals, allowed_assertions)
        self.report['races'].append(dict(case=case, overlapping_transactions=True,
            committed_mutations=1, winner_job_id=plans[winner].job_id))
        return bases[winner]

    def deny_read(self, plan, code, evaluation_id=None):
        try:
            self.read(plan, evaluation_id)
        except ContractError as error:
            require(error.code == code, 'UNEXPECTED_READ_DENIAL')
            self.report['denied_reads'].append(dict(workspace_id=plan.control.workspace_id, code=code))
        else:
            raise RuntimeError('PARTIAL_STORAGE_READ_ACCEPTED')

    def deny_write(self, store, plan, case, code):
        self.write(store, plan, case, state=None, status=Status.NOT_COMMITTED)
        error = self.report['jobs'][-1]['error']
        require(type(error) is dict and error.get('reason') == 'invalidQuery'
                and code in error.get('message', ''), 'NATIVE_ASSERTION_MISMATCH')
        self.report['verified_native_error_codes'][case] = code

    def run(self):
        rows, contexts = remaining_fixtures()
        for name, data in rows.items():
            if data:
                self.load(name, data)
        store = FixtureStore(self)
        control, _ = contexts['control_race']
        before = self.snapshot()
        contenders = tuple(replace(control, control=replace(control.control,
            revision=2, parent_digest=control.control.digest,
            policy=replace(control.control.policy, version='fixture-v2-' + side),
            updated_at=control.control.updated_at + timedelta(seconds=1)),
            previous=control.control, control_history=(control.control,)) for side in ('left', 'right'))
        winner = self.race(contenders, 'control_race', ('CONTROL_REPLAY_CONFLICT', 'CONTROL_STALE'))
        snapshot = self.read(self.fresh(winner))
        require(snapshot.control == winner.control and snapshot.control_history == (control.control, winner.control),
            'CONTROL_RACE_PARTIAL_STATE')
        assert_scope_preserved(before, self.snapshot(), control.control.workspace_id,
            {'prompt_evaluation_controls', 'prompt_evaluation_control_history'})
        self.record('overlapping_control_writers_one_complete_winner')

        _, report = contexts['report_race']
        before = self.snapshot()
        conflict = replace(report, record=replace(report.record, results=(
            replace(report.record.results[0], response_sha256='e' * 64),) + report.record.results[1:]))
        winner = self.race((report, conflict), 'report_race', ('EVALUATION_REPLAY_CONFLICT',))
        snapshot = self.read(self.fresh(winner), winner.record.evaluation_id)
        require(snapshot.record == winner.record, 'REPORT_RACE_PARTIAL_STATE')
        assert_scope_preserved(before, self.snapshot(), report.control.workspace_id,
            {'prompt_evaluations', 'prompt_evaluation_cases'})
        self.record('overlapping_report_writers_one_complete_winner')

        for case, (read_code, control_code, report_code) in PARTIAL_CASES.items():
            control, report = (self.fresh(plan) for plan in contexts[case])
            before = self.snapshot()
            self.deny_read(report, read_code, report.record.evaluation_id
                if case.startswith('report_') or case == 'orphan_case' else None)
            if control_code:
                proposal = control
                if case != 'history_without_head':
                    proposal = replace(control, control=replace(control.control,
                        revision=control.control.revision + 1, parent_digest=control.control.digest,
                        updated_at=control.control.updated_at + timedelta(seconds=1)),
                        previous=control.control, control_history=report.control_history)
                self.deny_write(store, proposal, case + '_control', control_code)
            self.deny_write(store, report, case + '_report', report_code)
            require(self.snapshot() == before, 'DENIED_WRITE_CHANGED_FIXTURE_ROWS')
            self.record(case + '_read_and_write_deny_preserve_exact_rows')
