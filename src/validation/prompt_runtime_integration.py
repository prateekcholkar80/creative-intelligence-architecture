"""Batch 16 opt-in owned native runtime binding checks; no serving/model writes."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import re
from pathlib import Path
from threading import Barrier
from unittest.mock import Mock, patch
from uuid import uuid4

from google.cloud import bigquery

from src.application.control_io import ControlIO
from src.application.control_io import query_config
from src.application.generation_admission import ProtectedGenerationAdmission, AdmissionSettings
from src.application.identity import IapIdentityAdapter, IapSettings
from src.application.management_service import digest
from src.application.prompt_contract import PromptLifecycle
from src.application.prompt_evaluation_query import PromptEvaluationWrite, EvaluationWriteKind, wire
from src.application.prompt_runtime_contract import TABLES as BINDING_TABLES, bind_invocation
from src.application.prompt_runtime_reader import read_sql, read_config, decode_snapshot
from src.application.prompt_runtime_service import PromptRuntimeService, ResolvedPrompt
from src.application.sharing_jobs import matches_sharing_job
from src.application.workflow_codec import decode_row
from src.application.workflow_schema import TABLES as WORKFLOW_TABLES, LOCATION
from src.ingestion.setup_workflow_tables import PROJECT, _schema
from src.ingestion.setup_workflow_tables import DATASET_ID
from src.validation import prompt_lifecycle_integration as lifecycle
from src.validation.prompt_evaluation_remaining import transaction_interval, validate_race
from src.validation.prompt_runtime_fixtures import runtime_fixtures
from src.validation.signing_read_integration import require

TABLES = dict(lifecycle.TABLES) | dict(WORKFLOW_TABLES) | dict(BINDING_TABLES)
CASE_GROUPS = {
    'CORE': frozenset({'authoritative_active_resolution', 'atomic_dispatch_binding_and_audit',
        'same_job_duplicate_and_original_recovery', 'discarded_response_recovery',
        'rollback_after_binding_preserves_all_rows', 'postcommit_error_without_invented_receipt',
        'absent_original_remains_unknown', 'duplicate_binding_denies',
        'exact_stored_binding_readback'}),
    'DENIALS': frozenset({'expired_evaluation_denies', 'expired_binding_deadline_denies',
        'changed_policy_denies', 'revoked_membership_denies',
        'missing_history_denies', 'missing_and_corrupt_stored_binding_deny',
        'stale_claim_after_promotion_denies', 'unrelated_fixture_rows_preserved'}),
    'RACE': frozenset({'overlapping_promotion_versus_claim', 'race_atomic_winner_preserves_other_scope'}),
}
CASE_GROUPS['ALL'] = frozenset().union(*CASE_GROUPS.values())


class RuntimeScope(lifecycle.LifecycleScope):
    def __init__(self, run_id):
        super().__init__(run_id)
        self.dataset = f'{PROJECT}.ci_prompt_runtime_it_{run_id}'
        self.labels = {'purpose': 'prompt_runtime_integration', 'run_id': run_id}

    def table(self, name):
        require(name in TABLES, 'UNRELATED_TABLE')
        return self.dataset + '.' + name

    def assert_sql(self, script):
        refs = re.findall(r'`([^`]+)`', script)
        bare = re.sub(r'`[^`]+`', ' ', script)
        unquoted = re.findall(r'\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+([\w-]+(?:\.[\w-]+){1,2})', bare, re.I)
        require(bool(refs) and set(refs) <= {self.table(name) for name in TABLES}
            and not unquoted and DATASET_ID not in script, 'UNSAFE_SQL')

    def relocate(self, script):
        for name in TABLES:
            script = script.replace(f'`{DATASET_ID}.{name}`', f'`{self.table(name)}`')
        self.assert_sql(script)
        return script


@contextmanager
def fixture_tables():
    # Reuse exact owned setup/cleanup guards, expanded to the binding parents.
    with patch.object(lifecycle, 'TABLES', TABLES):
        yield


def scoped_plan(plan, scope, fault=None):
    require(isinstance(scope, RuntimeScope) and fault in {None, 'rollback', 'postcommit'},
            'INVALID_RUNTIME_FIXTURE_PLAN')
    script = scope.relocate(plan.sql)
    marker = '\nCOMMIT TRANSACTION;' if fault == 'rollback' else '-- PROBE_AFTER_COMMIT'
    if fault is not None:
        require(script.count(marker) == 1, 'FAULT_MARKER_MISSING')
        code = 'FIXTURE_ROLLBACK' if fault == 'rollback' else 'FIXTURE_AFTER_COMMIT'
        script = script.replace(marker, "\nASSERT FALSE AS '" + code + "';\n" + marker)
    scope.assert_sql(script)
    return replace(plan, sql=script, job_id='runtime_it_' + scope.run_id + '_' + plan.job_id)


def validate_native_report(report):
    group = report.get('case_group', 'ALL')
    require(group in CASE_GROUPS, 'INVALID_RUNTIME_CASE_GROUP')
    cases = report['cases']
    require(len(cases) == len(CASE_GROUPS[group])
        and {case['case'] for case in cases} == CASE_GROUPS[group]
        and all(case['status'] == 'PASSED' for case in cases), 'INCOMPLETE_RUNTIME_CASES')
    for job_id, code in report['verified_native_error_codes'].items():
        jobs = [job for job in report['jobs'] if job['job_id'] == job_id]
        require(len(jobs) == 1, 'EXPECTED_NATIVE_ERROR_JOB_MISSING')
        job = jobs[0]
        require(job['status'] == ('COMMITTED' if code == 'FIXTURE_AFTER_COMMIT' else 'NOT_COMMITTED')
            and job['receipt_state'] is None and type(job['error']) is dict
            and job['error'].get('reason') == 'invalidQuery'
            and code in job['error'].get('message', ''), 'NATIVE_ASSERTION_MISMATCH')
    expected_codes = {'CORE': {'FIXTURE_ROLLBACK', 'FIXTURE_AFTER_COMMIT', 'RUNTIME_BINDING_ALREADY_EXISTS'},
        'DENIALS': {'RUNTIME_EVALUATION_EXPIRED', 'RUNTIME_POLICY_EXPIRED', 'RUNTIME_PROMPT_CONFLICT', 'DISPATCH_CONFLICT'},
        'RACE': set()}
    required = set().union(*expected_codes.values()) if group == 'ALL' else expected_codes[group]
    require(set(report['verified_native_error_codes'].values()) == required
        and len(report['verified_native_error_codes']) == {'CORE':3,'DENIALS':6,'RACE':0,'ALL':9}[group],
        'INCOMPLETE_RUNTIME_DENIALS')
    counts=Counter(report['verified_native_error_codes'].values())
    expected_counts={'CORE':Counter(FIXTURE_ROLLBACK=1,FIXTURE_AFTER_COMMIT=1,RUNTIME_BINDING_ALREADY_EXISTS=1),
        'DENIALS':Counter(RUNTIME_EVALUATION_EXPIRED=1,RUNTIME_POLICY_EXPIRED=1,RUNTIME_PROMPT_CONFLICT=2,DISPATCH_CONFLICT=2),
        'RACE':Counter()}
    expected_counts['ALL']=expected_counts['CORE']+expected_counts['DENIALS']
    require(counts==expected_counts[group],'INCOMPLETE_RUNTIME_DENIALS')
    races = report['races']
    require(len(races) == (1 if group in {'ALL', 'RACE'} else 0), 'INCOMPLETE_RUNTIME_RACES')
    for race in races:
        require(race['case'] == 'promotion_versus_claim', 'INVALID_RUNTIME_RACE_CASE')
        intervals = [tuple(datetime.fromisoformat(at) for at in job['transaction_interval']) for job in race['jobs']]
        require(validate_race(race['jobs'], intervals, ('RUNTIME_PROMPT_CONFLICT', 'LIFECYCLE_CONTROL_STALE'))
            == race['winner'], 'RACE_WINNER_MISMATCH')


class FixtureControlIO(ControlIO):
    def __init__(self, probe):
        super().__init__(probe.client)
        self.probe = probe

    def preflight(self):
        self.probe.own()

    def read(self, script, params):
        from src.application.control_io import query_config
        rows = self.probe.query(self.probe.scope.relocate(script), query_config(params))
        require(len(rows) == 1, 'CONTROL_READ_UNVERIFIED')
        return rows[0]


class RuntimeProbe(lifecycle.LifecycleProbe):
    def __init__(self, client):
        super().__init__(client)
        self.scope = RuntimeScope(uuid4().hex)
        self.report.update(batch=16, scope='ISOLATED_NATIVE_PROMPT_RUNTIME',
            fixture_dataset=self.scope.dataset, vertex_calls=0, nonbillable_adapter_calls=0)

    def load(self, name, rows):
        if not rows:
            return
        require(name in TABLES, 'UNRELATED_TABLE')
        self.own()
        actual = self.client.get_table(self.scope.table(name), retry=None, timeout=30)
        require(_schema(actual.schema) == _schema(TABLES[name].schema), 'SCHEMA_MISMATCH')
        self.counter += 1
        job = self.client.load_table_from_json(json.loads(wire(rows)), self.scope.table(name),
            job_config=bigquery.LoadJobConfig(schema=TABLES[name].schema, write_disposition='WRITE_APPEND'),
            job_id=f'runtime_it_{self.scope.run_id}_load_{self.counter}', project=PROJECT,
            location=LOCATION, num_retries=0, timeout=30)
        job.result(timeout=60, retry=None)
        require(job.error_result is None, 'FIXTURE_LOAD_FAILED')
        print(json.dumps(dict(event='runtime_fixture_loaded', table=name, rows=len(rows))), flush=True)

    def snapshot(self):
        fields=[]
        for name,spec in TABLES.items():
            where=(f" WHERE {spec.partition_field} BETWEEN TIMESTAMP('0001-01-01') "
                "AND TIMESTAMP('9999-12-31 23:59:59.999999+00')" if spec.partition_field else '')
            source=f'`{self.scope.table(name)}` t{where}'
            fields.extend([f'(SELECT COUNT(*) FROM {source}) AS {name}_count',
                f'ARRAY(SELECT TO_JSON_STRING(t) FROM {source} ORDER BY TO_JSON_STRING(t) LIMIT 1025) AS {name}'])
        row=self.query('SELECT '+', '.join(fields))[0]
        require(all(row[name+'_count']==len(row[name])<=1024 for name in TABLES), 'FIXTURE_SNAPSHOT_OVERFLOW')
        return {name:row[name] for name in TABLES}

    def request(self, context):
        epoch = int(datetime.now(timezone.utc).timestamp())
        identity = replace(context['identity'], issued_at=epoch-1, expires_at=epoch+599)
        return replace(context['read_request'], identity=identity)

    def capture(self, context):
        request = self.request(context)
        rows = self.query(self.scope.relocate(read_sql()), read_config(request))
        return decode_snapshot(request, rows)

    def resolved(self, context):
        capture = self.capture(context)
        b = bind_invocation(capture, capture.target.head.snapshot, context['profile'], context['behavior'],
            frozenset({digest(context['profile'])}), conversation_id=context['invocation']['conversation_id'],
            request_id=context['invocation']['request_id'], invocation_id=context['invocation']['invocation_id'],
            checked_at=datetime.now(timezone.utc))
        return ResolvedPrompt(b, capture)

    def prepare(self, context):
        resolved = self.resolved(context)
        adapter = IapIdentityAdapter(IapSettings(context['identity'].audience))
        adapter.authenticate = lambda headers: self.request(context).identity
        runtime = Mock(spec=PromptRuntimeService)
        runtime.resolve.return_value = resolved
        guard = ProtectedGenerationAdmission(client=self.client, verifier=adapter, worker_id='fixture-worker',
            settings=AdmissionSettings(frozenset({digest(context['profile'])}), True, require_registered_prompts=True),
            prompt_runtime=runtime)
        guard.io = FixtureControlIO(self)
        at = datetime.now(timezone.utc)
        with patch('src.ingestion.setup_prompt_runtime_tables.inspect', return_value={'status': 'VERIFIED'}):
            plan = guard.prepare({}, workspace_id=context['workspace'], conversation_id='fixture-conversation',
                request_id='fixture-request', invocation_id='fixture-invocation', job_id='fixture-job',
                attempt_id='fixture-attempt', start_at=at-timedelta(hours=1), end_at=at+timedelta(hours=1))
        return plan, resolved

    def execute(self, plan, *, status=lifecycle.Status.COMMITTED, error=None, receipt=True):
        self.own()
        self.scope.assert_sql(plan.sql)
        print(json.dumps(dict(event='runtime_native_started',job_id=plan.job_id)),flush=True)
        io = ControlIO(self.client)
        outcome = io.submit_once(plan)
        job = self.client.get_job(plan.job_id, project=PROJECT, location=LOCATION, retry=None, timeout=30)
        try:
            job.result(timeout=120, retry=None, job_retry=None)
        except Exception:
            pass
        outcome = io.recover(plan)
        job = self.client.get_job(plan.job_id, project=PROJECT, location=LOCATION, retry=None, timeout=30)
        require(matches_sharing_job(job, job_id=plan.job_id, sql=plan.sql, config=plan.config()), 'NATIVE_JOB_MISMATCH')
        info = dict(job_id=plan.job_id, status=outcome.native_status.value,
            receipt_state=outcome.rows[0].get('state') if outcome.rows else None,
            error=job.error_result, bytes_processed=job.total_bytes_processed)
        self.report['jobs'].append(info)
        require(outcome.native_status is status and (len(outcome.rows) == 1 if receipt else not outcome.rows),
            'UNEXPECTED_WRITE_OUTCOME')
        if receipt:
            require(outcome.rows[0] == dict(state='APPLIED', candidate=plan.candidate_json, audit=plan.audit_json)
                or decode_row(outcome.rows[0]['candidate'], 'model_invocations') == decode_row(plan.candidate_json, 'model_invocations')
                and decode_row(outcome.rows[0]['audit'], 'audit_events') == decode_row(plan.audit_json, 'audit_events')
                and outcome.rows[0]['state'] == 'APPLIED', 'DISPATCH_RESULT_INVALID')
        if error:
            require(type(info['error']) is dict and error in info['error'].get('message', ''), 'NATIVE_ASSERTION_MISMATCH')
            self.report['verified_native_error_codes'][plan.job_id] = error
        return outcome, info

    def atomic(self, before, after, plan, resolved):
        ws = plan.workspace_id
        mutable = {'workspaces', 'job_attempts', 'model_invocations', 'audit_events', 'invocation_prompt_bindings'}
        for name in TABLES:
            outside = lambda values: [row for row in values if json.loads(row).get('workspace_id') != ws]
            require(outside(before[name]) == outside(after[name]), 'UNRELATED_ROWS_CHANGED')
            if name not in mutable:
                require(before[name] == after[name], 'UNEXPECTED_ROWS_CHANGED')
        rows = lambda name: [json.loads(row) for row in after[name] if json.loads(row).get('workspace_id') == ws]
        require(len(rows('invocation_prompt_bindings')) == 1 and len(rows('audit_events')) == 1,
            'PARTIAL_RUNTIME_COMMIT')
        binding = rows('invocation_prompt_bindings')[0]
        normalized = dict(binding)
        normalized['bound_at'] = datetime.fromisoformat(normalized['bound_at'])
        require(normalized == resolved.binding.row(), 'BINDING_MISMATCH')
        require(decode_row(wire(rows('model_invocations')[0]), 'model_invocations')
            == decode_row(plan.candidate_json, 'model_invocations'), 'INVOCATION_MISMATCH')
        require(decode_row(wire(rows('audit_events')[0]), 'audit_events')
            == decode_row(plan.audit_json, 'audit_events'), 'AUDIT_MISMATCH')
        old = next(json.loads(row) for row in before['workspaces'] if json.loads(row)['workspace_id'] == ws)
        new = rows('workspaces')[0]
        require(new['revision'] == old['revision']+1
            and {k:v for k,v in new.items() if k not in {'revision','updated_at'}}
                == {k:v for k,v in old.items() if k not in {'revision','updated_at'}}, 'WORKSPACE_FENCE_MISMATCH')
        attempt = rows('job_attempts')[0]
        candidate = decode_row(plan.candidate_json, 'model_invocations')
        require(attempt['revision'] == 2 and attempt['dispatch_receipt_id'] == candidate['dispatch_receipt_id'],
            'ATTEMPT_RECEIPT_MISMATCH')

    def core(self, contexts):
        plan, resolved = self.prepare(contexts['core'])
        self.record('authoritative_active_resolution')
        plan = scoped_plan(plan, self.scope)
        before = self.snapshot()
        outcome, _ = self.execute(plan)
        after = self.snapshot()
        self.atomic(before, after, plan, resolved)
        self.record('atomic_dispatch_binding_and_audit')
        io = ControlIO(self.client)
        duplicate, recovered = io.submit_once(plan), io.recover(plan)
        require(not duplicate.submitted and duplicate.rows == recovered.rows == outcome.rows
            and self.snapshot() == after, 'RECOVERY_RESUBMITTED')
        self.record('same_job_duplicate_and_original_recovery')
        self.binding_read(contexts['core'], resolved)
        self.record('exact_stored_binding_readback')
        for name, fault in (('lost', None), ('rollback', 'rollback'), ('postcommit', 'postcommit')):
            raw, value = self.prepare(contexts[name])
            current = scoped_plan(raw, self.scope, fault)
            before = self.snapshot()
            if name == 'lost':
                self.query(current.sql, current.config(), current.job_id)
                result = io.recover(current)
                require(result.native_status is lifecycle.Status.COMMITTED and not result.submitted
                    and len(result.rows) == 1, 'LOST_RESPONSE_RECOVERY_FAILED')
                self.atomic(before, self.snapshot(), current, value)
                self.record('discarded_response_recovery')
            else:
                self.execute(current, status=lifecycle.Status.NOT_COMMITTED if fault == 'rollback'
                    else lifecycle.Status.COMMITTED, receipt=False,
                    error='FIXTURE_ROLLBACK' if fault == 'rollback' else 'FIXTURE_AFTER_COMMIT')
                if fault == 'rollback':
                    require(self.snapshot() == before, 'ROLLBACK_CHANGED_ROWS')
                    self.record('rollback_after_binding_preserves_all_rows')
                else:
                    self.atomic(before, self.snapshot(), current, value)
                    require(not io.recover(current).rows, 'INVENTED_RECEIPT')
                    self.record('postcommit_error_without_invented_receipt')
        raw, _ = self.prepare(contexts['unknown'])
        absent = scoped_plan(raw, self.scope)
        before = self.snapshot()
        result = io.recover(absent)
        require(result.native_status is lifecycle.Status.UNKNOWN and not result.submitted
            and self.snapshot() == before, 'ABSENCE_GRANTED_DISPATCH')
        self.record('absent_original_remains_unknown')
        raw, value = self.prepare(contexts['duplicate'])
        self.load('invocation_prompt_bindings', [value.binding.row()])
        before = self.snapshot()
        self.execute(scoped_plan(raw, self.scope), status=lifecycle.Status.NOT_COMMITTED,
            receipt=False, error='RUNTIME_BINDING_ALREADY_EXISTS')
        require(self.snapshot() == before, 'DUPLICATE_BINDING_CHANGED_ROWS')
        self.record('duplicate_binding_denies')

    def binding_read(self, context, resolved):
        from src.application.prompt_runtime_reader import BigQueryPromptRuntimeReader
        from src.application.prompt_evaluation_store import EvaluationStoreSettings
        reader = BigQueryPromptRuntimeReader(self.client,EvaluationStoreSettings(
            rpc_timeout_seconds=20,result_timeout_seconds=60))
        reader.preflight = self.own
        # Production read-binding uses table metadata and generated SQL. Adapt
        # only exact owned metadata references and SQL at the fixture boundary.
        original_get = self.client.get_table
        original_query = self.client.query
        def get_table(ref, **kwargs):
            name = ref.table_id if hasattr(ref, 'table_id') else str(ref).split('.')[-1]
            actual = original_get(self.scope.table(name), **kwargs)
            require(actual.project==PROJECT and actual.dataset_id==self.scope.dataset.split('.')[1]
                and actual.table_id==name and actual.expires is not None
                and _schema(actual.schema)==_schema(TABLES[name].schema), 'SCHEMA_MISMATCH')
            props = dict(actual._properties)
            props['tableReference'] = TABLES[name].table(PROJECT).reference.to_api_repr()
            # Owned probes require expiry; serving preflight requires none.
            # Adapt that deliberate fixture-only difference after verifying it.
            props.pop('expirationTime',None)
            return bigquery.Table.from_api_repr(props)
        def query(script, **kwargs):
            return original_query(self.scope.relocate(script), **kwargs)
        # Native matching must expect relocated SQL, never fabricate job metadata.
        from src.application import prompt_runtime_reader as module
        original_match = module.matches_sharing_job
        def match(job, **kwargs):
            return original_match(job, **(kwargs | {'sql': self.scope.relocate(kwargs['sql'])}))
        with patch.object(self.client, 'get_table', get_table), patch.object(self.client, 'query', query), \
                patch.object(module, 'matches_sharing_job', match):
            row = reader._read_binding(self.request(context), resolved.binding.invocation_id)
        require(row['bindings'] == [resolved.binding.row()], 'STORED_BINDING_MISMATCH')
        return row

    def run(self, group):
        require(group in CASE_GROUPS, 'INVALID_RUNTIME_CASE_GROUP')
        self.report['case_group'] = group
        names = {'CORE': ('core','lost','rollback','postcommit','unknown','duplicate'),
                 'DENIALS': ('expiry','binding_expiry','policy','revocation','missing','stored','stale'), 'RACE': ('race',)}
        scenarios = tuple(dict.fromkeys(name for part in (names if group == 'ALL' else (group,)) for name in names[part]))
        rows, contexts = runtime_fixtures(scenarios=scenarios)
        for name, values in rows.items():
            self.load(name, values)
        if group in {'ALL','CORE'}:
            self.core(contexts)
        if group in {'ALL','DENIALS'}:
            self.denials(contexts)
        if group in {'ALL','RACE'}:
            self.race(contexts['race'])

    def race(self, context):
        from src.validation.prompt_runtime_race import run_race
        run_race(self, context)

    def scoped_plan(self, plan, fault=None):
        return scoped_plan(plan, self.scope, fault)

    def replace_parameter(self, plan, name, value):
        cfg = plan.config()
        values = cfg.query_parameters
        require(sum(p.name == name for p in values) == 1, 'FIXTURE_PARAMETER_MISSING')
        for parameter in values:
            if parameter.name == name:
                parameter.value = value
        cfg.query_parameters = values
        return replace(plan, config_json=json.dumps(cfg.to_api_repr(), sort_keys=True))

    def denials(self, contexts):
        sentinel = self.snapshot()
        raw, resolved = self.prepare(contexts['expiry'])
        # Frozen historical observation with a canonically encoded expired
        # report. Cases and registry evaluation identity remain unchanged.
        from src.application.prompt_evaluation_storage import encode_evaluation
        record = replace(resolved.capture.evaluation.record,
            valid_until=resolved.capture.evaluation.record.completed_at+timedelta(seconds=1))
        summary, _ = encode_evaluation(resolved.capture.evaluation.control, record)
        from src.application.prompt_evaluation_query import json_row
        spec = TABLES['prompt_evaluations']
        assignments = ','.join(f'{field.name}=r.{field.name}' for field in spec.schema)
        self.query(f'UPDATE `{self.scope.table(spec.name)}` t SET {assignments} '
            f'FROM (SELECT {json_row(spec.schema,"PARSE_JSON(@fixture_row)")}) r '
            'WHERE t.workspace_id=r.workspace_id AND t.evaluation_id=r.evaluation_id; SELECT 1 AS updated',
            query_config([bigquery.ScalarQueryParameter('fixture_row','STRING',wire(summary))]))
        raw = self.replace_parameter(raw, 'runtime_prompt_evaluations_json', wire([summary]))
        before = self.snapshot()
        self.execute(scoped_plan(raw,self.scope),status=lifecycle.Status.NOT_COMMITTED,
            receipt=False,error='RUNTIME_EVALUATION_EXPIRED')
        require(self.snapshot()==before,'EXPIRY_CHANGED_ROWS')
        self.record('expired_evaluation_denies')
        raw, resolved = self.prepare(contexts['binding_expiry'])
        # Native guard on the immutable binding's eligibility deadline. The
        # supplied binding is historical; no expired runtime resolve is claimed.
        from src.application.prompt_runtime_contract import canonical
        payload=json.loads(resolved.binding.binding_canonical_json)
        expiry=datetime.now(timezone.utc)-timedelta(seconds=1)
        payload['eligibility']['eligible_until']=expiry.isoformat()
        binding=replace(resolved.binding,binding_canonical_json=canonical(payload),bound_at=expiry-timedelta(seconds=1))
        raw=self.replace_parameter(raw,'runtime_binding',wire(binding.row()))
        before=self.snapshot()
        self.execute(scoped_plan(raw,self.scope),status=lifecycle.Status.NOT_COMMITTED,
            receipt=False,error='RUNTIME_POLICY_EXPIRED')
        require(self.snapshot()==before,'POLICY_EXPIRY_CHANGED_ROWS')
        self.record('expired_binding_deadline_denies')
        context=contexts['policy']
        raw,resolved=self.prepare(context)
        old=resolved.capture.evaluation.control
        new=replace(old,revision=2,parent_digest=old.digest,updated_at=datetime.now(timezone.utc),
            policy=replace(old.policy,version='fixture-v2'))
        write=PromptEvaluationWrite(EvaluationWriteKind.CONTROL,new,self.request(context).identity,1,
            previous=old,control_history=(old,))
        self.query(self.scope.relocate(write.sql),write.job_config())
        before=self.snapshot()
        self.execute(scoped_plan(raw,self.scope),status=lifecycle.Status.NOT_COMMITTED,
            receipt=False,error='DISPATCH_CONFLICT')
        require(self.snapshot()==before,'STALE_POLICY_CHANGED_ROWS')
        self.record('changed_policy_denies')
        context=contexts['revocation'];raw,_=self.prepare(context)
        self.query(f"UPDATE `{self.scope.table('memberships')}` SET status='DISABLED',revision=revision+1 "
            f"WHERE workspace_id='{context['workspace']}'; SELECT 1 AS updated")
        before=self.snapshot()
        self.execute(scoped_plan(raw,self.scope),status=lifecycle.Status.NOT_COMMITTED,
            receipt=False,error='DISPATCH_CONFLICT')
        require(self.snapshot()==before,'REVOKED_CLAIM_CHANGED_ROWS')
        self.record('revoked_membership_denies')
        context=contexts['missing'];raw,_=self.prepare(context)
        self.query(f"DELETE FROM `{self.scope.table('prompt_registry_history')}` WHERE prompt_key='{context['key']}' "
            "AND revision=1 AND changed_at BETWEEN TIMESTAMP('0001-01-01') AND TIMESTAMP('9999-12-31'); SELECT 1 AS updated")
        before=self.snapshot()
        try:self.capture(context)
        except Exception as error:
            require(getattr(error,'code',None) in {'INVALID_PROMPT_REGISTRY_HISTORY','PROMPT_HISTORY_INCOMPLETE',
                'INVALID_PROMPT_HISTORY_LENGTH','PROMPT_HISTORY_REVISION_GAP','PROMPT_REGISTRY_HISTORY_MISMATCH',
                'INVALID_PROMPT_REGISTRY_INITIAL_STATE'},
                'WRONG_MISSING_HISTORY_DENIAL')
        else:raise RuntimeError('MISSING_HISTORY_ACCEPTED')
        self.execute(scoped_plan(raw,self.scope),status=lifecycle.Status.NOT_COMMITTED,
            receipt=False,error='RUNTIME_PROMPT_CONFLICT')
        require(self.snapshot()==before,'MISSING_HISTORY_CHANGED_ROWS')
        self.record('missing_history_denies')
        context=contexts['stored'];raw,resolved=self.prepare(context)
        before=self.snapshot()
        try:self.binding_read(context,resolved)
        except Exception as error:
            require(getattr(error,'code',None)=='RUNTIME_BINDING_READ_INVALID','WRONG_MISSING_BINDING_DENIAL')
        else:raise RuntimeError('MISSING_BINDING_ACCEPTED')
        require(self.snapshot()==before,'MISSING_BINDING_READ_CHANGED_ROWS')
        self.load('invocation_prompt_bindings',[resolved.binding.row()])
        self.query(f"UPDATE `{self.scope.table('invocation_prompt_bindings')}` SET binding_sha256=REPEAT('a',64) "
            f"WHERE workspace_id='{context['workspace']}'; SELECT 1 AS updated")
        before=self.snapshot()
        try:self.binding_read(context,resolved)
        except RuntimeError as error:require(str(error)=='STORED_BINDING_MISMATCH','WRONG_CORRUPT_BINDING_DENIAL')
        else:raise RuntimeError('CORRUPT_BINDING_ACCEPTED')
        require(self.snapshot()==before,'CORRUPT_BINDING_READ_CHANGED_ROWS')
        self.record('missing_and_corrupt_stored_binding_deny')
        context=contexts['stale'];raw,_=self.prepare(context)
        self.promote(context)
        before=self.snapshot()
        self.execute(scoped_plan(raw,self.scope),status=lifecycle.Status.NOT_COMMITTED,
            receipt=False,error='RUNTIME_PROMPT_CONFLICT')
        require(self.snapshot()==before,'STALE_LIFECYCLE_CHANGED_ROWS')
        self.record('stale_claim_after_promotion_denies')
        after=self.snapshot()
        for name in TABLES:
            unrelated=lambda values:[row for row in values if json.loads(row).get('workspace_id')=='fixture-unrelated-workspace']
            require(unrelated(sentinel[name])==unrelated(after[name]),'UNRELATED_ROWS_CHANGED')
        self.record('unrelated_fixture_rows_preserved')

    def promotion_plan(self, context):
        from src.application import prompt_lifecycle_store as storage
        from src.application.prompt_lifecycle_contract import LifecycleReadRequest
        source=context['states'][1].head.snapshot
        request=LifecycleReadRequest(context['other'],context['key'],source.task,source.vertex_prompt_id,
            source.vertex_version_id,self.request(context).identity,1,context['reports'][1].evaluation_id)
        capture=storage.decode_snapshot(request,self.query(self.scope.relocate(storage.read_sql()),storage.read_config(request)))
        return lifecycle.ScopedLifecycleWrite(capture,PromptLifecycle.ACTIVE,datetime.now(timezone.utc),
            'FIXTURE_REVIEWED',fixture_scope=self.scope,fixture_slot='promotion')

    def promote(self, context):
        plan=self.promotion_plan(context)
        with lifecycle.owned_sql(self.scope):
            store=lifecycle.FixtureLifecycleStore(self)
            outcome=store.submit(plan)
            if outcome.native_status is lifecycle.Status.UNKNOWN:outcome=store.recover(plan)
            require(outcome.native_status is lifecycle.Status.COMMITTED and outcome.receipt['state']=='APPLIED',
                'PROMOTION_NOT_COMMITTED')
        return plan,outcome


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--case-group', choices=CASE_GROUPS, default='ALL')
    parser.add_argument('--inspect-report', type=Path, help='Read-only terminal native-job metadata for a retained report')
    args = parser.parse_args()
    with bigquery.Client(project=PROJECT, location=LOCATION) as client, fixture_tables():
        if args.inspect_report:
            report=json.loads(args.inspect_report.read_text())
            run_id=report['fixture_dataset'].split('_')[-1]
            scope=RuntimeScope(run_id)
            require(report['fixture_dataset']==scope.dataset,'INVALID_INSPECTION_SCOPE')
            records=[]
            for entry in report['jobs']:
                require(entry['job_id'].startswith('runtime_it_'+run_id+'_'),'INVALID_INSPECTION_JOB')
                job=client.get_job(entry['job_id'],project=PROJECT,location=LOCATION,retry=None,timeout=30)
                scope.assert_sql(job.query)
                records.append(dict(job_id=job.job_id,state=job.state,error=job.error_result,
                    bytes_processed=job.total_bytes_processed))
            print(json.dumps(records,indent=2))
            return
        probe = RuntimeProbe(client)
        before = probe.serving_rows()
        try:
            probe.setup()
            probe.run(args.case_group)
            validate_native_report(probe.report)
            probe.report['status'] = 'PASSED'
        except Exception as error:
            probe.report.update(status='FAILED', error_code=getattr(error,'code',type(error).__name__),
                fixture_error=str(error)[:1500])
        finally:
            try:
                probe.cleanup()
            except Exception as error:
                probe.report.update(status='FAILED', cleanup='FAILED', cleanup_error=str(error)[:300])
            try:
                after = probe.serving_rows()
                require(before == after, 'SERVING_METADATA_CHANGED')
                probe.report.update(serving_rows_before=before, serving_rows_after=after,
                    serving_row_metadata_unchanged=True)
            except Exception as error:
                probe.report.update(status='FAILED', preservation_error=str(error)[:200])
            args.output_dir.mkdir(parents=True, exist_ok=True)
            path = args.output_dir / (probe.scope.run_id+'.json')
            path.write_text(json.dumps(probe.report, indent=2, default=str)+'\n')
            print(json.dumps(dict(event='runtime_complete', status=probe.report['status'],
                cases=len(probe.report['cases']), cleanup=probe.report['cleanup'], report=str(path))), flush=True)
        require(probe.report['status'] == 'PASSED', 'PROMPT_RUNTIME_PROBE_FAILED')


if __name__ == '__main__':
    main()
