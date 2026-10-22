"""4.2 Critical/Must have/shared pilot: owned native signing mutation checks.

--run --output-dir PATH opts into expiring fixture writes and owned cleanup.
No serving rows, genesis bootstrap, IAP deployment, secret access or activation.
Runs the installed SQL/query/result/recovery primitives with explicit fixture
actors. It does not claim deployed ProtectedSigningWriter/IAM verification.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import islice
import json
from pathlib import Path
import re
from threading import Barrier, Lock

from google.cloud import bigquery

from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_jobs import failed_sharing_job_status, matches_sharing_job
from src.application.sharing_publication_codec import decode_read_result, normalize
from src.application.sharing_publication_contract import POLICY_ID, propose_publication
from src.application.sharing_publication_reader import SigningCapturedRead, read_config, read_sql
from src.application.sharing_publication_read_contract import SigningReadLimits
from src.application.signing_write_query import SigningWriteKind, prepare_write, validate_write_result
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.validation.signing_read_integration import Probe as ReadProbe, ProbeScope, require

ACTOR = 'iap-v1:' + 'a' * 64  # Explicit native fixture, not an authenticated marketing user.


@dataclass(frozen=True)
class WriteProbeScope(ProbeScope):
    @property
    def dataset(self):
        return f'{PROJECT}.ci_signing_write_it_{self.run_id}'

    @property
    def labels(self):
        return dict(purpose='signing_write_integration', run_id=self.run_id)


class Probe(ReadProbe):
    def __init__(self, client):
        super().__init__(client)
        self.scope = WriteProbeScope(self.scope.run_id)
        self.report.update(scope='NATIVE_WRITE_SQL_QUERY_RESULT_AND_JOB_RECOVERY',fixture_dataset=self.scope.dataset)
        self._lock = Lock()
        # Sentinels prove scoped DML preserves other policies and consumers.
        for name, rows in self.rows.items():
            rows.extend(dict(row, policy_id='fixture-unrelated-policy') for row in deepcopy(rows))
        self.rows['signing_checkpoints'].append(dict(self.rows['signing_checkpoints'][0],
                                                    consumer_id='fixture-other-consumer'))

    def record(self, name, **details):
        self.report['cases'].append(dict(case=name, status='PASSED', **details))
        print(json.dumps(dict(event='signing_write_probe_case', case=name, status='PASSED')), flush=True)

    @staticmethod
    def in_scope(name, row):
        return row['policy_id'] == POLICY_ID and (name != 'signing_checkpoints'
                                                  or row['consumer_id'] == 'fixture-consumer')

    def job_id(self):
        with self._lock:
            self.counter += 1
            return f'signing_write_it_{self.scope.run_id}_{self.counter}'

    def capture(self):
        self.report['stage'] = 'CAPTURE'
        self.own()
        request = self.request()
        sql = self.scope.relocate(read_sql())
        config = read_config(request,SigningReadLimits())
        expected = bigquery.QueryJobConfig.from_api_repr(deepcopy(config.to_api_repr()))
        job_id = self.job_id()
        job = self.client.query(sql,job_config=config,job_id=job_id,project=PROJECT,location=LOCATION,
                                retry=None,job_retry=None,timeout=10)
        rows = [normalize(row) for row in islice(job.result(max_results=2,page_size=2,timeout=20,
                                                          retry=None,job_retry=None),2)]
        require(matches_sharing_job(job,job_id=job_id,sql=sql,config=expected)
                and job.state=='DONE' and job.error_result is None,'INVALID_CAPTURE_JOB')
        observation = decode_read_result(request,rows,now=datetime.now(timezone.utc))
        return SigningCapturedRead(request,observation,rows[0])

    def plan(self,command,kind=SigningWriteKind.PUBLICATION):
        cap = self.capture()
        previous = cap.observation.publications[-1]
        pub = propose_publication(previous,config=previous.config,status=previous.status,
                                  published_at=previous.published_at) if kind is SigningWriteKind.PUBLICATION else None
        return prepare_write(cap,kind=kind,actor_id=ACTOR,command_id=command,publication=pub)

    def execute(self,plan,*,fault=None,job_id=None):
        self.report['stage']='WRITE'
        self.own()
        sql = self.scope.relocate(plan.sql)
        if fault=='rollback':
            sql=sql.replace('-- PROBE_AFTER_FENCE',"ASSERT FALSE AS 'SIGNING_PROBE_ROLLBACK';")
        if fault=='postcommit':
            sql=sql.replace('-- PROBE_AFTER_COMMIT',"ASSERT FALSE AS 'SIGNING_PROBE_POSTCOMMIT';")
        self.scope.assert_sql(sql)
        config = plan.job_config()
        expected=bigquery.QueryJobConfig.from_api_repr(deepcopy(config.to_api_repr()))
        job_id=self.job_id() if job_id is None else job_id
        self.report['last_job_id']=job_id
        job = self.client.query(sql,job_config=config,job_id=job_id,project=PROJECT,location=LOCATION,
                                retry=None,job_retry=None,timeout=10)
        rows=None
        try:
            rows=[normalize(row) for row in islice(job.result(max_results=2,page_size=2,timeout=60,
                                                             retry=None,job_retry=None),2)]
        except Exception:
            pass
        parent=self.client.get_job(job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
        require(parent.state=='DONE' and matches_sharing_job(parent,job_id=job_id,sql=sql,config=expected),
                'INVALID_WRITE_JOB')
        status=(failed_sharing_job_status(self.client,job_id=job_id,timeout_seconds=10,parent=parent)
                if parent.error_result else SharingCommitStatus.COMMITTED)
        result=None
        if parent.error_result is None:
            require(rows is not None,'WRITE_RESULT_LOST')
            result=validate_write_result(plan,rows)
        info=dict(job_id=job_id,native_status=status.value,error_reason=(parent.error_result or {}).get('reason'),
                  bytes_processed=parent.total_bytes_processed,bytes_billed=parent.total_bytes_billed,
                  result_state=None if result is None else result.state.value)
        # Only this owned, key-free fixture records native SQL diagnostics.
        info['error_message'] = (parent.error_result or {}).get('message', '')[:500]
        with self._lock:
            self.report['jobs'].append(info)
        return dict(job=parent,info=info,status=status,result=result,sql=sql,config=expected)

    def recover(self,plan,execution):
        # Original job only; no query submission. Deliberately discard first result.
        job=self.client.get_job(execution['info']['job_id'],project=PROJECT,location=LOCATION,retry=None,timeout=10)
        require(matches_sharing_job(job,job_id=job.job_id,sql=execution['sql'],config=execution['config']),
                'RECOVERY_JOB_MISMATCH')
        rows=[normalize(row) for row in islice(job.result(max_results=2,page_size=2,timeout=20,
                                                         retry=None,job_retry=None),2)]
        return validate_write_result(plan,rows)

    def snapshot(self, *, unrelated=False):
        self.own()
        data={}
        for name in self.rows:
            sql=f'SELECT * FROM `{self.scope.table(name)}`'
            self.scope.assert_sql(sql)
            job=self.client.query(sql,job_id=self.job_id(),location=LOCATION,project=PROJECT,
                job_config=bigquery.QueryJobConfig(use_query_cache=False,use_legacy_sql=False,
                    maximum_bytes_billed=64*1024**2,job_timeout_ms=20000),retry=None,job_retry=None,timeout=10)
            data[name]=[normalize(row) for row in job.result(timeout=20,retry=None,job_retry=None)]
            data[name]=[row for row in data[name] if self.in_scope(name, row) != unrelated]
            data[name].sort(key=lambda row:json.dumps(row,sort_keys=True,default=str))
        return data

    def race(self,left,right):
        barrier=Barrier(2)
        def run(plan):
            barrier.wait(timeout=10)
            return self.execute(plan)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(run,plan) for plan in (left,right)]
            outcomes=[f.result(timeout=120) for f in futures]
        intervals=[]
        for outcome in outcomes:
            job_id=outcome['info']['job_id']
            children=list(self.client.list_jobs(project=PROJECT,parent_job=job_id,all_users=False,
                max_results=64,page_size=64,retry=None,timeout=10))
            require(0<len(children)<64 and all(isinstance(c,bigquery.QueryJob) and c.parent_job_id==job_id
                and c.project==PROJECT and c.location==LOCATION and c.state=='DONE' for c in children),
                'INVALID_RACE_CHILDREN')
            begin=[c.ended for c in children if c.statement_type=='BEGIN_TRANSACTION']
            end=[c.started for c in children if c.statement_type in ('COMMIT_TRANSACTION','ROLLBACK_TRANSACTION')]
            require(len(begin)==len(end)==1 and all(begin+end),'INVALID_TRANSACTION_INTERVAL')
            intervals.append((begin[0],end[0]))
            outcome['info']['transaction_interval']=[at.isoformat() for at in intervals[-1]]
        require(max(i[0] for i in intervals)<min(i[1] for i in intervals),'RACE_DID_NOT_OVERLAP')
        return outcomes

    def reset(self):
        for name,rows in self.rows.items():
            self.load(name,rows)

    def cases(self):
        baseline=self.snapshot()
        sentinels=self.snapshot(unrelated=True)
        first=self.plan('publication-first')
        published=self.execute(first)
        require(published['result'].state.value=='APPLIED','PUBLICATION_NOT_APPLIED')
        after=self.snapshot()
        require(len(after['signing_publications'])==6 and len(after['signing_key_history'])==6
                and after['signing_heads'][0]['control_revision']==8
                and after['signing_checkpoints']==baseline['signing_checkpoints'],'PUBLICATION_STATE_INVALID')
        self.record('publication_atomic_head_history_and_immutable_stamps')
        reused=self.execute(first)
        require(reused['result'].state.value=='REUSED' and self.snapshot()==after,'REPLAY_MUTATED_STATE')
        self.record('exact_publication_replay')
        recovered=self.recover(first,published)
        require(recovered==published['result'],'RECOVERY_RESULT_MISMATCH')
        self.record('discarded_response_original_job_recovery')
        changed=deepcopy(json.loads(first.publication_json));changed[0]['command_id']='different'
        from dataclasses import replace
        conflict=replace(first,publication_json=json.dumps(changed,sort_keys=True,separators=(',',':')))
        failed=self.execute(conflict)
        require(failed['status'] is SharingCommitStatus.NOT_COMMITTED and self.snapshot()==after,'CONFLICT_NOT_ROLLED_BACK')
        self.record('changed_same_revision_rejected')
        floor=self.plan('checkpoint-advance',SigningWriteKind.CHECKPOINT)
        applied=self.execute(floor)
        require(applied['result'].state.value=='APPLIED','CHECKPOINT_NOT_APPLIED')
        checkpoint_state=self.snapshot()
        require(checkpoint_state['signing_checkpoints'][0]['checkpoint_revision']==3
                and checkpoint_state['signing_checkpoints'][0]['publication_revision']==6
                and checkpoint_state['signing_heads'][0]['control_revision']==9,'INVALID_CHECKPOINT_STATE')
        self.record('checkpoint_cas_advance_shared_fence')
        require(self.execute(floor)['result'].state.value=='REUSED' and self.snapshot()==checkpoint_state,
                'CHECKPOINT_REPLAY_MUTATED')
        self.record('checkpoint_replay_preserves_floor')
        stale=self.plan('stale-parent')
        later=self.plan('later-parent');self.execute(later)
        before=self.snapshot()
        require(self.execute(stale)['status'] is SharingCommitStatus.NOT_COMMITTED and self.snapshot()==before,
                'STALE_PARENT_CHANGED_STATE')
        self.record('competing_parent_rejected')
        rollback=self.plan('rollback')
        before=self.snapshot()
        rolled=self.execute(rollback,fault='rollback')
        require(rolled['status'] is SharingCommitStatus.NOT_COMMITTED and self.snapshot()==before,'ROLLBACK_CHANGED_STATE')
        self.record('rollback_after_control_fence')
        committed=self.plan('postcommit')
        failed=self.execute(committed,fault='postcommit')
        require(failed['status'] is SharingCommitStatus.COMMITTED and self.snapshot()['signing_heads'][0]['revision']==8,
                'POSTCOMMIT_MISCLASSIFIED')
        self.record('postcommit_failure_positive_child_commit')
        self.reset()
        left,right=self.plan('left'),self.plan('right')
        outcomes=self.race(left,right)
        require(sorted(o['status'].value for o in outcomes)==['COMMITTED','NOT_COMMITTED'], 'PUBLICATION_RACE_UNSAFE')
        require(len(self.snapshot()['signing_publications'])==6,'PUBLICATION_RACE_DUPLICATED')
        self.record('overlapping_publication_fence_race')
        self.reset()
        self.load('signing_checkpoints',[row for row in self.rows['signing_checkpoints']
                                         if not self.in_scope('signing_checkpoints', row)])
        left,right=self.plan('first-floor-left',SigningWriteKind.CHECKPOINT),self.plan('first-floor-right',SigningWriteKind.CHECKPOINT)
        outcomes=self.race(left,right)
        require(sorted(o['status'].value for o in outcomes)==['COMMITTED','NOT_COMMITTED'],'CHECKPOINT_RACE_UNSAFE')
        require(len(self.snapshot()['signing_checkpoints'])==1,'FIRST_CHECKPOINT_DUPLICATED')
        self.record('overlapping_first_checkpoint_shared_head_fence')
        self.reset()
        left=self.plan('mixed-publication');right=self.plan('mixed-checkpoint',SigningWriteKind.CHECKPOINT)
        outcomes=self.race(left,right)
        require(sorted(o['status'].value for o in outcomes)==['COMMITTED','NOT_COMMITTED'],'MIXED_RACE_UNSAFE')
        self.record('publication_and_checkpoint_share_one_fence')
        require(self.snapshot(unrelated=True)==sentinels, 'UNRELATED_ROWS_CHANGED')
        self.record('other_policies_and_consumers_preserved')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true',required=True)
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client)
        try:
            probe.setup();probe.cases();probe.report['status']='PASSED'
        except Exception as error:
            code=getattr(error,'code',None)
            if not isinstance(code,str):code=str(error) if isinstance(error,RuntimeError) else ''
            probe.report.update(status='FAILED',error_code=code if re.fullmatch(r'[A-Z_]+',code) else type(error).__name__)
        finally:
            try:probe.cleanup()
            except Exception:probe.report.update(status='FAILED',cleanup='FAILED')
            args.output_dir.mkdir(parents=True,exist_ok=True)
            path=args.output_dir/(probe.scope.run_id+'.json')
            path.write_text(json.dumps(probe.report,indent=2)+'\n',encoding='utf-8')
            print(json.dumps(dict(event='signing_write_probe_complete',status=probe.report['status'],
                cases=len(probe.report['cases']),cleanup=probe.report['cleanup'],report=str(path))),flush=True)
        require(probe.report['status']=='PASSED','SIGNING_WRITE_PROBE_FAILED')


if __name__=='__main__':main()
