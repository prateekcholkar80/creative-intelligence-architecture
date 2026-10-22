"""4.2 Critical/Must have/shared pilot: isolated native bootstrap verification.

--run creates/deletes an expiring owned dataset. Production SQL/adapter are used
with strictly relocated resources/job slots and explicit mocked IAP identities.
This is not deployed IAP/IAM, secret loading, serving bootstrap or activation.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from itertools import islice
import json
from pathlib import Path
import re
from threading import Barrier
from unittest.mock import Mock, patch

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application import signing_bootstrap as bootstrap
from src.application.identity import ISSUER, IapIdentityAdapter, IapSettings, VerifiedIdentity, principal_for_subject
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_publication_codec import normalize
from src.application.sharing_publication_contract import POLICY_ID
from src.application.signing_control_writer import SigningWriteError
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
from src.validation.signing_read_integration import Probe as ReadProbe, ProbeScope, require

ORIGINAL_JOB_ID = bootstrap.bootstrap_job_id
AUDIENCE = '/projects/1/locations/asia-south1/services/fixture-bootstrap'


@dataclass(frozen=True)
class BootstrapProbeScope(ProbeScope):
    @property
    def dataset(self):
        return f'{PROJECT}.ci_signing_bootstrap_it_{self.run_id}'

    @property
    def labels(self):
        return dict(purpose='signing_bootstrap_integration',run_id=self.run_id)

    def slot(self, case: int, attempt: int):
        ORIGINAL_JOB_ID(attempt)
        require(type(case) is int and 0 <= case < 20,'INVALID_BOOTSTRAP_PROBE_CASE')
        return f'signing_bootstrap_it_{self.run_id}_{case}_{attempt}'


class Probe(ReadProbe):
    def __init__(self, client):
        super().__init__(client)
        self.scope=BootstrapProbeScope(self.scope.run_id)
        self.rows={name:[dict(row,policy_id='fixture-unrelated-policy') for row in rows]
                   for name,rows in self.rows.items()}
        self.report.update(scope='NATIVE_BOOTSTRAP_SQL_AND_ADAPTER_WITH_MOCKED_IDENTITY',fixture_dataset=self.scope.dataset)
        self.base_sql=self.scope.relocate(bootstrap.bootstrap_sql())
        self.case=-1
        self.sql=self.base_sql
        self.current=None

    def record(self,name,**details):
        self.report['cases'].append(dict(case=name,status='PASSED',**details))
        print(json.dumps(dict(event='signing_bootstrap_probe_case',case=name,status='PASSED')),flush=True)

    def identity(self, subject):
        adapter=IapIdentityAdapter(IapSettings(AUDIENCE))
        def authenticate(headers):
            epoch=int(datetime.now(timezone.utc).timestamp())
            return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,epoch-10,epoch+590)
        adapter.authenticate=Mock(side_effect=authenticate)
        return adapter

    def adapter(self, *, subject='fixture-bootstrap-owner', manifest=None):
        return bootstrap.ProtectedSigningBootstrap(client=self.client,identity=self.identity(subject),
            manifest=self.current if manifest is None else manifest,enabled=True)

    def next_case(self, *, fault=None):
        self.case+=1
        for name,rows in self.rows.items():self.load(name,rows)
        instant=datetime.now(timezone.utc)
        self.current=bootstrap.SigningBootstrapManifest(principal_for_subject(ISSUER,'fixture-bootstrap-owner').principal_id,
            self.pubs[0],instant,instant+timedelta(seconds=300))
        self.sql=self.base_sql
        if fault in ('rollback-first','rollback-all'):
            statement="ASSERT FALSE AS 'BOOTSTRAP_PROBE_ROLLBACK';"
            if fault=='rollback-first':statement="IF @attempt=0 THEN "+statement+" END IF;"
            self.sql=self.sql.replace('-- PROBE_AFTER_PUBLICATION',statement)
        elif fault=='postcommit':
            self.sql=self.sql.replace('-- PROBE_AFTER_COMMIT',"ASSERT FALSE AS 'BOOTSTRAP_PROBE_POSTCOMMIT';")
        self.scope.assert_sql(self.sql)

    def snapshot(self, *, unrelated=False):
        self.own()
        data={}
        for name in self.rows:
            sql=f'SELECT * FROM `{self.scope.table(name)}`'
            self.scope.assert_sql(sql)
            job=self.client.query(sql,job_id=self.job_id(),project=PROJECT,location=LOCATION,
                job_config=bigquery.QueryJobConfig(use_query_cache=False,use_legacy_sql=False,
                    maximum_bytes_billed=64*1024**2,job_timeout_ms=20000),retry=None,job_retry=None,timeout=10)
            data[name]=[normalize(row) for row in job.result(timeout=20,retry=None,job_retry=None)
                        if (row['policy_id']!=POLICY_ID)==unrelated]
            data[name].sort(key=lambda row:json.dumps(row,sort_keys=True,default=str))
        return data

    def wait_slot(self, attempt):
        self.own()
        job_id=self.scope.slot(self.case,attempt)
        job=self.client.get_job(job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
        try:job.result(max_results=2,page_size=2,timeout=60,retry=None,job_retry=None)
        except Exception:pass
        job=self.client.get_job(job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
        require(job.state=='DONE','BOOTSTRAP_SLOT_STILL_RUNNING')
        return job

    def execute(self,adapter,attempt=0):
        self.report['stage']='BOOTSTRAP'
        adapter.submit({},attempt=attempt)
        parent=self.wait_slot(attempt)
        outcome=adapter.recover({},attempt=attempt)
        self.report['jobs'].append(dict(job_id=parent.job_id,native_status=outcome.native_status.value,
            bytes_processed=parent.total_bytes_processed,bytes_billed=parent.total_bytes_billed,
            error_reason=(parent.error_result or {}).get('reason'),
            error_message=(parent.error_result or {}).get('message','')[:500]))
        return outcome

    def race(self, first, second):
        barrier=Barrier(2)
        def run(adapter):
            barrier.wait(timeout=10)
            return adapter.submit({})
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(run,adapter) for adapter in (first,second)]
            [future.result(timeout=90) for future in futures]
        parent=self.wait_slot(0)
        children=list(islice(self.client.list_jobs(project=PROJECT,parent_job=parent.job_id,all_users=False,
            max_results=64,page_size=64,retry=None,timeout=10),64))
        require(0<len(children)<64 and all(isinstance(child,bigquery.QueryJob) and child.parent_job_id==parent.job_id
            and child.project==PROJECT and child.location==LOCATION and child.state=='DONE' for child in children),
            'INVALID_BOOTSTRAP_CHILDREN')
        require(sum(child.statement_type=='COMMIT_TRANSACTION' and child.error_result is None
                    for child in children)==1,'BOOTSTRAP_EXECUTED_MULTIPLE_TIMES')
        for attempt in (1,2):
            try:self.client.get_job(self.scope.slot(self.case,attempt),project=PROJECT,location=LOCATION,retry=None,timeout=10)
            except NotFound:pass
            else:raise RuntimeError('UNEXPECTED_BOOTSTRAP_SLOT')
        self.report['jobs'].append(dict(job_id=parent.job_id,child_count=len(children),successful_commits=1,
                                       bytes_billed=parent.total_bytes_billed))
        return [adapter.recover({}) for adapter in (first,second)]

    def cases(self):
        sentinels=self.snapshot(unrelated=True)
        self.next_case()
        adapter=self.adapter()
        first=self.execute(adapter)
        require(first.native_status is SharingCommitStatus.COMMITTED and first.receipt is not None,'GENESIS_NOT_COMMITTED')
        state=self.snapshot()
        require([len(state[n]) for n in self.rows]==[1,1,1,0],'GENESIS_NOT_ATOMIC')
        self.record('atomic_initial_publication_history_head')
        observed=self.read(self.request())
        require(len(observed.publications)==1 and observed.publications[0].digest==self.current.publication.digest,
                'BOOTSTRAPPED_GENESIS_READ_INVALID')
        self.record('authoritative_reader_accepts_bootstrapped_genesis')
        before=self.client.query.call_count
        require(adapter.submit({})==first and self.adapter().submit({})==first,'BOOTSTRAP_REPLAY_CHANGED')
        require(self.client.query.call_count==before,'BOOTSTRAP_REPLAY_RESUBMITTED')
        self.record('discarded_response_and_restart_original_slot_recovery')
        changed=self.adapter(manifest=replace(self.current,accepted_at=self.current.accepted_at+timedelta(seconds=1)))
        require(changed.submit({}).native_status is SharingCommitStatus.UNKNOWN,'CHANGED_MANIFEST_ACCEPTED')
        try:changed.submit({},attempt=1)
        except SigningWriteError:pass
        else:raise RuntimeError('CHANGED_MANIFEST_ADVANCED')
        require(self.snapshot()==state,'CHANGED_MANIFEST_MUTATED')
        self.record('changed_manifest_cannot_remint_or_advance')
        self.next_case()
        outcomes=self.race(self.adapter(),self.adapter())
        require(all(o.native_status is SharingCommitStatus.COMMITTED and o.receipt for o in outcomes),'DUPLICATE_RECOVERY_FAILED')
        state=self.snapshot()
        require([len(state[n]) for n in self.rows]==[1,1,1,0],'DUPLICATE_GENESIS_CREATED')
        self.record('concurrent_identical_callers_one_native_job_one_commit')
        self.next_case()
        other_subject='fixture-other-owner'
        other=replace(self.current,owner_principal_id=principal_for_subject(ISSUER,other_subject).principal_id)
        outcomes=self.race(self.adapter(),self.adapter(subject=other_subject,manifest=other))
        require(sorted(o.native_status.value for o in outcomes)==['COMMITTED','UNKNOWN'],'CONFLICTING_OWNER_SLOT_UNSAFE')
        state=self.snapshot()
        require([len(state[n]) for n in self.rows]==[1,1,1,0],'CONFLICTING_GENESIS_DUPLICATED')
        self.record('conflicting_owners_share_same_native_slot')
        self.next_case(fault='rollback-first')
        adapter=self.adapter()
        require(self.execute(adapter).native_status is SharingCommitStatus.NOT_COMMITTED
                and all(not rows for rows in self.snapshot().values()),'PARTIAL_GENESIS_SURVIVED')
        self.record('rollback_after_publication_leaves_no_partial_policy')
        require(self.execute(adapter,1).native_status is SharingCommitStatus.COMMITTED,'POSITIVE_ROLLBACK_RETRY_FAILED')
        try:adapter.submit({},attempt=2)
        except SigningWriteError:pass
        else:raise RuntimeError('RETRIED_COMMITTED_ANCESTOR')
        self.record('fresh_positive_rollback_admits_next_slot_commit_blocks_later_slot')
        self.next_case()
        adapter=self.adapter()
        require(adapter.recover({}).native_status is SharingCommitStatus.UNKNOWN,'ABSENT_RECOVERY_NOT_UNKNOWN')
        before=self.client.query.call_count
        try:adapter.submit({},attempt=1)
        except SigningWriteError:pass
        else:raise RuntimeError('UNKNOWN_PREDECESSOR_ADVANCED')
        require(self.client.query.call_count==before,'UNKNOWN_SUBMITTED_QUERY')
        self.record('unknown_absent_predecessor_never_advances')
        self.next_case(fault='postcommit')
        adapter=self.adapter()
        outcome=self.execute(adapter)
        require(outcome.native_status is SharingCommitStatus.COMMITTED and outcome.receipt is None,'POSTCOMMIT_MISCLASSIFIED')
        try:adapter.submit({},attempt=1)
        except SigningWriteError:pass
        else:raise RuntimeError('POSTCOMMIT_NEW_SLOT')
        self.record('postcommit_failure_positive_commit_blocks_retry')
        self.next_case()
        self.load('signing_key_history',self.rows['signing_key_history']+[dict(self.rows['signing_key_history'][0],policy_id=POLICY_ID)])
        before=self.snapshot()
        require(self.execute(self.adapter()).native_status is SharingCommitStatus.NOT_COMMITTED
                and self.snapshot()==before,'PARTIAL_PREEXISTING_POLICY_OVERWRITTEN')
        self.record('preexisting_orphan_policy_denies_without_overwrite')
        self.next_case(fault='rollback-all')
        adapter=self.adapter()
        for attempt in range(3):
            require(self.execute(adapter,attempt).native_status is SharingCommitStatus.NOT_COMMITTED,'ROLLBACK_SLOT_NOT_CORRELATED')
        try:adapter.submit({},attempt=3)
        except ContractError:pass
        else:raise RuntimeError('BOOTSTRAP_ATTEMPT_CAP_BYPASSED')
        require(all(not rows for rows in self.snapshot().values()),'FAILED_SLOTS_LEFT_ROWS')
        self.record('three_reserved_attempt_cap_with_all_positive_rollbacks')
        require(self.snapshot(unrelated=True)==sentinels,'UNRELATED_POLICY_CHANGED')
        self.record('unrelated_policy_rows_preserved')

    def run(self):
        # Only the owned probe overrides installed identifiers/slot namespace.
        with patch.object(bootstrap,'bootstrap_job_id',side_effect=lambda attempt:self.scope.slot(self.case,attempt)), \
             patch.object(bootstrap,'bootstrap_sql',side_effect=lambda:self.sql), \
             patch.object(bootstrap.ProtectedSigningBootstrap,'_preflight',side_effect=self.own), \
             patch.object(self.client,'query',wraps=self.client.query):
            self.cases()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',action='store_true',required=True)
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        probe=Probe(client)
        try:
            probe.setup();probe.run();probe.report['status']='PASSED'
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
            print(json.dumps(dict(event='signing_bootstrap_probe_complete',status=probe.report['status'],
                cases=len(probe.report['cases']),cleanup=probe.report['cleanup'],report=str(path))),flush=True)
        require(probe.report['status']=='PASSED','SIGNING_BOOTSTRAP_PROBE_FAILED')


if __name__=='__main__':main()
