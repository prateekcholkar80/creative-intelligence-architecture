"""Opt-in isolated bootstrap executor/read probe. Never serving bootstrap or IAP proof."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack,contextmanager
from dataclasses import replace
from datetime import datetime,timezone,timedelta
import json
from pathlib import Path
from threading import Barrier,Lock
from unittest.mock import patch
from uuid import uuid4
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application import campaign_bootstrap_reader as reader,campaign_bootstrap_executor as executor_module
from src.application.campaign_bootstrap_contract import BootstrapManifest,CONTROL_HEAD,CONTROL_HISTORY
from src.application.campaign_bootstrap_query import bootstrap_spec
from src.application.identity import IapIdentityAdapter,IapSettings,VerifiedIdentity,principal_for_subject,ISSUER
from src.application.workflow_contract import ContractError
from src.application.sharing_jobs import matches_sharing_job,failed_sharing_job_status
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.campaign_review_storage import row_json
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION
from src.validation.campaign_publication_integration import Scope as PublicationScope,TABLES,TTL_MS,snapshot,load_rows,require
from src.validation.campaign_publication_fixtures import AUDIENCE,seed

GROUPS={'CORE':('disabled','empty_capture','absent_recovery','competing_manifests','original_recovery',
    'duplicate_submit','cold_recovery','expired_window_recovery'),
    'FAULTS':('rollback','rollback_cannot_remint','postcommit','lost_response'),
    'DENIALS':('orphan_rows','partial_control','read_overflow','genesis_without_original')}
OUTPUT=Path('data/generated/campaign_bootstrap_diagnostics')


def now():return datetime.now(timezone.utc)


def actor(subject,at):
    return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,
        int(at.timestamp())-5,int(at.timestamp())+600)


class Scope(PublicationScope):
    def __init__(self,run_id):
        super().__init__(run_id);self.dataset_id='ci_campaign_bootstrap_it_'+run_id
        self.dataset=PROJECT+'.'+self.dataset_id
        self.labels={'purpose':'campaign_bootstrap_integration','run_id':run_id}


def fixture_job_id(scope):return 'campaign_bootstrap_it_'+scope.run_id+'_0'


def scoped_plan(plan,scope,fault=None):
    sql=scope.relocate(plan.sql)
    require(fault in (None,'rollback','postcommit'),'Unknown bootstrap fault')
    if fault=='rollback':
        require(sql.count('COMMIT TRANSACTION;')==1,'Bootstrap commit marker differs')
        sql=sql.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'FIXTURE_BOOTSTRAP_ROLLBACK';\nCOMMIT TRANSACTION;")
    if fault=='postcommit':
        require(sql.count('SELECT * FROM bootstrap_result;')==1,'Bootstrap result marker differs')
        sql=sql.replace('SELECT * FROM bootstrap_result;',"ASSERT FALSE AS 'FIXTURE_BOOTSTRAP_POSTCOMMIT';\nSELECT * FROM bootstrap_result;")
    require(sql.count(';')<64,'Bootstrap child bound exceeded');scope.assert_sql(sql)
    return replace(plan,sql=sql,native_job_id=fixture_job_id(scope))


class TrackedClient:
    def __init__(self,client,scope,report,barrier=None):
        self.client=client;self.scope=scope;self.report=report;self.barrier=barrier;self.lock=Lock();self.lose=False
        self.inserts=0
    def __getattr__(self,name):return getattr(self.client,name)
    def get_job(self,native,**kwargs):
        require(native==fixture_job_id(self.scope) or native.startswith('campaign_bootstrap_read_'),
            'Unowned bootstrap native lookup')
        return self.client.get_job(native,**kwargs)
    def query(self,sql,**kwargs):
        self.scope.assert_sql(sql);native=kwargs.get('job_id','')
        require(kwargs.get('retry') is None and kwargs.get('job_retry') is None,'Automatic bootstrap retry')
        mutation=native==fixture_job_id(self.scope)
        if mutation:
            with self.lock:
                self.inserts+=1;self.report['insert_attempts'].append(native)
            if self.barrier:self.barrier.wait(timeout=90)
        else:
            require(native.startswith('campaign_bootstrap_read_') and 'INSERT' not in sql and 'UPDATE' not in sql,
                'Unexpected native bootstrap read')
            with self.lock:self.report['read_jobs'].append(native)
        job=self.client.query(sql,**kwargs)
        if mutation and self.lose:
            self.lose=False;job.result(timeout=120,retry=None,job_retry=None)
            raise RuntimeError('Synthetic discarded bootstrap response')
        return job


def record(report,name,**details):
    require(name not in {c['case'] for c in report['cases']},'Duplicate bootstrap case')
    report['cases'].append(dict(case=name,status='PASSED',**details))
    print(json.dumps(dict(case=name,status='PASSED')),flush=True)


@contextmanager
def fixture(client,report):
    scope=Scope(uuid4().hex);info=dict(dataset=scope.dataset,cleanup_verified=False);report['resources'].append(info)
    try:
        ds=bigquery.Dataset(scope.dataset);ds.location=LOCATION;ds.labels=scope.labels;ds.default_table_expiration_ms=TTL_MS
        created=client.create_dataset(ds,retry=None,timeout=20);scope.created=created.created;scope.own(client)
        for name in TABLES:client.create_table(scope.expected(name),retry=None,timeout=20)
        scope.inspect(client);yield scope
    finally:
        if scope.created is not None:
            scope.own(client);client.delete_dataset(scope.dataset,delete_contents=True,retry=None,timeout=20)
            try:client.get_dataset(scope.dataset,retry=None,timeout=20)
            except NotFound:info['cleanup_verified']=True
            else:raise ValueError('Bootstrap fixture deletion absence not verified')


@contextmanager
def environment(client,scope,report,fault=None,barrier=None):
    tracked=TrackedClient(client,scope,report,barrier);read_spec=reader.query_spec
    def query_spec():
        sql,cfg=read_spec();return scope.relocate(sql),cfg
    with ExitStack() as stack:
        stack.enter_context(patch.object(reader,'query_spec',side_effect=query_spec))
        for name in ('inspect_reviews','inspect_permissions'):
            stack.enter_context(patch.object(reader,name,side_effect=scope.inspect))
        stack.enter_context(patch.object(executor_module,'bootstrap_spec',side_effect=lambda manifest:scoped_plan(bootstrap_spec(manifest),scope,fault)))
        yield tracked


def service(client,*,subject='synthetic-bootstrap-owner',reason='SYNTHETIC_GENESIS',manifest=None,clock=now,enabled=True):
    if manifest is None:
        at=now();manifest=BootstrapManifest(actor(subject,at).principal.principal_id,AUDIENCE,at,at+timedelta(seconds=300),reason)
    identity=IapIdentityAdapter(IapSettings(AUDIENCE))
    identity.authenticate=lambda headers:actor(subject,clock())
    return executor_module.ProtectedCampaignBootstrap(client,identity,manifest,execution_enabled=enabled,clock=clock)


def expected_genesis(before,manifest):
    result={name:list(rows) for name,rows in before.items()};row=row_json(manifest.control.row())
    require(not any(result.values()),'Expected initially empty bootstrap fixture')
    result[CONTROL_HEAD]=[row];result[CONTROL_HISTORY]=[row];return result


def oracle(client,scope,report,plans):
    native=fixture_job_id(scope);job=client.get_job(native,location=LOCATION,retry=None,timeout=20)
    try:job.result(timeout=120,retry=None,job_retry=None,max_results=2)
    except Exception:pass
    job=client.get_job(native,location=LOCATION,retry=None,timeout=20)
    matches=[i for i,plan in enumerate(plans) if matches_sharing_job(job,job_id=native,sql=plan.sql,config=plan.config())]
    require(len(matches)==1 and job.state=='DONE','Native bootstrap config correlation differs')
    status=failed_sharing_job_status(client,job_id=native,parent=job,timeout_seconds=20) if job.error_result else Status.COMMITTED
    report['jobs'].append(dict(native_job_id=native,status=status.value,error=job.error_result,matched_plan=matches[0]))
    require(status in (Status.COMMITTED,Status.NOT_COMMITTED),'Native bootstrap outcome uncertain')
    return status,matches[0]


def unchanged(client,scope,tracked,report,name,operation,check):
    before=snapshot(client,scope);attempts=tracked.inserts;outcome=operation();check(outcome)
    require(tracked.inserts==attempts and snapshot(client,scope)==before,'Bootstrap recovery/denial mutated rows')
    record(report,name,insert_attempts=0,snapshot_unchanged=True);return outcome


def require_success(value):
    require(value.native_status is Status.COMMITTED and value.result_state=='APPLIED','Expected bootstrap APPLIED')


def require_unknown(value):require(value.native_status is Status.UNKNOWN,'Expected bootstrap UNKNOWN')


def denial(operation):
    try:operation()
    except ContractError as error:return error.code
    raise ValueError('Expected bootstrap denial')


def run_core(client,report):
    with fixture(client,report) as scope,environment(client,scope,report,barrier=Barrier(2)) as tracked:
        first=service(tracked);second=service(tracked,subject='synthetic-other-owner',reason='SYNTHETIC_CONFLICT')
        plans=[scoped_plan(bootstrap_spec(s.manifest),scope) for s in (first,second)]
        before=snapshot(client,scope)
        disabled=service(tracked,manifest=first.manifest,enabled=False)
        require(denial(lambda:disabled.submit({}))=='CAMPAIGN_BOOTSTRAP_DISABLED','Default bootstrap gate differs')
        record(report,'disabled',insert_attempts=0,snapshot_unchanged=True)
        captured=reader.read_capture(tracked,first.manifest,actor('synthetic-bootstrap-owner',now()),expected_audience=IapSettings(AUDIENCE))
        require(captured.snapshot.state_for(first.manifest)=='EMPTY' and snapshot(client,scope)==before,'Native empty capture differs')
        record(report,'empty_capture',insert_attempts=0,snapshot_unchanged=True)
        unchanged(client,scope,tracked,report,'absent_recovery',lambda:first.recover({}),require_unknown)
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(lambda s:s.submit({}),(first,second)))
        tracked.barrier=None;status,winner=oracle(client,scope,report,plans)
        services=(first,second);winning=services[winner];loser=1-winner
        require(status is Status.COMMITTED and tracked.inserts==2,'Competing callers did not share one native slot')
        if results[winner].native_status is Status.UNKNOWN:results[winner]=winning.recover({})
        require_success(results[winner]);require_unknown(results[loser])
        require(snapshot(client,scope)==expected_genesis(before,winning.manifest),'Competing bootstrap atomicity differs')
        record(report,'competing_manifests',insert_attempts=2,unique_native_jobs=1,winner=winner,
            native_job_id=fixture_job_id(scope),loser_status=results[loser].native_status.value,atomic_projection_verified=True)
        unchanged(client,scope,tracked,report,'original_recovery',lambda:winning.recover({}),require_success)
        unchanged(client,scope,tracked,report,'duplicate_submit',lambda:winning.submit({}),require_success)
        subject='synthetic-bootstrap-owner' if winner==0 else 'synthetic-other-owner'
        cold=service(tracked,subject=subject,manifest=winning.manifest)
        unchanged(client,scope,tracked,report,'cold_recovery',lambda:cold.recover({}),require_success)
        future=winning.manifest.deadline_at+timedelta(seconds=1)
        expired=service(tracked,subject=subject,manifest=winning.manifest,clock=lambda:future)
        unchanged(client,scope,tracked,report,'expired_window_recovery',lambda:expired.recover({}),require_success)


def run_faults(client,report):
    for fault,name in (('rollback','rollback'),('postcommit','postcommit'),(None,'lost_response')):
        with fixture(client,report) as scope,environment(client,scope,report,fault=fault) as tracked:
            instance=service(tracked);plan=scoped_plan(bootstrap_spec(instance.manifest),scope,fault)
            before=snapshot(client,scope);tracked.lose=name=='lost_response';outcome=instance.submit({})
            status,_=oracle(client,scope,report,[plan])
            if outcome.native_status is Status.UNKNOWN:outcome=instance.recover({})
            expected=Status.NOT_COMMITTED if fault=='rollback' else Status.COMMITTED
            require(status is expected and outcome.native_status is expected,'Bootstrap fault classification differs')
            require(tracked.inserts==1 and snapshot(client,scope)==(before if fault=='rollback' else expected_genesis(before,instance.manifest)),
                'Bootstrap fault projection/submission differs')
            if fault:require(outcome.result_state is None,'Fault invented bootstrap receipt')
            else:require_success(outcome)
            record(report,name,native_job_id=fixture_job_id(scope),native_status=status.value,
                result_state=outcome.result_state,insert_attempts=1,atomic_projection_verified=True)
            if fault=='rollback':
                unchanged(client,scope,tracked,report,'rollback_cannot_remint',lambda:instance.submit({}),
                    lambda value:require(value.native_status is Status.NOT_COMMITTED,'Rollback reminted a native slot'))


def run_denials(client,report):
    for name in GROUPS['DENIALS']:
        with fixture(client,report) as scope,environment(client,scope,report) as tracked:
            instance=service(tracked);row=instance.manifest.control.row()
            if name=='orphan_rows':
                rows,_=seed(now());load_rows(client,scope,'campaign_reviewer_grants_versions',rows['campaign_reviewer_grants_versions'][:1])
            elif name=='partial_control':load_rows(client,scope,CONTROL_HEAD,[row])
            elif name=='read_overflow':load_rows(client,scope,CONTROL_HEAD,[row]*3)
            else:
                load_rows(client,scope,CONTROL_HEAD,[row]);load_rows(client,scope,CONTROL_HISTORY,[row])
            expected={'orphan_rows':'CAMPAIGN_BOOTSTRAP_EXISTING_STATE_DENIED',
                'partial_control':'CAMPAIGN_BOOTSTRAP_EXISTING_STATE_DENIED',
                'read_overflow':'CAMPAIGN_BOOTSTRAP_READ_OVERFLOW',
                'genesis_without_original':'CAMPAIGN_BOOTSTRAP_ALREADY_PRESENT'}[name]
            unchanged(client,scope,tracked,report,name,lambda:denial(lambda:instance.submit({})),
                lambda code:require(code==expected,'Bootstrap denial assertion differs: '+code))


def validate_report(report):
    wanted=GROUPS[report['group']]
    require(len(report['cases'])==len(wanted) and {c['case'] for c in report['cases']}==set(wanted)
        and all(c['status']=='PASSED' for c in report['cases']) and report['resources']
        and all(r['cleanup_verified'] for r in report['resources']) and not report['errors']
        and report['serving_writes']==report['model_calls']==0,'Incomplete native bootstrap report')
    counts={'CORE':(1,2,1),'FAULTS':(3,3,3),'DENIALS':(4,0,0)}[report['group']]
    require((len(report['resources']),len(report['insert_attempts']),len(report['jobs']))==counts,'Bootstrap resource/submission count differs')
    require(len({j['native_job_id'] for j in report['jobs']})==len(report['jobs'])
        and set(report['insert_attempts'])=={j['native_job_id'] for j in report['jobs']}
        and all(j['status'] in ('COMMITTED','NOT_COMMITTED') for j in report['jobs']), 'Native bootstrap evidence incomplete')
    owned={'campaign_bootstrap_it_'+r['dataset'].rsplit('_',1)[-1]+'_0' for r in report['resources']}
    require(set(report['insert_attempts'])<=owned and report['read_jobs']
        and len(set(report['read_jobs']))==len(report['read_jobs'])
        and all(native.startswith('campaign_bootstrap_read_') for native in report['read_jobs']),
        'Native bootstrap namespace/read evidence differs')
    for c in report['cases']:
        if c.get('insert_attempts')==0:require(c['snapshot_unchanged'],'Read/denial preservation absent')
        else:require(c['atomic_projection_verified'],'Bootstrap atomic projection absent')
    if report['group']=='CORE':
        race=next(c for c in report['cases'] if c['case']=='competing_manifests')
        require(race['insert_attempts']==2 and race['unique_native_jobs']==1 and race['loser_status']=='UNKNOWN'
            and report['jobs'][0]['status']=='COMMITTED','Competing bootstrap evidence differs')
    if report['group']=='FAULTS':
        jobs=report['jobs'];codes=('FIXTURE_BOOTSTRAP_ROLLBACK','FIXTURE_BOOTSTRAP_POSTCOMMIT')
        require([j['status'] for j in jobs]==['NOT_COMMITTED','COMMITTED','COMMITTED']
            and all(jobs[i]['error'] and code in jobs[i]['error'].get('message','') for i,code in enumerate(codes)),
            'Bootstrap native fault evidence differs')
        by_case={c['case']:c for c in report['cases']}
        require(by_case['rollback']['result_state'] is None and by_case['postcommit']['result_state'] is None
            and by_case['lost_response']['result_state']=='APPLIED','Fault invented a bootstrap receipt')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',required=True,action='store_true')
    parser.add_argument('--group',choices=tuple(GROUPS),required=True);args=parser.parse_args()
    report=dict(status='FAILED',group=args.group,run_id=uuid4().hex,cases=[],resources=[],insert_attempts=[],read_jobs=[],jobs=[],
        errors=[],serving_writes=0,model_calls=0,synthetic_identity=True,started_at=now().isoformat())
    OUTPUT.mkdir(parents=True,exist_ok=True);path=OUTPUT/(report['run_id']+'.json')
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        try:({'CORE':run_core,'FAULTS':run_faults,'DENIALS':run_denials}[args.group])(client,report);validate_report(report);report['status']='PASSED'
        except Exception as error:report['errors'].append(type(error).__name__+': '+str(error))
        finally:report['completed_at']=now().isoformat();path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(status=report['status'],report=str(path),errors=report['errors'])),flush=True)
    if report['status']!='PASSED':raise SystemExit(1)


if __name__=='__main__':main()
