"""Opt-in isolated native publication transactions; never serving/model writes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone
import json
from pathlib import Path
import re
from threading import Barrier,Lock
from unittest.mock import patch
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application import campaign_publication_reader as reader
from src.application.campaign_permission_publication import build_publication,validate_result,_expr
from src.application.campaign_permission_storage import TABLE_SCHEMAS,table_metadata as permission_table,encode_record,head_row,PREFIX
from src.application.campaign_review_storage import SCHEMAS,table_metadata as review_table,encode_review,review_head,row_json,parse_row
from src.application.campaign_permission_reader import Checkpoint
from src.application.campaign_publication_reader import PublicationReadRequest
from src.application.campaign_permission_lifecycle import Action
from src.application.campaign_acceptance_contract import PolicyStatus
from src.application.identity import IapSettings
from src.application.sharing_jobs import matches_sharing_job,failed_sharing_job_status
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION,DATASET,settings
from src.ingestion.setup_workflow_tables import validate_table
from src.validation.campaign_permission_integration import Scope as ReadScope,TTL_MS,load_rows,fixture_bytes
from src.validation.campaign_publication_fixtures import seed,actor,AUDIENCE
from src.validation.prompt_evaluation_remaining import transaction_interval,validate_race

TABLES=dict(TABLE_SCHEMAS)|dict(SCHEMAS)
GROUPS={'CORE':('atomic_create','same_job_recovery','exact_reuse','replace_draft','approve_claim','revoke_claim',
               'rollback_after_writes','postcommit_without_receipt','discarded_response_recovery','absent_original_unknown'),
        'DENIALS':('stale_head_denies','revoked_grant_denies','revoked_review_denies','changed_evidence_denies'),
        'RACE':('overlapping_competing_genesis',)}
OUTPUT=Path('data/generated/campaign_publication_diagnostics')
REPORT_LOCK=Lock()


def require(value,message):
    if not value:raise ValueError(message)


class Scope(ReadScope):
    def __init__(self,run_id):
        super().__init__(run_id);self.dataset_id='ci_campaign_publication_it_'+run_id
        self.dataset=PROJECT+'.'+self.dataset_id
        self.labels={'purpose':'campaign_publication_integration','run_id':run_id}

    def table(self,name):
        require(name in TABLES,'Unrelated publication fixture table');return self.dataset+'.'+name

    def expected(self,name):
        require(name in TABLES,'Unrelated fixture metadata')
        metadata=(permission_table if name in TABLE_SCHEMAS else review_table)(name,PROJECT).to_api_repr()
        metadata['tableReference']['datasetId']=self.dataset_id
        return bigquery.Table.from_api_repr(metadata)

    def assert_sql(self,sql):
        refs=re.findall(r'`([^`]+)`',sql);bare=re.sub(r'`[^`]+`',' ',sql)
        require(refs and set(refs)<={self.table(n) for n in TABLES}
            and f'{PROJECT}.{DATASET}' not in sql
            and not re.search(r'\b(?:FROM|JOIN|UPDATE|INTO|TABLE)\s+[\w-]+\.[\w.-]+',bare,re.I)
            and not re.search(r'\b(?:DELETE|DROP|TRUNCATE|MERGE)\b',bare,re.I),'Unsafe publication fixture SQL')

    def relocate(self,sql):
        for name in TABLES:sql=sql.replace(f'`{PROJECT}.{DATASET}.{name}`',f'`{self.table(name)}`')
        self.assert_sql(sql);return sql

    def inspect(self,client):
        self.own(client)
        for name in TABLES:
            actual=client.get_table(self.table(name),retry=None,timeout=20)
            require(actual.created is not None and actual.expires is not None and
                abs((actual.expires-actual.created).total_seconds()-TTL_MS/1000)<=120,'Fixture expiry differs')
            copied=bigquery.Table.from_api_repr(deepcopy(actual.to_api_repr()));copied.expires=None
            validate_table(copied,self.expected(name))
        return {'status':'VERIFIED'}


def snapshot(client,scope):
    scope.own(client)
    sql=' UNION ALL '.join(f"SELECT '{name}' table_name,TO_JSON_STRING(t) record_json FROM `{scope.table(name)}` t" for name in TABLES)
    scope.assert_sql(sql)
    cfg=bigquery.QueryJobConfig(use_query_cache=False,maximum_bytes_billed=512*1024*1024,job_timeout_ms=60000)
    job=client.query(sql,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
    result={name:[] for name in TABLES};count=0
    for row in job.result(timeout=60,max_results=1000):
        count+=1;require(count<1000,'Snapshot overflow')
        data=json.loads(row['record_json']);data['recorded_at']=datetime.fromisoformat(data['recorded_at'])
        result[row['table_name']].append(row_json(data))
    for rows in result.values():rows.sort()
    return result


def expected_mutation(before,plan):
    after=deepcopy(before);params={p.name:p.values for p in plan.config().query_parameters if isinstance(p,bigquery.ArrayQueryParameter)}
    audit=parse_row(plan.audit_json);prefix=PREFIX[audit['permission_kind']]
    for name,parameter,key,value in [('campaign_permission_controls','new_control','control_id','campaign-permissions'),
                                    (prefix+'_heads','new_head','record_id',audit['record_id'])]:
        after[name]=[row for row in after[name] if json.loads(row)[key]!=value]+list(params[parameter]);after[name].sort()
    for name,parameter in [('campaign_permission_control_history','new_control'),(prefix+'_versions','new_history'),
                           ('campaign_permission_publication_audits','expected_audit')]:
        after[name].extend(params[parameter]);after[name].sort()
    return after


def scoped_plan(plan,scope,case,fault=None):
    script=scope.relocate(plan.sql)
    if fault=='rollback':
        require(script.count('COMMIT TRANSACTION;')==1,'Commit marker missing')
        script=script.replace('COMMIT TRANSACTION;',"ASSERT FALSE AS 'FIXTURE_PUBLICATION_ROLLBACK';\nCOMMIT TRANSACTION;")
    elif fault=='postcommit':
        require(script.count('SELECT * FROM publication_result;')==1,'Result marker missing')
        script=script.replace('SELECT * FROM publication_result;',"ASSERT FALSE AS 'FIXTURE_PUBLICATION_POSTCOMMIT';\nSELECT * FROM publication_result;")
    require(fault in (None,'rollback','postcommit') and script.count(';')<64,'Invalid fixture fault/script limit')
    scope.assert_sql(script)
    return replace(plan,sql=script,native_job_id='publication_it_'+scope.run_id+'_'+case+'_'+plan.command_sha256)


def submit(client,scope,plan,report,*,discard_result=False):
    scope.own(client);scope.assert_sql(plan.sql)
    with REPORT_LOCK:report['mutation_submissions']+=1
    try:
        job=client.query(plan.sql,job_config=plan.config(),job_id=plan.native_job_id,location=LOCATION,
                         retry=None,job_retry=None,timeout=20)
        values=tuple(dict(row) for row in job.result(timeout=120,max_results=2))
        if discard_result:values=()
    except Exception:
        job=client.get_job(plan.native_job_id,location=LOCATION,retry=None,timeout=20)
        values=()
    require(matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()),'Native publication identity/config mismatch')
    original=client.get_job(plan.native_job_id,location=LOCATION,retry=None,timeout=20)
    require(matches_sharing_job(original,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()) and original.state=='DONE','Original publication job mismatch')
    if original.error_result:
        status=failed_sharing_job_status(client,job_id=original.job_id,parent=original,timeout_seconds=20)
        result=None
    else:
        status=SharingCommitStatus.COMMITTED
        if discard_result:values=tuple(dict(row) for row in original.result(timeout=60,max_results=2))
        result=validate_result(plan,values)
    details=dict(native_job_id=original.job_id,status=status.value,receipt_state=result,error=original.error_result,
        submission_result_discarded=discard_result)
    with REPORT_LOCK:report['jobs'].append(details)
    return original,status,result


def recover(client,plan):
    job=client.get_job(plan.native_job_id,location=LOCATION,retry=None,timeout=20)
    require(matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()) and job.error_result is None,'Original recovery mismatch')
    return validate_result(plan,tuple(dict(row) for row in job.result(timeout=60,max_results=2)))


def replace_head(client,scope,name,row):
    scope.own(client);schema=TABLES[name]
    query=f"UPDATE `{scope.table(name)}` t SET "+','.join(f'{n}=c.{n}' for n,_,_ in schema)+f" FROM (SELECT {_expr(schema,'@fixture_row')}) c WHERE t.record_id=@record_id; ASSERT @@row_count=1 AS 'FIXTURE_HEAD_COUNT';"
    scope.assert_sql(query)
    cfg=settings([bigquery.ScalarQueryParameter('fixture_row','STRING',row_json(row)),
                  bigquery.ScalarQueryParameter('record_id','STRING',row['record_id'])])
    client.query(query,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20).result(timeout=90)


def append_rows(client,scope,name,rows):
    scope.own(client)
    cfg=bigquery.LoadJobConfig(schema=scope.expected(name).schema,write_disposition='WRITE_APPEND')
    client.load_table_from_json([json.loads(row_json(r)) for r in rows],scope.table(name),job_config=cfg,
        job_id='publication_append_'+scope.run_id+'_'+uuid4().hex,num_retries=0,location=LOCATION,timeout=20).result(timeout=90)


def case(report,name,**details):
    require(name not in {c['case'] for c in report['cases']},'Duplicate case')
    report['cases'].append(dict(case=name,status='PASSED',**details));print(json.dumps(dict(case=name,status='PASSED')),flush=True)


def validate_report(report):
    wanted=set(GROUPS[report['group']])
    require(len(report['cases'])==len(wanted) and {c['case'] for c in report['cases']}==wanted
        and all(c['status']=='PASSED' for c in report['cases']) and report['cleanup_verified']
        and not report['errors'] and report['serving_writes']==0 and report['model_calls']==0,'Incomplete publication report')
    require(all(j['status'] in {'COMMITTED','NOT_COMMITTED'} for j in report['jobs']),'Unknown native publication outcome')
    count={'CORE':8,'DENIALS':4,'RACE':2}[report['group']]
    require(len(report['jobs'])==report['mutation_submissions']==count
        and len({j['native_job_id'] for j in report['jobs']})==count,'Native submission evidence incomplete')
    if report['group']=='CORE':
        require(sum(j['status']=='COMMITTED' for j in report['jobs'])==7
            and sum(j['receipt_state']=='REUSED' for j in report['jobs'])==1
            and sum(j['submission_result_discarded'] for j in report['jobs'])==1,'Core receipt outcomes incomplete')
        codes={'FIXTURE_PUBLICATION_ROLLBACK','FIXTURE_PUBLICATION_POSTCOMMIT'}
        failed=[j for j in report['jobs'] if j['error']]
        require(len(failed)==2 and all(any(code in j['error'].get('message','') for j in failed) for code in codes)
            and all(j['receipt_state'] is None for j in failed),'Fault commit evidence incomplete')
    if report['group']=='DENIALS':
        require(all(j['status']=='NOT_COMMITTED' and j['receipt_state'] is None and j['error'] for j in report['jobs']),
                'Denial rollback evidence incomplete')
    if report['group']=='RACE':
        race=report['cases'][0]
        require(len(race['jobs'])==2 and set(race['jobs'])=={j['native_job_id'] for j in report['jobs']},
                'Race job correlation incomplete')
        ordered=[next(j for j in report['jobs'] if j['native_job_id']==job_id) for job_id in race['jobs']]
        intervals=[tuple(datetime.fromisoformat(v) for v in interval) for interval in race['transaction_intervals']]
        require(validate_race(ordered,intervals,('PUBLICATION_CONTROL_EXACT','PUBLICATION_HEAD_EXACT'))==race['winner'],
                'Race report winner/overlap differs')


def run(client,scope,report):
    ds=bigquery.Dataset(scope.dataset);ds.location=LOCATION;ds.labels=scope.labels;ds.default_table_expiration_ms=TTL_MS
    created=client.create_dataset(ds,retry=None,timeout=20);scope.created=created.created;scope.own(client)
    for name in TABLES:client.create_table(scope.expected(name),retry=None,timeout=20)
    rows,contexts=seed(datetime.now(timezone.utc))
    for name,values in rows.items():
        if values:load_rows(client,scope,name,values)
    scope.inspect(client)
    original_spec=reader.query_spec
    def relocated(req):
        sql,cfg=original_spec(req);return scope.relocate(sql),cfg
    def prepare(context,candidate=None):
        captured=reader.read_capture(client,context['request'],actor(datetime.now(timezone.utc)),expected_audience=IapSettings(AUDIENCE))
        return captured.plan(candidate=context['candidate'] if candidate is None else candidate,reason_code='SYNTHETIC_NATIVE_REVIEW')
    def check(name,context,*,fault=None,denial=None,discard_result=False):
        plan=scoped_plan(prepare(context),scope,name,fault);before=snapshot(client,scope)
        job,status,receipt=submit(client,scope,plan,report,discard_result=discard_result);after=snapshot(client,scope)
        committed=denial is None and fault!='rollback'
        require(status is (SharingCommitStatus.COMMITTED if committed else SharingCommitStatus.NOT_COMMITTED),'Native commit classification differs')
        require(after==(expected_mutation(before,plan) if committed else before),'Atomic publication or unrelated preservation differs')
        if fault or denial:
            require(receipt is None and job.error_result is not None and (denial or ('FIXTURE_PUBLICATION_ROLLBACK' if fault=='rollback' else 'FIXTURE_PUBLICATION_POSTCOMMIT')) in job.error_result.get('message',''),'Native assertion differs')
        else:require(receipt=='APPLIED','Expected APPLIED receipt')
        case(report,name,native_job_id=job.job_id,commit_status=status.value,receipt_state=receipt)
        return plan
    with patch.object(reader,'query_spec',side_effect=relocated),patch.object(reader,'inspect_reviews',side_effect=scope.inspect),patch.object(reader,'inspect_permissions',side_effect=scope.inspect):
        if report['group']=='CORE':
            first=check('atomic_create',contexts['create'])
            submitted=report['mutation_submissions'];require(recover(client,first)=='APPLIED','Same-job recovery differs')
            require(report['mutation_submissions']==submitted,'Recovery resubmitted')
            case(report,'same_job_recovery',native_job_id=first.native_job_id,mutation_submissions=0)
            replay=replace(first,native_job_id=first.native_job_id+'_replay')
            before=snapshot(client,scope);job,status,receipt=submit(client,scope,replay,report)
            require(status is SharingCommitStatus.COMMITTED and receipt=='REUSED' and snapshot(client,scope)==before,'Exact replay differs')
            case(report,'exact_reuse',native_job_id=job.job_id)
            check('replace_draft',contexts['replace']);check('approve_claim',contexts['approve']);check('revoke_claim',contexts['revoke'])
            check('rollback_after_writes',contexts['rollback'],fault='rollback')
            check('postcommit_without_receipt',contexts['postcommit'],fault='postcommit')
            lost=check('discarded_response_recovery',contexts['lost'],discard_result=True)
            submitted=report['mutation_submissions'];require(recover(client,lost)=='APPLIED','Discarded-response recovery differs')
            require(submitted==report['mutation_submissions'],'Discarded-response resubmitted')
            try:client.get_job('publication_absent_'+scope.run_id,location=LOCATION,retry=None,timeout=20)
            except NotFound:case(report,'absent_original_unknown',commit_status='UNKNOWN',mutation_submissions=0)
            else:raise ValueError('Unexpected absent original')
        elif report['group']=='DENIALS':
            mapping=[('stale','stale_head_denies','PUBLICATION_HEAD_EXACT'),('grant_revoked','revoked_grant_denies','PUBLICATION_GRANT_HEAD_EXACT'),
                     ('review_revoked','revoked_review_denies','PUBLICATION_REVIEW_HEAD_EXACT'),('evidence_changed','changed_evidence_denies','PUBLICATION_EVIDENCE_HEAD_EXACT')]
            for key,name,denial in mapping:
                context=contexts[key];plan=scoped_plan(prepare(context),scope,name)
                if key=='stale':
                    row=encode_record(context['candidate'],recorded_at=datetime.now(timezone.utc))
                    append_rows(client,scope,PREFIX['CLAIM']+'_versions',[row]);append_rows(client,scope,PREFIX['CLAIM']+'_heads',[head_row(row)])
                elif key in {'grant_revoked','review_revoked'}:
                    original=context['grant' if key=='grant_revoked' else 'review']
                    modified=replace(original,revision=2,parent_sha256=original.sha256,status='REVOKED',recorded_at=datetime.now(timezone.utc))
                    row=encode_review(modified);prefix='campaign_reviewer_grants' if key=='grant_revoked' else 'campaign_review_captures'
                    # Distinct fixture load job ID for append after initial seeding.
                    append_rows(client,scope,prefix+'_versions',[row])
                    replace_head(client,scope,prefix+'_heads',review_head(row))
                else:
                    e=context['evidence'];changed=replace(e,content_sha256='b'*64,review=replace(e.review,revision=2,parent_sha256=e.sha256))
                    row=encode_record(changed,recorded_at=datetime.now(timezone.utc));prefix=PREFIX['EVIDENCE']
                    append_rows(client,scope,prefix+'_versions',[row])
                    replace_head(client,scope,prefix+'_heads',head_row(row))
                before=snapshot(client,scope);job,status,receipt=submit(client,scope,plan,report)
                require(status is SharingCommitStatus.NOT_COMMITTED and receipt is None and job.error_result
                    and denial in job.error_result.get('message','') and snapshot(client,scope)==before,'Stale/revoked denial differs')
                case(report,name,native_job_id=job.job_id,commit_status=status.value,reason_code=denial)
        else:
            context=contexts['race'];capture=reader.read_capture(client,context['request'],actor(datetime.now(timezone.utc)),expected_audience=IapSettings(AUDIENCE))
            plans=[scoped_plan(capture.plan(candidate=replace(context['candidate'],wording=(f'Synthetic competing draft {i}.',)),reason_code='SYNTHETIC_RACE'),scope,'race'+str(i)) for i in range(2)]
            before=snapshot(client,scope);barrier=Barrier(2)
            def contender(plan):barrier.wait(timeout=30);return submit(client,scope,plan,report)
            with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(contender,plans))
            descriptions=[];intervals=[]
            for job,status,receipt in results:
                children=list(client.list_jobs(parent_job=job.job_id,all_users=False,max_results=64,page_size=64,retry=None,timeout=20))
                intervals.append(transaction_interval(children,job.job_id))
                descriptions.append(dict(status=status.value,receipt_state=receipt,error=job.error_result))
            winner=validate_race(descriptions,intervals,('PUBLICATION_CONTROL_EXACT','PUBLICATION_HEAD_EXACT'))
            require(snapshot(client,scope)==expected_mutation(before,plans[winner]),'Race winner/loser preservation differs')
            case(report,'overlapping_competing_genesis',winner=winner,jobs=[p.native_job_id for p in plans],
                transaction_intervals=[[v.isoformat() for v in interval] for interval in intervals])


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true',required=True)
    parser.add_argument('--group',choices=tuple(GROUPS),required=True);args=parser.parse_args()
    scope=Scope(uuid4().hex);report=dict(status='FAILED',group=args.group,run_id=scope.run_id,dataset=scope.dataset,
        cases=[],jobs=[],mutation_submissions=0,errors=[],cleanup_verified=False,serving_writes=0,model_calls=0,
        started_at=datetime.now(timezone.utc).isoformat())
    OUTPUT.mkdir(parents=True,exist_ok=True);path=OUTPUT/(scope.run_id+'.json')
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        try:run(client,scope,report)
        except Exception as error:report['errors'].append(type(error).__name__+': '+str(error))
        finally:
            if scope.created is not None:
                try:
                    scope.own(client);client.delete_dataset(scope.dataset,delete_contents=True,retry=None,timeout=20)
                    try:client.get_dataset(scope.dataset,retry=None,timeout=20)
                    except NotFound:report['cleanup_verified']=True
                    else:raise ValueError('Fixture cleanup absence not verified')
                except Exception as error:report['errors'].append('cleanup: '+str(error))
            report['completed_at']=datetime.now(timezone.utc).isoformat()
            if not report['errors']:
                try:validate_report(report);report['status']='PASSED'
                except Exception as error:report['errors'].append(str(error))
            path.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(dict(status=report['status'],report=str(path),errors=report['errors'])),flush=True)
    if report['status']!='PASSED':raise SystemExit(1)


if __name__=='__main__':main()
