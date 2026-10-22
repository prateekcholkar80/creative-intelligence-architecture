"""Owned expiring BigQuery fixtures for policy-review transactions, not approvals."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from uuid import uuid4
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application.campaign_policy_approval_publication import (
    AUTHORITY,RECEIPTS,REVOCATIONS,CONTROL,HISTORY,AUDIT,
    ApprovalPublicationSources,PolicyAuthorityPublisherManifest,build_approval_publication,
)
from src.application.campaign_policy_approval_receipt import (
    PolicyApprovalAuthority,PolicyApprovalReceipt,PolicyApprovalPins,decode_current,table_metadata,_text,
)
from src.application.campaign_policy_approval_reader import SQL as READ_SQL, decode_proven_snapshot
from src.application.campaign_review_storage import PublicationControl,table_metadata as control_table,row_json
from src.application.campaign_permission_publication import Rows,validate_result
from src.application.control_io import ControlIO,ControlPlan,query_config
from src.application.sharing_jobs import matches_sharing_job
from src.validation.prompt_evaluation_remaining import transaction_interval,validate_race
from src.application.identity import VerifiedIdentity,principal_for_subject,ISSUER,IapSettings
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

NAMES=(AUTHORITY,RECEIPTS,REVOCATIONS,CONTROL,HISTORY,AUDIT)
AUDIENCE='/projects/1042059373096/locations/asia-south1/services/ci-prompt-registrar'
PREFIX='ci_policy_approval_it_'


def relocate_sql(sql,dataset):
    _require(dataset.startswith(PREFIX) and dataset[len(PREFIX):].isalnum(),'POLICY_FIXTURE_SCOPE')
    for name in NAMES:sql=sql.replace(f'`{PROJECT}.ci_brand_memory.{name}`',f'`{PROJECT}.{dataset}.{name}`')
    _require(f'`{PROJECT}.ci_brand_memory.' not in sql,'POLICY_FIXTURE_RELOCATION')
    return sql


def relocate(plan,dataset,suffix=''):
    native='ci_policy_approval_fixture_'+hashlib.sha256((dataset+plan.native_job_id).encode()).hexdigest()+suffix
    return ControlPlan('synthetic-fixture','synthetic-workspace',plan.command_sha256,native,
        relocate_sql(plan.sql,dataset),plan.config_json,'POLICY_APPROVAL_FIXTURE','{}',plan.audit_json,RECEIPTS)


def identity(now):
    subject='accounts.google.com:policy-approval-native-fixture'
    return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,
        int(now.timestamp())-1,int(now.timestamp())+599)


def settle(io,plan,result):
    observed=[result.native_status.value]
    for _ in range(2):
        if result.native_status is not Status.UNKNOWN:break
        result=io.recover(plan);observed.append(result.native_status.value)
    return result,observed


def verify_race_metadata(client,plans,outcomes):
    _require(len(plans)==len(outcomes)==2,'POLICY_FIXTURE_RACE_INPUT')
    infos=[];intervals=[]
    for plan,outcome in zip(plans,outcomes):
        parent=client.get_job(plan.job_id,project=PROJECT,location=LOCATION,retry=None,timeout=20)
        _require(matches_sharing_job(parent,job_id=plan.job_id,sql=plan.sql,config=plan.config())
            and parent.state=='DONE','POLICY_FIXTURE_RACE_ROOT')
        children=list(client.list_jobs(project=PROJECT,parent_job=plan.job_id,all_users=False,
            max_results=64,page_size=64,retry=None,timeout=30))
        interval=transaction_interval(children,plan.job_id);intervals.append(interval)
        infos.append(dict(job_id=plan.job_id,status=outcome.native_status.value,
            receipt_state=outcome.rows[0].get('state') if outcome.rows else None,
            error=parent.error_result,transaction_interval=[v.isoformat() for v in interval]))
    winner=validate_race(infos,intervals,('POLICY_PUBLICATION_CONTROL','POLICY_PUBLICATION_HISTORY'))
    return dict(winner=winner,jobs=infos,overlap_measured=True,
        overlap_seconds=(min(v[1] for v in intervals)-max(v[0] for v in intervals)).total_seconds())


def run(output, *, receipt_revocation_only=False, proof_only=False):
    _require(not output.exists(),'POLICY_FIXTURE_REPORT_EXISTS')
    dataset=PREFIX+uuid4().hex
    _require(type(receipt_revocation_only) is bool and type(proof_only) is bool
        and not (receipt_revocation_only and proof_only),'POLICY_FIXTURE_MODE')
    report=dict(status='RUNNING',mode='PUBLICATION_PROOF' if proof_only else 'RECEIPT_REVOCATION' if receipt_revocation_only else 'CORE',dataset=dataset,identity='SYNTHETIC_NOT_DEPLOYED_IAP',cases=[],native_jobs=[],
        production_approval_writes=0,production_control_writes=0,model_calls=0,cleanup_verified=False)
    def save():output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    save();created=False
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        io=ControlIO(client,result_timeout_seconds=90)
        schemas={name:(table_metadata(name).schema if name in {AUTHORITY,RECEIPTS,REVOCATIONS} else control_table(name,PROJECT).schema) for name in NAMES}
        def normalize(name,value):
            data=json.loads(value)
            for field in schemas[name]:
                if field.field_type=='TIMESTAMP' and data.get(field.name) is not None:
                    data[field.name]=datetime.fromisoformat(data[field.name].replace('Z','+00:00'))
            return row_json(data)
        def snapshot():
            fields=[]
            for index,name in enumerate(NAMES):
                fields += [f'ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=65536,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.{dataset}.{name}` t LIMIT 65) rows_{index}',
                    f'(SELECT COUNT(*) FROM `{PROJECT}.{dataset}.{name}`) count_{index}']
            row=io.read('SELECT '+','.join(fields),[])
            _require(set(row)=={f'{kind}_{i}' for kind in ('rows','count') for i in range(len(NAMES))},'POLICY_FIXTURE_SNAPSHOT_SHAPE')
            result={};size=0
            for index,name in enumerate(NAMES):
                values=row[f'rows_{index}'];count=row[f'count_{index}']
                _require(type(values) is list and type(count) is int and count==len(values)<65
                    and all(type(v) is str and len(v.encode())<=65536 for v in values),'POLICY_FIXTURE_SNAPSHOT')
                size+=sum(len(v.encode()) for v in values);_require(size<=262144,'POLICY_FIXTURE_SNAPSHOT_SIZE')
                result[name]=tuple(sorted(normalize(name,value) for value in values))
            return result
        def sources(before,authority_id='fixture-authority',receipt_id=None):
            def selected(name,field,value):return tuple(v for v in before[name] if json.loads(v)[field]==value)
            revs=tuple(v for v in before[REVOCATIONS] if (json.loads(v)['target_kind'],json.loads(v)['target_id'])
                in {('AUTHORITY',authority_id),('RECEIPT',receipt_id)})
            return ApprovalPublicationSources(Rows('CONTROL','campaign-permissions',before[CONTROL][0],
                tuple(sorted(before[HISTORY],key=lambda s:json.loads(s)['revision']))),
                selected(AUTHORITY,'authority_id',authority_id),selected(RECEIPTS,'receipt_id',receipt_id),revs)
        def build(action,receipt_id=None):
            before=snapshot();now=datetime.now(timezone.utc);actor=identity(now)
            root=PolicyAuthorityPublisherManifest('synthetic-policy-publisher',AUDIENCE,(actor.principal.principal_id,),
                'fixture-workspace',now-timedelta(seconds=1),now+timedelta(seconds=299))
            candidate=authority if action=='CREATE_AUTHORITY' else (
                PolicyApprovalReceipt(receipt_id,pins,authority.authority_id,authority.sha256,
                    actor.principal.principal_id,now,now+timedelta(minutes=10)) if action=='CREATE_RECEIPT' else None)
            plan=build_approval_publication(action,identity=actor,expected_audience=IapSettings(AUDIENCE),
                sources=sources(before,receipt_id=receipt_id),authority_id=authority.authority_id,receipt_id=receipt_id,
                candidate=candidate,publisher_manifest=None if action=='CREATE_RECEIPT' else root,
                confirmed_pins_sha256=pins.sha256 if action=='CREATE_RECEIPT' else None,
                checked_at=now,reason_code='SYNTHETIC_NATIVE_FIXTURE')
            return plan,before
        def expected_after(before,plan,action):
            params={p.name:p for p in plan.config().query_parameters}
            destination=AUTHORITY if action=='CREATE_AUTHORITY' else RECEIPTS if action=='CREATE_RECEIPT' else REVOCATIONS
            expected=dict(before)
            expected[destination]=tuple(sorted(before[destination]+tuple(normalize(destination,v) for v in params['candidate_record'].values)))
            control=tuple(normalize(CONTROL,v) for v in params['candidate_control'].values)
            expected[CONTROL]=control;expected[HISTORY]=tuple(sorted(before[HISTORY]+control))
            expected[AUDIT]=tuple(sorted(before[AUDIT]+tuple(normalize(AUDIT,v) for v in params['candidate_audit'].values)))
            return expected
        def record(plan,label):
            report['native_jobs'].append(dict(label=label,job_id=plan.job_id,sql_sha256=hashlib.sha256(plan.sql.encode()).hexdigest(),
                config_sha256=hashlib.sha256(plan.config_json.encode()).hexdigest()));save()
        def execute(plan,label):
            record(plan,label);outcome,observations=settle(io,plan,io.submit_once(plan))
            report['cases'].append(dict(name=label,native_status=outcome.native_status.value,observations=observations,job_id=plan.job_id));save()
            return outcome
        def read_current():
            start=datetime.now(timezone.utc)
            sql=relocate_sql(READ_SQL,dataset)
            config=query_config([bigquery.ScalarQueryParameter('authority_id','STRING',authority.authority_id),
                bigquery.ScalarQueryParameter('receipt_id','STRING','receipt-main')])
            read_id='ci_policy_approval_fixture_read_'+uuid4().hex
            job=client.query(sql,job_config=config,job_id=read_id,project=PROJECT,location=LOCATION,retry=None,job_retry=None,timeout=20)
            values=list(job.result(max_results=2,page_size=2,retry=None,job_retry=None,timeout=45))
            original=client.get_job(read_id,project=PROJECT,location=LOCATION,retry=None,timeout=20)
            _require(len(values)==1 and matches_sharing_job(original,job_id=read_id,sql=sql,config=config)
                and original.state=='DONE' and original.error_result is None,'POLICY_FIXTURE_READ_CORRELATION')
            row=dict(values[0])
            for key in ('authorities','receipts'):row[key]=[_text(json.loads(v)) for v in row[key]]
            return decode_proven_snapshot(row,pins=pins,authority_id=authority.authority_id,authority_sha256=authority.sha256,
                receipt_id='receipt-main',identity=identity(start),native_job_id=read_id,started=start,ended=datetime.now(timezone.utc))
        try:
            ds=bigquery.Dataset(f'{PROJECT}.{dataset}');ds.location=LOCATION;ds.default_table_expiration_ms=3600000
            ds.labels={'purpose':'policy-approval-fixture'};client.create_dataset(ds,retry=None,timeout=20);created=True
            for name in NAMES:client.create_table(bigquery.Table(f'{PROJECT}.{dataset}.{name}',schema=schemas[name]),retry=None,timeout=20)
            now=datetime.now(timezone.utc);actor=identity(now)
            control=PublicationControl(1,None,'a'*64,actor.principal.principal_id,now).row()
            for name in (CONTROL,HISTORY):
                job=client.load_table_from_json([json.loads(row_json(control))],f'{PROJECT}.{dataset}.{name}',
                    job_config=bigquery.LoadJobConfig(schema=schemas[name],write_disposition='WRITE_APPEND'),num_retries=0,timeout=20)
                job.result(retry=None,timeout=45)
            authority=PolicyApprovalAuthority('fixture-authority',AUDIENCE,'fixture-workspace',(actor.principal.principal_id,),
                ('ACCEPTANCE','CLARIFICATION'),now-timedelta(minutes=1),now+timedelta(hours=1))
            pins=PolicyApprovalPins('fixture-workspace','CLARIFICATION','CLARIFICATION','fixture-policy','draft-v1',*[c*64 for c in 'bcdef'])
            unrelated=replace(authority,authority_id='unrelated-authority')
            row=dict(authority_id=unrelated.authority_id,authority_sha256=unrelated.sha256,canonical_json=_text(unrelated.payload()))
            job=client.load_table_from_json([row],f'{PROJECT}.{dataset}.{AUTHORITY}',
                job_config=bigquery.LoadJobConfig(schema=schemas[AUTHORITY],write_disposition='WRITE_APPEND'),num_retries=0,timeout=20)
            job.result(retry=None,timeout=45)
            plan,before=build('CREATE_AUTHORITY');native=relocate(plan,dataset)
            result=execute(native,'AUTHORITY_ATOMIC_COMMIT');_require(result.native_status is Status.COMMITTED,'POLICY_FIXTURE_AUTHORITY_COMMIT')
            validate_result(plan,result.rows);after=snapshot();_require(after==expected_after(before,plan,'CREATE_AUTHORITY'),'POLICY_FIXTURE_AUTHORITY_PRESERVATION')
            recovered=io.recover(native);validate_result(plan,recovered.rows)
            _require(recovered.native_status is Status.COMMITTED and snapshot()==after,'POLICY_FIXTURE_ORIGINAL_RECOVERY')
            report['cases'].append(dict(name='ORIGINAL_AUTHORITY_RECOVERY',native_status=recovered.native_status.value));save()
            changed=json.loads(native.config_json);changed['jobTimeoutMs']='119000'
            conflict=io.submit_once(replace(native,config_json=json.dumps(changed,sort_keys=True)))
            _require(conflict.native_status is Status.UNKNOWN and not conflict.submitted and snapshot()==after,'POLICY_FIXTURE_RESERVED_SLOT')
            report['cases'].append(dict(name='RESERVED_SLOT_CONFLICT_DENIED',native_status=conflict.native_status.value));save()
            plan,before=build('CREATE_RECEIPT','receipt-main');native=relocate(plan,dataset)
            result=execute(native,'RECEIPT_ATOMIC_COMMIT');_require(result.native_status is Status.COMMITTED,'POLICY_FIXTURE_RECEIPT_COMMIT')
            validate_result(plan,result.rows);_require(snapshot()==expected_after(before,plan,'CREATE_RECEIPT'),'POLICY_FIXTURE_RECEIPT_PRESERVATION')
            _require(not read_current().release_allowed,'POLICY_FIXTURE_RECEIPT_NOT_RELEASE')
            report['cases'].append(dict(name='CURRENT_RECEIPT_READ_NO_RELEASE',state='PASSED'));save()
            if proof_only:
                baseline=snapshot();current=read_current()
                _require(current.publication_sha256 is not None,'POLICY_FIXTURE_PUBLICATION_PROOF_MISSING')
                report['cases'].append(dict(name='CURRENT_EXACT_AUDIT_CONTROL_PROOF',state='PASSED'));save()
                def mutate(label,statement,parameters):
                    _require(f'`{PROJECT}.ci_brand_memory.' not in statement and dataset.startswith(PREFIX),'POLICY_FIXTURE_MUTATION_SCOPE')
                    native_id='ci_policy_proof_deformation_'+uuid4().hex
                    report['native_jobs'].append(dict(label=label,job_id=native_id,sql_sha256=hashlib.sha256(statement.encode()).hexdigest()));save()
                    job=client.query(statement,job_config=query_config(parameters),job_id=native_id,
                        project=PROJECT,location=LOCATION,retry=None,job_retry=None,timeout=20)
                    job.result(max_results=0,retry=None,job_retry=None,timeout=45)
                    _require(job.state=='DONE' and job.error_result is None and job.num_dml_affected_rows==1,'POLICY_FIXTURE_DEFORMATION')
                def denied(label,expected_code):
                    corrupted=snapshot()
                    try:read_current()
                    except ValueError as error:_require(expected_code in str(error),'POLICY_FIXTURE_PROOF_DENIAL_CODE')
                    else:raise ValueError('POLICY_FIXTURE_CORRUPT_PROOF_ACCEPTED')
                    _require(snapshot()==corrupted,'POLICY_FIXTURE_READ_MUTATED_SOURCE')
                    report['cases'].append(dict(name=label,state='PASSED'));save()
                def restore(name,text):
                    job=client.load_table_from_json([json.loads(text)],f'{PROJECT}.{dataset}.{name}',
                        job_config=bigquery.LoadJobConfig(schema=schemas[name],write_disposition='WRITE_APPEND'),num_retries=0,timeout=20)
                    job.result(retry=None,timeout=45)
                params=[bigquery.ScalarQueryParameter('receipt_id','STRING','receipt-main')]
                audit_row=next(v for v in baseline[AUDIT] if json.loads(v)['action']=='POLICY_APPROVAL_CREATE_RECEIPT')
                mutate('REMOVE_RECEIPT_PUBLICATION_AUDIT',f"DELETE FROM `{PROJECT}.{dataset}.{AUDIT}` WHERE record_id=@receipt_id AND action='POLICY_APPROVAL_CREATE_RECEIPT'",params)
                denied('ORPHAN_RECEIPT_WITHOUT_PUBLICATION_AUDIT_DENIED','PUBLICATION_MISSING_DUPLICATE')
                restore(AUDIT,audit_row);_require(snapshot()==baseline,'POLICY_FIXTURE_AUDIT_RESTORE')
                mutate('SPOOF_REVIEW_PUBLICATION_ACTOR',f"UPDATE `{PROJECT}.{dataset}.{AUDIT}` SET changed_by=@actor WHERE record_id=@receipt_id AND action='POLICY_APPROVAL_CREATE_RECEIPT'",
                    params+[bigquery.ScalarQueryParameter('actor','STRING','iap-v1:'+'f'*64)])
                denied('SPOOFED_AUDIT_REVIEWER_DENIED','REVIEW_PUBLICATION_CONFLICT')
                mutate('RESTORE_REVIEW_PUBLICATION_ACTOR',f"UPDATE `{PROJECT}.{dataset}.{AUDIT}` SET changed_by=@actor WHERE record_id=@receipt_id AND action='POLICY_APPROVAL_CREATE_RECEIPT'",
                    params+[bigquery.ScalarQueryParameter('actor','STRING',json.loads(audit_row)['changed_by'])])
                _require(snapshot()==baseline,'POLICY_FIXTURE_ACTOR_RESTORE')
                missing=next(v for v in baseline[HISTORY] if json.loads(v)['revision']==2)
                mutate('REMOVE_PUBLICATION_CONTROL_PREFIX',f"DELETE FROM `{PROJECT}.{dataset}.{HISTORY}` WHERE control_id='campaign-permissions' AND revision=2",[])
                denied('INCOMPLETE_SHARED_CONTROL_PREFIX_DENIED','PERMISSION_CONTROL_CHAIN_CONFLICT')
                restore(HISTORY,missing);_require(snapshot()==baseline,'POLICY_FIXTURE_HISTORY_RESTORE')
                _require(read_current().publication_sha256==current.publication_sha256,'POLICY_FIXTURE_PROOF_RESTORE')
            elif receipt_revocation_only:
                original_plan,original_native=plan,native
                revoke,before=build('REVOKE_RECEIPT','receipt-main');native=relocate(revoke,dataset)
                result=execute(native,'RECEIPT_REVOCATION_COMMIT')
                _require(result.native_status is Status.COMMITTED,'POLICY_FIXTURE_RECEIPT_REVOKE')
                validate_result(revoke,result.rows);after=snapshot()
                _require(after==expected_after(before,revoke,'REVOKE_RECEIPT'),'POLICY_FIXTURE_RECEIPT_REVOKE_PRESERVATION')
                try:read_current()
                except ValueError as error:_require('REVOKED' in str(error),'POLICY_FIXTURE_RECEIPT_REVOKED_CODE')
                else:raise ValueError('POLICY_FIXTURE_RECEIPT_REVOKED_READ_ACCEPTED')
                report['cases'].append(dict(name='CURRENT_RECEIPT_REVOCATION_DENIED',state='PASSED'));save()
                recovered=io.recover(original_native);validate_result(original_plan,recovered.rows)
                _require(recovered.native_status is Status.COMMITTED and snapshot()==after,'POLICY_FIXTURE_HISTORICAL_NOT_ELIGIBILITY')
                report['cases'].append(dict(name='HISTORICAL_COMMIT_RECOVERY_AFTER_RECEIPT_REVOCATION',native_status=recovered.native_status.value,release_allowed=False));save()
            else:
                plan,before=build('CREATE_RECEIPT','receipt-rollback');native=relocate(plan,dataset)
                marker="ASSERT @@row_count=1 AS 'POLICY_PUBLICATION_CAS';"
                _require(native.sql.count(marker)==1,'POLICY_FIXTURE_ROLLBACK_INJECTION')
                native=replace(native,sql=native.sql.replace(marker,marker+"\nASSERT FALSE AS 'POLICY_FIXTURE_FORCED_ROLLBACK';"))
                result=execute(native,'ROLLBACK_AFTER_FENCE_MUTATION');_require(result.native_status is Status.NOT_COMMITTED and snapshot()==before,'POLICY_FIXTURE_ROLLBACK')
                left,before=build('CREATE_RECEIPT','receipt-race-left');right,right_before=build('CREATE_RECEIPT','receipt-race-right')
                _require(before==right_before,'POLICY_FIXTURE_RACE_SOURCE')
                plans=[relocate(p,dataset) for p in (left,right)]
                for p in plans:record(p,'RACING_PUBLICATION')
                with ThreadPoolExecutor(max_workers=2) as pool:raw=list(pool.map(io.submit_once,plans))
                settled=[settle(io,p,r) for p,r in zip(plans,raw)];outcomes=[r[0] for r in settled]
                report['race_observations']=[r[1] for r in settled];save()
                _require(sorted(o.native_status.value for o in outcomes)==sorted([Status.COMMITTED.value,Status.NOT_COMMITTED.value]),'POLICY_FIXTURE_RACE_OUTCOMES')
                winner=next(i for i,o in enumerate(outcomes) if o.native_status is Status.COMMITTED)
                validate_result((left,right)[winner],outcomes[winner].rows)
                _require(snapshot()==expected_after(before,(left,right)[winner],'CREATE_RECEIPT'),'POLICY_FIXTURE_RACE_PRESERVATION')
                metadata=verify_race_metadata(client,plans,outcomes)
                _require(metadata['winner']==winner,'POLICY_FIXTURE_RACE_WINNER')
                report['race_metadata']=metadata
                report['cases'].append(dict(name='SHARED_FENCE_COMPETITION',winner=winner,states=[o.native_status.value for o in outcomes],overlap_measured=True));save()
                plan,before=build('CREATE_RECEIPT','receipt-lost');native=relocate(plan,dataset);record(native,'DISCARDED_RESPONSE')
                io.submit_once(native)  # Deliberately discard the response; never resubmit.
                result,observed=settle(io,native,io.recover(native))
                _require(result.native_status is Status.COMMITTED,'POLICY_FIXTURE_LOST_RECOVERY');validate_result(plan,result.rows)
                _require(snapshot()==expected_after(before,plan,'CREATE_RECEIPT'),'POLICY_FIXTURE_LOST_PRESERVATION')
                report['cases'].append(dict(name='DISCARDED_RESPONSE_RECOVERY',native_status=result.native_status.value,observations=observed));save()
                stale,_=build('CREATE_RECEIPT','receipt-after-revocation')
                revoke,before=build('REVOKE_AUTHORITY');native=relocate(revoke,dataset)
                result=execute(native,'AUTHORITY_REVOCATION_COMMIT');_require(result.native_status is Status.COMMITTED,'POLICY_FIXTURE_REVOKE')
                validate_result(revoke,result.rows);after=snapshot();_require(after==expected_after(before,revoke,'REVOKE_AUTHORITY'),'POLICY_FIXTURE_REVOKE_PRESERVATION')
                stale_native=relocate(stale,dataset);result=execute(stale_native,'PREPARED_BEFORE_REVOCATION_DENIED')
                _require(result.native_status is Status.NOT_COMMITTED and snapshot()==after,'POLICY_FIXTURE_STALE_DENIED')
                try:read_current()
                except ValueError as error:_require('REVOKED' in str(error),'POLICY_FIXTURE_REVOKED_CODE')
                else:raise ValueError('POLICY_FIXTURE_REVOKED_READ_ACCEPTED')
                report['cases'].append(dict(name='CURRENT_REVOKED_READ_DENIED',state='PASSED'));save()
            report['status']='PASSED'
        except BaseException as error:
            report['status']='FAILED';report['error_type']=type(error).__name__;report['error']=str(error)[:2000];save();raise
        finally:
            if created:
                client.delete_dataset(f'{PROJECT}.{dataset}',delete_contents=True,not_found_ok=False,retry=None,timeout=30)
                try:client.get_dataset(f'{PROJECT}.{dataset}',retry=None,timeout=20)
                except NotFound:report['cleanup_verified']=True
                else:report['cleanup_verified']=False
            report['finished_at']=datetime.now(timezone.utc).isoformat();save()
    _require(report['cleanup_verified'],'POLICY_FIXTURE_CLEANUP')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run',action='store_true');parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--receipt-revocation-only',action='store_true');parser.add_argument('--proof-only',action='store_true')
    args=parser.parse_args();_require(args.run,'POLICY_FIXTURE_EXPLICIT_RUN')
    result=run(args.output,receipt_revocation_only=args.receipt_revocation_only,proof_only=args.proof_only);print(json.dumps(dict(status=result['status'],cases=len(result['cases']),cleanup_verified=result['cleanup_verified'])))


if __name__=='__main__':main()
