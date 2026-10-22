"""Isolated five-table operator-policy SQL verification, never serving approval.

Synthetic typed identities/manifests and a fixture control are explicitly not IAP
verification or authorized appointments. Only an owned expiring dataset is written.
"""
import argparse,json,hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime,timedelta,timezone
from pathlib import Path
from uuid import uuid4
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from src.application.identity import VerifiedIdentity,principal_for_subject,ISSUER
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest,build_operator_publication
from src.application.campaign_operator_policy_storage import (
    HEAD_TABLE,HISTORY_TABLE,OperatorPolicyVersion,table_metadata,decode_state,
)
from src.application.campaign_review_publication_contract import ReviewOperatorPolicy
from src.application.campaign_review_storage import PublicationControl,table_metadata as control_table,row_json,parse_row
from src.application.campaign_publication_reader import _native_row
from src.application.campaign_permission_publication import Rows,validate_result
from src.application.control_io import ControlIO,ControlPlan
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

NAMES=(HEAD_TABLE,HISTORY_TABLE,'campaign_permission_controls','campaign_permission_control_history','campaign_permission_publication_audits')
AUDIENCE='/projects/1042059373096/locations/asia-south1/services/ci-marketing'


def fixture_identity(now):
    subject='accounts.google.com:operator-policy-native-fixture'
    return VerifiedIdentity(principal_for_subject(ISSUER,subject),ISSUER,subject,AUDIENCE,
        int(now.timestamp())-1,int(now.timestamp())+599)


def load_fixture_control(client,destination,control):
    _require(destination.startswith(PROJECT+'.ci_operator_policy_it_') and destination.count('.')==2
        and destination.rsplit('.',1)[-1] in {'campaign_permission_controls','campaign_permission_control_history'},
        'OPERATOR_FIXTURE_CONTROL_SCOPE')
    cfg=bigquery.LoadJobConfig(schema=control_table(destination.rsplit('.',1)[-1],PROJECT).schema,
        write_disposition='WRITE_APPEND')
    job=client.load_table_from_json([json.loads(row_json(control))],destination,job_config=cfg,num_retries=0,timeout=20)
    job.result(timeout=45,retry=None)


def relocate(plan,dataset,suffix=''):
    _require(dataset.startswith('ci_operator_policy_it_') and dataset.replace('_','').isalnum(),'OPERATOR_FIXTURE_SCOPE')
    sql=plan.sql
    for name in NAMES:sql=sql.replace(f'`{PROJECT}.ci_brand_memory.{name}`',f'`{PROJECT}.{dataset}.{name}`')
    _require('`'+PROJECT+'.ci_brand_memory.' not in sql,'OPERATOR_FIXTURE_RELOCATION_INCOMPLETE')
    # Never reserve a production policy-revision job slot in a fixture run.
    native='ci_operator_fixture_'+hashlib.sha256(json.dumps([dataset,plan.native_job_id]).encode()).hexdigest()+suffix
    return ControlPlan('synthetic-native-fixture','operator-policy-fixture',plan.command_sha256,native,
        sql,plan.config_json,'OPERATOR_POLICY_FIXTURE','{}',plan.audit_json,HEAD_TABLE)


def settle_original(io,plan,outcome):
    observations=[outcome.native_status.value]
    for _ in range(2):
        if outcome.native_status is not Status.UNKNOWN:break
        outcome=io.recover(plan)
        observations.append(outcome.native_status.value)
    return outcome,observations


def run(output):
    _require(not output.exists(),'OPERATOR_FIXTURE_REPORT_EXISTS')
    dataset='ci_operator_policy_it_'+uuid4().hex
    report=dict(status='RUNNING',started_at=datetime.now(timezone.utc).isoformat(),dataset=dataset,cases=[],native_jobs=[],serving_writes=0,actual_approvals=0,model_calls=0,
        identity='SYNTHETIC_NOT_IAP',cleanup_verified=False)
    def save():output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2,default=str)+'\n')
    def track(plan,label):
        report['native_jobs'].append(dict(label=label,job_id=plan.job_id,sql_sha256=hashlib.sha256(plan.sql.encode()).hexdigest(),config_sha256=hashlib.sha256(plan.config_json.encode()).hexdigest()))
        save()
    save();created=False
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        io=ControlIO(client,result_timeout_seconds=90)
        try:
            ds=bigquery.Dataset(f'{PROJECT}.{dataset}');ds.location=LOCATION;ds.default_table_expiration_ms=3600000
            ds.labels={'purpose':'ci-operator-policy-fixture'}
            client.create_dataset(ds,retry=None,timeout=20);created=True
            for name in NAMES:
                source=table_metadata(name,PROJECT) if name in {HEAD_TABLE,HISTORY_TABLE} else control_table(name,PROJECT)
                target=bigquery.Table(f'{PROJECT}.{dataset}.{name}',schema=source.schema)
                client.create_table(target,retry=None,timeout=20)
            now=datetime.now(timezone.utc);actor=fixture_identity(now)
            control=PublicationControl(1,None,'a'*64,actor.principal.principal_id,now).row()
            for name in ('campaign_permission_controls','campaign_permission_control_history'):
                load_fixture_control(client,f'{PROJECT}.{dataset}.{name}',control)

            def snapshot():
                result={}
                for name in NAMES:
                    row=io.read(f'SELECT ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.{dataset}.{name}` t LIMIT 65) records, (SELECT COUNT(*) FROM `{PROJECT}.{dataset}.{name}`) n',[])
                    _require(row['n']==len(row['records']) and row['n']<65,'OPERATOR_FIXTURE_SNAPSHOT_OVERFLOW')
                    result[name]=tuple(sorted(row_json(_native_row(v)) for v in row['records']))
                return result

            def build(policy,status='ACTIVE'):
                records=snapshot();checked=datetime.now(timezone.utc)
                current_manifest=OperatorPublisherManifest('synthetic-native-fixture',AUDIENCE,(actor.principal.principal_id,),checked-timedelta(seconds=1),checked+timedelta(seconds=299))
                heads=records[HEAD_TABLE];history=tuple(sorted(records[HISTORY_TABLE],key=lambda s:parse_row(s)['revision']))
                previous=decode_state(parse_row(heads[0]),tuple(parse_row(s) for s in history),checked_at=checked).current if heads else None
                c=Rows('CONTROL','campaign-permissions',records['campaign_permission_controls'][0],
                    tuple(sorted(records['campaign_permission_control_history'],key=lambda s:parse_row(s)['revision'])))
                candidate=OperatorPolicyVersion(policy,previous.sha256 if previous else None,status,
                    actor.principal.principal_id,'SYNTHETIC_NATIVE_FIXTURE',checked)
                return build_operator_publication(identity=fixture_identity(checked),manifest=current_manifest,control=c,
                    head_json=heads[0] if heads else None,history_json=history,candidate=candidate,checked_at=checked)

            policy=ReviewOperatorPolicy('campaign-review-operators',1,AUDIENCE,(actor.principal.principal_id,),
                (actor.principal.principal_id,),(actor.principal.principal_id,),('CLAIM','EVIDENCE','OFFER'),
                300,60,now-timedelta(seconds=1),now+timedelta(hours=1))
            genesis=build(policy);native=relocate(genesis,dataset)
            track(native,'GENESIS')
            result,observations=settle_original(io,native,io.submit_once(native))
            report['genesis_observations']=observations;save()
            _require(result.native_status is Status.COMMITTED,'OPERATOR_FIXTURE_GENESIS')
            validate_result(genesis,result.rows);after=snapshot()
            report['cases'].append(dict(name='GENESIS',native_status=result.native_status.value,job_id=result.job_id));save()
            recovered=io.recover(native);validate_result(genesis,recovered.rows)
            _require(recovered.native_status is Status.COMMITTED and snapshot()==after,'OPERATOR_FIXTURE_RECOVERY')
            report['cases'].append(dict(name='ORIGINAL_RECOVERY',native_status=recovered.native_status.value));save()

            different=json.loads(native.config_json);different['jobTimeoutMs']='119000'
            conflict=io.submit_once(replace(native,config_json=json.dumps(different,sort_keys=True)))
            _require(conflict.native_status is Status.UNKNOWN and not conflict.submitted and snapshot()==after,
                'OPERATOR_FIXTURE_GLOBAL_SLOT_CONFLICT')
            report['cases'].append(dict(name='RESERVED_SLOT_DIFFERENT_CONFIG_DENIED',native_status=conflict.native_status.value));save()

            # Competing plans bind the same old policy/fence. Root concurrency is
            # recorded; transaction-interval overlap is not inferred from it.
            left=build(replace(policy,revision=2,max_review_seconds=59))
            right=build(replace(policy,revision=2,max_review_seconds=58))
            # Distinct owned fixture slots exercise SQL fencing independently
            # of the production revision-slot collision checked above.
            plans=[relocate(p,dataset,'_fence_'+str(index)) for index,p in enumerate((left,right))]
            for index,p in enumerate(plans):track(p,'COMPETING_FENCE_'+str(index))
            with ThreadPoolExecutor(max_workers=2) as pool:outcomes=list(pool.map(io.submit_once,plans))
            report['competition_initial_statuses']=[o.native_status.value for o in outcomes];save()
            settled=[settle_original(io,p,o) for p,o in zip(plans,outcomes)]
            outcomes=[value[0] for value in settled]
            report['competition_observations']=[value[1] for value in settled];save()
            _require(sorted(o.native_status.value for o in outcomes)==['COMMITTED','NOT_COMMITTED'],'OPERATOR_FIXTURE_COMPETITION')
            winner=next(i for i,o in enumerate(outcomes) if o.native_status is Status.COMMITTED)
            validate_result((left,right)[winner],outcomes[winner].rows)
            loser=client.get_job(outcomes[1-winner].job_id,location=LOCATION,retry=None,timeout=20)
            message=(loser.error_result or {}).get('message','')
            concurrent='concurrent update' in message.lower() and 'campaign_permission_controls' in message
            _require(concurrent,'OPERATOR_FIXTURE_TRANSACTION_CONTENTION_UNPROVEN')
            report['cases'].append(dict(name='COMPETING_FENCE',statuses=[o.native_status.value for o in outcomes],
                jobs=[o.job_id for o in outcomes],distinct_fixture_slots=True,
                transaction_contention='NATIVE_CONCURRENT_UPDATE_AT_SHARED_CONTROL',overlap_duration='NOT_MEASURED'));save()
            state=snapshot();current=decode_state(parse_row(state[HEAD_TABLE][0]),tuple(parse_row(s) for s in sorted(state[HISTORY_TABLE],key=lambda s:parse_row(s)['revision'])),checked_at=datetime.now(timezone.utc)).current
            _require(current.policy.revision==2 and len(state['campaign_permission_control_history'])==3,'OPERATOR_FIXTURE_WINNER_STATE')
            rollback=build(replace(current.policy,revision=3,max_review_seconds=57))
            forced=relocate(rollback,dataset)
            marker=f'INSERT `{PROJECT}.{dataset}.{HISTORY_TABLE}`'
            _require(marker in forced.sql,'OPERATOR_FIXTURE_FAULT_MARKER')
            forced=replace(forced,sql=forced.sql.replace(marker,"ASSERT FALSE AS 'SYNTHETIC_OPERATOR_ROLLBACK';\n"+marker,1),job_id=forced.job_id+'_rollback')
            track(forced,'POSITIVE_ROLLBACK')
            failed,observations=settle_original(io,forced,io.submit_once(forced))
            report['rollback_observations']=observations;save()
            _require(failed.native_status is Status.NOT_COMMITTED and snapshot()==state,'OPERATOR_FIXTURE_ROLLBACK')
            report['cases'].append(dict(name='POSITIVE_ROLLBACK',native_status=failed.native_status.value,job_id=failed.job_id));save()
            revoked=build(replace(current.policy,revision=3),status='REVOKED')
            revoked_native=relocate(revoked,dataset);track(revoked_native,'REVOCATION')
            committed,observations=settle_original(io,revoked_native,io.submit_once(revoked_native))
            report['revocation_observations']=observations;save()
            validate_result(revoked,committed.rows)
            _require(committed.native_status is Status.COMMITTED,'OPERATOR_FIXTURE_REVOCATION')
            final=snapshot();stored=decode_state(parse_row(final[HEAD_TABLE][0]),tuple(parse_row(s) for s in sorted(final[HISTORY_TABLE],key=lambda s:parse_row(s)['revision'])),checked_at=datetime.now(timezone.utc))
            try:stored.require_current(datetime.now(timezone.utc))
            except ValueError as error:_require(getattr(error,'code',None)=='OPERATOR_POLICY_NOT_CURRENT','OPERATOR_FIXTURE_REVOCATION_CODE')
            else:raise ValueError('OPERATOR_FIXTURE_REVOKED_RELEASED')
            report['cases'].append(dict(name='REVOCATION_AND_CURRENT_READ_DENIAL',native_status=committed.native_status.value,job_id=committed.job_id));save()
            report['status']='PASSED_ISOLATED_SQL_NOT_IAP_OR_APPROVAL'
        except Exception as error:
            report['status']='FAILED';report['safe_error_type']=type(error).__name__;report['safe_error_code']=getattr(error,'code',None);raise
        finally:
            if created:
                client.delete_dataset(f'{PROJECT}.{dataset}',delete_contents=True,not_found_ok=False,retry=None,timeout=20)
                try:client.get_dataset(f'{PROJECT}.{dataset}',retry=None,timeout=20)
                except NotFound:report['cleanup_verified']=True
            save()
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=run(args.output);print(json.dumps(result,indent=2))

if __name__=='__main__':main()
