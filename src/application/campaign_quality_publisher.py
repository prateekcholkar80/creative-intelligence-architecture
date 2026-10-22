"""Owned IAP measurement publisher, disabled by default; no approval or release.

The evaluator supplies immutable documents; this adapter obtains current native
provider/control captures itself. It retains original intents, never remints a
slot after uncertain outcomes, and verifies current qualification after commit.
"""
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
import json
from threading import RLock
from uuid import uuid4
from google.cloud import bigquery
from src.application.campaign_quality_publication import (
    MeasurementPublisherManifest,build_publication,CONTROL,HISTORY,AUDIT,
)
from src.application.campaign_quality_qualification import BENCHMARKS,REPORTS,REVOCATIONS,SCHEMAS,table_metadata,MAX_CASES,QualificationRequirements,_document
from src.application.campaign_quality_qualification_reader import QualityQualificationReader
from src.application.campaign_review_storage import table_metadata as control_table,row_json
from src.application.campaign_publication_reader import _identity,_native_row
from src.application.campaign_permission_publication import Rows,validate_result
from src.application.control_io import ControlIO,ControlPlan,ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.identity import IapIdentityAdapter
from src.application.operation_budget import require_owned_worker,operation_call,OperationStopped
from src.application.campaign_policy_approval_receipt import PolicyApprovalPins
from src.application.workflow_contract import _require
from src.application.workflow_schema import TABLES as WORKFLOW
from src.application.prompt_runtime_contract import TABLES as BINDINGS
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table


@dataclass(frozen=True,slots=True)
class MeasurementIntent:
    actor_id: str
    pins: object
    requirements: object
    publication: object
    io_plan: ControlPlan


def capture_sql():
    fields=['CURRENT_TIMESTAMP() observed_at','@read_id read_id']
    ids="SELECT JSON_VALUE(c,'$.provider.invocation_id') FROM UNNEST(JSON_QUERY_ARRAY(@report_json,'$.cases')) c"
    invocation_scope=f'workspace_id=@workspace_id AND invocation_id IN ({ids})'
    request_scope=f'workspace_id=@workspace_id AND request_id IN (SELECT request_id FROM `{PROJECT}.ci_application.model_invocations` WHERE {invocation_scope})'
    for key,name,scope in (('invocations','model_invocations',invocation_scope),('bindings','invocation_prompt_bindings',invocation_scope),('requests','requests',request_scope)):
        fields += [f'ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=65536,TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_application.{name}` t WHERE {scope} LIMIT {MAX_CASES+1}) {key}',
            f'(SELECT COUNT(*) FROM `{PROJECT}.ci_application.{name}` WHERE {scope}) {key}_count']
    for key,name,scope,limit in (('control',CONTROL,"control_id='campaign-permissions'",2),('history',HISTORY,"control_id='campaign-permissions'",65)):
        fields += [f'ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.ci_brand_memory.{name}` t WHERE {scope} LIMIT {limit}) {key}',
            f'(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{name}` WHERE {scope}) {key}_count']
    fields += [f'ARRAY(SELECT canonical_json FROM `{PROJECT}.ci_brand_memory.{BENCHMARKS}` WHERE document_sha256=@benchmark_sha256 LIMIT 2) benchmarks',
        f'(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{BENCHMARKS}` WHERE document_sha256=@benchmark_sha256) benchmark_count',
        f'(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{REVOCATIONS}` WHERE (target_kind=\'REPORT\' AND target_sha256=@evaluation_sha256) OR (target_kind=\'BENCHMARK\' AND target_sha256=@benchmark_sha256)) revocation_count']
    return 'SELECT '+',\n'.join(fields)


class MeasurementPublisher:
    def __init__(self,*,client,identity_adapter,manifest,enabled=False,clock=lambda:datetime.now(timezone.utc)):
        _require(type(identity_adapter) is IapIdentityAdapter and type(manifest) is MeasurementPublisherManifest
            and client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None
            and type(enabled) is bool and manifest.capability.expected_audience==identity_adapter.settings.expected_audience,
            'MEASUREMENT_PUBLISHER_CONFIGURATION')
        self.client,self.verifier,self.manifest,self.enabled,self.clock=client,identity_adapter,manifest,enabled,clock
        self.io=ControlIO(client,result_timeout_seconds=90);self._lock=RLock();self._issued={};self._used=set()

    def _actor(self,headers):
        require_owned_worker();_require(self.enabled,'MEASUREMENT_PUBLICATION_DISABLED')
        actor=self.verifier.authenticate(headers);_identity(actor,self.verifier.settings,self.clock())
        self.manifest.capability.require_actor(actor,self.clock());return actor

    def _capture(self,headers,pins,benchmark,report):
        actor=self._actor(headers)
        expected=[table_metadata(name) for name in SCHEMAS]
        expected += [control_table(name,PROJECT) for name in (CONTROL,HISTORY,AUDIT)]
        expected += [WORKFLOW[name].table(PROJECT) for name in ('model_invocations','requests')]
        expected += [BINDINGS['invocation_prompt_bindings'].table(PROJECT)]
        for table in expected:validate_table(operation_call(self.client.get_table,table.reference,retry=None,timeout=10),table)
        started=self.clock();read_id=uuid4().hex
        params=[bigquery.ScalarQueryParameter(k,'STRING',getattr(pins,k)) for k in ('workspace_id','benchmark_sha256','evaluation_sha256')]
        params += [bigquery.ScalarQueryParameter('report_json','STRING',report),bigquery.ScalarQueryParameter('read_id','STRING',read_id)]
        row=self.io.read(capture_sql(),params);ended=self.clock()
        groups=('invocations','bindings','requests','control','history')
        _require(set(row)=={'observed_at','read_id','benchmarks','benchmark_count','revocation_count'}|set(groups)|{g+'_count' for g in groups}
            and row['read_id']==read_id and started<=row['observed_at']<=ended<started+timedelta(seconds=60),
            'MEASUREMENT_CAPTURE_WINDOW')
        for group in groups:
            _require(type(row[group]) is list and type(row[group+'_count']) is int and len(row[group])==row[group+'_count']
                and len(row[group])<=({'control':1,'history':64}.get(group,MAX_CASES))
                and all(type(v) is str and len(v.encode())<=65536 for v in row[group]),'MEASUREMENT_CAPTURE_COVERAGE')
        _require(len(row['control'])==1 and type(row['benchmark_count']) is int and row['benchmark_count']==len(row['benchmarks'])<=1
            and type(row['revocation_count']) is int and row['revocation_count']==0,'MEASUREMENT_CAPTURE_REVOKED_OR_CONFLICT')
        control=Rows('CONTROL','campaign-permissions',row_json(_native_row(row['control'][0])),
            tuple(row_json(v) for v in sorted((_native_row(v) for v in row['history']),key=lambda v:v['revision'])))
        current=self._actor(headers);_require(current.principal==actor.principal,'MEASUREMENT_PUBLISHER_ACTOR_CHANGED')
        return actor,control,dict(benchmark_json=benchmark,report_json=report,existing_benchmarks=tuple(row['benchmarks']),
            **{g:tuple(row[g]) for g in groups[:3]}),ended

    def prepare(self,headers,*,pins,requirements,benchmark_json,report_json):
        with self._lock:
            actor=self._actor(headers);_require(len(self._issued)<16,'MEASUREMENT_PUBLISHER_CAPACITY')
            _require(type(pins) is PolicyApprovalPins and type(requirements) is QualificationRequirements
                and pins.workspace_id==self.manifest.workspace_id and requirements.review_sha256==self.manifest.review_sha256
                and requirements.evaluator_sha256==self.manifest.evaluator_sha256,'MEASUREMENT_PUBLICATION_SCOPE')
            _document(benchmark_json,pins.benchmark_sha256)
            report=_document(report_json,pins.evaluation_sha256)
            _require(report.get('recorded_by')==actor.principal.principal_id,'MEASUREMENT_PUBLICATION_RECORDER')
            actor,control,capture,now=self._capture(headers,pins,benchmark_json,report_json)
            plan=build_publication(identity=actor,manifest=self.manifest,control=control,capture=capture,
                pins=pins,requirements=requirements,checked_at=now)
            _require(plan.native_job_id not in self._issued,'MEASUREMENT_ORIGINAL_INTENT_REQUIRED')
            native=ControlPlan(actor.principal.principal_id,pins.workspace_id,plan.command_sha256,plan.native_job_id,
                plan.sql,plan.config_json,'MEASUREMENT','{}',plan.audit_json,REPORTS)
            intent=MeasurementIntent(actor.principal.principal_id,pins,requirements,plan,native)
            self._issued[native.job_id]=intent;return intent

    def _authorized(self,headers,intent):
        _require(type(intent) is MeasurementIntent and self._issued.get(intent.io_plan.job_id) is intent,
            'MEASUREMENT_ORIGINAL_INTENT_REQUIRED')
        _require(self._actor(headers).principal.principal_id==intent.actor_id,'MEASUREMENT_PUBLISHER_ACTOR_CHANGED')

    def _result(self,headers,intent,result):
        self._authorized(headers,intent)
        if result.native_status is Status.COMMITTED:
            try:
                _require(result.job_id==intent.io_plan.job_id,'MEASUREMENT_RESULT_JOB_CHANGED')
                validate_result(intent.publication,result.rows)
                reader=QualityQualificationReader(client=self.client,identity_adapter=self.verifier,pins=intent.pins,
                    requirements=intent.requirements,reader_ids=(intent.actor_id,),recorder_ids=(intent.actor_id,),
                    publisher_manifest_sha256=self.manifest.sha256,clock=self.clock)
                reader.read(headers,intent.pins)
                self._authorized(headers,intent)
            except OperationStopped:raise
            except Exception:return ControlOutcome(result.job_id,Status.UNKNOWN,submitted=result.submitted)
        return result

    def publish(self,headers,intent):
        with self._lock:
            self._authorized(headers,intent)
            if intent.io_plan.job_id in self._used:return self.recover(headers,intent)
            self._used.add(intent.io_plan.job_id)
            return self._result(headers,intent,self.io.submit_once(intent.io_plan))

    def recover(self,headers,intent):
        with self._lock:
            self._authorized(headers,intent)
            return self._result(headers,intent,self.io.recover(intent.io_plan))
