"""Owned-process IAP operator-policy publisher, disabled by default.

No implicit authority, control bootstrap, reviewer seed, campaign-policy approval
or serving activation. Cold/expired-manifest recovery is deliberately withheld;
there is no alternate job, reconstructed intent or model call.
"""
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
from threading import RLock
from uuid import uuid4
from google.cloud import bigquery
from src.application.identity import IapIdentityAdapter
from src.application.operation_budget import require_owned_worker,operation_call
from src.application.control_io import ControlIO,ControlPlan,ControlOutcome
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest,build_operator_publication
from src.application.campaign_review_publication_contract import ReviewOperatorPolicy
from src.application.campaign_operator_policy_storage import (
    HEAD_TABLE,HISTORY_TABLE,POLICY_ID,OperatorPolicyVersion,decode_state,table_metadata,encode_record,
)
from src.application.campaign_publication_reader import _native_row
from src.application.campaign_permission_publication import Rows,validate_result,MAX_CAPTURE_BYTES
from src.application.campaign_review_storage import row_json,parse_row,decode_control_state,table_metadata as control_table
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table

SOURCES={'head':HEAD_TABLE,'history':HISTORY_TABLE,'control':'campaign_permission_controls',
    'controls':'campaign_permission_control_history'}


@dataclass(frozen=True,slots=True)
class OperatorPublicationCapture:
    control:Rows
    head_json:str|None
    history_json:tuple[str,...]
    read_id:str
    checked_at:datetime


@dataclass(frozen=True,slots=True)
class OperatorPublicationIntent:
    actor_id:str
    publication:object
    io_plan:ControlPlan


def capture_sql():
    fields=['CURRENT_TIMESTAMP() observed_at','@read_id read_id']
    for key,name in SOURCES.items():
        scope='policy_id=@policy_id' if key in {'head','history'} else "control_id='campaign-permissions'"
        fields.extend([f"ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<={MAX_CAPTURE_BYTES},TO_JSON_STRING(t),NULL) FROM `{PROJECT}.ci_brand_memory.{name}` t WHERE {scope} LIMIT 65) {key}",
            f"(SELECT COUNT(*) FROM `{PROJECT}.ci_brand_memory.{name}` WHERE {scope}) {key}_count"])
    return 'SELECT '+',\n'.join(fields)


class OperatorPolicyPublisher:
    def __init__(self,*,client,identity_adapter,manifest,enabled=False,clock=lambda:datetime.now(timezone.utc)):
        _require(isinstance(identity_adapter,IapIdentityAdapter) and type(manifest) is OperatorPublisherManifest
            and type(enabled) is bool and client.project==PROJECT and client.location==LOCATION
            and client.default_query_job_config is None,'OPERATOR_PUBLISHER_SETTINGS')
        _require(identity_adapter.settings.expected_audience==manifest.expected_audience,'OPERATOR_PUBLISHER_AUDIENCE')
        self.client,self.identity_adapter,self.manifest=client,identity_adapter,manifest
        self.enabled,self.clock=enabled,clock
        self.io=ControlIO(client,result_timeout_seconds=90)
        self._lock,self._issued,self._used=RLock(),{},set()
        self._slots={}

    def _actor(self,headers):
        require_owned_worker();_require(self.enabled,'OPERATOR_PUBLISHER_DISABLED')
        actor=self.identity_adapter.authenticate(headers)
        self.manifest.require_actor(actor,self.clock());return actor

    def _capture(self,actor):
        self.manifest.require_actor(actor,self.clock())
        for name in tuple(SOURCES.values())+('campaign_permission_publication_audits',):
            expected=table_metadata(name,PROJECT) if name in {HEAD_TABLE,HISTORY_TABLE} else control_table(name,PROJECT)
            validate_table(operation_call(self.client.get_table,expected.reference,retry=None,timeout=10),expected)
        started=self.clock();read_id='operator-publication-read-'+uuid4().hex
        row=self.io.read(capture_sql(),[bigquery.ScalarQueryParameter('policy_id','STRING',POLICY_ID),
            bigquery.ScalarQueryParameter('read_id','STRING',read_id)])
        now=self.clock();self.manifest.require_actor(actor,now)
        _require(set(row)=={'observed_at','read_id'}|set(SOURCES)|{k+'_count' for k in SOURCES}
            and row['read_id']==read_id and started<=row['observed_at']<=now<started+timedelta(seconds=60),
            'OPERATOR_PUBLICATION_CAPTURE_WINDOW')
        for key in SOURCES:
            _require(type(row[key]) is list and type(row[key+'_count']) is int
                and len(row[key])==row[key+'_count'] and 0<=len(row[key])<65
                and all(type(v) is str for v in row[key]),'OPERATOR_PUBLICATION_CAPTURE_COVERAGE')
            row[key]=[row_json(_native_row(v)) for v in row[key]]
        _require(len(row['head'])<=1 and len(row['control'])==1
            and sum(len(v.encode()) for key in SOURCES for v in row[key])<=MAX_CAPTURE_BYTES,
            'OPERATOR_PUBLICATION_CAPTURE_SIZE')
        head=row['head'][0] if row['head'] else None
        history=tuple(sorted(row['history'],key=lambda v:parse_row(v)['revision']))
        _require((head is None)==(not history),'OPERATOR_PUBLICATION_PARTIAL_STATE')
        if head:decode_state(parse_row(head),tuple(parse_row(v) for v in history),checked_at=now)
        controls=tuple(sorted(row['controls'],key=lambda v:parse_row(v)['revision']))
        decode_control_state(parse_row(row['control'][0]),tuple(parse_row(v) for v in controls),now)
        control=Rows('CONTROL','campaign-permissions',row['control'][0],controls)
        return OperatorPublicationCapture(control,head,history,read_id,now)

    def read_current(self,headers):
        actor=self._actor(headers);capture=self._capture(actor)
        _require(capture.head_json is not None,'OPERATOR_POLICY_NOT_PUBLISHED')
        state=decode_state(parse_row(capture.head_json),tuple(parse_row(v) for v in capture.history_json),checked_at=capture.checked_at)
        state.require_current(self.clock())
        # read_id is a query-bound marker; ControlIO verifies the native job.
        # Do not mislabel it as a native job ID in OperatorPolicySnapshot.
        return state

    def prepare(self,headers,*,policy,status='ACTIVE',reason_code):
        with self._lock:
            actor=self._actor(headers)
            _require(type(policy) is ReviewOperatorPolicy,'OPERATOR_PUBLICATION_POLICY_REQUIRED')
            key=(actor.principal.principal_id,policy.revision)
            signature=(policy.sha256,status,reason_code)
            if key in self._slots:
                old_signature,intent=self._slots[key]
                _require(old_signature==signature,'OPERATOR_POLICY_SLOT_CONFLICT')
                return intent
            _require(len(self._issued)<32,'OPERATOR_PUBLISHER_CAPACITY')
            capture=self._capture(actor)
            previous=decode_state(parse_row(capture.head_json),tuple(parse_row(v) for v in capture.history_json),
                checked_at=capture.checked_at).current if capture.head_json else None
            candidate=OperatorPolicyVersion(policy,previous.sha256 if previous else None,status,
                actor.principal.principal_id,reason_code,capture.checked_at)
            plan=build_operator_publication(identity=actor,manifest=self.manifest,control=capture.control,
                head_json=capture.head_json,history_json=capture.history_json,candidate=candidate,checked_at=capture.checked_at)
            io_plan=ControlPlan(actor.principal.principal_id,POLICY_ID,plan.command_sha256,plan.native_job_id,
                plan.sql,plan.config_json,'OPERATOR_POLICY',row_json(encode_record(candidate)),
                plan.audit_json,HEAD_TABLE)
            intent=OperatorPublicationIntent(actor.principal.principal_id,plan,io_plan)
            _require(plan.native_job_id not in self._issued,'OPERATOR_ORIGINAL_INTENT_REQUIRED')
            self._issued[plan.native_job_id]=intent;self._slots[key]=(signature,intent);return intent

    def _authorize(self,headers,intent):
        actor=self._actor(headers)
        _require(type(intent) is OperatorPublicationIntent
            and self._issued.get(intent.io_plan.job_id) is intent
            and actor.principal.principal_id==intent.actor_id,'OPERATOR_ORIGINAL_INTENT_REQUIRED')
        self._capture(actor)
        return actor

    def _result(self,intent,result):
        if result.job_id!=intent.io_plan.job_id:
            return ControlOutcome(intent.io_plan.job_id,Status.UNKNOWN,submitted=result.submitted)
        if result.native_status is Status.COMMITTED:
            try:validate_result(intent.publication,result.rows)
            except Exception:return ControlOutcome(result.job_id,Status.UNKNOWN,submitted=result.submitted)
        return result

    def publish(self,headers,intent):
        with self._lock:
            self._authorize(headers,intent)
            job=intent.io_plan.job_id
            if job in self._used:return self.recover(headers,intent)
            self._used.add(job) # before I/O: uncertainty never resubmits.
            result=self.io.submit_once(intent.io_plan)
            self._authorize(headers,intent)
            return self._result(intent,result)

    def recover(self,headers,intent):
        with self._lock:
            self._authorize(headers,intent)
            result=self.io.recover(intent.io_plan)
            self._authorize(headers,intent)
            return self._result(intent,result)
