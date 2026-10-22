"""Server-only publication adapter; disabled by default, no bootstrap or activation.

Prepared intents are internal captures, never HTTP-deserialized plans. Recovery
reads the original native job only; no historical business receipt is inferred.
"""
from dataclasses import dataclass,replace
from datetime import datetime,timezone
from itertools import islice
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application.identity import IapIdentityAdapter
from src.application.campaign_publication_reader import read_capture,PublicationCapture,_identity
from src.application.campaign_permission_lifecycle import Action
from src.application.campaign_permission_publication import validate_result
from src.application.sharing_jobs import matches_sharing_job,failed_sharing_job_status
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_writer_executor import WorkflowWriteOutcome
from src.application.workflow_contract import _require,ContractError
from src.application.operation_budget import OperationStopped,operation_call,operation_iter
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION


@dataclass(frozen=True,slots=True)
class PublicationIntent:
    capture: PublicationCapture
    candidate: object
    reason_code: str

    def plan(self):
        _require(isinstance(self.capture,PublicationCapture),'INVALID_PUBLICATION_INTENT')
        return self.capture.plan(candidate=self.candidate,reason_code=self.reason_code)


class PublicationExecutor:
    def __init__(self,client,identity_adapter,*,execution_enabled=False,
                 clock=lambda:datetime.now(timezone.utc)):
        _require(isinstance(identity_adapter,IapIdentityAdapter)
            and type(execution_enabled) is bool,'INVALID_PUBLICATION_EXECUTOR_SETTINGS')
        _require(client.project==PROJECT and client.location==LOCATION
            and client.default_query_job_config is None,'INVALID_PUBLICATION_EXECUTOR_CLIENT')
        self.client=client;self.identity_adapter=identity_adapter
        self.execution_enabled=execution_enabled;self.clock=clock

    def _actor(self,headers):
        _require(self.execution_enabled,'PUBLICATION_EXECUTION_DISABLED')
        actor=self.identity_adapter.authenticate(headers)
        _identity(actor,self.identity_adapter.settings,self.clock())
        return actor

    def prepare(self,headers,request,*,candidate=None,reason_code):
        actor=self._actor(headers)
        capture=read_capture(self.client,request,actor,
            expected_audience=self.identity_adapter.settings,clock=self.clock)
        intent=PublicationIntent(capture,candidate,reason_code);intent.plan()
        return intent

    def _authority(self,headers,intent):
        _require(isinstance(intent,PublicationIntent),'INVALID_PUBLICATION_INTENT')
        actor=self._actor(headers)
        _require(actor.principal==intent.capture.identity.principal,'PUBLICATION_INTENT_ACTOR_MISMATCH')
        request=intent.capture.request
        # Current authorization must work both before creation and after commit.
        if request.action is Action.CREATE_DRAFT:
            request=replace(request,action=Action.REPLACE_DRAFT)
        try:
            capture=read_capture(self.client,request,actor,
                expected_audience=self.identity_adapter.settings,clock=self.clock)
        except ContractError as error:
            if (intent.capture.request.action is not Action.CREATE_DRAFT
                    or error.code!='PUBLICATION_READ_MISSING'):raise
            capture=read_capture(self.client,intent.capture.request,actor,
                expected_audience=self.identity_adapter.settings,clock=self.clock)
        return capture

    def _outcome(self,plan,job,submitted=False):
        unknown=WorkflowWriteOutcome(plan.native_job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)
        if not matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()):return unknown
        try:
            if job.state!='DONE':
                operation_call(job.result,retry=None,job_retry=None,timeout=30,max_results=2,page_size=2)
            if job.state!='DONE' or not matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()):return unknown
            if job.error_result is not None:
                status=failed_sharing_job_status(self.client,job_id=plan.native_job_id,timeout_seconds=10,parent=job)
                return WorkflowWriteOutcome(plan.native_job_id,status,submitted=submitted)
            rows=tuple(dict(row) for row in islice(operation_iter(operation_call(job.result,
                retry=None,job_retry=None,timeout=30,max_results=2,page_size=2)),2))
            if not matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()):return unknown
            state=validate_result(plan,rows)
            return WorkflowWriteOutcome(plan.native_job_id,SharingCommitStatus.COMMITTED,state,submitted)
        except OperationStopped:raise
        except Exception:return unknown

    def _original(self,plan,submitted=False):
        try:
            job=operation_call(self.client.get_job,plan.native_job_id,project=PROJECT,
                location=LOCATION,retry=None,timeout=10)
            return self._outcome(plan,job,submitted)
        except OperationStopped:raise
        except Exception:return WorkflowWriteOutcome(plan.native_job_id,SharingCommitStatus.UNKNOWN,submitted=submitted)

    def recover(self,headers,intent):
        self._authority(headers,intent);plan=intent.plan()
        outcome=self._original(plan)
        self._authority(headers,intent)
        return outcome

    def recover_receipt(self,headers,intent):
        """Historical business proof only; never dispatches a mutation."""
        from src.application.campaign_publication_receipt import read_receipt
        from src.application.campaign_permission_reader import Checkpoint
        # Historic review/evidence may be expired or revoked. Current explicit
        # reviewer authority is still mandatory; historical eligibility is not.
        _require(isinstance(intent,PublicationIntent),'INVALID_PUBLICATION_INTENT')
        actor=self._actor(headers)
        _require(actor.principal==intent.capture.identity.principal,'PUBLICATION_INTENT_ACTOR_MISMATCH')
        request=replace(intent.capture.request,action=Action.REPLACE_DRAFT,review=None,
            evidence=(),permission=Checkpoint(intent.capture.request.record_id))
        def authorize():
            current=self._actor(headers)
            _require(current.principal==actor.principal,'PUBLICATION_INTENT_ACTOR_MISMATCH')
            try:
                return read_capture(self.client,request,current,
                    expected_audience=self.identity_adapter.settings,clock=self.clock)
            except ContractError as error:
                if (intent.capture.request.action is not Action.CREATE_DRAFT
                        or error.code!='PUBLICATION_READ_MISSING'):raise
                return read_capture(self.client,replace(request,action=Action.CREATE_DRAFT),current,
                    expected_audience=self.identity_adapter.settings,clock=self.clock)
        authorize()
        receipt=read_receipt(self.client,intent,clock=self.clock)
        authorize()
        return receipt

    def submit(self,headers,intent):
        self._authority(headers,intent);plan=intent.plan()
        try:
            job=operation_call(self.client.get_job,plan.native_job_id,project=PROJECT,
                location=LOCATION,retry=None,timeout=10)
            outcome=self._outcome(plan,job)
        except NotFound:
            # Revalidate exact original captures and identity lifetime at dispatch.
            fresh=self._authority(headers,intent)
            _require((fresh.control,fresh.grant,fresh.permission,fresh.review,fresh.evidence)==
                (intent.capture.control,intent.capture.grant,intent.capture.permission,
                 intent.capture.review,intent.capture.evidence),'PUBLICATION_INTENT_STALE')
            _identity(intent.capture.identity,self.identity_adapter.settings,self.clock())
            from src.application.campaign_permission_publication import PLAN_TTL
            _require(intent.capture.checked_at<=self.clock()<intent.capture.checked_at+PLAN_TTL,
                'PUBLICATION_INTENT_EXPIRED')
            try:
                job=operation_call(self.client.query,plan.sql,job_config=plan.config(),
                    job_id=plan.native_job_id,project=PROJECT,location=LOCATION,retry=None,
                    job_retry=None,api_method=bigquery.enums.QueryApiMethod.INSERT,timeout=10)
                outcome=self._outcome(plan,job,True)
            except OperationStopped:raise
            except Exception:outcome=self._original(plan,True)
        except OperationStopped:raise
        except Exception:outcome=WorkflowWriteOutcome(plan.native_job_id,SharingCommitStatus.UNKNOWN)
        self._authority(headers,intent)
        return outcome
