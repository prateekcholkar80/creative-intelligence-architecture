"""Disabled trusted one-slot bootstrap. No alternate jobs, grants or activation.

The manifest is explicit server configuration, never an HTTP-deserialized plan.
Historical native recovery ignores the old dispatch window but requires a fresh
owner identity. No native result is treated as current reviewer authority.
"""
from datetime import datetime,timezone
from itertools import islice
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from src.application.identity import IapIdentityAdapter,IapSettings
from src.application.campaign_bootstrap_contract import BootstrapManifest
from src.application.campaign_bootstrap_query import bootstrap_spec,validate_bootstrap_result
from src.application.campaign_bootstrap_reader import authorize_owner,read_capture
from src.application.sharing_jobs import matches_sharing_job,failed_sharing_job_status
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_writer_executor import WorkflowWriteOutcome
from src.application.workflow_contract import _require
from src.application.operation_budget import OperationStopped,operation_call,operation_iter
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION


class ProtectedCampaignBootstrap:
    def __init__(self,client,identity_adapter,manifest,*,execution_enabled=False,
                 clock=lambda:datetime.now(timezone.utc)):
        _require(isinstance(identity_adapter,IapIdentityAdapter) and isinstance(manifest,BootstrapManifest)
            and type(execution_enabled) is bool,'INVALID_CAMPAIGN_BOOTSTRAP_SETTINGS')
        _require(client.project==PROJECT and client.location==LOCATION
            and client.default_query_job_config is None,'INVALID_CAMPAIGN_BOOTSTRAP_CLIENT')
        _require(identity_adapter.settings==IapSettings(manifest.expected_audience),'CAMPAIGN_BOOTSTRAP_AUDIENCE_MISMATCH')
        self.client=client;self.identity_adapter=identity_adapter;self.manifest=manifest
        self.execution_enabled=execution_enabled;self.clock=clock

    def _actor(self,headers):
        _require(self.execution_enabled,'CAMPAIGN_BOOTSTRAP_DISABLED')
        actor=self.identity_adapter.authenticate(headers)
        authorize_owner(self.manifest,actor,self.identity_adapter.settings,self.clock())
        return actor

    def _capture(self,actor):
        return read_capture(self.client,self.manifest,actor,
            expected_audience=self.identity_adapter.settings,clock=self.clock)

    def _get(self,plan):
        return operation_call(self.client.get_job,plan.native_job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)

    def _classify(self,plan,job,submitted=False):
        unknown=WorkflowWriteOutcome(plan.native_job_id,Status.UNKNOWN,submitted=submitted)
        if not matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()):return unknown
        try:
            if job.state!='DONE':
                operation_call(job.result,retry=None,job_retry=None,timeout=30,max_results=2,page_size=2)
            if job.state!='DONE' or not matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()):return unknown
            if job.error_result is not None:
                status=failed_sharing_job_status(self.client,job_id=plan.native_job_id,parent=job,timeout_seconds=10)
                return WorkflowWriteOutcome(plan.native_job_id,status,submitted=submitted)
            rows=tuple(dict(row) for row in islice(operation_iter(operation_call(job.result,
                retry=None,job_retry=None,timeout=30,max_results=2,page_size=2)),2))
            if not matches_sharing_job(job,job_id=plan.native_job_id,sql=plan.sql,config=plan.config()):return unknown
            state=validate_bootstrap_result(plan,rows)
            return WorkflowWriteOutcome(plan.native_job_id,Status.COMMITTED,state,submitted)
        except OperationStopped:raise
        except Exception:return unknown

    def _original(self,plan,submitted=False):
        try:return self._classify(plan,self._get(plan),submitted)
        except OperationStopped:raise
        except Exception:return WorkflowWriteOutcome(plan.native_job_id,Status.UNKNOWN,submitted=submitted)

    def _release(self,headers,outcome):
        actor=self._actor(headers)
        if outcome.result_state is not None:
            # A successful native result alone is historical evidence. A complete
            # fresh exact genesis is required to expose the successful receipt.
            capture=self._capture(actor)
            _require(capture.snapshot.state_for(self.manifest)=='EXACT_GENESIS',
                'CAMPAIGN_BOOTSTRAP_GENESIS_NOT_OBSERVED')
        self._actor(headers)
        return outcome

    def recover(self,headers):
        self._actor(headers);plan=bootstrap_spec(self.manifest)
        return self._release(headers,self._original(plan))

    def submit(self,headers):
        self._actor(headers);plan=bootstrap_spec(self.manifest)
        try:
            job=self._get(plan)
        except NotFound:
            actor=self._actor(headers)
            self.manifest.authorize(actor,self.clock())
            capture=self._capture(actor)
            # Existing genesis with an absent original is not permission to mint
            # another native mutation (e.g. after native job retention expires).
            _require(capture.snapshot.state_for(self.manifest)=='EMPTY','CAMPAIGN_BOOTSTRAP_ALREADY_PRESENT')
            actor=self._actor(headers);self.manifest.authorize(actor,self.clock())
            try:
                job=operation_call(self.client.query,plan.sql,job_config=plan.config(),job_id=plan.native_job_id,
                    project=PROJECT,location=LOCATION,retry=None,job_retry=None,
                    api_method=bigquery.enums.QueryApiMethod.INSERT,timeout=10)
                outcome=self._classify(plan,job,True)
            except OperationStopped:raise
            except Exception:outcome=self._original(plan,True)
        except OperationStopped:raise
        except Exception:outcome=WorkflowWriteOutcome(plan.native_job_id,Status.UNKNOWN)
        else:outcome=self._classify(plan,job)
        return self._release(headers,outcome)
