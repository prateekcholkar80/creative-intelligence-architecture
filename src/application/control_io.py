"""4.4 bounded native control-job I/O, correlation and conservative recovery."""
from dataclasses import dataclass
from itertools import islice
import json
from uuid import uuid4

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from src.application.operation_budget import OperationStopped,operation_call,operation_iter
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.sharing_jobs import failed_sharing_job_status,matches_sharing_job
from src.application.sharing_publication_codec import normalize
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_contract import ContractError
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION
from src.application.workflow_schema import TABLES
from src.ingestion.setup_workflow_tables import validate_dataset,validate_table


def query_config(parameters,settings=SharingQuerySettings(maximum_bytes_billed=512*1024*1024)):
    return bigquery.QueryJobConfig(query_parameters=parameters,use_query_cache=False,
        maximum_bytes_billed=settings.maximum_bytes_billed,job_timeout_ms=settings.job_timeout_ms,
        create_session=False,script_options=bigquery.ScriptOptions(
            statement_timeout_ms=settings.statement_timeout_ms,
            statement_byte_budget=min(settings.maximum_bytes_billed,512*1024*1024)))


@dataclass(frozen=True,slots=True)
class ControlPlan:
    actor_id: str
    workspace_id: str
    command_id: str
    job_id: str
    sql: str
    config_json: str
    kind: str
    candidate_json: str
    audit_json: str
    candidate_table: str

    def config(self):
        return bigquery.QueryJobConfig.from_api_repr(json.loads(self.config_json))


@dataclass(frozen=True,slots=True)
class ControlOutcome:
    job_id: str
    native_status: SharingCommitStatus
    rows: tuple[dict,...] = ()
    submitted: bool = False


class ControlIO:
    def __init__(self,client,*,result_timeout_seconds=45):
        if (not isinstance(client,bigquery.Client) or client.project!=PROJECT or client.location!=LOCATION
                or client.default_query_job_config is not None):
            raise ValueError('Fixed-project/region ADC control client required')
        if type(result_timeout_seconds) is not int or not 1<=result_timeout_seconds<=90:
            raise ValueError('Bounded control result wait required')
        self.client=client
        self.result_timeout_seconds=result_timeout_seconds

    def preflight(self):
        """Fresh exact destination metadata before any controlled write proposal."""
        validate_dataset(operation_call(self.client.get_dataset,PROJECT+'.ci_application',retry=None,timeout=10))
        for spec in TABLES.values():
            expected=spec.table(PROJECT)
            validate_table(operation_call(self.client.get_table,expected.reference,retry=None,timeout=10),expected)

    def original_job(self,job_id):
        return operation_call(self.client.get_job,job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)

    def read(self,sql,parameters):
        config=query_config(parameters);job_id='workflow_control_read_'+uuid4().hex
        job=operation_call(self.client.query,sql,job_config=config,job_id=job_id,location=LOCATION,
                           retry=None,job_retry=None,timeout=10)
        rows=tuple(normalize(row) for row in islice(operation_iter(operation_call(job.result,
            max_results=2,page_size=2,retry=None,job_retry=None,timeout=20)),2))
        if (len(rows)!=1 or not matches_sharing_job(job,job_id=job_id,sql=sql,config=config)
                or job.state!='DONE' or job.error_result is not None):
            raise ContractError('CONTROL_READ_UNVERIFIED')
        return rows[0]

    def _outcome(self,plan,job):
        config=plan.config()
        if not matches_sharing_job(job,job_id=plan.job_id,sql=plan.sql,config=config):
            return ControlOutcome(plan.job_id,SharingCommitStatus.UNKNOWN)
        rows=None
        if job.state!='DONE':
            try:
                rows=tuple(normalize(row) for row in islice(operation_iter(operation_call(job.result,
                    max_results=2,page_size=2,retry=None,job_retry=None,timeout=self.result_timeout_seconds)),2))
            except OperationStopped:raise
            except Exception:pass
        if not matches_sharing_job(job,job_id=plan.job_id,sql=plan.sql,config=config) or job.state!='DONE':
            return ControlOutcome(plan.job_id,SharingCommitStatus.UNKNOWN)
        if job.error_result is not None:
            status=failed_sharing_job_status(self.client,job_id=plan.job_id,timeout_seconds=10,parent=job)
            return ControlOutcome(plan.job_id,status)
        try:
            if rows is None:
                rows=tuple(normalize(row) for row in islice(operation_iter(operation_call(job.result,
                    max_results=2,page_size=2,retry=None,job_retry=None,timeout=20)),2))
            if len(rows)!=1 or not matches_sharing_job(job,job_id=plan.job_id,sql=plan.sql,config=config):
                return ControlOutcome(plan.job_id,SharingCommitStatus.UNKNOWN)
            return ControlOutcome(plan.job_id,SharingCommitStatus.COMMITTED,rows)
        except OperationStopped:raise
        except Exception:return ControlOutcome(plan.job_id,SharingCommitStatus.UNKNOWN)

    def recover(self,plan):
        try:
            job=operation_call(self.client.get_job,plan.job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
            return self._outcome(plan,job)
        except OperationStopped:raise
        except Exception:return ControlOutcome(plan.job_id,SharingCommitStatus.UNKNOWN)

    def submit_once(self,plan):
        """Caller must consume a fresh issued plan before this single entry."""
        submitted=False
        try:
            job=operation_call(self.client.get_job,plan.job_id,project=PROJECT,location=LOCATION,retry=None,timeout=10)
        except NotFound:
            try:
                job=operation_call(self.client.query,plan.sql,job_config=plan.config(),job_id=plan.job_id,
                    location=LOCATION,retry=None,job_retry=None,timeout=10)
                submitted=True
            except OperationStopped:raise
            except Exception:return ControlOutcome(plan.job_id,SharingCommitStatus.UNKNOWN)
        except OperationStopped:raise
        except Exception:return ControlOutcome(plan.job_id,SharingCommitStatus.UNKNOWN)
        result=self._outcome(plan,job)
        return ControlOutcome(result.job_id,result.native_status,result.rows,submitted)
