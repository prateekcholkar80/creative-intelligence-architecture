"""Native BigQuery sharing-job correlation and conservative failed-job evidence.

These server-internal helpers do not authenticate or grant write permission.
Production callers pin their fixed SQL/scope; the isolated probe pins its own
allowlisted relocated SQL without making production table scope configurable.
"""
from itertools import islice
from src.application.operation_budget import OperationStopped, operation_call, operation_iter

from google.cloud import bigquery

from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import PROJECT

CHILD_LIMIT = 64


def _parameter_nulls(parameters):
    """BigQuery omits parameterValue for SQL NULL scalar parameters."""
    normalized = []
    for parameter in parameters:
        value = dict(parameter)
        scalar = value.get('parameterType', {}).get('type') not in {'ARRAY', 'STRUCT'}
        if scalar and value.get('parameterValue', {}) == {}:
            value['parameterValue'] = {'value': None}
        normalized.append(value)
    return normalized


def matches_sharing_job(job, *, job_id: str, sql: str, config: bigquery.QueryJobConfig) -> bool:
    if (not isinstance(job, bigquery.QueryJob) or job.project != PROJECT or job.location != LOCATION
            or job.job_id != job_id or job.parent_job_id is not None):
        return False
    actual_config = job.to_api_repr().get('configuration', {})
    actual = actual_config.get('query', {})
    expected = config.to_api_repr()
    if (actual.get('query') != sql or actual.get('useLegacySql', False) is not False
            or actual.get('useQueryCache') is not False or actual.get('createSession', False) is not False
            or actual.get('connectionProperties') or actual_config.get('dryRun', False) is not False
            or actual_config.get('jobTimeoutMs') != expected['jobTimeoutMs']):
        return False
    for key in ('maximumBytesBilled', 'scriptOptions'):
        value = expected['query'].get(key)
        if isinstance(value, dict):
            if any(actual.get(key, {}).get(k) != v for k, v in value.items() if v is not None):
                return False
        elif actual.get(key) != value:
            return False
    parameters = actual.get('queryParameters', [])
    wanted = expected['query']['queryParameters']
    return (isinstance(parameters, list) and len(parameters) == len(wanted)
            and sorted(_parameter_nulls(parameters), key=lambda p: p['name'])
                == sorted(_parameter_nulls(wanted), key=lambda p: p['name']))


def failed_sharing_job_status(client: bigquery.Client, *, job_id: str, timeout_seconds: int,
                             parent: bigquery.QueryJob) -> SharingCommitStatus:
    """Require a correlated failed terminal parent before examining its children."""
    if (client.project != PROJECT or not isinstance(parent, bigquery.QueryJob)
            or parent.job_id != job_id or parent.project != PROJECT or parent.location != LOCATION
            or parent.parent_job_id is not None or parent.state != 'DONE' or parent.error_result is None):
        return SharingCommitStatus.UNKNOWN
    try:
        result = operation_call(client.list_jobs, project=PROJECT, parent_job=job_id, all_users=False,
            max_results=CHILD_LIMIT, page_size=CHILD_LIMIT, retry=None, timeout=timeout_seconds)
        children = list(islice(operation_iter(result), CHILD_LIMIT))
        if not children or len(children) >= CHILD_LIMIT:
            return SharingCommitStatus.UNKNOWN
        if any(not isinstance(child, bigquery.QueryJob) or child.project != PROJECT
               or child.location != LOCATION or child.parent_job_id != job_id
               or child.state != 'DONE' for child in children):
            return SharingCommitStatus.UNKNOWN
        commits = [child for child in children if child.statement_type == 'COMMIT_TRANSACTION'
                   and child.error_result is None]
        rollbacks = [child for child in children if child.statement_type == 'ROLLBACK_TRANSACTION'
                     and child.error_result is None]
        if len(commits) == 1 and not rollbacks:
            return SharingCommitStatus.COMMITTED
        if len(rollbacks) == 1 and not commits:
            return SharingCommitStatus.NOT_COMMITTED
    except OperationStopped:
        raise
    except Exception:
        pass
    return SharingCommitStatus.UNKNOWN
