"""Opt-in serving startup for protected intake; no model worker or output release."""
import atexit,base64,hashlib,json,os
from pathlib import Path
from threading import RLock
from src.application.identity import IapSettings
from src.application.operation_budget import OperationSettings
from src.application.workflow_submission_service import SubmissionSettings
from src.application.workflow_submission_host import WorkflowSubmissionConfig,WorkflowSubmissionHost
from src.application.workflow_history_service import HistorySettings
from src.application.workflow_history_host import WorkflowHistoryConfig,WorkflowHistoryHost
from src.application.workflow_read_host import WorkflowReadConfig,WorkflowReadHost
from src.application.sharing_operation_host import SharingHostSettings
from src.application.workflow_contract import _require
from src.application.sharing_query import SharingQuerySettings
from src.ui.workflow_runtime import WorkflowUiRuntime,WorkflowUiSettings,install_workflow_runtime,get_workflow_runtime

_LOCK=RLock()
_INSTALLED=None
_CONFIG=None
CONFIG_ENV='CI_CAMPAIGN_SERVING_CONFIG'
KEY_ENV='CI_CAMPAIGN_HISTORY_CURSOR_KEY_B64'


def install_from_config(value,cursor_key):
    """Deployment-owned configuration only; never accepts browser values."""
    global _INSTALLED,_CONFIG
    required={'iap_audience','workspace_id','conversation_id','submission'}
    _require(type(value) is dict and set(value) in (required,required|{'interpreter_runtime'}),'CAMPAIGN_STARTUP_CONFIG_INVALID')
    _require(type(cursor_key) is bytes and len(cursor_key)>=32,'CAMPAIGN_CURSOR_KEY_REQUIRED')
    raw=dict(value['submission']);raw['model_profiles']=tuple(raw.get('model_profiles',()))
    raw['operation']=OperationSettings(**raw.get('operation',{}))
    if 'query' in raw:raw['query']=SharingQuerySettings(**raw['query'])
    submission=SubmissionSettings(**raw)
    _require(submission.enabled,'CAMPAIGN_INTAKE_NOT_ENABLED')
    scope=WorkflowUiSettings(value['workspace_id'],value['conversation_id']);iap=IapSettings(value['iap_audience'])
    with _LOCK:
        if _INSTALLED is not None:
            _require((value,hashlib.sha256(cursor_key).hexdigest())==_CONFIG,'CAMPAIGN_STARTUP_CONFIG_CHANGED')
            _require(all(not h.retired and h.worker_pid is not None for h in _INSTALLED[1]),'CAMPAIGN_HOST_RETIRED_RESTART_REQUIRED')
            return _INSTALLED[0]
        _require(get_workflow_runtime() is None,'CAMPAIGN_RUNTIME_ALREADY_INSTALLED')
        settings=SharingHostSettings(enabled=True)
        hosts=(WorkflowSubmissionHost(WorkflowSubmissionConfig(iap,submission,True,value.get('interpreter_runtime')),settings),
            WorkflowHistoryHost(WorkflowHistoryConfig(iap,HistorySettings(enabled=True,cursor_key=cursor_key)),settings),
            WorkflowReadHost(WorkflowReadConfig(iap,enabled=True),settings))
        try:
            for host in hosts:host.start()
            from src.application.campaign_attachment_composition import compose_attachments
            from src.application.identity import IapIdentityAdapter
            from google.cloud import bigquery
            from src.ingestion.setup_workflow_tables import PROJECT,LOCATION
            # A missing attachment config must not initialize another cloud client.
            from src.application.campaign_attachment_composition import CONFIG_ENV as ATTACHMENTS_CONFIG_ENV
            attachments=None
            if os.environ.get(ATTACHMENTS_CONFIG_ENV):
                attachments=compose_attachments(client=bigquery.Client(project=PROJECT,location=LOCATION),
                    verifier=IapIdentityAdapter(iap))
            runtime=WorkflowUiRuntime(*hosts,scope,attachment_service=attachments)
            install_workflow_runtime(runtime)
        except BaseException:
            for host in reversed(hosts):host.close()
            raise
        _INSTALLED=(runtime,hosts);_CONFIG=(json.loads(json.dumps(value)),hashlib.sha256(cursor_key).hexdigest())
        for host in hosts:atexit.register(host.close)
        return runtime


def startup(environ=None):
    env=os.environ if environ is None else environ
    if env.get('CI_DURABLE_WORKFLOW_UI_ENABLED')!='true' or not env.get(CONFIG_ENV):return None
    try:
        path=Path(env[CONFIG_ENV]);_require(path.is_absolute(),'CAMPAIGN_STARTUP_CONFIG_PATH')
        _require(path.stat().st_size<=65536,'CAMPAIGN_STARTUP_CONFIG_SIZE')
        value=json.loads(path.read_text())
        cursor_key=base64.b64decode(env.get(KEY_ENV,''),validate=True)
        return install_from_config(value,cursor_key)
    except Exception:
        # Never expose manifest contents, cursor material or SDK errors on startup.
        raise RuntimeError('CAMPAIGN_SERVING_STARTUP_FAILED') from None
