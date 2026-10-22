"""4.4 protected reads over the existing owned-process deadline supervisor."""
from collections import deque
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
import base64
import hashlib
import re
import sys

from src.application.access_capture import capture_from_wire
from src.application.access_policy import Action
from src.application.artifact_download import ArtifactReadSettings
from src.application.identity import IapSettings
from src.application.operation_budget import OperationSettings
from src.application.operation_channel import encode, receive
from src.application.protected_records import ArtifactSummary, OutputMetadata, OutputSummary
from src.application.protected_service import ArtifactDownload, JobStatus
from src.application.sharing_host_contract import require
from src.application.sharing_operation_host import SharingHostError, SharingOperationHost
from src.application.workflow_contract import ARTIFACT_TYPES, ArtifactLimits, JobState
from src.retrieval.request import CANONICAL_SKUS

READ_METHODS=frozenset(('get_job_status','get_output_summary','download_artifact'))
CHUNK_BYTES=16384


@dataclass(frozen=True,slots=True)
class WorkflowReadConfig:
    iap: IapSettings
    enabled: bool = False
    operation: OperationSettings = OperationSettings()
    artifacts: ArtifactReadSettings = ArtifactReadSettings()

    def __post_init__(self):
        if (type(self.iap) is not IapSettings or type(self.enabled) is not bool
                or type(self.operation) is not OperationSettings or type(self.artifacts) is not ArtifactReadSettings):
            raise ValueError('Typed trusted workflow read settings required')


def config_from_wire(value):
    require(type(value) is dict and set(value)=={'iap','enabled','operation','artifacts'})
    artifacts=dict(value['artifacts']);artifacts['limits']=ArtifactLimits(**artifacts['limits'])
    return WorkflowReadConfig(IapSettings(**value['iap']),value['enabled'],
        OperationSettings(**value['operation']),ArtifactReadSettings(**artifacts))


def request_values(method, headers, parameters):
    require(method in READ_METHODS and isinstance(headers,Mapping) and len(headers)<=32
        and all(type(k) is str and 1<=len(k)<=128 and type(v) is str and len(v)<=16384 for k,v in headers.items())
        and type(parameters) is dict)
    fields={'workspace_id','job_id'} | ({'artifact_id'} if method=='download_artifact' else set())
    require(set(parameters)==fields and all(type(v) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',v)
        for v in parameters.values()))
    value=dict(method=method,headers=dict(headers),parameters=parameters)
    encode(value)
    return value


def metadata_wire(value):
    raw=asdict(value)
    return {k:v.isoformat() if isinstance(v,datetime) else v for k,v in raw.items()}


def metadata_from_wire(value, kind, limits=None):
    require(type(value) is dict and set(value)==set(kind.__dataclass_fields__))
    raw=dict(value)
    time_key='updated_at' if kind is JobStatus else 'created_at'
    raw[time_key]=datetime.fromisoformat(raw[time_key])
    require(raw[time_key].tzinfo is not None and raw[time_key].utcoffset() is not None)
    for key in ('workspace_id','conversation_id','job_id'):
        require(type(raw[key]) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',raw[key]))
    require(raw['canonical_sku'] in CANONICAL_SKUS)
    if kind is JobStatus:
        require(raw['state'] in {state.value for state in JobState} and type(raw['revision']) is int and 1<=raw['revision']<2**63)
        require(raw['output_id'] is None or type(raw['output_id']) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',raw['output_id']))
    else:
        require(type(raw['output_id']) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',raw['output_id'])
            and raw['source_system']=='SYNTHETIC_POC' and raw['approval_scope']=='SYNTHETIC_POC_ONLY'
            and raw['initial_review_status']=='NOT_REVIEWED' and raw['real_business_approved'] is False)
    if kind is OutputSummary:
        require(type(limits) is ArtifactLimits)
        require(type(raw['downloads']) in (list,tuple) and len(raw['downloads'])<=len(ARTIFACT_TYPES))
        downloads=[];ids=set();kinds=set()
        for item in raw['downloads']:
            require(type(item) is dict and set(item)==set(ArtifactSummary.__dataclass_fields__))
            artifact_id=item['artifact_id'];artifact_kind=item['kind']
            require(type(artifact_id) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',artifact_id)
                and artifact_kind in ARTIFACT_TYPES and artifact_id not in ids and artifact_kind not in kinds
                and item['filename']==f'{artifact_id}.{ARTIFACT_TYPES[artifact_kind][0]}'
                and item['content_type']==ARTIFACT_TYPES[artifact_kind][1]
                and type(item['size_bytes']) is int
                and 1<=item['size_bytes']<=limits.maximum_artifact_bytes
                and type(item['sha256']) is str and re.fullmatch(r'[0-9a-f]{64}',item['sha256']))
            ids.add(artifact_id);kinds.add(artifact_kind);downloads.append(ArtifactSummary(**item))
        raw['downloads']=tuple(downloads)
    return kind(**raw)


class WorkflowReadHost(SharingOperationHost):
    """Same supervision, separate fixed module/config; no signing or write client."""
    def __init__(self,config,settings=None):
        from src.application.sharing_operation_host import SharingHostSettings
        super().__init__(config,SharingHostSettings() if settings is None else settings)
        self._captures=deque(maxlen=256)
        self._pending_capture=None

    @property
    def captures(self):
        return tuple(self._captures)

    def _valid_config(self,config):
        return type(config) is WorkflowReadConfig

    @property
    def operation_settings(self):
        return self.config.operation

    def _enabled(self):
        if not self.config.enabled or not self.settings.enabled:
            raise SharingHostError('SHARING_HOST_DISABLED')
        if self.retired:
            raise SharingHostError('SHARING_HOST_RETIRED')

    def _command(self,fd):
        return [sys.executable,'-m','src.application.workflow_read_worker',str(fd)]

    def _request_values(self,method,headers,parameters):
        return request_values(method,headers,parameters)

    def _receive_reply(self,method,command_id,remaining):
        reply=receive(self._channel,remaining)
        require(reply.get('id')==command_id)
        if 'error' in reply:
            require(set(reply)<={'id','error','capture'})
            capture=reply.pop('capture',None)
            if capture is not None:
                captured=capture_from_wire(capture)
                require(captured.trace_id==command_id and not captured.decision.allowed)
                self._captures.append(captured)
            return reply
        require(set(reply)=={'id','result'} and type(reply['result']) is dict)
        result=reply['result']
        require(set(result)=={'value','capture'})
        captured=capture_from_wire(result['capture'])
        require(captured.trace_id==command_id and captured.decision.allowed)
        if method=='download_artifact':
            value=result['value']
            require(type(value) is dict and set(value)=={'filename','content_type','sha256','size','output'})
            size=value['size']
            require(type(size) is int and 1<=size<=self.config.artifacts.limits.maximum_artifact_bytes)
            require(type(value['sha256']) is str and re.fullmatch(r'[0-9a-f]{64}',value['sha256']))
            raw=bytearray()
            for sequence in range((size+CHUNK_BYTES-1)//CHUNK_BYTES):
                part=receive(self._channel,remaining)
                require(set(part)=={'id','sequence','data'} and part['id']==command_id and type(part['sequence']) is int and part['sequence']==sequence)
                data=base64.b64decode(part['data'],validate=True)
                require(len(data)==min(CHUNK_BYTES,size-len(raw)))
                raw.extend(data)
            require(hashlib.sha256(raw).hexdigest()==value['sha256'])
            value['data']=bytes(raw)
        self._pending_capture=captured
        return reply

    def _decode_result(self,method,value):
        raw=value['value']
        if method=='get_job_status':
            return metadata_from_wire(raw,JobStatus)
        if method=='get_output_summary':
            return metadata_from_wire(raw,OutputSummary,self.config.artifacts.limits)
        require(type(raw['filename']) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}\.(png|json|txt)',raw['filename'])
            and raw['content_type']=={'png':'image/png','json':'application/json','txt':'text/plain'}[raw['filename'].rsplit('.',1)[1]])
        raw=dict(raw);raw.pop('size');raw['output']=metadata_from_wire(raw['output'],OutputMetadata)
        return ArtifactDownload(**raw)

    def _validate_result_scope(self,method,parameters,result):
        metadata=result.output if isinstance(result,ArtifactDownload) else result
        capture=self._pending_capture
        require(capture is not None)
        require(metadata.workspace_id==parameters['workspace_id'] and metadata.job_id==parameters['job_id']
            and capture.target.workspace_id==metadata.workspace_id and capture.target.job_id==metadata.job_id
            and capture.target.conversation_id==metadata.conversation_id
            and capture.decision.action==(Action.DOWNLOAD_ARTIFACT if method=='download_artifact' else Action.READ_DRAFT))
        if method=='download_artifact':require(result.filename.rsplit('.',1)[0]==parameters['artifact_id'])
        self._captures.append(capture)
        self._pending_capture=None

    def get_job_status(self,headers,*,cancellation=None,**parameters):
        return self._invoke('get_job_status',headers,parameters,cancellation)

    def get_output_summary(self,headers,*,cancellation=None,**parameters):
        return self._invoke('get_output_summary',headers,parameters,cancellation)

    def download_artifact(self,headers,*,cancellation=None,**parameters):
        return self._invoke('download_artifact',headers,parameters,cancellation)
