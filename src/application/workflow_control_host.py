"""Owned management-operation host; no browser roles, plans or dispatch grants."""
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
import re
import sys

from src.application.identity import IapSettings
from src.application.management_service import ManagementSettings, WorkspaceBootstrapManifest, MembershipSyncMapping, KINDS
from src.application.operation_budget import OperationSettings
from src.application.operation_channel import encode
from src.application.sharing_host_contract import require
from src.application.sharing_operation_host import SharingOperationHost, SharingHostError, SharingHostSettings

METHODS=frozenset({'manage','recover_management'})


@dataclass(frozen=True,slots=True)
class WorkflowControlConfig:
    iap: IapSettings
    management: ManagementSettings = ManagementSettings()

    def __post_init__(self):
        if type(self.iap) is not IapSettings or type(self.management) is not ManagementSettings:
            raise ValueError('Typed trusted control configuration required')


def config_from_wire(value):
    require(type(value) is dict and set(value)=={'iap','management'})
    raw=dict(value['management']);raw['operation']=OperationSettings(**raw['operation'])
    if raw['bootstrap'] is not None:
        boot=dict(raw['bootstrap']);boot['roles']=tuple(boot['roles'])
        for name in ('created_at','expires_at'):boot[name]=datetime.fromisoformat(boot[name])
        raw['bootstrap']=WorkspaceBootstrapManifest(**boot)
    mappings=[]
    for value in raw['sync_mappings']:
        mapping=dict(value);mapping['roles']=tuple(mapping['roles'])
        for name in ('verified_at','expires_at'):mapping[name]=datetime.fromisoformat(mapping[name])
        mappings.append(MembershipSyncMapping(**mapping))
    raw['sync_mappings']=tuple(mappings)
    return WorkflowControlConfig(IapSettings(**value['iap']),ManagementSettings(**raw))


def request_values(method,headers,parameters):
    require(method in METHODS and isinstance(headers,Mapping) and len(headers)<=32
        and all(type(k) is str and 1<=len(k)<=128 and type(v) is str and len(v)<=16384 for k,v in headers.items())
        and type(parameters) is dict)
    required={'workspace_id','command_id'}
    if method=='manage':
        required|={'kind'}
        kind=parameters.get('kind');require(kind in KINDS)
        fields={'BOOTSTRAP':set(),'MEMBER':{'target_subject','roles','status'},'SYNC':{'mapping_id'},
            'WORKSPACE_STATUS':{'status'},'ARCHIVE':{'conversation_id','expected_revision'},'CREATE_CONVERSATION':{'conversation_id'}}[kind]
        required|=fields
        if kind!='BOOTSTRAP':required|={'expected_workspace_revision'}
        allowed=required|({'slot'} if kind=='BOOTSTRAP' else {'expected_revision'} if kind in {'MEMBER','SYNC'} else set())
    else:allowed=required
    require(required<=set(parameters)<=allowed)
    for name in ('workspace_id','command_id','conversation_id','mapping_id'):
        if name in parameters:require(type(parameters[name]) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}',parameters[name]))
    for name in ('expected_revision','expected_workspace_revision','slot'):
        if name in parameters:require(type(parameters[name]) is int and 1<=parameters[name]<2**63)
    if 'roles' in parameters:require(type(parameters['roles']) in (tuple,list) and len(parameters['roles'])<=4 and all(type(r) is str for r in parameters['roles']))
    values=dict(method=method,headers=dict(headers),parameters=dict(parameters));encode(values)
    return values


class WorkflowControlHost(SharingOperationHost):
    def __init__(self,config,settings=SharingHostSettings()):super().__init__(config,settings)
    def _valid_config(self,config):return type(config) is WorkflowControlConfig
    @property
    def operation_settings(self):return self.config.management.operation
    def _enabled(self):
        if not self.config.management.enabled or not self.settings.enabled:raise SharingHostError('SHARING_HOST_DISABLED')
        if self.retired:raise SharingHostError('SHARING_HOST_RETIRED')
    def _command(self,fd):return [sys.executable,'-m','src.application.workflow_control_worker',str(fd)]
    def _config_values(self):
        def wire(value):
            if isinstance(value,datetime):return value.isoformat()
            if isinstance(value,dict):return {k:wire(v) for k,v in value.items()}
            if isinstance(value,(tuple,list)):return [wire(v) for v in value]
            return value
        return wire(asdict(self.config))
    def _request_values(self,method,headers,parameters):return request_values(method,headers,parameters)
    def _decode_result(self,method,value):
        require(type(value) is dict and set(value)=={'workspace_id','command_id','query_job_id','native_status','state','resource_id','revision'})
        require(value['native_status'] in {'COMMITTED','NOT_COMMITTED','UNKNOWN'} and value['state'] in {None,'APPLIED','REUSED'})
        require((value['state'] is None)==(value['resource_id'] is None)==(value['revision'] is None))
        require(value['revision'] is None or type(value['revision']) is int and 1<=value['revision']<2**63)
        require(value['resource_id'] is None or type(value['resource_id']) is str
            and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9:_-]{0,255}',value['resource_id']))
        for name in ('workspace_id','command_id','query_job_id'):
            require(type(value[name]) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,255}',value[name]))
        require(value['state'] is None or value['native_status']=='COMMITTED')
        return value
    def _validate_result_scope(self,method,parameters,result):
        require(result['workspace_id']==parameters['workspace_id'] and result['command_id']==parameters['command_id'])
    def manage(self,headers,*,cancellation=None,**parameters):return self._invoke('manage',headers,parameters,cancellation)
    def recover_management(self,headers,*,cancellation=None,**parameters):return self._invoke('recover_management',headers,parameters,cancellation)
