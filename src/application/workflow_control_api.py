"""Disabled-by-default protected ASGI management commands and recovery."""
from src.application.sharing_api import SharingApi, _request
from src.application.workflow_control_host import WorkflowControlHost, request_values, METHODS


class WorkflowControlApi(SharingApi):
    response_version='workflow-control-api-v1'
    def _valid_host(self,host):return type(host) is WorkflowControlHost
    def _request(self,scope):return _request(scope,self._settings,prefix='/v1/control/',methods=METHODS)
    def _validate_parameters(self,operation,headers,parameters):request_values(operation,headers,parameters)
    def _invoke_operation(self,operation,headers,parameters,cancellation):
        return getattr(self._host,operation)(headers,cancellation=cancellation,**parameters)
    async def _respond_result(self,operation,value,request_id,respond):
        await respond(200,dict(version=self.response_version,request_id=request_id,operation=operation,result=value))
