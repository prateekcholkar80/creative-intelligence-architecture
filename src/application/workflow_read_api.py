"""4.4 opt-in protected status/output/download HTTP routes."""
from src.application.protected_service import ArtifactDownload
from src.application.sharing_api import SharingApi, _request
from src.application.workflow_read_host import READ_METHODS, WorkflowReadHost, metadata_wire, request_values


class WorkflowReadApi(SharingApi):
    response_version = 'workflow-read-api-v1'

    def _valid_host(self,host):
        return type(host) is WorkflowReadHost

    def _request(self,scope):
        return _request(scope,self._settings,prefix='/v1/workflow/',methods=READ_METHODS)

    def _validate_parameters(self,operation,headers,parameters):
        request_values(operation,headers,parameters)

    def _invoke_operation(self,operation,headers,parameters,cancellation):
        return getattr(self._host,operation)(headers,cancellation=cancellation,**parameters)

    async def _respond_result(self,operation,value,request_id,respond):
        if isinstance(value,ArtifactDownload):
            await respond(200,raw=value.data,headers=(
                (b'content-type',value.content_type.encode('ascii')),
                (b'content-disposition',f'attachment; filename="{value.filename}"'.encode('ascii')),
                (b'x-request-id',request_id.encode('ascii')),
                (b'x-content-sha256',value.sha256.encode('ascii'))))
        else:
            await respond(200,dict(version='workflow-read-api-v1',request_id=request_id,
                                  operation=operation,result=metadata_wire(value)))
