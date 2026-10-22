"""Default-disabled protected ASGI conversation/job history route."""
from dataclasses import asdict
from datetime import datetime

from src.application.sharing_api import SharingApi, _request
from src.application.workflow_history_host import METHODS, WorkflowHistoryHost, request_values


def _wire(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _wire(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_wire(child) for child in value]
    return value


class WorkflowHistoryApi(SharingApi):
    """HTTP projection over a prestarted owned history worker."""

    response_version = 'workflow-history-api-v1'

    def _valid_host(self, host):
        return type(host) is WorkflowHistoryHost

    def _request(self, scope):
        return _request(scope, self._settings, prefix='/v1/history/', methods=METHODS)

    def _validate_parameters(self, operation, headers, parameters):
        request_values(operation, headers, parameters)

    def _invoke_operation(self, operation, headers, parameters, cancellation):
        return self._host.list_jobs(headers, cancellation=cancellation, **parameters)

    async def _respond_result(self, operation, value, request_id, respond):
        await respond(200, dict(version=self.response_version, request_id=request_id,
            operation=operation, result=_wire(asdict(value))))
