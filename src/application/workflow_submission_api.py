"""Default-disabled protected ASGI campaign submission route."""
from dataclasses import asdict

from src.application.sharing_api import SharingApi, _request
from src.application.workflow_submission_host import (
    METHODS, WorkflowSubmissionHost, request_values,
)


class WorkflowSubmissionApi(SharingApi):
    """HTTP projection over a prestarted owned submission worker."""

    response_version = 'workflow-submission-api-v1'

    def _valid_host(self, host):
        return type(host) is WorkflowSubmissionHost

    def _request(self, scope):
        return _request(scope, self._settings, prefix='/v1/submissions/', methods=METHODS)

    def _validate_parameters(self, operation, headers, parameters):
        request_values(operation, headers, parameters)

    def _invoke_operation(self, operation, headers, parameters, cancellation):
        return self._host.submit(headers, cancellation=cancellation, **parameters)

    async def _respond_result(self, operation, value, request_id, respond):
        await respond(200, dict(version=self.response_version, request_id=request_id,
            operation=operation, result=asdict(value)))
