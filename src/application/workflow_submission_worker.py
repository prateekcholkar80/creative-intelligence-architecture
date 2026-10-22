"""Fixed Priority 4.7 submission worker; no model calls or browser-owned policy."""
from contextlib import ExitStack
from dataclasses import asdict
import re
import socket
import sys
import time

from src.application.access_policy import AuthorizationError
from src.application.identity import IapIdentityAdapter, IdentityError
from src.application.operation_budget import OperationStopped, worker_operation_scope
from src.application.operation_channel import receive, send
from src.application.operation_transport import create_workflow_read_clients
from src.application.sharing_host_contract import require
from src.application.workflow_contract import ContractError
from src.application.workflow_submission_host import config_from_wire, request_values
from src.application.workflow_submission_service import ProtectedSubmissionService


def _compose(config, resources):
    clients = create_workflow_read_clients(settings=config.submission.operation)
    resources.callback(clients.close)
    return ProtectedSubmissionService(client=clients.bigquery,
        verifier=IapIdentityAdapter(config.iap), settings=config.submission)


def _compose_interpreter(config,resources):
    require(config.interpretation_enabled)
    from src.application.interpreter_submission import ProtectedInterpreterSubmissionService
    clients=create_workflow_read_clients(settings=config.submission.operation)
    resources.callback(clients.close)
    profiles=[p for p in config.submission.model_profiles if p['stage']=='INTERPRETER']
    require(len(profiles)==1)
    submission=ProtectedInterpreterSubmissionService(client=clients.bigquery,verifier=IapIdentityAdapter(config.iap),
        profile=profiles[0],worker_id=config.submission.worker_id,enabled=True,capture_history=True)
    from src.application.campaign_attachment_composition import compose_attachments
    intake=compose_attachments(client=clients.bigquery,verifier=submission.verifier)
    if intake is not None:
        from src.application.campaign_omission_service import OmissionTurnService
        submission.omission_service=OmissionTurnService(submission=submission,intake=intake)
        resources.callback(intake.store.storage.close)
    if config.interpreter_runtime is not None:
        from src.application.interpreter_serving_composition import compose
        return compose(submission,config.interpreter_runtime,resources)
    return submission


def execute(service, method, headers, parameters):
    require(method in {'submit','interpret','intake_options','evidence','omission'})
    if method=='omission':return asdict(service.omission(headers,**parameters))
    if method=='intake_options':return asdict(service.intake_options(headers,**parameters))
    if method=='evidence':
        from src.application.question_chat_transport import encode_result
        readiness=service.evidence_readiness(headers,**parameters)
        turn=service.question_turn(headers,**parameters)
        return encode_result(parameters['workspace_id'],parameters['conversation_id'],readiness,turn)
    return asdict(service.submit(headers, **parameters))


def serve(channel):
    channel.setblocking(False)
    hello = receive(channel, lambda: 30)
    require(set(hello) == {'config', 'nonce'})
    config = config_from_wire(hello['config'])
    require(config.submission.enabled)
    send(channel, {'ready': hello['nonce']}, lambda: 30)
    services = {}
    with ExitStack() as resources:
        while True:
            message = receive(channel, lambda: 30, poll_seconds=30)
            require(set(message) == {'id', 'deadline', 'method', 'headers', 'parameters'}
                and type(message['id']) is str and re.fullmatch(r'[0-9a-f]{32}', message['id']))
            values = request_values(message['method'], message['headers'], message['parameters'])

            def remaining():
                left = message['deadline'] - time.monotonic()
                if left <= 0:
                    raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
                return left

            try:
                with worker_operation_scope(message['deadline']):
                    method=values['method']
                    require(method not in {'interpret','intake_options','evidence','omission'} or config.interpretation_enabled)
                    service_key='interpret' if method in {'intake_options','evidence','omission'} else method
                    if service_key not in services:
                        with ExitStack() as setup:
                            services[service_key] = (_compose_interpreter if method in {'interpret','intake_options','evidence','omission'} else _compose)(config,setup)
                            resources.enter_context(setup.pop_all())
                    result = execute(services[service_key], values['method'], values['headers'], values['parameters'])
                    send(channel, dict(id=message['id'], result=result), remaining)
            except (IdentityError, AuthorizationError, ContractError):
                send(channel, dict(id=message['id'], error='ACCESS_DENIED'), remaining)
            except OperationStopped as error:
                send(channel, dict(id=message['id'], error=error.code), remaining)
            except Exception:
                send(channel, dict(id=message['id'], error='SHARING_HOST_OPERATION_FAILED'), remaining)


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        raise SystemExit(2)
    channel = socket.socket(fileno=int(sys.argv[1]))
    try:
        serve(channel)
    except BaseException:
        pass
    finally:
        channel.close()


if __name__ == '__main__':
    main()
