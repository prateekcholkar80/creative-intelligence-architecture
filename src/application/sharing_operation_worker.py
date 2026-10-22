"""Owned POSIX worker entry point; fixed sharing methods, no public endpoint."""
from __future__ import annotations

from contextlib import ExitStack
import re
import socket
import sys
import time
from uuid import uuid4

from src.application.access_policy import AuthorizationError, Visibility
from src.application.access_reader import AccessReadError, BigQueryAccessReader
from src.application.identity import IdentityError, IapIdentityAdapter
from src.application.operation_budget import OperationStopped, worker_operation_scope
from src.application.operation_channel import receive, send
from src.application.operation_transport import create_runtime_cloud_clients
from src.application.sharing_executor import SharingExecutorError
from src.application.sharing_host_contract import config_from_wire, pack_result, request_values, require
from src.application.sharing_publication_reader import BigQuerySigningReader
from src.application.sharing_runtime import RuntimeSharingSealer, SigningRuntimeError
from src.application.sharing_runtime_executor import RuntimeProtectedSharingExecutor
from src.application.sharing_secret_loader import SharingSecretLoader
from src.application.workflow_contract import ContractError


def _compose(config, resources):
    clients = create_runtime_cloud_clients(settings=config.runtime.operation)
    resources.callback(clients.close)
    runtime = RuntimeSharingSealer(reader=BigQuerySigningReader(client=clients.bigquery),
        loader=SharingSecretLoader(client=clients.secrets), instance_id='host_' + uuid4().hex,
        settings=config.runtime)
    resources.callback(runtime.close)
    return RuntimeProtectedSharingExecutor(client=clients.bigquery, sealer=runtime,
        reader=BigQueryAccessReader(clients.bigquery), verifier=IapIdentityAdapter(config.iap),
        settings=config.executor)


def _safe_error(error):
    if isinstance(error, AuthorizationError):
        return 'SHARING_HOST_ACCESS_DENIED'
    if isinstance(error, (OperationStopped, ContractError, IdentityError, AccessReadError,
                          SharingExecutorError, SigningRuntimeError)):
        code = error.code
        if type(code) is str and re.fullmatch(r'[A-Z][A-Z0-9_]{0,95}', code):
            return code
    return 'SHARING_HOST_OPERATION_FAILED'


def serve(channel):
    """One serial runtime per owned worker; no automatic replacement or replay."""
    channel.setblocking(False)
    startup = receive(channel, lambda: 30)
    require(set(startup) == {'config','nonce'})
    config = config_from_wire(startup['config'])
    require(config.runtime.enabled and config.executor.execution_enabled)
    send(channel, {'ready':startup['nonce']}, lambda:30)
    executor = None
    # EOF/retirement closes resources in the worker, never in the request thread.
    # If a close blocks, the supervisor can still kill this owned process group.
    with ExitStack() as resources:
        while True:
            message = receive(channel, lambda:30, poll_seconds=30)  # EOF wakes idle waiting.
            require(set(message) == {'id','deadline','method','headers','parameters'})
            require(type(message['id']) is str and re.fullmatch(r'[0-9a-f]{32}',message['id']))
            values = request_values(message['method'],message['headers'],message['parameters'])
            try:
                with worker_operation_scope(message['deadline']):
                    if executor is None:
                        with ExitStack() as setup:
                            executor = _compose(config,setup)  # ADC is inside the inherited budget.
                            resources.enter_context(setup.pop_all())
                    parameters = values['parameters']
                    if values['method'] == 'issue_intent':
                        parameters['visibility'] = Visibility(parameters['visibility'])
                        result = executor.issue_intent(values['headers'], **parameters)
                    else:
                        result = getattr(executor, values['method'])(values['headers'], parameters['token'])
                    payload = pack_result(values['method'],result)
                reply = {'id':message['id'],'result':payload}
            except Exception as error:
                reply = {'id':message['id'],'error':_safe_error(error)}
            def remaining():
                left = message['deadline']-time.monotonic()
                if left <= 0:
                    raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
                return left
            send(channel, reply, remaining)
            del message, values, reply


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        raise SystemExit(2)
    channel = socket.socket(fileno=int(sys.argv[1]))
    try:
        serve(channel)
    except BaseException:
        # No raw assertions, keys, exception strings or SDK logs to parent pipes.
        pass
    finally:
        channel.close()


if __name__ == '__main__':
    main()
