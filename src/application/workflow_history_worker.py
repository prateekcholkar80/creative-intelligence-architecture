"""Fixed Priority 4.7 history worker; BigQuery reads only, no local fallback."""
from contextlib import ExitStack
from dataclasses import asdict
from datetime import datetime
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
from src.application.workflow_history_host import config_from_wire, request_values
from src.application.workflow_history_service import ProtectedHistoryService


def _compose(config, resources):
    clients = create_workflow_read_clients(settings=config.history.operation)
    resources.callback(clients.close)
    return ProtectedHistoryService(client=clients.bigquery,
        verifier=IapIdentityAdapter(config.iap), settings=config.history)


def _wire(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _wire(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_wire(child) for child in value]
    return value


def execute(service, method, headers, parameters):
    require(method == 'list_jobs')
    return _wire(asdict(service.list_jobs(headers, **parameters)))


def serve(channel):
    channel.setblocking(False)
    hello = receive(channel, lambda: 30)
    require(set(hello) == {'config', 'nonce'})
    config = config_from_wire(hello['config'])
    require(config.history.enabled)
    send(channel, {'ready': hello['nonce']}, lambda: 30)
    service = None
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
                    if service is None:
                        with ExitStack() as setup:
                            service = _compose(config, setup)
                            resources.enter_context(setup.pop_all())
                    result = execute(service, values['method'], values['headers'], values['parameters'])
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
