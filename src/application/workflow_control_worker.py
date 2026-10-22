"""Fixed bounded management worker. No model, GCS write or identity fallback."""
from contextlib import ExitStack
import re
import socket
import sys
import time

from src.application.access_policy import AuthorizationError
from src.application.identity import IapIdentityAdapter, IdentityError
from src.application.management_service import ProtectedManagementService, digest
from src.application.operation_budget import OperationStopped, worker_operation_scope
from src.application.operation_channel import receive, send
from src.application.operation_transport import create_workflow_read_clients
from src.application.sharing_host_contract import require
from src.application.workflow_codec import decode_row
from src.application.workflow_contract import ContractError
from src.application.workflow_control_host import config_from_wire, request_values
from src.ingestion.setup_workflow_tables import PROJECT


def _compose(config,resources):
    clients=create_workflow_read_clients(settings=config.management.operation);resources.callback(clients.close)
    return ProtectedManagementService(client=clients.bigquery,verifier=IapIdentityAdapter(config.iap),settings=config.management)


def execute(service,method,headers,parameters):
    if method=='manage':
        parameters=dict(parameters)
        if 'roles' in parameters:parameters['roles']=tuple(parameters['roles'])
        plan=service.prepare(headers,**parameters);outcome=service.submit(headers,plan)
    else:
        actor=service._identity(headers,write=False)
        key='management_v1_'+digest([PROJECT,parameters['workspace_id'],actor.principal.principal_id,parameters['command_id']])
        plan=service._issued.get(key)
        if plan is None:
            candidates=[p for p in service._issued.values() if p.actor_id==actor.principal.principal_id
                and p.workspace_id==parameters['workspace_id'] and p.command_id==parameters['command_id']]
            if candidates:plan=sorted(candidates,key=lambda p:p.job_id)[-1]
        if plan is None:
            # Original-job reconstruction is read-only, including cold workers.
            plan=service.original_plan(headers,**parameters)
        outcome=service.recover(headers,plan)
    candidate=None if not outcome.rows else decode_row(outcome.rows[0]['candidate'],plan.candidate_table)
    return dict(workspace_id=plan.workspace_id,command_id=plan.command_id,query_job_id=plan.job_id,native_status=outcome.native_status.value,
        state=None if candidate is None else outcome.rows[0]['state'],
        resource_id=None if candidate is None else candidate.get('principal_id',candidate.get('conversation_id',candidate['workspace_id'])),
        revision=None if candidate is None else candidate.get('access_revision',candidate.get('revision')))


def serve(channel):
    channel.setblocking(False);hello=receive(channel,lambda:30);require(set(hello)=={'config','nonce'})
    config=config_from_wire(hello['config']);require(config.management.enabled)
    send(channel,{'ready':hello['nonce']},lambda:30);service=None
    with ExitStack() as resources:
        while True:
            message=receive(channel,lambda:30,poll_seconds=30)
            require(set(message)=={'id','deadline','method','headers','parameters'} and type(message['id']) is str
                and re.fullmatch(r'[0-9a-f]{32}',message['id']))
            values=request_values(message['method'],message['headers'],message['parameters'])
            def remaining():
                left=message['deadline']-time.monotonic()
                if left<=0:raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
                return left
            try:
                with worker_operation_scope(message['deadline']):
                    if service is None:
                        with ExitStack() as setup:service=_compose(config,setup);resources.enter_context(setup.pop_all())
                    result=execute(service,values['method'],values['headers'],values['parameters'])
                    send(channel,dict(id=message['id'],result=result),remaining)
            except (IdentityError,AuthorizationError,ContractError):send(channel,dict(id=message['id'],error='ACCESS_DENIED'),remaining)
            except OperationStopped as error:send(channel,dict(id=message['id'],error=error.code),remaining)
            except Exception:send(channel,dict(id=message['id'],error='SHARING_HOST_OPERATION_FAILED'),remaining)


def main():
    if len(sys.argv)!=2 or not sys.argv[1].isdigit():raise SystemExit(2)
    channel=socket.socket(fileno=int(sys.argv[1]))
    try:serve(channel)
    except BaseException:pass
    finally:channel.close()


if __name__=='__main__':main()
