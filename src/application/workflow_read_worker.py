"""Fixed owned read worker. Verified bytes cross only the owned local socket."""
from contextlib import ExitStack
import base64
import re
import socket
import sys
import time

from src.application.access_reader import BigQueryAccessReader
from src.application.artifact_download import GcsArtifactReader
from src.application.identity import IapIdentityAdapter
from src.application.operation_budget import OperationStopped, worker_operation_scope
from src.application.operation_channel import receive, send
from src.application.operation_transport import create_workflow_read_clients
from src.application.protected_service import ArtifactDownload, ProtectedServiceError, ProtectedWorkflowService
from src.application.sharing_host_contract import require
from src.application.workflow_read_host import CHUNK_BYTES, config_from_wire,metadata_wire,request_values


def _compose(config,resources):
    clients=create_workflow_read_clients(settings=config.operation);resources.callback(clients.close)
    return ProtectedWorkflowService(IapIdentityAdapter(config.iap),BigQueryAccessReader(clients.bigquery),
        GcsArtifactReader(clients.storage,config.artifacts))


def serve(channel):
    channel.setblocking(False)
    hello=receive(channel,lambda:30);require(set(hello)=={'config','nonce'})
    config=config_from_wire(hello['config']);require(config.enabled)
    send(channel,{'ready':hello['nonce']},lambda:30)
    service=None
    with ExitStack() as resources:
        while True:
            message=receive(channel,lambda:30,poll_seconds=30)
            require(set(message)=={'id','deadline','method','headers','parameters'}
                and type(message['id']) is str and re.fullmatch(r'[0-9a-f]{32}',message['id']))
            values=request_values(message['method'],message['headers'],message['parameters'])
            def remaining():
                left=message['deadline']-time.monotonic()
                if left<=0:raise OperationStopped('OPERATION_DEADLINE_EXCEEDED')
                return left
            try:
                with worker_operation_scope(message['deadline']):
                    if service is None:
                        with ExitStack() as setup:
                            service=_compose(config,setup);resources.enter_context(setup.pop_all())
                    value,capture=service.read_api(values['headers'],operation=values['method'],
                        parameters=values['parameters'],trace_id=message['id'])
                    payload=metadata_wire(value) if not isinstance(value,ArtifactDownload) else dict(
                        filename=value.filename,content_type=value.content_type,sha256=value.sha256,
                        size=len(value.data),output=metadata_wire(value.output))
                    send(channel,{'id':message['id'],'result':{'value':payload,'capture':capture.wire()}},remaining)
                    if isinstance(value,ArtifactDownload):
                        for sequence,offset in enumerate(range(0,len(value.data),CHUNK_BYTES)):
                            send(channel,{'id':message['id'],'sequence':sequence,
                                'data':base64.b64encode(value.data[offset:offset+CHUNK_BYTES]).decode('ascii')},remaining)
            except ProtectedServiceError as error:
                reply=dict(id=message['id'],error='ACCESS_DENIED')
                if error.capture is not None:reply['capture']=error.capture.wire()
                send(channel,reply,remaining)
            except OperationStopped as error:
                send(channel,{'id':message['id'],'error':error.code},remaining)
            except Exception:
                send(channel,{'id':message['id'],'error':'SHARING_HOST_OPERATION_FAILED'},remaining)


def main():
    if len(sys.argv)!=2 or not sys.argv[1].isdigit():raise SystemExit(2)
    channel=socket.socket(fileno=int(sys.argv[1]))
    try:serve(channel)
    except BaseException:pass
    finally:channel.close()


if __name__=='__main__':main()
