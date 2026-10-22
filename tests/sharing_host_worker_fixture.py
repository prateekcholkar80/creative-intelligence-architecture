"""Offline owned-process fixture only; never a serving worker or DB fallback."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

from src.application.operation_channel import receive, send
from src.application.identity import IapIdentityAdapter, IapSettings
from src.application.sharing_operation_worker import serve
from tests import test_sharing_runtime_executor as fixtures
from tests.test_sharing_query import result_row
from tests.test_identity import AUDIENCE


def record(path, value):
    with Path(path).open('a') as output:
        output.write(json.dumps(value) + '\n')


def main():
    fd, mode, path = sys.argv[1:]
    channel=socket.socket(fileno=int(fd));channel.setblocking(False)
    if mode == 'startup-hang':
        signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60);return
    if mode in ('partial','mismatch','private-error','late','death','hang','cleanup','helper'):
        hello=receive(channel,lambda:30)
        send(channel,{'ready':hello['nonce']},lambda:30)
        if mode == 'cleanup':
            signal.signal(signal.SIGTERM,signal.SIG_IGN)
            record(path,{'cleanup':os.getpid()})
            time.sleep(60);return
        message=receive(channel,lambda:30)
        record(path,{'method':message['method'],'pid':os.getpid(),'deadline':message['deadline']})
        if mode == 'death':os._exit(1)
        if mode == 'partial':
            channel.send(b'\x00\x00\x00\x40{');time.sleep(60);return
        if mode in ('hang','helper'):
            signal.signal(signal.SIGTERM,signal.SIG_IGN)
            if mode == 'helper':
                code='''import json,os,signal,sys,time
def record(key):
    with open(sys.argv[1],'a') as output: output.write(json.dumps({key:os.getpid()})+'\\n')
def stop(*args):
    record('helper_stopped')
    raise SystemExit(0)
signal.signal(signal.SIGTERM,stop)
record('helper_ready')
time.sleep(60)
'''
                helper=subprocess.Popen([sys.executable,'-c',code,path])
                record(path,{'helper':helper.pid})
            time.sleep(60);return
        if mode == 'late':time.sleep(max(0,message['deadline']-time.monotonic())+.1)
        reply={'id':message['id'],'error':'private token and secret content'}
        if mode == 'mismatch':reply={'id':'0'*32,'result':{'token':'untrusted'}}
        if mode == 'late':reply={'id':message['id'],'result':{'token':'late-token'}}
        send(channel,reply,lambda:30)
        time.sleep(60);return

    real_monotonic=time.monotonic
    fixture=fixtures.RuntimeExecutorTests()
    fixture.setUp()
    # Restore the actual same-host monotonic clock while retaining deterministic
    # application timestamps and real SDK objects with mocked cloud methods.
    def compose(config,resources):
        resources.callback(fixture.doCleanups)
        if mode == 'real-iap':
            # Only the ephemeral PUBLIC certificate is supplied through this
            # offline fixture file; the signed assertion travels through IPC.
            public = json.loads(Path(path + '.public.json').read_text())
            certificate = patch('src.application.identity._BoundedKeyRequest',
                return_value=fixtures.Mock(return_value=SimpleNamespace(status=200,data=json.dumps(public).encode())))
            certificate.start(); fixture.addCleanup(certificate.stop)
            clock = patch('google.auth._helpers.utcnow', return_value=fixture.now.replace(tzinfo=None))
            clock.start(); fixture.addCleanup(clock.stop)
            fixture.executor.verifier = IapIdentityAdapter(IapSettings(AUDIENCE))
        if mode == 'access-denied':
            fixture.cloud.member['status'] = 'DISABLED'
        if mode == 'floor-rollback':
            original_pubs,original_checkpoint=fixture.pubs,fixture.checkpoint
            fixture.advance()
            release=fixture.executor._release
            def rollback_after_release(*args):
                value=release(*args)
                record(path,{'floor':fixture.runtime.state.head.revision})
                fixture.pubs,fixture.checkpoint=original_pubs,original_checkpoint
                return value
            fixture.executor._release=rollback_after_release
        original=fixture.query
        def query(sql,**kwargs):
            if mode == 'real-iap':
                record(path, {'cloud_read':kwargs.get('job_id')})
            if mode == 'slow-composed' and kwargs.get('job_id') == fixture.original.job_id:
                record(path, {'working':kwargs['job_id']})
                time.sleep(.4)
            value=original(sql,**kwargs)
            if kwargs.get('job_id') == fixture.original.job_id:
                record(path,{'insert':kwargs['job_id'],'timeout':kwargs['timeout']})
                if mode == 'committed-hang':
                    signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)
            return value
        fixture.client.query.side_effect=query
        fixture.cloud.insert_rows=[result_row()]
        if mode == 'recover':
            fixture.committed()
        return fixture.executor
    with patch('time.monotonic',side_effect=real_monotonic),\
            patch('src.application.sharing_operation_worker._compose',side_effect=compose):
        try:serve(channel)
        finally:channel.close();fixture.doCleanups()


if __name__=='__main__':main()
