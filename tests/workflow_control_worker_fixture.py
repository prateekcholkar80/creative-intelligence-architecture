"""Owned offline worker fixture: real IAP signatures, fake native storage only."""
import json
from pathlib import Path
import signal
import socket
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock,patch

from src.application import workflow_control_worker as worker
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from tests.test_management_service import service, applied


def main():
    fd,mode,path=sys.argv[1:];channel=socket.socket(fileno=int(fd))
    svc=service()
    from src.application.identity import IapIdentityAdapter
    svc.verifier=IapIdentityAdapter(svc.verifier.settings)
    fetch=Mock(return_value=SimpleNamespace(status=200,data=Path(path).read_bytes()))
    if mode=='hang':
        def hang(*a,**kw):signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)
        svc.io.read.side_effect=hang
    svc.io.submit_once=Mock(side_effect=lambda plan:ControlOutcome(plan.job_id,Status.UNKNOWN) if mode=='lost' else applied(plan))
    svc.io.recover=Mock(side_effect=lambda plan:applied(plan,False))
    try:
        with patch.object(worker,'_compose',return_value=svc),patch('src.application.identity._BoundedKeyRequest',return_value=fetch):
            worker.serve(channel)
    except BaseException:pass
    finally:channel.close();svc.io.client.close()


if __name__=='__main__':main()
