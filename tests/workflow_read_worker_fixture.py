"""Owned read-worker fixture: ephemeral public certificate and mocked BQ/GCS."""
import json
import signal
import socket
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from tests.test_protected_download import ProtectedDownloadTests
from src.application.workflow_read_worker import serve


def main():
    fd,mode,path=sys.argv[1:]
    fixture=ProtectedDownloadTests();fixture.setUpClass();fixture.setUp()
    public=json.loads(Path(path).read_text())
    fixture.fetch.return_value=SimpleNamespace(status=200,data=json.dumps(public).encode())
    from tests.test_protected_service import workspace,member
    membership=member()
    tables=dict(workspaces=[workspace()],memberships=[membership],jobs=[fixture.job],
        conversations=[fixture.parent],requests=[fixture.request],outputs=[fixture.output])
    def query(sql,**kwargs):
        if mode=='hang':
            signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)
        table=next(name for name in tables if f'.{name}`' in sql)
        return Mock(result=Mock(return_value=tables[table]))
    fixture.client.query.side_effect=query
    if mode=='revoked':
        def revoke(**kwargs):membership['status']='REVOKED';return fixture.png
        fixture.blob.download_as_bytes.side_effect=revoke
    if mode=='large':
        raw=b'POC draft\n'*12000
        manifest=fixture.manifest('BRIEF_TEXT',raw);fixture.output['artifacts'].append(manifest)
        fixture.configure_blob(raw,manifest)
    def compose(config,resources):
        resources.callback(fixture.doCleanups)
        return fixture.service
    channel=socket.socket(fileno=int(fd))
    try:
        with patch('src.application.workflow_read_worker._compose',side_effect=compose):serve(channel)
    finally:channel.close();fixture.doCleanups()


if __name__=='__main__':main()
