"""Priority 4.7 closing UI integration with signed IAP and authoritative fixtures."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from google.auth import jwt

from src.application.identity import (
    ASSERTION_HEADER, ISSUER, IapIdentityAdapter, IapSettings,
    principal_for_subject,
)
from src.application.protected_records import (
    ArtifactSummary, OutputMetadata, OutputSummary,
)
from src.application.protected_service import ArtifactDownload, JobStatus
from src.application.sharing_operation_host import SharingHostError
from src.application.workflow_history_host import (
    WorkflowHistoryConfig, WorkflowHistoryHost,
)
from src.application.workflow_history_service import (
    ConversationHistoryPage, HistoryJob, HistorySettings,
)
from src.application.workflow_read_host import WorkflowReadConfig, WorkflowReadHost
from src.application.workflow_submission_host import (
    WorkflowSubmissionConfig, WorkflowSubmissionHost,
)
from src.application.workflow_submission_service import (
    SubmissionReceipt, SubmissionSettings,
)
from src.ui.workflow_runtime import WorkflowUiError, WorkflowUiRuntime, WorkflowUiSettings
from src.ui.workflow_session import empty_workflow_session, workflow_rows
from tests.test_campaign_chat import preferences
from tests.test_identity import AUDIENCE, signer_and_public_key

WORKSPACE = 'ui-integration-workspace'
CONVERSATION = 'ui-integration-conversation'
SUBJECT = 'accounts.google.com:ui-integration-owner'
FOREIGN_SUBJECT = 'accounts.google.com:ui-integration-peer'


class AuthoritativeWorkflowFixture:
    """Shared BigQuery/GCS-shaped state behind the three typed host contracts."""

    def __init__(self, verifier: IapIdentityAdapter):
        self.verifier = verifier
        self.owner_id = principal_for_subject(ISSUER, SUBJECT).principal_id
        self.active = True
        self.submission_count = 0
        self.download_count = 0
        self.manifest_current = True
        self.now = datetime.now(timezone.utc)
        self.raw = b'authoritatively verified banner bytes'
        self.artifact = ArtifactSummary('ui-banner', 'BANNER_PNG', 'ui-banner.png',
            'image/png', len(self.raw), hashlib.sha256(self.raw).hexdigest())
        self.receipt = SubmissionReceipt(WORKSPACE, CONVERSATION, 'ui-request',
            'ui-job', 'COMMITTED', 'QUEUED', True)
        self.jobs = (
            HistoryJob('ui-job', 'ui-request', 'CI-SKU-0001', 'SUCCEEDED', 3,
                'ui-output', None, self.now, self.now + timedelta(seconds=3)),
            HistoryJob('older-job', 'older-request', 'CI-SKU-0002', 'FAILED', 2,
                None, 'GENERATION_FAILED', self.now - timedelta(days=1), self.now),
        )
        self.summary = OutputSummary(WORKSPACE, CONVERSATION, 'ui-output', 'ui-job',
            'CI-SKU-0001', 'SYNTHETIC_POC', 'SYNTHETIC_POC_ONLY', 'NOT_REVIEWED',
            False, self.now + timedelta(seconds=2), (self.artifact,))

    def authorize(self, headers):
        identity = self.verifier.authenticate(headers)
        if not self.active or identity.principal.principal_id != self.owner_id:
            raise SharingHostError('ACCESS_DENIED')

    def submit(self, headers, **parameters):
        self.authorize(headers)
        if (parameters != dict(workspace_id=WORKSPACE, conversation_id=CONVERSATION,
                preferences=preferences(), retry_key='stable-retry')):
            raise SharingHostError('ACCESS_DENIED')
        self.submission_count += 1
        return self.receipt if self.submission_count == 1 else SubmissionReceipt(
            WORKSPACE, CONVERSATION, 'ui-request', 'ui-job', 'COMMITTED', 'QUEUED', False)

    def list_jobs(self, headers, **parameters):
        self.authorize(headers)
        if parameters.get('workspace_id') != WORKSPACE or parameters.get(
                'conversation_id') != CONVERSATION or parameters.get('page_size') != 1:
            raise SharingHostError('ACCESS_DENIED')
        cursor = parameters.get('cursor')
        selected, next_cursor = ((self.jobs[:1], 'signed-cursor') if cursor is None
            else (self.jobs[1:], None) if cursor == 'signed-cursor'
            else (_ for _ in ()).throw(SharingHostError('ACCESS_DENIED')))
        return ConversationHistoryPage(WORKSPACE, CONVERSATION, 'PRIVATE', 1,
            self.now - timedelta(days=2), self.now, selected, next_cursor)

    def get_job_status(self, headers, **parameters):
        self.authorize(headers)
        if parameters != dict(workspace_id=WORKSPACE, job_id='ui-job'):
            raise SharingHostError('ACCESS_DENIED')
        job = self.jobs[0]
        return JobStatus(WORKSPACE, CONVERSATION, job.job_id, job.canonical_sku,
            job.state, job.revision, job.output_id, job.updated_at)

    def get_output_summary(self, headers, **parameters):
        self.authorize(headers)
        if parameters != dict(workspace_id=WORKSPACE, job_id='ui-job'):
            raise SharingHostError('ACCESS_DENIED')
        return self.summary

    def download_artifact(self, headers, **parameters):
        self.authorize(headers)
        if (parameters != dict(workspace_id=WORKSPACE, job_id='ui-job',
                artifact_id='ui-banner') or not self.manifest_current):
            raise SharingHostError('ACCESS_DENIED')
        self.download_count += 1
        metadata = OutputMetadata(**{name: getattr(self.summary, name)
            for name in OutputMetadata.__dataclass_fields__})
        return ArtifactDownload(self.raw, self.artifact.filename,
            self.artifact.content_type, self.artifact.sha256, metadata)


class WorkflowUiIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.signer, public = signer_and_public_key()
        response = SimpleNamespace(status=200,
            data=json.dumps({'fixture-key': public}).encode())
        fetch = Mock(return_value=response)
        bounded = Mock(return_value=fetch)
        bounded.return_value.close = Mock()
        self.identity_patch = patch(
            'src.application.identity._BoundedKeyRequest', bounded)
        self.identity_patch.start()
        self.addCleanup(self.identity_patch.stop)

        verifier = IapIdentityAdapter(IapSettings(AUDIENCE))
        self.store = AuthoritativeWorkflowFixture(verifier)
        submission = WorkflowSubmissionHost(WorkflowSubmissionConfig(
            IapSettings(AUDIENCE), SubmissionSettings()))
        history = WorkflowHistoryHost(WorkflowHistoryConfig(
            IapSettings(AUDIENCE), HistorySettings()))
        reader = WorkflowReadHost(WorkflowReadConfig(IapSettings(AUDIENCE)))
        for number, host in enumerate((submission, history, reader), start=1):
            host._process = Mock(pid=900 + number)
            host._ready = True
        submission.submit = Mock(side_effect=self.store.submit)
        history.list_jobs = Mock(side_effect=self.store.list_jobs)
        reader.get_job_status = Mock(side_effect=self.store.get_job_status)
        reader.get_output_summary = Mock(side_effect=self.store.get_output_summary)
        reader.download_artifact = Mock(side_effect=self.store.download_artifact)
        self.runtime = WorkflowUiRuntime(submission, history, reader,
            WorkflowUiSettings(WORKSPACE, CONVERSATION, page_size=1))

    def headers(self, subject=SUBJECT):
        epoch = int(datetime.now(timezone.utc).timestamp())
        token = jwt.encode(self.signer, dict(iss=ISSUER, aud=AUDIENCE, sub=subject,
            iat=epoch - 1, exp=epoch + 599)).decode()
        return {ASSERTION_HEADER: token, 'cookie': 'never-forwarded'}

    def test_owner_duplicate_reconnect_refresh_and_deferred_download(self):
        headers = self.headers()
        state = self.runtime.load_history(empty_workflow_session(), headers)
        self.assertEqual([row['job_id'] for row in workflow_rows(state)], ['ui-job'])
        state = self.runtime.load_history(state, headers, cursor=state.next_cursor)
        self.assertEqual([row['job_id'] for row in workflow_rows(state)],
            ['ui-job', 'older-job'])

        first = self.runtime.submit(state, headers,
            preferences=deepcopy(preferences()), retry_key='stable-retry')
        second = self.runtime.submit(first.state, headers,
            preferences=deepcopy(preferences()), retry_key='stable-retry')
        self.assertEqual((first.receipt.request_id, first.receipt.job_id),
            (second.receipt.request_id, second.receipt.job_id))
        self.assertEqual(len(second.state.submissions), 1)

        reconnected = self.runtime.load_history(empty_workflow_session(), headers)
        refreshed = self.runtime.load_history(second.state, headers)
        self.assertEqual(reconnected.jobs[0].job_id, 'ui-job')
        self.assertEqual(refreshed.jobs[0].revision, 3)
        details = self.runtime.job_details(headers, job_id='ui-job')
        fetch = self.runtime.deferred_download(headers, job_id='ui-job',
            artifact=details.output.downloads[0])
        self.assertEqual(self.store.download_count, 0)
        self.assertEqual(fetch(), self.store.raw)
        self.assertEqual(self.store.download_count, 1)

    def test_foreign_or_revoked_access_and_changed_manifest_fail_closed(self):
        for headers, active in ((self.headers(FOREIGN_SUBJECT), True),
                                (self.headers(), False)):
            self.store.active = active
            with self.assertRaises(WorkflowUiError) as caught:
                self.runtime.load_history(empty_workflow_session(), headers)
            self.assertEqual(caught.exception.code, 'ACCESS_DENIED')
            self.store.active = True

        details = self.runtime.job_details(self.headers(), job_id='ui-job')
        fetch = self.runtime.deferred_download(self.headers(), job_id='ui-job',
            artifact=details.output.downloads[0])
        self.store.manifest_current = False
        with self.assertRaises(WorkflowUiError) as caught:
            fetch()
        self.assertEqual(caught.exception.code, 'ACCESS_DENIED')
        self.assertEqual(self.store.download_count, 0)


if __name__ == '__main__':
    unittest.main()
