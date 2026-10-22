"""Authenticated Streamlit runtime composition tests; no cloud or model calls."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from src.application.identity import ASSERTION_HEADER, IapSettings
from src.application.protected_records import ArtifactSummary, OutputMetadata, OutputSummary
from src.application.protected_service import ArtifactDownload, JobStatus
from src.application.sharing_operation_host import SharingHostError
from src.application.workflow_history_host import WorkflowHistoryConfig, WorkflowHistoryHost
from src.application.workflow_history_service import ConversationHistoryPage, HistorySettings
from src.application.workflow_submission_host import WorkflowSubmissionConfig, WorkflowSubmissionHost
from src.application.workflow_submission_service import SubmissionReceipt, SubmissionSettings
from src.application.workflow_read_host import WorkflowReadConfig, WorkflowReadHost
from src.ui.workflow_runtime import (
    WorkflowUiError, WorkflowUiJobDetails, WorkflowUiRuntime, WorkflowUiSettings,
    WorkflowUiSubmissionResult, get_workflow_runtime, iap_headers,
    install_workflow_runtime,
)
from src.ui.workflow_session import (
    DURABLE_UI_ENV, apply_history, apply_submission, empty_workflow_session, workflow_rows,
)
from tests.test_campaign_chat import catalog, preferences
from tests.test_identity import AUDIENCE

WS = 'ui-workspace'
CONV = 'ui-conversation'
HEADERS = {ASSERTION_HEADER: 'header.payload.signature', 'cookie': 'private'}


class RepeatedHeaders(dict):
    def __init__(self, values):
        super().__init__()
        self.values = values

    def get_all(self, key):
        return list(self.values) if key.lower() == ASSERTION_HEADER else []


class SafeHostDiagnosticTests(unittest.TestCase):
    def test_diagnostics_exclude_provider_messages_and_untrusted_codes(self):
        for error in (RuntimeError('private-token'), SharingHostError('private-token')):
            with self.assertLogs('src.ui.workflow_runtime', level='WARNING') as logs:
                result = WorkflowUiRuntime._safe_host_error(error)
            self.assertEqual(result.code, 'WORKFLOW_RUNTIME_UNAVAILABLE')
            self.assertNotIn('private-token', str(logs.output))

    def test_known_host_code_is_logged_without_changing_denial(self):
        with self.assertLogs('src.ui.workflow_runtime', level='WARNING') as logs:
            result = WorkflowUiRuntime._safe_host_error(SharingHostError('ACCESS_DENIED'))
        self.assertEqual(result.code, 'ACCESS_DENIED')
        self.assertIn('ACCESS_DENIED', str(logs.output))


def hosts():
    submission = WorkflowSubmissionHost(WorkflowSubmissionConfig(
        IapSettings(AUDIENCE), SubmissionSettings()))
    history = WorkflowHistoryHost(WorkflowHistoryConfig(
        IapSettings(AUDIENCE), HistorySettings()))
    reader = WorkflowReadHost(WorkflowReadConfig(IapSettings(AUDIENCE)))
    submission._process = Mock(pid=101)
    history._process = Mock(pid=102)
    reader._process = Mock(pid=103)
    submission._ready = history._ready = reader._ready = True
    return submission, history, reader


def page():
    at = datetime.now(timezone.utc)
    return ConversationHistoryPage(WS, CONV, 'PRIVATE', 1, at, at, (), None)


def output_summary():
    at = datetime.now(timezone.utc)
    raw = b'verified banner bytes'
    artifact = ArtifactSummary('banner-1', 'BANNER_PNG', 'banner-1.png',
        'image/png', len(raw), hashlib.sha256(raw).hexdigest())
    return OutputSummary(WS, CONV, 'output-1', 'job-1', 'CI-SKU-0001',
        'SYNTHETIC_POC', 'SYNTHETIC_POC_ONLY', 'NOT_REVIEWED', False, at,
        (artifact,))


def job_status(state='SUCCEEDED'):
    at = datetime.now(timezone.utc)
    return JobStatus(WS, CONV, 'job-1', 'CI-SKU-0001', state, 2,
        'output-1' if state == 'SUCCEEDED' else None, at)


class WorkflowUiRuntimeTests(unittest.TestCase):
    def runtime(self):
        submission, history, reader = hosts()
        return WorkflowUiRuntime(submission, history, reader,
            WorkflowUiSettings(WS, CONV, 20)), submission, history, reader

    def test_scope_and_prestarted_hosts_are_required(self):
        with self.assertRaises(ValueError):
            WorkflowUiSettings('bad scope', CONV)
        submission, history, reader = hosts()
        submission._process = None
        with self.assertRaises(ValueError):
            WorkflowUiRuntime(submission, history, reader, WorkflowUiSettings(WS, CONV))

    def test_only_one_iap_assertion_is_forwarded(self):
        self.assertEqual(iap_headers(HEADERS),
            {ASSERTION_HEADER: 'header.payload.signature'})
        for candidate in ({}, RepeatedHeaders(['one', 'two'])):
            with self.subTest(candidate=type(candidate)), self.assertRaises(WorkflowUiError) as caught:
                iap_headers(candidate)
            self.assertEqual(caught.exception.code, 'AUTHENTICATION_REQUIRED')

    def test_history_uses_fixed_scope_and_updates_authoritative_state(self):
        runtime, _, history, _ = self.runtime()
        first = page()
        first = ConversationHistoryPage(first.workspace_id, first.conversation_id,
            first.visibility, first.access_revision, first.conversation_created_at,
            first.conversation_updated_at, first.jobs, 'cursor.part')
        history.list_jobs = Mock(side_effect=(first, page()))
        state = runtime.load_history(empty_workflow_session(), HEADERS)
        self.assertTrue(state.history_loaded)
        self.assertEqual((state.workspace_id, state.conversation_id), (WS, CONV))
        self.assertEqual(history.list_jobs.call_args_list[0].args,
            ({ASSERTION_HEADER: 'header.payload.signature'},))
        self.assertEqual(history.list_jobs.call_args_list[0].kwargs,
            dict(workspace_id=WS, conversation_id=CONV, page_size=20))
        state = runtime.load_history(state, HEADERS, cursor='cursor.part')
        self.assertIsNone(state.next_cursor)
        history.list_jobs.assert_called_with(
            {ASSERTION_HEADER: 'header.payload.signature'},
            workspace_id=WS, conversation_id=CONV, page_size=20,
            cursor='cursor.part')

    def test_submission_preserves_unknown_without_claiming_queued(self):
        runtime, submission, history, _ = self.runtime()
        history.list_jobs = Mock(return_value=page())
        submission.submit = Mock(return_value=SubmissionReceipt(
            WS, CONV, 'request-1', 'job-1', 'UNKNOWN', None, True))
        state = runtime.load_history(empty_workflow_session(), HEADERS)
        submitted = runtime.submit(state, HEADERS,
            preferences=preferences(), retry_key='retry-1')
        row = workflow_rows(submitted.state)[0]
        self.assertIsNone(row['state'])
        self.assertEqual(row['persistence_status'], 'UNKNOWN')
        called = submission.submit.call_args
        self.assertEqual(called.args, ({ASSERTION_HEADER: 'header.payload.signature'},))
        self.assertEqual(called.kwargs, dict(workspace_id=WS, conversation_id=CONV,
            preferences=preferences(), retry_key='retry-1'))

    def test_host_details_are_reduced_to_safe_runtime_codes(self):
        runtime, submission, history, reader = self.runtime()
        history.list_jobs = Mock(side_effect=SharingHostError('PRIVATE_PROVIDER_DETAIL'))
        with self.assertRaises(WorkflowUiError) as caught:
            runtime.load_history(empty_workflow_session(), HEADERS)
        self.assertEqual(caught.exception.code, 'WORKFLOW_RUNTIME_UNAVAILABLE')
        submission.submit = Mock(side_effect=SharingHostError('ACCESS_DENIED'))
        with self.assertRaises(WorkflowUiError) as caught:
            runtime.submit(empty_workflow_session(), HEADERS,
                preferences=preferences(), retry_key='retry-1')
        self.assertEqual(caught.exception.code, 'ACCESS_DENIED')
        reader.get_job_status = Mock(side_effect=SharingHostError('PRIVATE_DETAIL'))
        with self.assertRaises(WorkflowUiError) as caught:
            runtime.job_details(HEADERS, job_id='job-1')
        self.assertEqual(caught.exception.code, 'WORKFLOW_RUNTIME_UNAVAILABLE')

    def test_nonterminal_status_does_not_request_an_output(self):
        runtime, _, _, reader = self.runtime()
        reader.get_job_status = Mock(return_value=job_status('RUNNING'))
        reader.get_output_summary = Mock()
        result = runtime.job_details(HEADERS, job_id='job-1')
        self.assertEqual(result.status.state, 'RUNNING')
        self.assertIsNone(result.output)
        reader.get_output_summary.assert_not_called()

    def test_success_summary_and_download_are_bound_to_exact_job_and_descriptor(self):
        runtime, _, _, reader = self.runtime()
        summary = output_summary()
        reader.get_job_status = Mock(return_value=job_status())
        reader.get_output_summary = Mock(return_value=summary)
        details = runtime.job_details(HEADERS, job_id='job-1')
        self.assertEqual(details.output, summary)
        raw = b'verified banner bytes'
        metadata = OutputMetadata(**{key: getattr(summary, key)
            for key in OutputMetadata.__dataclass_fields__})
        reader.download_artifact = Mock(return_value=ArtifactDownload(raw,
            'banner-1.png', 'image/png', hashlib.sha256(raw).hexdigest(), metadata))
        result = runtime.download(HEADERS, job_id='job-1',
            artifact=summary.downloads[0])
        self.assertEqual(result.data, raw)
        reader.download_artifact.assert_called_once_with(
            {ASSERTION_HEADER: 'header.payload.signature'}, workspace_id=WS,
            job_id='job-1', artifact_id='banner-1')
        changed = ArtifactSummary('banner-1', 'BANNER_PNG', 'banner-1.png',
            'image/png', len(raw) + 1, hashlib.sha256(raw).hexdigest())
        with self.assertRaises(WorkflowUiError):
            runtime.download(HEADERS, job_id='job-1', artifact=changed)
        reader.download_artifact.reset_mock()
        fetch = runtime.deferred_download(HEADERS, job_id='job-1',
            artifact=summary.downloads[0])
        reader.download_artifact.assert_not_called()
        self.assertEqual(fetch(), raw)

    def test_runtime_install_is_idempotent_but_cannot_be_replaced(self):
        first, _, _, _ = self.runtime()
        second, _, _, _ = self.runtime()
        with patch('src.ui.workflow_runtime._RUNTIME', None):
            install_workflow_runtime(first)
            install_workflow_runtime(first)
            self.assertIs(get_workflow_runtime(), first)
            with self.assertRaises(RuntimeError):
                install_workflow_runtime(second)


class WorkflowRuntimeUiTests(unittest.TestCase):
    def setUp(self):
        self.service = Mock()
        self.service.catalog.return_value = catalog()
        self.service.reference_catalog.return_value = catalog()
        from src.application.campaign_chat import Interpretation
        self.service.interpret.return_value = Interpretation(deepcopy(preferences()))
        self.runtime = Mock(spec=WorkflowUiRuntime)
        self.runtime.settings = WorkflowUiSettings(WS, CONV)
        self.runtime.interpretation_available = True
        from src.application.interpreter_submission import InterpreterIntakeOptions
        self.runtime.intake_options.return_value=InterpreterIntakeOptions(WS,CONV,('NEW','RETURNING'),'a'*64,'v1')
        self.runtime.question_turn.return_value = None
        # Synthetic allowed gate for routing-only tests, not live eligibility.
        from types import SimpleNamespace
        self.runtime.evidence_readiness.return_value = SimpleNamespace(release_allowed=True)
        self.runtime.load_history.return_value = apply_history(
            empty_workflow_session(), page())
        receipt = SubmissionReceipt(WS, CONV, 'request-1', 'job-1',
            'COMMITTED', 'QUEUED', True)
        state = apply_submission(self.runtime.load_history.return_value, receipt)
        self.runtime.submit.return_value = WorkflowUiSubmissionResult(state, receipt)
        self.runtime.job_details.return_value = WorkflowUiJobDetails(
            job_status(), output_summary())
        self.runtime.deferred_download.return_value = lambda: b'verified banner bytes'
        self.service_patch = patch('src.ui.campaign_chat.CampaignChatService',
            return_value=self.service)
        self.runtime_patch = patch('src.ui.campaign_chat.get_workflow_runtime',
            return_value=self.runtime)
        self.service_patch.start()
        self.runtime_patch.start()
        self.addCleanup(self.service_patch.stop)
        self.addCleanup(self.runtime_patch.stop)
        self.app = AppTest.from_file(
            str(Path(__file__).resolve().parents[1] / 'app.py'), default_timeout=15)

    def test_authenticated_runtime_restores_history_and_requires_explicit_selection(self):
        with patch.dict(os.environ, {DURABLE_UI_ENV: 'true'}):
            self.app.run()
        self.assertFalse(self.app.exception)
        self.runtime.load_history.assert_called_once()
        self.assertTrue(self.app.chat_input[0].disabled)
        self.service.reference_catalog.assert_called_once()
        self.service.catalog.assert_not_called()

    def test_raw_chat_uses_protected_interpretation_not_local_model(self):
        from src.application.interpreter_submission import InterpreterSubmissionReceipt
        self.runtime.interpret.return_value=InterpreterSubmissionReceipt(WS,CONV,'request-1','invocation-1','native-1','COMMITTED')
        with patch.dict(os.environ,{DURABLE_UI_ENV:'true'}):
            self.app.run()
            self.app.selectbox[0].select('CI-SKU-0001');self.app.selectbox[1].select('US')
            self.app.selectbox[2].select('META');self.app.selectbox[3].select('NEW');self.app.run()
            self.app.chat_input[0].set_value('Create a conversion campaign').run()
        self.assertFalse(self.app.exception)
        self.runtime.interpret.assert_called_once()
        self.service.interpret.assert_not_called();self.service.generate.assert_not_called()

    def test_status_refresh_exposes_deferred_protected_download(self):
        with patch.dict(os.environ, {DURABLE_UI_ENV: 'true'}):
            self.runtime.load_history.return_value=self.runtime.submit.return_value.state
            self.app.run()
            next(button for button in self.app.button
                 if button.label == 'Refresh job status').click().run()
        self.assertFalse(self.app.exception)
        self.runtime.job_details.assert_called_once()
        self.runtime.deferred_download.assert_called_once()
        self.runtime.download.assert_not_called()
        self.assertEqual([button.label for button in self.app.get('download_button')],
            ['Download banner'])


if __name__ == '__main__':
    unittest.main()
