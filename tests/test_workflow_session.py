"""Pure durable UI state and default-disabled Streamlit integration tests."""
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from src.application.workflow_contract import ContractError
from src.application.workflow_history_service import ConversationHistoryPage, HistoryJob
from src.application.workflow_submission_service import SubmissionReceipt
from src.ui.workflow_session import (
    DURABLE_UI_ENV, apply_history, apply_submission, durable_ui_enabled,
    empty_workflow_session, workflow_rows,
)
from tests.test_campaign_chat import catalog

WS = 'ui-workspace'
CONV = 'ui-conversation'


def job(number, at, state='RUNNING', revision=1):
    return HistoryJob(f'job-{number}', f'request-{number}', 'CI-SKU-0001', state,
        revision, 'output-1' if state == 'SUCCEEDED' else None,
        'SAFE_FAILURE' if state in {'FAILED', 'BLOCKED', 'RECOVERY_REQUIRED'} else None,
        at, at + timedelta(seconds=revision))


def page(jobs=(), cursor=None):
    at = datetime.now(timezone.utc) - timedelta(minutes=5)
    return ConversationHistoryPage(WS, CONV, 'PRIVATE', 1, at, at,
        tuple(jobs), cursor)


class WorkflowSessionTests(unittest.TestCase):
    def test_durable_mode_requires_exact_explicit_opt_in(self):
        self.assertFalse(durable_ui_enabled({}))
        for value in ('1', 'TRUE', 'yes', ''):
            self.assertFalse(durable_ui_enabled({DURABLE_UI_ENV: value}))
        self.assertTrue(durable_ui_enabled({DURABLE_UI_ENV: 'true'}))

    def test_submission_receipt_is_idempotent_and_unknown_never_claims_queued(self):
        receipt = SubmissionReceipt(WS, CONV, 'request-1', 'job-1',
            'UNKNOWN', None, True)
        state = apply_submission(empty_workflow_session(), receipt)
        self.assertIs(apply_submission(state, receipt), state)
        row = workflow_rows(state)[0]
        self.assertIsNone(row['state'])
        self.assertEqual(row['persistence_status'], 'UNKNOWN')
        changed = SubmissionReceipt(WS, CONV, 'request-1', 'job-1',
            'NOT_COMMITTED', None, False)
        with self.assertRaises(ContractError):
            apply_submission(state, changed)

    def test_first_history_page_restores_and_refresh_replaces_current_jobs(self):
        at = datetime.now(timezone.utc) - timedelta(minutes=1)
        initial = apply_history(empty_workflow_session(), page(
            (job(2, at + timedelta(seconds=2)), job(1, at)), 'cursor-1'))
        self.assertEqual([row['job_id'] for row in workflow_rows(initial)],
            ['job-2', 'job-1'])
        refreshed = apply_history(initial, page((job(2, at + timedelta(seconds=2),
            state='SUCCEEDED', revision=2),), None))
        self.assertEqual((refreshed.jobs[0].state, refreshed.jobs[0].revision),
            ('SUCCEEDED', 2))
        self.assertIsNone(refreshed.next_cursor)
        with self.assertRaises(ContractError):
            apply_history(initial, page((job(2, at + timedelta(seconds=2),
                state='FAILED', revision=1),), None))

    def test_next_page_requires_exact_cursor_scope_order_and_no_overlap(self):
        at = datetime.now(timezone.utc) - timedelta(minutes=1)
        state = apply_history(empty_workflow_session(), page(
            (job(3, at + timedelta(seconds=3)), job(2, at + timedelta(seconds=2))),
            'cursor-1'))
        appended = apply_history(state, page((job(1, at),), None),
            requested_cursor='cursor-1')
        self.assertEqual([value.job_id for value in appended.jobs],
            ['job-3', 'job-2', 'job-1'])
        for candidate, cursor in ((page((job(2, at),), None), 'cursor-1'),
                                  (page((job(1, at),), None), 'wrong')):
            with self.assertRaises(ContractError):
                apply_history(state, candidate, requested_cursor=cursor)
        foreign = ConversationHistoryPage('foreign', CONV, 'PRIVATE', 1,
            at, at, (), None)
        with self.assertRaises(ContractError):
            apply_history(state, foreign)


class WorkflowSessionUiTests(unittest.TestCase):
    def setUp(self):
        self.service = Mock()
        self.service.catalog.return_value = catalog()
        self.service_patch = patch('src.ui.campaign_chat.CampaignChatService',
            return_value=self.service)
        self.service_patch.start()
        self.addCleanup(self.service_patch.stop)
        self.app_path = str(Path(__file__).resolve().parents[1] / 'app.py')

    def test_default_ui_remains_session_only(self):
        with patch.dict(os.environ, {DURABLE_UI_ENV: ''}):
            app = AppTest.from_file(self.app_path, default_timeout=15).run()
        self.assertFalse(app.exception)
        self.assertTrue(any('Conversation history lasts for this session' in item.value
            for item in app.caption))
        self.assertFalse(any('Protected workflow runtime composition' in item.value
            for item in app.info))

    def test_opt_in_fails_closed_until_protected_runtime_is_composed(self):
        with patch.dict(os.environ, {DURABLE_UI_ENV: 'true'}):
            app = AppTest.from_file(self.app_path, default_timeout=15).run()
        self.assertFalse(app.exception)
        self.assertTrue(any('Protected workflow runtime composition is not active' in item.value
            for item in app.info))
        self.assertTrue(app.chat_input[0].disabled)
        self.service.catalog.assert_not_called()
        self.service.interpret.assert_not_called()
        self.service.generate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
