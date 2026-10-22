"""Offline protected workflow-submission tests; no cloud writes or model calls."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest
from unittest.mock import Mock, patch

from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from src.application.identity import IapIdentityAdapter, IapSettings
from src.application.management_service import digest
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from src.application.workflow_codec import decode_row, encode_row
from src.application.workflow_contract import ContractError
from src.application.workflow_submission_service import (
    ProtectedSubmissionService, SubmissionSettings,
)
from src.application.workflow_writer_executor import WorkflowWriteOutcome
from src.ingestion.setup_workflow_tables import LOCATION, PROJECT
from src.validation.control_fixtures import profile
from tests.test_identity import AUDIENCE
from tests.test_management_service import CONV, WS, conversation, identity, member, workspace


PREFERENCES = dict(canonical_sku='CI-SKU-0001', objective='CONVERSION',
    audience='Synthetic POC fixture', channel='PAID_SOCIAL', visual_reference='NONE')


def settings(enabled=True):
    return SubmissionSettings(enabled=enabled, worker_id='submission-worker',
        model_profiles=(profile('INTERPRETER'), profile('PLANNER')),
        renderer_version='fixture-renderer-v1',
        renderer_settings_sha256=digest(['fixture-renderer']))


def snapshot(*, membership=None, parent=None, ws=None):
    at = datetime.now(timezone.utc)
    return dict(observed_at=at,
        workspaces=[encode_row(workspace(updated_at=at-timedelta(seconds=1)) if ws is None else ws,
            'workspaces')],
        memberships=[encode_row(member(updated_at=at-timedelta(seconds=1))
            if membership is None else membership, 'memberships')],
        conversations=[encode_row(conversation(updated_at=at-timedelta(seconds=1))
            if parent is None else parent, 'conversations')])


def service(enabled=True):
    client = bigquery.Client(project=PROJECT, location=LOCATION,
        credentials=AnonymousCredentials())
    verifier = IapIdentityAdapter(IapSettings(AUDIENCE))
    verifier.authenticate = Mock(side_effect=lambda _headers: identity())
    result = ProtectedSubmissionService(client=client, verifier=verifier,
        settings=settings(enabled))
    result.io.read = Mock(side_effect=lambda *_args, **_kwargs: snapshot())
    result.writer.submit = Mock(side_effect=lambda plan: WorkflowWriteOutcome(
        plan.job_id, Status.COMMITTED, 'APPLIED', True))
    return result


class WorkflowSubmissionServiceTests(unittest.TestCase):
    def setUp(self):
        self.service = service()
        self.addCleanup(self.service.io.client.close)

    def submit(self, **changes):
        values = dict(workspace_id=WS, conversation_id=CONV,
            preferences=deepcopy(PREFERENCES), retry_key='fixture-retry')
        return self.service.submit({}, **(values | changes))

    def test_defaults_deny_before_identity_read_or_write(self):
        disabled = service(False)
        self.addCleanup(disabled.io.client.close)
        with self.assertRaises(ContractError) as caught:
            disabled.submit({}, workspace_id=WS, conversation_id=CONV,
                preferences=PREFERENCES, retry_key='fixture-retry')
        self.assertEqual(caught.exception.code, 'SUBMISSION_DISABLED')
        disabled.verifier.authenticate.assert_not_called()
        disabled.io.read.assert_not_called()
        disabled.writer.submit.assert_not_called()

    def test_submission_builds_exact_queued_rows_and_minimal_receipt(self):
        receipt = self.submit()
        plan = self.service.writer.submit.call_args.args[0]
        request = decode_row(plan.request_json, 'requests')
        job = decode_row(plan.workflow_job_json, 'jobs')
        self.assertEqual(request['principal_id'], identity().principal.principal_id)
        self.assertEqual(request['submitted_preferences']['preferences'], PREFERENCES)
        self.assertEqual((request['request_status'], job['state']), ('READY', 'QUEUED'))
        self.assertEqual(plan.config().maximum_bytes_billed, 512 * 1024**2)
        self.assertEqual(plan.config().script_options.statement_byte_budget, 512 * 1024**2)
        self.assertEqual((receipt.persistence_status, receipt.job_state, receipt.submitted),
            ('COMMITTED', 'QUEUED', True))
        self.assertEqual(set(receipt.__dataclass_fields__), {'workspace_id', 'conversation_id',
            'request_id', 'job_id', 'persistence_status', 'job_state', 'submitted'})
        self.assertNotIn('BEAUTYFEEDS', plan.request_json)

    def test_same_retry_key_reuses_exact_plan_and_changed_input_conflicts(self):
        first = self.submit()
        self.service.writer.submit.side_effect = lambda plan: WorkflowWriteOutcome(
            plan.job_id, Status.COMMITTED, 'APPLIED', False)
        second = self.submit()
        plans = [call.args[0] for call in self.service.writer.submit.call_args_list]
        self.assertIs(plans[0], plans[1])
        self.assertEqual((first.request_id, first.job_id), (second.request_id, second.job_id))
        self.assertFalse(second.submitted)
        changed = deepcopy(PREFERENCES); changed['objective'] = 'AWARENESS'
        with self.assertRaises(ContractError) as caught:
            self.submit(preferences=changed)
        self.assertEqual(caught.exception.code, 'SUBMISSION_IDEMPOTENCY_CONFLICT')
        self.assertEqual(self.service.writer.submit.call_count, 2)

    def test_unknown_and_rollback_never_claim_queued_state(self):
        for status in (Status.UNKNOWN, Status.NOT_COMMITTED):
            with self.subTest(status=status):
                current = service(); self.addCleanup(current.io.client.close)
                current.writer.submit.side_effect = lambda plan, value=status: WorkflowWriteOutcome(
                    plan.job_id, value, None, True)
                receipt = current.submit({}, workspace_id=WS, conversation_id=CONV,
                    preferences=PREFERENCES, retry_key='retry-' + status.value.lower())
                self.assertEqual(receipt.persistence_status, status.value)
                self.assertIsNone(receipt.job_state)

    def test_non_creator_foreign_owner_and_changed_identity_fail_closed(self):
        cases = ('role', 'owner', 'identity')
        for case in cases:
            with self.subTest(case=case):
                current = service(); self.addCleanup(current.io.client.close)
                if case == 'role':
                    current.io.read.side_effect = lambda *_a, **_k: snapshot(
                        membership=member(roles=['VIEWER']))
                elif case == 'owner':
                    current.io.read.side_effect = lambda *_a, **_k: snapshot(
                        parent=conversation(owner_id='another-principal'))
                else:
                    current.verifier.authenticate.side_effect = [identity(), identity(
                        'accounts.google.com:different-submission-user')]
                with self.assertRaises((ContractError, PermissionError)):
                    current.submit({}, workspace_id=WS, conversation_id=CONV,
                        preferences=PREFERENCES, retry_key='retry-' + case)
                if case != 'identity':
                    current.writer.submit.assert_not_called()

    def test_invalid_preferences_settings_and_provider_objects_do_not_escape(self):
        for preferences in ({**PREFERENCES, 'canonical_sku': 'OTHER'},
                            {key: value for key, value in PREFERENCES.items() if key != 'audience'}):
            with self.subTest(preferences=preferences), self.assertRaises(ContractError):
                self.submit(preferences=preferences, retry_key=digest([preferences]))
        with self.assertRaises(ContractError):
            SubmissionSettings(enabled=True)
        with patch('google.genai.Client', side_effect=AssertionError('no model client')):
            self.submit(retry_key='no-model-call')


if __name__ == '__main__':
    unittest.main()
