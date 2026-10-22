"""Pure chatbot state for durable submission and BigQuery history reconnect."""
from __future__ import annotations

from dataclasses import dataclass
import os

from src.application.management_service import require
from src.application.workflow_history_service import ConversationHistoryPage, HistoryJob
from src.application.workflow_submission_service import SubmissionReceipt

DURABLE_UI_ENV = 'CI_DURABLE_WORKFLOW_UI_ENABLED'
MAX_SESSION_SUBMISSIONS = 64


def durable_ui_enabled(environ=None) -> bool:
    """Explicit opt-in only; an absent or malformed value remains disabled."""
    value = (os.environ if environ is None else environ).get(DURABLE_UI_ENV)
    return value == 'true'


@dataclass(frozen=True, slots=True)
class UiSubmission:
    request_id: str
    job_id: str
    persistence_status: str
    job_state: str | None


@dataclass(frozen=True, slots=True)
class WorkflowSessionState:
    workspace_id: str | None = None
    conversation_id: str | None = None
    jobs: tuple[HistoryJob, ...] = ()
    submissions: tuple[UiSubmission, ...] = ()
    next_cursor: str | None = None
    history_loaded: bool = False


def empty_workflow_session() -> WorkflowSessionState:
    return WorkflowSessionState()


def _scope(state, workspace_id, conversation_id):
    require(type(state) is WorkflowSessionState, 'INVALID_UI_WORKFLOW_STATE')
    if state.workspace_id is None:
        require(state.conversation_id is None and not state.jobs and not state.submissions
            and state.next_cursor is None and not state.history_loaded,
            'INVALID_UI_WORKFLOW_STATE')
        return workspace_id, conversation_id
    require(state.workspace_id == workspace_id and state.conversation_id == conversation_id,
        'UI_WORKFLOW_SCOPE_MISMATCH')
    return state.workspace_id, state.conversation_id


def apply_submission(state: WorkflowSessionState,
                     receipt: SubmissionReceipt) -> WorkflowSessionState:
    require(type(receipt) is SubmissionReceipt, 'INVALID_UI_SUBMISSION')
    workspace_id, conversation_id = _scope(
        state, receipt.workspace_id, receipt.conversation_id)
    candidate = UiSubmission(receipt.request_id, receipt.job_id,
        receipt.persistence_status, receipt.job_state)
    existing = [value for value in state.submissions
        if value.request_id == candidate.request_id or value.job_id == candidate.job_id]
    if existing:
        require(len(existing) == 1 and existing[0] == candidate,
            'UI_SUBMISSION_CONFLICT')
        return state
    require(len(state.submissions) < MAX_SESSION_SUBMISSIONS,
        'UI_SUBMISSION_CAPACITY_EXCEEDED')
    return WorkflowSessionState(workspace_id, conversation_id, state.jobs,
        state.submissions + (candidate,), state.next_cursor, state.history_loaded)


def apply_history(state: WorkflowSessionState, page: ConversationHistoryPage,
                  *, requested_cursor: str | None = None) -> WorkflowSessionState:
    require(type(page) is ConversationHistoryPage, 'INVALID_UI_HISTORY')
    workspace_id, conversation_id = _scope(
        state, page.workspace_id, page.conversation_id)
    require(all(type(job) is HistoryJob for job in page.jobs), 'INVALID_UI_HISTORY')
    keys = [(job.created_at, job.job_id) for job in page.jobs]
    require(len({job.job_id for job in page.jobs}) == len(page.jobs)
        and keys == sorted(keys, reverse=True), 'INVALID_UI_HISTORY')
    if requested_cursor is None:
        previous = {job.job_id: job for job in state.jobs}
        for job in page.jobs:
            prior = previous.get(job.job_id)
            require(prior is None or job.revision > prior.revision
                or job.revision == prior.revision and job == prior,
                'UI_HISTORY_REVISION_REGRESSION')
        jobs = page.jobs
    else:
        require(state.history_loaded and requested_cursor == state.next_cursor,
            'UI_HISTORY_CURSOR_MISMATCH')
        existing_ids = {job.job_id for job in state.jobs}
        require(not existing_ids.intersection(job.job_id for job in page.jobs),
            'UI_HISTORY_OVERLAP')
        jobs = state.jobs + page.jobs
        combined = [(job.created_at, job.job_id) for job in jobs]
        require(combined == sorted(combined, reverse=True), 'INVALID_UI_HISTORY')
    return WorkflowSessionState(workspace_id, conversation_id, tuple(jobs),
        state.submissions, page.next_cursor, True)


def workflow_rows(state: WorkflowSessionState) -> tuple[dict, ...]:
    """Minimal rendering projection; uncertain writes never claim a job state."""
    require(type(state) is WorkflowSessionState, 'INVALID_UI_WORKFLOW_STATE')
    rows = [dict(job_id=job.job_id, request_id=job.request_id, state=job.state,
        canonical_sku=job.canonical_sku, updated_at=job.updated_at,
        source='BIGQUERY_HISTORY') for job in state.jobs]
    known = {row['job_id'] for row in rows}
    for submission in reversed(state.submissions):
        if submission.job_id not in known:
            rows.insert(0, dict(job_id=submission.job_id,
                request_id=submission.request_id,
                state=submission.job_state,
                canonical_sku=None, updated_at=None,
                source='SUBMISSION_RECEIPT',
                persistence_status=submission.persistence_status))
    return tuple(rows)
