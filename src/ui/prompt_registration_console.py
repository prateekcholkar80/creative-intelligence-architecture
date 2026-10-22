"""Opt-in, IAP-only single-operator console for existing Batch 13 commands.

No tokens are returned, logged or retained in session state. This console does
not implicitly bootstrap roles, generate campaigns, promote prompts or provide a durable
approval store. A lost process/session requires authoritative operator recovery.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit

from src.application.identity import (
    ASSERTION_HEADER, IapIdentityAdapter, IapSettings, VerifiedIdentity, ensure_fresh,
)
from src.application.operation_budget import OperationSettings, operation_scope
from src.application.planner_prompt_registration import (
    INITIAL_PROMPT_ID, INITIAL_VERSION_ID, live_operator, planner_request,
    review_projection,
)
from src.application.prompt_registration_command import PromptRegistrationCommand
from src.application.workflow_contract import _id

PROJECT_NUMBER = '1042059373096'
REVIEW_LIFETIME = timedelta(minutes=5)
SESSION_KEY = 'ci_prompt_registration_console_v1'


class ConsoleError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__('Prompt registration console operation denied.')


@dataclass(frozen=True, slots=True)
class ConsoleSettings:
    iap: IapSettings
    origin: str
    workspace_id: str | None = None
    writes_enabled: bool = False

    def __post_init__(self):
        parsed = urlsplit(self.origin)
        if (not isinstance(self.iap, IapSettings)
                or not self.iap.expected_audience.startswith(f'/projects/{PROJECT_NUMBER}/')
                or parsed.scheme != 'https' or parsed.path or parsed.query
                or parsed.fragment or parsed.username or parsed.password
                or re.fullmatch(r'[a-z0-9-]+(?:\.[a-z0-9-]+)+', parsed.netloc) is None
                or type(self.writes_enabled) is not bool):
            raise ConsoleError('CONSOLE_CONFIGURATION_INVALID')
        if self.workspace_id is not None:
            _id(self.workspace_id)
        if self.writes_enabled and self.workspace_id is None:
            raise ConsoleError('CONSOLE_WORKSPACE_REQUIRED')

    @classmethod
    def from_environment(cls):
        if os.environ.get('CI_PROMPT_REGISTRATION_CONSOLE_ENABLED') != 'true':
            raise ConsoleError('CONSOLE_DISABLED')
        return cls(IapSettings.from_environment(),
            os.environ.get('CI_PROMPT_REGISTRATION_ORIGIN', ''),
            os.environ.get('CI_PROMPT_REGISTRATION_WORKSPACE_ID') or None,
            os.environ.get('CI_PROMPT_REGISTRATION_WRITES_ENABLED') == 'true')


def request_identity(headers, settings):
    """Use only server request headers, never form/query/session identity fields."""
    assertions = [value for key, value in headers.items()
                  if key.lower() == ASSERTION_HEADER]
    origins = [value for key, value in headers.items() if key.lower() == 'origin']
    # Streamlit's header projection can combine repeated values. Reject them.
    if hasattr(headers, 'get_all'):
        assertions = list(headers.get_all(ASSERTION_HEADER))
        origins = list(headers.get_all('origin'))
    if len(assertions) != 1 or origins != [settings.origin]:
        raise ConsoleError('CONSOLE_REQUEST_INVALID')
    trusted_headers = {ASSERTION_HEADER: assertions[0]}
    identity = IapIdentityAdapter(settings.iap).authenticate(trusted_headers)
    return identity, trusted_headers


def identity_projection(identity):
    ensure_fresh(identity)
    return dict(principal_id=identity.principal.principal_id,
                identity_subject=identity.subject, audience=identity.audience)


class RegistrationSession:
    """Server-memory review only; uncertainty freezes the original command."""
    def __init__(self):
        self.command = None
        self.principal_id = None
        self.expires_at = None
        self.attempted = False

    def _identity(self, identity, settings):
        if not isinstance(identity, VerifiedIdentity):
            raise ConsoleError('CONSOLE_IDENTITY_REQUIRED')
        ensure_fresh(identity)
        if identity.audience != settings.iap.expected_audience:
            raise ConsoleError('CONSOLE_IDENTITY_SCOPE_INVALID')
        if self.principal_id not in (None, identity.principal.principal_id):
            raise ConsoleError('CONSOLE_SESSION_ACTOR_CHANGED')

    def prepare(self, operator, headers, identity, settings, *, now):
        self._identity(identity, settings)
        if os.environ.get('CI_OPERATOR_CONSOLE_ENABLED') == 'true':
            section = st.selectbox('Console section', ('Prompt registration', 'Operator status'))
            if section == 'Operator status':
                from src.ui.campaign_operator_console import render
                render(st, headers, identity, settings)
                return
        if settings.workspace_id is None:
            raise ConsoleError('CONSOLE_WORKSPACE_REQUIRED')
        if self.command is not None:
            raise ConsoleError('CONSOLE_ORIGINAL_REVIEW_RETAINED')
        command = operator.prepare(headers, workspace_id=settings.workspace_id,
            request=planner_request(INITIAL_PROMPT_ID, INITIAL_VERSION_ID))
        if (not isinstance(command, PromptRegistrationCommand)
                or command.workspace_id != settings.workspace_id
                or command.approved_preview.request.proposed_by != identity.principal.principal_id):
            raise ConsoleError('CONSOLE_PREVIEW_SCOPE_INVALID')
        self.command = command
        self.principal_id = identity.principal.principal_id
        self.expires_at = now + REVIEW_LIFETIME
        return review_projection(command.approved_preview, authorized=True)

    def submit(self, operator, headers, identity, settings, *, confirmed_sha256, now):
        self._identity(identity, settings)
        if not settings.writes_enabled:
            raise ConsoleError('CONSOLE_WRITES_DISABLED')
        if (self.command is None or self.command.workspace_id != settings.workspace_id
                or now >= self.expires_at):
            raise ConsoleError('CONSOLE_REVIEW_UNAVAILABLE')
        if confirmed_sha256 != self.command.approved_preview_sha256:
            raise ConsoleError('CONSOLE_REVIEW_NOT_CONFIRMED')
        self.attempted = True
        # Existing command service reauthenticates, rechecks current ADMIN access,
        # recovers the same native job, and verifies exact current/history state.
        result = operator.register(headers, self.command,
            confirmed_sha256=confirmed_sha256)
        return dict(native_job_id=result.native_job_id,
            registration_state=result.registration_state.value,
            snapshot_sha256=result.registered.state.head.snapshot.snapshot_sha256,
            record_digest=result.registered.state.head.digest,
            status=result.registered.state.head.status.value,
            revision=result.registered.state.head.revision,
            history_rows=len(result.registered.state.history))


def safe_error(error):
    code = getattr(error, 'code', '')
    return code if type(code) is str and re.fullmatch(r'[A-Z][A-Z0-9_]{0,95}', code) \
        else 'CONSOLE_OPERATION_UNAVAILABLE'


def main():
    import streamlit as st
    st.set_page_config(page_title='Creative Intelligence — Prompt registration')
    st.title('Prompt registration')
    try:
        settings = ConsoleSettings.from_environment()
        with operation_scope(OperationSettings(30)):
            identity, headers = request_identity(st.context.headers, settings)
        st.success('Google identity verified')
        st.json(identity_projection(identity))
        st.caption('This identity must be explicitly assigned application roles before registration.')
        if os.environ.get('CI_OPERATOR_CONSOLE_ENABLED') == 'true':
            section = st.selectbox('Console section', ('Prompt registration', 'Operator status'))
            if section == 'Operator status':
                from src.ui.campaign_operator_console import render
                render(st, headers, identity, settings)
                return
        if settings.workspace_id is None:
            from src.ui.workspace_bootstrap_console import render
            if not render(st, headers, identity, settings):
                st.info('Workspace bootstrap is pending. Registration is unavailable.')
            return
        choices=('campaign-planner',)
        if os.environ.get('CI_CLARIFICATION_REGISTRATION_ENABLED') == 'true':
            from src.ui.clarification_registration_console import KEYS
            choices+=KEYS
        if os.environ.get('CI_GROUNDED_PLANNER_REGISTRATION_CONFIG'):
            from src.application.campaign_grounded_planner_definition import KEY
            choices+=(KEY,)
        if len(choices)>1:
            selected=st.selectbox('Prompt to review',choices)
            if selected=='campaign-grounded-planner-evaluation':
                from src.ui.grounded_planner_registration_console import render
                render(st,headers,identity,settings);return
            if selected!='campaign-planner':
                from src.ui.clarification_registration_console import render
                render(st,headers,identity,settings,selected);return
        session = st.session_state.get(SESSION_KEY)
        if session is None:
            session = RegistrationSession()
            st.session_state[SESSION_KEY] = session
        if not isinstance(session, RegistrationSession):
            raise ConsoleError('CONSOLE_SESSION_INVALID')
        config = json.loads(Path('config/campaign_brief_generator.json').read_text())
        if st.button('Prepare exact-version DRAFT review', disabled=session.command is not None):
            with operation_scope(OperationSettings(120)), live_operator(config,
                    iap_settings=settings.iap, execution_enabled=False) as operator:
                session.prepare(operator, headers, identity, settings,
                    now=datetime.now(timezone.utc))
        if session.command is None:
            return
        session._identity(identity, settings)
        st.json(review_projection(session.command.approved_preview, authorized=True))
        st.caption('Review expires after five minutes. A lost session cannot restore this approval.')
        with st.form('confirm_exact_prompt_review'):
            confirmed = st.text_input('Enter the reviewed preview SHA-256')
            submit = st.form_submit_button('Register as DRAFT', disabled=not settings.writes_enabled)
        if submit:
            if not settings.writes_enabled:
                raise ConsoleError('CONSOLE_WRITES_DISABLED')
            with operation_scope(OperationSettings(120)), live_operator(config,
                    iap_settings=settings.iap, execution_enabled=settings.writes_enabled) as operator:
                result = session.submit(operator, headers, identity, settings,
                    confirmed_sha256=confirmed, now=datetime.now(timezone.utc))
            st.success('DRAFT registration verified in BigQuery')
            st.json(result)
    except Exception as error:
        st.error(safe_error(error))
        st.caption('Reload for expired identity. After an uncertain write, retain this session for original-job recovery.')


if __name__ == '__main__':
    main()
