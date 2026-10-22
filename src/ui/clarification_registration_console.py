"""Opt-in exact-key clarification reviews in the protected registration console.

Only server configuration selects resources. Session state holds issued commands,
never identity assertions or open clients. No policy approval or promotion here.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os

from src.application.campaign_clarification_policy import ClarificationPolicyRequest, read_policy
from src.application.campaign_clarification_prompts import definitions, INTENT_KEY, QUESTION_KEY
from src.application.clarification_prompt_registration import (
    ClarificationPromptRegistrationOperator, ClarificationReviewStore,
)
from src.application.operation_budget import OperationSettings, operation_scope
from src.application.planner_prompt_registration import live_operator, review_projection
from src.application.prompt_contract import PromptTask
from src.application.vertex_prompt_reader import VertexPromptReadRequest
from src.ui.prompt_registration_console import (
    ConsoleError, RegistrationSession, REVIEW_LIFETIME,
)

KEYS = (INTENT_KEY, QUESTION_KEY)
SESSION_KEY = 'ci_clarification_registration_console_v1'


@dataclass(frozen=True, slots=True)
class ClarificationConsoleConfig:
    policy: ClarificationPolicyRequest
    settings_prompt_id: str
    settings_version_id: str
    targets: tuple[tuple[str, str, str], ...]

    def __post_init__(self):
        if (type(self.policy) is not ClarificationPolicyRequest
                or type(self.targets) is not tuple or len(self.targets) != 2
                or any(type(t) is not tuple or len(t) != 3 for t in self.targets)
                or tuple(t[0] for t in self.targets) != KEYS):
            raise ConsoleError('CONSOLE_CLARIFICATION_CONFIGURATION_INVALID')
        for key, prompt_id, version_id in self.targets:
            VertexPromptReadRequest(key, PromptTask.INTERPRETER, prompt_id,
                                    version_id, 'configuration-validation', 'configuration-validation')
        VertexPromptReadRequest('campaign-planner', PromptTask.PLANNER,
            self.settings_prompt_id, self.settings_version_id,
            'configuration-validation', 'configuration-validation')
        if len({t[1:] for t in self.targets}) != 2:
            raise ConsoleError('CONSOLE_CLARIFICATION_CONFIGURATION_INVALID')

    @classmethod
    def from_environment(cls):
        raw = os.environ.get('CI_CLARIFICATION_REGISTRATION_CONFIG', '')
        try:
            if len(raw) > 8192:
                raise ValueError()
            value = json.loads(raw)
            if set(value) != {'policy_id', 'policy_version', 'policy_sha256',
                    'settings_prompt_id', 'settings_version_id', 'targets'}:
                raise ValueError()
            targets = value['targets']
            if type(targets) is not dict or set(targets) != set(KEYS):
                raise ValueError()
            if any(type(targets[k]) is not dict or set(targets[k]) != {'prompt_id', 'version_id'} for k in KEYS):
                raise ValueError()
            return cls(ClarificationPolicyRequest(value['policy_id'], value['policy_version'],
                value['policy_sha256']), value['settings_prompt_id'], value['settings_version_id'],
                tuple((k, targets[k]['prompt_id'], targets[k]['version_id']) for k in KEYS))
        except Exception:
            raise ConsoleError('CONSOLE_CLARIFICATION_CONFIGURATION_INVALID') from None


class ClarificationRegistrationSession(RegistrationSession):
    def __init__(self, config, key):
        super().__init__()
        if type(config) is not ClarificationConsoleConfig or key not in KEYS:
            raise ConsoleError('CONSOLE_CLARIFICATION_CONFIGURATION_INVALID')
        self.config, self.key = config, key
        self.review_store = ClarificationReviewStore()

    def prepare_exact(self, operator, request, headers, identity, settings, *, now):
        self._identity(identity, settings)
        if settings.workspace_id is None:
            raise ConsoleError('CONSOLE_WORKSPACE_REQUIRED')
        if self.command is not None:
            raise ConsoleError('CONSOLE_ORIGINAL_REVIEW_RETAINED')
        target = next(t for t in self.config.targets if t[0] == self.key)
        if (request.prompt_key, request.prompt_id, request.version_id) != target:
            raise ConsoleError('CONSOLE_PREVIEW_SCOPE_INVALID')
        command = operator.prepare(headers, workspace_id=settings.workspace_id, request=request)
        if (command.workspace_id != settings.workspace_id
                or command.approved_preview.request.vertex_request != request
                or command.approved_preview.request.proposed_by != identity.principal.principal_id):
            raise ConsoleError('CONSOLE_PREVIEW_SCOPE_INVALID')
        self.command = command
        self.principal_id = identity.principal.principal_id
        self.expires_at = now + REVIEW_LIFETIME


@contextmanager
def console_operator(session, settings, *, execution_enabled):
    """Fresh disposable clients on each action; retain only server-issued bindings."""
    config = session.config
    with live_operator({}, iap_settings=settings.iap, execution_enabled=execution_enabled) as base:
        reader = base.preview_service.registry_reader
        snapshot = reader.read_exact(config.settings_prompt_id, config.settings_version_id).state.head.snapshot
        policy = read_policy(reader.client, config.policy)
        definition = next(d for d in definitions(policy, checked_at=datetime.now(timezone.utc))
                          if d.prompt_key == session.key)
        target = next(t for t in config.targets if t[0] == session.key)
        request = VertexPromptReadRequest(session.key, PromptTask.INTERPRETER, target[1], target[2],
            definition.output_schema_version, snapshot.safety_policy_version)
        operator = ClarificationPromptRegistrationOperator(policy_request=config.policy,
            policy_reader=lambda r: read_policy(reader.client, r), settings_snapshot=snapshot,
            prompt_key=session.key, review_store=session.review_store,
            preview_service=base.preview_service, command_service=base.command_service,
            identity_adapter=base.identity_adapter)
        yield operator, request


def render(st, headers, identity, settings, key):
    config = ClarificationConsoleConfig.from_environment()
    sessions = st.session_state.setdefault(SESSION_KEY, {})
    if type(sessions) is not dict or any(k not in KEYS for k in sessions):
        raise ConsoleError('CONSOLE_SESSION_INVALID')
    session = sessions.setdefault(key, ClarificationRegistrationSession(config, key))
    if (type(session) is not ClarificationRegistrationSession
            or session.config != config or session.key != key):
        raise ConsoleError('CONSOLE_SESSION_CONFIGURATION_CHANGED')
    session._identity(identity, settings)
    st.caption('Exact clarification prompt review. DRAFT registration does not enable execution.')
    if st.button('Prepare exact-version DRAFT review', disabled=session.command is not None):
        with operation_scope(OperationSettings(120)), console_operator(
                session, settings, execution_enabled=False) as (operator, request):
            session.prepare_exact(operator, request, headers, identity, settings,
                                  now=datetime.now(timezone.utc))
    if session.command is None:
        return
    st.json(review_projection(session.command.approved_preview, authorized=True))
    st.caption('Review expires after five minutes. Keep this session for original-job recovery.')
    with st.form('confirm_clarification_' + key):
        confirmed = st.text_input('Enter the reviewed preview SHA-256')
        submit = st.form_submit_button('Register as DRAFT', disabled=not settings.writes_enabled)
    if submit:
        # Deny before cloud composition; submit also independently checks this gate.
        if not settings.writes_enabled:
            raise ConsoleError('CONSOLE_WRITES_DISABLED')
        with operation_scope(OperationSettings(120)), console_operator(
                session, settings, execution_enabled=True) as (operator, request):
            if request != session.command.approved_preview.request.vertex_request:
                raise ConsoleError('CONSOLE_PREVIEW_SCOPE_INVALID')
            result = session.submit(operator, headers, identity, settings,
                confirmed_sha256=confirmed, now=datetime.now(timezone.utc))
        st.success('DRAFT registration verified in BigQuery')
        st.json(result)
