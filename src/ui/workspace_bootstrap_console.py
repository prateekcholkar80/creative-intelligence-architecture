"""Explicit short-lived single-owner bootstrap; no default grants or retries."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os

from src.application.control_io import ControlOutcome
from src.application.identity import ISSUER, IapIdentityAdapter, ensure_fresh, principal_for_subject
from src.application.management_service import (
    ManagementSettings, ProtectedManagementService, WorkspaceBootstrapManifest, digest,
)
from src.application.operation_budget import OperationSettings, operation_scope
from src.application.operation_transport import create_workflow_read_clients
from src.application.sharing_coordinator_contract import SharingCommitStatus
from src.application.workflow_codec import decode_row
from src.application.workspace_contract import validate_workspace
from src.ingestion.setup_workflow_tables import PROJECT

SESSION_KEY = 'ci_workspace_bootstrap_console_v1'
LOGGER = logging.getLogger(__name__)


class BootstrapConsoleError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__('Workspace bootstrap console operation denied.')


def require(condition, code):
    if not condition:
        raise BootstrapConsoleError(code)


@dataclass(frozen=True, slots=True)
class BootstrapConsoleSettings:
    manifest: WorkspaceBootstrapManifest

    def __post_init__(self):
        require(type(self.manifest) is WorkspaceBootstrapManifest
                and self.manifest.roles == ('ADMIN',), 'BOOTSTRAP_CONFIGURATION_INVALID')

    @classmethod
    def from_environment(cls):
        if os.environ.get('CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED') != 'true':
            return None
        try:
            raw = os.environ['CI_WORKSPACE_BOOTSTRAP_MANIFEST_JSON']
            require(len(raw.encode('utf-8')) <= 4096, 'BOOTSTRAP_CONFIGURATION_INVALID')
            def unique(pairs):
                require(len(dict(pairs)) == len(pairs), 'BOOTSTRAP_CONFIGURATION_INVALID')
                return dict(pairs)
            value = json.loads(raw, object_pairs_hook=unique)
            require(type(value) is dict and set(value) == {
                'workspace_id', 'owner_subject', 'roles', 'manifest_id', 'created_at', 'expires_at'
            } and type(value['roles']) is list, 'BOOTSTRAP_CONFIGURATION_INVALID')
            return cls(WorkspaceBootstrapManifest(value['workspace_id'], value['owner_subject'],
                tuple(value['roles']), value['manifest_id'],
                datetime.fromisoformat(value['created_at']), datetime.fromisoformat(value['expires_at'])))
        except BootstrapConsoleError:
            raise
        except Exception:
            raise BootstrapConsoleError('BOOTSTRAP_CONFIGURATION_INVALID') from None


def review_projection(settings):
    manifest = settings.manifest
    return dict(workspace_id=manifest.workspace_id, owner_subject=manifest.owner_subject,
        principal_id=principal_for_subject(ISSUER, manifest.owner_subject).principal_id,
        roles=list(manifest.roles), manifest_id=manifest.manifest_id,
        created_at=manifest.created_at.isoformat(), expires_at=manifest.expires_at.isoformat(),
        proposed_status='ACTIVE', proposed_revision=1)


@contextmanager
def live_management(settings, iap):
    operation = OperationSettings(120)
    clients = create_workflow_read_clients(settings=operation)
    try:
        yield ProtectedManagementService(client=clients.bigquery,
            verifier=IapIdentityAdapter(iap), settings=ManagementSettings(
                enabled=True, bootstrap=settings.manifest, operation=operation))
    finally:
        clients.close()


class BootstrapSession:
    """Consume one UI entry before cloud composition; later actions recover only."""
    def __init__(self, settings):
        self.settings = settings
        self.attempted = False
        self.last_result = None
        self.last_error = None

    def _identity(self, identity, console_settings, settings):
        ensure_fresh(identity)
        require(self.settings == settings, 'BOOTSTRAP_ORIGINAL_MANIFEST_RETAINED')
        require(identity.issuer == ISSUER and identity.subject == settings.manifest.owner_subject
                and identity.audience == console_settings.iap.expected_audience,
                'BOOTSTRAP_OWNER_REQUIRED')
        require(not console_settings.writes_enabled and console_settings.workspace_id is None,
                'BOOTSTRAP_REGISTRATION_MUST_BE_DISABLED')

    def _result(self, outcome):
        manifest = self.settings.manifest
        job_id = f'workspace_bootstrap_v1_{digest([PROJECT, manifest.workspace_id])}_1'
        require(type(outcome) is ControlOutcome and outcome.job_id == job_id,
                'BOOTSTRAP_RESULT_INVALID')
        require(type(outcome.native_status) is SharingCommitStatus, 'BOOTSTRAP_RESULT_INVALID')
        state = None
        if outcome.native_status is SharingCommitStatus.COMMITTED:
            require(len(outcome.rows) == 1 and set(outcome.rows[0]) == {'state', 'candidate', 'audit'}
                    and outcome.rows[0]['state'] in {'APPLIED', 'REUSED'}, 'BOOTSTRAP_RESULT_INVALID')
            row = decode_row(outcome.rows[0]['candidate'], 'workspaces')
            validate_workspace(row, workspace_id=manifest.workspace_id)
            require(row['status'] == 'ACTIVE' and row['revision'] == 1
                    and row['created_at'] == manifest.created_at
                    and row['updated_at'] == manifest.created_at, 'BOOTSTRAP_RESULT_INVALID')
            state = outcome.rows[0]['state']
        else:
            require(not outcome.rows, 'BOOTSTRAP_RESULT_INVALID')
        return dict(workspace_id=manifest.workspace_id, native_job_id=job_id,
            native_status=outcome.native_status.value, state=state)

    def submit(self, headers, identity, console_settings, settings, *, confirmation, now):
        self._identity(identity, console_settings, settings)
        require(not self.attempted, 'BOOTSTRAP_RECOVERY_ONLY')
        manifest = settings.manifest
        require(confirmation == manifest.workspace_id, 'BOOTSTRAP_CONFIRMATION_REQUIRED')
        require(manifest.created_at <= now < manifest.expires_at, 'BOOTSTRAP_MANIFEST_EXPIRED')
        self.attempted = True
        with operation_scope(OperationSettings(120)), live_management(settings, console_settings.iap) as service:
            plan = service.prepare(headers, kind='BOOTSTRAP', workspace_id=manifest.workspace_id,
                command_id=manifest.manifest_id, slot=1)
            return self._result(service.submit(headers, plan))

    def recover(self, headers, identity, console_settings, settings):
        self._identity(identity, console_settings, settings)
        # Cold original-plan reconstruction never registers an issued write plan.
        self.attempted = True
        with operation_scope(OperationSettings(120)), live_management(settings, console_settings.iap) as service:
            plan = service.original_plan(headers, workspace_id=settings.manifest.workspace_id,
                command_id=settings.manifest.manifest_id)
            return self._result(service.recover(headers, plan))


def render(st, headers, identity, console_settings):
    settings = BootstrapConsoleSettings.from_environment()
    if settings is None:
        return False
    session = st.session_state.get(SESSION_KEY)
    if session is None:
        session = BootstrapSession(settings)
        st.session_state[SESSION_KEY] = session
    require(type(session) is BootstrapSession, 'BOOTSTRAP_SESSION_INVALID')
    session._identity(identity, console_settings, settings)
    st.subheader('Initial workspace bootstrap')
    st.json(review_projection(settings))
    st.caption('Creates one workspace and your ADMIN membership with an audit. Campaign generation stays disabled.')
    now = datetime.now(timezone.utc)
    if session.attempted:
        st.info('A bootstrap action was already attempted in this session. Creation is disabled; use original-job recovery below.')
    if now >= settings.manifest.expires_at:
        st.warning('The bootstrap approval window expired at '
            f'{settings.manifest.expires_at.isoformat()} (UTC). Creation is disabled. '
            'Reloading or editing the workspace ID cannot renew it. Original-job recovery remains available.')
    elif now < settings.manifest.created_at:
        st.info(f'The bootstrap approval window opens at {settings.manifest.created_at.isoformat()} (UTC).')
    with st.form('confirm_workspace_bootstrap'):
        confirmation = st.text_input('Enter the workspace ID to confirm bootstrap')
        submit = st.form_submit_button('Create workspace and ADMIN membership',
            disabled=session.attempted or not settings.manifest.created_at
            <= now < settings.manifest.expires_at)
    recover = st.button('Recover original bootstrap job (read only)')
    if submit or recover:
        from src.ui.prompt_registration_console import safe_error
        session.last_result = None
        session.last_error = None
        action = 'submit' if submit else 'recover'
        LOGGER.info('workspace_bootstrap action=%s phase=started', action)
        try:
            with st.spinner('Verifying bootstrap transaction in BigQuery…'):
                session.last_result = session.submit(headers, identity, console_settings, settings,
                    confirmation=confirmation, now=datetime.now(timezone.utc)) if submit else \
                    session.recover(headers, identity, console_settings, settings)
            LOGGER.info('workspace_bootstrap action=%s phase=finished status=%s',
                action, session.last_result['native_status'])
        except Exception as error:
            session.last_error = safe_error(error)
            # Do not log exception bodies, request headers, assertions or credentials.
            LOGGER.warning('workspace_bootstrap action=%s phase=failed code=%s', action, session.last_error)
    if session.last_error is not None:
        st.error(session.last_error)
        st.caption('No successful bootstrap was verified. Recover the original job; an error does not prove rollback.')
    if session.last_result is not None:
        st.caption('Last verified transaction receipt in this session; this does not establish current workspace access.')
        st.json(session.last_result)
        if session.last_result['native_status'] == SharingCommitStatus.COMMITTED.value:
            st.success('Bootstrap transaction verified. Registration configuration remains disabled.')
        else:
            st.warning('Bootstrap is not verified committed. Only original-job recovery is available.')
    return True
