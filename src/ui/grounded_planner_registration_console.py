"""One configured DRAFT target in the existing protected registration console."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
import json,os
from src.ui.prompt_registration_console import RegistrationSession,ConsoleError,REVIEW_LIFETIME
from src.application.grounded_planner_registration import GroundedPlannerTarget,GroundedReviewStore,GroundedPlannerRegistrationOperator
from src.application.campaign_grounded_planner_definition import KEY
from src.application.planner_prompt_registration import live_operator,review_projection
from src.application.vertex_prompt_reader import VertexPromptReadRequest
from src.application.prompt_contract import PromptTask
from src.application.operation_budget import operation_scope,OperationSettings

CONFIG_ENV='CI_GROUNDED_PLANNER_REGISTRATION_CONFIG'
WRITES_ENV='CI_GROUNDED_PLANNER_DRAFT_IMPORT_ENABLED'
SESSION_KEY='ci_grounded_planner_registration_v1'


def target_from_environment():
    path=Path(os.environ[CONFIG_ENV])
    if not path.is_absolute() or path.stat().st_size>8192:raise ConsoleError('GROUNDED_CONFIG_PATH')
    return GroundedPlannerTarget(**json.loads(path.read_text()))


class GroundedRegistrationSession(RegistrationSession):
    def __init__(self,target):
        super().__init__();self.target=target;self.review_store=GroundedReviewStore()

    def prepare_exact(self,operator,request,headers,identity,settings):
        self._identity(identity,settings)
        if settings.workspace_id is None or self.command is not None:raise ConsoleError('CONSOLE_REVIEW_UNAVAILABLE')
        command=operator.prepare(headers,workspace_id=settings.workspace_id,request=request)
        if command.workspace_id!=settings.workspace_id or command.approved_preview.request.vertex_request!=request or command.approved_preview.request.proposed_by!=identity.principal.principal_id:
            raise ConsoleError('CONSOLE_PREVIEW_SCOPE_INVALID')
        self.command=command;self.principal_id=identity.principal.principal_id
        self.expires_at=datetime.now(timezone.utc)+REVIEW_LIFETIME


@contextmanager
def console_operator(session,settings,*,enabled):
    with live_operator({},iap_settings=settings.iap,execution_enabled=enabled) as base:
        t=session.target
        source=base.preview_service.registry_reader.read_exact(t.settings_prompt_id,t.settings_version_id).state.head.snapshot
        operator=GroundedPlannerRegistrationOperator(target=t,settings_snapshot=source,review_store=session.review_store,
            preview_service=base.preview_service,command_service=base.command_service,identity_adapter=base.identity_adapter)
        request=VertexPromptReadRequest(KEY,PromptTask.PLANNER,t.prompt_id,t.version_id,'campaign-draft-v1',source.safety_policy_version)
        yield operator,request


def render(st,headers,identity,settings):
    target=target_from_environment();session=st.session_state.get(SESSION_KEY)
    if session is None:
        session=GroundedRegistrationSession(target);st.session_state[SESSION_KEY]=session
    if type(session) is not GroundedRegistrationSession or session.target!=target:raise ConsoleError('CONSOLE_SESSION_CONFIGURATION_CHANGED')
    session._identity(identity,settings)
    scoped=replace(settings,writes_enabled=os.environ.get(WRITES_ENV)=='true')
    st.caption('Exact brand-neutral planner DRAFT registration only. Model execution, activation and release remain disabled.')
    if st.button('Prepare exact-version DRAFT review',disabled=session.command is not None):
        with operation_scope(OperationSettings(120)),console_operator(session,scoped,enabled=False) as (operator,request):
            session.prepare_exact(operator,request,headers,identity,scoped)
    if session.command is None:return
    st.json(review_projection(session.command.approved_preview,authorized=True))
    with st.form('confirm_grounded_planner'):
        confirmed=st.text_input('Enter the reviewed preview SHA-256')
        submit=st.form_submit_button('Register as DRAFT',disabled=not scoped.writes_enabled)
    if submit:
        if not scoped.writes_enabled:raise ConsoleError('CONSOLE_WRITES_DISABLED')
        with operation_scope(OperationSettings(120)),console_operator(session,scoped,enabled=True) as (operator,request):
            if request!=session.command.approved_preview.request.vertex_request:raise ConsoleError('CONSOLE_PREVIEW_SCOPE_INVALID')
            result=session.submit(operator,headers,identity,scoped,confirmed_sha256=confirmed,now=datetime.now(timezone.utc))
        st.success('DRAFT registration verified in BigQuery');st.json(result)
