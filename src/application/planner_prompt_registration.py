"""Batch 13 High/Must have: exact planner review and authorized DRAFT import.

Run without --apply for a read-only inspection. ADC grants infrastructure access,
never application roles. Registration requires a real IAP assertion, fresh ADMIN
membership, and confirmation of a freshly prepared hash in this process. Exported
reviews cannot be imported as approvals, runtime prompts or recovery state.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, ExitStack
from dataclasses import asdict
from datetime import datetime, timezone
import getpass
import json
from pathlib import Path
import re

import agentplatform
import google.auth
from google.cloud import bigquery

from src.application.access_policy import Action, AccessDecision, POLICY_VERSION, Reason
from src.application.access_reader import BigQueryAccessReader
from src.application.identity import ASSERTION_HEADER, IapIdentityAdapter, IapSettings, ensure_fresh
from src.application.operation_budget import OperationSettings, operation_scope
from src.application.operation_transport import (
    OperationAuthorizedSession, OperationRequest, OperationSession, SCOPES,
)
from src.application.planner_prompt_parity import (
    PLANNER_OUTPUT_SCHEMA_VERSION, PLANNER_PROMPT_KEY,
    PLANNER_SAFETY_POLICY_VERSION, PlannerParityStatus, compare_planner_prompt,
)
from src.application.prompt_contract import PromptTask, VERTEX_PROJECT
from src.application.prompt_import_preview import (
    PromptImportPreviewRequest, PromptImportPreviewService,
)
from src.application.prompt_registration_command import (
    PromptRegistrationCommand, PromptRegistrationCommandError,
    PromptRegistrationCommandService,
)
from src.application.prompt_registry_executor import (
    PromptRegistryExecutor, PromptRegistryExecutorSettings,
)
from src.application.prompt_registry_reader import BigQueryPromptRegistryReader
from src.application.vertex_prompt_reader import (
    AgentPlatformPromptVersionTransport, VertexPromptReadRequest,
)
from src.application.workflow_schema import LOCATION

INITIAL_PROMPT_ID = '8077808464061005824'
INITIAL_VERSION_ID = '1'


def planner_request(prompt_id: str, version_id: str) -> VertexPromptReadRequest:
    return VertexPromptReadRequest(PLANNER_PROMPT_KEY, PromptTask.PLANNER,
        prompt_id, version_id, PLANNER_OUTPUT_SCHEMA_VERSION,
        PLANNER_SAFETY_POLICY_VERSION)


def review_projection(preview, *, authorized: bool) -> dict:
    """Complete canonical review, with bounded diffs and no identity assertions."""
    snapshot = preview.candidate
    return {
        'event': 'planner_prompt_registration_review',
        'registration_authorized': authorized,
        'status': preview.status.value,
        'preview_sha256': preview.preview_sha256,
        'previewed_at': preview.request.previewed_at.isoformat(),
        'snapshot_sha256': snapshot.snapshot_sha256,
        'template_sha256': snapshot.template_sha256,
        'vertex_project': snapshot.vertex_project,
        'vertex_location': snapshot.vertex_location,
        'vertex_prompt_id': snapshot.vertex_prompt_id,
        'vertex_version_id': snapshot.vertex_version_id,
        'prompt_key': snapshot.prompt_key,
        'task': snapshot.task.value,
        'model_id': snapshot.model_id,
        'system_instruction': snapshot.system_instruction,
        'prompt_template': snapshot.prompt_template,
        'required_variables': list(snapshot.required_variables),
        'generation_config': json.loads(snapshot.generation_config_json),
        'safety_settings': json.loads(snapshot.safety_settings_json),
        'output_schema': json.loads(snapshot.output_schema_json),
        'output_schema_version': snapshot.output_schema_version,
        'safety_policy_version': snapshot.safety_policy_version,
        'differences': [asdict(item) for item in preview.differences],
        'current_revision': None if preview.exact_state is None
            else preview.exact_state.head.revision,
        'active_snapshot_sha256': None if preview.active_state is None
            else preview.active_state.head.snapshot.snapshot_sha256,
        'proposed_status': None if not authorized or preview.proposal is None
            else preview.proposal.head.status.value,
        'mutation_performed': False,
    }


class PlannerPromptRegistrationOperator:
    def __init__(self, *, preview_service, command_service, identity_adapter,
                 config: dict):
        if command_service.preview_service is not preview_service:
            raise ValueError('One trusted preview service is required')
        self.preview_service = preview_service
        self.command_service = command_service
        self.identity_adapter = identity_adapter
        self.config = config

    def _preview(self, request, proposed_by):
        preview = self.preview_service.preview(PromptImportPreviewRequest(
            request, datetime.now(timezone.utc), proposed_by))
        if compare_planner_prompt(preview.candidate, self.config).status \
                is not PlannerParityStatus.PASS:
            raise PromptRegistrationCommandError('PLANNER_PROMPT_PARITY_FAILED')
        return preview

    def inspect(self, request):
        # This marker is never a verified principal or a write authorization.
        return self._preview(request, 'unauthenticated-read-only-inspection')

    def _authorize(self, identity, workspace_id):
        ensure_fresh(identity)
        decision = self.command_service.access_reader.authorize(
            identity, Action.MANAGE_MODELS, workspace_id=workspace_id)
        if (not isinstance(decision, AccessDecision) or not decision.allowed
                or decision.action is not Action.MANAGE_MODELS
                or decision.reason is not Reason.ALLOWED_WORKSPACE_ACTION
                or decision.workspace_id != workspace_id
                or decision.principal_id != identity.principal.principal_id
                or type(decision.membership_revision) is not int
                or decision.membership_revision < 1
                or decision.resource_id is not None
                or decision.resource_revision is not None
                or decision.policy_version != POLICY_VERSION):
            raise PromptRegistrationCommandError('PROMPT_REGISTRATION_ACCESS_INVALID')
        return decision

    def prepare(self, headers, *, workspace_id, request):
        identity = self.identity_adapter.authenticate(headers)
        before = self._authorize(identity, workspace_id)
        preview = self._preview(request, identity.principal.principal_id)
        after = self._authorize(identity, workspace_id)
        if after.membership_revision < before.membership_revision:
            raise PromptRegistrationCommandError('PROMPT_REGISTRATION_ACCESS_REGRESSION')
        return PromptRegistrationCommand(workspace_id, preview, preview.preview_sha256)

    def register(self, headers, command, *, confirmed_sha256):
        if (not isinstance(command, PromptRegistrationCommand)
                or type(confirmed_sha256) is not str
                or confirmed_sha256 != command.approved_preview_sha256):
            raise PromptRegistrationCommandError('PROMPT_REGISTRATION_REVIEW_NOT_CONFIRMED')
        identity = self.identity_adapter.authenticate(headers)
        return self.command_service.execute(identity, command)


@contextmanager
def live_operator(config, *, iap_settings=None, execution_enabled=False):
    """Fixed project/region ADC composition; does not bootstrap access or IAM."""
    with ExitStack() as stack:
        discovery = stack.enter_context(OperationSession())
        credentials, _ = google.auth.default(scopes=SCOPES,
            request=OperationRequest(discovery))
        session = stack.enter_context(OperationAuthorizedSession(credentials))
        client = bigquery.Client(project=VERTEX_PROJECT, location=LOCATION,
            credentials=credentials, _http=session)
        stack.callback(client.close)
        vertex = agentplatform.Client(project=VERTEX_PROJECT, location=LOCATION,
            credentials=credentials)
        reader = BigQueryPromptRegistryReader(client)
        preview = PromptImportPreviewService(
            AgentPlatformPromptVersionTransport(vertex), reader)
        command = PromptRegistrationCommandService(
            access_reader=BigQueryAccessReader(client), preview_service=preview,
            executor=PromptRegistryExecutor(client, PromptRegistryExecutorSettings(
                execution_enabled=execution_enabled)), registry_reader=reader)
        yield PlannerPromptRegistrationOperator(preview_service=preview,
            command_service=command, config=config,
            identity_adapter=None if iap_settings is None else IapIdentityAdapter(iap_settings))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prompt-id', default=INITIAL_PROMPT_ID)
    parser.add_argument('--version-id', default=INITIAL_VERSION_ID)
    parser.add_argument('--config', type=Path,
        default=Path('config/campaign_brief_generator.json'))
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--workspace-id')
    parser.add_argument('--review-file', type=Path,
        help='Optional review export only; never read back as approval or state')
    parser.add_argument('--timeout-seconds', type=int, default=120)
    args = parser.parse_args()
    try:
        settings = OperationSettings(args.timeout_seconds)
        request = planner_request(args.prompt_id, args.version_id)
        try:
            iap = IapSettings.from_environment() if args.apply else None
        except ValueError:
            raise PromptRegistrationCommandError(
                'PROMPT_REGISTRATION_IAP_NOT_CONFIGURED') from None
        if args.apply and not args.workspace_id:
            raise PromptRegistrationCommandError('PROMPT_REGISTRATION_WORKSPACE_REQUIRED')
        config = json.loads(args.config.read_text())
        headers = {ASSERTION_HEADER: getpass.getpass('IAP assertion: ')} \
            if args.apply else None
        with operation_scope(settings), live_operator(config,
                iap_settings=iap, execution_enabled=args.apply) as operator:
            if args.apply:
                # No credential arguments, exports, logs, or ADC-to-user fallback.
                command = operator.prepare(headers, workspace_id=args.workspace_id,
                    request=request)
                preview = command.approved_preview
            else:
                preview = operator.inspect(request)
            review = review_projection(preview, authorized=args.apply)
            encoded = json.dumps(review, indent=2, sort_keys=True)
            print(encoded)
            if args.review_file is not None:
                args.review_file.write_text(encoded + '\n')
        if args.apply:
            # Human review time is outside the provider operation budget.
            confirmed = input('Confirm the reviewed preview SHA-256: ').strip()
            if confirmed != command.approved_preview_sha256:
                raise PromptRegistrationCommandError(
                    'PROMPT_REGISTRATION_REVIEW_NOT_CONFIRMED')
            headers = {ASSERTION_HEADER: getpass.getpass('Fresh IAP assertion: ')}
            # Retain the exact server-held command; compose fresh cloud clients.
            with operation_scope(settings), live_operator(config,
                    iap_settings=iap, execution_enabled=True) as operator:
                result = operator.register(headers, command, confirmed_sha256=confirmed)
            print(json.dumps({
                'event': 'planner_prompt_draft_verified',
                'native_job_id': result.native_job_id,
                'registration_state': result.registration_state.value,
                'snapshot_sha256': result.registered.state.head.snapshot.snapshot_sha256,
                'record_digest': result.registered.state.head.digest,
                'status': result.registered.state.head.status.value,
                'revision': result.registered.state.head.revision,
                'history_rows': len(result.registered.state.history),
                'recovered_existing_state': result.recovered_existing_state,
            }, sort_keys=True))
    except Exception as error:
        code = getattr(error, 'code', 'PROMPT_REGISTRATION_OPERATOR_UNAVAILABLE')
        if type(code) is not str or re.fullmatch(r'[A-Z][A-Z0-9_]{0,95}', code) is None:
            code = 'PROMPT_REGISTRATION_OPERATOR_UNAVAILABLE'
        print(json.dumps({'event': 'planner_prompt_registration_failed',
            'error_code': code, 'mutation_retried': False}, sort_keys=True))
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
