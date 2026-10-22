"""Opt-in E2 diagnostic execution with fresh policy/prompt/access context.

No generation, dispatch, serving activation or release permission. Installed
server settings/readers are trusted dependencies, never browser-supplied values.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json

from src.application.campaign_acceptance_contract import Stage, _utc, _hash
from src.application.campaign_policy_reader import read_policy, DraftPolicySnapshot
from src.application.campaign_quality_checks import QualityCandidate, QualityReport, check_candidate
from src.application.campaign_quality_product_reader import SKUS, _access, read_product_fact
from src.application.campaign_quality_request_reader import RequestContextRead, read_request_context, MAX_REQUESTS
from src.application.prompt_contract import PromptTask
from src.application.prompt_runtime_contract import RuntimeBehavior, validate_runtime_eligibility, validate_runtime_profile
from src.application.prompt_runtime_reader import RuntimeReadRequest
from src.application.workflow_contract import _require, _id, _digest, canonical_json_bytes, validate_profile


@dataclass(frozen=True, slots=True)
class QualityRequest:
    workspace_id: str
    conversation_id: str
    request_id: str
    sku: str
    stage: Stage
    country: str
    channel: str
    user_input_sha256: str
    conversation_sha256: str

    def __post_init__(self):
        for key in ('workspace_id','conversation_id','request_id'): _id(getattr(self,key))
        _digest(self.user_input_sha256); _digest(self.conversation_sha256)
        _require(self.sku in SKUS and isinstance(self.stage,Stage)
            and self.country in {'US','CA','GB','AU'}
            and self.channel in {'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'},'INVALID_QUALITY_REQUEST')

    def payload(self):
        return {key:(value.value if key=='stage' else value)
            for key in self.__dataclass_fields__ for value in (getattr(self,key),)}


@dataclass(frozen=True, slots=True)
class QualityExecutionSettings:
    policy_id: str
    policy_version: str
    policy_specification_sha256: str
    policy_header_sha256: str
    behavior: RuntimeBehavior
    model_profile_json: str
    approved_profiles: frozenset[str]

    def __post_init__(self):
        _id(self.policy_id); _id(self.policy_version)
        _digest(self.policy_specification_sha256); _digest(self.policy_header_sha256)
        _require(type(self.behavior) is RuntimeBehavior and type(self.model_profile_json) is str
            and len(self.model_profile_json.encode())<=16384,'INVALID_QUALITY_EXECUTION_SETTINGS')
        profile=json.loads(self.model_profile_json)
        _require(type(profile) is dict and canonical_json_bytes(profile).decode()==self.model_profile_json
            and type(self.approved_profiles) is frozenset and 0<len(self.approved_profiles)<=32,
            'INVALID_QUALITY_EXECUTION_SETTINGS')
        validate_profile(profile)
        for digest in self.approved_profiles: _digest(digest)


@dataclass(frozen=True, slots=True)
class QualityExecutionResult:
    report: QualityReport
    context_canonical_json: str
    context_sha256: str

    @property
    def release_allowed(self):
        # The only policy reader currently supported is DRAFT-storage-v1.
        return False


def _request_capture(request, *, client, identity, started, clock):
    """Validate the trusted reader boundary; never substitute caller-held history."""
    capture=read_request_context(client,workspace_id=request.workspace_id,
        conversation_id=request.conversation_id,request_id=request.request_id,identity=identity,clock=clock)
    _require(type(capture) is RequestContextRead and type(capture.bindings_canonical_json) is str
        and len(capture.bindings_canonical_json.encode())<=65536,'QUALITY_REQUEST_CAPTURE_REQUIRED')
    try:
        pins=json.loads(capture.bindings_canonical_json)
    except (ValueError,RecursionError):
        _require(False,'QUALITY_REQUEST_CAPTURE_REQUIRED')
    keys={'version','workspace_id','conversation_id','request_id','principal_id','access_sha256',
        'request_sha256','message_sha256','request_record_sha256','request_revision','operation',
        'request_status','canonical_sku','channel','history_kind','history_count','history_sha256'}
    _require(type(pins) is dict and set(pins)==keys
        and canonical_json_bytes(pins).decode()==capture.bindings_canonical_json
        and _hash(pins)==capture.bindings_sha256,'QUALITY_REQUEST_CAPTURE_REQUIRED')
    _require(pins['version']=='campaign-quality-request-bindings-v1'
        and pins['workspace_id']==request.workspace_id and pins['conversation_id']==request.conversation_id
        and pins['request_id']==request.request_id and pins['principal_id']==identity.principal.principal_id,
        'QUALITY_REQUEST_CAPTURE_SCOPE_MISMATCH')
    for key in ('access_sha256','request_sha256','request_record_sha256','history_sha256'):
        _digest(pins[key])
    if pins['message_sha256'] is not None: _digest(pins['message_sha256'])
    _require(pins['history_kind']=='PERSISTED_REQUEST_RECORDS'
        and type(pins['history_count']) is int and 1<=pins['history_count']<=MAX_REQUESTS
        and type(pins['request_revision']) is int and pins['request_revision']>0,
        'QUALITY_REQUEST_HISTORY_COVERAGE_UNSUPPORTED')
    _id(capture.native_job_id)
    now=_utc(clock())
    _require(started<=_utc(capture.observed_at)<=now<_utc(capture.valid_until)
        <=capture.observed_at+timedelta(seconds=60),'QUALITY_REQUEST_CAPTURE_STALE')
    return capture,pins


def _bind_request(request,pins):
    expected_operation='GENERATE' if request.stage is Stage.GENERATION else 'INTERPRET'
    _require(request.stage is not Stage.REFINEMENT and pins['operation']==expected_operation,
        'QUALITY_REQUEST_STAGE_UNSUPPORTED')
    expected_hash=pins['request_sha256'] if expected_operation=='GENERATE' else pins['message_sha256']
    _require(expected_hash is not None and request.user_input_sha256==expected_hash,
        'QUALITY_REQUEST_INPUT_MISMATCH')
    _require(request.conversation_sha256==pins['history_sha256'],'QUALITY_REQUEST_HISTORY_MISMATCH')
    # An unresolved interpretation has no authoritative selected SKU/channel.
    _require(pins['canonical_sku']==request.sku,'QUALITY_REQUEST_SKU_UNBOUND')
    if expected_operation=='GENERATE':
        _require(pins['request_status']=='READY' and pins['message_sha256'] is None,
            'QUALITY_REQUEST_STATE_UNSUPPORTED')
    else:
        _require(pins['request_status'] in {'READY','NEEDS_CLARIFICATION'},'QUALITY_REQUEST_STATE_UNSUPPORTED')
        if request.stage is Stage.CLARIFICATION:
            _require(pins['request_status']=='NEEDS_CLARIFICATION','QUALITY_REQUEST_STATE_UNSUPPORTED')
    family='EMAIL' if request.channel=='KLAVIYO_EMAIL' else 'PAID_SOCIAL'
    _require(pins['channel']==family,'QUALITY_REQUEST_CHANNEL_UNBOUND')


def _context(request, settings, *, client, runtime_reader, identity, access, clock):
    policy=read_policy(client,policy_id=settings.policy_id,version=settings.policy_version,
        specification_sha256=settings.policy_specification_sha256,header_sha256=settings.policy_header_sha256)
    _require(type(policy) is DraftPolicySnapshot and policy.policy_id==settings.policy_id
        and policy.version==settings.policy_version and policy.specification_sha256==settings.policy_specification_sha256
        and policy.header_sha256==settings.policy_header_sha256,'QUALITY_POLICY_CAPTURE_REQUIRED')
    task=PromptTask.INTERPRETER if request.stage in {Stage.INTERPRETATION,Stage.CLARIFICATION} else PromptTask.PLANNER
    _require(settings.behavior.task is task,'QUALITY_PROMPT_STAGE_MISMATCH')
    runtime_request=RuntimeReadRequest(request.workspace_id,request.conversation_id,
        settings.behavior.prompt_key,task,identity,access.membership_revision)
    capture=runtime_reader.read(runtime_request)
    now=_utc(clock())
    # Validate captured access scope even when using an installed reader adapter.
    _require(capture.request.workspace_id==request.workspace_id and capture.request.identity==identity
        and capture.request.membership_revision==access.membership_revision
        and capture.request.prompt_key==runtime_request.prompt_key and capture.request.task is task,
        'QUALITY_RUNTIME_SCOPE_MISMATCH')
    pins=validate_runtime_eligibility(capture,checked_at=now)
    _require(now-timedelta(seconds=60)<=capture.observed_at<=now,'QUALITY_RUNTIME_CAPTURE_STALE')
    validate_runtime_profile(capture.target.head.snapshot,json.loads(settings.model_profile_json),
        settings.behavior,settings.approved_profiles)
    # Native read IDs/observation times may differ on a reread; authoritative
    # content/revisions must not. Do not silently replace the original pins.
    return dict(policy=dict(policy_id=policy.policy_id,version=policy.version,
        specification_sha256=policy.specification_sha256,header_sha256=policy.header_sha256,
        snapshot_sha256=policy.snapshot_sha256,header=policy.header_canonical_json,
        rules_sha256=_hash(list(policy.rule_canonical_json)),serving_ready=False),
        prompt=pins,model_profile_sha256=_hash(json.loads(settings.model_profile_json))),now


def run_current_quality(request, build_candidate, *, settings, client, runtime_reader,
                        access_reader, identity, clock=lambda: datetime.now(timezone.utc)):
    """Read/check/reread context, withholding on changes and always denying release.

    Candidate construction is a trusted server annotation function, not a model
    dispatch callback. Input/history digests must match mandatory native captures.
    GENERATE input means accepted structured input; INTERPRET means stored message
    hash. Conversation means current persisted request records, never transcript.
    No atomic snapshot across the independent readers is claimed.
    """
    _require(type(request) is QualityRequest and type(settings) is QualityExecutionSettings
        and callable(build_candidate),'INVALID_QUALITY_EXECUTION')
    before=_access(access_reader,identity,request.workspace_id,request.conversation_id)
    started=_utc(clock())
    original_request,request_pins=_request_capture(request,client=client,identity=identity,
        started=started,clock=clock)
    _bind_request(request,request_pins)
    initial,initial_at=_context(request,settings,client=client,runtime_reader=runtime_reader,
        identity=identity,access=before,clock=clock)
    _require(started<=initial_at<started+timedelta(seconds=60),'QUALITY_EXECUTION_EXPIRED')
    read=read_product_fact(client,request.sku,clock=clock)
    candidate=build_candidate(read.fact)
    _require(type(candidate) is QualityCandidate and all(getattr(candidate,key)==getattr(request,key)
        for key in ('request_id','sku','stage','country','channel')),'QUALITY_EXECUTION_CANDIDATE_MISMATCH')
    after=_access(access_reader,identity,request.workspace_id,request.conversation_id)
    _require(before==after,'QUALITY_EXECUTION_ACCESS_CHANGED')
    rechecked=_utc(clock())
    _require(started<=rechecked<started+timedelta(seconds=60),'QUALITY_EXECUTION_EXPIRED')
    report=check_candidate(candidate,(read.fact,),checked_at=rechecked,not_before=started)
    current,current_at=_context(request,settings,client=client,runtime_reader=runtime_reader,
        identity=identity,access=after,clock=clock)
    _require(initial==current,'QUALITY_EXECUTION_CONTEXT_CHANGED')
    final_request,final_request_pins=_request_capture(request,client=client,identity=identity,
        started=current_at,clock=clock)
    _require(final_request.observed_at>=original_request.observed_at
        and final_request.native_job_id!=original_request.native_job_id
        and final_request_pins==request_pins,'QUALITY_EXECUTION_REQUEST_CONTEXT_CHANGED')
    # Policy/prompt rereads can take time too; check source and runtime expiry at
    # the final clock and recheck user access after those provider calls.
    final_access=_access(access_reader,identity,request.workspace_id,request.conversation_id)
    _require(final_access==after,'QUALITY_EXECUTION_ACCESS_CHANGED')
    ended=_utc(clock())
    _require(rechecked<=current_at<=ended<started+timedelta(seconds=60)
        and ended<datetime.fromisoformat(current['prompt']['eligible_until'])
        and ended<original_request.valid_until and ended<final_request.valid_until
        and ended<read.fact.valid_until,'QUALITY_EXECUTION_EXPIRED')
    context=dict(version='campaign-quality-execution-v2',request=request.payload(),
        request_bindings=request_pins,request_bindings_sha256=original_request.bindings_sha256,
        request_native_job_ids=[original_request.native_job_id,final_request.native_job_id],
        request_coverage=dict(history='PERSISTED_REQUEST_RECORDS',transcript_authoritative=False,
            workflow_catalog='ORIGINAL_20_SKUS',market_and_exact_platform_authoritative=False,
            channel_family=request_pins['channel'],input_kind='ACCEPTED_STRUCTURED_INPUT'
                if request_pins['operation']=='GENERATE' else 'STORED_MESSAGE_HASH'),
        candidate_sha256=candidate.sha256,evidence_sha256=report.evidence_sha256,
        product_native_job_id=read.native_job_id,report_sha256=report.sha256,
        access=final_access.to_dict(),sources=current,started_at=started.isoformat(),checked_at=ended.isoformat())
    encoded=canonical_json_bytes(context).decode()
    return QualityExecutionResult(report,encoded,_hash(context))
