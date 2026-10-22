"""Bounded owned-host evidence transport. Withheld text never crosses IPC."""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
import json

from src.application.campaign_acceptance_contract import Stage, Decision, CheckState
from src.application.campaign_chat_evidence_service import EvidenceChatReadiness
from src.application.campaign_evidence_dispatch import EvidenceInvocationDraft
from src.application.campaign_quality_checks import QualityCandidate, QualityReport, RuleCheck, FactSpan, SourceFact
from src.application.campaign_quality_execution import QualityExecutionResult
from src.application.campaign_question_view import QuestionTurn
from src.application.operation_channel import encode
from src.application.workflow_contract import _require
from src.application.workflow_invocation_result_query import ModelResultObservation


@dataclass(frozen=True, slots=True)
class EvidenceChatResult:
    workspace_id: str
    conversation_id: str
    readiness: EvidenceChatReadiness
    turn: QuestionTurn | None


def _json(value):
    if isinstance(value, datetime): return value.isoformat()
    if isinstance(value, Enum): return value.value
    if isinstance(value, Decimal): return str(value)
    raise ValueError('QUESTION_TRANSPORT_VALUE')


def _shape(value, cls):
    _require(type(value) is dict and set(value) == set(cls.__dataclass_fields__), 'QUESTION_TRANSPORT_SHAPE')
    return dict(value)


def encode_result(workspace_id, conversation_id, readiness, turn, *, now=None):
    _require(type(readiness) is EvidenceChatReadiness, 'QUESTION_TRANSPORT_READINESS')
    value = None
    if turn is not None:
        _require(type(turn) is QuestionTurn, 'QUESTION_TRANSPORT_TURN')
        turn.validate(workspace_id, conversation_id, now or datetime.now(timezone.utc))
        if readiness.release_allowed and turn.quality.release_allowed and turn.quality.report.decision is Decision.PASS:
            value = json.loads(json.dumps(asdict(turn), default=_json, allow_nan=False))
    result = dict(workspace_id=workspace_id, conversation_id=conversation_id,
        readiness=asdict(readiness), turn=value)
    # Includes protocol envelope overhead; never increase the shared frame limit.
    encode(dict(id='0'*32, result=result))
    return result


def decode_result(value, *, now=None):
    raw = _shape(value, EvidenceChatResult)
    ready = _shape(raw['readiness'], EvidenceChatReadiness)
    _require(type(ready['reason_codes']) in (list,tuple) and len(ready['reason_codes']) <= 32
        and all(type(s) is str and 0 < len(s) <= 128 for s in ready['reason_codes'])
        and type(ready['prompt_statuses']) in (list,tuple) and len(ready['prompt_statuses']) <= 2
        and all(type(pair) in (list,tuple) and len(pair) == 2
            and all(type(s) is str and 0 < len(s) <= 128 for s in pair) for pair in ready['prompt_statuses']),
        'QUESTION_TRANSPORT_READINESS')
    readiness = EvidenceChatReadiness(tuple(ready['reason_codes']),tuple(tuple(p) for p in ready['prompt_statuses']))
    turn = None
    if raw['turn'] is not None:
        # No caller-supplied release boolean or unsupported approval contract.
        _require(readiness.release_allowed, 'QUESTION_TRANSPORT_RELEASE_WITHHELD')
        fields = _shape(raw['turn'], QuestionTurn)
        draft = _shape(fields['draft'], EvidenceInvocationDraft)
        observation = _shape(draft['observation'], ModelResultObservation)
        if observation['estimated_cost'] is not None:
            observation['estimated_cost'] = Decimal(observation['estimated_cost'])
        draft['observation'] = ModelResultObservation(**observation)
        candidate = _shape(fields['candidate'], QualityCandidate)
        candidate['stage'] = Stage(candidate['stage'])
        candidate['spans'] = tuple(FactSpan(**_shape(s,FactSpan)) for s in candidate['spans'])
        quality = _shape(fields['quality'], QualityExecutionResult)
        report = _shape(quality['report'], QualityReport)
        report['checked_at'] = datetime.fromisoformat(report['checked_at'])
        report['not_before'] = datetime.fromisoformat(report['not_before'])
        report['decision'] = Decision(report['decision'])
        report['checks'] = tuple(RuleCheck(c['rule_id'],CheckState(c['state']),c['reason_code'])
            for c in (_shape(c,RuleCheck) for c in report['checks']))
        quality['report'] = QualityReport(**report)
        sources = []
        for source in fields['sources']:
            source = _shape(source,SourceFact)
            for key in ('observed_at','valid_until'): source[key] = datetime.fromisoformat(source[key])
            sources.append(SourceFact(**source))
        turn = QuestionTurn(EvidenceInvocationDraft(**draft),QualityCandidate(**candidate),
            QualityExecutionResult(**quality),datetime.fromisoformat(fields['expires_at']),tuple(sources))
        turn.validate(raw['workspace_id'],raw['conversation_id'],now or datetime.now(timezone.utc))
        _require(turn.quality.release_allowed and turn.quality.report.decision is Decision.PASS,
            'QUESTION_TRANSPORT_RELEASE_WITHHELD')
    return EvidenceChatResult(raw['workspace_id'],raw['conversation_id'],readiness,turn)
