"""4.4 server IPC projections; no authentication, dispatch or commit proof."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
import re

from src.application.access_policy import Visibility
from src.application.identity import IapSettings
from src.application.operation_channel import ChannelError, encode
from src.application.sharing_coordinator_contract import SharingCommitObservation, SharingCommitStatus
from src.application.sharing_executor import (
    SharingExecutorSettings, SharingExecutionOutcome, SharingRetryIssuance, SharingRetryIssuanceState,
)
from src.application.sharing_query import SharingQueryResult, SharingResultState
from src.application.sharing_runtime import SigningRuntimeSettings
from src.application.sharing_refresh_contract import SigningRefreshSettings
from src.application.operation_budget import OperationSettings

METHODS = frozenset(('issue_intent', 'submit', 'recover', 'issue_retry', 'submit_retry',
                     'recover_retry', 'recover_receipt'))
INTENT_FIELDS = frozenset(('workspace_id', 'conversation_id', 'visibility',
    'expected_membership_revision', 'expected_conversation_revision',
    'expected_workspace_revision', 'retry_key'))


def require(value):
    if not value:
        raise ChannelError()


@dataclass(frozen=True, slots=True)
class SharingWorkerConfig:
    iap: IapSettings
    runtime: SigningRuntimeSettings
    executor: SharingExecutorSettings = SharingExecutorSettings()

    def __post_init__(self):
        if (type(self.iap) is not IapSettings or type(self.runtime) is not SigningRuntimeSettings
                or type(self.executor) is not SharingExecutorSettings):
            raise ValueError('Typed server worker configuration is required')


def config_from_wire(value):
    require(type(value) is dict and set(value) == {'iap','runtime','executor'})
    runtime = dict(value['runtime'])
    runtime['refresh'] = SigningRefreshSettings(**runtime['refresh'])
    runtime['operation'] = OperationSettings(**runtime['operation'])
    return SharingWorkerConfig(IapSettings(**value['iap']), SigningRuntimeSettings(**runtime),
                               SharingExecutorSettings(**value['executor']))


def request_values(method, headers, parameters):
    require(method in METHODS and isinstance(headers, Mapping) and len(headers) <= 32
            and type(parameters) is dict)
    require(all(type(k) is str and 1 <= len(k) <= 128 and type(v) is str
                and len(v) <= 16384 for k,v in headers.items()))
    if method == 'issue_intent':
        require(set(parameters) == INTENT_FIELDS)
        parameters = dict(parameters)
        visibility = parameters['visibility']
        if isinstance(visibility, Visibility):
            parameters['visibility'] = visibility.value
        require(parameters['visibility'] in ('PRIVATE','TEAM'))
        for key in ('workspace_id','conversation_id','retry_key'):
            require(type(parameters[key]) is str and 1 <= len(parameters[key]) <= 128)
        for key in ('expected_membership_revision','expected_conversation_revision','expected_workspace_revision'):
            require(type(parameters[key]) is int and 1 <= parameters[key] < 2**63)
    else:
        require(set(parameters) == {'token'} and type(parameters['token']) is str
                and 1 <= len(parameters['token']) <= 8192)
    value = dict(method=method, headers=dict(headers), parameters=parameters)
    encode(value)  # Bound the combined UTF-8 projection before any worker I/O.
    return value


def _result(value):
    if value is None:
        return None
    require(type(value) is SharingQueryResult)
    return dict(state=value.state.value, audit_id=value.audit_id,
                observed_at=value.observed_at.isoformat(), resulting_revision=value.resulting_revision)


def pack_result(method, value):
    if method == 'issue_intent':
        require(type(value) is str and len(value) <= 8192)
        return {'token':value}
    if method == 'recover_receipt':
        return _result(value)
    if method == 'issue_retry':
        require(type(value) is SharingRetryIssuance)
        return dict(state=value.state.value, token=value.token, result=_result(value.result))
    require(type(value) is SharingExecutionOutcome)
    return dict(observation=asdict(value.observation), result=_result(value.result))


def _unpack_result(value):
    if value is None:
        return None
    require(type(value) is dict and set(value) == {'state','audit_id','observed_at','resulting_revision'})
    require(type(value['audit_id']) is str and re.fullmatch(r'[^\s\x00-\x1f\x7f]{1,128}',value['audit_id']))
    timestamp = datetime.fromisoformat(value['observed_at'])
    require(timestamp.tzinfo is not None and timestamp.utcoffset() is not None)
    revision = value['resulting_revision']
    require(revision is None or type(revision) is int and 1 <= revision < 2**63)
    state = SharingResultState(value['state'])
    require((state is SharingResultState.UNKNOWN) == (revision is None))
    return SharingQueryResult(state, value['audit_id'], timestamp, revision)


def unpack_result(method, value):
    require(type(value) is dict)
    if method == 'issue_intent':
        require(set(value) == {'token'} and type(value['token']) is str and 1 <= len(value['token']) <= 8192)
        return value['token']
    if method == 'recover_receipt':
        return _unpack_result(value)
    if method == 'issue_retry':
        require(set(value) == {'state','token','result'})
        return SharingRetryIssuance(SharingRetryIssuanceState(value['state']), value['token'],
                                   _unpack_result(value['result']))
    require(set(value) == {'observation','result'} and type(value['observation']) is dict)
    observation = dict(value['observation'])
    require(set(observation) == {'audit_id','status','query_job_id'})
    observation['status'] = SharingCommitStatus(observation['status'])
    return SharingExecutionOutcome(SharingCommitObservation(**observation), _unpack_result(value['result']))
