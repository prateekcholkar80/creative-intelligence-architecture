"""Priority 4.7 history reads over the owned-process supervisor."""
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
import base64
import binascii
import re
import sys

from src.application.identity import IapSettings
from src.application.operation_budget import OperationSettings
from src.application.operation_channel import encode
from src.application.sharing_host_contract import require
from src.application.sharing_operation_host import (
    SharingHostError, SharingHostSettings, SharingOperationHost,
)
from src.application.workflow_contract import JobState
from src.application.workflow_history_service import (
    ConversationHistoryPage, HistoryJob, HistorySettings, MAX_PAGE_SIZE,
)
from src.retrieval.request import CANONICAL_SKUS

METHODS = frozenset({'list_jobs'})
_ID = r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}'


@dataclass(frozen=True, slots=True)
class WorkflowHistoryConfig:
    iap: IapSettings
    history: HistorySettings = HistorySettings()

    def __post_init__(self):
        if type(self.iap) is not IapSettings or type(self.history) is not HistorySettings:
            raise ValueError('Typed trusted history configuration required')


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b'=').decode('ascii')


def config_from_wire(value):
    require(type(value) is dict and set(value) == {'iap', 'history'}
        and type(value['history']) is dict)
    raw = dict(value['history'])
    key = raw.get('cursor_key')
    try:
        require(type(key) is str and len(key) <= 128)
        decoded = base64.b64decode(key + '=' * (-len(key) % 4), altchars=b'-_', validate=True)
        require(_b64(decoded) == key)
    except (ValueError, TypeError, binascii.Error):
        require(False)
    raw['cursor_key'] = decoded
    raw['operation'] = OperationSettings(**raw['operation'])
    return WorkflowHistoryConfig(IapSettings(**value['iap']), HistorySettings(**raw))


def request_values(method, headers, parameters):
    require(method in METHODS and isinstance(headers, Mapping) and len(headers) <= 32
        and all(type(k) is str and 1 <= len(k) <= 128 and type(v) is str
                and len(v) <= 16384 for k, v in headers.items())
        and type(parameters) is dict
        and {'workspace_id', 'conversation_id', 'page_size'} <= set(parameters)
        <= {'workspace_id', 'conversation_id', 'page_size', 'cursor'})
    require(all(type(parameters[name]) is str and re.fullmatch(_ID, parameters[name])
        for name in ('workspace_id', 'conversation_id')))
    require(type(parameters['page_size']) is int
        and 1 <= parameters['page_size'] <= MAX_PAGE_SIZE)
    cursor = parameters.get('cursor')
    require(cursor is None or type(cursor) is str and 1 <= len(cursor) <= 2048
        and re.fullmatch(r'[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', cursor))
    values = dict(method=method, headers=dict(headers), parameters=dict(parameters))
    encode(values)
    return values


def _time(value):
    try:
        result = datetime.fromisoformat(value)
        require(result.tzinfo is not None and result.utcoffset() is not None)
        return result
    except (ValueError, TypeError):
        require(False)


class WorkflowHistoryHost(SharingOperationHost):
    """Prestarted read worker; malformed or uncertain IPC retires the host."""

    def __init__(self, config, settings=SharingHostSettings()):
        super().__init__(config, settings)

    def _valid_config(self, config):
        return type(config) is WorkflowHistoryConfig

    @property
    def operation_settings(self):
        return self.config.history.operation

    def _enabled(self):
        if not self.config.history.enabled or not self.settings.enabled:
            raise SharingHostError('SHARING_HOST_DISABLED')
        if self.retired:
            raise SharingHostError('SHARING_HOST_RETIRED')

    def _command(self, fd):
        return [sys.executable, '-m', 'src.application.workflow_history_worker', str(fd)]

    def _config_values(self):
        value = asdict(self.config)
        value['history']['cursor_key'] = _b64(self.config.history.cursor_key)
        return value

    def _request_values(self, method, headers, parameters):
        return request_values(method, headers, parameters)

    def _decode_result(self, method, value):
        require(type(value) is dict and set(value) == {
            'workspace_id', 'conversation_id', 'visibility', 'access_revision',
            'conversation_created_at', 'conversation_updated_at', 'jobs', 'next_cursor'})
        require(type(value['workspace_id']) is str and re.fullmatch(_ID, value['workspace_id'])
            and type(value['conversation_id']) is str and re.fullmatch(_ID, value['conversation_id'])
            and value['visibility'] in {'PRIVATE', 'TEAM'}
            and type(value['access_revision']) is int and 1 <= value['access_revision'] < 2**63
            and type(value['jobs']) is list and len(value['jobs']) <= MAX_PAGE_SIZE)
        created = _time(value['conversation_created_at'])
        updated = _time(value['conversation_updated_at'])
        require(created <= updated)
        cursor = value['next_cursor']
        require(cursor is None or type(cursor) is str and 1 <= len(cursor) <= 2048
            and re.fullmatch(r'[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', cursor))
        jobs = []
        order = []
        for item in value['jobs']:
            require(type(item) is dict and set(item) == set(HistoryJob.__dataclass_fields__))
            for name in ('job_id', 'request_id'):
                require(type(item[name]) is str and re.fullmatch(_ID, item[name]))
            require(item['canonical_sku'] in CANONICAL_SKUS
                and item['state'] in {state.value for state in JobState}
                and type(item['revision']) is int and 1 <= item['revision'] < 2**63
                and (item['output_id'] is None or type(item['output_id']) is str
                     and re.fullmatch(_ID, item['output_id']))
                and (item['safe_error_code'] is None or type(item['safe_error_code']) is str
                     and re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}', item['safe_error_code'])))
            job_created, job_updated = _time(item['created_at']), _time(item['updated_at'])
            require(job_created <= job_updated)
            if item['state'] == 'SUCCEEDED':
                require(item['output_id'] is not None and item['safe_error_code'] is None)
            elif item['state'] in {'BLOCKED', 'FAILED', 'RECOVERY_REQUIRED'}:
                require(item['output_id'] is None and item['safe_error_code'] is not None)
            else:
                require(item['output_id'] is None and item['safe_error_code'] is None)
            order.append((job_created, item['job_id']))
            jobs.append(HistoryJob(**(dict(item) | {
                'created_at': job_created, 'updated_at': job_updated})))
        require(len(set(order)) == len(order) and order == sorted(order, reverse=True))
        return ConversationHistoryPage(value['workspace_id'], value['conversation_id'],
            value['visibility'], value['access_revision'], created, updated,
            tuple(jobs), cursor)

    def _validate_result_scope(self, method, parameters, result):
        require(result.workspace_id == parameters['workspace_id']
            and result.conversation_id == parameters['conversation_id']
            and len(result.jobs) <= parameters['page_size'])

    def list_jobs(self, headers, *, cancellation=None, **parameters):
        return self._invoke('list_jobs', headers, parameters, cancellation)
