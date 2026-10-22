"""Priority 4.7 protected submission over the owned-process supervisor."""
from collections.abc import Mapping
from dataclasses import asdict, dataclass
import re
import sys

from src.application.identity import IapSettings
from src.application.operation_budget import OperationSettings
from src.application.operation_channel import encode
from src.application.sharing_host_contract import require
from src.application.sharing_operation_host import (
    SharingHostError, SharingHostSettings, SharingOperationHost,
)
from src.application.sharing_query import SharingQuerySettings
from src.application.workflow_contract import MAX_AUDIENCE_CHARACTERS, PREFERENCE_FIELDS
from src.application.workflow_submission_service import SubmissionReceipt, SubmissionSettings

METHODS = frozenset({'submit','interpret','intake_options','evidence','omission'})


@dataclass(frozen=True, slots=True)
class WorkflowSubmissionConfig:
    iap: IapSettings
    submission: SubmissionSettings = SubmissionSettings()
    interpretation_enabled: bool = False
    interpreter_runtime: dict | None = None

    def __post_init__(self):
        if (type(self.iap) is not IapSettings or type(self.submission) is not SubmissionSettings
                or type(self.interpretation_enabled) is not bool):
            raise ValueError('Typed trusted submission configuration required')
        if self.interpreter_runtime is not None:
            from src.application.interpreter_serving_composition import validate_config
            validate_config(self.interpreter_runtime)


def config_from_wire(value):
    require(type(value) is dict and set(value) == {'iap', 'submission','interpretation_enabled','interpreter_runtime'})
    raw = dict(value['submission'])
    raw['operation'] = OperationSettings(**raw['operation'])
    raw['query'] = SharingQuerySettings(**raw['query'])
    raw['model_profiles'] = tuple(raw['model_profiles'])
    return WorkflowSubmissionConfig(IapSettings(**value['iap']), SubmissionSettings(**raw),value['interpretation_enabled'],value['interpreter_runtime'])


def request_values(method, headers, parameters):
    require(method in METHODS and isinstance(headers, Mapping) and len(headers) <= 32
        and all(type(k) is str and 1 <= len(k) <= 128 and type(v) is str
                and len(v) <= 16384 for k, v in headers.items())
        and type(parameters) is dict
        and set(parameters) == ({'workspace_id','conversation_id','submission_id'} if method=='omission' else {'workspace_id','conversation_id'} if method in {'intake_options','evidence'} else {'workspace_id','conversation_id','message','retry_key','selection'} if method=='interpret'
            else {'workspace_id', 'conversation_id', 'preferences', 'retry_key'}))
    for name in (('workspace_id','conversation_id') if method in {'intake_options','evidence','omission'} else ('workspace_id', 'conversation_id', 'retry_key')):
        require(type(parameters[name]) is str and re.fullmatch(
            r'[A-Za-z0-9][A-Za-z0-9_-]{0,127}', parameters[name]))
    if method=='omission':require(type(parameters['submission_id']) is str and re.fullmatch(r'[a-f0-9]{64}',parameters['submission_id']))
    if method in {'intake_options','evidence','omission'}:
        values=dict(method=method,headers=dict(headers),parameters=dict(parameters));encode(values);return values
    if method=='interpret':
        from src.application.grounded_interpreter_submission import validate_selection
        message=parameters['message']
        require(type(message) is str and 0<len(message)<=2000 and len(message.encode())<=65536)
        try:selection=validate_selection(parameters['selection'])
        except ValueError:require(False)
        values=dict(method=method,headers=dict(headers),parameters={**parameters,'selection':selection})
        encode(values);return values
    preferences = parameters['preferences']
    require(type(preferences) is dict and set(preferences) == set(PREFERENCE_FIELDS)
        and all(type(value) is str and 1 <= len(value) <= MAX_AUDIENCE_CHARACTERS
                for value in preferences.values()))
    values = dict(method=method, headers=dict(headers), parameters={
        **parameters, 'preferences': dict(preferences)})
    encode(values)
    return values


class WorkflowSubmissionHost(SharingOperationHost):
    """Prestarted submission worker; uncertain calls retire without replay."""

    def __init__(self, config, settings=SharingHostSettings()):
        super().__init__(config, settings)

    def _valid_config(self, config):
        return type(config) is WorkflowSubmissionConfig

    @property
    def operation_settings(self):
        return self.config.submission.operation

    def _enabled(self):
        if not self.config.submission.enabled or not self.settings.enabled:
            raise SharingHostError('SHARING_HOST_DISABLED')
        if self.retired:
            raise SharingHostError('SHARING_HOST_RETIRED')

    def _command(self, fd):
        return [sys.executable, '-m', 'src.application.workflow_submission_worker', str(fd)]

    def _config_values(self):
        return asdict(self.config)

    def _request_values(self, method, headers, parameters):
        return request_values(method, headers, parameters)

    def _decode_result(self, method, value):
        if method=='evidence':
            from src.application.question_chat_transport import decode_result
            return decode_result(value)
        if method=='intake_options':
            from src.application.interpreter_submission import InterpreterIntakeOptions
            require(type(value) is dict and set(value)==set(InterpreterIntakeOptions.__dataclass_fields__))
            require(type(value['audiences']) is list and 0<len(value['audiences'])<=32
                and all(type(v) is str and 0<len(v)<=160 for v in value['audiences'])
                and re.fullmatch(r'[0-9a-f]{64}',value['snapshot_sha256']))
            return InterpreterIntakeOptions(**(value|{'audiences':tuple(value['audiences'])}))
        if method in {'interpret','omission'}:
            from src.application.interpreter_submission import InterpreterSubmissionReceipt
            require(type(value) is dict and set(value)==set(InterpreterSubmissionReceipt.__dataclass_fields__))
            for key in ('workspace_id','conversation_id','request_id','invocation_id','native_job_id'):
                require(type(value[key]) is str and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,255}',value[key]))
            require(value['native_status'] in {'COMMITTED','NOT_COMMITTED','UNKNOWN'})
            return InterpreterSubmissionReceipt(**value)
        require(type(value) is dict and set(value) == set(SubmissionReceipt.__dataclass_fields__))
        return SubmissionReceipt(**value)

    def _validate_result_scope(self, method, parameters, result):
        require(result.workspace_id == parameters['workspace_id']
            and result.conversation_id == parameters['conversation_id'])

    def submit(self, headers, *, cancellation=None, **parameters):
        return self._invoke('submit', headers, parameters, cancellation)

    def interpret(self,headers,*,cancellation=None,**parameters):
        if not self.config.interpretation_enabled:
            raise SharingHostError('INTERPRETER_SUBMISSION_DISABLED')
        return self._invoke('interpret',headers,parameters,cancellation)

    def intake_options(self,headers,*,cancellation=None,**parameters):
        return self._invoke('intake_options',headers,parameters,cancellation)

    def evidence(self,headers,*,cancellation=None,**parameters):
        if not self.config.interpretation_enabled or self.config.interpreter_runtime is None:
            raise SharingHostError('INTERPRETER_SUBMISSION_DISABLED')
        return self._invoke('evidence',headers,parameters,cancellation)

    def apply_omission(self,headers,*,cancellation=None,**parameters):
        if not self.config.interpretation_enabled:raise SharingHostError('INTERPRETER_SUBMISSION_DISABLED')
        return self._invoke('omission',headers,parameters,cancellation)
