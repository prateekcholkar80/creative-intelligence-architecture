"""Trusted request-worker composition; configuration never grants prompt approval."""
from datetime import datetime,timezone
from google import genai
from google.genai import types
import agentplatform,json
from src.application.campaign_clarification_policy import ClarificationPolicyRequest
from src.application.campaign_clarification_prompts import INTENT_KEY,QUESTION_KEY
from src.application.registered_interpreter_processor import RegisteredInterpreterProcessor
from src.application.vertex_prompt_reader import AgentPlatformPromptVersionTransport
from src.application.access_reader import BigQueryAccessReader
from src.application.operation_budget import operation_timeout,check_operation
from src.application.workflow_contract import _require
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION


def validate_config(value):
    _require(type(value) is dict and {'enabled','policy','targets'} <= set(value)
        and set(value) <= {'enabled','policy','targets','question_quality_policy'}
        and type(value['enabled']) is bool,'INTERPRETER_SERVING_CONFIG')
    if 'question_quality_policy' in value:
        from src.application.workflow_contract import _id,_digest
        quality=value['question_quality_policy']
        _require(type(quality) is dict and set(quality)=={'policy_id','policy_version',
            'policy_specification_sha256','policy_header_sha256'},'QUESTION_QUALITY_POLICY_CONFIG')
        for key in ('policy_id','policy_version'):_id(quality[key])
        for key in ('policy_specification_sha256','policy_header_sha256'):_digest(quality[key])
    ClarificationPolicyRequest(**value['policy'])
    targets=value['targets']
    _require(type(targets) in (list,tuple) and len(targets)==2 and all(type(t) in (list,tuple) and len(t)==3 for t in targets)
        and [t[0] for t in targets]==[INTENT_KEY,QUESTION_KEY]
        and all(type(v) is str and v.isdecimal() and 0<len(v)<=32 for t in targets for v in t[1:]),'INTERPRETER_SERVING_TARGETS')
    return value


def provider_call(profile,system,prompt,schema,snapshot):
    check_operation()
    generation=json.loads(snapshot.generation_config_json)
    _require(generation.pop('automatic_function_calling_disabled') is True,'INTERPRETER_TOOL_POLICY')
    level=generation.pop('thinking_level')
    config=types.GenerateContentConfig(**generation,system_instruction=system,response_schema=schema,
        thinking_config=None if level is None else types.ThinkingConfig(thinking_level=level),
        safety_settings=[types.SafetySetting(**s) for s in json.loads(snapshot.safety_settings_json)],
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        tool_config=types.ToolConfig(function_calling_config=types.FunctionCallingConfig(mode='NONE')))
    timeout=max(1,int(operation_timeout(profile['timeout_ms']/1000)*1000))
    with genai.Client(vertexai=True,project=PROJECT,location=LOCATION,
            http_options=types.HttpOptions(timeout=timeout,retry_options=types.HttpRetryOptions(attempts=1))) as model:
        result=model.models.generate_content(model=profile['model_id'],contents=prompt,config=config)
    check_operation();return result


class InterpreterServingSubmission:
    """Persist first, then attempt gated execution; return no provider text."""
    def __init__(self,submission,processor,questions=None):
        self.submission,self.processor,self.questions=submission,processor,questions
    def intake_options(self,headers,**parameters):
        return self.submission.intake_options(headers,**parameters)
    def omission(self,headers,**parameters):
        # Save the explicit new turn only. The normal processing callback is
        # deliberately not invoked before separate reevaluation/admission.
        return self.submission.omission(headers,**parameters)
    def submit(self,headers,**parameters):
        receipt=self.submission.submit(headers,**parameters)
        if receipt.native_status=='COMMITTED':
            result=self.processor.process(headers,receipt)
            if self.questions is not None and result.state in {'COMMITTED','SUCCEEDED_STORED','RECOVERED'}:
                self.questions.process(headers,receipt)
        return receipt

    def evidence_readiness(self,headers,**scope):
        _require(self.questions is not None,'CHAT_EVIDENCE_SERVICE_NOT_INSTALLED')
        return self.questions.check(headers,**scope)

    def question_turn(self,headers,**scope):
        _require(self.questions is not None,'CHAT_EVIDENCE_SERVICE_NOT_INSTALLED')
        return self.questions.question_turn(headers,**scope)


def compose(submission,raw,resources):
    config=validate_config(raw)
    vertex=agentplatform.Client(project=PROJECT,location=LOCATION)
    close=getattr(vertex,'close',None)
    if callable(close):resources.callback(close)
    processor=RegisteredInterpreterProcessor(client=submission.client,verifier=submission.verifier,
        access_reader=BigQueryAccessReader(submission.client),policy_request=ClarificationPolicyRequest(**config['policy']),
        targets=tuple(tuple(t) for t in config['targets']),vertex_transport=AgentPlatformPromptVersionTransport(vertex),
        provider_transport=provider_call,profile=submission.profile,worker_id=submission.worker_id,enabled=config['enabled'])
    from src.application.question_serving_composition import ServingQuestions
    questions=ServingQuestions(submission=submission,access_reader=processor.access_reader,
        policy_request=processor.policy_request,targets=processor.targets,
        vertex_transport=processor.vertex_transport,provider_transport=provider_call,enabled=config['enabled'],
        quality_policy=config.get('question_quality_policy'))
    return InterpreterServingSubmission(submission,processor,questions)
