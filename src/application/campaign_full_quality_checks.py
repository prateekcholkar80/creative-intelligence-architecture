"""All-rule diagnostic observations; never release or evaluator qualification."""
from dataclasses import dataclass
import json
from src.application.campaign_acceptance_contract import RULE_IDS,_hash
from src.application.campaign_quality_checks import QualityCandidate,QualityReport,IMPLEMENTED,VERSION as DETERMINISTIC_VERSION
from src.application.campaign_semantic_checks import SemanticCatalog,SemanticInput,SEMANTIC_RULE_IDS,verify_result,prompt,APPLICABILITY_VERSION
from src.application.campaign_stage_applicability import stage_applicability
from src.application.workflow_contract import _require,canonical_json_bytes

VERSION='campaign-full-quality-observation-v2'


@dataclass(frozen=True,slots=True)
class FullQualityEvaluator:
    catalog: SemanticCatalog
    semantic_evaluator_sha256: str
    observe: object

    def __post_init__(self):
        _require(type(self.catalog) is SemanticCatalog and self.semantic_evaluator_sha256==self.catalog.sha256
            and callable(self.observe),'FULL_QUALITY_EVALUATOR_PIN')

    @property
    def evaluator_sha256(self):
        return _hash(dict(version=VERSION,semantic_evaluator_sha256=self.semantic_evaluator_sha256,
            deterministic_version=DETERMINISTIC_VERSION,max_rules_per_observation=16))

    def for_stage(self,binding):
        from dataclasses import replace
        from src.application.campaign_evaluator_observer import DurableEvaluatorObserver
        if type(self.observe) is DurableEvaluatorObserver:
            _require(self.observe.evaluator_sha256==self.evaluator_sha256,'FULL_QUALITY_DURABLE_EVALUATOR_PIN')
            return replace(self,observe=self.observe.for_stage(binding))
        # Existing supplied callbacks remain diagnostic-only; they cannot supply
        # native evaluator receipts or become qualifying through this adapter.
        return self

    def assess(self,candidate,base,*,turns,evidence,selection,output_contract=None,source_applicability=None):
        _require(type(candidate) is QualityCandidate and type(base) is QualityReport
            and base.candidate_sha256==candidate.sha256,'FULL_QUALITY_CANDIDATE_BINDING')
        _require([c.rule_id for c in base.checks]==list(RULE_IDS),'FULL_QUALITY_BASE_COVERAGE')
        from src.application.campaign_evaluator_observer import DurableEvaluatorObserver
        if type(self.observe) is DurableEvaluatorObserver:
            binding=self.observe.binding
            _require(binding is not None and binding.candidate_sha256==candidate.sha256
                and binding.request_id==candidate.request_id and binding.stage==candidate.stage.value,
                'FULL_QUALITY_DURABLE_STAGE_CHANGED')
        scoped=self.catalog.contract_version==APPLICABILITY_VERSION
        if scoped:
            context=stage_applicability(candidate.stage.value,candidate.text,output_contract)
            _require(type(source_applicability) is dict,'FULL_QUALITY_SOURCE_APPLICABILITY_REQUIRED')
        else:_require(output_contract is None and source_applicability is None,'FULL_QUALITY_LEGACY_CONTEXT_CHANGED')
        cases=[];prepared=[]
        # Preflight every bounded payload before the first observer call. Keep
        # the existing context limit; never truncate required criteria/evidence.
        rules=tuple(sorted(SEMANTIC_RULE_IDS))
        for offset in range(0,len(rules),16):
            case=SemanticInput(candidate.stage.value,candidate.country,candidate.channel,candidate.text,
                canonical_json_bytes(turns).decode(),canonical_json_bytes(evidence).decode(),
                rules[offset:offset+16],'STAGE_OUTPUT_OBSERVATION' if scoped else 'GLOBAL_CAMPAIGN_RELEASE',canonical_json_bytes(selection).decode(),
                canonical_json_bytes(context).decode() if scoped else '{}',canonical_json_bytes(source_applicability).decode() if scoped else '{}')
            prepared.append(prompt(self.catalog,case));cases.append(case)
        semantic={};decisions=[];observations=[]
        for case,(system,content) in zip(cases,prepared):
            # No expected labels, case IDs or requested pass states cross this callback.
            checked=verify_result(self.catalog,case,self.observe(system,content,case.rule_ids))
            semantic.update({c['rule_id']:(c,case.sha256) for c in checked['checks']})
            decisions.append(checked['requested_content_decision']);observations.append(checked)
        _require(set(semantic)==set(SEMANTIC_RULE_IDS),'FULL_QUALITY_SEMANTIC_COVERAGE')
        literal={c.rule_id:c for c in base.checks}
        checks=[]
        for rid in RULE_IDS:
            if rid in IMPLEMENTED:
                check=literal[rid];checks.append(dict(rule_id=rid,state=check.state.value,reason_code=check.reason_code,
                    evidence_sha256=base.evidence_sha256,origin='DETERMINISTIC'))
            else:
                check,input_sha=semantic[rid];checks.append(dict(rule_id=rid,state=check['handling_state'],
                    reason_code='SEMANTIC_OBSERVATION',evidence_sha256=_hash(dict(input_sha256=input_sha,check=check)),
                    origin='SEMANTIC',observation=check))
        result=dict(version=VERSION,candidate_sha256=candidate.sha256,base_report_sha256=base.sha256,
            evaluator_sha256=self.evaluator_sha256,input_sha256=_hash([case.sha256 for case in cases]),checks=checks,
            semantic_requested_content_decisions=decisions,semantic_observations=observations,qualifying=False,release_allowed=False,
            source_authority_granted_by_judge=False,rendered_pixels_checked=False)
        if scoped:result.update(stage_applicability=context,source_applicability=source_applicability)
        # Keep the raw semantic input hash and exact observations for later
        # measured report provenance. They cannot replace native execution proof.
        from src.application.campaign_evaluator_observer import DurableEvaluatorObserver
        if type(self.observe) is DurableEvaluatorObserver:
            result['durable_evaluator_receipts']=list(self.observe.receipts)
        return dict(result,observation_sha256=_hash(result))
