"""P6 policy/check reduction fixtures, not measured quality or actual approval."""
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import re
import unittest

from src.application.campaign_acceptance_contract import (
    RULE_IDS, Stage, Decision, CheckState, PolicyStatus, AcceptanceRule,
    AcceptancePolicy, AcceptanceContext, CheckObservation, evaluate_acceptance,
)
from src.application.workflow_contract import ContractError, canonical_json_bytes

NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)
STAGES = tuple(sorted(Stage, key=lambda s: s.value))


def policy():
    return AcceptancePolicy('fixture-policy', 'v1', 'a' * 64,
        PolicyStatus.APPROVED, 'fixture-manager', NOW - timedelta(days=1),
        NOW - timedelta(hours=1), NOW + timedelta(hours=1), ('CA', 'GB'),
        ('KLAVIYO_EMAIL', 'META'), tuple(AcceptanceRule(r, STAGES, Decision.BLOCK,
                                                     'fixture-checker-v1') for r in RULE_IDS))


def context(stage=Stage.GENERATION):
    return AcceptanceContext('workspace-1', 'conversation-1', 'request-1',
                             'b' * 64, 'c' * 64, 'd' * 64, 'e' * 64,
                             stage, 'CA', 'META')


def observations(p, c):
    h = hashlib.sha256(canonical_json_bytes(c.payload())).hexdigest()
    return tuple(CheckObservation(r.rule_id, r.checker_version, p.sha256, h,
        NOW, CheckState.PASS, 'FIXTURE_SUPPORTED', 'f' * 64)
        for r in p.rules if c.stage in r.stages)


class AcceptanceContractTests(unittest.TestCase):
    def gate(self, p=None, c=None, checks=None, **kwargs):
        p = p or policy(); c = c or context()
        return evaluate_acceptance(p, c, observations(p, c) if checks is None else checks,
                                   **({'checked_at': NOW, 'not_before': NOW} | kwargs))

    def test_policy_rule_ids_match_review_specification(self):
        document = Path(__file__).resolve().parents[1] / 'docs/CAMPAIGN_ACCEPTANCE_POLICY.md'
        ids = re.findall(r'^\| ([AGCU]\d+) \|', document.read_text(), re.M)
        self.assertEqual(tuple(sorted(ids)), RULE_IDS)
        self.assertEqual(len(ids), 47)

    def test_valid_all_four_stages_derive_stable_immutable_result(self):
        for stage in Stage:
            with self.subTest(stage=stage):
                a = self.gate(c=context(stage)); b = self.gate(c=context(stage))
                self.assertEqual(a, b)
                self.assertEqual(a.decision, Decision.PASS)
                self.assertEqual(a.failed_rule_ids, ())
                with self.assertRaises(FrozenInstanceError):
                    a.decision = Decision.BLOCK

    def test_rule_coverage_missing_duplicate_extra_reordered_denied(self):
        p = policy()
        for rules in ((), p.rules[:-1], p.rules[::-1], p.rules + p.rules[:1],
                      p.rules[:-1] + p.rules[:1], list(p.rules)):
            with self.subTest(count=len(rules)), self.assertRaises(ContractError):
                replace(p, rules=rules)

    def test_each_stage_requires_all_areas(self):
        p = policy()
        rules = tuple(replace(r, stages=(Stage.GENERATION,),
                              stage_exclusion_reason='FIXTURE_GENERATION_ONLY')
                      if r.rule_id.startswith('U') else r for r in p.rules)
        with self.assertRaises(ContractError):
            replace(p, rules=rules)

    def test_rules_cannot_define_pass_failure_or_invalid_stages(self):
        r = policy().rules[0]
        for values in ({'failure_action': Decision.PASS}, {'failure_action': 'BLOCK'},
                       {'stages': ()}, {'stages': list(STAGES)}, {'stages': STAGES[::-1]},
                       {'stages': (Stage.GENERATION,) * 2}, {'rule_id': 'A99'}):
            with self.subTest(values=values), self.assertRaises(ContractError):
                replace(r, **values)

    def test_draft_revoked_future_expired_policy_cannot_pass(self):
        p = policy()
        variants = [replace(p, status=PolicyStatus.DRAFT),
                    replace(p, status=PolicyStatus.REVOKED),
                    replace(p, effective_at=NOW + timedelta(seconds=1)),
                    replace(p, valid_until=NOW)]
        for candidate in variants:
            with self.subTest(status=candidate.status), self.assertRaises(ContractError):
                self.gate(p=candidate)

    def test_approval_metadata_and_windows_required(self):
        p = policy()
        for changes in ({'approved_by': None}, {'approved_at': None},
                        {'approved_at': NOW}, {'valid_until': p.effective_at},
                        {'effective_at': NOW.replace(tzinfo=None)},
                        {'specification_sha256': 'bad'}):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                replace(p, **changes)
        draft = replace(p, status=PolicyStatus.DRAFT, approved_by=None, approved_at=None)
        with self.assertRaises(ContractError):
            self.gate(p=draft)

    def test_country_channel_scope_not_blanket_europe_or_worldwide(self):
        for c in (replace(context(), country='DE'), replace(context(), country='EUROPE'),
                  replace(context(), channel='TIKTOK')):
            with self.assertRaises(ContractError):
                self.gate(c=c)
        for changes in ({'countries': ('EUROPE',)}, {'countries': ('CA', 'CA')},
                        {'countries': ('ca',)}, {'channels': ('UNKNOWN',)},
                        {'channels': []}):
            with self.assertRaises(ContractError):
                replace(policy(), **changes)

    def test_missing_duplicate_extra_out_of_order_checks_fail_closed(self):
        p, c = policy(), context(); checks = observations(p, c)
        for values in ((), checks[:-1], checks[::-1], checks + checks[:1],
                       checks[:-1] + checks[:1], list(checks)):
            with self.subTest(count=len(values)), self.assertRaises(ContractError):
                self.gate(checks=values)

    def test_changed_candidate_evidence_prompt_model_or_parent_rejects_checks(self):
        p, c = policy(), context(); checks = observations(p, c)
        for name in ('candidate_sha256', 'evidence_sha256', 'prompt_sha256',
                     'model_profile_sha256', 'workspace_id', 'conversation_id', 'request_id'):
            value = '9' * 64 if name.endswith('sha256') else 'other-id'
            with self.subTest(name=name), self.assertRaises(ContractError):
                self.gate(c=replace(c, **{name: value}), checks=checks)

    def test_policy_and_checker_pins_reject_replay(self):
        p, c = policy(), context(); checks = observations(p, c)
        for changes in ({'policy_sha256': '0' * 64}, {'checker_version': 'old-checker'},
                        {'context_sha256': '0' * 64}):
            with self.assertRaises(ContractError):
                self.gate(checks=(replace(checks[0], **changes),) + checks[1:])
        with self.assertRaises(ContractError):
            self.gate(p=replace(p, version='v2'), checks=checks)

    def test_check_freshness_boundaries(self):
        p, c = policy(), context(); checks = observations(p, c)
        for stamp in (NOW - timedelta(microseconds=1), NOW + timedelta(microseconds=1)):
            with self.assertRaises(ContractError):
                self.gate(checks=(replace(checks[0], checked_at=stamp),) + checks[1:])
        with self.assertRaises(ContractError):
            self.gate(not_before=NOW + timedelta(microseconds=1))
        with self.assertRaises(ContractError):
            self.gate(not_before=p.effective_at - timedelta(microseconds=1))

    def test_every_rule_failure_blocks_and_uncertain_checks_require_review(self):
        p, c = policy(), context(); checks = observations(p, c)
        for i, check in enumerate(checks):
            for state, expected in [(CheckState.FAIL, Decision.BLOCK),
                                    (CheckState.UNAVAILABLE, Decision.NEEDS_REVIEW),
                                    (CheckState.INCONCLUSIVE, Decision.NEEDS_REVIEW)]:
                changed = checks[:i] + (replace(check, state=state),) + checks[i+1:]
                result = self.gate(checks=changed)
                self.assertEqual(result.decision, expected)
                self.assertEqual(result.failed_rule_ids, (check.rule_id,))

    def test_block_review_clarify_precedence_and_no_score_compensation(self):
        p = policy()
        rules = tuple(replace(r, failure_action=Decision.CLARIFY if i == 0
                              else Decision.NEEDS_REVIEW if i == 1 else Decision.BLOCK)
                      for i, r in enumerate(p.rules))
        p = replace(p, rules=rules); checks = observations(p, context())
        for count, expected in [(1, Decision.CLARIFY), (2, Decision.NEEDS_REVIEW),
                                (3, Decision.BLOCK)]:
            result = self.gate(p=p, checks=tuple(replace(o, state=CheckState.FAIL)
                                                if i < count else o
                                                for i, o in enumerate(checks)))
            self.assertEqual(result.decision, expected)
        unavailable = (replace(checks[0], state=CheckState.UNAVAILABLE),) + checks[1:]
        self.assertEqual(self.gate(p=p, checks=unavailable).decision, Decision.NEEDS_REVIEW)

    def test_policy_configured_stage_applicability_cannot_be_removed_by_observation(self):
        p = policy()
        rules = tuple(replace(r, stages=(Stage.GENERATION,),
                              stage_exclusion_reason='FIXTURE_ARTIFACT_ONLY') if r.rule_id == 'U07' else r
                      for r in p.rules)
        p = replace(p, rules=rules); c = context(Stage.CLARIFICATION)
        self.assertEqual(len(observations(p, c)), 46)
        self.assertEqual(self.gate(p=p, c=c).decision, Decision.PASS)
        with self.assertRaises(ContractError):
            self.gate(p=p, checks=observations(p, c))

    def test_digests_change_with_reason_reference_and_time(self):
        p, c = policy(), context(); checks = observations(p, c); original = self.gate()
        for values in ({'reason_code': 'OTHER_REASON'}, {'evidence_reference_sha256': '0'*64}):
            result = self.gate(checks=(replace(checks[0], **values),) + checks[1:])
            self.assertNotEqual(result.sha256, original.sha256)
        self.assertNotEqual(self.gate(checked_at=NOW + timedelta(seconds=1)).sha256,
                            original.sha256)

    def test_no_coercion_of_check_states_timestamps_or_context(self):
        p, c = policy(), context(); o = observations(p, c)[0]
        for values in ({'state': True}, {'state': 'PASS'}, {'checked_at': NOW.replace(tzinfo=None)},
                       {'reason_code': ''}, {'evidence_reference_sha256': None}):
            with self.assertRaises(ContractError):
                replace(o, **values)
        with self.assertRaises(ContractError):
            replace(c, stage='GENERATION')
        with self.assertRaises(ContractError):
            evaluate_acceptance(None, c, (), checked_at=NOW, not_before=NOW)

    def test_stage_exclusions_require_pinned_reason(self):
        r = policy().rules[0]
        with self.assertRaises(ContractError):
            replace(r, stages=(Stage.GENERATION,))
        with self.assertRaises(ContractError):
            replace(r, stages=(Stage.GENERATION,), stage_exclusion_reason='')

    def test_equivalent_timezone_policy_and_checks_are_canonical(self):
        p, c = policy(), context()
        offset = timezone(timedelta(hours=5, minutes=30))
        other = replace(p, approved_at=p.approved_at.astimezone(offset),
                        effective_at=p.effective_at.astimezone(offset),
                        valid_until=p.valid_until.astimezone(offset))
        self.assertEqual(p.sha256, other.sha256)
        checks = tuple(replace(o, checked_at=o.checked_at.astimezone(offset))
                       for o in observations(p, c))
        self.assertEqual(self.gate(checks=checks), self.gate())


if __name__ == '__main__':
    unittest.main()
