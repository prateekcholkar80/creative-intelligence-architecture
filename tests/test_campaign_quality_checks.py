from dataclasses import replace,FrozenInstanceError
from datetime import datetime,timedelta,timezone
from unittest import TestCase
from src.application.campaign_quality_checks import (
    SourceFact,FactSpan,QualityCandidate,check_candidate,require_release,CHANNEL_FIELDS,
)
from src.application.campaign_acceptance_contract import Stage,CheckState,Decision,RULE_IDS
from src.application.workflow_contract import canonical_json_bytes,ContractError

NOW=datetime(2026,10,15,10,tzinfo=timezone.utc)


def source(**changes):
    return SourceFact(**(dict(source_id='product-21',sku='CI-SKU-0021',fields_json=canonical_json_bytes(
        dict(product_name='Renew Resurfacing Night Serum',full_size_ml='30')).decode(),
        observed_at=NOW,valid_until=NOW+timedelta(minutes=1),access_allowed=True)|changes))


def candidate(s=None,text=None,field='product_name',**changes):
    s=s or source();text=text if text is not None else 'Renew Resurfacing Night Serum'
    return QualityCandidate(**(dict(request_id='request-1',stage=Stage.INTERPRETATION,sku='CI-SKU-0021',country='US',
        channel='META',text=text,spans=(FactSpan(0,len(text),s.source_id,field,s.sha256),))|changes))


def check(c=None,s=None,**changes):
    s=s or source();c=c or candidate(s)
    report=check_candidate(c,(s,),**(dict(checked_at=NOW,not_before=NOW)|changes))
    return report,{r.rule_id:r for r in report.checks}


class QualityChecksTests(TestCase):
    def test_exact_fact_has_bounded_support_but_unimplemented_rules_prevent_global_pass(self):
        report,checks=check()
        self.assertEqual(tuple(checks),RULE_IDS)
        for rid in ('A01','G01','G03','G07','G10'):self.assertIs(checks[rid].state,CheckState.PASS)
        self.assertIs(checks['C01'].state,CheckState.UNAVAILABLE)
        self.assertIs(report.decision,Decision.NEEDS_REVIEW)
        with self.assertRaises(FrozenInstanceError):report.decision=Decision.PASS

    def test_actual_numeric_and_product_name_changes_fail_without_reading_expected_labels(self):
        for text,field in (('100','full_size_ml'),('Another product','product_name')):
            report,checks=check(candidate(text=text,field=field))
            self.assertIs(checks['A01'].state,CheckState.FAIL);self.assertIs(report.decision,Decision.BLOCK)

    def test_cross_sku_source_and_rehashed_cross_sku_pins_fail(self):
        s=source(sku='CI-SKU-0022');report,checks=check(candidate(s),s)
        self.assertIs(checks['G03'].state,CheckState.FAIL);self.assertIs(report.decision,Decision.BLOCK)

    def test_missing_tampered_source_or_unknown_field_is_not_a_valid_citation(self):
        s=source();c=candidate(s)
        for span in (replace(c.spans[0],source_id='missing'),replace(c.spans[0],source_sha256='a'*64),
            replace(c.spans[0],field='not_in_source')):
            report,checks=check(replace(c,spans=(span,)))
            self.assertIs(checks['G10'].state,CheckState.FAIL);self.assertIs(report.decision,Decision.BLOCK)

    def test_expiry_stale_floor_future_observation_and_access_revocation_block(self):
        for s in (source(observed_at=NOW-timedelta(seconds=1)),source(observed_at=NOW+timedelta(seconds=1)),
            source(observed_at=NOW-timedelta(minutes=1),valid_until=NOW),source(access_allowed=False)):
            report,checks=check(candidate(s),s)
            self.assertIs(checks['G07'].state,CheckState.FAIL);self.assertIs(report.decision,Decision.BLOCK)

    def test_unannotated_or_appended_text_never_becomes_supported_by_a_valid_citation(self):
        c=candidate();report,checks=check(replace(c,text=c.text+'. Guaranteed permanent results'))
        self.assertIs(checks['G01'].state,CheckState.INCONCLUSIVE)
        self.assertIsNot(report.decision,Decision.PASS)
        report,checks=check(replace(c,spans=()))
        self.assertIs(checks['G10'].state,CheckState.INCONCLUSIVE)

    def test_overlapping_boolean_and_out_of_bounds_spans_are_rejected(self):
        c=candidate()
        with self.assertRaises(ContractError):replace(c,spans=c.spans+c.spans)
        with self.assertRaises(ContractError):replace(c,spans=(replace(c.spans[0],end=999),))
        with self.assertRaises(ContractError):replace(c.spans[0],start=True)

    def test_all_channels_require_exact_nonempty_sections_without_certifying_content(self):
        for channel,fields in CHANNEL_FIELDS.items():
            sections=canonical_json_bytes(dict.fromkeys(fields,'Draft placeholder')).decode()
            c=candidate(channel=channel,stage=Stage.GENERATION,sections_json=sections)
            report,checks=check(c);self.assertIs(checks['U06'].state,CheckState.PASS)
            self.assertIs(checks['G01'].state,CheckState.INCONCLUSIVE)
            for altered in ({k:'x' for k in fields[1:]},dict.fromkeys(fields,' '),dict.fromkeys(fields,'x')|{'extra':'x'}):
                report,checks=check(replace(c,sections_json=canonical_json_bytes(altered).decode()))
                self.assertIs(checks['U06'].state,CheckState.FAIL);self.assertIsNot(report.decision,Decision.PASS)

    def test_source_and_candidate_json_must_be_canonical_bounded_and_immutable(self):
        with self.assertRaises(ContractError):source(fields_json='{"b":"x", "a":"y"}')
        with self.assertRaises(ContractError):source(fields_json='x'*65537)
        with self.assertRaises(ContractError):source(access_allowed=1)
        with self.assertRaises(ContractError):candidate(text='e\u0301')
        with self.assertRaises(ContractError):candidate(country='WW')

    def test_duplicate_sources_and_invalid_windows_fail(self):
        c=candidate();s=source()
        with self.assertRaises(ContractError):check_candidate(c,(s,s),checked_at=NOW,not_before=NOW)
        with self.assertRaises(ContractError):check(not_before=NOW+timedelta(seconds=1))

    def test_report_binds_request_stage_text_scope_and_sources(self):
        c=candidate();s=source();report,_=check(c,s)
        for changed in (replace(c,request_id='other'),replace(c,channel='TIKTOK'),replace(c,stage=Stage.REFINEMENT)):
            with self.assertRaises(ContractError):require_release(changed,(s,),report,checked_at=NOW,not_before=NOW)
        self.assertNotEqual(report.evidence_sha256,check(s=source(access_allowed=False))[0].evidence_sha256)

    def test_forged_pass_and_release_reuse_never_release_bytes_or_dispatch(self):
        c=candidate();s=source();report,_=check(c,s)
        for r in (report,replace(report,decision=Decision.PASS),replace(report,sha256='a'*64)):
            with self.assertRaises(ContractError):require_release(c,(s,),r,checked_at=NOW,not_before=NOW)
        with self.assertRaises(ContractError):require_release(c,(s,),report,checked_at=NOW-timedelta(seconds=1),not_before=NOW-timedelta(seconds=1))
