"""Synthetic supplied permission cases, not approved brand claims or studies."""
from dataclasses import replace, FrozenInstanceError
from datetime import timedelta, timezone
from decimal import Decimal, localcontext
import unittest
from src.application.campaign_permission_contract import (
    Approval, EvidencePin, EvidenceKind, ClaimPermission, ClaimKind, OfferPermission,
)
from src.application.campaign_acceptance_contract import PolicyStatus
from src.application.workflow_contract import ContractError
from tests.test_campaign_acceptance_contract import NOW


def approval():
    return Approval('fixture-v1',1,None,PolicyStatus.APPROVED,'fixture-manager',
                    NOW-timedelta(days=2),NOW-timedelta(days=1),NOW+timedelta(days=1))


def evidence():
    return EvidencePin('fixture-study','CI-SKU-0001','fixture-formula-v1',
        EvidenceKind.FINISHED_PRODUCT_CLINICAL,'a'*64,approval(),
        endpoint='Decrease in appearance of fine lines',sample_size=33,duration_days=28,
        usage_conditions='Use according to approved instructions',population='Fixture adults',
        statistic=Decimal('97'),statistic_meaning='PARTICIPANTS_PERCENT')


def claim():
    return ClaimPermission('fixture-claim','CI-SKU-0001','fixture-formula-v1',
        ClaimKind.CLINICAL_RESULT,approval(),(evidence(),),
        ('97% of participants showed improvement in the appearance of fine lines.',),
        ('Four-week fixture study of 33 adults. Results vary.',),('CA',),('META',),('NEW','RETURNING'))


def claim_use(c=None, **changes):
    c=c or claim()
    return c.validate_use(**(dict(sku=c.sku,formula_version=c.formula_version,wording=c.wording[0],
        qualifications=c.qualifications,country='CA',channel='META',audience='NEW',checked_at=NOW)|changes))


def offer():
    return OfferPermission('fixture-offer',approval(),('CA',),('KLAVIYO_EMAIL',),('RETURNING',),
        ('CI-SKU-0001',),'CAD',Decimal('250'),Decimal('25'),('CI-SKU-0002',),
        'FIXTURE',False,False,False,'STANDARD',('Fixture offer excludes subscriptions.',))


def offer_use(o=None, **changes):
    o=o or offer()
    return o.validate_use(**(dict(candidate_offer_sha256=o.sha256,country='CA',channel='KLAVIYO_EMAIL',
        audience='RETURNING',sku='CI-SKU-0001',currency='CAD',is_subscription=False,checked_at=NOW)|changes))


class PermissionTests(unittest.TestCase):
    def test_valid_scoped_claim_offer_stable_immutable(self):
        self.assertEqual(claim_use(),claim().sha256)
        self.assertEqual(offer_use(),offer().sha256)
        with self.assertRaises(FrozenInstanceError): claim().sku='CI-SKU-0002'

    def test_website_ingredient_consumer_cannot_prove_clinical_claim(self):
        c=claim()
        for kind in (EvidenceKind.WEBSITE_SUMMARY,EvidenceKind.INGREDIENT_RESEARCH,
                     EvidenceKind.CONSUMER_STUDY,EvidenceKind.PRODUCT_FACT):
            if kind is EvidenceKind.CONSUMER_STUDY:
                e=replace(evidence(),kind=kind)
            else:
                e=EvidencePin('fixture','CI-SKU-0001','fixture-formula-v1',kind,'a'*64,approval())
            with self.assertRaises(ContractError):replace(c,evidence=(e,))

    def test_fact_based_fallback_without_clinical_study(self):
        e=EvidencePin('fact','CI-SKU-0001','fixture-formula-v1',EvidenceKind.PRODUCT_FACT,'a'*64,approval())
        c=replace(claim(),kind=ClaimKind.PRODUCT_FACT,evidence=(e,),wording=('30 mL.',),qualifications=())
        self.assertEqual(claim_use(c),c.sha256)

    def test_cross_sku_formula_source_and_candidate_denied(self):
        for values in ({'sku':'CI-SKU-0002'},{'formula_version':'other-formula'}):
            with self.assertRaises(ContractError):replace(claim(),evidence=(replace(evidence(),**values),))
            with self.assertRaises(ContractError):claim_use(**values)

    def test_percentage_improvement_mutation_missing_qualification_denied(self):
        with self.assertRaises(ContractError):replace(claim(),qualifications=())
        for values in ({'wording':'Reduces wrinkles by 97%.'},{'qualifications':()},
                       {'qualifications':list(claim().qualifications)}):
            with self.assertRaises(ContractError):claim_use(**values)

    def test_country_channel_audience_mismatch(self):
        for values in ({'country':'GB'},{'country':'EUROPE'},{'channel':'TIKTOK'},{'audience':'UNKNOWN'}):
            with self.assertRaises(ContractError):claim_use(**values)
            with self.assertRaises(ContractError):offer_use(**values)

    def test_missing_revoked_expired_future_approval_or_evidence_denied(self):
        a=approval()
        bad=[replace(a,status=PolicyStatus.DRAFT),replace(a,status=PolicyStatus.REVOKED),
             replace(a,valid_until=NOW),replace(a,effective_at=NOW+timedelta(seconds=1))]
        for candidate in bad:
            with self.assertRaises(ContractError):claim_use(replace(claim(),approval=candidate))
            with self.assertRaises(ContractError):claim_use(replace(claim(),evidence=(replace(evidence(),review=candidate),)))
            with self.assertRaises(ContractError):offer_use(replace(offer(),approval=candidate))
        with self.assertRaises(ContractError):replace(a,approved_by=None)

    def test_endorsement_requires_separate_permission_reference(self):
        with self.assertRaises(ContractError):
            EvidencePin('derm','CI-SKU-0001','fixture-formula-v1',EvidenceKind.DERMATOLOGIST_RECOMMENDATION,'a'*64,approval())
        e=EvidencePin('derm','CI-SKU-0001','fixture-formula-v1',EvidenceKind.DERMATOLOGIST_RECOMMENDATION,
                      'a'*64,approval(),permission_reference_sha256='b'*64)
        c=replace(claim(),kind=ClaimKind.DERMATOLOGIST_RECOMMENDATION,evidence=(e,))
        self.assertEqual(claim_use(c),c.sha256)

    def test_study_method_population_numbers_required_no_float_or_bool(self):
        for values in ({'sample_size':True},{'duration_days':0},{'population':None},
                       {'usage_conditions':''},{'statistic':97.0},{'statistic':Decimal('NaN')},
                       {'statistic':Decimal('101')},{'statistic_meaning':'GUARANTEE'}):
            with self.assertRaises(ContractError):replace(evidence(),**values)
        fact=EvidencePin('fact','CI-SKU-0001','fixture-formula-v1',EvidenceKind.PRODUCT_FACT,'a'*64,approval())
        with self.assertRaises(ContractError):replace(fact,sample_size=33)

    def test_evidence_missing_duplicate_order_and_scope(self):
        c=claim(); e=evidence()
        for values in ((),(e,e),[e],(replace(e,evidence_id='z'),replace(e,evidence_id='a'))):
            with self.assertRaises(ContractError):replace(c,evidence=values)
        for sku in ('CI-SKU-0000','CI-SKU-0021','Renew',None):
            with self.assertRaises(ContractError):replace(c,sku=sku)

    def test_revision_parent_and_timezone(self):
        a=approval()
        for values in ({'revision':True},{'revision':0},{'revision':2},{'parent_sha256':'a'*64},
                       {'approved_at':NOW},{'effective_at':NOW.replace(tzinfo=None)}):
            with self.assertRaises(ContractError):replace(a,**values)
        newer=replace(a,revision=2,parent_sha256=claim().sha256)
        self.assertNotEqual(replace(claim(),approval=newer).sha256,claim().sha256)
        offset=timezone(timedelta(hours=5,minutes=30))
        equivalent=replace(a,approved_at=a.approved_at.astimezone(offset),
                           effective_at=a.effective_at.astimezone(offset),valid_until=a.valid_until.astimezone(offset))
        self.assertEqual(replace(claim(),approval=equivalent).sha256,claim().sha256)

    def test_offers_contradictory_codes_invalid_amounts_gifts_denied(self):
        for values in ({'automatic':True},{'code':None},{'automatic':1},
                       {'threshold':250.0},{'threshold':Decimal('-1')},
                       {'discount_percent':Decimal('101')},{'currency':'ANY'},
                       {'gift_skus':('CI-SKU-0002','CI-SKU-0002')},{'skus':()},
                       {'qualifications':()}):
            with self.assertRaises(ContractError):replace(offer(),**values)
        automatic=replace(offer(),automatic=True,code=None)
        self.assertEqual(offer_use(automatic),automatic.sha256)

    def test_offer_expiry_threshold_code_gift_change_and_subscription_denied(self):
        o=offer()
        for values in ({'threshold':Decimal('100')},{'code':'OTHER'},
                       {'gift_skus':('CI-SKU-0003',)},{'stacking_allowed':True},
                       {'approval':replace(o.approval,valid_until=NOW+timedelta(hours=1))}):
            with self.assertRaises(ContractError):offer_use(replace(o,**values),candidate_offer_sha256=o.sha256)
        for values in ({'currency':'USD'},{'sku':'CI-SKU-0002'},{'is_subscription':True},
                       {'is_subscription':1},{'checked_at':o.approval.valid_until}):
            with self.assertRaises(ContractError):offer_use(**values)

    def test_decimal_hash_is_scale_and_context_independent(self):
        o=offer()
        equivalent=replace(o,threshold=Decimal('250.00'),discount_percent=Decimal('25.0'))
        self.assertEqual(o.sha256,equivalent.sha256)
        with localcontext() as ctx:
            ctx.prec=2
            self.assertEqual(o.sha256,equivalent.sha256)
        self.assertEqual(evidence().sha256,replace(evidence(),statistic=Decimal('97.0')).sha256)
        self.assertEqual(replace(o,threshold=Decimal('0')).sha256,
                         replace(o,threshold=Decimal('-0.00')).sha256)

    def test_scope_and_wording_bounded_unique_and_typed(self):
        c=claim()
        for values in ({'wording':()},{'wording':('x','x')},{'wording':(' x',)},
                       {'countries':('EUROPE',)},{'channels':('UNKNOWN',)},
                       {'audiences':('NEW','NEW')},{'qualifications':('x\ny',)}):
            with self.assertRaises(ContractError):replace(c,**values)


if __name__=='__main__':unittest.main()
