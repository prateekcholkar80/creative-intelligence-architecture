"""Review-only real-brand E1 case integrity, never current claim eligibility.

The separately recorded catalog import receipt is checkpoint metadata. This
validator does not authenticate it, requery BigQuery or approve evidence/labels.
"""
import argparse,hashlib,json
from pathlib import Path
from src.validation.campaign_e1_benchmark import _encoded,_exact,_text,_unique,_pairs,_reject_constant,MAX_BYTES
from src.application.campaign_acceptance_contract import RULE_IDS,Stage,Decision
from src.application.workflow_contract import _require,_digest
from src.ingestion.add_naked_thriving_products import TARGETS,EVIDENCE,CORRECTED_JOB_ID,BUCKET
from src.ingestion.configure_campaign_acceptance import manifest,SOURCES

ROOT=Path(__file__).resolve().parents[2]
DEFAULT=ROOT/'config/evaluation/campaign_e1_brand_cases_v01.json'


def validate(package,receipt):
    _exact(package,'schema_version status approved_by serving_enabled policy_version policy_specification_sha256 '
        'products sources cases benchmark_execution_status labels_status claim_permissions_created limitations '
        'evidence_availability language_baseline')
    raw=_encoded(package)
    _require(package['schema_version']=='campaign-e1-brand-cases-v1' and package['status']=='DRAFT'
        and package['approved_by'] is None and package['serving_enabled'] is False
        and package['claim_permissions_created'] is False and package['benchmark_execution_status']=='NOT_RUN'
        and package['labels_status']=='PROPOSED','E1_BRAND_DRAFT_ONLY')
    _require(package['evidence_availability']=='WEBSITE_SUMMARIES_ONLY_CONFIRMED_BY_USER'
        and package['language_baseline']==dict(mode='APPEARANCE_FOCUSED',
            examples=['appearance of fine lines','healthy-looking glow','support your routine'],
            authority='EXPLICIT_USER_DIRECTION_2026_10_15',sku_specific_claim_permission_required=True,
            baseline_is_claim_permission=False)
        and package['language_baseline']['sku_specific_claim_permission_required'] is True
        and package['language_baseline']['baseline_is_claim_permission'] is False,'E1_BRAND_LANGUAGE_NOT_PERMISSION')
    policy=manifest(SOURCES['draft-v0.2'],version='draft-v0.2')[0]
    _require(package['policy_version']=='draft-v0.2' and package['policy_specification_sha256']==policy['specification_sha256'],
        'E1_BRAND_POLICY_CONFLICT')
    _require(type(receipt) is dict and receipt.get('status')=='VERIFIED_APPLIED'
        and receipt.get('native_job_id')==CORRECTED_JOB_ID and receipt.get('original_full_rows_preserved') is True
        and receipt.get('claim_permissions_created') is False and receipt.get('serving_enabled') is False
        and receipt.get('total_rows',{}).get('product_master')==23,'E1_BRAND_CATALOG_CHECKPOINT_REQUIRED')
    _require(type(package['products']) is list and len(package['products'])==3,'E1_BRAND_PRODUCT_COVERAGE')
    skus=[]
    for p in package['products']:
        _exact(p,'slot_id canonical_sku brand_product_name source_variant_id formula_version source_ids mapping_status')
        sku=p['canonical_sku'];_require(type(sku) is str and sku in TARGETS,'E1_BRAND_PRODUCT_COVERAGE')
        name,size,variant=TARGETS[sku];skus.append(sku)
        _require(p['brand_product_name']==name and p['source_variant_id']==variant and p['formula_version'] is None
            and p['mapping_status']=='CATALOG_IDENTITY_VERIFIED_FORMULA_AND_CLAIMS_PENDING',
            'E1_BRAND_UNVERIFIED_FORMULA_OR_PERMISSION')
        _unique(p['source_ids'])
        pin=receipt.get('gcs_pins',{}).get(sku,{})
        _require(type(pin.get('generation')) is int and pin['generation']>0 and type(pin.get('sha256')) is str
            and pin.get('gcs_uri')==f'gs://{BUCKET}/products/{sku}/primary.jpg','E1_BRAND_IMAGE_CHECKPOINT_REQUIRED')
        _digest(pin['sha256'])
    _require(skus==sorted(TARGETS),'E1_BRAND_PRODUCT_COVERAGE')
    sources=package['sources'];_require(type(sources) is list and len(sources)==4,'E1_BRAND_SOURCE_COVERAGE')
    by_id={}
    for source in sources:
        _require(type(source) is dict and type(source.get('source_id')) is str
            and source['source_id'] not in by_id and source.get('formula_verified') is False
            and source.get('claim_approved') is False,'E1_BRAND_SOURCE_NOT_APPROVAL')
        _require(type(source.get('url')) is str and source['url'].startswith('https://www.nakedandthriving.com/'),
            'E1_BRAND_OFFICIAL_SOURCE_REQUIRED')
        projection=json.dumps(source['facts'],sort_keys=True,separators=(',',':')).encode()
        _require(hashlib.sha256(projection).hexdigest()==source.get('projection_sha256'),'E1_BRAND_SOURCE_PROJECTION_CONFLICT')
        by_id[source['source_id']]=source
    for p in package['products']:
        _require(set(p['source_ids'])<=set(by_id),'E1_BRAND_SOURCE_MISSING')
        source=by_id[p['source_ids'][0]];facts=source['facts'];name,size,variant=TARGETS[p['canonical_sku']]
        _require(source['source_kind']=='OFFICIAL_PRODUCT_PAGE_AND_JSON' and facts['product_name']==name
            and facts['full_size_ml']==size and facts['source_variant_id']==variant,'E1_BRAND_SOURCE_PRODUCT_CONFLICT')
    clinical=by_id.get('official-clinical-summary',{})
    _require(clinical.get('source_kind')=='OFFICIAL_WEBSITE_SUMMARY_NOT_STUDY_REPORT'
        and clinical['facts'].get('underlying_reports_reviewed') is False
        and clinical['facts'].get('population_details') is None,'E1_BRAND_STUDY_REVIEW_REQUIRED')
    cases=package['cases'];_require(type(cases) is list and len(cases)==26,'E1_BRAND_CASE_COVERAGE')
    ids=[];stages=set();channels=set();countries=set();outcomes=set();covered=set()
    for c in cases:
        _exact(c,'case_id canonical_sku rule_ids stage channel country candidate expected_decision expected_reason '
            'expected_source_ids label_status rendered_artifact_verified')
        for key in ('case_id','candidate','expected_reason'):_text(c[key])
        _unique(c['rule_ids']);_unique(c['expected_source_ids'])
        _require(set(c['rule_ids'])<=set(RULE_IDS) and set(c['expected_source_ids'])<=set(by_id)
            and type(c['canonical_sku']) is str and c['canonical_sku'] in TARGETS
            and c['label_status']=='PROPOSED' and c['rendered_artifact_verified'] is False,'E1_BRAND_CASE_PINS')
        _require(all(type(c[k]) is str for k in ('stage','channel','country','expected_decision'))
            and c['stage'] in {s.value for s in Stage} and c['channel'] in {'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'}
            and c['country'] in {'US','CA','GB','AU'} and c['expected_decision'] in {d.value for d in Decision},'E1_BRAND_CASE_SCOPE')
        if c['case_id'].endswith('website-before-after'):
            _require(c['expected_decision']=='BLOCK' and 'C07' in c['rule_ids'],'E1_BRAND_PROHIBITED_VISUAL')
        if c['case_id'].endswith(('study-report-hold','permission-missing')):
            _require(c['expected_decision']=='NEEDS_REVIEW','E1_BRAND_UNAPPROVED_CLAIM_PASS')
        ids.append(c['case_id']);stages.add(c['stage']);channels.add(c['channel']);countries.add(c['country'])
        outcomes.add(c['expected_decision']);covered.add(c['canonical_sku'])
    _require(ids==sorted(set(ids)) and covered==set(TARGETS) and len(stages)==4 and len(channels)==4
        and len(countries)==4 and len(outcomes)==4,'E1_BRAND_CASE_COVERAGE')
    _unique(package['limitations'])
    return dict(status='VALID_BRAND_DRAFT_NOT_READY',case_count=len(cases),source_count=len(sources),
        canonical_skus=skus,package_sha256=hashlib.sha256(raw).hexdigest(),catalog_native_job_id=receipt['native_job_id'],
        live_catalog_rechecked=False,labels_reviewed=False,model_quality_measured=False,e1_complete=False,
        serving_enabled=False,formula_versions_verified=False,study_reports_reviewed=False)


def load(path=DEFAULT,receipt_path=EVIDENCE/'catalog_import_verification_20261015.json'):
    def bounded(p):
        with Path(p).open('rb') as source:raw=source.read(MAX_BYTES+1)
        _require(len(raw)<=MAX_BYTES,'E1_BRAND_PACKAGE_TOO_LARGE')
        return json.loads(raw,object_pairs_hook=_pairs,parse_constant=_reject_constant)
    return validate(bounded(path),bounded(receipt_path))


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--path',type=Path,default=DEFAULT)
    args=parser.parse_args();print(json.dumps(load(args.path),indent=2))


if __name__=='__main__':main()
