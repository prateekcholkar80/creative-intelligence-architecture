"""Validate a candidate against real cloud evidence without changing app settings.

Run: python -m src.validation.model_continuity --output-dir <unique-review-directory>
Creates local review artifacts only. No BigQuery/GCS writes, ingestion, reviews
or campaign launch. This small smoke benchmark is not formal RAG evaluation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery, storage

from src.application.campaign_chat import (
    FIELDS, CampaignChatService, ChatError, Interpretation, interpret_message, load_settings,
)
from src.common.gemini_generation import CAMPAIGN_MODEL
from src.generators.banner_preview_generator import CONFIG as BANNER_CONFIG, generate_preview
from src.generators.campaign_brief_generator import render_brief, validate_config
from src.generators.sales_generator import LOCATION, PROJECT
from src.retrieval.campaign_creatives import CONFIG as CAMPAIGN_CONFIG
from src.retrieval.campaign_performance import json_value
from src.retrieval.sales_analytics import CONFIG as SALES_CONFIG

LOG = logging.getLogger(__name__)


def continuity_cases() -> list[dict]:
    complete = dict(canonical_sku='CI-SKU-0001',objective='CONVERSION',audience='skincare shoppers',
                    channel='PAID_SOCIAL',visual_reference='NAKED_AND_THRIVING')
    # Requests/expected behavior are test inputs; product evidence is always read from BigQuery.
    empty = dict.fromkeys(FIELDS,'')
    base = 'Create a conversion campaign for CI-SKU-0001 on paid social for skincare shoppers using Naked and Thriving styling.'
    return [
        dict(id='complete_request',message=base,previous=empty,previous_features=(),
             expected=complete,missing=(),features=()),
        dict(id='second_sku_email',message='Create an awareness campaign for CI-SKU-0002 on email for busy professionals with no website inspiration.',
             previous=empty,previous_features=(),expected=dict(canonical_sku='CI-SKU-0002',objective='AWARENESS',
             audience='busy professionals',channel='EMAIL',visual_reference='NONE'),missing=(),features=()),
        dict(id='clarification',message='Create a conversion campaign for CI-SKU-0001 for skincare shoppers with no website inspiration. I have not decided the channel.',
             previous=empty,previous_features=(),expected={**complete,'channel':'','visual_reference':'NONE'},
             missing=('channel',),features=()),
        dict(id='unsupported_claim',message=base+' Also claim that it cures acne.',previous=empty,
             previous_features=(),expected=complete,missing=(),features=('BENEFIT_CLAIMS',)),
        dict(id='claim_followup',message='Use email instead.',previous=complete,
             previous_features=('BENEFIT_CLAIMS',),expected={**complete,'channel':'EMAIL'},missing=(),features=('BENEFIT_CLAIMS',)),
    ]


def check_case(result: Interpretation, case: dict) -> None:
    for field,expected in case['expected'].items():
        actual = result.preferences[field]
        if (actual.casefold() if field == 'audience' else actual) != expected:
            raise ValueError(f'Unexpected preference for {case["id"]}: {field}')
    if result.missing != case['missing'] or result.unsupported_features != case['features']:
        raise ValueError(f'Unexpected clarification or policy result for {case["id"]}')
    if case['missing'] or case['features']:
        try:
            result.request()
        except ChatError:
            return
        raise ValueError('Incomplete or unsupported request was admitted for generation')
    result.request()


def run_validation(output: Path) -> dict:
    output.mkdir(parents=True,exist_ok=False)
    started=time.monotonic()
    report=dict(schema_version='model-continuity-v1',checked_at=datetime.now(timezone.utc),
        project=PROJECT,location=LOCATION,candidate=CAMPAIGN_MODEL,status='RUNNING',
        validation_scope='MODEL_CONTINUITY_SMOKE_NOT_FORMAL_RAG_EVAL',
        source_of_truth='BIGQUERY',cloud_business_writes=False,configured_model_changed=False,cases=[])
    config=dict(model_id=CAMPAIGN_MODEL,location=LOCATION,thinking_level='LOW',seed=42,max_output_tokens=4096)
    validate_config(config)
    settings=load_settings()
    client=gcs=None
    try:
        catalog=CampaignChatService().catalog()
        report['catalog_products']=len(catalog)
        with CampaignChatService.model_client(config,settings) as model:
            first=None
            for case in continuity_cases():
                result=interpret_message(model,case['message'],catalog,case['previous'],settings,
                                         CAMPAIGN_MODEL,case['previous_features'])
                record=dict(case_id=case['id'],status='CHECKING',preferences=result.preferences,
                            missing=result.missing,unsupported_features=result.unsupported_features,model=result.model)
                report['cases'].append(record)
                check_case(result,case)
                record['status']='PASSED'
                LOG.info(json.dumps(dict(event='model_continuity_case',case_id=case['id'],status='PASSED')))
                if first is None:
                    first=result
            client=bigquery.Client(project=PROJECT,location=LOCATION)
            gcs=storage.Client(project=PROJECT)
            preview,brief,bundle=generate_preview(client,gcs,model,first.request(),
                json.loads(CAMPAIGN_CONFIG.read_text()),json.loads(SALES_CONFIG.read_text()),
                config,json.loads(BANNER_CONFIG.read_text()))
            if (not brief['approval_recheck_passed'] or not preview.metadata['approval_recheck_passed']
                    or hashlib.sha256(preview.png).hexdigest() != preview.metadata['sha256']
                    or not all(preview.metadata['validation'].values())):
                raise ValueError('Candidate preview did not pass final evidence/image validation')
            report.update(status='PASSED',planner=brief['model'],
                preview=dict(canonical_sku=brief['canonical_sku'],metadata=preview.metadata,
                             evidence_references=len(brief['evidence_index']),original_images=len(bundle.image_bytes),
                             review_status='NOT_REVIEWED',registered=False))
            (output/'banner.png').write_bytes(preview.png)
            (output/'campaign_brief.json').write_text(json.dumps(brief,indent=2,default=json_value))
            (output/'campaign_brief.txt').write_text(render_brief(brief))
    except Exception as error:
        report.update(status='FAILED',error_type=type(error).__name__)
        raise
    finally:
        if client is not None:
            client.close()
        if gcs is not None:
            gcs.close()
        report['duration_seconds']=round(time.monotonic()-started,3)
        (output/'validation.json').write_text(json.dumps(report,indent=2,default=json_value))
    return report


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    report=run_validation(args.output_dir)
    print(json.dumps(dict(status=report['status'],model=report['candidate'],location=report['location'],
        cases=len(report['cases']),evidence_references=report['preview']['evidence_references'],output=str(args.output_dir))))


if __name__ == '__main__':
    main()
