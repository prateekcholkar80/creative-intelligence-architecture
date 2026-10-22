"""Read-only official-product source capture for evaluation, never release authority."""
from dataclasses import dataclass
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from uuid import uuid4
import hashlib,io,json
from PIL import Image
from google.cloud import bigquery
from src.application.workflow_contract import _require,canonical_json_bytes
from src.application.campaign_acceptance_contract import _hash
from src.application.campaign_planner_projection import parse_json
from src.application.sharing_publication_codec import normalize
from src.application.sharing_jobs import matches_sharing_job
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION

VERSION='evaluation-official-product-sources-v1'
SKUS=('CI-SKU-0021','CI-SKU-0022','CI-SKU-0023')
BUCKET='creative-brand-memory-'+PROJECT
MAX_IMAGE_BYTES=10*1024**2
SQL=f'''SELECT CURRENT_TIMESTAMP() observed_at,
 ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.ci_product.product_master` t WHERE sku=@sku LIMIT 2) products,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_product.product_master` WHERE sku=@sku) product_count,
 ARRAY(SELECT TO_JSON_STRING(t) FROM `{PROJECT}.ci_product.product_assets` t WHERE sku=@sku AND is_primary LIMIT 2) assets,
 (SELECT COUNT(*) FROM `{PROJECT}.ci_product.product_assets` WHERE sku=@sku AND is_primary) asset_count'''

@dataclass(frozen=True,slots=True)
class EvaluationProductSources:
    manifest:dict
    image_bytes:bytes


def _capture(client,sku,clock):
    started=clock();cfg=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('sku','STRING',sku)],
        use_legacy_sql=False,use_query_cache=False,create_session=False,maximum_bytes_billed=64*1024**2,job_timeout_ms=60000)
    job_id='evaluation_product_sources_'+uuid4().hex
    job=client.query(SQL,job_id=job_id,job_config=cfg,location=LOCATION,retry=None,job_retry=None,timeout=20)
    rows=tuple(normalize(r) for r in job.result(timeout=60,max_results=2,retry=None,job_retry=None));ended=clock()
    _require(len(rows)==1 and matches_sharing_job(job,job_id=job_id,sql=SQL,config=cfg) and job.state=='DONE' and job.error_result is None,
        'EVALUATION_PRODUCT_NATIVE_UNVERIFIED')
    row=rows[0];_require(started<=row['observed_at']<=ended<started+timedelta(seconds=60),'EVALUATION_PRODUCT_CAPTURE_WINDOW')
    _require(set(row)=={'observed_at','products','product_count','assets','asset_count'} and row['product_count']==row['asset_count']==1
        and type(row['product_count']) is type(row['asset_count']) is int and len(row['products'])==len(row['assets'])==1,
        'EVALUATION_PRODUCT_SOURCE_COVERAGE')
    product=parse_json(row['products'][0]);asset=parse_json(row['assets'][0]);uri=f'gs://{BUCKET}/products/{sku}/primary.jpg'
    _require(product['sku']==asset['sku']==sku and product['source_system']==asset['source_system']=='NAKED_AND_THRIVING_OFFICIAL'
        and product['brand']=='Naked & Thriving' and product['product_status']=='ACTIVE'
        and type(product['product_name']) is str and product['product_name'].strip()==product['product_name'] and bool(product['product_name'])
        and product['primary_image_uri']==asset['gcs_uri']==uri and asset['asset_id']==f'AST-{sku}-PRIMARY'
        and asset['asset_type']=='IMAGE' and asset['asset_role']=='PRIMARY_PRODUCT_IMAGE' and asset['is_primary'] is True
        and asset['mime_type']=='image/jpeg' and type(asset['width']) is type(asset['height']) is int
        and 0<asset['width']*asset['height']<=20_000_000 and asset['width']>0 and asset['height']>0,
        'EVALUATION_PRODUCT_SOURCE_INVALID')
    size=Decimal(str(product['size_value']));_require(size.is_finite() and 0<size<=10000 and product['size_unit']=='mL','EVALUATION_PRODUCT_SIZE')
    return product,asset,job_id,row['observed_at']


def capture_official_product_sources(client,gcs,sku,*,clock=lambda:datetime.now(timezone.utc)):
    """Caller must recheck IAP/current application access around this source read."""
    _require(sku in SKUS and client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,
        'EVALUATION_PRODUCT_CLIENT_SCOPE')
    product,asset,before_id,observed=_capture(client,sku,clock)
    blob=gcs.bucket(BUCKET).blob(f'products/{sku}/primary.jpg');blob.reload(retry=None,timeout=20)
    generation=blob.generation
    _require(type(generation) is int and generation>0 and blob.content_type==asset['mime_type']
        and type(blob.size) is int and 0<blob.size<=MAX_IMAGE_BYTES,'EVALUATION_PRODUCT_OBJECT_METADATA')
    raw=blob.download_as_bytes(if_generation_match=generation,retry=None,timeout=30,checksum='auto')
    _require(type(raw) is bytes and len(raw)==blob.size and len(raw)<=MAX_IMAGE_BYTES,'EVALUATION_PRODUCT_IMAGE_SIZE')
    with Image.open(io.BytesIO(raw)) as image:
        _require(image.format=='JPEG' and image.size==(asset['width'],asset['height']) and getattr(image,'n_frames',1)==1,
            'EVALUATION_PRODUCT_IMAGE_DECODE')
        image.verify()
    product_after,asset_after,after_id,ended=_capture(client,sku,clock)
    _require(product_after==product and asset_after==asset,'EVALUATION_PRODUCT_SOURCE_CHANGED')
    blob.reload(retry=None,timeout=20)
    _require(blob.generation==generation and blob.content_type==asset['mime_type'] and blob.size==len(raw),
        'EVALUATION_PRODUCT_OBJECT_CHANGED')
    _require(ended<observed+timedelta(seconds=120),'EVALUATION_PRODUCT_CAPTURE_EXPIRED')
    identity=dict(sku=sku,brand=product['brand'],product_name=product['product_name'],full_size_ml=format(Decimal(str(product['size_value'])).normalize(),'f'))
    value=dict(version=VERSION,identity=identity,identity_sha256=_hash(identity),native_job_ids=[before_id,after_id],
        product_row_sha256=_hash(product),asset_row_sha256=_hash(asset),source_url=asset['source_url'],
        image=dict(asset_id=asset['asset_id'],gcs_uri=asset['gcs_uri'],generation=str(generation),sha256=hashlib.sha256(raw).hexdigest(),
            mime_type=asset['mime_type'],width=asset['width'],height=asset['height'],byte_count=len(raw)),
        byte_binding='CAPTURED_GCS_GENERATION_AND_SHA256_NOT_PRIOR_BIGQUERY_DIGEST',
        observed_at=observed.isoformat(),checked_at=ended.isoformat(),source_asset_description=asset['asset_description'],
        visual_claim_review='REQUIRED_ACTUAL_IMAGE_REVIEW',claim_use_authorized=False,brand_approval_verified=False,
        qualifying=False,release_allowed=False)
    return EvaluationProductSources(dict(value,sha256=_hash(value)),raw)
