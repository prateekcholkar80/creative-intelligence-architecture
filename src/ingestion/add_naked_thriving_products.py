"""Explicit operator-only, append-only import of three user-authorized N&T SKUs.

Never updates the original catalog, creates claims or enables serving. The original
fixed native slot plus one explicit positively-rolled-back corrected slot are the
only attempts; changed plans cannot mint arbitrary retries.
Other administrative catalog writers must remain quiescent during this operation.
"""
import argparse
from datetime import datetime
from decimal import Decimal
import hashlib,io,json,re
from pathlib import Path
from uuid import uuid4
from PIL import Image
from google.cloud import bigquery,storage
from google.api_core.exceptions import NotFound,Conflict,PreconditionFailed
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION,settings
from src.application.sharing_jobs import matches_sharing_job,failed_sharing_job_status
from src.application.sharing_coordinator_contract import SharingCommitStatus

ROOT=Path(__file__).resolve().parents[2]
MANIFEST=ROOT/'data/curated/naked_and_thriving_catalog_v1.json'
EVIDENCE=ROOT/'data/generated/campaign_e1_diagnostics'
BUCKET='creative-brand-memory-'+PROJECT
SOURCE='NAKED_AND_THRIVING_OFFICIAL'
TARGETS={'CI-SKU-0021':('Renew Resurfacing Night Serum',30,'44455352205558'),
    'CI-SKU-0022':('Rejuvenate Restorative Face Oil',35,'44455299023094'),
    'CI-SKU-0023':('Prevent Anti-Aging Face Oil',35,'44455107330294')}
JOB_ID='nt_catalog_additions_v1_global'
CORRECTED_JOB_ID='nt_catalog_additions_v2_global'
MAX_BYTES=16*1024*1024
SCHEMAS={'product_master': (('sku', 'STRING', 'NULLABLE'), ('brand', 'STRING', 'NULLABLE'), ('product_name', 'STRING', 'NULLABLE'), ('category', 'STRING', 'NULLABLE'), ('subcategory', 'STRING', 'NULLABLE'), ('product_type', 'STRING', 'NULLABLE'), ('description', 'STRING', 'NULLABLE'), ('size_value', 'NUMERIC', 'NULLABLE'), ('size_unit', 'STRING', 'NULLABLE'), ('price', 'NUMERIC', 'NULLABLE'), ('currency', 'STRING', 'NULLABLE'), ('skin_types', 'STRING', 'REPEATED'), ('skin_concerns', 'STRING', 'REPEATED'), ('benefits', 'STRING', 'REPEATED'), ('usage_instructions', 'STRING', 'NULLABLE'), ('country_of_origin', 'STRING', 'NULLABLE'), ('launch_date', 'DATE', 'NULLABLE'), ('product_status', 'STRING', 'NULLABLE'), ('primary_image_uri', 'STRING', 'NULLABLE'), ('source_system', 'STRING', 'NULLABLE'), ('source_product_id', 'STRING', 'NULLABLE'), ('created_at', 'TIMESTAMP', 'NULLABLE'), ('updated_at', 'TIMESTAMP', 'NULLABLE')), 'ingredients': (('ingredient_id', 'STRING', 'NULLABLE'), ('inci_name', 'STRING', 'NULLABLE'), ('common_name', 'STRING', 'NULLABLE'), ('ingredient_category', 'STRING', 'NULLABLE'), ('primary_function', 'STRING', 'NULLABLE'), ('description', 'STRING', 'NULLABLE'), ('typical_benefits', 'STRING', 'REPEATED'), ('related_skin_concerns', 'STRING', 'REPEATED'), ('source_system', 'STRING', 'NULLABLE'), ('source_reference', 'STRING', 'NULLABLE'), ('created_at', 'TIMESTAMP', 'NULLABLE'), ('updated_at', 'TIMESTAMP', 'NULLABLE')), 'product_ingredients': (('sku', 'STRING', 'NULLABLE'), ('ingredient_id', 'STRING', 'NULLABLE'), ('ingredient_sequence', 'INTEGER', 'NULLABLE'), ('concentration_percent', 'NUMERIC', 'NULLABLE'), ('concentration_text', 'STRING', 'NULLABLE'), ('is_active_ingredient', 'BOOLEAN', 'NULLABLE'), ('is_hero_ingredient', 'BOOLEAN', 'NULLABLE'), ('ingredient_role', 'STRING', 'NULLABLE'), ('source_ingredient_text', 'STRING', 'NULLABLE'), ('created_at', 'TIMESTAMP', 'NULLABLE'), ('updated_at', 'TIMESTAMP', 'NULLABLE')), 'product_assets': (('asset_id', 'STRING', 'NULLABLE'), ('sku', 'STRING', 'NULLABLE'), ('asset_type', 'STRING', 'NULLABLE'), ('asset_role', 'STRING', 'NULLABLE'), ('gcs_uri', 'STRING', 'NULLABLE'), ('file_name', 'STRING', 'NULLABLE'), ('mime_type', 'STRING', 'NULLABLE'), ('width', 'INTEGER', 'NULLABLE'), ('height', 'INTEGER', 'NULLABLE'), ('alt_text', 'STRING', 'NULLABLE'), ('asset_description', 'STRING', 'NULLABLE'), ('is_primary', 'BOOLEAN', 'NULLABLE'), ('display_order', 'INTEGER', 'NULLABLE'), ('source_system', 'STRING', 'NULLABLE'), ('source_url', 'STRING', 'NULLABLE'), ('created_at', 'TIMESTAMP', 'NULLABLE'), ('updated_at', 'TIMESTAMP', 'NULLABLE'))}


def _default(value):
    if isinstance(value,Decimal):return str(value)
    if isinstance(value,datetime):return value.isoformat()
    raise TypeError('Unsupported import value')


def encoded(value):
    text=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False,
        default=_default)
    if len(text.encode())>MAX_BYTES:raise ValueError('Import capture exceeds limit')
    return text


def split_ingredients(text):
    if type(text) is not str or not 1<=len(text)<=4000:raise ValueError('Ingredient source required')
    parts=[];start=0;depth=0
    for i,char in enumerate(text):
        if char=='(':depth+=1
        elif char==')':
            depth-=1
            if depth<0:raise ValueError('Unbalanced ingredient list')
        elif char==',' and depth==0:parts.append(text[start:i].strip());start=i+1
    parts.append(text[start:].strip())
    if depth or not 1<=len(parts)<=128 or any(not part for part in parts):raise ValueError('Invalid ingredient sequence')
    return parts


def prepare_rows(manifest,images):
    products=manifest['products']
    if (manifest['version']!='nt-catalog-additions-v1' or manifest['claim_permissions_created'] is not False
        or len(products)!=3 or {p['canonical_sku'] for p in products}!=set(TARGETS)):
        raise ValueError('Exact authorized import required')
    stamp=products[0]['fetched_at'];datetime.fromisoformat(stamp)
    rows={name:[] for name in SCHEMAS};ingredients={}
    for p in products:
        sku=p['canonical_sku'];name,size,variant=TARGETS[sku];image=images[sku]
        if (p['product_name']!=name or p['brand']!='Naked & Thriving' or p['size_ml']!=size
            or p['source_variant_id']!=variant or p['source_system']!=SOURCE or p['formula_version'] is not None
            or not p['source_product_url'].startswith('https://www.nakedandthriving.com/products/')
            or image['source_variant_id']!=variant or image['source_product_id']!=p['source_product_id']
            or image['mime_type']!='image/jpeg' or not re.fullmatch('[0-9a-f]{64}',image['sha256'])
            or type(p['observed_price_minor']) is not int or p['observed_price_minor']<0 or p['currency']!='USD'):
            raise ValueError('Product/variant/source pins conflict')
        uri=f'gs://{BUCKET}/products/{sku}/primary.jpg'
        rows['product_master'].append(dict(sku=sku,brand=p['brand'],product_name=name,category='Skincare',subcategory=None,
            product_type=p['product_type'],description='Official source product metadata; no benefit-claim approval. Formula version unverified.',
            size_value=str(size),size_unit='mL',price=str(Decimal(p['observed_price_minor'])/Decimal(100)),currency='USD',
            skin_types=[],skin_concerns=[],benefits=[],usage_instructions=p['usage_instructions'],country_of_origin=None,
            launch_date=None,product_status='ACTIVE',primary_image_uri=uri,source_system=SOURCE,
            source_product_id=p['source_product_id']+':variant:'+variant,created_at=stamp,updated_at=stamp))
        raw=p['source_ingredient_text']
        for seq,ingredient in enumerate(split_ingredients(raw),1):
            inci=ingredient.rstrip('*').strip();key='ING-NT-'+hashlib.sha256(inci.casefold().encode()).hexdigest()[:16].upper()
            ingredients.setdefault(key,dict(ingredient_id=key,inci_name=inci,common_name=None,ingredient_category=None,
                primary_function=None,description=None,typical_benefits=[],related_skin_concerns=[],source_system=SOURCE,
                source_reference=p['source_product_url'],created_at=stamp,updated_at=stamp))
            rows['product_ingredients'].append(dict(sku=sku,ingredient_id=key,ingredient_sequence=seq,
                concentration_percent=None,concentration_text=None,is_active_ingredient=None,is_hero_ingredient=None,
                ingredient_role=None,source_ingredient_text=raw,created_at=stamp,updated_at=stamp))
        rows['product_assets'].append(dict(asset_id='AST-'+sku+'-PRIMARY',sku=sku,asset_type='IMAGE',
            asset_role='PRIMARY_PRODUCT_IMAGE',gcs_uri=uri,file_name='primary.jpg',mime_type='image/jpeg',
            width=image['width'],height=image['height'],alt_text='Naked & Thriving '+name,
            asset_description='Unaltered official pack image; clinical-results badge is not a claim-use approval.',
            is_primary=True,display_order=1,source_system=SOURCE,source_url=image['source_url'],created_at=stamp,updated_at=stamp))
    rows['ingredients']=sorted(ingredients.values(),key=lambda r:r['ingredient_id'])
    for table,values in rows.items():
        if any(set(row)!={n for n,_,_ in SCHEMAS[table]} for row in values):raise ValueError('Row schema differs')
    encoded(rows);return rows


def preflight(client):
    if client.project!=PROJECT or client.location!=LOCATION or client.default_query_job_config is not None:
        raise ValueError('Fixed project/region/client required')
    for name,schema in SCHEMAS.items():
        t=client.get_table(f'{PROJECT}.ci_product.{name}',retry=None,timeout=20)
        if (t.table_type!='TABLE' or t.location!=LOCATION or t.expires is not None or t.time_partitioning
            or t.range_partitioning or t.clustering_fields
            or tuple((f.name,f.field_type,f.mode) for f in t.schema)!=schema):raise ValueError('Catalog schema/layout conflict')


def snapshot(client):
    sql=' UNION ALL '.join(f"SELECT '{n}' table_name,TO_JSON_STRING(t) row_json,COUNT(*) OVER() row_count FROM `{PROJECT}.ci_product.{n}` t" for n in SCHEMAS)+' ORDER BY table_name,row_json LIMIT 2049'
    cfg=settings([]);native='nt_catalog_snapshot_'+uuid4().hex
    j=client.query(sql,job_config=cfg,job_id=native,location=LOCATION,retry=None,job_retry=None,timeout=20)
    values=list(j.result(timeout=60,max_results=2049,page_size=2049,retry=None,job_retry=None))
    original=client.get_job(native,location=LOCATION,retry=None,timeout=20)
    if not matches_sharing_job(original,job_id=native,sql=sql,config=cfg) or original.state!='DONE' or original.error_result:
        raise ValueError('Native catalog snapshot mismatch')
    if len(values)>=2049:raise ValueError('Catalog capture overflow')
    groups={name:[] for name in SCHEMAS};counts={}
    for row in values:
        if row['table_name'] not in groups or type(row['row_count']) is not int:raise ValueError('Unexpected snapshot scope')
        name=row['table_name'];counts.setdefault(name,row['row_count'])
        if counts[name]!=row['row_count']:raise ValueError('Snapshot count conflict')
        groups[name].append(row['row_json'])
    if any(len(groups[n])!=counts.get(n,0) for n in groups):raise ValueError('Partial catalog capture')
    encoded(groups);return groups,native


def write_spec(baseline,rows):
    if set(baseline)!=set(SCHEMAS) or set(rows)!=set(SCHEMAS):raise ValueError('Four complete tables required')
    existing=[json.loads(r) for r in baseline['product_master']]
    if (len(existing)!=20 or {r['sku'] for r in existing}!={f'CI-SKU-{i:04}' for i in range(1,21)}
        or any(r['source_system']!= 'BEAUTYFEEDS' for r in existing)):
        raise ValueError('Original 20-SKU catalog required; no overwrite/import replay')
    for table,key in (('ingredients','ingredient_id'),('product_assets','asset_id')):
        if {json.loads(row)[key] for row in baseline[table]} & {row[key] for row in rows[table]}:
            raise ValueError('New ingredient/asset ID already exists; never overwrite')
    if any(json.loads(row)['sku'] in TARGETS for row in baseline['product_ingredients']):
        raise ValueError('Orphan target ingredient rows already exist')
    params=[];parts=[]
    for name,schema in SCHEMAS.items():
        def projection():
            expressions=[]
            for column,kind,mode in schema:
                value=f"JSON_VALUE(item,'$.{column}')"
                if mode=='REPEATED':value=f"JSON_VALUE_ARRAY(item,'$.{column}')"
                elif kind!='STRING':value=f'CAST({value} AS {"INT64" if kind=="INTEGER" else "BOOL" if kind=="BOOLEAN" else kind})'
                expressions.append(value+' AS '+column)
            return ','.join(expressions)
        for prefix,items in (('old',baseline[name]),('new',[encoded(r) for r in rows[name]])):
            key=prefix+'_'+name;params.append(bigquery.ArrayQueryParameter(key,'STRING',items))
            parts.append(f'CREATE TEMP TABLE {key} AS SELECT {projection()} FROM UNNEST(@{key}) item;')
    parts.append('BEGIN\nBEGIN TRANSACTION;')
    for name,schema in SCHEMAS.items():
        table=f'`{PROJECT}.ci_product.{name}`'
        actual=f'SELECT TO_JSON_STRING(t) row_json,COUNT(*) copies FROM {table} t GROUP BY row_json'
        expected=f'SELECT TO_JSON_STRING(t) row_json,COUNT(*) copies FROM old_{name} t GROUP BY row_json'
        parts.append(f"ASSERT NOT EXISTS(({actual}) EXCEPT DISTINCT ({expected})) AND NOT EXISTS(({expected}) EXCEPT DISTINCT ({actual})) AS 'NT_CATALOG_STALE_{name}';")
    for name,schema in SCHEMAS.items():
        columns=','.join(n for n,_,_ in schema)
        parts.append(f'INSERT `{PROJECT}.ci_product.{name}` ({columns}) SELECT {columns} FROM new_{name};')
    parts+=['COMMIT TRANSACTION;\nEXCEPTION WHEN ERROR THEN\nROLLBACK TRANSACTION;\nRAISE;\nEND;',"SELECT 'APPLIED' outcome;"]
    cfg=settings(params);cfg.maximum_bytes_billed=768*1024*1024;cfg.job_timeout_ms=120000
    encoded(cfg.to_api_repr());return '\n'.join(parts),cfg


def authorize_corrected_submission(client,original_plan):
    original=client.get_job(JOB_ID,location=LOCATION,retry=None,timeout=20)
    cfg=bigquery.QueryJobConfig.from_api_repr(original_plan['config'])
    if not matches_sharing_job(original,job_id=JOB_ID,sql=original_plan['sql'],config=cfg):
        raise ValueError('Original failed import is not correlated')
    status=failed_sharing_job_status(client,job_id=JOB_ID,timeout_seconds=20,parent=original)
    if status is not SharingCommitStatus.NOT_COMMITTED:raise ValueError('Positive original rollback is required')
    current,native=snapshot(client)
    if current!=original_plan['baseline']:raise ValueError('Original catalog changed after rollback')
    return dict(original_job_id=JOB_ID,status=status.value,unchanged_snapshot_job=native)


def upload_images(client,images):
    pins={}
    for sku,image in images.items():
        blob=client.bucket(BUCKET).blob(f'products/{sku}/primary.jpg')
        try:blob.reload(retry=None,timeout=20)
        except NotFound:
            raw=Path(image['local_path']).read_bytes()
            if hashlib.sha256(raw).hexdigest()!=image['sha256'] or len(raw)!=image['size_bytes']:raise ValueError('Local image pin mismatch')
            with Image.open(io.BytesIO(raw)) as img:
                if img.format!='JPEG' or img.size!=(image['width'],image['height']):raise ValueError('Image format/dimension mismatch')
            try:blob.upload_from_string(raw,content_type='image/jpeg',if_generation_match=0,retry=None,timeout=30)
            except PreconditionFailed:pass
            blob.reload(retry=None,timeout=20)
        generation=int(blob.generation)
        stored=blob.download_as_bytes(if_generation_match=generation,retry=None,timeout=30)
        if hashlib.sha256(stored).hexdigest()!=image['sha256'] or len(stored)!=image['size_bytes'] or blob.content_type!='image/jpeg':
            raise ValueError('Pinned GCS bytes differ; never overwrite')
        with Image.open(io.BytesIO(stored)) as img:
            if img.format!='JPEG' or img.size!=(image['width'],image['height']):raise ValueError('Stored image format/dimension mismatch')
        pins[sku]=dict(gcs_uri=f'gs://{BUCKET}/{blob.name}',generation=generation,sha256=image['sha256'],size_bytes=len(stored))
    return pins


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true')
    parser.add_argument('--verified-rollback-retry',action='store_true',help='One explicit corrected slot; requires correlated original rollback and unchanged catalog')
    args=parser.parse_args();native_id=CORRECTED_JOB_ID if args.verified_rollback_retry else JOB_ID
    manifest=json.loads(MANIFEST.read_text());images={sku:json.loads((EVIDENCE/(sku+'_image.json')).read_text()) for sku in TARGETS}
    additions=prepare_rows(manifest,images);original_path=EVIDENCE/'catalog_import_plan_v1.json'
    plan_path=EVIDENCE/('catalog_import_plan_v2.json' if args.verified_rollback_retry else 'catalog_import_plan_v1.json')
    with bigquery.Client(project=PROJECT,location=LOCATION) as client:
        preflight(client)
        if plan_path.exists():
            plan=json.loads(plan_path.read_text());baseline=plan['baseline']
            if plan['rows']!=additions:raise ValueError('Frozen plan differs; no new import slot')
        else:
            if args.verified_rollback_retry:
                original_plan=json.loads(original_path.read_text());baseline=original_plan['baseline']
                guard=authorize_corrected_submission(client,original_plan);native=guard['unchanged_snapshot_job']
            else:baseline,native=snapshot(client)
            sql,cfg=write_spec(baseline,additions)
            plan=dict(baseline=baseline,rows=additions,snapshot_job=native,sql=sql,config=cfg.to_api_repr())
            plan_path.write_text(json.dumps(plan,indent=2)+'\n')
        sql,cfg=write_spec(baseline,additions)
        if plan['sql']!=sql or plan['config']!=cfg.to_api_repr():raise ValueError('Frozen SQL/config differs')
        if not args.apply:
            print(json.dumps(dict(status='PREPARED_NO_WRITES',new_rows={n:len(r) for n,r in additions.items()},job_id=native_id)));return
        pins=upload_images(storage.Client(project=PROJECT),images)
        try:job=client.get_job(native_id,location=LOCATION,retry=None,timeout=20)
        except NotFound:
            if args.verified_rollback_retry:authorize_corrected_submission(client,json.loads(original_path.read_text()))
            try:job=client.query(sql,job_config=cfg,job_id=native_id,location=LOCATION,retry=None,job_retry=None,timeout=20)
            except Conflict:job=client.get_job(native_id,location=LOCATION,retry=None,timeout=20)
        if not matches_sharing_job(job,job_id=native_id,sql=sql,config=cfg):raise ValueError('Original native slot conflict; no resubmission')
        result=[dict(row) for row in job.result(timeout=120,retry=None,job_retry=None,max_results=2)]
        if result!=[dict(outcome='APPLIED')]:raise ValueError('No exact applied result')
        after,read_native=snapshot(client)
        for name in SCHEMAS:
            expected=sorted(baseline[name]+[encoded(row) for row in additions[name]])
            # Normalize native JSON timestamp/numeric formatting through SQL's
            # typed projection by comparing parsed values with scalar normalization.
            def normalized(text):
                row=json.loads(text)
                for col,kind,mode in SCHEMAS[name]:
                    if row.get(col) is not None and kind=='TIMESTAMP':row[col]=datetime.fromisoformat(row[col].replace('Z','+00:00')).isoformat()
                    elif row.get(col) is not None and kind=='NUMERIC':row[col]=str(Decimal(str(row[col])).normalize())
                return encoded(row)
            if sorted(map(normalized,after[name]))!=sorted(map(normalized,expected)):raise ValueError('Full post-import preservation differs')
        report=dict(status='VERIFIED_APPLIED',native_job_id=native_id,verification_job_id=read_native,
            new_rows={n:len(r) for n,r in additions.items()},total_rows={n:len(r) for n,r in after.items()},
            original_full_rows_preserved=True,gcs_pins=pins,claim_permissions_created=False,serving_enabled=False)
        (EVIDENCE/'catalog_import_verification_20261015.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))


if __name__=='__main__':main()
