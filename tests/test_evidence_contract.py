"""Unpublished evidence projections; no source clients, vectors, bytes or writes."""
from copy import deepcopy
from datetime import timedelta
import hashlib
import unittest
from unittest.mock import patch

from src.application.evidence_contract import (
    CHUNK_TYPES, EVIDENCE_VERSION, SOURCE_TABLES, evidence_reference_id, evidence_row_id,
    validate_evidence_bundle, validate_evidence_snapshot,
)
from src.application.workflow_contract import ARTIFACT_BUCKET, ContractError, canonical_json_bytes
from src.application.workflow_schema import TABLES
from src.generators.sales_generator import PROJECT
from tests.test_execution_contract import execution
from tests.test_output_contract import context
from tests.test_workflow_contract import CONVERSATION, TIME, digest

SKU = 'CI-SKU-0001'
SNAPSHOT = digest('knowledge snapshot')
CAMPAIGN = f'SYN-CMP-20260101-{SKU}-01'


def reference(table, key):
    return f'{PROJECT}.{table}#{key}'


def product(sku=SKU):
    return dict(sku=sku, brand='Fixture brand', product_name='Fixture product', product_type='SERUM',
        price='25.000000000', currency='USD', source_system='BEAUTYFEEDS',
        primary_image_uri=f'gs://{ARTIFACT_BUCKET}/products/{sku}/primary.jpg')


def rule():
    return dict(rule_id='POC-FIXTURE', brand='*', scope='PROJECT_POC', authority='PROJECT_POC_POLICY',
        rule_status='ACTIVE_FOR_POC', rule_text='Use identity wording only.', source_system='SYNTHETIC_POC',
        source_reference='config/brand_memory.json#rules')


def claim(sku=SKU):
    return dict(claim_id=f'CLM-{sku}', sku=sku, claim_type='PRODUCT_NAME', claim_text='Fixture product',
        evidence_reference=reference('ci_product.product_master', f'sku={sku}&field=product_name'),
        evidence_excerpt='Fixture product', evidence_source_system='BEAUTYFEEDS', approval_status='APPROVED',
        approval_scope='SYNTHETIC_POC_ONLY', approved_by='SYNTHETIC_POC_REVIEWER',
        approval_reference='config/brand_memory.json#review_fixture', approved_at=TIME.isoformat(),
        source_system='SYNTHETIC_POC')


def chunk(kind='PRODUCT_KNOWLEDGE', sku=SKU, section='colors'):
    source, status, scope = CHUNK_TYPES[kind]
    global_kind = kind in {'POC_RULE', 'WEBSITE_VISUAL_REFERENCE'}
    refs = {
        'PRODUCT_KNOWLEDGE': reference('ci_product.product_master', f'sku={sku}'),
        'INGREDIENT_KNOWLEDGE': reference('ci_product.product_ingredients', f'sku={sku}'),
        'POC_APPROVED_CLAIM': reference('ci_brand_memory.product_claims', f'claim_id=CLM-{sku}'),
        'POC_RULE': reference('ci_brand_memory.brand_guidelines', 'rule_id=POC-FIXTURE'),
        'WEBSITE_VISUAL_REFERENCE': f'data/research/naked_and_thriving_visual_reference.json#{section}',
        'CAMPAIGN_CREATIVE': reference('ci_creative.campaign_master', f'campaign_id={CAMPAIGN}'),
    }
    text = 'Fixture product\nUse identity wording only.'
    evidence = [claim(sku)['evidence_reference']] if kind == 'POC_APPROVED_CLAIM' else [refs[kind]]
    return dict(chunk_id=f'KCH-{kind}-{section}', snapshot_id=SNAPSHOT, chunk_type=kind,
        brand='Naked & Thriving' if kind == 'WEBSITE_VISUAL_REFERENCE' else '*' if global_kind else 'Fixture brand',
        sku=None if global_kind else sku, campaign_id=CAMPAIGN if kind == 'CAMPAIGN_CREATIVE' else None,
        asset_id=f'SYN-AST-{CAMPAIGN}-template-v1' if kind == 'CAMPAIGN_CREATIVE' else None,
        claim_id=f'CLM-{sku}' if kind == 'POC_APPROVED_CLAIM' else None,
        chunk_text=text, source_system=source, source_reference=refs[kind], evidence_references=evidence,
        image_uris=[] if global_kind or kind == 'POC_APPROVED_CLAIM' else [product(sku)['primary_image_uri']],
        retrieval_status=status, approval_scope=scope, content_sha256=hashlib.sha256(text.encode()).hexdigest())


def sales(sku=SKU):
    return dict(sku=sku, source_system='SYNTHETIC_POC', currency='USD', channel='ECOMMERCE',
        start_date='2026-01-01', end_date='2026-01-02', units_sold=10, orders=8, row_count=2, day_count=2,
        gross_sales='250', discount_amount='10', refund_amount='5', net_sales='235')


def performance():
    return dict(sku=SKU, source_system='SYNTHETIC_POC', currency='USD', campaign_id=CAMPAIGN,
        asset_id=f'SYN-AST-{CAMPAIGN}-template-v1', start_date='2026-01-01', end_date='2026-01-02',
        attribution_model='SYNTHETIC_SALES_CAPPED_ATTRIBUTION', impressions=1000, clicks=100, conversions=10,
        attributed_units=12, performance_rows=2, performance_days=2, budget='50', spend='50', attributed_revenue='200',
        ctr='0.100000000', conversion_rate='0.100000000', roas='4.000000000')


def image(kind='PRODUCT_IMAGE', sku=SKU):
    sha = digest('fixture image')
    asset = f'AST-{sku}-PRIMARY' if kind == 'PRODUCT_IMAGE' else (
        f'SYN-AST-{CAMPAIGN}-template-v1' if kind == 'CAMPAIGN_IMAGE' else f'SYN-GEN-{sku}-{sha}')
    path = f'products/{sku}/primary.jpg' if kind == 'PRODUCT_IMAGE' else (
        f'campaigns/{CAMPAIGN}/template-v1/{sha}.png' if kind == 'CAMPAIGN_IMAGE' else
        f'generated_campaigns/{sku}/grounded-banner-preview-v1/{sha}.png')
    return dict(sku=sku, asset_id=asset, campaign_id=CAMPAIGN if kind == 'CAMPAIGN_IMAGE' else None,
        source_system='BEAUTYFEEDS' if kind == 'PRODUCT_IMAGE' else 'SYNTHETIC_POC',
        gcs_uri=f'gs://{ARTIFACT_BUCKET}/{path}', gcs_generation='1730000000000000', content_sha256=sha,
        mime_type='image/jpeg' if kind == 'PRODUCT_IMAGE' else 'image/png', width=1200, height=628,
        description='Fixture visual reference.', embedding_id='EMB-fixture', model_id='multimodalembedding@001',
        model_location='asia-south1', dimension=512, task_type='IMAGE', snapshot_id=None if kind == 'PRODUCT_IMAGE' else SNAPSHOT,
        original_product_image=dict(asset_id=f'AST-{sku}-PRIMARY', gcs_uri=product(sku)['primary_image_uri'],
            gcs_generation='1730000000000000', content_sha256=sha) if kind == 'GENERATED_IMAGE' else None,
        usage_scope='SOURCE_PRODUCT_IMAGE' if kind == 'PRODUCT_IMAGE' else 'SYNTHETIC_POC_ONLY' if kind == 'CAMPAIGN_IMAGE'
            else 'NOT_HISTORICAL_PERFORMANCE_OR_BENEFIT_EVIDENCE',
        review=dict(review_id='fixture-review', revision=2, status='APPROVED_FOR_POC',
                    approval_scope='POC_VISUAL_REUSE_ONLY') if kind == 'GENERATED_IMAGE' else None)


def row(kind, record, *, ref=None):
    result = dict.fromkeys(f.name for f in TABLES['evidence_snapshots'].schema)
    attempt, req, parent = execution(sku=record.get('sku') or SKU)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id', 'attempt_id'):
        result[key] = attempt[key]
    table = 'ci_brand_memory.poc_visual_reference_chunks' if record.get('chunk_type') == 'WEBSITE_VISUAL_REFERENCE' else SOURCE_TABLES[kind]
    if kind == 'KNOWLEDGE_CHUNK':
        key = record['chunk_id']; ref = ref or record['source_reference']
    elif kind == 'PRODUCT':
        key = f'sku={record["sku"]}'
    elif kind == 'POC_RULE':
        key = f'rule_id={record["rule_id"]}'
    elif kind == 'POC_CLAIM':
        key = f'claim_id={record["claim_id"]}'; ref = ref or reference('ci_brand_memory.product_claims', key)
        result['approval_reference'] = record['approval_reference']
    elif kind == 'SALES_SUMMARY':
        key = (f'sku={record["sku"]}&channel=ECOMMERCE&source_system=SYNTHETIC_POC'
               f'&start_date={record["start_date"]}&end_date={record["end_date"]}')
    elif kind == 'CAMPAIGN_PERFORMANCE':
        key = (f'campaign_id={record["campaign_id"]}&asset_id={record["asset_id"]}&source_system=SYNTHETIC_POC'
               f'&start_date={record["start_date"]}&end_date={record["end_date"]}')
    else:
        key = f'asset_id={record["asset_id"]}'
        result.update(gcs_uri=record['gcs_uri'], gcs_generation=record['gcs_generation'], image_sha256=record['content_sha256'],
            embedding_model=record['model_id'], embedding_dimensions=record['dimension'])
        if kind == 'GENERATED_IMAGE':
            result.update(approval_reference=reference('ci_creative.generated_asset_reviews', 'review_id=fixture-review'), review_revision=2)
    ref = ref or reference(table, key)
    if kind == 'KNOWLEDGE_CHUNK' or kind.endswith('_IMAGE') or kind == 'CAMPAIGN_PERFORMANCE':
        result['knowledge_snapshot_id'] = record.get('snapshot_id') if kind != 'CAMPAIGN_PERFORMANCE' else SNAPSHOT
    if kind in {'CAMPAIGN_IMAGE', 'GENERATED_IMAGE'} or record.get('chunk_type') == 'CAMPAIGN_CREATIVE':
        result.update(rank=1, cosine_distance=0.3, ranking_version='EXACT_COSINE-v1',
            embedding_model='gemini-embedding-001' if kind == 'KNOWLEDGE_CHUNK' else 'multimodalembedding@001',
            embedding_dimensions=768 if kind == 'KNOWLEDGE_CHUNK' else 512)
    result.update(evidence_snapshot_id='test-evidence-snapshot', canonical_sku=req['canonical_sku'], source_kind=kind,
        source_table=f'{PROJECT}.{table}', source_record_id=key, source_system=record['source_system'],
        content_sha256=record.get('content_sha256') or hashlib.sha256(canonical_json_bytes(record)).hexdigest(),
        evidence_schema_version=EVIDENCE_VERSION, evidence_payload=dict(reference=ref, record=deepcopy(record)),
        validated_at=TIME+timedelta(seconds=1), created_at=TIME+timedelta(seconds=1))
    result['evidence_ref_id'] = evidence_row_id(result['source_table'], key, ref)
    return result


def complete_bundle():
    return [row('PRODUCT', product()), row('POC_RULE', rule()), row('PRODUCT_IMAGE', image()),
            row('SALES_SUMMARY', sales()), *[row('KNOWLEDGE_CHUNK', chunk(k))
                for k in ('PRODUCT_KNOWLEDGE', 'INGREDIENT_KNOWLEDGE', 'POC_RULE')]]


class EvidenceContractTests(unittest.TestCase):
    def setUp(self):
        self.attempt, self.req, self.job = execution()
        self.row = row('PRODUCT', product())

    def validate(self, candidate=None):
        validate_evidence_snapshot(self.row if candidate is None else candidate, request=self.req,
            job=self.job, attempt=self.attempt, conversation=CONVERSATION)

    def bundle(self, rows=None, output=None):
        validate_evidence_bundle(complete_bundle() if rows is None else rows, request=self.req, job=self.job,
            attempt=self.attempt, conversation=CONVERSATION, output=output)

    def rehash(self, candidate):
        record = candidate['evidence_payload']['record']
        candidate['content_sha256'] = hashlib.sha256(canonical_json_bytes(record)).hexdigest()
        return candidate

    def test_all_supported_sources(self):
        for kind, record in [('PRODUCT', product()), ('POC_RULE', rule()), ('POC_CLAIM', claim()),
                ('SALES_SUMMARY', sales()), ('CAMPAIGN_PERFORMANCE', performance()),
                *[(k, image(k)) for k in ('PRODUCT_IMAGE', 'CAMPAIGN_IMAGE', 'GENERATED_IMAGE')],
                *[('KNOWLEDGE_CHUNK', chunk(k)) for k in CHUNK_TYPES]]:
            with self.subTest(kind=kind, chunk=record.get('chunk_type')):
                self.validate(row(kind, record))

    def test_all_twenty_canonical_products(self):
        for number in range(1, 21):
            sku = f'CI-SKU-{number:04d}'
            self.attempt, self.req, self.job = execution(sku=sku)
            self.validate(row('PRODUCT', product(sku)))

    def test_exact_row_shape_and_required_fields(self):
        for mutate in (lambda r: r.pop('source_revision'), lambda r: r.update(raw_vector=[]),
                       lambda r: r.update(created_at=None), lambda r: r.update(created_at=TIME.isoformat())):
            candidate = deepcopy(self.row); mutate(candidate)
            with self.assertRaises(ContractError): self.validate(candidate)

    def test_scope_parent_and_sku_bindings(self):
        for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id', 'attempt_id', 'canonical_sku'):
            with self.subTest(key=key), self.assertRaises(ContractError): self.validate(self.row | {key: 'other'})

    def test_parent_validation_not_skipped(self):
        self.req['request_sha256'] = '0'*64
        with self.assertRaises(ContractError): self.validate()

    def test_time_order_deadline_and_closed_attempt(self):
        for change in (dict(validated_at=TIME), dict(validated_at=TIME+timedelta(seconds=2)),
                       dict(created_at=self.job['deadline_at']), dict(validated_at=TIME.replace(tzinfo=None))):
            with self.subTest(change=change), self.assertRaises(ContractError): self.validate(self.row | change)
        self.attempt, self.req, self.job = execution('FAILED')
        self.validate()
        with self.assertRaises(ContractError): self.validate(self.row | dict(created_at=TIME+timedelta(seconds=21)))

    def test_only_fixed_source_tables_and_version(self):
        for change in (dict(source_table='other.ci_product.product_master'), dict(source_kind='RAW_SALES'),
                       dict(evidence_schema_version='v2'), dict(source_table=f'{PROJECT}.ci_brand_memory.knowledge_chunks')):
            with self.assertRaises(ContractError): self.validate(self.row | change)

    def test_source_ids_and_citation_ids_must_match(self):
        for key in ('evidence_ref_id', 'source_record_id'):
            with self.assertRaises(ContractError): self.validate(self.row | {key: 'other'})
        candidate = deepcopy(self.row); candidate['evidence_payload']['reference'] = 'invented'
        candidate['evidence_ref_id'] = evidence_row_id(candidate['source_table'], candidate['source_record_id'], 'invented')
        with self.assertRaises(ContractError): self.validate(candidate)

    def test_hash_and_source_provenance(self):
        for change in (dict(content_sha256='0'*64), dict(content_sha256='A'*64), dict(source_system='REAL_BUSINESS')):
            with self.assertRaises(ContractError): self.validate(self.row | change)
        candidate = deepcopy(self.row); candidate['evidence_payload']['record']['source_system'] = 'SYNTHETIC_POC'
        with self.assertRaises(ContractError): self.validate(self.rehash(candidate))

    def test_json_bound_and_exact_allowlists(self):
        for mutate in (lambda p: p.update(extra=True), lambda p: p.update(record=[]),
                       lambda p: p['record'].update(vectors=[0.1]), lambda p: p['record'].update(raw_rows=[]),
                       lambda p: p['record'].update(product_name='x'*65536), lambda p: p['record'].update(price=float('nan'))):
            candidate = deepcopy(self.row); mutate(candidate['evidence_payload'])
            with self.assertRaises(ContractError): self.validate(candidate)

    def test_decimal_strings_preserve_exact_numeric(self):
        for value in (1.2, True, '1e3', 'NaN', '-1', '0.0000000001', '100000000000000000000000000000'):
            candidate = deepcopy(self.row); candidate['evidence_payload']['record']['price'] = value
            with self.subTest(value=value), self.assertRaises(ContractError): self.validate(self.rehash(candidate))
        candidate = deepcopy(self.row); candidate['evidence_payload']['record']['price'] = '25.0000000000'
        self.validate(self.rehash(candidate))

    def test_no_unexpected_images_vectors_review_or_ranking(self):
        for key, value in [('gcs_uri', 'gs://other/image'), ('image_sha256', digest('image')), ('gcs_generation', '1'),
                ('embedding_model', 'invented'), ('rank', 1), ('cosine_distance', 0.1), ('ranking_version', 'v1'),
                ('review_revision', 1), ('approval_reference', 'invented')]:
            with self.subTest(key=key), self.assertRaises(ContractError): self.validate(self.row | {key: value})

    def test_chunk_hash_is_text_hash_not_json_hash(self):
        candidate = row('KNOWLEDGE_CHUNK', chunk()); self.validate(candidate)
        with self.assertRaises(ContractError): self.validate(self.rehash(candidate))
        candidate = row('KNOWLEDGE_CHUNK', chunk()); candidate['evidence_payload']['record']['chunk_text'] += ' changed'
        with self.assertRaises(ContractError): self.validate(candidate)

    def test_chunk_snapshot_scope_and_eligibility(self):
        for change in (dict(sku='CI-SKU-0002'), dict(approval_scope='APPROVED'), dict(retrieval_status='REVIEW_ONLY'),
                       dict(chunk_type='REJECTED_CREATIVE'), dict(source_system='OTHER'), dict(snapshot_id='x'),
                       dict(claim_id='invented'), dict(image_uris=['gs://other/primary.jpg'])):
            record = chunk(); record.update(change)
            with self.subTest(change=change), self.assertRaises(ContractError): self.validate(row('KNOWLEDGE_CHUNK', record))

    def test_styling_uses_separate_authoritative_view(self):
        candidate = row('KNOWLEDGE_CHUNK', chunk('WEBSITE_VISUAL_REFERENCE')); self.validate(candidate)
        candidate['source_table'] = f'{PROJECT}.{SOURCE_TABLES["KNOWLEDGE_CHUNK"]}'
        candidate['evidence_ref_id'] = evidence_row_id(candidate['source_table'], candidate['source_record_id'], candidate['evidence_payload']['reference'])
        with self.assertRaises(ContractError): self.validate(candidate)

    def test_claims_are_only_mock_approved_product_names(self):
        for change in (dict(claim_type='BENEFIT'), dict(approval_status='PENDING_REVIEW'), dict(approval_status='REJECTED'),
                       dict(approval_scope='REAL_BUSINESS'), dict(approved_by='personal-email'), dict(evidence_source_system='OTHER'),
                       dict(approved_at=(TIME+timedelta(seconds=2)).isoformat()), dict(evidence_excerpt='other')):
            record = claim(); record.update(change)
            with self.subTest(change=change), self.assertRaises(ContractError): self.validate(row('POC_CLAIM', record))

    def test_claim_approval_reference_and_absent_revision(self):
        candidate = row('POC_CLAIM', claim())
        for change in (dict(approval_reference='invented'), dict(review_revision=1)):
            with self.assertRaises(ContractError): self.validate(candidate | change)
        # Citation strings name recorded provenance; no config file is read.
        alias = row('POC_CLAIM', claim(), ref='config/brand_memory.json#review_fixture'); self.validate(alias)

    def test_sales_coverage_counts_and_financial_equation(self):
        for change in (dict(row_count=1), dict(day_count=True), dict(orders=11), dict(units_sold=2**63),
                       dict(net_sales='240'), dict(discount_amount='-1'), dict(start_date='20260101'),
                       dict(end_date='2025-12-31'), dict(currency='usd'), dict(channel='OTHER')):
            record = sales(); record.update(change)
            with self.subTest(change=change), self.assertRaises(ContractError): self.validate(row('SALES_SUMMARY', record))

    def test_performance_coverage_counts_and_ratios(self):
        for change in (dict(impressions=99), dict(conversions=101), dict(attributed_units=1), dict(performance_days=1),
                       dict(spend='40'), dict(ctr='0.2'), dict(roas=4.0), dict(attribution_model='ACTUAL')):
            record = performance(); record.update(change)
            with self.subTest(change=change), self.assertRaises(ContractError): self.validate(row('CAMPAIGN_PERFORMANCE', record))

    def test_zero_denominator_is_null_not_zero(self):
        record = performance(); record.update(impressions=0, clicks=0, conversions=0, attributed_units=0,
            budget='0', spend='0', attributed_revenue='0', ctr=None, conversion_rate=None, roas=None)
        self.validate(row('CAMPAIGN_PERFORMANCE', record))
        record['ctr'] = '0'
        with self.assertRaises(ContractError): self.validate(row('CAMPAIGN_PERFORMANCE', record))

    def test_pinned_image_paths_and_generations(self):
        for kind in ('PRODUCT_IMAGE', 'CAMPAIGN_IMAGE', 'GENERATED_IMAGE'):
            for change in (dict(gcs_uri='gs://other/image.png'), dict(gcs_generation='01'), dict(gcs_generation='0'),
                           dict(gcs_generation='1'*21), dict(content_sha256='invalid'), dict(width=True), dict(height=0)):
                record = image(kind); record.update(change)
                with self.subTest(kind=kind, change=change), self.assertRaises(ContractError): self.validate(row(kind, record))

    def test_image_pins_match_payload(self):
        candidate = row('PRODUCT_IMAGE', image())
        for change in (dict(image_sha256=digest('other')), dict(gcs_generation='1'), dict(content_sha256=digest('other')),
                       dict(embedding_model='other'), dict(embedding_dimensions=768)):
            with self.assertRaises(ContractError): self.validate(candidate | change)

    def test_generated_image_review_and_scope(self):
        for change in (dict(status='PENDING_REVIEW'), dict(approval_scope='REAL_BUSINESS'), dict(revision=True), dict(review_id='other')):
            record = image('GENERATED_IMAGE'); record['review'].update(change)
            with self.assertRaises(ContractError): self.validate(row('GENERATED_IMAGE', record))
        with self.assertRaises(ContractError): self.validate(row('GENERATED_IMAGE', image('GENERATED_IMAGE')) | dict(review_revision=1))

    def test_rank_vector_completeness_strict_types_and_cosine_bounds(self):
        candidate = row('KNOWLEDGE_CHUNK', chunk('CAMPAIGN_CREATIVE'))
        for change in (dict(rank=0), dict(rank=True), dict(rank=2**63), dict(cosine_distance=None),
                       dict(cosine_distance=float('nan')), dict(cosine_distance=2.001), dict(cosine_distance=-0.001),
                       dict(ranking_version=None), dict(embedding_dimensions=512), dict(embedding_model=None)):
            with self.assertRaises(ContractError): self.validate(candidate | change)
        for distance in (-1e-13, 0, 2, 2+1e-13): self.validate(candidate | dict(cosine_distance=distance))

    def test_complete_bundle_and_aliases_are_supported(self):
        rows = complete_bundle(); self.bundle(rows)
        rows += [row('POC_RULE', rule(), ref=rule()['source_reference'])]; self.bundle(rows)
        self.assertEqual(rows[0]['evidence_payload']['reference'], rows[4]['evidence_payload']['reference'])
        self.assertNotEqual(rows[0]['evidence_ref_id'], rows[4]['evidence_ref_id'])

    def test_empty_oversized_or_incomplete_bundle(self):
        for rows in ([], [self.row]*257, [self.row], complete_bundle()[1:], complete_bundle()[:-1]):
            with self.assertRaises(ContractError): self.bundle(rows)

    def test_duplicate_logical_rows_and_conflicting_source_aliases(self):
        rows = complete_bundle()
        with self.assertRaises(ContractError): self.bundle(rows + [deepcopy(rows[0])])
        conflicting = row('POC_RULE', rule() | dict(rule_text='Different rule.'), ref=rule()['source_reference'])
        with self.assertRaises(ContractError): self.bundle(rows + [conflicting])

    def test_no_mixed_workflow_or_knowledge_snapshots(self):
        for key, value in [('evidence_snapshot_id', 'other-snapshot'), ('knowledge_snapshot_id', digest('other'))]:
            rows = complete_bundle(); rows[4][key] = value
            if key == 'knowledge_snapshot_id': rows[4]['evidence_payload']['record']['snapshot_id'] = value
            with self.assertRaises(ContractError): self.bundle(rows)

    def test_policy_applicability_and_wording(self):
        rows = complete_bundle(); rows[1] = row('POC_RULE', rule() | dict(brand='Other brand'))
        with self.assertRaises(ContractError): self.bundle(rows)
        rows = complete_bundle(); rows[1] = row('POC_RULE', rule() | dict(rule_text='Different policy.'))
        with self.assertRaises(ContractError): self.bundle(rows)

    def test_claim_wording_and_claim_chunk_parent(self):
        rows = complete_bundle() + [row('POC_CLAIM', claim()), row('KNOWLEDGE_CHUNK', chunk('POC_APPROVED_CLAIM'))]
        self.bundle(rows)
        with self.assertRaises(ContractError): self.bundle([r for r in rows if r['source_kind'] != 'POC_CLAIM'])
        rows[-2] = row('POC_CLAIM', claim() | dict(claim_text='Different name', evidence_excerpt='Different name'))
        with self.assertRaises(ContractError): self.bundle(rows)

    def test_style_requires_selection_and_all_four_sections(self):
        styling = [row('KNOWLEDGE_CHUNK', chunk('WEBSITE_VISUAL_REFERENCE', section=s))
                   for s in ('typography', 'colors', 'layout', 'creative_patterns')]
        from src.application.workflow_contract import request_fingerprint
        self.req['visual_reference'] = self.req['submitted_preferences']['preferences']['visual_reference'] = 'NONE'
        self.req['request_sha256'] = request_fingerprint(self.req)
        self.job['request_sha256'] = self.req['request_sha256']
        with self.assertRaises(ContractError): self.bundle(complete_bundle() + styling)
        self.attempt, self.req, self.job = execution()
        if self.req['visual_reference'] == 'NONE':
            self.req['visual_reference'] = self.req['submitted_preferences']['preferences']['visual_reference'] = 'NAKED_AND_THRIVING'
            self.req['request_sha256'] = request_fingerprint(self.req); self.job['request_sha256'] = self.req['request_sha256']
        self.bundle(complete_bundle() + styling)
        with self.assertRaises(ContractError): self.bundle(complete_bundle() + styling[:-1])

    def test_rank_history_requires_contiguous_positions(self):
        candidate = row('CAMPAIGN_IMAGE', image('CAMPAIGN_IMAGE')) | dict(rank=2)
        with self.assertRaises(ContractError): self.bundle(complete_bundle() + [candidate])

    def test_campaign_text_images_and_performance_agree(self):
        record = chunk('CAMPAIGN_CREATIVE'); record['image_uris'].append(image('CAMPAIGN_IMAGE')['gcs_uri'])
        extra = [row('KNOWLEDGE_CHUNK', record), row('CAMPAIGN_IMAGE', image('CAMPAIGN_IMAGE')),
                 row('CAMPAIGN_PERFORMANCE', performance())]
        self.bundle(complete_bundle() + extra)
        for remove in range(3):
            with self.assertRaises(ContractError): self.bundle(complete_bundle() + extra[:remove] + extra[remove+1:])
        for change in (dict(rank=2), dict(cosine_distance=0.4), dict(ranking_version='other')):
            with self.assertRaises(ContractError): self.bundle(complete_bundle() + [extra[0], extra[1] | change, extra[2]])

    def test_generated_reference_pins_original_product_image(self):
        extra = row('GENERATED_IMAGE', image('GENERATED_IMAGE')); self.bundle(complete_bundle() + [extra])
        extra['evidence_payload']['record']['original_product_image']['gcs_generation'] = '2'
        self.validate(extra)  # Structurally valid alone, inconsistent with the complete bundle.
        with self.assertRaises(ContractError): self.bundle(complete_bundle() + [extra])

    def output_context(self):
        output, self.req, self.job = context()
        self.attempt, _, _ = execution('SUCCEEDED')
        self.attempt.update(updated_at=self.job['updated_at'], completed_at=self.job['completed_at'])
        rows = complete_bundle()
        output['brief']['evidence_index'] = [dict(evidence_id=evidence_reference_id(ref), reference=ref)
            for ref in sorted({r['evidence_payload']['reference'] for r in rows})]
        output['brief']['copy']['evidence_ids'] = [output['brief']['evidence_index'][0]['evidence_id']]
        output['brief']['grounded_evidence']['product']['identity'] = product()
        output['brief']['product_image']['metadata'] = image()
        grounded = output['brief']['grounded_evidence']
        grounded['approved_claims']['claims'] = []
        grounded['policies']['rules'] = [rule()]
        grounded['campaign_performance']['summaries'] = []
        grounded['sales'].update(start_date=sales()['start_date'], end_date=sales()['end_date'], currency='USD',
            totals={k: v for k, v in sales().items() if k in ('units_sold', 'orders', 'row_count', 'day_count',
                'gross_sales', 'discount_amount', 'refund_amount', 'net_sales')})
        return output, rows

    def test_output_citations_and_pins_bind_to_complete_snapshot(self):
        output, rows = self.output_context(); self.bundle(rows, output)

    def test_output_cannot_mix_snapshot_or_knowledge_ids(self):
        for mutate in (lambda o: o.update(evidence_snapshot_id='other'), lambda o: o['brief'].update(snapshot_id=digest('other'))):
            output, rows = self.output_context(); mutate(output)
            with self.assertRaises(ContractError): self.bundle(rows, output)

    def test_output_cannot_drop_add_or_relabel_citations(self):
        for mutate in (lambda o: o['brief']['evidence_index'].pop(),
                       lambda o: o['brief']['evidence_index'].append(dict(evidence_id='invented', reference='invented')),
                       lambda o: o['brief']['evidence_index'][0].update(reference='invented')):
            output, rows = self.output_context(); mutate(output)
            with self.assertRaises(ContractError): self.bundle(rows, output)

    def test_output_claim_ids_product_identity_and_image_generation(self):
        for mutate in (lambda o: o['brief']['copy'].update(claim_ids=['invented']),
                       lambda o: o['brief']['grounded_evidence']['product']['identity'].update(product_name='Other'),
                       lambda o: o['brief']['product_image']['metadata'].update(gcs_generation='2')):
            output, rows = self.output_context(); mutate(output)
            with self.assertRaises(ContractError): self.bundle(rows, output)

    def test_evidence_cannot_be_created_after_brief(self):
        output, rows = self.output_context(); rows[0]['created_at'] = TIME+timedelta(seconds=3)
        with self.assertRaises(ContractError): self.bundle(rows, output)

    def test_output_projected_policies_claims_and_analytics_must_match(self):
        for mutate in (lambda g: g['approved_claims']['claims'].append(claim()),
                       lambda g: g['policies']['rules'][0].update(rule_text='Different wording'),
                       lambda g: g['campaign_performance']['summaries'].append(performance()),
                       lambda g: g['sales']['totals'].update(net_sales='1000'),
                       lambda g: g['sales'].update(start_date='2026-01-02')):
            output, rows = self.output_context(); mutate(output['brief']['grounded_evidence'])
            with self.assertRaises(ContractError): self.bundle(rows, output)

    def test_bundle_has_a_total_payload_limit(self):
        with patch('src.application.evidence_contract.MAX_SNAPSHOT_BYTES', 1):
            with self.assertRaises(ContractError): self.bundle()

    def test_multiple_periods_for_one_selected_campaign_are_ambiguous(self):
        record = chunk('CAMPAIGN_CREATIVE'); record['image_uris'].append(image('CAMPAIGN_IMAGE')['gcs_uri'])
        extra = [row('KNOWLEDGE_CHUNK', record), row('CAMPAIGN_IMAGE', image('CAMPAIGN_IMAGE')),
                 row('CAMPAIGN_PERFORMANCE', performance()),
                 row('CAMPAIGN_PERFORMANCE', performance() | dict(start_date='2026-02-01', end_date='2026-02-02'))]
        with self.assertRaises(ContractError): self.bundle(complete_bundle() + extra)

    def test_validation_is_pure_and_does_not_mutate(self):
        rows = complete_bundle(); before = deepcopy(rows)
        with (patch('google.cloud.bigquery.Client', side_effect=AssertionError('no BQ calls')),
              patch('google.cloud.storage.Client', side_effect=AssertionError('no GCS calls'))):
            self.bundle(rows)
        self.assertEqual(rows, before)

    def test_errors_do_not_expose_payload_or_paths(self):
        candidate = deepcopy(self.row); candidate['evidence_payload']['record']['product_name'] = 'sensitive\nname'
        with self.assertRaises(ContractError) as caught: self.validate(self.rehash(candidate))
        self.assertNotIn('sensitive', str(caught.exception)); self.assertNotIn(PROJECT, str(caught.exception))


if __name__ == '__main__':
    unittest.main()
