"""Pure output/artifact contracts with unpublished fixtures; no GCS or DB calls."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import timedelta
import hashlib
import json
import unittest
from unittest.mock import patch

from src.application.workflow_contract import (
    ARTIFACT_BUCKET, ArtifactLimits, BRIEF_VERSION, ContractError, JobState,
    OUTPUT_GUARDRAIL_VERSION, artifact_gcs_uri, request_fingerprint, transition_job,
    validate_artifact, validate_output,
)
from src.application.workflow_schema import SCHEMA_VERSION, TABLES
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_workflow_contract import CONVERSATION, TIME, digest, job, request


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def artifact(kind='BANNER_PNG', artifact_id='test-banner', sku='CI-SKU-0001'):
    return dict(artifact_id=artifact_id, kind=kind,
        gcs_uri=artifact_gcs_uri(workspace_id=CONVERSATION.workspace_id, conversation_id=CONVERSATION.resource_id,
            canonical_sku=sku, output_id='test-output', artifact_id=artifact_id, kind=kind),
        gcs_generation='1730000000000000', sha256=digest('fixture bytes'),
        content_type={'BANNER_PNG': 'image/png', 'BRIEF_JSON': 'application/json', 'BRIEF_TEXT': 'text/plain'}[kind],
        size_bytes=100)


def context(sku='CI-SKU-0001'):
    req = request()
    req['canonical_sku'] = req['submitted_preferences']['preferences']['canonical_sku'] = sku
    req['request_sha256'] = request_fingerprint(req)
    running = transition_job(job(req), request=req, conversation=CONVERSATION, target=JobState.RUNNING,
        expected_revision=1, attempt_id='test-attempt', now=TIME+timedelta(seconds=1))
    completed = transition_job(running, request=req, conversation=CONVERSATION, target=JobState.SUCCEEDED,
        expected_revision=2, attempt_id='test-attempt', output_id='test-output', now=TIME+timedelta(seconds=5))
    brief = dict(version=BRIEF_VERSION, canonical_sku=sku, source_system='SYNTHETIC_POC',
        evidence_source_of_truth='BIGQUERY', status='POC_REVIEW_DRAFT', approval_scope='SYNTHETIC_POC_ONLY',
        request={k: req[k] for k in ('canonical_sku', 'objective', 'audience', 'channel', 'visual_reference')},
        snapshot_id=digest('knowledge snapshot'), created_at=(TIME+timedelta(seconds=2)).isoformat(),
        approval_checked_at=(TIME+timedelta(seconds=1)).isoformat(), approval_recheck_passed=True,
        concept='Fixture product spotlight',
        copy=dict(headline='Fixture product', body='Explore the product.', cta='Shop now', claim_ids=[],
                  evidence_ids=['test-evidence'], neutral_copy_authority='PROJECT_CREATIVE_PROPOSAL'),
        visual_direction=dict(layout_id='PRODUCT_RIGHT_COPY_LEFT'),
        product_image=dict(metadata=dict(sku=sku, asset_id=f'AST-{sku}-PRIMARY', source_system='BEAUTYFEEDS',
            gcs_uri=f'gs://{ARTIFACT_BUCKET}/products/{sku}/primary.jpg')),
        historical_reference=None, experiment=dict(status='UNTESTED_PROPOSAL'),
        grounded_evidence=dict(product=dict(identity=dict(sku=sku, source_system='BEAUTYFEEDS')),
            policies=dict(scope='PROJECT_POC_POLICY'), approved_claims=dict(scope='SYNTHETIC_POC_ONLY'),
            sales=dict(source_system='SYNTHETIC_POC'), campaign_performance=dict(source_system='SYNTHETIC_POC')),
        limitations=['Fixture draft; no benefit or business approval.'], model_selection=dict(layout='PRODUCT_RIGHT_COPY_LEFT'),
        model=dict(requested_model='gemini-3.5-flash', location='asia-south1', response_sha256=digest('response'), seed=42,
                   generation_settings=dict(thinking_level='LOW', max_output_tokens=4096),
                   response_validation=dict(finish_reason='STOP', prompt_blocked=False, safety_passed=True)),
        evidence_index=[dict(evidence_id='test-evidence', reference=f'{PROJECT}.ci_product.product_master#sku={sku}')])
    row = dict.fromkeys(f.name for f in TABLES['outputs'].schema)
    for key in ('schema_version', 'workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id',
                'trace_id', 'canonical_sku', 'policy_version'):
        row[key] = completed[key]
    row.update(output_id='test-output', attempt_id='test-attempt', source_system='SYNTHETIC_POC',
        approval_scope='SYNTHETIC_POC_ONLY', initial_review_status='NOT_REVIEWED', real_business_approved=False,
        evidence_snapshot_id='test-evidence-snapshot', brief_schema_version=BRIEF_VERSION, brief=brief,
        guardrail_summary=dict(schema_version=OUTPUT_GUARDRAIL_VERSION, grounding='PASSED',
            benefit_claims_authorized=False, product_image_integrity='PASSED', human_review='REQUIRED',
            content_screening='NOT_INTEGRATED'), renderer_version='test-renderer-v1',
        model_profiles=deepcopy(req['model_profiles']), artifacts=[artifact(sku=sku)], created_at=TIME+timedelta(seconds=4))
    return row, req, completed


class OutputContractTests(unittest.TestCase):
    def setUp(self):
        self.row, self.req, self.job = context()

    def validate(self, row=None, **kwargs):
        validate_output(self.row if row is None else row, request=self.req, job=self.job,
                        conversation=CONVERSATION, **kwargs)

    def validate_artifact(self, record=None, **kwargs):
        validate_artifact(artifact() if record is None else record, workspace_id=CONVERSATION.workspace_id,
            conversation_id=CONVERSATION.resource_id, canonical_sku='CI-SKU-0001', output_id='test-output', **kwargs)

    def add_brief_json(self):
        record = artifact('BRIEF_JSON', 'test-json')
        raw = canonical(self.row['brief'])
        record.update(sha256=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw))
        self.row['artifacts'].append(record)

    def test_valid_completed_output_and_all_twenty_canonical_skus(self):
        for number in range(1, 21):
            row, req, completed = context(f'CI-SKU-{number:04d}')
            validate_output(row, request=req, job=completed, conversation=CONVERSATION)

    def test_reserved_artifact_path_and_fixed_project_bucket(self):
        self.assertEqual(ARTIFACT_BUCKET, f'creative-brand-memory-{PROJECT}')
        self.assertEqual(artifact()['gcs_uri'], f'gs://{ARTIFACT_BUCKET}/workflow_artifacts/v1/'
                         'test-workspace/test-conversation/CI-SKU-0001/test-output/test-banner.png')
        self.validate_artifact()

    def test_same_sku_output_cannot_borrow_parent_scope_or_attempt(self):
        for key in ('workspace_id', 'conversation_id', 'owner_id', 'job_id', 'request_id', 'trace_id',
                    'output_id', 'attempt_id', 'policy_version', 'canonical_sku', 'parent_output_id'):
            row = deepcopy(self.row)
            row[key] = 'other'
            with self.subTest(key=key), self.assertRaises(ContractError):
                self.validate(row)

    def test_incomplete_failed_or_recovery_job_cannot_publish_output(self):
        for target in (JobState.RUNNING, JobState.RECOVERY_REQUIRED, JobState.FAILED, JobState.BLOCKED):
            candidate = deepcopy(self.job)
            candidate.update(state=target.value, output_id=None)
            if target in (JobState.RUNNING, JobState.RECOVERY_REQUIRED):
                candidate['completed_at'] = None
            candidate['safe_error_code'] = None if target == JobState.RUNNING else 'CALL_UNKNOWN'
            with self.subTest(target=target), self.assertRaises(ContractError):
                validate_output(self.row, request=self.req, job=candidate, conversation=CONVERSATION)

    def test_existing_request_job_validators_are_enforced(self):
        for target, key, value in ((self.req, 'request_sha256', '0'*64), (self.job, 'attempt_count', 0),
                                   (self.req, 'revision', True), (self.job, 'principal_id', 'different')):
            previous = target[key]
            target[key] = value
            with self.assertRaises(ContractError):
                self.validate()
            target[key] = previous

    def test_required_shape_boolean_and_int64_types(self):
        for change in ({'real_business_approved': 0}, {'real_business_approved': 'false'},
                       {'created_at': TIME.isoformat()}, {'artifacts': None}, {'model_profiles': None},
                       {'schema_version': 'old'}, {'raw_token': 'do-not-persist'}):
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.validate(self.row | change)
        row = deepcopy(self.row)
        del row['parent_output_id']
        with self.assertRaises(ContractError):
            self.validate(row)
        record = artifact() | {'size_bytes': 2**63}
        with self.assertRaises(ContractError):
            self.validate_artifact(record)

    def test_original_poc_provenance_and_unreviewed_status_are_required(self):
        for change in ({'source_system': 'BEAUTYFEEDS'}, {'approval_scope': 'REAL_BUSINESS'},
                       {'initial_review_status': 'APPROVED'}, {'real_business_approved': True}):
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.validate(self.row | change)

    def test_renderer_and_stage_settings_must_match_pinned_request(self):
        with self.assertRaises(ContractError):
            self.validate(self.row | {'renderer_version': 'changed-renderer'})
        for key, value in (('model_id', 'unpublished-model'), ('thinking_level', 'HIGH'),
                           ('max_output_tokens', 5000), ('prompt_version', 'changed'), ('settings_sha256', digest('changed'))):
            row = deepcopy(self.row)
            row['model_profiles'][1][key] = value
            with self.subTest(key=key), self.assertRaises(ContractError):
                self.validate(row)
        self.row['model_profiles'].reverse()
        self.validate()  # Order does not alter the pinned stage settings.

    def test_duplicate_missing_or_malformed_profile_snapshots_are_rejected(self):
        for profiles in ([], self.row['model_profiles'][:1], [self.row['model_profiles'][0]]*2):
            with self.assertRaises(ContractError):
                self.validate(self.row | {'model_profiles': profiles})

    def test_output_creation_is_between_job_creation_and_completion(self):
        for stamp in (TIME-timedelta(seconds=1), TIME+timedelta(seconds=6), TIME.replace(tzinfo=None)):
            with self.assertRaises(ContractError):
                self.validate(self.row | {'created_at': stamp})

    def test_versioned_guardrails_require_safe_poc_values_and_exact_booleans(self):
        for change in ({'schema_version': 'unknown'}, {'grounding': 'NOT_PASSED'}, {'human_review': 'APPROVED'},
                       {'benefit_claims_authorized': True}, {'benefit_claims_authorized': 0},
                       {'product_image_integrity': 'NOT_PASSED'}, {'content_screening': 'FAILED'},
                       {'raw_model_response': 'not permitted'}):
            row = deepcopy(self.row)
            row['guardrail_summary'].update(change)
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.validate(row)
        self.row['guardrail_summary']['content_screening'] = 'PASSED'
        self.validate()

    def test_brief_metadata_must_match_request_and_poc_scope(self):
        for change in ({'version': 'unknown'}, {'canonical_sku': 'CI-SKU-0002'}, {'source_system': 'ACTUAL'},
                       {'evidence_source_of_truth': 'LOCAL_CACHE'}, {'status': 'PUBLISHED'},
                       {'approval_scope': 'REAL_BUSINESS'}, {'approval_recheck_passed': 1},
                       {'request': self.row['brief']['request'] | {'audience': 'different'}},
                       {'raw_response': 'not allowed'}):
            row = deepcopy(self.row)
            row['brief'].update(change)
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.validate(row)
        with self.assertRaises(ContractError):
            self.validate(self.row | {'brief_schema_version': 'unknown'})

    def test_brief_timestamps_must_be_aware_ordered_and_inside_job(self):
        for key, stamp in (('created_at', TIME.isoformat()), ('created_at', 'not-a-date'),
                           ('created_at', TIME.replace(tzinfo=None).isoformat()),
                           ('created_at', (TIME+timedelta(seconds=5)).isoformat()),
                           ('approval_checked_at', (TIME+timedelta(seconds=3)).isoformat()),
                           ('approval_checked_at', (TIME-timedelta(seconds=1)).isoformat())):
            row = deepcopy(self.row)
            row['brief'][key] = stamp
            with self.subTest(key=key, stamp=stamp), self.assertRaises(ContractError):
                self.validate(row)

    def test_foreign_product_metadata_and_real_performance_relabeling_are_rejected(self):
        for path, key, value in ((('product_image', 'metadata'), 'sku', 'CI-SKU-0002'),
                                 (('product_image', 'metadata'), 'gcs_uri', artifact()['gcs_uri']),
                                 (('grounded_evidence', 'product', 'identity'), 'source_system', 'SYNTHETIC_POC'),
                                 (('grounded_evidence', 'sales'), 'source_system', 'ACTUAL'),
                                 (('grounded_evidence', 'approved_claims'), 'scope', 'REAL_BUSINESS')):
            row = deepcopy(self.row)
            section = row['brief']
            for part in path:
                section = section[part]
            section[key] = value
            with self.subTest(path=path), self.assertRaises(ContractError):
                self.validate(row)

    def test_brief_model_metadata_cannot_change_planner_or_accept_blocked_response(self):
        for change in ({'requested_model': 'different'}, {'location': 'us-central1'},
                       {'response_sha256': 'not-a-hash'}, {'raw_response': 'private data'},
                       {'response_validation': dict(finish_reason='STOP', prompt_blocked=0, safety_passed=1)},
                       {'response_validation': dict(finish_reason='MAX_TOKENS', prompt_blocked=False, safety_passed=True)}):
            row = deepcopy(self.row)
            row['brief']['model'].update(change)
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.validate(row)

    def test_claimed_thinking_seed_and_token_limits_match_pinned_settings(self):
        for change in ({'seed': 43}, {'seed': True},
                       {'generation_settings': dict(thinking_level='HIGH', max_output_tokens=4096)},
                       {'generation_settings': dict(thinking_level='LOW', max_output_tokens=5000)},
                       {'generation_settings': dict(thinking_level='LOW', max_output_tokens=4096.0)},
                       {'generation_settings': {}}, {'temperature': 1}):
            row = deepcopy(self.row)
            row['brief']['model'].update(change)
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.validate(row)

    def test_float64_temperature_and_json_integer_are_equivalent_without_accepting_bool(self):
        self.req['model_profiles'][1]['temperature'] = 0.0
        self.req['request_sha256'] = request_fingerprint(self.req)
        self.job['request_sha256'] = self.req['request_sha256']
        self.row['model_profiles'] = deepcopy(self.req['model_profiles'])
        self.row['brief']['model']['generation_settings']['temperature'] = 0
        self.row['brief']['model']['temperature'] = 0
        self.validate()
        self.row['brief']['model']['generation_settings']['temperature'] = False
        with self.assertRaises(ContractError):
            self.validate()

    def test_invalid_duplicate_and_unresolved_copy_evidence_refs_are_rejected(self):
        for mutate in (lambda b: b.update(evidence_index=[]),
                       lambda b: b['evidence_index'].append(deepcopy(b['evidence_index'][0])),
                       lambda b: b['copy'].update(evidence_ids=['unknown']),
                       lambda b: b['copy'].update(claim_ids=['same', 'same']),
                       lambda b: b['evidence_index'][0].update(reference='unsafe\nreference')):
            row = deepcopy(self.row)
            mutate(row['brief'])
            with self.assertRaises(ContractError):
                self.validate(row)

    def test_non_json_nan_oversized_and_too_deep_payloads_fail_safely(self):
        deep = None
        for _ in range(14):
            deep = {'nested': deep}
        for payload in (float('nan'), b'raw bytes', TIME, 'x'*65537, deep, '\ud800'):
            row = deepcopy(self.row)
            row['brief']['concept'] = payload
            with self.subTest(payload_type=type(payload)), self.assertRaises(ContractError):
                self.validate(row)

    def test_scoped_path_rejects_other_bucket_original_image_and_cross_output(self):
        uri = artifact()['gcs_uri']
        for candidate in (uri.replace(ARTIFACT_BUCKET, 'different-bucket'),
                          f'gs://{ARTIFACT_BUCKET}/products/CI-SKU-0001/primary.jpg',
                          uri.replace('test-output/', 'different-output/'),
                          uri.replace('test-workspace/', 'different-workspace/'),
                          uri.replace('CI-SKU-0001', 'CI-SKU-0002'), uri+'?generation=123', uri+'#fragment'):
            with self.assertRaises(ContractError):
                self.validate_artifact(artifact() | {'gcs_uri': candidate})

    def test_unsafe_scope_segments_and_unsupported_skus_never_form_gcs_uri(self):
        args = dict(workspace_id='test-workspace', conversation_id='test-conversation', canonical_sku='CI-SKU-0001',
                    output_id='test-output', artifact_id='test-artifact', kind='BANNER_PNG')
        for name in ('workspace_id', 'conversation_id', 'output_id', 'artifact_id'):
            for value in ('../other', 'a/b', 'a\\b', 'a%2fb', 'a.b', 'a\x00', 'a b', 'ü', 'x'*129, None):
                with self.subTest(name=name, value=value), self.assertRaises(ContractError):
                    artifact_gcs_uri(**(args | {name: value}))
        with self.assertRaises(ContractError):
            artifact_gcs_uri(**(args | {'canonical_sku': 'SKU-1'}))

    def test_artifact_kind_mime_generation_digest_and_size_are_strict(self):
        for change in ({'kind': 'EXECUTABLE'}, {'content_type': 'image/jpeg'}, {'gcs_generation': '0'},
                       {'gcs_generation': '0123'}, {'gcs_generation': '+123'}, {'gcs_generation': 123},
                       {'gcs_generation': '1'*21}, {'sha256': 'A'*64}, {'sha256': 'abc'}, {'size_bytes': True},
                       {'size_bytes': 0}, {'size_bytes': 32*1024*1024+1}, {'unexpected': 'data'}):
            with self.subTest(change=change), self.assertRaises(ContractError):
                self.validate_artifact(artifact() | change)

    def test_banner_required_and_duplicate_artifact_ids_or_kinds_fail(self):
        for artifacts in ([], [artifact('BRIEF_TEXT', 'text')], [artifact(), artifact()],
                          [artifact(), artifact(artifact_id='different-banner')],
                          [artifact(), artifact('BRIEF_TEXT', 'test-banner')]):
            with self.assertRaises(ContractError):
                self.validate(self.row | {'artifacts': artifacts})

    def test_configured_per_artifact_and_total_byte_limits_are_enforced(self):
        limits = ArtifactLimits(maximum_artifact_bytes=100, maximum_total_bytes=150)
        self.validate(limits=limits)
        self.row['artifacts'].append(artifact('BRIEF_TEXT', 'text'))
        with self.assertRaises(ContractError):
            self.validate(limits=limits)
        with self.assertRaises(ContractError):
            self.validate_artifact(artifact() | {'size_bytes': 101}, limits=limits)
        for changes in ({'maximum_artifact_bytes': True}, {'maximum_artifact_bytes': 0},
                        {'maximum_total_bytes': -1}, {'maximum_total_bytes': 384*1024*1024+1}):
            with self.assertRaises(ContractError):
                ArtifactLimits(**changes)
        with self.assertRaises(ContractError):
            self.validate(limits={'maximum_artifact_bytes': 100})
        with self.assertRaises(FrozenInstanceError):
            limits.maximum_total_bytes = 1

    def test_optional_json_and_text_artifacts_can_be_validated_together(self):
        self.add_brief_json()
        self.row['artifacts'].append(artifact('BRIEF_TEXT', 'test-text'))
        self.validate()
        self.row['artifacts'].reverse()
        self.validate()

    def test_changed_brief_or_hash_cannot_reuse_old_json_artifact_metadata(self):
        self.add_brief_json()
        self.validate()
        self.row['brief']['copy']['headline'] = 'Different fixture copy'
        with self.assertRaises(ContractError):
            self.validate()
        self.row, self.req, self.job = context()
        self.add_brief_json()
        self.row['artifacts'][1]['sha256'] = digest('different JSON bytes')
        with self.assertRaises(ContractError):
            self.validate()

    def test_safe_errors_do_not_expose_content_or_paths(self):
        secret = 'secret user supplied content'
        self.row['brief']['raw_response'] = secret
        with self.assertRaises(ContractError) as caught:
            self.validate()
        self.assertNotIn(secret, str(caught.exception))
        self.assertNotIn('gs://', str(caught.exception))

    def test_validation_preserves_inputs_and_never_calls_cloud_clients(self):
        self.add_brief_json()
        before = deepcopy((self.row, self.req, self.job))
        with patch('google.cloud.bigquery.Client') as bq, patch('google.cloud.storage.Client') as gcs:
            self.validate()
            self.validate_artifact()
            bq.assert_not_called()
            gcs.assert_not_called()
        self.assertEqual(before, (self.row, self.req, self.job))


if __name__ == '__main__':
    unittest.main()
