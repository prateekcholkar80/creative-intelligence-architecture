"""Real signed IAP assertions + full fake BQ/GCS records; no cloud writes."""
from copy import deepcopy
from dataclasses import asdict, FrozenInstanceError
from datetime import timedelta
import hashlib
from io import BytesIO
import unittest
from unittest.mock import Mock

from google.api_core.exceptions import NotFound, PreconditionFailed
from PIL import Image

from src.application.access_policy import Principal
from src.application.artifact_download import ArtifactReadSettings, GcsArtifactReader
from src.application.identity import IdentityError
from src.application.protected_service import ProtectedServiceError, ProtectedWorkflowService
from src.application.workflow_contract import (
    ARTIFACT_BUCKET, ArtifactLimits, artifact_gcs_uri, canonical_json_bytes, request_fingerprint,
)
from src.ingestion.setup_workflow_tables import PROJECT
from tests.test_output_contract import context
from tests import test_protected_service as gateway_fixtures
from tests.test_protected_service import (
    EPOCH, OWNER, WORKSPACE, conversation, member, workspace,
)


def image_bytes(format='PNG', size=(1200, 628)):
    buffer = BytesIO()
    Image.new('RGB', size, '#eedbcc').save(buffer, format=format)
    return buffer.getvalue()


class ProtectedDownloadTests(unittest.TestCase):
    # Reuse only the crypto/adapter setup helpers, not the existing test cases.
    setUpClass = classmethod(gateway_fixtures.ProtectedServiceTests.setUpClass.__func__)
    headers = gateway_fixtures.ProtectedServiceTests.headers
    rows = gateway_fixtures.ProtectedServiceTests.rows

    def setUp(self):
        gateway_fixtures.ProtectedServiceTests.setUp(self)
        self.storage = Mock(project=PROJECT)
        self.blob = self.storage.bucket.return_value.blob.return_value
        self.gcs = GcsArtifactReader(self.storage)
        self.service = ProtectedWorkflowService(self.verifier, self.reader, self.gcs)
        self.output, self.request, self.job = context()
        for row in (self.output, self.request, self.job):
            row.update(workspace_id=WORKSPACE, owner_id=OWNER)
            if 'principal_id' in row:
                row['principal_id'] = OWNER
        self.request['request_sha256'] = request_fingerprint(self.request)
        self.job['request_sha256'] = self.request['request_sha256']
        self.parent = conversation(conversation_id='test-conversation')
        self.png = image_bytes()
        self.output['artifacts'] = [self.manifest('BANNER_PNG', self.png)]
        self.configure_blob(self.png, self.output['artifacts'][0])

    def manifest(self, kind, raw):
        artifact_id = {'BANNER_PNG': 'test-banner', 'BRIEF_JSON': 'test-json', 'BRIEF_TEXT': 'test-text'}[kind]
        return dict(artifact_id=artifact_id, kind=kind,
            gcs_uri=artifact_gcs_uri(workspace_id=WORKSPACE, conversation_id='test-conversation',
                canonical_sku=self.output['canonical_sku'], output_id='test-output',
                artifact_id=artifact_id, kind=kind), gcs_generation='1730000000000000',
            sha256=hashlib.sha256(raw).hexdigest(), size_bytes=len(raw),
            content_type={'BANNER_PNG': 'image/png', 'BRIEF_JSON': 'application/json', 'BRIEF_TEXT': 'text/plain'}[kind])

    def configure_blob(self, raw, manifest):
        self.storage.reset_mock()
        self.blob.reload.side_effect = None
        self.blob.download_as_bytes.side_effect = None
        self.blob.generation = int(manifest['gcs_generation'])
        self.blob.size = len(raw)
        self.blob.content_type = manifest['content_type']
        self.blob.content_encoding = None
        self.blob.download_as_bytes.return_value = raw

    def record_rows(self, *, output=None, request=None, parent=None, membership=None, job=None):
        return ([workspace()], [member() if membership is None else membership],
                [self.job if job is None else job], [self.parent if parent is None else parent],
                [self.request if request is None else request], [self.output if output is None else output],
                [workspace()], [member() if membership is None else membership],
                [self.parent if parent is None else parent])

    def allowed(self, **final_changes):
        self.rows(*self.record_rows(), *self.record_rows(**final_changes))

    def download(self, artifact_id='test-banner', headers=None):
        return self.service.download_artifact(self.headers() if headers is None else headers,
            workspace_id=WORKSPACE, job_id='test-job', artifact_id=artifact_id)

    def denied(self, **kwargs):
        with self.assertRaises(ProtectedServiceError) as caught:
            self.download(**kwargs)
        self.assertEqual(caught.exception.code, 'ACCESS_DENIED')
        self.assertIsNone(caught.exception.__cause__)
        self.assertNotIn('gs://', str(caught.exception))

    def test_download_real_signature_full_records_pinned_generation_and_safe_response(self):
        self.allowed()
        result = self.download()
        self.assertEqual(result.data, self.png)
        self.assertEqual(result.filename, 'test-banner.png')
        self.assertEqual(result.content_type, 'image/png')
        self.assertEqual(result.sha256, hashlib.sha256(self.png).hexdigest())
        self.assertEqual(result.output.source_system, 'SYNTHETIC_POC')
        self.assertEqual(result.output.initial_review_status, 'NOT_REVIEWED')
        self.assertIs(result.output.real_business_approved, False)
        for name in ('owner_id', 'principal_id', 'gcs_uri', 'gcs_generation'):
            self.assertNotIn(name, asdict(result.output))
        with self.assertRaises(FrozenInstanceError):
            result.filename = 'other.png'
        self.assertEqual(self.client.query.call_count, 18)
        self.storage.bucket.assert_called_once_with(ARTIFACT_BUCKET)
        name = self.output['artifacts'][0]['gcs_uri'].split(f'{ARTIFACT_BUCKET}/', 1)[1]
        self.storage.bucket.return_value.blob.assert_called_once_with(name, generation=1730000000000000)
        for call in (self.blob.reload.call_args, self.blob.download_as_bytes.call_args):
            self.assertEqual(call.kwargs['if_generation_match'], 1730000000000000)
            self.assertEqual(call.kwargs['timeout'], 20)
            self.assertEqual(call.kwargs['retry'].deadline, 20)
        download = self.blob.download_as_bytes.call_args.kwargs
        self.assertEqual((download['start'], download['end']), (0, len(self.png)))
        self.assertIs(download['raw_download'], True)
        self.assertEqual(self.fetch.call_count, 1)

    def test_full_queries_use_job_derived_ids_partition_windows_and_parameters(self):
        self.allowed()
        self.download()
        for offset in (0, 9):
            query = self.client.query.call_args_list[offset+4]
            self.assertIn('.ci_application.requests`', query.args[0])
            params = {p.name: p.value for p in query.kwargs['job_config'].query_parameters}
            self.assertEqual(params['request_id'], self.job['request_id'])
            self.assertEqual(params['start_at'], self.job['created_at']-timedelta(days=7))
            self.assertEqual(params['end_at'], self.job['created_at'])
            output_query = self.client.query.call_args_list[offset+5]
            self.assertIn('artifacts', output_query.args[0])
            self.assertIn('created_at>=@start_at AND created_at<=@end_at', output_query.args[0])
            self.assertFalse(output_query.kwargs['job_config'].use_query_cache)
            self.assertEqual(output_query.kwargs['job_config'].maximum_bytes_billed, 64*1024**2)

    def test_json_download_matches_canonical_stored_brief(self):
        raw = canonical_json_bytes(self.output['brief'])
        manifest = self.manifest('BRIEF_JSON', raw)
        self.output['artifacts'].append(manifest)
        self.configure_blob(raw, manifest)
        self.allowed()
        result = self.download('test-json')
        self.assertEqual(result.data, raw)
        self.assertEqual(result.filename, 'test-json.json')

    def test_text_download_accepts_utf8_and_newlines(self):
        raw = 'POC draft — benefit claims pending.\nReview required.\n'.encode()
        manifest = self.manifest('BRIEF_TEXT', raw)
        self.output['artifacts'].append(manifest)
        self.configure_blob(raw, manifest)
        self.allowed()
        self.assertEqual(self.download('test-text').data, raw)

    def test_missing_forged_expired_assertions_never_touch_bq_or_gcs(self):
        for headers in ({}, self.headers(exp=EPOCH-60),
                        {next(iter(self.headers())): 'forged'}):
            self.denied(headers=headers)
        self.client.query.assert_not_called()
        self.storage.bucket.assert_not_called()

    def test_reader_rejects_unverified_principal_before_database_lookup(self):
        with self.assertRaises(IdentityError):
            self.reader.read_artifact(Principal(OWNER), workspace_id=WORKSPACE,
                                      job_id='test-job', artifact_id='test-banner')
        self.client.query.assert_not_called()

    def test_nonmember_revoked_suspended_or_admin_only_never_touch_gcs(self):
        for members in ([], [member(status='REVOKED')], [member(status='SUSPENDED')], [member(roles=['ADMIN'])]):
            self.rows([workspace()], members)
            self.denied()
        self.storage.bucket.assert_not_called()

    def test_private_peer_denied_before_request_or_output_lookup(self):
        self.rows([workspace()], [member()], [self.job | dict(owner_id='peer', principal_id='peer')],
                  [self.parent | dict(owner_id='peer')])
        self.denied()
        self.assertEqual(self.client.query.call_count, 4)
        self.storage.bucket.assert_not_called()

    def test_team_peer_download_allowed_without_owner_mutation_rights(self):
        for row in (self.request, self.job, self.output):
            row['owner_id'] = 'peer'
            if 'principal_id' in row:
                row['principal_id'] = 'peer'
        self.request['request_sha256'] = request_fingerprint(self.request)
        self.job['request_sha256'] = self.request['request_sha256']
        self.parent.update(owner_id='peer', visibility='TEAM')
        self.allowed()
        self.assertEqual(self.download().data, self.png)

    def test_archived_conversation_remains_readable(self):
        self.parent['status'] = 'ARCHIVED'
        self.allowed()
        self.assertEqual(self.download().data, self.png)

    def test_missing_or_duplicate_full_records_never_touch_gcs(self):
        for index in (2, 4, 5):
            for missing in (True, False):
                rows = list(self.record_rows())
                rows[index] = [] if missing else rows[index]*2
                self.rows(*rows)
                self.denied()
        self.storage.bucket.assert_not_called()

    def test_invalid_full_parent_or_output_contract_never_touches_gcs(self):
        for changes in (dict(request_id='other'), dict(attempt_id='other'), dict(source_system='REAL'),
                        dict(real_business_approved=True), dict(renderer_version='changed'),
                        dict(model_profiles=[]), dict(brief={'from_local_export': True})):
            self.rows(*self.record_rows(output=self.output | changes))
            self.denied()
        self.storage.bucket.assert_not_called()

    def test_nonterminal_job_never_touches_gcs(self):
        job = self.job | dict(state='RUNNING', output_id=None, completed_at=None)
        self.rows(*self.record_rows(job=job))
        self.denied()
        self.storage.bucket.assert_not_called()

    def test_missing_requested_artifact_never_touches_gcs(self):
        self.rows(*self.record_rows())
        self.denied(artifact_id='missing')
        self.storage.bucket.assert_not_called()

    def test_browser_cannot_supply_object_url_owner_or_date(self):
        for value in ('gs://other-bucket/file.png', '../test-banner', 'test-banner?owner=peer'):
            self.rows(*self.record_rows())
            self.denied(artifact_id=value)
        with self.assertRaises(TypeError):
            self.service.download_artifact(self.headers(), workspace_id=WORKSPACE, job_id='test-job',
                artifact_id='test-banner', gcs_uri='gs://other/file', owner_id='peer')
        self.storage.bucket.assert_not_called()

    def test_unsafe_authoritative_uri_never_touches_gcs(self):
        for uri in ('gs://other/file.png', f'gs://{ARTIFACT_BUCKET}/products/CI-SKU-0001/primary.jpg'):
            output = deepcopy(self.output)
            output['artifacts'][0]['gcs_uri'] = uri
            self.rows(*self.record_rows(output=output))
            self.denied()
        self.storage.bucket.assert_not_called()

    def test_revocation_during_bigquery_queries_stops_before_gcs(self):
        rows = list(self.record_rows())
        rows[7] = [member(status='REVOKED')]
        self.rows(*rows)
        self.denied()
        self.storage.bucket.assert_not_called()

    def test_membership_revoked_after_bytes_fetched_blocks_release(self):
        self.allowed(membership=member(status='REVOKED'))
        self.denied()
        self.blob.download_as_bytes.assert_called_once()

    def test_workspace_disabled_after_download_blocks_release(self):
        rows = [*self.record_rows(), *self.record_rows()]
        rows[9] = [workspace(status='DISABLED')]
        self.rows(*rows)
        self.denied()

    def test_sharing_removed_after_download_blocks_release(self):
        for row in (self.request, self.job, self.output):
            row['owner_id'] = 'peer'
            if 'principal_id' in row:
                row['principal_id'] = 'peer'
        self.request['request_sha256'] = request_fingerprint(self.request)
        self.job['request_sha256'] = self.request['request_sha256']
        self.parent.update(owner_id='peer', visibility='TEAM')
        self.allowed(parent=self.parent | dict(visibility='PRIVATE', access_revision=2))
        self.denied()
        self.blob.download_as_bytes.assert_called_once()

    def test_changed_manifest_after_download_blocks_release(self):
        changed = deepcopy(self.output)
        changed['artifacts'][0]['gcs_generation'] = '1730000000000001'
        self.allowed(output=changed)
        self.denied()

    def test_changed_stored_brief_after_download_blocks_release(self):
        changed = deepcopy(self.output)
        changed['brief']['concept'] = 'Changed concept'
        self.allowed(output=changed)
        self.denied()

    def test_identity_expiry_during_gcs_read_blocks_release(self):
        def fetch(**kwargs):
            self.clock.return_value = EPOCH+700
            return self.png
        self.blob.download_as_bytes.side_effect = fetch
        self.allowed()
        self.denied()
        self.assertEqual(self.client.query.call_count, 9)

    def test_current_parent_owner_change_before_gcs_blocks_release(self):
        rows = list(self.record_rows())
        rows[8] = [self.parent | dict(owner_id='peer', visibility='TEAM')]
        self.rows(*rows)
        self.denied()
        self.storage.bucket.assert_not_called()

    def test_request_outside_bounded_window_is_denied_even_if_returned_by_store(self):
        request = self.request | dict(created_at=self.job['created_at']-timedelta(days=8))
        self.rows(*self.record_rows(request=request))
        self.denied()
        self.storage.bucket.assert_not_called()

    def test_output_window_over_seven_days_is_denied_before_content_query(self):
        job = self.job | dict(completed_at=self.job['created_at']+timedelta(days=8),
                             updated_at=self.job['created_at']+timedelta(days=8))
        self.rows(*self.record_rows(job=job))
        self.denied()
        self.assertEqual(self.client.query.call_count, 4)
        self.storage.bucket.assert_not_called()

    def test_metadata_generation_size_mime_or_encoding_mismatch_prevents_body_read(self):
        for name, value in (('generation', 9), ('size', len(self.png)+1),
                            ('content_type', 'image/jpeg'), ('content_encoding', 'gzip')):
            self.configure_blob(self.png, self.output['artifacts'][0])
            setattr(self.blob, name, value)
            self.allowed()
            self.denied()
            self.blob.download_as_bytes.assert_not_called()

    def test_corrupt_short_oversized_or_nonbytes_payloads_are_denied(self):
        for raw in (b'x'*len(self.png), self.png[:-1], self.png+b'x', 'not bytes'):
            self.blob.download_as_bytes.return_value = raw
            self.allowed()
            self.denied()

    def test_wrong_image_format_dimensions_or_broken_png_denied_despite_correct_hash(self):
        for raw in (image_bytes('JPEG'), image_bytes(size=(300, 200)), self.png[:40]):
            manifest = self.manifest('BANNER_PNG', raw)
            self.output['artifacts'] = [manifest]
            self.configure_blob(raw, manifest)
            self.allowed()
            self.denied()

    def test_invalid_utf8_or_control_characters_in_text_are_denied(self):
        for raw in (b'\xff', b'draft\x00bad', b'draft\x7fbad', b'\n\t'):
            manifest = self.manifest('BRIEF_TEXT', raw)
            self.output['artifacts'] = [self.manifest('BANNER_PNG', self.png), manifest]
            self.configure_blob(raw, manifest)
            self.allowed()
            self.denied(artifact_id='test-text')

    def test_animated_png_is_denied_despite_valid_hash_and_dimensions(self):
        buffer = BytesIO()
        Image.new('RGB', (1200, 628), 'red').save(buffer, format='PNG', save_all=True,
            append_images=[Image.new('RGB', (1200, 628), 'blue')], duration=100, loop=0)
        raw = buffer.getvalue()
        manifest = self.manifest('BANNER_PNG', raw)
        self.output['artifacts'] = [manifest]
        self.configure_blob(raw, manifest)
        self.allowed()
        self.denied()

    def test_json_with_valid_declared_hash_but_wrong_brief_never_touches_gcs(self):
        raw = b'{"untrusted":true}'
        self.output['artifacts'].append(self.manifest('BRIEF_JSON', raw))
        self.allowed()
        self.denied(artifact_id='test-json')
        self.storage.bucket.assert_not_called()

    def test_generation_unavailable_precondition_failure_or_outage_denied_without_fallback(self):
        for error in (NotFound('private path'), PreconditionFailed('private generation'), RuntimeError('secret')):
            self.blob.reload.side_effect = error
            self.allowed()
            self.denied()
        self.blob.download_as_bytes.assert_not_called()

    def test_download_failure_or_post_download_bq_outage_blocks_release(self):
        self.blob.download_as_bytes.side_effect = RuntimeError('private cloud details')
        self.allowed()
        self.denied()
        self.configure_blob(self.png, self.output['artifacts'][0])
        jobs = self.rows(*self.record_rows(), *self.record_rows())
        jobs[9].result.side_effect = RuntimeError('private BQ details')
        self.denied()

    def test_configured_size_limit_rejects_artifact_before_gcs(self):
        self.gcs = GcsArtifactReader(self.storage,
            ArtifactReadSettings(limits=ArtifactLimits(maximum_artifact_bytes=100, maximum_total_bytes=200)))
        self.service = ProtectedWorkflowService(self.verifier, self.reader, self.gcs)
        self.allowed()
        self.denied()
        self.storage.bucket.assert_not_called()

    def test_configuration_is_server_only_and_bounds_are_strict(self):
        for kwargs in (dict(timeout_seconds=True), dict(timeout_seconds=61), dict(limits='browser limits')):
            with self.assertRaises(ValueError):
                ArtifactReadSettings(**kwargs)
        with self.assertRaises(ValueError):
            GcsArtifactReader(Mock(project='other'))
        with self.assertRaises(ValueError):
            GcsArtifactReader(self.storage, settings={})
        with self.assertRaises(ValueError):
            ProtectedWorkflowService(self.verifier, self.reader, artifact_reader='browser URL')

    def test_download_never_invokes_write_upload_delete_or_signed_url(self):
        self.allowed()
        self.download()
        for name in ('upload_from_string', 'delete', 'generate_signed_url', 'patch'):
            getattr(self.blob, name).assert_not_called()
        for call in self.client.query.call_args_list:
            self.assertTrue(call.args[0].startswith('SELECT '))


if __name__ == '__main__':
    unittest.main()
