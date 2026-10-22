"""Gateway integration: real JWT verification plus fake authoritative BQ results."""
from copy import deepcopy
from dataclasses import asdict, FrozenInstanceError
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from google.api_core.exceptions import Forbidden
from google.auth import jwt
from google.auth.crypt import ES256Signer

from src.application.access_reader import BigQueryAccessReader
from src.application.identity import ASSERTION_HEADER, ISSUER, IapIdentityAdapter, IapSettings, principal_for_subject
from src.application.protected_service import ProtectedServiceError, ProtectedWorkflowService
from src.application.workflow_schema import SCHEMA_VERSION
from src.ingestion.setup_workflow_tables import PROJECT

NOW = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
EPOCH = int(NOW.timestamp())
AUDIENCE = '/projects/123456789/locations/asia-south1/services/test-campaign'
WORKSPACE = 'fixture-workspace'
SUBJECT = 'accounts.google.com:test-subject'
OWNER = principal_for_subject(ISSUER, SUBJECT).principal_id


def base():
    return dict(schema_version=SCHEMA_VERSION, workspace_id=WORKSPACE, created_at=NOW-timedelta(minutes=2), updated_at=NOW)


def workspace(**changes):
    return dict(base(), status='ACTIVE', revision=1) | changes


def member(**changes):
    return dict(base(), principal_id=OWNER, identity_issuer=ISSUER, identity_subject=SUBJECT,
        roles=['VIEWER'], status='ACTIVE', revision=1, assignment_origin='ADMIN', updated_by='fixture-admin') | changes


def conversation(**changes):
    return dict(base(), conversation_id='fixture-conversation', owner_id=OWNER, visibility='PRIVATE',
                status='ACTIVE', access_revision=1) | changes


def job(**changes):
    return dict(base(), job_id='fixture-job', request_id='fixture-request', principal_id=OWNER,
        conversation_id='fixture-conversation', owner_id=OWNER, canonical_sku='CI-SKU-0001',
        state='SUCCEEDED', revision=3, attempt_count=1, active_attempt_id='fixture-attempt',
        output_id='fixture-output', safe_error_code=None, completed_at=NOW,
        deadline_at=NOW+timedelta(minutes=3)) | changes


def output(**changes):
    return dict(schema_version=SCHEMA_VERSION, workspace_id=WORKSPACE,
        conversation_id='fixture-conversation', owner_id=OWNER, output_id='fixture-output',
        job_id='fixture-job', request_id='fixture-request', attempt_id='fixture-attempt',
        canonical_sku='CI-SKU-0001', source_system='SYNTHETIC_POC', approval_scope='SYNTHETIC_POC_ONLY',
        initial_review_status='NOT_REVIEWED', real_business_approved=False,
        created_at=NOW-timedelta(minutes=1), artifacts=[dict(artifact_id='fixture-banner',
            kind='BANNER_PNG', gcs_uri=(
                'gs://creative-brand-memory-project-661d501a-e709-4276-a26/'
                'workflow_artifacts/v1/fixture-workspace/fixture-conversation/'
                'CI-SKU-0001/fixture-output/fixture-banner.png'),
            gcs_generation='123', sha256='a'*64, content_type='image/png',
            size_bytes=1024)]) | changes


class ProtectedServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        key = ec.generate_private_key(ec.SECP256R1())
        cls.signer = ES256Signer.from_string(key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()), key_id='fixture-key')
        cls.public = key.public_key().public_bytes(serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo).decode()

    def setUp(self):
        self.client = Mock(project=PROJECT)
        self.reader = BigQueryAccessReader(self.client)
        self.verifier = IapIdentityAdapter(IapSettings(AUDIENCE))
        self.service = ProtectedWorkflowService(self.verifier, self.reader)
        self.claims = dict(iss=ISSUER, sub=SUBJECT, aud=AUDIENCE, iat=EPOCH-10, exp=EPOCH+590,
                           email='personal@example.invalid', roles=['ADMIN'])
        self.clock = Mock(return_value=EPOCH)
        response = SimpleNamespace(status=200, data=json.dumps({'fixture-key': self.public}).encode())
        self.fetch = Mock(return_value=response)
        for patcher in (patch('src.application.identity._BoundedKeyRequest', return_value=self.fetch),
                        patch('src.application.identity.time.time', self.clock),
                        patch('google.auth._helpers.utcnow', return_value=NOW.replace(tzinfo=None))):
            patcher.start()
            self.addCleanup(patcher.stop)

    def headers(self, **changes):
        return {ASSERTION_HEADER: jwt.encode(self.signer, self.claims | changes).decode()}

    def rows(self, *results):
        self.client.query.reset_mock()
        jobs = []
        for result in results:
            query = Mock()
            query.result.return_value = deepcopy(result)
            jobs.append(query)
        self.client.query.side_effect = jobs
        return jobs

    def read(self, summary=False, headers=None):
        method = self.service.get_output_summary if summary else self.service.get_job_status
        return method(self.headers() if headers is None else headers, workspace_id=WORKSPACE, job_id='fixture-job')

    def allowed_job_rows(self, *, membership=None, parent=None, record=None):
        return ([workspace()], [member() if membership is None else membership],
                [job() if record is None else record], [conversation() if parent is None else parent])

    def allowed_output_rows(self):
        return (*self.allowed_job_rows(), [output()], [workspace()], [member()], [conversation()])

    def test_status_authenticates_real_signature_and_returns_safe_immutable_projection(self):
        self.rows(*self.allowed_job_rows())
        result = self.read()
        self.assertEqual(result.state, 'SUCCEEDED')
        self.assertEqual(result.conversation_id, 'fixture-conversation')
        self.assertEqual(result.output_id, 'fixture-output')
        self.assertEqual(self.fetch.call_count, 1)
        self.assertNotIn('owner_id', asdict(result))
        self.assertNotIn('principal_id', asdict(result))
        with self.assertRaises(FrozenInstanceError):
            result.state = 'RUNNING'
        sql = self.client.query.call_args_list[2].args[0]
        self.assertIn('workspace_id=@workspace_id AND job_id=@job_id', sql)
        self.assertNotIn('*', sql)

    def test_output_summary_uses_job_partition_bounds_and_omits_private_payloads(self):
        self.rows(*self.allowed_output_rows())
        summary = self.read(summary=True)
        self.assertEqual(summary.source_system, 'SYNTHETIC_POC')
        self.assertIs(summary.real_business_approved, False)
        self.assertEqual(summary.initial_review_status, 'NOT_REVIEWED')
        self.assertEqual(summary.downloads[0].artifact_id, 'fixture-banner')
        self.assertEqual(summary.downloads[0].filename, 'fixture-banner.png')
        for name in ('owner_id', 'brief', 'artifacts', 'gcs_uri', 'evidence_payload', 'model_profiles'):
            self.assertNotIn(name, asdict(summary))
        query = self.client.query.call_args_list[4]
        self.assertIn('created_at>=@start_at AND created_at<=@end_at', query.args[0])
        params = {p.name: p for p in query.kwargs['job_config'].query_parameters}
        self.assertEqual(params['output_id'].value, 'fixture-output')
        self.assertEqual(params['start_at'].value, job()['created_at'])
        self.assertEqual(params['end_at'].value, NOW)
        self.assertEqual(params['start_at'].type_, 'TIMESTAMP')
        self.assertFalse(query.kwargs['job_config'].use_query_cache)
        self.assertEqual(self.client.query.call_count, 8)

    def test_missing_forged_expired_wrong_audience_assertions_never_query_database(self):
        for headers in ({}, {'X-Goog-Authenticated-User-Email': 'admin@example.invalid'},
                        {ASSERTION_HEADER: self.headers()[ASSERTION_HEADER][:-8]+'tampered'},
                        self.headers(exp=EPOCH-60), self.headers(aud=AUDIENCE+'-other')):
            with self.subTest(headers_type=type(headers)), self.assertRaises(ProtectedServiceError):
                self.read(headers=headers)
        self.client.query.assert_not_called()

    def test_every_call_authenticates_including_reconnect_without_assertion(self):
        self.rows(*self.allowed_job_rows())
        self.read()
        with self.assertRaises(ProtectedServiceError):
            self.read(headers={})
        self.assertEqual(self.client.query.call_count, 4)
        self.rows(*self.allowed_job_rows())
        self.read()
        self.assertEqual(self.fetch.call_count, 2)

    def test_nonmember_inactive_and_role_denial_stop_before_job_lookup(self):
        for members in ([], [member(status='REVOKED')], [member(status='SUSPENDED')],
                        [member(roles=['ADMIN'])], [member(roles=[])]):
            self.rows([workspace()], members)
            with self.subTest(members=members), self.assertRaises(ProtectedServiceError):
                self.read(summary=True)
            self.assertEqual(self.client.query.call_count, 2)

    def test_private_peer_output_denial_stops_before_output_read(self):
        self.rows(*self.allowed_job_rows(record=job(owner_id='peer', principal_id='peer'),
            parent=conversation(owner_id='peer')))
        with self.assertRaises(ProtectedServiceError):
            self.read(summary=True)
        self.assertEqual(self.client.query.call_count, 4)

    def test_team_shared_peer_job_is_readable_without_owner_edit_rights(self):
        self.rows(*self.allowed_job_rows(record=job(owner_id='peer', principal_id='peer'),
            parent=conversation(owner_id='peer', visibility='TEAM')))
        self.assertEqual(self.read().job_id, 'fixture-job')

    def test_copied_owner_cannot_override_current_conversation(self):
        self.rows(*self.allowed_job_rows(parent=conversation(owner_id='peer', visibility='TEAM')))
        with self.assertRaises(ProtectedServiceError):
            self.read()

    def test_revocation_is_read_fresh_next_call(self):
        self.rows(*self.allowed_job_rows(), [workspace()], [member(status='REVOKED', revision=2)])
        self.read()
        with self.assertRaises(ProtectedServiceError):
            self.read()
        self.assertEqual(self.client.query.call_count, 6)

    def test_membership_revoked_during_output_lookup_denies_release(self):
        self.rows(*self.allowed_job_rows(), [output()], [workspace()], [member(status='REVOKED')])
        with self.assertRaises(ProtectedServiceError):
            self.read(summary=True)
        self.assertEqual(self.client.query.call_count, 7)

    def test_team_sharing_revoked_during_output_lookup_denies_release(self):
        self.rows(*self.allowed_job_rows(record=job(owner_id='peer', principal_id='peer'),
            parent=conversation(owner_id='peer', visibility='TEAM')), [output(owner_id='peer')],
            [workspace()], [member()], [conversation(owner_id='peer', access_revision=2)])
        with self.assertRaises(ProtectedServiceError):
            self.read(summary=True)
        self.assertEqual(self.client.query.call_count, 8)

    def test_identity_expiry_during_output_lookup_denies_release(self):
        queries = self.rows(*self.allowed_job_rows(), [output()])
        def expired(**kwargs):
            self.clock.return_value = EPOCH+620
            return [output()]
        queries[-1].result.side_effect = expired
        with self.assertRaises(ProtectedServiceError):
            self.read(summary=True)
        self.assertEqual(self.client.query.call_count, 5)

    def test_missing_and_duplicate_job_records_fail_closed(self):
        for rows in ([], [job(), job()]):
            self.rows([workspace()], [member()], rows)
            with self.assertRaises(ProtectedServiceError):
                self.read()

    def test_missing_and_duplicate_output_records_fail_closed(self):
        for rows in ([], [output(), output()]):
            self.rows(*self.allowed_job_rows(), rows)
            with self.assertRaises(ProtectedServiceError):
                self.read(summary=True)
            self.assertEqual(self.client.query.call_count, 5)

    def test_malformed_job_scope_and_lifecycle_fail_closed(self):
        for change in ({'workspace_id': 'other'}, {'job_id': 'other'}, {'schema_version': 'old'},
                       {'principal_id': 'other'}, {'owner_id': ''}, {'conversation_id': ''},
                       {'canonical_sku': 'CI-SKU-0021'}, {'canonical_sku': []}, {'state': []},
                       {'state': 'UNKNOWN'}, {'revision': True}, {'attempt_count': -1},
                       {'completed_at': None}, {'output_id': None}, {'active_attempt_id': None},
                       {'updated_at': NOW.replace(tzinfo=None)}, {'deadline_at': job()['created_at']},
                       {'safe_error_code': 'raw\nerror'}):
            self.rows([workspace()], [member()], [job(**change)])
            with self.subTest(change=change), self.assertRaises(ProtectedServiceError):
                self.read()
        malformed = job()
        del malformed['active_attempt_id']
        self.rows([workspace()], [member()], [malformed])
        with self.assertRaises(ProtectedServiceError):
            self.read()

    def test_queued_running_recovery_and_failed_statuses_never_claim_output(self):
        for change in (dict(state='QUEUED', active_attempt_id=None, attempt_count=0),
                       dict(state='RUNNING'), dict(state='RECOVERY_REQUIRED', safe_error_code='CALL_UNKNOWN'),
                       dict(state='FAILED', safe_error_code='REQUEST_FAILED', completed_at=NOW),
                       dict(state='BLOCKED', safe_error_code='GROUNDING_BLOCKED', completed_at=NOW)):
            state = change['state']
            record = job(output_id=None, completed_at=None) | change
            self.rows(*self.allowed_job_rows(record=record))
            self.assertEqual(self.read().state, state)
            self.rows(*self.allowed_job_rows(record=record))
            with self.assertRaises(ProtectedServiceError):
                self.read(summary=True)
            self.assertEqual(self.client.query.call_count, 4)

    def test_output_links_must_match_job_and_original_poc_provenance(self):
        for change in ({'workspace_id': 'other'}, {'conversation_id': 'other'}, {'owner_id': 'peer'},
                       {'output_id': 'other'}, {'job_id': 'other'}, {'request_id': 'other'},
                       {'attempt_id': 'other'}, {'canonical_sku': 'CI-SKU-0002'}, {'schema_version': 'old'},
                       {'source_system': 'ACTUAL'}, {'approval_scope': 'REAL_BUSINESS'},
                       {'initial_review_status': 'APPROVED'}, {'real_business_approved': True},
                       {'real_business_approved': 0}, {'created_at': NOW+timedelta(seconds=1)},
                       {'created_at': NOW-timedelta(minutes=3)}):
            self.rows(*self.allowed_job_rows(), [output(**change)])
            with self.subTest(change=change), self.assertRaises(ProtectedServiceError):
                self.read(summary=True)

    def test_output_partition_span_is_bounded_by_authoritative_job(self):
        self.rows(*self.allowed_job_rows(record=job(created_at=NOW-timedelta(days=7, seconds=1))))
        with self.assertRaises(ProtectedServiceError):
            self.read(summary=True)
        self.assertEqual(self.client.query.call_count, 4)
        self.rows(*self.allowed_job_rows(record=job(created_at=NOW-timedelta(days=7))),
                  [output()], [workspace()], [member()], [conversation()])
        self.assertEqual(self.read(summary=True).output_id, 'fixture-output')

    def test_archive_permits_summaries_but_disabled_conversation_fails(self):
        self.rows(*self.allowed_job_rows(parent=conversation(status='ARCHIVED')))
        self.assertEqual(self.read().state, 'SUCCEEDED')
        self.rows(*self.allowed_job_rows(parent=conversation(status='DELETED')))
        with self.assertRaises(ProtectedServiceError):
            self.read()

    def test_unavailable_database_and_identity_key_fetch_are_safe_denials(self):
        for error in (Forbidden('secret cloud details'), RuntimeError('secret cloud details')):
            self.client.query.side_effect = error
            with self.assertRaises(ProtectedServiceError) as caught:
                self.read()
            self.assertNotIn('secret', str(caught.exception))
            self.assertEqual(caught.exception.code, 'ACCESS_DENIED')
            self.assertIsNone(caught.exception.__cause__)
        self.fetch.side_effect = RuntimeError('private assertion details')
        self.client.query.reset_mock()
        with self.assertRaises(ProtectedServiceError):
            self.read()
        self.client.query.assert_not_called()

    def test_absent_private_forbidden_and_invalid_records_have_same_public_error(self):
        messages = []
        for rows in ([], [job(owner_id='peer', principal_id='peer')], [job(schema_version='old')]):
            self.rows([workspace()], [member()], rows, [conversation(owner_id='peer')])
            with self.assertRaises(ProtectedServiceError) as caught:
                self.read()
            messages.append((caught.exception.code, str(caught.exception)))
        self.assertEqual(len(set(messages)), 1)

    def test_gateway_has_no_generation_or_write_entry_points(self):
        for name in ('generate', 'interpret', 'review_for_poc', 'set_visibility', 'bootstrap'):
            self.assertFalse(hasattr(self.service, name))
        with self.assertRaises(ValueError):
            ProtectedWorkflowService('browser identity', self.reader)
        with self.assertRaises(ValueError):
            ProtectedWorkflowService(self.verifier, 'local json records')
        with self.assertRaises(ProtectedServiceError):
            self.service.download_artifact(self.headers(), workspace_id=WORKSPACE,
                                          job_id='fixture-job', artifact_id='fixture-banner')
        self.client.query.assert_not_called()


if __name__ == '__main__':
    unittest.main()
