"""Protected operator composition; synthetic authority, no cloud operations."""
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from src.application import campaign_operator_runtime as runtime
from src.application.campaign_operator_policy_publisher import OperatorPolicyPublisher
from src.application.control_io import ControlOutcome
from src.application.sharing_coordinator_contract import SharingCommitStatus as Status
from tests.test_campaign_review_publication_writer import policy


class OperatorRuntimeTests(TestCase):
    def setUp(self):
        self.publisher = Mock(spec=OperatorPolicyPublisher)
        # Exact type is intentional: constructor cannot install a browser adapter.
        self.publisher.__class__ = OperatorPolicyPublisher
        self.plan = SimpleNamespace(command_sha256='a'*64,
            proposed_record_sha256='b'*64, native_job_id='original-job')
        self.intent = SimpleNamespace(publication=self.plan)
        self.publisher.prepare.return_value = self.intent
        self.publisher.publish.return_value = ControlOutcome('original-job', Status.COMMITTED)
        self.publisher.read_current.return_value = SimpleNamespace(current=SimpleNamespace(
            sha256='b'*64, policy=policy()))
        self.worker = patch.object(runtime, 'require_owned_worker'); self.worker.start()
        self.addCleanup(self.worker.stop)
        # Construct a real owned adapter shell, with mocked I/O-facing methods.
        publisher = object.__new__(OperatorPolicyPublisher)
        for method in ('prepare', 'publish', 'recover', 'read_current'):
            setattr(publisher, method, getattr(self.publisher, method))
        self.service = runtime.ProtectedOperatorRuntime(publisher=publisher,
            policy=policy(), reason_code='EXPLICIT_REVIEW')

    def test_exact_review_and_current_authority_required_for_receipt(self):
        review = self.service.prepare({'assertion': 'opaque'})
        result = self.service.execute({}, confirmed_sha256=review.command_sha256)
        self.assertFalse(result['approval_verified'])
        self.assertEqual(result['current_record_sha256'], 'b'*64)
        self.publisher.read_current.assert_called_once_with({})

    def test_no_review_or_wrong_hash_never_submits(self):
        with self.assertRaisesRegex(ValueError, 'REVIEW_REQUIRED'):
            self.service.execute({}, confirmed_sha256='a'*64)
        self.service.prepare({})
        with self.assertRaisesRegex(ValueError, 'NOT_CONFIRMED'):
            self.service.execute({}, confirmed_sha256='c'*64)
        self.publisher.publish.assert_not_called()

    def test_revocation_or_changed_current_head_withholds_receipt(self):
        self.service.prepare({})
        self.publisher.read_current.side_effect = ValueError('REVOKED')
        with self.assertRaisesRegex(ValueError, 'REVOKED'):
            self.service.execute({}, confirmed_sha256='a'*64)
        self.publisher.read_current.side_effect = None
        self.publisher.read_current.return_value.current.sha256 = 'c'*64
        with self.assertRaisesRegex(ValueError, 'AUTHORITY_CHANGED'):
            self.service.execute({}, confirmed_sha256='a'*64)

    def test_unknown_does_not_claim_current_authority(self):
        self.service.prepare({})
        self.publisher.publish.return_value = ControlOutcome('original-job', Status.UNKNOWN)
        result = self.service.execute({}, confirmed_sha256='a'*64)
        self.assertNotIn('current_record_sha256', result)
        self.publisher.read_current.assert_not_called()

    def test_recovery_uses_only_retained_intent_and_reauthenticates(self):
        self.service.prepare({})
        self.publisher.recover.return_value = ControlOutcome('original-job', Status.UNKNOWN)
        self.service.execute({'fresh': 'assertion'}, confirmed_sha256='a'*64, recover=True)
        self.publisher.recover.assert_called_once_with({'fresh': 'assertion'}, self.intent)
        self.publisher.publish.assert_not_called()

    def test_unauthorized_prepare_and_current_read_fail_closed(self):
        self.publisher.prepare.side_effect = ValueError('DENIED')
        with self.assertRaisesRegex(ValueError, 'DENIED'): self.service.prepare({})
        self.publisher.read_current.side_effect = ValueError('EXPIRED')
        with self.assertRaisesRegex(ValueError, 'EXPIRED'): self.service.current({})

    def test_composition_keeps_writes_disabled_by_default(self):
        from tests.test_campaign_operator_policy_publication import manifest
        from src.application.identity import IapIdentityAdapter, IapSettings
        from tests.test_campaign_permission_lifecycle import AUDIENCE
        from src.ingestion.setup_workflow_tables import PROJECT, LOCATION
        client = Mock(project=PROJECT, location=LOCATION, default_query_job_config=None)
        with patch('src.application.campaign_operator_policy_publisher.ControlIO'):
            service = runtime.compose_operator_runtime(client=client,
                identity_adapter=IapIdentityAdapter(IapSettings(AUDIENCE)),
                manifest=manifest(), policy=policy(), reason_code='EXPLICIT_REVIEW')
        self.assertFalse(service._publisher.enabled)
        with patch('src.application.campaign_operator_policy_publisher.require_owned_worker'):
            with self.assertRaisesRegex(ValueError, 'OPERATOR_PUBLISHER_DISABLED'):
                service.prepare({})
        client.query.assert_not_called()

    def test_unsupervised_worker_cannot_compose(self):
        self.worker.stop()
        with self.assertRaisesRegex(ValueError, 'owned worker'):
            runtime.compose_operator_runtime(client=None, identity_adapter=None,
                manifest=None, policy=None, reason_code='DENIED')

    def test_read_only_composition_does_not_invent_a_reviewer_policy(self):
        service = runtime.ProtectedOperatorRuntime(publisher=self.service._publisher,
            policy=None, reason_code='READ_ONLY')
        self.assertFalse(service.current({})['approval_verified'])
        with self.assertRaisesRegex(ValueError, 'PROPOSAL_UNCONFIGURED'): service.prepare({})
        self.publisher.prepare.assert_not_called()
