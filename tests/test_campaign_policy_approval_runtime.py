"""Current review bindings around quality execution; no evaluation/release grant."""
from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
import json
from src.application import campaign_policy_approval_runtime as runtime
from src.application.campaign_policy_approval_reader import PolicyApprovalReader
from src.application.campaign_policy_approval_receipt import CurrentPolicyApproval
from src.application.campaign_quality_execution import QualityExecutionResult
from src.application.campaign_acceptance_contract import _hash
from src.application.workflow_contract import canonical_json_bytes
from tests.test_campaign_policy_approval_receipt import fixture
from tests.test_campaign_permission_lifecycle import NOW


class ApprovalRuntimeTests(TestCase):
    def setUp(self):
        row,args,authority,receipt=fixture()
        self.acceptance=replace(receipt,pins=replace(receipt.pins,kind='ACCEPTANCE'))
        self.clarification=receipt
        self.readers=[]
        for i,value in enumerate((self.acceptance,self.clarification)):
            reader=object.__new__(PolicyApprovalReader);reader.pins=value.pins;reader.client=Mock()
            reader.read=Mock(side_effect=[CurrentPolicyApproval(value,f'initial-{i}',NOW,NOW+timedelta(seconds=60),'a'*64),
                CurrentPolicyApproval(value,f'current-{i}',NOW,NOW+timedelta(seconds=60),'a'*64)])
            self.readers.append(reader)
        self.guard=runtime.PolicyApprovalRuntimeGuard(acceptance_reader=self.readers[0],clarification_reader=self.readers[1],clock=lambda:NOW)
        policy=SimpleNamespace(request=self.guard.policy_request,policy_canonical_json='canonical-policy',
            record_sha256='f'*64,native_job_id='initial-policy',observed_at=NOW,valid_until=NOW+timedelta(seconds=60))
        self.policy_patch=patch.object(runtime,'read_policy',side_effect=[policy,SimpleNamespace(**dict(vars(policy),native_job_id='current-policy'))]);self.policy_patch.start();self.addCleanup(self.policy_patch.stop)
        self.owner=patch.object(runtime,'require_owned_worker');self.owner.start();self.addCleanup(self.owner.stop)
        self.context=dict(version='campaign-quality-authoritative-execution-v1',report_sha256='f'*64,
            request=dict(workspace_id=receipt.pins.workspace_id,stage='CLARIFICATION'),sources=dict(
                policy=dict(policy_id=receipt.pins.policy_id,version=receipt.pins.policy_version,snapshot_sha256=receipt.pins.policy_sha256),
                prompt=dict(snapshot_sha256=receipt.pins.prompt_snapshot_sha256),model_profile_sha256=receipt.pins.model_profile_sha256))

    def quality(self):return QualityExecutionResult(SimpleNamespace(sha256='f'*64),canonical_json_bytes(self.context).decode(),_hash(self.context))

    def test_exact_before_after_reads_bind_without_granting_release(self):
        token=self.guard.capture({});quality=self.guard.bind({},token,self.quality())
        self.assertFalse(quality.release_allowed)
        bindings=json.loads(quality.context_canonical_json)['policy_review_bindings']
        self.assertFalse(bindings['qualifying_evaluation_verified']);self.assertEqual(len(bindings['receipts']),2)
        with self.assertRaisesRegex(ValueError,'CHECKPOINT'):self.guard.bind({},token,self.quality())

    def test_current_read_revocation_or_changed_authority_withholds(self):
        token=self.guard.capture({});self.readers[0].read.side_effect=ValueError('revoked')
        with self.assertRaisesRegex(ValueError,'revoked'):self.guard.bind({},token,self.quality())
        self.assertEqual(self.guard._pending,{})

    def changed_source(self, field):
        if field=='prompt':self.context['sources'][field]['snapshot_sha256']='0'*64
        elif field=='policy':self.context['sources'][field]['snapshot_sha256']='0'*64
        else:self.context['sources'][field]='0'*64
        token=self.guard.capture({})
        with self.assertRaisesRegex(ValueError,'CHANGED'):self.guard.bind({},token,self.quality())

    def test_actual_prompt_change_withholds(self):self.changed_source('prompt')
    def test_actual_profile_change_withholds(self):self.changed_source('model_profile_sha256')
    def test_actual_acceptance_change_withholds(self):self.changed_source('policy')

    def test_reconstructed_checkpoint_and_context_hash_change_are_rejected(self):
        token=self.guard.capture({})
        with self.assertRaisesRegex(ValueError,'CHECKPOINT'):self.guard.bind({},replace(token),self.quality())
        with self.assertRaisesRegex(ValueError,'CONTEXT_CHANGED'):
            self.guard.bind({},token,replace(self.quality(),context_sha256='0'*64))

    def test_discard_releases_capacity_after_failed_quality_check(self):
        token=self.guard.capture({});self.guard.discard(token);self.guard.discard(token)
        self.assertEqual(self.guard._pending,{})

    def test_unproven_or_changed_publication_cannot_bind_quality(self):
        token=self.guard.capture({})
        current=CurrentPolicyApproval(self.acceptance,'new-read',NOW,NOW+timedelta(seconds=60),'b'*64)
        self.readers[0].read.side_effect=None;self.readers[0].read.return_value=current
        with self.assertRaisesRegex(ValueError,'PUBLICATION_CHANGED'):
            self.guard.bind({},token,self.quality())
