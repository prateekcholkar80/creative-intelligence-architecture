"""Request binding composition guards; synthetic interface positives stay diagnostic."""
from dataclasses import replace
from datetime import timedelta
import json
from unittest import TestCase
from unittest.mock import Mock, patch

from src.application import campaign_quality_execution as execution
from src.application.campaign_acceptance_contract import Stage
from src.application.campaign_quality_request_reader import read_request_context
from src.application.workflow_contract import ContractError
from tests import test_campaign_quality_execution as execution_fixtures
from tests.test_campaign_quality_request_reader import client as request_client, capture as stored_capture
from tests.test_workflow_history_service import identity

request=execution_fixtures.request
request_capture=execution_fixtures.request_capture


class RequestExecutionTests(TestCase):
    def setUp(self):
        # Reuse installed dependency fixtures, without inheriting/rerunning their tests.
        self.fixture=execution_fixtures.QualityExecutionTests('test_fresh_rechecks_bind_every_context_and_never_release')
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.current=self.fixture.current

    def run_check(self, **kw):return self.fixture.run_check(**kw)

    def captures(self, **changes):
        self.fixture.requests.side_effect=[request_capture(self.current),
            request_capture(self.current,native_id='request-read-2',**changes)]

    def test_changed_request_history_revision_access_status_and_resolution_withhold(self):
        for changes in (dict(request_sha256='c'*64),dict(history_sha256='c'*64),
            dict(request_record_sha256='c'*64),dict(request_revision=2),dict(access_sha256='c'*64),
            dict(history_count=2),dict(request_status='BLOCKED'),dict(canonical_sku='CI-SKU-0022'),
            dict(channel='EMAIL')):
            self.captures(**changes)
            with self.subTest(changes=changes),self.assertRaisesRegex(ContractError,
                'QUALITY_EXECUTION_REQUEST_CONTEXT_CHANGED'):self.run_check()

    def test_capture_scope_identity_hash_shape_and_history_kind_are_revalidated(self):
        for changes in (dict(workspace_id='other'),dict(conversation_id='other'),dict(request_id='other'),
            dict(principal_id='other'),dict(history_kind='CHAT_TRANSCRIPT'),dict(history_count=True),
            dict(request_revision=False),dict(extra='x')):
            self.fixture.requests.side_effect=None
            self.fixture.requests.return_value=request_capture(self.current,**changes)
            b=Mock()
            with self.assertRaises(ContractError):self.run_check(b=b)
            b.assert_not_called()
        for changed in (replace(request_capture(self.current),bindings_sha256='c'*64),
            replace(request_capture(self.current),bindings_canonical_json='[]'),None):
            self.fixture.requests.return_value=changed
            with self.assertRaises(ContractError):self.run_check()

    def test_checker_runs_before_final_capture_and_change_during_check_denies(self):
        events=[]
        self.fixture.requests.side_effect=lambda *a,**kw: (
            events.append('capture') or request_capture(self.current,native_id=f'capture-{len(events)}',
                history_sha256='c'*64 if 'check' in events else 'b'*64))
        original=execution.check_candidate
        def checking(*a,**kw):
            events.append('check')
            return original(*a,**kw)
        with patch.object(execution,'check_candidate',side_effect=checking):
            with self.assertRaisesRegex(ContractError,'QUALITY_EXECUTION_REQUEST_CONTEXT_CHANGED'):
                self.run_check()
        self.assertEqual(events,['capture','check','capture'])

    def test_new_read_ids_and_times_do_not_rebase_content_but_reuse_of_one_job_denies(self):
        original=request_capture(self.current)
        # Same content with a genuinely distinct read ID is valid diagnostically.
        elapsed=0
        self.fixture.clock=lambda:self.current.observed_at+timedelta(seconds=elapsed)
        def build(fact):
            nonlocal elapsed
            elapsed=1
            return execution_fixtures.builder(fact)
        self.fixture.requests.side_effect=[original,replace(original,native_job_id='fresh-read',
            observed_at=original.observed_at+timedelta(seconds=1),valid_until=original.valid_until+timedelta(seconds=1))]
        result=self.run_check(b=build)
        context=json.loads(result.context_canonical_json)
        self.assertEqual(context['request_native_job_ids'],['request-read-1','fresh-read'])
        self.assertEqual(context['request_bindings_sha256'],original.bindings_sha256)
        self.assertFalse(result.release_allowed)
        elapsed=0
        self.fixture.requests.side_effect=[original,original]
        with self.assertRaisesRegex(ContractError,'QUALITY_EXECUTION_REQUEST_CONTEXT_CHANGED'):self.run_check()

    def test_stale_initial_final_and_original_capture_expiry_deny(self):
        original=request_capture(self.current)
        for first,second in ((replace(original,observed_at=original.observed_at-timedelta(seconds=1)),original),
            (original,replace(original,native_job_id='fresh',valid_until=original.observed_at)),
            (replace(original,valid_until=original.valid_until+timedelta(seconds=1)),original)):
            self.fixture.requests.side_effect=[first,second]
            with self.assertRaises(ContractError):self.run_check()
        # Original expiry is never extended by a fresh final capture.
        self.fixture.requests.side_effect=[replace(original,valid_until=original.observed_at+timedelta(seconds=1)),
            replace(original,native_job_id='fresh')]
        elapsed=0
        def access(*a,**kw):
            nonlocal elapsed
            if self.fixture.access.authorize.call_count%3==0:elapsed=2
            return self.fixture.decision
        self.fixture.access.authorize.reset_mock()
        self.fixture.access.authorize.side_effect=access
        self.fixture.clock=lambda:self.current.observed_at+timedelta(seconds=elapsed)
        with self.assertRaisesRegex(ContractError,'QUALITY_EXECUTION_EXPIRED'):self.run_check()

    def test_stage_unresolved_sku_and_coarse_channel_mismatch_deny(self):
        for changes in (dict(operation='INTERPRET'),dict(canonical_sku=None),dict(canonical_sku='CI-SKU-0001'),
            dict(channel=None),dict(channel='EMAIL'),dict(request_status='BLOCKED'),dict(message_sha256='f'*64)):
            self.fixture.requests.side_effect=None
            self.fixture.requests.return_value=request_capture(self.current,**changes)
            b=Mock()
            with self.assertRaises(ContractError):self.run_check(b=b)
            b.assert_not_called()
        self.fixture.requests.return_value=request_capture(self.current)
        with self.assertRaisesRegex(ContractError,'QUALITY_REQUEST_STAGE_UNSUPPORTED'):
            self.run_check(r=request(stage=Stage.REFINEMENT))

    def test_interpretation_uses_message_hash_and_refuses_structured_input_hash(self):
        pins=json.loads(request_capture(self.current).bindings_canonical_json)
        pins.update(operation='INTERPRET',message_sha256='f'*64,request_status='READY')
        r=request(stage=Stage.INTERPRETATION,user_input_sha256='f'*64)
        execution._bind_request(r,pins)
        with self.assertRaisesRegex(ContractError,'QUALITY_REQUEST_INPUT_MISMATCH'):
            execution._bind_request(replace(r,user_input_sha256=pins['request_sha256']),pins)
        with self.assertRaisesRegex(ContractError,'QUALITY_REQUEST_STATE_UNSUPPORTED'):
            execution._bind_request(replace(r,stage=Stage.CLARIFICATION),pins)
        execution._bind_request(replace(r,stage=Stage.CLARIFICATION),pins | dict(request_status='NEEDS_CLARIFICATION'))

    def test_capture_unavailability_before_or_after_check_has_no_fallback(self):
        for responses in ([RuntimeError('unavailable')],
                          [request_capture(self.current),RuntimeError('unavailable')]):
            self.fixture.requests.side_effect=responses
            with self.assertRaises(RuntimeError):self.run_check()

    def test_real_request_reader_legacy_sku_cannot_be_relabelled_as_nt_request(self):
        row=stored_capture();stored_identity=identity()
        # Use the real decoder/native SDK boundary; no patches to catalog validation.
        read=read_request_context(request_client(row),workspace_id='quality-request-workspace',
            conversation_id='fixture-conversation',request_id='fixture-request',identity=stored_identity,
            clock=lambda:row['observed_at'])
        pins=json.loads(read.bindings_canonical_json)
        with self.assertRaisesRegex(ContractError,'QUALITY_REQUEST_SKU_UNBOUND'):
            execution._bind_request(request(user_input_sha256=pins['request_sha256'],
                conversation_sha256=pins['history_sha256']),pins)

    def test_real_request_reader_rejects_nt_row_before_annotation(self):
        row=stored_capture()
        value=json.loads(row['requests'][0]);value['canonical_sku']='CI-SKU-0021'
        value['submitted_preferences']['preferences']['canonical_sku']='CI-SKU-0021'
        row['requests']=[json.dumps(value)]
        with self.assertRaises(ContractError):
            read_request_context(request_client(row),workspace_id='quality-request-workspace',
                conversation_id='fixture-conversation',request_id='fixture-request',identity=identity(),
                clock=lambda:row['observed_at'])
