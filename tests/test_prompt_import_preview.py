"""Offline exact-Vertex import preview and canonical-diff tests."""
from dataclasses import replace
from datetime import timedelta
import unittest

from src.application.prompt_contract import (
    PromptLifecycle,
    PromptTask,
    initial_prompt_record,
    propose_prompt_transition,
)
from src.application.prompt_import_preview import (
    MAX_DIFF_FIELDS,
    MAX_DIFF_PREVIEW_CHARACTERS,
    PromptDiffChange,
    PromptImportPreviewError,
    PromptImportPreviewRequest,
    PromptImportPreviewService,
    PromptImportPreviewStatus,
    canonical_diff,
)
from src.application.prompt_registry_codec import PromptRegistryState
from src.application.prompt_registry_reader import (
    PromptRegistryRead,
    PromptRegistryReadError,
    PromptRegistryReadRequest,
)
from src.application.vertex_prompt_reader import read_exact_vertex_prompt
from src.application.workflow_contract import ContractError
from tests.test_prompt_contract import ACTOR, NOW, snapshot
from tests.test_vertex_prompt_reader import FakeTransport, request as vertex_request


def active_state(*, version='3', system_instruction=None):
    value = snapshot(vertex_version_id=version)
    if system_instruction is not None:
        value = replace(value, system_instruction=system_instruction)
    records = [initial_prompt_record(
        value, imported_at=NOW - timedelta(seconds=3), imported_by=ACTOR)]
    records.append(propose_prompt_transition(
        records[-1], status=PromptLifecycle.REGISTERED,
        changed_at=NOW - timedelta(seconds=2), changed_by=ACTOR,
        reason_code='OPERATOR_REVIEWED'))
    records.append(propose_prompt_transition(
        records[-1], status=PromptLifecycle.EVALUATED,
        changed_at=NOW - timedelta(seconds=1), changed_by=ACTOR,
        reason_code='EVALUATED', evaluation_id='evaluation-1'))
    records.append(propose_prompt_transition(
        records[-1], status=PromptLifecycle.ACTIVE,
        changed_at=NOW, changed_by=ACTOR,
        reason_code='OPERATOR_REVIEWED', evaluation_id='evaluation-1'))
    return PromptRegistryState(records[-1], tuple(records))


def captured(state, request):
    return PromptRegistryRead(request, state, NOW + timedelta(seconds=1))


class FakeRegistryReader:
    def __init__(self, *, exact=None, active=None,
                 exact_error='PROMPT_REGISTRY_NOT_FOUND',
                 active_error='PROMPT_REGISTRY_NOT_FOUND'):
        self.exact = exact
        self.active = active
        self.exact_error = exact_error
        self.active_error = active_error
        self.calls = []

    def read_exact(self, prompt_id, version_id):
        self.calls.append(('exact', prompt_id, version_id))
        if self.exact is None:
            raise PromptRegistryReadError(self.exact_error)
        request = PromptRegistryReadRequest.exact(prompt_id, version_id)
        return captured(self.exact, request)

    def read_eligible(self, task, prompt_key):
        self.calls.append(('eligible', task, prompt_key))
        if self.active is None:
            raise PromptRegistryReadError(self.active_error)
        request = PromptRegistryReadRequest.eligible(task, prompt_key)
        return captured(self.active, request)


def preview_request(**changes):
    values = {
        'vertex_request': vertex_request(),
        'previewed_at': NOW + timedelta(seconds=2),
        'proposed_by': ACTOR,
    }
    return PromptImportPreviewRequest(**(values | changes))


class PromptImportPreviewTests(unittest.TestCase):
    def service(self, reader=None, transport=None):
        self.transport = FakeTransport() if transport is None else transport
        self.reader = FakeRegistryReader() if reader is None else reader
        return PromptImportPreviewService(self.transport, self.reader)

    def assert_code(self, code, function, *args, **kwargs):
        with self.assertRaises(
                (ContractError, PromptImportPreviewError)) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_unregistered_without_active_returns_hash_bound_draft(self):
        result = self.service().preview(preview_request())
        self.assertEqual(result.status, PromptImportPreviewStatus.UNREGISTERED)
        self.assertIsNone(result.exact_state)
        self.assertIsNone(result.active_state)
        self.assertEqual(result.proposal.head.snapshot, result.candidate)
        self.assertEqual(result.proposal.head.status, PromptLifecycle.DRAFT)
        self.assertEqual(result.proposal.head.changed_at,
                         preview_request().previewed_at)
        self.assertEqual(result.proposal.head.changed_by, ACTOR)
        self.assertEqual(len(result.differences), MAX_DIFF_FIELDS)
        self.assertTrue(all(item.change is PromptDiffChange.ADDED
                            and item.before is None
                            for item in result.differences))
        self.assertEqual(len(result.preview_sha256), 64)
        self.assertEqual(self.transport.calls, [{
            'prompt_id': '1234567890', 'version_id': '4',
            'timeout_ms': 10_000}])
        self.assertEqual(self.reader.calls, [
            ('exact', '1234567890', '4'),
            ('eligible', PromptTask.PLANNER, 'campaign-planner')])

    def test_active_baseline_produces_ordered_exact_field_diff(self):
        active = active_state(version='3')
        result = self.service(FakeRegistryReader(active=active)).preview(
            preview_request())
        self.assertEqual(result.active_state, active)
        # Same template/settings and a different Vertex address means no content
        # field changed; version identity remains bound by the preview hash.
        self.assertEqual(result.differences, ())
        changed = active_state(
            version='3',
            system_instruction='Use verified context for {campaign_request}.')
        result = self.service(FakeRegistryReader(active=changed)).preview(
            preview_request())
        self.assertEqual(
            [item.field for item in result.differences],
            ['system_instruction'])
        entry = result.differences[0]
        self.assertEqual(entry.change, PromptDiffChange.MODIFIED)
        self.assertNotEqual(entry.before.sha256, entry.after.sha256)

    def test_existing_exact_state_is_returned_without_new_proposal(self):
        candidate = read_exact_vertex_prompt(FakeTransport(), vertex_request())
        exact = active_state(version='4')
        self.assertEqual(exact.head.snapshot, candidate)
        reader = FakeRegistryReader(exact=exact, active=exact)
        first = self.service(reader).preview(preview_request())
        second = self.service(reader, FakeTransport()).preview(preview_request())
        self.assertEqual(first.status, PromptImportPreviewStatus.EXISTING)
        self.assertEqual(first.exact_state, exact)
        self.assertIsNone(first.proposal)
        self.assertEqual(first.differences, ())
        self.assertEqual(first.preview_sha256, second.preview_sha256)

    def test_vertex_and_exact_registry_content_mismatch_fails_closed(self):
        exact = active_state(
            version='4',
            system_instruction='Use verified context for {campaign_request}.')
        self.assert_code(
            'PROMPT_PREVIEW_VERTEX_REGISTRY_MISMATCH',
            self.service(FakeRegistryReader(exact=exact)).preview,
            preview_request())

    def test_active_state_consistency_is_required_for_same_candidate(self):
        exact = active_state(version='4')
        self.assert_code(
            'PROMPT_PREVIEW_ACTIVE_STATE_CONFLICT',
            self.service(FakeRegistryReader(exact=exact)).preview,
            preview_request())
        reader = FakeRegistryReader(active=exact)
        self.assert_code(
            'PROMPT_PREVIEW_ACTIVE_STATE_CONFLICT',
            self.service(reader).preview, preview_request())

    def test_only_not_found_is_an_expected_registry_outcome(self):
        for target in ('exact', 'active'):
            reader = FakeRegistryReader()
            if target == 'exact':
                reader.exact_error = 'PROMPT_REGISTRY_DUPLICATE_HEAD'
            else:
                reader.active_error = 'PROMPT_REGISTRY_INCOMPLETE_HISTORY'
            with self.subTest(target=target):
                self.assert_code(
                    getattr(reader, target + '_error'),
                    self.service(reader).preview, preview_request())

    def test_vertex_contract_and_provider_errors_are_safe(self):
        wrong = FakeTransport()
        wrong.value._dataset_version.name = (
            'projects/project-661d501a-e709-4276-a26/locations/asia-south1/'
            'datasets/1234567890/datasetVersions/5')
        self.assert_code(
            'VERTEX_PROMPT_VERSION_IDENTITY_MISMATCH',
            self.service(transport=wrong).preview, preview_request())

        class FailedTransport(FakeTransport):
            def get_exact_version(self, **kwargs):
                raise RuntimeError('private provider response')

        with self.assertRaises(PromptImportPreviewError) as caught:
            self.service(transport=FailedTransport()).preview(preview_request())
        self.assertEqual(caught.exception.code,
                         'PROMPT_PREVIEW_VERTEX_UNAVAILABLE')
        self.assertNotIn('private', str(caught.exception))

    def test_request_actor_time_and_service_configuration_are_strict(self):
        for changes in (
                {'vertex_request': {}},
                {'previewed_at': NOW.replace(tzinfo=None)},
                {'proposed_by': 'bad actor'}):
            with self.subTest(changes=changes), self.assertRaises(ContractError):
                preview_request(**changes)
        for timeout in (0, 31, True):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                PromptImportPreviewService(
                    FakeTransport(), FakeRegistryReader(),
                    vertex_timeout_seconds=timeout)
        with self.assertRaises(ValueError):
            PromptImportPreviewService(FakeTransport(), object())

    def test_diff_snippets_are_bounded_but_hash_full_canonical_values(self):
        candidate = read_exact_vertex_prompt(FakeTransport(), vertex_request())
        long = replace(
            candidate,
            system_instruction=('A' * 600 + ' {campaign_request}'))
        differences = canonical_diff(candidate, long)
        self.assertEqual(len(differences), 1)
        value = differences[0].after
        self.assertTrue(value.truncated)
        self.assertEqual(len(value.preview), MAX_DIFF_PREVIEW_CHARACTERS)
        self.assertGreater(value.utf8_bytes, len(value.preview.encode('utf-8')))

    def test_preview_object_rejects_omitted_diff_or_changed_outer_hash(self):
        result = self.service().preview(preview_request())
        self.assert_code('PROMPT_IMPORT_PREVIEW_DIFF_MISMATCH',
                         replace, result, differences=())
        self.assert_code('PROMPT_IMPORT_PREVIEW_HASH_MISMATCH',
                         replace, result, preview_sha256='f' * 64)

    def test_preview_has_no_writer_local_fallback_or_model_path(self):
        service = self.service()
        result = service.preview(preview_request())
        self.assertEqual(len(self.transport.calls), 1)
        self.assertEqual(len(self.reader.calls), 2)
        for forbidden in ('submit', 'register', 'activate', 'generate', 'model'):
            self.assertFalse(hasattr(service, forbidden))
        self.assertIsNotNone(result.proposal)


if __name__ == '__main__':
    unittest.main()
