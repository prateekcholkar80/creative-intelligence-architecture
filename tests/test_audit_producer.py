"""Fresh access/service audit production and immutable replay checks."""
from copy import deepcopy
from datetime import timedelta
import unittest
from unittest.mock import patch

from src.application.access_capture import capture_access
from src.application.audit_contract import (
    AuditTarget, MutationKind, MutationObservation, MutationOutcome, ResourceType,
    WorkflowAction, WorkflowObservation,
)
from src.application.audit_producer import produce_access_audit,produce_service_audit
from src.application.workflow_contract import ContractError
from tests.test_audit_contract import ACTOR,RESOURCE,WORKSPACE,context
from tests.test_workflow_contract import TIME


class AuditProducerTests(unittest.TestCase):
    def test_fresh_access_capture_retains_exact_row_and_replays(self):
        ctx=context();capture=capture_access(ctx['observation'],target=ctx['target'],
            trace_id='a'*32,observed_at=TIME)
        envelope=produce_access_audit(capture,recorded_at=TIME+timedelta(seconds=1))
        row=envelope.row()
        self.assertEqual(row['audit_id'],'access-v1:'+'a'*32)
        self.assertEqual(row['membership_revision'],5)
        self.assertEqual(row['resource_revision'],3)
        envelope.validate_replay(deepcopy(row))
        with self.assertRaisesRegex(ContractError,'AUDIT_OBSERVATION_MISMATCH'):
            envelope.validate_replay(row|{'allowed':False})

    def test_service_evidence_append_binds_scope_outcome_and_replay(self):
        target=AuditTarget(ResourceType.EVIDENCE_SNAPSHOT,'evidence-snapshot-1',WORKSPACE,
            conversation_id=RESOURCE.resource_id,owner_id=ACTOR.principal_id,
            request_id='fixture-request',job_id='fixture-job')
        observation=WorkflowObservation('fixture-worker',WORKSPACE,WorkflowAction.APPEND_EVIDENCE)
        mutation=MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED)
        envelope=produce_service_audit(observation,target=target,mutation=mutation,
            trace_id='b'*32,observed_at=TIME,recorded_at=TIME+timedelta(seconds=1))
        row=envelope.row()
        self.assertEqual(row['actor_kind'],'SERVICE')
        self.assertEqual(row['action'],'APPEND_EVIDENCE')
        self.assertEqual(row['resource_id'],'evidence-snapshot-1')
        self.assertEqual(row['mutation_outcome'],'APPLIED')
        self.assertIsNone(row['new_revision'])
        envelope.validate_replay(deepcopy(row))

    def test_stale_future_or_wrongly_typed_observations_fail_closed(self):
        ctx=context();capture=capture_access(ctx['observation'],target=ctx['target'],
            trace_id='c'*32,observed_at=TIME)
        for recorded in (TIME-timedelta(seconds=1),TIME+timedelta(seconds=31)):
            with self.subTest(recorded=recorded),self.assertRaisesRegex(
                    ContractError,'STALE_AUDIT_OBSERVATION'):
                produce_access_audit(capture,recorded_at=recorded)
        with self.assertRaises(ContractError):
            produce_access_audit(object(),recorded_at=TIME)

    def test_service_action_target_and_mutation_contracts_remain_authoritative(self):
        target=AuditTarget(ResourceType.EVIDENCE_SNAPSHOT,'evidence-snapshot-1',WORKSPACE,
            conversation_id=RESOURCE.resource_id,owner_id=ACTOR.principal_id,
            request_id='fixture-request',job_id='fixture-job')
        observation=WorkflowObservation('fixture-worker',WORKSPACE,WorkflowAction.APPEND_OUTPUT)
        mutation=MutationObservation(MutationKind.APPEND,MutationOutcome.APPLIED)
        with self.assertRaisesRegex(ContractError,'AUDIT_SCOPE_MISMATCH'):
            produce_service_audit(observation,target=target,mutation=mutation,trace_id='d'*32,
                observed_at=TIME,recorded_at=TIME+timedelta(seconds=1))

    def test_producers_do_not_call_cloud_or_authenticate(self):
        ctx=context();capture=capture_access(ctx['observation'],target=ctx['target'],
            trace_id='e'*32,observed_at=TIME)
        with (patch('google.cloud.bigquery.Client',side_effect=AssertionError('no BQ')),
              patch('google.cloud.storage.Client',side_effect=AssertionError('no GCS')),
              patch('src.application.identity.IapIdentityAdapter.authenticate',
                    side_effect=AssertionError('no authentication'))):
            envelope=produce_access_audit(capture,recorded_at=TIME+timedelta(seconds=1))
            envelope.validate_replay(envelope.row())


if __name__=='__main__':
    unittest.main()
