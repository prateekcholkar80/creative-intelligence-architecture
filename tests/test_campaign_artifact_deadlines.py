"""Clock-controlled deadline boundaries; no sleeps, cloud calls or provider use."""
import json
from unittest import TestCase
from unittest.mock import patch
from src.application.operation_budget import worker_operation_scope,OperationStopped
from tests.test_campaign_artifact_execution import ArtifactExecutionTests


class ArtifactDeadlineTests(TestCase):
    def test_expired_source_check_cannot_reserve_or_upload(self):
        p,e,store,objects,guard,*_=ArtifactExecutionTests().fixture();clock=[0.]
        def expire(intent):clock[0]=101.;return True
        guard.side_effect=expire
        with self.assertRaises(OperationStopped),patch('src.application.operation_budget.time.monotonic',side_effect=lambda:clock[0]),worker_operation_scope(100):
            e.execute(p)
        store.reserve.assert_not_called();objects.put_original.assert_not_called()

    def test_expired_score_cannot_reserve_a_new_scoring_slot(self):
        from src.application.campaign_artifact_measurement import measure_banner
        p,e,store,objects,guard,intents,outcomes,_=ArtifactExecutionTests().fixture();clock=[0.]
        def expire(*args,**kw):
            value=measure_banner(*args,**kw);clock[0]=101.;return value
        with self.assertRaises(OperationStopped),patch('src.application.operation_budget.time.monotonic',side_effect=lambda:clock[0]),worker_operation_scope(100),\
                patch('src.application.campaign_artifact_execution.measure_banner',side_effect=expire):
            e.execute(p)
        self.assertEqual(store.reserve.call_count,1);self.assertEqual(len(outcomes),2)
        objects.put_original.assert_called_once()
        # A fresh explicit action reuses the saved artifact, creates only scoring.
        result=ArtifactExecutionTests().execute(e,p)
        self.assertEqual(result['measurements']['status'],'PASS');objects.put_original.assert_called_once()

    def test_expiry_after_upload_holds_original_slot_without_second_upload(self):
        p,e,store,objects,guard,intents,outcomes,_=ArtifactExecutionTests().fixture();clock=[0.]
        put=objects.put_original.side_effect
        def expire(*args,**kw):
            value=put(*args,**kw);clock[0]=101.;return value
        objects.put_original.side_effect=expire
        with self.assertRaises(OperationStopped),patch('src.application.operation_budget.time.monotonic',side_effect=lambda:clock[0]),worker_operation_scope(100):
            e.execute(p)
        self.assertEqual(len(outcomes),1)
        with self.assertRaisesRegex(ValueError,'ARTIFACT_ORIGINAL_UNCERTAIN'):ArtifactExecutionTests().execute(e,p)
        objects.put_original.assert_called_once()
