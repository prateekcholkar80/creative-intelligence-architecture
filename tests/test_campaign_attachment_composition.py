from datetime import datetime,timezone,timedelta
import hashlib,json,tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_attachment_composition as module
from tests.test_campaign_attachment_contract import plan


class AttachmentCompositionTests(TestCase):
    def test_independent_reads_overlap_but_final_pass_waits_for_initial_pair(self):
        from threading import Barrier,Lock
        barrier=Barrier(2);lock=Lock();events=[]
        def read(kind):
            with lock:events.append(kind)
            barrier.wait(timeout=3)
            return Mock()
        reader=module.StoredAttachmentPlanReader(client=Mock(),verifier=Mock(),access_reader=Mock(),policy_request=Mock())
        with patch.object(module,'read_execution_authority',side_effect=lambda *a,**k:read('authority')), \
             patch.object(module,'read_policy',side_effect=lambda *a,**k:read('policy')):
            with self.assertRaises((ValueError,TypeError)):
                reader._optional_reference(Mock(),'w','c','request')
        self.assertCountEqual(events[:2],['authority','policy'])
        self.assertCountEqual(events[2:],['authority','policy'])

    def test_optional_reference_binds_fresh_request_authority_and_policy(self):
        now=datetime.now(timezone.utc);identity=Mock();identity.principal.principal_id='pilot'
        authority=dict(workspace_id='w',conversation_id='c',request_id='request',sku='CI-SKU-0001',
            country='US',channel='META',audience='NEW',request_sha256='a'*64,principal_id='pilot')
        content=json.dumps(dict(authority=authority,transcript_sha256='b'*64))
        captures=[Mock(content_canonical_json=content,content_sha256='c'*64,native_job_id=n,
            valid_until=now+timedelta(seconds=30)) for n in ('read1','read2')]
        policies=[Mock(record_sha256='d'*64,native_job_id=n,valid_until=now+timedelta(seconds=30)) for n in ('p1','p2')]
        for policy in policies:policy.request.policy_sha256='f'*64
        reader=module.StoredAttachmentPlanReader(client=Mock(),verifier=Mock(),access_reader=Mock(),policy_request=Mock(),clock=lambda:now)
        with patch.object(module,'read_execution_authority',side_effect=captures),patch.object(module,'read_policy',side_effect=policies):
            result=reader._optional_reference(identity,'w','c','request')
        value=json.loads(result.canonical_json)
        self.assertEqual(value['scope']['request_id'],'request');self.assertFalse(value['generation_authorized'])
        captures[1].content_sha256='e'*64
        with patch.object(module,'read_execution_authority',side_effect=captures),patch.object(module,'read_policy',side_effect=policies):
            with self.assertRaises(ValueError):reader._optional_reference(identity,'w','c','request')

    def test_absent_or_disabled_config_does_not_create_cloud_clients(self):
        with patch.object(module.storage,'Client') as storage:
            self.assertIsNone(module.compose_attachments(client=Mock(),verifier=Mock(),environ={}))
            with tempfile.TemporaryDirectory() as folder:
                path=Path(folder)/'config.json';path.write_text(json.dumps(dict(enabled=False,bucket='bucket',policy={})))
                self.assertIsNone(module.compose_attachments(client=Mock(),verifier=Mock(),environ={module.CONFIG_ENV:str(path)}))
            storage.assert_not_called()

    def test_reader_requires_exact_persisted_response_and_successful_call(self):
        now=datetime.now(timezone.utc);response='stored intent'
        digest=hashlib.sha256(response.encode()).hexdigest()
        records=dict(model_invocations=['call'],campaign_transcript_responses=[json.dumps(dict(
            request_id='request',invocation_id='invocation',content_sha256=digest,response_text=response))])
        capture=Mock(valid_until=now+timedelta(seconds=30));capture.records.return_value=records
        reader=module.StoredAttachmentPlanReader(client=Mock(),verifier=Mock(),access_reader=Mock(),policy_request=Mock(),clock=lambda:now)
        call=dict(request_id='request',invocation_id='invocation',stage='INTERPRETER',state='SUCCEEDED')
        with patch.object(module,'read_question_context',return_value=capture),patch.object(module,'decode_row',return_value=call), \
             patch.object(module,'verify_stored_response',return_value=digest),patch.object(module,'read_bound_gaps') as gaps:
            gaps.return_value=Mock(plan=plan(),valid_until=now+timedelta(seconds=20))
            self.assertEqual(reader({},workspace_id='w',conversation_id='c',request_id='request'),gaps.return_value.plan)
            self.assertEqual(gaps.call_args.kwargs['observed_interpreter_response'],response)
            call['state']='FAILED'
            with patch.object(reader,'_optional_reference',return_value='optional') as optional:
                self.assertEqual(reader({},workspace_id='w',conversation_id='c',request_id='request'),'optional')
                optional.assert_called_once()
            call['state']='SUCCEEDED';records['campaign_transcript_responses'][0]=records['campaign_transcript_responses'][0].replace(response,'changed')
            with self.assertRaises(ValueError):reader({},workspace_id='w',conversation_id='c',request_id='request')
