"""SDK-shaped runtime reads; no live IAP, SQL or model dispatch."""
from copy import deepcopy
from dataclasses import replace
import re
import unittest
from unittest.mock import Mock

from src.application.prompt_contract import PromptTask
from src.application.prompt_runtime_reader import (
    BigQueryPromptRuntimeReader,RuntimeReadRequest,RuntimeReadError,decode_snapshot,read_sql,read_config,
)
from src.application.workflow_contract import ContractError
from tests.test_prompt_lifecycle_store import Client,native_job,snapshot_rows
from tests.test_prompt_runtime_contract import capture


def metadata_client():
    client=Client();get_table=client.get_table
    from src.application.workflow_schema import TABLES as WORKFLOW
    from src.application.prompt_runtime_contract import TABLES as BINDING
    def metadata(ref,**kwargs):
        if ref.table_id=='conversations' or ref.table_id in BINDING:
            spec=WORKFLOW['conversations'] if ref.table_id=='conversations' else BINDING[ref.table_id]
            table=spec.table(client.project);table._properties.update(type='TABLE',location=client.location)
            return table
        return get_table(ref,**kwargs)
    client.get_table=metadata
    return client


def request():
    c = capture()
    return RuntimeReadRequest(c.request.workspace_id,'conversation-1',c.request.prompt_key,
                             c.request.task,c.request.identity,c.request.membership_revision)


class RuntimeReaderTests(unittest.TestCase):
    def test_native_query_uses_creator_owner_exact_active_and_current_evaluation(self):
        sql,cfg = read_sql(),read_config(request())
        self.assertIn("'CREATOR' IN UNNEST(roles)",sql)
        self.assertNotIn("'ADMIN' IN UNNEST(roles)",sql)
        self.assertIn('owner_id=@principal_id',sql)
        self.assertIn("prompt_key=@prompt_key AND status='ACTIVE'",sql)
        self.assertIn('evaluation_id=(SELECT evaluation_id FROM target_heads_snapshot)',sql)
        self.assertIn('t.snapshot_sha256=r.snapshot_sha256',sql)
        self.assertEqual(set(re.findall(r'(?<!@)@([a-z_]+)',sql)),{p.name for p in cfg.query_parameters})
        self.assertFalse(cfg.use_query_cache)
        self.assertEqual(cfg.maximum_bytes_billed,512*1024**2)

    def test_complete_capture_and_original_evaluation_anchor_decode(self):
        self.assertEqual(decode_snapshot(request(),snapshot_rows(capture())),capture())

    def test_duplicate_absent_partial_corrupt_and_cross_task_heads_deny(self):
        original = snapshot_rows(capture())[0]
        for name in ('target_heads','active_heads','controls','prompt_evaluations','prompt_evaluation_cases','target_history'):
            for mode in ('missing','duplicate'):
                row = deepcopy(original)
                row[name] = [] if mode=='missing' else row[name]+row[name]
                row[name+'_count'] = len(row[name])
                with self.subTest(name=name,mode=mode),self.assertRaises(ContractError):
                    decode_snapshot(request(),[row])
        with self.assertRaises(ContractError):
            decode_snapshot(replace(request(),task=PromptTask.INTERPRETER),[original])
        row = deepcopy(original);row['prompt_evaluations'][0]['evaluation_digest']='a'*64
        with self.assertRaises(ContractError):
            decode_snapshot(request(),[row])

    def test_native_identity_before_and_after_result_is_required(self):
        client = metadata_client()
        def query(script,**kwargs):
            job=native_job(kwargs['job_id'],script,kwargs['job_config'])
            job.result=Mock(return_value=snapshot_rows(capture()))
            return job
        client.query = Mock(side_effect=query)
        reader = BigQueryPromptRuntimeReader(client)
        self.assertEqual(reader.read(request()),capture())
        client.query.assert_called_once()
        def wrong(script,**kwargs):
            job = query(script,**kwargs);job._properties['jobReference']['location']='us-central1';return job
        client.query.side_effect=wrong
        with self.assertRaises(RuntimeReadError):reader.read(request())

    def test_metadata_and_cloud_failure_have_no_fallback_or_replay(self):
        client=Client();client.query=Mock(side_effect=TimeoutError('private provider details'))
        reader=BigQueryPromptRuntimeReader(client)
        with self.assertRaises(RuntimeReadError) as error:reader.read(request())
        self.assertEqual(error.exception.code,'RUNTIME_READ_UNAVAILABLE')
        self.assertNotIn('private',str(error.exception));client.query.assert_called_once()

    def test_fresh_stored_binding_exact_count_and_native_identity_required(self):
        from tests.test_prompt_runtime_contract import binding
        client=metadata_client();row=dict(observed_at=capture().observed_at,
            binding_count=1,bindings=[binding().row()])
        def query(script,**kwargs):
            self.assertIn('owner_id=@principal_id',script)
            self.assertEqual(set(re.findall(r'(?<!@)@([a-z_]+)',script)),
                             {p.name for p in kwargs['job_config'].query_parameters})
            job=native_job(kwargs['job_id'],script,kwargs['job_config'])
            job.result=Mock(return_value=[row]);return job
        client.query=Mock(side_effect=query);reader=BigQueryPromptRuntimeReader(client)
        self.assertEqual(reader.read_binding(request(),'invocation-1'),row)
        for count,values in ((0,[]),(2,[binding().row(),binding().row()]),(True,[binding().row()])):
            row.update(binding_count=count,bindings=values)
            with self.assertRaises(RuntimeReadError):reader.read_binding(request(),'invocation-1')


if __name__ == '__main__':unittest.main()
