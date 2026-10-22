from copy import deepcopy
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
from src.application import campaign_quality_authority_writer as writer
from src.application.access_policy import AccessDecision,Action,Reason
from src.application.workflow_contract import ContractError
from tests.test_campaign_quality_authority_reader import fixture
from tests.test_workflow_history_service import identity


class AuthorityWriterTests(TestCase):
    def setUp(self):
        self.row,self.payload=fixture();self.row['authorities']=[];self.identity=identity()
        self.access=AccessDecision(True,Action.GENERATE_DRAFT,Reason.ALLOWED_OWNER,
            self.payload['workspace_id'],self.identity.principal.principal_id,1,None,None)
        self.owner=AccessDecision(True,Action.READ_DRAFT,Reason.ALLOWED_OWNER,self.payload['workspace_id'],
            self.identity.principal.principal_id,1,self.payload['conversation_id'],1)
        self.reader=Mock(authorize=Mock(side_effect=lambda ident,action,**kw:
            self.access if action is Action.GENERATE_DRAFT else self.owner));self.client=Mock()
        at=self.row['observed_at']
        self.native=patch.object(writer,'_read_native',return_value=(self.row,'native-read',at,at)).start()
        self.addCleanup(patch.stopall)

    def prepare(self,payload=None,settings=writer.AuthorityWriterSettings(True)):
        return writer.prepare_authority_write(self.client,self.payload if payload is None else payload,
            identity=self.identity,access_reader=self.reader,settings=settings)

    def test_default_denies_before_source_reads(self):
        with self.assertRaisesRegex(ContractError,'AUTHORITY_WRITES_DISABLED'):
            self.prepare(settings=writer.AuthorityWriterSettings())
        self.native.assert_not_called();self.reader.authorize.assert_not_called()

    def test_preparation_is_source_validated_nonmutating_and_globally_reserved(self):
        before=deepcopy(self.row);plan=self.prepare()
        self.assertEqual(before,self.row);self.client.query.assert_not_called()
        self.assertIn('AUTHORITY_SOURCE_CHANGED',plan.sql)
        self.assertIn('AUTHORITY_CAPTURE_AFTER_DISPATCH',plan.sql)
        self.assertIn("created_at BETWEEN TIMESTAMP('0001-01-01')",plan.sql)
        # Both started_at-partitioned sources plus the post-dispatch assertion.
        self.assertEqual(plan.sql.count(writer.partition_predicate('model_invocations')),3)
        self.assertIn('revision=revision+1',plan.sql)
        self.assertIn('ROLLBACK TRANSACTION',plan.sql)
        self.assertNotIn('DELETE ',plan.sql)
        self.assertNotIn('FULL OUTER JOIN UNNEST',plan.sql)
        cfg=plan.config();params={p.name:p for p in cfg.query_parameters}
        for key in ('expected_calls','expected_jobs','expected_attempts','expected_bindings'):
            self.assertEqual(params[key].type_,'STRING')
            self.assertEqual(params[key].value,'[]')
        self.assertIn("HAVING COUNTIF(origin='CURRENT')!=COUNTIF(origin='EXPECTED')",plan.sql)
        other=self.prepare(self.payload|dict(country='CA'))
        self.assertEqual(plan.job_id,other.job_id)
        self.assertNotEqual(plan.candidate_json,other.candidate_json)

    def test_owner_or_role_denial_happens_before_source_capture(self):
        self.reader.authorize.side_effect=None
        self.reader.authorize.return_value=SimpleNamespace(allowed=True)
        with self.assertRaises(ContractError):self.prepare()
        self.native.assert_not_called()

    def test_context_mismatch_cannot_be_published(self):
        with self.assertRaises(ContractError):self.prepare(self.payload|dict(request_sha256='c'*64))
        with self.assertRaises(ContractError):self.prepare(self.payload|dict(sku='CI-SKU-0022'))

    def test_submission_reuses_original_io_and_recovery_never_submits(self):
        plan=self.prepare()
        io=Mock();io.submit_once.return_value=SimpleNamespace(rows=({'state':'APPLIED','payload_sha256':plan.command_id},))
        io.recover.return_value=SimpleNamespace(rows=())
        with patch.object(writer,'ControlIO',return_value=io):
            writer.submit_authority_write(self.client,plan,identity=self.identity,access_reader=self.reader,
                settings=writer.AuthorityWriterSettings(True))
            io.submit_once.assert_called_once_with(plan)
            writer.submit_authority_write(self.client,plan,identity=self.identity,access_reader=self.reader,
                settings=writer.AuthorityWriterSettings(True),recover_only=True)
            io.recover.assert_called_once_with(plan)
            self.assertEqual(io.submit_once.call_count,1)

    def test_wrong_receipt_or_default_submit_denies(self):
        plan=self.prepare()
        with self.assertRaises(ContractError):
            writer.submit_authority_write(self.client,plan,identity=self.identity,access_reader=self.reader)
        io=Mock();io.submit_once.return_value=SimpleNamespace(rows=({'state':'APPLIED','payload_sha256':'c'*64},))
        with patch.object(writer,'ControlIO',return_value=io),self.assertRaises(ContractError):
            writer.submit_authority_write(self.client,plan,identity=self.identity,access_reader=self.reader,
                settings=writer.AuthorityWriterSettings(True))
