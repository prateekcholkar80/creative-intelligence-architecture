"""Strict explicit schema decoding, exact native NUMERIC, and audit handoff."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime,timezone
from decimal import Decimal
import json
import unittest

from src.application.access_capture import capture_access,capture_from_wire
from src.application.access_policy import authorize,Action
from src.application.audit_contract import BootstrapObservation, AuditTarget, ResourceType, MutationObservation,MutationKind,MutationOutcome,validate_audit_event
from src.application.workflow_codec import encode_row,decode_row,sql_row
from src.application.workflow_contract import ContractError
from src.application.management_service import audit_row,digest
from tests.test_management_service import workspace,ACTOR,WS,NOW
from tests.test_invocation_contract import invocation,estimated


class CodecTests(unittest.TestCase):
    def test_native_numeric_literal_is_exact_decimal(self):
        inv,_=invocation('SUCCEEDED');inv=estimated(inv,Decimal('9007199254740993.123456789'))
        raw=encode_row(inv,'model_invocations').replace('"9007199254740993.123456789"','9007199254740993.123456789')
        self.assertEqual(decode_row(raw,'model_invocations')['estimated_cost'],inv['estimated_cost'])
    def test_repeated_profiles_and_json_values_roundtrip(self):
        from tests.test_workflow_contract import request
        row=request();row['model_profiles'][0]['temperature']=0.5
        self.assertEqual(decode_row(encode_row(row,'requests'),'requests'),row)
    def test_missing_extra_duplicate_nonfinite_and_oversized_json_fail_closed(self):
        good=encode_row(workspace(),'workspaces')
        for raw in ('{}',good.replace('"revision":1','"revision":1,"revision":2'),
                    good.replace('"revision":1','"revision":NaN'),good[:-1]+',"secret":"no"}',good+' '*524288):
            with self.subTest(raw=raw[:60]),self.assertRaises(ContractError):decode_row(raw,'workspaces')
    def test_numeric_strings_do_not_reinterpret_string_business_fields(self):
        row=workspace();row['workspace_id']='123'
        raw=encode_row(row,'workspaces').replace('"revision":1','"revision":"1"')
        decoded=decode_row(raw,'workspaces');self.assertEqual(decoded['revision'],1);self.assertEqual(decoded['workspace_id'],'123')
    def test_sql_uses_fixed_parameter_and_every_destination_type(self):
        sql=sql_row('candidate','model_invocations')
        self.assertIn('AS NUMERIC',sql);self.assertIn('AS TIMESTAMP',sql);self.assertIn('AS profile',sql)
        with self.assertRaises(ValueError):sql_row('caller_sql','model_invocations')
    def test_schema_required_values_cannot_be_null(self):
        raw=encode_row(workspace(),'workspaces').replace('"revision":1','"revision":null')
        with self.assertRaises(ContractError):decode_row(raw,'workspaces')


class CaptureTests(unittest.TestCase):
    def capture(self):
        return capture_access(authorize(None,None,Action.READ_DRAFT,workspace_id=WS),target=None,trace_id='fixture-trace')
    def test_anonymous_denial_wire_is_bound_to_validated_audit(self):
        value=self.capture();self.assertEqual(capture_from_wire(value.wire()),value)
        row=decode_row(value.audit_json,'audit_events');self.assertEqual(row['actor_kind'],'ANONYMOUS');self.assertIsNone(row['resource_id'])
    def test_changed_observation_or_audit_cannot_cross_wire(self):
        for mode in ('decision','audit','target'):
            value=self.capture().wire()
            if mode=='decision':value['decision']['allowed']=True
            elif mode=='audit':value['audit_json']=value['audit_json'].replace('AUTHENTICATION_REQUIRED','ALLOWED_OWNER')
            else:value['target']={}
            with self.subTest(mode=mode),self.assertRaises((ContractError,ValueError,TypeError)):capture_from_wire(value)
    def test_bootstrap_audit_binds_exact_manifest_hash(self):
        pin=digest(['explicit-manifest'])
        observation=BootstrapObservation(ACTOR,WS,pin);target=AuditTarget(ResourceType.WORKSPACE,WS,WS)
        mutation=MutationObservation(MutationKind.CREATE,MutationOutcome.APPLIED,1)
        row=audit_row(observation,target,mutation,pin,NOW)
        with self.assertRaises(ContractError):validate_audit_event(row,observation=replace(observation,manifest_sha256='b'*64),
            target=target,mutation=mutation,observed_at=NOW,recorded_at=NOW,trace_id=pin)


if __name__=='__main__':unittest.main()
