"""Read-only P6 draft decoding and native-correlation guards."""
from copy import deepcopy
from dataclasses import FrozenInstanceError
import json
import unittest
from unittest.mock import Mock, patch
from src.application.campaign_policy_reader import (
    decode_snapshot, read_policy, query_spec, parse_payload, MAX_JSON_BYTES,
    PROJECT, LOCATION, POLICY_ID, VERSION,
)
from src.ingestion.configure_campaign_acceptance import manifest, digest, SOURCES
from src.application.workflow_contract import ContractError, canonical_json_bytes
from tests.test_campaign_acceptance_contract import NOW


def rows():
    return tuple(dict(r,record_kind='RULE' if 'rule_id' in r else 'HEADER',
                      rule_id=r.get('rule_id'),recorded_at=NOW) for r in manifest())


def decode(values=None, **changes):
    baseline=manifest()[0]
    pins=dict(policy_id=POLICY_ID,version=VERSION,specification_sha256=baseline['specification_sha256'],
              header_sha256=baseline['payload_sha256'],native_job_id='fixture-native-read')
    return decode_snapshot(rows() if values is None else values, **(pins|changes))


def change_payload(row, **changes):
    result=dict(row); value=json.loads(row['payload_canonical_json']); value.update(changes)
    result['payload_canonical_json']=canonical_json_bytes(value).decode()
    result['payload_sha256']=digest(result['payload_canonical_json'].encode())
    return result


class CampaignPolicyReaderTests(unittest.TestCase):
    def v02(self):
        records=manifest(SOURCES['draft-v0.2'],version='draft-v0.2')
        values=tuple(dict(r,record_kind='RULE' if 'rule_id' in r else 'HEADER',
            rule_id=r.get('rule_id'),recorded_at=NOW) for r in records)
        pins=dict(policy_id=POLICY_ID,version='draft-v0.2',specification_sha256=records[0]['specification_sha256'],
            header_sha256=records[0]['payload_sha256'],native_job_id='fixture-v02-read')
        return values,pins

    def test_new_draft_version_decodes_explicit_restrictions_without_serving_readiness(self):
        values,pins=self.v02();snapshot=decode_snapshot(values,**pins)
        self.assertEqual(snapshot.version,'draft-v0.2');self.assertEqual(len(snapshot.rule_canonical_json),47)
        self.assertEqual(json.loads(snapshot.header_canonical_json)['brand_directives']['ai_people'],'PROHIBITED')
        with self.assertRaises(ContractError):snapshot.require_serving_ready()

    def test_new_version_cannot_forge_overlay_clearance_or_loosen_visual_restrictions(self):
        values,pins=self.v02()
        for changes in ({'brand_directives':dict(ai_people='ALLOW')},{'benchmark_countries':['US','FR']},
            {'country_scope_status':'APPROVED'},{'predecessor_version':'other'},
            {'deferred_benchmark_regions':[]},{'serving_enabled':True}):
            head=change_payload(values[0],**changes)
            with self.assertRaises(ContractError):decode_snapshot((head,)+values[1:],**(pins|{'header_sha256':head['payload_sha256']}))
        # New headers cannot masquerade as the old format, even with new pins.
        with self.assertRaises(ContractError):decode_snapshot(values,**(pins|{'version':VERSION}))

    def test_complete_snapshot_stable_order_independent_and_immutable(self):
        snapshot=decode()
        self.assertEqual(snapshot,decode(rows()[::-1]))
        self.assertEqual(len(snapshot.rule_canonical_json),47)
        with self.assertRaises(FrozenInstanceError): snapshot.version='other'
        with self.assertRaises(ContractError):snapshot.require_serving_ready()

    def test_partial_duplicate_extra_or_missing_header_denied(self):
        r=rows()
        for candidate in (r[:-1],r+r[-1:],r[:-1]+r[:1],r[1:]+r[-1:]):
            with self.assertRaises(ContractError):decode(candidate)

    def test_row_scope_status_schema_and_timestamp_denied(self):
        r=rows()
        for changes in ({'policy_id':'other'},{'version':'other'},{'status':'APPROVED'},
                        {'specification_sha256':'0'*64},{'recorded_at':NOW.replace(tzinfo=None)},
                        {'extra':True}):
            with self.assertRaises(ContractError):decode((dict(r[0],**changes),)+r[1:])

    def test_payload_hash_and_canonical_json_denied(self):
        r=rows()
        for value in ('{}',r[0]['payload_canonical_json']+' ',
                      '{"status":"DRAFT","status":"APPROVED"}', '[1]', 'null',
                      '"'+ 'x'*MAX_JSON_BYTES+'"'):
            with self.assertRaises(ContractError):parse_payload(value) if value!='{}' else decode((dict(r[0],payload_canonical_json=value),)+r[1:])

    def test_rehashed_rule_tampering_still_denied_by_ruleset_pin(self):
        r=rows()
        changed=change_payload(r[1],acceptance_rule='Fake efficacy guarantee')
        with self.assertRaises(ContractError):decode((r[0],changed)+r[2:])

    def test_header_draft_only_even_when_caller_pins_forged_hash(self):
        r=rows()
        for values in ({'serving_enabled':True},{'status':'APPROVED'},
                       {'approved_by':'manager'},{'rule_count':True},
                       {'country_scope_status':'ALL_COUNTRIES_APPROVED'}):
            head=change_payload(r[0],**values)
            with self.assertRaises(ContractError):
                decode((head,)+r[1:],header_sha256=head['payload_sha256'])

    def test_rule_schema_identity_area_and_state_denied(self):
        r=rows()
        for values in ({'rule_id':'A99'},{'area':'COMPLIANCE'},
                       {'checker_status':'IMPLEMENTED'},{'acceptance_rule':''},
                       {'applicability_status':'APPROVED'},{'unexpected':'field'}):
            with self.assertRaises(ContractError):decode((r[0],change_payload(r[1],**values))+r[2:])

    def test_parameterized_single_snapshot_query_and_bound_configuration(self):
        sql,cfg=query_spec('safe-id','safe-version')
        self.assertNotIn('safe-id',sql)
        self.assertIn('UNION ALL',sql); self.assertIn('LIMIT 50',sql)
        self.assertNotIn('INSERT',sql); self.assertNotIn('UPDATE',sql)
        self.assertFalse(cfg.use_query_cache)
        self.assertEqual(cfg.maximum_bytes_billed,256*1024*1024)
        self.assertEqual(cfg.job_timeout_ms,'60000')

    def client(self, *, alteration=None, values=None):
        client=Mock(project=PROJECT)
        def query(sql, **kwargs):
            cfg=kwargs['job_config']
            native=Mock(project=PROJECT,location=LOCATION,job_id=kwargs['job_id'],query=sql,
                        state='DONE',error_result=None)
            config=deepcopy(cfg.to_api_repr()); config['query']['query']=sql
            native.to_api_repr.return_value={'configuration':config}
            if alteration: alteration(native,config)
            client.get_job.return_value=native
            return Mock(result=Mock(return_value=rows() if values is None else values))
        client.query.side_effect=query
        return client

    def read(self, client, **changes):
        header=manifest()[0]
        values=dict(policy_id=POLICY_ID,version=VERSION,specification_sha256=header['specification_sha256'],
                    header_sha256=header['payload_sha256'])
        with patch('src.application.campaign_policy_reader.preflight',return_value=[]):
            return read_policy(client,**(values|changes))

    def test_fresh_reads_no_cache_fallback_or_writes(self):
        client=self.client()
        first=self.read(client);second=self.read(client)
        self.assertNotEqual(first.native_job_id,second.native_job_id)
        self.assertEqual(first.snapshot_sha256,second.snapshot_sha256)
        client.create_table.assert_not_called()
        for call in client.query.call_args_list:
            self.assertIsNone(call.kwargs['retry']);self.assertIsNone(call.kwargs['job_retry'])

    def test_mismatched_native_job_never_returns_snapshot(self):
        alterations=[lambda j,c:setattr(j,'project','other'),
                     lambda j,c:setattr(j,'job_id','other'),
                     lambda j,c:setattr(j,'query','SELECT 1'),
                     lambda j,c:setattr(j,'location','US'),
                     lambda j,c:setattr(j,'state','RUNNING'),
                     lambda j,c:setattr(j,'error_result',{'reason':'failure'}),
                     lambda j,c:c.update(jobTimeoutMs='1'),
                     lambda j,c:c['query'].update(useQueryCache=True)]
        for mutate in alterations:
            with self.assertRaises(ContractError):self.read(self.client(alteration=mutate))

    def test_invalid_pin_and_missing_schema_deny_before_query(self):
        client=self.client()
        with self.assertRaises(ContractError):self.read(client,header_sha256='bad')
        client.query.assert_not_called()
        head=manifest()[0]
        with patch('src.application.campaign_policy_reader.preflight',return_value=['missing']):
            with self.assertRaises(ContractError):read_policy(client,policy_id=POLICY_ID,version=VERSION,
                specification_sha256=head['specification_sha256'],header_sha256=head['payload_sha256'])
        client.query.assert_not_called()

    def test_provider_failure_propagates_without_resubmission(self):
        client=self.client();client.query.side_effect=TimeoutError('fixture')
        with self.assertRaises(TimeoutError):self.read(client)
        self.assertEqual(client.query.call_count,1)
        client.create_table.assert_not_called()

    def test_sql_limit_and_decoder_reject_extra_scoped_rows(self):
        with self.assertRaises(ContractError):self.read(self.client(values=rows()+rows()[:2]))


if __name__=='__main__':unittest.main()
