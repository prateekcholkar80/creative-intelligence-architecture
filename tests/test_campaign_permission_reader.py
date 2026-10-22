"""Permission snapshot reader guards using supplied synthetic rows."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock,patch
import unittest
from src.application.campaign_permission_reader import (
    Checkpoint,PermissionRequest,query_spec,decode_snapshot,read_permission,MAX_ROWS,
)
from src.application.campaign_permission_storage import encode_record,head_row
from src.application.campaign_acceptance_contract import PolicyStatus
from src.application.workflow_contract import ContractError
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION
from tests.test_campaign_permission_contract import claim,offer,evidence,NOW


def records(record,scope,history=None):
    history=history or (encode_record(record,recorded_at=NOW),)
    head=head_row(history[-1])|dict(payload_canonical_json=None)
    return (head|dict(scope_role=scope,row_role='HEAD',scoped_row_count=1),)+tuple(
        row|dict(scope_role=scope,row_role='HISTORY',scoped_row_count=len(history)) for row in history)


def request(kind='CLAIM',**changes):
    key={'CLAIM':claim().claim_id,'OFFER':offer().offer_id,'EVIDENCE':evidence().evidence_id}[kind]
    return PermissionRequest(kind,Checkpoint(key),**changes)


def supplied(kind='CLAIM'):
    record={'CLAIM':claim(),'OFFER':offer(),'EVIDENCE':evidence()}[kind]
    rows=records(record,'TARGET')
    return rows+records(evidence(),'EVIDENCE') if kind=='CLAIM' else rows


def decode(rows=None,req=None,now=NOW):
    req=req or request()
    return decode_snapshot(supplied(req.kind) if rows is None else rows,req,
                           native_job_id='fixture-read',checked_at=now)


class PermissionReaderTests(unittest.TestCase):
    def test_all_kinds_exact_current_snapshot_and_independent_claim_evidence(self):
        for kind in ('CLAIM','OFFER','EVIDENCE'):
            result=decode(req=request(kind))
            self.assertEqual(result.state.kind,kind)
            self.assertEqual(len(result.evidence_states),1 if kind=='CLAIM' else 0)

    def test_missing_partial_duplicate_head_and_truncated_history_denied(self):
        rows=supplied()
        for values in ((),rows[:2],rows[1:],rows+rows[:1],rows[:-1]):
            with self.assertRaises(ContractError):decode(values)
        with self.assertRaises(ContractError):decode((rows[0]|dict(scoped_row_count=2),)+rows[1:])
        with self.assertRaises(ContractError):decode((rows[0],rows[1]|dict(scoped_row_count=2))+rows[2:])

    def test_row_scope_extra_fields_roles_count_and_digest_conflict(self):
        rows=supplied()
        for values in ({'scope_role':'OTHER'},{'row_role':'OTHER'},{'scoped_row_count':True},
                       {'record_id':'other'},{'record_sha256':'0'*64},{'extra':True}):
            with self.assertRaises(ContractError):decode((rows[0]|values,)+rows[1:])

    def test_revoked_current_evidence_denies_unchanged_claim(self):
        e=evidence();later=replace(e,review=replace(e.review,revision=2,
            parent_sha256=e.sha256,status=PolicyStatus.REVOKED))
        history=(encode_record(e,recorded_at=NOW),encode_record(later,recorded_at=NOW))
        with self.assertRaises(ContractError):decode(records(claim(),'TARGET')+records(later,'EVIDENCE',history))

    def test_changed_current_evidence_and_unexpected_evidence_scope_denied(self):
        changed=replace(evidence(),content_sha256='b'*64)
        with self.assertRaises(ContractError):decode(records(claim(),'TARGET')+records(changed,'EVIDENCE'))
        extra=replace(evidence(),evidence_id='extra')
        with self.assertRaises(ContractError):decode(supplied()+records(extra,'EVIDENCE'))

    def test_request_and_evidence_checkpoints_deny_stale_or_conflicting_state(self):
        for req in (PermissionRequest('CLAIM',Checkpoint(claim().claim_id,2)),
                    PermissionRequest('CLAIM',Checkpoint(claim().claim_id,1,'0'*64)),
                    request(evidence_checkpoints=(Checkpoint(evidence().evidence_id,2),)),
                    request(evidence_checkpoints=(Checkpoint('other'),))):
            with self.assertRaises(ContractError):decode(req=req)
        req=request(evidence_checkpoints=(Checkpoint(evidence().evidence_id,1,evidence().sha256),))
        self.assertEqual(decode(req=req).state.current,claim())

    def test_expired_draft_future_records_and_overflow_deny(self):
        with self.assertRaises(ContractError):decode(now=claim().approval.valid_until)
        c=replace(claim(),approval=replace(claim().approval,status=PolicyStatus.DRAFT,
                                           approved_by=None,approved_at=None))
        with self.assertRaises(ContractError):decode(records(c,'TARGET')+records(evidence(),'EVIDENCE'))
        with self.assertRaises(ContractError):decode(now=NOW-timedelta(seconds=1))
        with self.assertRaises(ContractError):decode(supplied()[:1]*MAX_ROWS)

    def test_request_parameters_bounded_no_scope_injection_or_invalid_kind(self):
        for build in (lambda:PermissionRequest('OTHER',Checkpoint('id')),
                      lambda:Checkpoint('id',True),lambda:Checkpoint('id',65),
                      lambda:Checkpoint('id',1,'bad'),
                      lambda:PermissionRequest('OFFER',Checkpoint('id'),(Checkpoint('e'),)),
                      lambda:request(evidence_checkpoints=(Checkpoint('e'),Checkpoint('e')))):
            with self.assertRaises(ContractError):build()
        identifier="id'OR1=1"
        sql,cfg=query_spec(PermissionRequest('CLAIM',Checkpoint(identifier)))
        self.assertNotIn(identifier,sql)
        self.assertEqual(cfg.query_parameters[0].value,identifier)
        self.assertIn('COUNT(*) OVER',sql);self.assertIn('JSON_QUERY_ARRAY',sql)
        self.assertIn('BYTE_LENGTH',sql)
        self.assertFalse(cfg.use_query_cache);self.assertEqual(cfg.job_timeout_ms,'60000')
        self.assertEqual(len(cfg.query_parameters),1)
        for kind in ('OFFER','EVIDENCE'):
            alternate,_=query_spec(request(kind))
            self.assertIn('FROM UNNEST(ARRAY<STRING>[])',alternate)
            self.assertNotIn('SELECT CAST(NULL AS STRING) record_id WHERE FALSE',alternate)

    def client(self,mutate=None,values=None):
        c=Mock(project=PROJECT)
        def query(sql,**kwargs):
            native=Mock(project=PROJECT,location=LOCATION,job_id=kwargs['job_id'],query=sql,
                         state='DONE',error_result=None)
            config=deepcopy(kwargs['job_config'].to_api_repr());config['query']['query']=sql
            native.to_api_repr.return_value={'configuration':config}
            if mutate:mutate(native,config)
            c.get_job.return_value=native
            return Mock(result=Mock(return_value=supplied() if values is None else values))
        c.query.side_effect=query
        return c

    def read(self,c,req=None):
        with patch('src.application.campaign_permission_reader.inspect',return_value={'status':'VERIFIED'}):
            return read_permission(c,req or request(),clock=lambda:NOW)

    def test_each_read_is_fresh_single_snapshot_no_writes(self):
        c=self.client();a=self.read(c);b=self.read(c)
        self.assertNotEqual(a.native_job_id,b.native_job_id)
        self.assertEqual(c.query.call_count,2)
        self.assertIsNone(c.query.call_args.kwargs['job_retry'])
        c.create_table.assert_not_called()

    def test_submitted_config_mutation_cannot_redefine_the_native_expectation(self):
        c=self.client();query=c.query.side_effect
        def mutate(sql,**kw):
            kw['job_config'].maximum_bytes_billed=1
            return query(sql,**kw)
        c.query.side_effect=mutate
        with self.assertRaisesRegex(ContractError,'PERMISSION_READ_NATIVE_JOB_MISMATCH'):self.read(c)

    def test_result_rpc_and_job_retry_are_disabled(self):
        c=self.client();query=c.query.side_effect;returned=[]
        def track(sql,**kw):
            job=query(sql,**kw);returned.append(job);return job
        c.query.side_effect=track;self.read(c)
        kw=returned[0].result.call_args.kwargs
        self.assertIsNone(kw['retry']);self.assertIsNone(kw['job_retry'])

    def test_native_job_identity_configuration_or_outcome_mismatch_denies(self):
        for mutate in (lambda j,c:setattr(j,'project','other'),lambda j,c:setattr(j,'location','US'),
                       lambda j,c:setattr(j,'job_id','other'),lambda j,c:setattr(j,'query','SELECT 1'),
                       lambda j,c:setattr(j,'state','RUNNING'),lambda j,c:setattr(j,'error_result',{'reason':'failure'}),
                       lambda j,c:c['query'].update(useQueryCache=True),lambda j,c:c.update(jobTimeoutMs='1')):
            with self.assertRaises(ContractError):self.read(self.client(mutate))

    def test_missing_schema_wrong_project_and_bad_request_deny_before_query(self):
        c=self.client();c.project='other'
        with self.assertRaises(ContractError):self.read(c)
        c.query.assert_not_called()
        c=self.client()
        with patch('src.application.campaign_permission_reader.inspect',return_value={'status':'MISSING_RESOURCES'}):
            with self.assertRaises(ContractError):read_permission(c,request())
        c.query.assert_not_called()
        with self.assertRaises(ContractError):read_permission(c,None)

    def test_provider_errors_and_absence_no_resubmission(self):
        c=self.client();c.query.side_effect=TimeoutError('fixture')
        with self.assertRaises(TimeoutError):self.read(c)
        self.assertEqual(c.query.call_count,1)
        with self.assertRaises(ContractError):self.read(self.client(values=()))


if __name__=='__main__':unittest.main()
