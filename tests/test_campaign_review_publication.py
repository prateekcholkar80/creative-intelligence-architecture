"""P6 review storage and SQL construction, not native execution or approval."""
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import json
import unittest
from src.application.campaign_review_storage import (
    SCHEMAS,ReviewVersion,PublicationControl,encode_review,review_head,
    decode_review,decode_review_state,decode_control_state,row_json,parse_row,table_metadata,
)
from src.application.campaign_permission_publication import Rows,build_publication,validate_result
from src.application.campaign_permission_lifecycle import Action
from src.application.campaign_permission_storage import encode_record,head_row
from src.application.workflow_contract import ContractError
from src.ingestion.configure_campaign_acceptance import PROJECT
from tests.test_campaign_permission_lifecycle import identity,grant,draft,capture,current,NOW
from tests.test_campaign_permission_contract import claim,evidence


def bundle(action=Action.CREATE_DRAFT,kind='CLAIM'):
    c=draft(kind);g=ReviewVersion(grant(),1,None,'ACTIVE','fixture-operator',NOW)
    r=ReviewVersion(capture(c,kind),1,None,'ACTIVE',identity().principal.principal_id,NOW)
    ctrl=PublicationControl(1,None,'a'*64,'fixture-operator',NOW)
    grows=encode_review(g);rrows=encode_review(r)
    permission=Rows(kind,getattr(c,{'CLAIM':'claim_id','OFFER':'offer_id','EVIDENCE':'evidence_id'}[kind]),None,())
    if action is not Action.CREATE_DRAFT:
        original=claim() if action is Action.REVOKE else c
        h=encode_record(original,recorded_at=NOW)
        permission=Rows(kind,h['record_id'],row_json(head_row(h)),(row_json(h),))
    source=encode_record(evidence(),recorded_at=NOW)
    ev=Rows('EVIDENCE',source['record_id'],row_json(head_row(source)),(row_json(source),))
    return dict(identity=identity(),control=Rows('CONTROL','campaign-permissions',row_json(ctrl.row()),(row_json(ctrl.row()),)),
        grant=Rows('GRANT',g.record_id,row_json(review_head(grows)),(row_json(grows),)),
        permission=permission,candidate=None if action is Action.REVOKE else c,
        review=Rows('REVIEW',r.record_id,row_json(review_head(rrows)),(row_json(rrows),)) if action is Action.APPROVE else None,
        evidence=(ev,) if action is Action.APPROVE and kind=='CLAIM' else (),checked_at=NOW,reason_code='FIXTURE_REVIEW')


class ReviewStorageTests(unittest.TestCase):
    def test_seven_proposed_schemas_and_review_roundtrips(self):
        self.assertEqual(len(SCHEMAS),7)
        for name in SCHEMAS:
            t=table_metadata(name,PROJECT)
            self.assertEqual(t.dataset_id,'ci_brand_memory');self.assertIsNone(t.expires)
        for kind,v in [('GRANT',grant()),('REVIEW',capture(draft()))]:
            record=ReviewVersion(v,1,None,'ACTIVE','fixture-actor',NOW);row=encode_review(record)
            self.assertEqual(decode_review(row,kind),record)
            self.assertEqual(parse_row(row_json(row)),row)
            self.assertEqual(decode_review_state(review_head(row),(row,),kind=kind,record_id=record.record_id,checked_at=NOW),record)

    def test_review_corruption_duplicate_gaps_and_expiry_denied(self):
        record=ReviewVersion(grant(),1,None,'ACTIVE','fixture-actor',NOW);row=encode_review(record)
        for values in ({'revision':True},{'payload_sha256':'0'*64},{'record_sha256':'0'*64},
                       {'payload_canonical_json':row['payload_canonical_json']+' '},{'extra':'field'}):
            with self.assertRaises(ContractError):decode_review(row|values,'GRANT')
        for history in ((),(row,row)):
            with self.assertRaises(ContractError):decode_review_state(review_head(row),history,kind='GRANT',record_id=record.record_id,checked_at=NOW)
        with self.assertRaises(ContractError):decode_review_state(review_head(row),(row,),kind='GRANT',record_id=record.record_id,checked_at=grant().valid_until)

    def test_revocation_and_immutable_review_capture(self):
        first=ReviewVersion(capture(draft()),1,None,'ACTIVE','fixture-actor',NOW)
        revoked=replace(first,revision=2,parent_sha256=first.sha256,status='REVOKED')
        rows=(encode_review(first),encode_review(revoked))
        with self.assertRaises(ContractError):decode_review_state(review_head(rows[-1]),rows,kind='REVIEW',record_id=first.record_id,checked_at=NOW)
        changed=replace(first,revision=2,parent_sha256=first.sha256,value=replace(first.value,review_evidence_sha256='0'*64))
        rows=(encode_review(first),encode_review(changed))
        with self.assertRaises(ContractError):decode_review_state(review_head(rows[-1]),rows,kind='REVIEW',record_id=first.record_id,checked_at=NOW)

    def test_control_requires_complete_exact_chain_no_bootstrap(self):
        first=PublicationControl(1,None,'a'*64,'fixture-actor',NOW)
        second=PublicationControl(2,first.sha256,'b'*64,'fixture-actor',NOW)
        self.assertEqual(decode_control_state(second.row(),(first.row(),second.row()),NOW),second)
        for history in ((),(second.row(),),(first.row(),first.row())):
            with self.assertRaises(ContractError):decode_control_state(second.row(),history,NOW)
        with self.assertRaises(ContractError):decode_control_state(first.row()|dict(revision=True),(first.row(),),NOW)


class PublicationQueryTests(unittest.TestCase):
    def test_all_permission_kinds_create_and_approve_have_disabled_deterministic_plans(self):
        for kind in ('CLAIM','OFFER','EVIDENCE'):
            for action in (Action.CREATE_DRAFT,Action.APPROVE):
                args=bundle(action,kind);a=build_publication(action,**args);b=build_publication(action,**args)
                self.assertEqual(a,b);self.assertFalse(a.execution_enabled)
                self.assertTrue(a.native_job_id.startswith('campaign_permission_publish_'))
                self.assertFalse(a.config().use_query_cache)
                self.assertEqual(a.config().job_timeout_ms,'120000')
                self.assertEqual(a.config().maximum_bytes_billed,768*1024*1024)
                self.assertEqual(a.config().to_api_repr(),json.loads(a.config_json))

    def test_atomic_fence_history_audit_exact_replay_and_rollback_sql(self):
        plan=build_publication(Action.APPROVE,**bundle(Action.APPROVE))
        for required in ('BEGIN TRANSACTION','COMMIT TRANSACTION','ROLLBACK TRANSACTION','RAISE;',
                         'PUBLICATION_CAS_CONFLICT','PUBLICATION_GRANT_HISTORY',
                         'PUBLICATION_REVIEW_HISTORY','PUBLICATION_EVIDENCE_HISTORY',
                         'PUBLICATION_REPLAY_AUDIT','PUBLICATION_REPLAY_HEAD',
                         'PUBLICATION_REPLAY_CONTROL_HISTORY',"'REUSED'","'APPLIED'",
                         'PUBLICATION_IDENTITY_EXPIRED','PUBLICATION_CANDIDATE_EXPIRED'):
            self.assertIn(required,plan.sql)
        self.assertIn('UPDATE `'+PROJECT+'.ci_brand_memory.campaign_permission_controls`',plan.sql)
        self.assertIn('INSERT `'+PROJECT+'.ci_brand_memory.campaign_permission_publication_audits`',plan.sql)
        self.assertNotIn('DELETE',plan.sql);self.assertNotIn('TRUNCATE',plan.sql)
        self.assertNotIn(identity().subject,plan.sql)
        self.assertLess(plan.sql.index('PUBLICATION_GRANT_HISTORY'),plan.sql.index('IF (SELECT COUNT(*)'))

    def test_all_snapshot_comparisons_require_both_directions_and_counts(self):
        import re
        plan=build_publication(Action.APPROVE,**bundle(Action.APPROVE))
        comparisons=re.findall(r"ASSERT (.*?) AS '(PUBLICATION_[A-Z_]+)_EXACT';",plan.sql,re.S)
        self.assertGreaterEqual(len(comparisons),12)
        for expression,label in comparisons:
            self.assertIn('COUNT(*)',expression)
            self.assertEqual(expression.count('EXCEPT DISTINCT'),2,label)
        self.assertLessEqual(plan.sql.count(';'),64)

    def test_replay_and_fresh_write_have_distinct_current_control_and_permission_pins(self):
        plan=build_publication(Action.CREATE_DRAFT,**bundle())
        replay=plan.sql.split('IF (SELECT COUNT(*)')[1].split('\nELSE\n')[0]
        fresh=plan.sql.split('\nELSE\n')[1]
        self.assertIn('FROM new_control',replay)
        self.assertIn('FROM new_head',replay)
        self.assertIn('FROM old_control',fresh)
        self.assertIn('FROM old_head',fresh)
        self.assertNotIn('UPDATE `',replay)

    def test_draft_replacement_and_revocation_build_scoped_update_not_restore(self):
        for action in (Action.REPLACE_DRAFT,Action.REVOKE):
            plan=build_publication(action,**bundle(action))
            self.assertIn('t.record_sha256=(SELECT record_sha256 FROM old_head)',plan.sql)
            audit=parse_row(plan.audit_json)
            self.assertEqual(audit['action'],action.value)
            self.assertEqual(audit['previous_revision'],1)
            self.assertEqual(audit['outcome'],'APPLIED')

    def test_missing_control_review_or_evidence_never_builds_plan(self):
        args=bundle(Action.APPROVE)
        for changes in ({'control':Rows('CONTROL','campaign-permissions',None,())},
                        {'review':None},{'evidence':()},
                        {'permission':Rows('CLAIM',draft().claim_id,None,())}):
            with self.assertRaises(ContractError):build_publication(Action.APPROVE,**(args|changes))

    def test_corrupt_authority_history_and_changed_review_content_denied(self):
        args=bundle(Action.APPROVE)
        corrupt=replace(args['grant'],history_json=(args['grant'].history_json[0].replace('ACTIVE','REVOKED'),))
        with self.assertRaises(ContractError):build_publication(Action.APPROVE,**(args|dict(grant=corrupt)))
        changed=replace(args['candidate'],wording=('Changed after review.',))
        with self.assertRaises(ContractError):build_publication(Action.APPROVE,**(args|dict(candidate=changed)))
        head=parse_row(args['evidence'][0].head_json);head['record_sha256']='0'*64
        changed=replace(args['evidence'][0],head_json=row_json(head))
        with self.assertRaises(ContractError):build_publication(Action.APPROVE,**(args|dict(evidence=(changed,))))

    def test_exact_result_validation_rejects_invented_partial_or_changed_receipt(self):
        plan=build_publication(Action.CREATE_DRAFT,**bundle())
        row=dict(state='APPLIED',command_sha256=plan.command_sha256,record_sha256=plan.proposed_record_sha256,
                 control_sha256=plan.candidate_control_sha256)
        self.assertEqual(validate_result(plan,(row,)),'APPLIED')
        self.assertEqual(validate_result(plan,(row|dict(state='REUSED'),)),'REUSED')
        for values in ((),(row,row),(row|dict(state='UNKNOWN'),),(row|dict(record_sha256='0'*64),),(row|dict(extra=True),)):
            with self.assertRaises(ContractError):validate_result(plan,values)

    def test_identity_window_and_capture_changes_pin_distinct_native_job(self):
        args=bundle();a=build_publication(Action.CREATE_DRAFT,**args)
        other=build_publication(Action.CREATE_DRAFT,**(args|dict(identity=replace(identity(),expires_at=identity().expires_at+1))))
        self.assertNotEqual(a.native_job_id,other.native_job_id)
        other=build_publication(Action.CREATE_DRAFT,**(args|dict(reason_code='OTHER')))
        self.assertNotEqual(a.native_job_id,other.native_job_id)

    def test_unbounded_or_partial_capture_denied(self):
        with self.assertRaises(ContractError):Rows('CLAIM','id',None,('x',))
        with self.assertRaises(ContractError):Rows('CLAIM','id','x'*65537,('x',))
        with self.assertRaises(ContractError):Rows('CLAIM','id','x',tuple('x' for _ in range(65)))

    def test_larger_evidence_bundles_fail_before_exceeding_native_statement_cap(self):
        args=bundle(Action.APPROVE)
        pins=tuple(replace(evidence(),evidence_id=f'fixture-study-{i}') for i in range(6))
        candidate=replace(args['candidate'],evidence=pins)
        review=ReviewVersion(capture(candidate),1,None,'ACTIVE',identity().principal.principal_id,NOW)
        row=encode_review(review)
        captures=[]
        for pin in pins:
            wire=encode_record(pin,recorded_at=NOW)
            captures.append(Rows('EVIDENCE',pin.evidence_id,row_json(head_row(wire)),(row_json(wire),)))
        with self.assertRaises(ContractError) as caught:
            build_publication(Action.APPROVE,**(args|dict(candidate=candidate,evidence=tuple(captures),
                review=Rows('REVIEW',review.record_id,row_json(review_head(row)),(row_json(row),)))))
        self.assertEqual(caught.exception.code,'PERMISSION_PUBLICATION_SCRIPT_LIMIT')


if __name__=='__main__':unittest.main()
