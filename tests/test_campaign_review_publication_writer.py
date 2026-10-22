"""Pure reviewer-record mutation and fenced SQL contracts, no live approvals."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
import json
from src.application.campaign_review_publication_contract import ReviewOperatorPolicy,ReviewAction as Action,propose_review_publication,structural_records
from src.application.campaign_review_publication_query import build_review_publication
from src.application.campaign_permission_publication import Rows,validate_result
from src.application.campaign_review_storage import ReviewVersion,PublicationControl,encode_review,review_head,row_json,parse_row
from src.application.campaign_permission_storage import encode_record,head_row
from src.application.workflow_contract import ContractError
from tests.test_campaign_permission_lifecycle import identity,grant,draft,capture,NOW,AUDIENCE

OPERATOR=identity('synthetic-operator')


def policy():
    return ReviewOperatorPolicy('campaign-review-operators',1,AUDIENCE,(OPERATOR.principal.principal_id,),
        (identity().principal.principal_id,),(OPERATOR.principal.principal_id,),('CLAIM','EVIDENCE','OFFER'),
        7200,600,NOW-timedelta(hours=1),NOW+timedelta(hours=2))


def policy_snapshot(value=None,checked_at=NOW):
    from src.application.campaign_operator_policy_storage import OperatorPolicyVersion,encode_record,head_row,decode_state
    from src.application.campaign_operator_policy_reader import OperatorPolicySnapshot
    value=value or policy();records=[];previous=None
    for revision in range(1,value.revision+1):
        record=OperatorPolicyVersion(replace(value,revision=revision),previous,'ACTIVE',OPERATOR.principal.principal_id,
            'EXPLICIT_FIXTURE_POLICY',NOW)
        records.append(encode_record(record));previous=record.sha256
    state=decode_state(head_row(records[-1]),tuple(records),checked_at=checked_at)
    return OperatorPolicySnapshot(state,row_json(head_row(records[-1])),tuple(row_json(r) for r in records),'fixture-policy-read',checked_at)


def rows(*records):
    encoded=tuple(encode_review(record) for record in records);last=records[-1]
    return Rows(last.kind,last.record_id,row_json(review_head(encoded[-1])),tuple(row_json(row) for row in encoded))


def grant_rows():return rows(ReviewVersion(grant(),1,None,'ACTIVE',OPERATOR.principal.principal_id,NOW))


def permission_rows():
    row=encode_record(draft(),recorded_at=NOW)
    return Rows('CLAIM',row['record_id'],row_json(head_row(row)),(row_json(row),))


def control_rows():
    row=PublicationControl(1,None,'a'*64,OPERATOR.principal.principal_id,NOW).row()
    return Rows('CONTROL','campaign-permissions',row_json(row),(row_json(row),))


def inputs(action=Action.CREATE_GRANT):
    target=Rows('GRANT',grant().grant_id,None,());actor=OPERATOR;value=grant();g=None;p=None
    if action in (Action.REVISE_GRANT,Action.REVOKE_GRANT):
        target=grant_rows();value=replace(grant(),version='v2') if action is Action.REVISE_GRANT else None
    if action in (Action.CREATE_REVIEW,Action.REVOKE_REVIEW):
        actor=identity();value=capture(draft());g=grant_rows()
        target=Rows('REVIEW',value.review_id,None,());p=permission_rows()
        if action is Action.REVOKE_REVIEW:
            target=rows(ReviewVersion(value,1,None,'ACTIVE',actor.principal.principal_id,NOW));value=None;p=None
    return dict(identity=actor,policy=policy(),target=target,value=value,grant=g,permission=p,checked_at=NOW,reason_code='EXPLICIT_FIXTURE_ACTION')


def propose(action,**changes):return propose_review_publication(action,**(inputs(action)|changes))


class ReviewTransitionTests(TestCase):
    def test_all_five_actions_and_immutable_review_revocation(self):
        for action in Action:
            result=propose(action)
            self.assertEqual(result.record.kind,'GRANT' if 'GRANT' in action.value else 'REVIEW')
            self.assertEqual(result.record.revision,1 if action in (Action.CREATE_GRANT,Action.CREATE_REVIEW) else 2)
            self.assertEqual(result.record.status,'REVOKED' if 'REVOKE' in action.value else 'ACTIVE')
        self.assertEqual(propose(Action.REVOKE_REVIEW).record.value,capture(draft()))

    def test_policy_has_explicit_roles_no_inferred_bootstrap_or_reviewer_operator(self):
        with self.assertRaises(ContractError):propose(Action.CREATE_GRANT,identity=identity())
        with self.assertRaises(ContractError):propose(Action.CREATE_REVIEW,identity=OPERATOR)
        with self.assertRaises(ContractError):propose(Action.CREATE_GRANT,identity=identity('bootstrap-owner'))

    def test_policy_invalid_roles_duplicate_untyped_or_missing_explicit_limits(self):
        for changes in ({'grant_operator_ids':()},{'reviewer_principal_ids':()},
            {'grant_operator_ids':('email@example.com',)},{'reviewer_principal_ids':(identity().principal.principal_id,)*2},
            {'review_revoker_ids':([], )},{'max_grant_seconds':True},{'max_review_seconds':0},
            {'kinds':('UNKNOWN',)},{'valid_until':NOW-timedelta(hours=2)}):
            with self.assertRaises((ContractError,ValueError)):replace(policy(),**changes)

    def test_policy_window_identity_audience_and_expiry_denied(self):
        for changes in ({'identity':None},{'identity':replace(OPERATOR,audience=AUDIENCE+'-other')},
            {'identity':replace(OPERATOR,expires_at=int(NOW.timestamp()))},
            {'policy':replace(policy(),valid_until=NOW)},
            {'policy':replace(policy(),effective_at=NOW+timedelta(seconds=1))}):
            with self.assertRaises(ContractError):propose(Action.CREATE_GRANT,**changes)

    def test_grant_scope_whitelist_kind_audience_and_lifetime(self):
        for candidate in (replace(grant(),principal_ids=(OPERATOR.principal.principal_id,)),
            replace(grant(),expected_iap_audience=AUDIENCE+'-other'),replace(grant(),grant_id='other'),
            replace(grant(),valid_until=NOW+timedelta(hours=2)),replace(grant(),valid_until=NOW)):
            with self.assertRaises(ContractError):propose(Action.CREATE_GRANT,value=candidate)
        with self.assertRaises(ContractError):propose(Action.CREATE_GRANT,policy=replace(policy(),kinds=('CLAIM',)))

    def test_expired_grant_can_be_renewed_or_revoked_but_not_used_for_review(self):
        expired=replace(grant(),effective_at=NOW-timedelta(hours=3),valid_until=NOW-timedelta(seconds=1))
        old=rows(ReviewVersion(expired,1,None,'ACTIVE',OPERATOR.principal.principal_id,NOW-timedelta(hours=2)))
        self.assertEqual(propose(Action.REVISE_GRANT,target=old,value=replace(grant(),version='v2')).record.revision,2)
        self.assertEqual(propose(Action.REVOKE_GRANT,target=old).record.status,'REVOKED')
        with self.assertRaises(ContractError):propose(Action.CREATE_REVIEW,grant=old,value=replace(capture(draft()),grant_sha256=expired.sha256))

    def test_grant_revision_requires_new_version_and_terminal_revocation_never_restores(self):
        with self.assertRaises(ContractError):propose(Action.REVISE_GRANT,value=grant())
        first=structural_records(grant_rows(),NOW)[0]
        revoked=propose(Action.REVOKE_GRANT).record
        for action in (Action.REVISE_GRANT,Action.REVOKE_GRANT):
            with self.assertRaises(ContractError):propose(action,target=rows(first,revoked))

    def test_historical_grant_version_cannot_be_reused_to_reactivate_old_review_pins(self):
        first=structural_records(grant_rows(),NOW)[0]
        second=replace(first,value=replace(first.value,version='v2'),revision=2,parent_sha256=first.sha256)
        history=rows(first,second)
        for candidate in (grant(),replace(grant(),valid_until=NOW+timedelta(minutes=59))):
            with self.assertRaises(ContractError):propose(Action.REVISE_GRANT,target=history,value=candidate)
        result=propose(Action.REVISE_GRANT,target=history,value=replace(grant(),version='v3'))
        self.assertEqual(result.record.revision,3)

    def test_partial_duplicate_chain_and_cross_audience_history_denied(self):
        source=grant_rows()
        with self.assertRaises(ContractError):propose(Action.REVISE_GRANT,target=replace(source,history_json=source.history_json*2))
        first=structural_records(source,NOW)[0]
        second=replace(first,value=replace(first.value,expected_iap_audience=AUDIENCE+'-other'),revision=2,parent_sha256=first.sha256)
        encoded=encode_review(second)
        changed=Rows('GRANT',first.record_id,row_json(review_head(encoded)),source.history_json+(row_json(encoded),))
        with self.assertRaises(ContractError):propose(Action.REVISE_GRANT,target=changed)

    def test_review_exact_actor_grant_content_candidate_and_timestamp(self):
        review=capture(draft())
        for value in (replace(review,principal_id=OPERATOR.principal.principal_id),replace(review,grant_sha256='b'*64),
            replace(review,content_sha256='c'*64),replace(review,record_id='other'),
            replace(review,reviewed_at=NOW-timedelta(seconds=1)),replace(review,valid_until=NOW+timedelta(minutes=11))):
            with self.assertRaises(ContractError):propose(Action.CREATE_REVIEW,value=value)
        with self.assertRaises(ContractError):propose(Action.CREATE_REVIEW,permission=None)
        with self.assertRaises(ContractError):propose(Action.CREATE_REVIEW,grant=rows(replace(structural_records(grant_rows(),NOW)[0],status='REVOKED')))

    def test_reviews_never_replace_existing_content_or_create_over_existing(self):
        current=inputs(Action.REVOKE_REVIEW)['target']
        with self.assertRaises(ContractError):propose(Action.CREATE_REVIEW,target=current)
        with self.assertRaises(ContractError):propose(Action.REVOKE_REVIEW,value=capture(draft()))
        with self.assertRaises(ContractError):propose(Action.REVOKE_REVIEW,identity=identity('outsider'))

    def test_explicit_revoker_can_revoke_review_after_grant_revocation(self):
        first=structural_records(grant_rows(),NOW)[0]
        g=rows(first,replace(first,revision=2,parent_sha256=first.sha256,status='REVOKED'))
        result=propose(Action.REVOKE_REVIEW,identity=OPERATOR,grant=g)
        self.assertEqual(result.record.status,'REVOKED')
        self.assertEqual(result.record.value,capture(draft()))

    def test_revoking_overscoped_grant_remains_possible_after_whitelist_shrinks(self):
        shrunk=replace(policy(),reviewer_principal_ids=(identity('new-reviewer').principal.principal_id,),kinds=('CLAIM',))
        self.assertEqual(propose(Action.REVOKE_GRANT,policy=shrunk).record.status,'REVOKED')
        with self.assertRaises(ContractError):propose(Action.CREATE_REVIEW,policy=shrunk)


    def test_expired_review_still_revocable_without_rewriting_its_payload(self):
        expired=replace(capture(draft()),reviewed_at=NOW-timedelta(minutes=30),valid_until=NOW-timedelta(seconds=1))
        target=rows(ReviewVersion(expired,1,None,'ACTIVE',identity().principal.principal_id,NOW-timedelta(minutes=30)))
        result=propose(Action.REVOKE_REVIEW,target=target,identity=OPERATOR)
        self.assertEqual(result.record.status,'REVOKED');self.assertEqual(result.record.value,expired)

    def test_policy_cardinality_bound_and_changed_versions_bind_distinct_hashes(self):
        original=policy()
        principals=tuple(sorted(identity('fixture-'+str(i)).principal.principal_id for i in range(257)))
        with self.assertRaises(ContractError):replace(original,reviewer_principal_ids=principals)
        bounded=replace(original,reviewer_principal_ids=principals[:256])
        self.assertNotEqual(bounded.sha256,original.sha256)
        for changed in (replace(original,revision=2),replace(original,max_review_seconds=601),
            replace(original,review_revoker_ids=()),replace(original,valid_until=NOW+timedelta(hours=3))):
            self.assertNotEqual(changed.sha256,original.sha256)


class ReviewPublicationQueryTests(TestCase):
    def plan(self,action=Action.CREATE_GRANT,**changes):
        args=inputs(action)|changes
        return build_review_publication(action,control=control_rows(),policy_snapshot=policy_snapshot(args['policy'],args['checked_at']),**args)

    def test_all_actions_deterministic_disabled_bound_to_global_control_and_atomic_audit(self):
        for action in Action:
            first=self.plan(action);self.assertEqual(first,self.plan(action));self.assertFalse(first.execution_enabled)
            self.assertTrue(first.native_job_id.startswith('campaign_review_publish_'))
            audit=parse_row(first.audit_json)
            self.assertEqual(audit['action'],'REVIEW_CONTROL_'+action.value)
            self.assertEqual(audit['control_revision'],2)
            self.assertEqual(first.sql.count('COMMIT TRANSACTION;'),1)
            self.assertIn('ROLLBACK TRANSACTION;',first.sql);self.assertIn('REVIEW_PUBLICATION_CAS_CONFLICT',first.sql)
            self.assertIn('campaign_permission_controls',first.sql);self.assertIn('campaign_permission_publication_audits',first.sql)
            self.assertLessEqual(first.sql.count(';'),64)
            self.assertLessEqual(len(first.config_json.encode()),262144)

    def test_policy_and_exact_capture_changes_change_command_and_native_id(self):
        first=self.plan();second=self.plan(policy=replace(policy(),revision=2))
        self.assertNotEqual(first.command_sha256,second.command_sha256)
        shifted=self.plan(checked_at=NOW+timedelta(seconds=1))
        self.assertNotEqual(first.native_job_id,shifted.native_job_id)

    def test_replay_checks_exact_heads_histories_audit_and_preserves_other_records(self):
        sql=self.plan(Action.CREATE_REVIEW).sql
        for code in ('REPLAY_AUDIT','REPLAY_CONTROL_HISTORY','REPLAY_HEAD','REPLAY_HISTORY','GRANT_HEAD','GRANT_HISTORY','PERMISSION_HEAD','PERMISSION_HISTORY'):
            self.assertIn('REVIEW_PUBLICATION_'+code,sql)
        self.assertIn('EXCEPT DISTINCT',sql);self.assertIn('record_id=@record_id',sql)
        self.assertNotIn('DELETE',sql);self.assertNotIn('MERGE',sql)

    def test_no_unused_empty_array_parameters_and_explicit_revocation_ignores_old_expiry(self):
        create=self.plan();params=create.config().to_api_repr()['query']['queryParameters']
        names={p['name'] for p in params}
        self.assertNotIn('old_target_head',names);self.assertNotIn('old_target_history',names)
        for parameter in params:
            if parameter['parameterType']['type']=='ARRAY':self.assertTrue(parameter['parameterValue']['arrayValues'])
        revoke=self.plan(Action.REVOKE_REVIEW)
        self.assertNotIn('candidate_valid_until',revoke.config_json)
        self.assertNotIn('REVIEW_PUBLICATION_GRANT_EXPIRED',revoke.sql)
        self.assertIn('REVIEW_PUBLICATION_POLICY_EXPIRED',revoke.sql)

    def test_control_limit_and_wrong_control_scope_deny(self):
        records=[];previous=None
        for revision in range(1,65):
            item=PublicationControl(revision,previous,'a'*64,OPERATOR.principal.principal_id,NOW)
            records.append(row_json(item.row()));previous=item.sha256
        limited=Rows('CONTROL','campaign-permissions',records[-1],tuple(records))
        with self.assertRaises(ContractError):build_review_publication(Action.CREATE_GRANT,control=limited,policy_snapshot=policy_snapshot(),**inputs())
        with self.assertRaises(ContractError):build_review_publication(Action.CREATE_GRANT,control=replace(control_rows(),record_id='other'),policy_snapshot=policy_snapshot(),**inputs())

    def test_result_pins_and_grant_review_audit_references(self):
        for action in Action:
            plan=self.plan(action);audit=parse_row(plan.audit_json)
            self.assertEqual(audit['record_sha256'],plan.proposed_record_sha256)
            self.assertEqual(audit['review_id'] is not None,'REVIEW' in action.value)
            result=dict(state='APPLIED',command_sha256=plan.command_sha256,record_sha256=plan.proposed_record_sha256,control_sha256=plan.candidate_control_sha256)
            self.assertEqual(validate_result(plan,(result,)),'APPLIED')
            self.assertEqual(validate_result(plan,(result|{'state':'REUSED'},)),'REUSED')
            with self.assertRaises(ContractError):validate_result(plan,(result|{'record_sha256':'b'*64},))
