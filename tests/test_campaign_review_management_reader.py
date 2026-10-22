"""Atomic management capture/binding guards; synthetic identities, no cloud calls."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from unittest.mock import Mock,patch
from contextlib import ExitStack
from google.cloud import bigquery
from src.application import campaign_review_management_reader as reader
from src.application.campaign_review_publication_contract import ReviewAction as Action,structural_records
from src.application.campaign_review_publication_query import build_review_publication
from src.application.campaign_permission_reader import Checkpoint
from src.application.campaign_permission_publication import Rows
from src.application.campaign_operator_policy_storage import POLICY_ID,HEAD_TABLE,HISTORY_TABLE,encode_record,head_row,decode_state
from src.application.campaign_review_storage import row_json,parse_row,PublicationControl
from src.application.campaign_permission_storage import encode_record as encode_permission,head_row as permission_head
from src.application.operation_budget import OperationStopped
from src.ingestion.configure_campaign_acceptance import PROJECT,LOCATION
from src.application.identity import IapSettings
from src.application.workflow_contract import ContractError
from tests.test_campaign_review_publication_writer import (
    inputs,control_rows,policy_snapshot,rows,OPERATOR,
)
from tests.test_campaign_permission_lifecycle import NOW,AUDIENCE,identity,draft,capture as review_value


def request(action=Action.CREATE_GRANT,**changes):
    args=inputs(action)
    values=dict(action=action,target=Checkpoint(args['target'].record_id))
    if args['grant']:values['grant']=Checkpoint(args['grant'].record_id)
    if args['permission']:
        values.update(permission_kind=args['permission'].kind,permission=Checkpoint(args['permission'].record_id))
    return reader.ManagementReadRequest(**(values|changes))


def wire(action=Action.CREATE_GRANT,*,args=None,snapshot=None,control=None):
    args=args or inputs(action);snapshot=snapshot or policy_snapshot();result=[]
    def append(label,head,history):
        for role,values in (('HEAD',[head] if head else []),('HISTORY',history)):
            for row in values:
                result.append(dict(capture_kind=label,row_role=role,
                    record_id=row.get('record_id',row.get('control_id',row.get('policy_id'))),
                    revision=row['revision'],record_json=row_json(row),scoped_row_count=len(values)))
    append('POLICY',parse_row(snapshot.head_json),tuple(parse_row(r) for r in snapshot.history_json))
    append('CONTROL',*(control or control_rows()).decoded())
    append('TARGET',*args['target'].decoded())
    for label,key in (('GRANT','grant'),('PERMISSION','permission')):
        if args[key]:append(label,*args[key].decoded())
    return tuple(result)


def decode(action=Action.CREATE_GRANT,*,values=None,req=None,actor=None):
    return reader.decode_capture(wire(action) if values is None else values,req or request(action),
        actor or inputs(action)['identity'],expected_audience=IapSettings(AUDIENCE),
        native_job_id='fixture-atomic-management-read',checked_at=NOW)


class ManagementCaptureTests(TestCase):
    def test_all_five_actions_capture_and_plan_from_one_policy_snapshot(self):
        for action in Action:
            capture=decode(action);plan=capture.plan(value=inputs(action)['value'],reason_code='EXPLICIT_FIXTURE_ACTION')
            self.assertFalse(plan.execution_enabled)
            self.assertEqual(capture.native_job_id,capture.policy_snapshot.native_job_id)
            self.assertEqual(capture.checked_at,capture.policy_snapshot.checked_at)
            self.assertEqual(capture.target,inputs(action)['target'])
            self.assertIn('REVIEW_PUBLICATION_POLICY_HEAD',plan.sql)

    def test_request_requires_action_specific_sources_and_fixed_checkpoints(self):
        for changes in ({'action':'CREATE_GRANT'},{'target':None},{'grant':Checkpoint('extra')},
            {'permission_kind':'CLAIM'},{'policy':Checkpoint('other')},{'control':Checkpoint('other')}):
            with self.assertRaises(ContractError):request(**changes)
        for changes in ({'grant':None},{'permission':None},{'permission_kind':'OTHER'}):
            with self.assertRaises(ContractError):request(Action.CREATE_REVIEW,**changes)
        with self.assertRaises(ContractError):request(Action.REVOKE_REVIEW,permission_kind='CLAIM')

    def test_missing_policy_control_source_and_partial_or_duplicate_groups_deny(self):
        complete=wire(Action.CREATE_REVIEW)
        for label in ('POLICY','CONTROL','GRANT','PERMISSION'):
            with self.assertRaises(ContractError):decode(Action.CREATE_REVIEW,values=tuple(r for r in complete if r['capture_kind']!=label))
        for values in (complete[1:],complete+complete[:1],complete[:1]*reader.MAX_ROWS):
            with self.assertRaises(ContractError):decode(Action.CREATE_REVIEW,values=values)

    def test_projection_scope_types_counts_and_oversized_or_bad_json_deny(self):
        complete=wire()
        for changes in ({'capture_kind':'EXTRA'},{'capture_kind':[]},{'record_id':'other'},
            {'row_role':'OTHER'},{'revision':True},{'revision':2},{'scoped_row_count':True},
            {'scoped_row_count':2},{'record_json':None},{'record_json':'{'},{'record_json':'x'*65537},{'extra':True}):
            with self.assertRaises(ContractError):decode(values=(complete[0]|changes,)+complete[1:])

    def test_unknown_actor_wrong_action_role_audience_and_expired_identity_deny(self):
        for actor in (identity('outsider'),identity(),replace(OPERATOR,audience=AUDIENCE+'-other'),
            replace(OPERATOR,expires_at=int(NOW.timestamp()))):
            with self.assertRaises(ContractError):decode(actor=actor)
        with self.assertRaises(ContractError):decode(Action.CREATE_REVIEW,actor=OPERATOR)

    def test_policy_revocation_expiry_and_historical_checkpoint_guards(self):
        snap=policy_snapshot();first=snap.state.current
        second=replace(first,policy=replace(first.policy,revision=2),parent_sha256=first.sha256,status='REVOKED')
        history=(encode_record(first),encode_record(second));head=head_row(history[-1])
        stopped=replace(snap,state=decode_state(head,history,checked_at=NOW),head_json=row_json(head),
            history_json=tuple(row_json(r) for r in history))
        with self.assertRaises(ContractError):decode(values=wire(snapshot=stopped))
        expired=policy_snapshot(replace(first.policy,valid_until=NOW))
        with self.assertRaises(ContractError):decode(values=wire(snapshot=expired))
        req=request(policy=Checkpoint(POLICY_ID,1,first.sha256))
        self.assertEqual(decode(req=req).policy_snapshot.state.current,first)
        for checkpoint in (Checkpoint(POLICY_ID,2),Checkpoint(POLICY_ID,1,'b'*64)):
            with self.assertRaises(ContractError):decode(req=request(policy=checkpoint))

    def test_absent_creation_never_satisfies_a_pinned_or_higher_target_floor(self):
        for checkpoint in (Checkpoint(request().target.record_id,2),Checkpoint(request().target.record_id,1,'a'*64)):
            with self.assertRaises(ContractError):decode(req=request(target=checkpoint))
        # Existing targets cannot be silently replaced by a CREATE action.
        args=inputs(Action.REVISE_GRANT)
        with self.assertRaises(ContractError):decode(values=wire(args=args))

    def test_control_grant_review_and_permission_exact_historical_floors(self):
        for action,label,field in ((Action.REVISE_GRANT,'TARGET','target'),(Action.REVOKE_REVIEW,'TARGET','target'),
            (Action.CREATE_REVIEW,'GRANT','grant'),(Action.CREATE_REVIEW,'PERMISSION','permission'),
            (Action.CREATE_GRANT,'CONTROL','control')):
            complete=wire(action);head=next(parse_row(r['record_json']) for r in complete if r['capture_kind']==label and r['row_role']=='HEAD')
            sha=head['control_sha256'] if label=='CONTROL' else head['record_sha256']
            record_id=getattr(request(action),field).record_id
            decode(action,req=request(action,**{field:Checkpoint(record_id,1,sha)}))
            for checkpoint in (Checkpoint(record_id,2),Checkpoint(record_id,1,'f'*64)):
                with self.assertRaises(ContractError):decode(action,req=request(action,**{field:checkpoint}))

    def test_expired_and_revoked_source_grants_deny_review_creation_but_allow_cleanup(self):
        for revoked in (False,True):
            args=inputs(Action.REVOKE_REVIEW);g=structural_records(args['grant'],NOW)[0]
            if revoked:
                args['grant']=rows(g,replace(g,revision=2,parent_sha256=g.sha256,status='REVOKED'))
            else:
                # The first historical payload must still match the review pin.
                old=replace(g,value=replace(g.value,valid_until=NOW-timedelta(seconds=1)),recorded_at=NOW-timedelta(hours=2))
                review=structural_records(args['target'],NOW)[0]
                args['grant']=rows(old);args['target']=rows(replace(review,value=replace(review.value,grant_sha256=old.value.sha256)))
            capture=decode(Action.REVOKE_REVIEW,values=wire(Action.REVOKE_REVIEW,args=args),actor=OPERATOR)
            self.assertIn('REVIEW_CONTROL_REVOKE_REVIEW',capture.plan(reason_code='EXPLICIT_CLEANUP').audit_json)
            create_args=inputs(Action.CREATE_REVIEW)|{'grant':args['grant']}
            with self.assertRaises(ContractError):decode(Action.CREATE_REVIEW,values=wire(Action.CREATE_REVIEW,args=create_args))

    def test_review_revocation_requires_owner_or_explicit_revoker_not_any_reviewer(self):
        snap=policy_snapshot();p=snap.state.current.policy
        other=identity('other-reviewer');p=replace(p,reviewer_principal_ids=tuple(sorted(p.reviewer_principal_ids+(other.principal.principal_id,))))
        with self.assertRaises(ContractError):decode(Action.REVOKE_REVIEW,actor=other,values=wire(Action.REVOKE_REVIEW,snapshot=policy_snapshot(p)))

    def test_plan_revalidates_review_content_and_grant_candidate_scope(self):
        capture=decode(Action.CREATE_REVIEW);value=inputs(Action.CREATE_REVIEW)['value']
        with self.assertRaises(ContractError):capture.plan(value=replace(value,content_sha256='a'*64),reason_code='EXPLICIT_FIXTURE_ACTION')
        capture=decode();value=inputs()['value']
        with self.assertRaises(ContractError):capture.plan(value=replace(value,principal_ids=(identity('outsider').principal.principal_id,)),reason_code='EXPLICIT_FIXTURE_ACTION')

    def test_claim_offer_and_evidence_reviews_bind_their_exact_stored_content(self):
        for kind in ('CLAIM','OFFER','EVIDENCE'):
            args=inputs(Action.CREATE_REVIEW);value=draft(kind);review=review_value(value,kind)
            row=encode_permission(value,recorded_at=NOW)
            args.update(target=Rows('REVIEW',review.review_id,None,()),
                permission=Rows(kind,row['record_id'],row_json(permission_head(row)),(row_json(row),)))
            req=request(Action.CREATE_REVIEW,permission_kind=kind,permission=Checkpoint(row['record_id']))
            result=decode(Action.CREATE_REVIEW,values=wire(Action.CREATE_REVIEW,args=args),req=req)
            self.assertEqual(result.permission.kind,kind)
            self.assertFalse(result.plan(value=review,reason_code='REVIEW_EXACT_CONTENT').execution_enabled)

    def test_future_and_terminal_targets_or_grants_deny_without_returning_capture(self):
        args=inputs(Action.REVISE_GRANT);first=structural_records(args['target'],NOW)[0]
        for target in (rows(replace(first,recorded_at=NOW+timedelta(seconds=1))),
            rows(first,replace(first,revision=2,parent_sha256=first.sha256,status='REVOKED'))):
            with self.assertRaises(ContractError):decode(Action.REVISE_GRANT,values=wire(Action.REVISE_GRANT,args=args|{'target':target}))
        with self.assertRaises(ContractError):decode(Action.REVOKE_GRANT,values=wire())

    def test_global_fence_limit_is_readable_but_cannot_produce_a_write_plan(self):
        history=[];previous=None
        for revision in range(1,65):
            item=PublicationControl(revision,previous,'a'*64,OPERATOR.principal.principal_id,NOW)
            history.append(row_json(item.row()));previous=item.sha256
        control=Rows('CONTROL','campaign-permissions',history[-1],tuple(history))
        result=decode(values=wire(control=control))
        with self.assertRaises(ContractError):result.plan(value=inputs()['value'],reason_code='EXPLICIT_FIXTURE_ACTION')

    def test_conflicting_partition_counts_and_injected_rows_are_rejected(self):
        complete=wire(Action.CREATE_REVIEW)
        injected=complete+(complete[0]|{'capture_kind':'GRANT','record_id':request(Action.CREATE_REVIEW).grant.record_id},)
        with self.assertRaises(ContractError):decode(Action.CREATE_REVIEW,values=injected)
        snapshot=policy_snapshot(replace(inputs()['policy'],revision=2))
        values=list(wire(snapshot=snapshot));history_indices=[i for i,r in enumerate(values) if r['capture_kind']=='POLICY' and r['row_role']=='HISTORY']
        values[history_indices[-1]]=values[history_indices[-1]]|{'scoped_row_count':1}
        with self.assertRaises(ContractError):decode(values=tuple(values))

    def test_single_parameterized_statement_captures_every_required_source(self):
        for action in Action:
            sql,cfg=reader.query_spec(request(action));self.assertNotIn('INSERT',sql);self.assertNotIn('BEGIN TRANSACTION',sql)
            self.assertIn(HEAD_TABLE,sql);self.assertIn(HISTORY_TABLE,sql)
            self.assertTrue(sql.endswith('LIMIT 326'));self.assertFalse(cfg.use_query_cache)
            for parameter in cfg.query_parameters:self.assertNotIn(parameter.value,sql)
            self.assertEqual(sql.count('COUNT(*) OVER'),1)


class ManagementNativeAdapterTests(TestCase):
    def client(self):
        client=Mock();job=Mock(state='DONE',error_result=None);job.result.return_value=wire()
        client.query.return_value=job;client.get_job.return_value=job
        return client,job

    def patches(self):
        stack=ExitStack()
        for name in ('inspect_policy','inspect_reviews','inspect_permissions'):
            stack.enter_context(patch.object(reader,name,return_value={'status':'VERIFIED'}))
        stack.enter_context(patch.object(reader,'matches_sharing_job',return_value=True))
        return stack

    def test_native_read_is_one_query_with_disabled_retries_and_same_job_reconciliation(self):
        client,job=self.client()
        with self.patches():capture=reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        client.query.assert_called_once();client.get_job.assert_called_once()
        self.assertEqual(client.get_job.call_args.args,(capture.native_job_id,))
        self.assertIsNone(client.query.call_args.kwargs['retry']);self.assertIsNone(client.query.call_args.kwargs['job_retry'])
        self.assertIsNone(job.result.call_args.kwargs['retry']);self.assertIsNone(job.result.call_args.kwargs['job_retry'])

    def test_missing_resources_or_identity_expiring_during_preflight_prevents_query(self):
        client,_=self.client()
        with self.patches(),patch.object(reader,'inspect_reviews',return_value={'status':'MISSING_RESOURCES'}):
            with self.assertRaises(ContractError):reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        client.query.assert_not_called()
        with self.patches():
            with self.assertRaises(ContractError):reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),
                clock=Mock(side_effect=[NOW,NOW+timedelta(seconds=301)]))
        client.query.assert_not_called()

    def test_wrong_native_job_or_missing_policy_never_releases_capture(self):
        client,job=self.client()
        with self.patches(),patch.object(reader,'matches_sharing_job',return_value=False):
            with self.assertRaises(ContractError):reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        job.result.return_value=()
        with self.patches():
            with self.assertRaises(ContractError) as error:reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        self.assertTrue(error.exception.native_job_id.startswith('campaign_review_management_read_'))

    def test_real_sdk_native_metadata_must_match_exact_query_and_configuration(self):
        client,job=self.client();actual_matches=reader.matches_sharing_job
        def original(native,**kwargs):
            arguments=client.query.call_args;config=arguments.kwargs['job_config'].to_api_repr()
            config['query']['query']=arguments.args[0]
            payload=dict(jobReference=dict(projectId=PROJECT,location=LOCATION,jobId=native),
                configuration=config,status=dict(state='DONE'))
            return bigquery.QueryJob.from_api_repr(payload,client)
        client.get_job.side_effect=original
        with self.patches(),patch.object(reader,'matches_sharing_job',wraps=actual_matches):
            result=reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
        self.assertEqual(result.target,inputs()['target'])
        good=original(result.native_job_id)
        payload=good.to_api_repr();payload['configuration']['query']['useQueryCache']=True
        client.get_job.side_effect=None;client.get_job.return_value=bigquery.QueryJob.from_api_repr(payload,client)
        with self.patches(),patch.object(reader,'matches_sharing_job',wraps=actual_matches):
            with self.assertRaises(ContractError):reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)

    def test_stops_and_provider_failures_propagate_without_another_submission(self):
        for error in (OperationStopped('OPERATION_CANCELLED'),RuntimeError('provider unavailable')):
            client,job=self.client();job.result.side_effect=error
            with self.patches():
                with self.assertRaises(type(error)):reader.read_capture(client,request(),OPERATOR,expected_audience=IapSettings(AUDIENCE),clock=lambda:NOW)
            client.query.assert_called_once();client.get_job.assert_not_called()


class OperatorPolicyTransactionBindingTests(TestCase):
    def plan(self,snapshot=None,**changes):
        return build_review_publication(Action.CREATE_GRANT,policy_snapshot=snapshot or policy_snapshot(),
            control=control_rows(),**(inputs()|changes))

    def test_no_supplied_policy_only_bypass_or_stale_capture_timestamp(self):
        for snapshot in (None,replace(policy_snapshot(),checked_at=NOW-timedelta(seconds=1))):
            with self.assertRaises(ContractError):build_review_publication(Action.CREATE_GRANT,policy_snapshot=snapshot,control=control_rows(),**inputs())

    def test_policy_payload_state_head_and_history_must_all_match(self):
        snap=policy_snapshot()
        for changed in (replace(snap,history_json=()),replace(snap,history_json=snap.history_json*2),
            replace(snap,head_json='{}'),replace(snap,state=policy_snapshot(replace(inputs()['policy'],max_review_seconds=601)).state)):
            with self.assertRaises(ContractError):self.plan(changed)
        with self.assertRaises(ContractError):self.plan(policy=replace(inputs()['policy'],max_review_seconds=601))

    def test_head_and_complete_history_checks_precede_replay_and_writes(self):
        plan=self.plan();sql=plan.sql
        for table,temp,code in ((HEAD_TABLE,'operator_policy_head','POLICY_HEAD'),(HISTORY_TABLE,'operator_policy_history','POLICY_HISTORY')):
            self.assertIn(table,sql);self.assertIn('policy_id=@operator_policy_id',sql)
            self.assertIn('FROM '+temp,sql);self.assertIn('REVIEW_PUBLICATION_'+code,sql)
            self.assertLess(sql.index("AS 'REVIEW_PUBLICATION_"+code+"'"),sql.index('IF (SELECT COUNT(*)'))
        self.assertEqual(sql.count('COMMIT TRANSACTION;'),1);self.assertIn('ROLLBACK TRANSACTION;',sql)

    def test_publisher_metadata_change_binds_new_command_even_for_identical_policy(self):
        snap=policy_snapshot();record=replace(snap.state.current,reason_code='DIFFERENT_REVIEWED_PUBLICATION')
        history=(encode_record(record),);head=head_row(history[-1])
        changed=replace(snap,state=decode_state(head,history,checked_at=NOW),head_json=row_json(head),history_json=(row_json(history[0]),))
        self.assertEqual(snap.state.current.policy,changed.state.current.policy)
        self.assertNotEqual(self.plan(snap).native_job_id,self.plan(changed).native_job_id)
