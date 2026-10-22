"""Pure controlled genesis guards; supplied rows/identity are not live proof."""
from dataclasses import replace
from datetime import timedelta
from unittest import TestCase
from unittest.mock import patch
from src.application.campaign_bootstrap_contract import (
    BootstrapManifest,BootstrapSnapshot,TABLES,CONTROL_HEAD,CONTROL_HISTORY,bootstrap_job_id,
    MAXIMUM_BYTES_BILLED,JOB_TIMEOUT_MS,
)
from src.application.campaign_bootstrap_query import bootstrap_spec,prepare_bootstrap,validate_bootstrap_result
from src.application.campaign_review_storage import row_json,PublicationControl
from src.application.workflow_contract import ContractError
from tests.test_campaign_permission_lifecycle import identity,NOW,AUDIENCE


def manifest():
    return BootstrapManifest(identity().principal.principal_id,AUDIENCE,NOW,NOW+timedelta(minutes=5),'REVIEWED_GENESIS')


def snapshot(rows=None):
    rows={} if rows is None else rows
    return BootstrapSnapshot(tuple((name,rows.get(name,())) for name in TABLES))


def genesis(value=None):
    row=row_json((value or manifest()).control.row())
    return snapshot({CONTROL_HEAD:(row,),CONTROL_HISTORY:(row,)})


class BootstrapContractTests(TestCase):
    def test_frozen_manifest_and_control_bind_exact_owner_audience_window_reason(self):
        value=manifest();self.assertEqual(value,manifest());self.assertEqual(value.sha256,manifest().sha256)
        self.assertEqual(value.control.command_sha256,value.sha256)
        self.assertEqual(value.control.changed_by,identity().principal.principal_id)
        self.assertEqual(value.control.revision,1);self.assertIsNone(value.control.parent_sha256)
        for changed in (replace(value,owner_principal_id=identity('other').principal.principal_id),
            replace(value,expected_audience=AUDIENCE+'-other'),replace(value,reason_code='OTHER_REVIEW'),
            replace(value,deadline_at=NOW+timedelta(seconds=299)),replace(value,accepted_at=NOW+timedelta(seconds=1))):
            self.assertNotEqual(value.sha256,changed.sha256)
            self.assertNotEqual(value.control.sha256,changed.control.sha256)

    def test_owner_must_be_explicit_iap_principal_and_exact_regional_audience(self):
        for owner in ('',None,'admin@example.com','iap-v1:'+'G'*64):
            with self.assertRaises(ContractError):replace(manifest(),owner_principal_id=owner)
        for audience in ('','https://example.com','/projects/1/locations/us-central1/services/foo'):
            with self.assertRaises(ValueError):replace(manifest(),expected_audience=audience)

    def test_manifest_window_boundaries_timezone_and_short_identity(self):
        value=manifest();value.authorize(identity(),NOW)
        for start,end in ((NOW,NOW),(NOW,NOW-timedelta(seconds=1)),(NOW,NOW+timedelta(seconds=301))):
            with self.assertRaises(ContractError):replace(value,accepted_at=start,deadline_at=end)
        for when in (NOW-timedelta(microseconds=1),value.deadline_at):
            with self.assertRaises(ContractError):value.authorize(identity(),when)
        with self.assertRaises(ContractError):replace(value,accepted_at=NOW.replace(tzinfo=None))
        short=replace(identity(),expires_at=int((NOW+timedelta(seconds=60)).timestamp()))
        with self.assertRaises(ContractError):value.authorize(short,NOW)

    def test_wrong_actor_audience_unverified_or_future_identity_denied(self):
        value=manifest()
        for actor in (None,identity('other'),replace(identity(),audience=AUDIENCE+'-other'),
            replace(identity(),issued_at=int((NOW+timedelta(seconds=1)).timestamp()))):
            with self.assertRaises(ContractError):value.authorize(actor,NOW)

    def test_all_thirteen_tables_required_and_no_mutable_or_duplicate_capture(self):
        empty=snapshot();self.assertEqual(empty.state_for(manifest()),'EMPTY')
        self.assertEqual(len(TABLES),13)
        for capture in (empty.tables[:-1],tuple(reversed(empty.tables)),empty.tables+(empty.tables[0],),list(empty.tables)):
            with self.assertRaises(ContractError):BootstrapSnapshot(capture)
        with self.assertRaises(ContractError):BootstrapSnapshot(tuple((name,[]) for name in TABLES))

    def test_exact_genesis_reused_without_implicit_grants(self):
        value=manifest();self.assertEqual(genesis(value).state_for(value),'EXACT_GENESIS')
        nonempty={name for name,rows in genesis().tables if rows}
        self.assertEqual(nonempty,{CONTROL_HEAD,CONTROL_HISTORY})
        self.assertFalse(prepare_bootstrap(value,identity(),genesis(),checked_at=NOW).execution_enabled)

    def test_missing_duplicate_conflicting_and_advanced_control_denied(self):
        value=manifest();row=row_json(value.control.row())
        advanced=PublicationControl(2,value.control.sha256,'a'*64,value.owner_principal_id,NOW)
        for rows in ({CONTROL_HEAD:(row,)},{CONTROL_HISTORY:(row,)},
            {CONTROL_HEAD:(row,row),CONTROL_HISTORY:(row,)},
            {CONTROL_HEAD:(row,),CONTROL_HISTORY:(row,row)},
            {CONTROL_HEAD:(row_json(advanced.row()),),CONTROL_HISTORY:(row,row_json(advanced.row()))}):
            with self.assertRaises(ContractError):snapshot(rows).state_for(value)
        with self.assertRaises(ContractError):genesis(replace(value,reason_code='CONFLICT')).state_for(value)

    def test_orphans_in_every_other_table_deny_bootstrap_and_replay(self):
        value=manifest();arbitrary=row_json(value.control.row())
        for table in TABLES:
            if table in (CONTROL_HEAD,CONTROL_HISTORY):continue
            for baseline in (snapshot(),genesis()):
                changed=dict(baseline.tables);changed[table]=(arbitrary,)
                with self.assertRaises(ContractError):snapshot(changed).state_for(value)

    def test_overflow_and_noncanonical_rows_are_not_truncated(self):
        row=row_json(manifest().control.row())
        for values in ((row,)*3,(' '+row,),('{}',),('x'*65537,)):
            with self.assertRaises(ContractError):snapshot({CONTROL_HEAD:values})

    def test_global_native_slot_independent_of_owner_time_manifest_and_version(self):
        original=bootstrap_job_id()
        other=replace(manifest(),owner_principal_id=identity('other').principal.principal_id)
        self.assertEqual(bootstrap_spec(manifest()).native_job_id,bootstrap_spec(other).native_job_id)
        self.assertNotEqual(bootstrap_spec(manifest()).config_json,bootstrap_spec(other).config_json)
        with patch('src.application.campaign_bootstrap_contract.VERSION','future-manifest-format'):
            self.assertEqual(bootstrap_job_id(),original)
        self.assertTrue(original.endswith('_0'))


class BootstrapQueryTests(TestCase):
    def test_plan_determinism_disabled_profile_and_no_submission_or_token_fields(self):
        first=prepare_bootstrap(manifest(),identity(),snapshot(),checked_at=NOW)
        later=prepare_bootstrap(manifest(),replace(identity(),issued_at=int(NOW.timestamp())),
            snapshot(),checked_at=NOW+timedelta(seconds=1))
        self.assertEqual(first,later);self.assertFalse(first.execution_enabled)
        self.assertEqual(first.config().maximum_bytes_billed,MAXIMUM_BYTES_BILLED)
        self.assertEqual(int(first.config().job_timeout_ms),JOB_TIMEOUT_MS)
        self.assertFalse(first.config().use_query_cache)
        self.assertNotIn('identity_issued_at',first.config_json)
        self.assertNotIn('jwt',first.config_json.lower())

    def test_only_control_rows_inserted_and_all_tables_checked(self):
        plan=bootstrap_spec(manifest());sql=plan.sql
        for name in TABLES:self.assertIn(f'`project-661d501a-e709-4276-a26.ci_brand_memory.{name}`',sql)
        self.assertEqual(sql.count('INSERT INTO `'),2)
        self.assertIn('INSERT INTO `project-661d501a-e709-4276-a26.ci_brand_memory.'+CONTROL_HEAD+'`',sql)
        self.assertNotIn('DELETE',sql);self.assertNotIn('UPDATE',sql)
        self.assertEqual(sql.count('COMMIT TRANSACTION;'),1)
        self.assertEqual(sql.count('ROLLBACK TRANSACTION;'),1)
        self.assertIn('EXCEPT DISTINCT SELECT * FROM proposed_control',sql)
        self.assertLessEqual(sql.count(';'),64)

    def test_prepare_requires_valid_authority_and_complete_snapshot(self):
        for actor,rows in ((None,snapshot()),(identity('other'),snapshot()),(identity(),None)):
            with self.assertRaises(ContractError):prepare_bootstrap(manifest(),actor,rows,checked_at=NOW)

    def test_result_exact_pins_shape_state_and_original_observation_window(self):
        value=manifest();plan=bootstrap_spec(value)
        result=dict(state='APPLIED',manifest_sha256=value.sha256,control_sha256=value.control.sha256,observed_at=NOW)
        self.assertEqual(validate_bootstrap_result(plan,(result,)),'APPLIED')
        self.assertEqual(validate_bootstrap_result(plan,(result|{'state':'REUSED'},)),'REUSED')
        for values in ((),(result,result),[result]):
            with self.assertRaises(ContractError):validate_bootstrap_result(plan,values)
        for changes in ({'state':'UNKNOWN'},{'manifest_sha256':'a'*64},{'control_sha256':'b'*64},
            {'observed_at':value.deadline_at},{'observed_at':NOW-timedelta(microseconds=1)},{'extra':True}):
            with self.assertRaises(ContractError):validate_bootstrap_result(plan,(result|changes,))
