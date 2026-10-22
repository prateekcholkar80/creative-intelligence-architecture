from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch
import hashlib
from copy import deepcopy
from src.application.marketing_pilot import MarketingPilot
from tests.test_identity import AUDIENCE

class MarketingPilotTests(TestCase):
    def manifest(self):return dict(iap_audience=AUDIENCE,workspace_id='workspace',submission={},interpreter_runtime={'enabled':False},release_enabled=False)
    def instance(self):
        from tests.test_management_service import client
        current=client();self.addCleanup(current.close)
        with patch('src.application.marketing_pilot.atexit.register'):
            return MarketingPilot(client=current,manifest=self.manifest(),cursor_key=b'x'*32)
    def test_release_and_model_execution_cannot_be_enabled(self):
        for change in ({'release_enabled':True},{'interpreter_runtime':{'enabled':True}}):
            with self.assertRaises(ValueError):MarketingPilot(client=Mock(),manifest=self.manifest()|change,cursor_key=b'x'*32)
    def test_creator_role_is_explicit_and_preserves_existing_roles(self):
        pilot=self.instance();actor=SimpleNamespace(subject='subject',principal=SimpleNamespace(principal_id='principal'))
        ws=dict(workspace_id='workspace',revision=1);member=dict(roles=['ADMIN','REVIEWER'],revision=1)
        pilot.inspect=Mock(return_value=(actor,ws,member,None,'conversation'))
        pilot.management=Mock();pilot.management.io.original_job.side_effect=__import__('google.api_core.exceptions',fromlist=['NotFound']).NotFound('missing')
        pilot.management.submit.return_value=SimpleNamespace(native_status=SimpleNamespace(value='COMMITTED'))
        self.assertEqual(pilot.setup({},'CREATOR'),'COMMITTED')
        kwargs=pilot.management.prepare.call_args.kwargs
        self.assertEqual(kwargs['roles'],('ADMIN','CREATOR','REVIEWER'));self.assertEqual(kwargs['target_subject'],'subject')
    def test_nonadmin_cannot_assign_creator_and_noncreator_cannot_create(self):
        pilot=self.instance();pilot.inspect=Mock(return_value=(Mock(),{},dict(roles=['VIEWER']),None,'conversation'))
        with self.assertRaises(ValueError):pilot.setup({},'CREATOR')
        with self.assertRaises(ValueError):pilot.setup({},'CONVERSATION')
    def test_cached_runtime_still_requires_current_authority(self):
        pilot=self.instance();pilot._runtimes[('principal','conversation')]=(Mock(),[])
        pilot.inspect=Mock(side_effect=ValueError('revoked'))
        with self.assertRaises(ValueError):pilot.runtime({})
    def test_retired_hosts_do_not_auto_restart_or_replay(self):
        pilot=self.instance();actor=SimpleNamespace(principal=SimpleNamespace(principal_id='principal'))
        pilot.inspect=Mock(return_value=(actor,{},dict(roles=['CREATOR']),{},'conversation'))
        pilot._runtimes[('principal','conversation')]=(Mock(),[Mock(retired=True)])
        with self.assertRaisesRegex(ValueError,'MARKETING_HOST_RETIRED'):pilot.runtime({})

    def active_pilot(self):
        from tests.test_management_service import identity, workspace, member, conversation
        from src.application.workflow_codec import encode_row
        pilot=self.instance();actor=identity()
        conv='marketing-'+hashlib.sha256(actor.principal.principal_id.encode()).hexdigest()[:32]
        ws=workspace(workspace_id='workspace')
        membership=member(workspace_id='workspace')
        resource=conversation(workspace_id='workspace',conversation_id=conv)
        row={'conversations':[encode_row(resource,'conversations')]}
        pilot.verifier.authenticate=Mock(return_value=actor)
        pilot.management._snapshot=Mock(return_value=(row,ws,[membership]))
        return pilot,actor,conv,row,ws,membership

    def test_revoked_membership_denies_cached_runtime_and_reconnect(self):
        pilot,actor,conv,row,ws,member=self.active_pilot()
        cached=Mock();host=Mock(retired=False)
        pilot._runtimes[(actor.principal.principal_id,conv)]=(cached,(host,))
        self.assertIs(pilot.runtime({}),cached)
        revoked=deepcopy(member);revoked['status']='REVOKED'
        pilot.management._snapshot.return_value=(row,ws,[revoked])
        for operation in (pilot.runtime,pilot.reconnect):
            with self.assertRaisesRegex(ValueError,'MARKETING_MEMBERSHIP_REQUIRED'):operation({})
        cached.assert_not_called();host.start.assert_not_called()

    def test_removed_creator_denies_cached_runtime_even_with_active_membership(self):
        pilot,actor,conv,row,ws,member=self.active_pilot()
        pilot._runtimes[(actor.principal.principal_id,conv)]=(Mock(),(Mock(retired=False),))
        member['roles']=['ADMIN']
        with self.assertRaisesRegex(ValueError,'MARKETING_SETUP_REQUIRED'):pilot.runtime({})

    def test_reconnect_closes_only_current_owner_hosts_and_never_submits(self):
        pilot,actor,conv,*_=self.active_pilot()
        host=Mock();other=Mock()
        pilot._runtimes[(actor.principal.principal_id,conv)]=(Mock(),(host,))
        pilot._runtimes[('other','other-conversation')]=(Mock(),(other,))
        fresh=Mock();pilot.runtime=Mock(return_value=fresh)
        pilot.management.submit=Mock()
        self.assertIs(pilot.reconnect({}),fresh)
        host.close.assert_called_once();other.close.assert_not_called()
        pilot.management.submit.assert_not_called()
        self.assertNotIn((actor.principal.principal_id,conv),pilot._runtimes)

    def test_separate_evaluation_conversation_preserves_default_and_owner_namespace(self):
        from src.application.marketing_pilot import conversation_id,EVALUATION_CONVERSATION
        principal='pilot'
        self.assertEqual(conversation_id(principal),'marketing-'+hashlib.sha256(principal.encode()).hexdigest()[:32])
        evaluation=conversation_id(principal,EVALUATION_CONVERSATION)
        self.assertNotEqual(evaluation,conversation_id(principal))
        self.assertNotEqual(evaluation,conversation_id('other',EVALUATION_CONVERSATION))
        with self.assertRaises(ValueError):conversation_id(principal,'browser-arbitrary-conversation')

    def test_evaluation_setup_uses_distinct_original_command_without_touching_default(self):
        from src.application.marketing_pilot import EVALUATION_CONVERSATION
        from google.api_core.exceptions import NotFound
        pilot=self.instance();actor=SimpleNamespace(principal=SimpleNamespace(principal_id='pilot'))
        pilot.inspect=Mock(return_value=(actor,dict(workspace_id='workspace',revision=4),dict(roles=['CREATOR']),None,'evaluation-conversation'))
        pilot.management=Mock();pilot.management.io.original_job.side_effect=NotFound('absent')
        pilot.management.submit.return_value=SimpleNamespace(native_status=SimpleNamespace(value='COMMITTED'))
        pilot.setup({},'CONVERSATION',context=EVALUATION_CONVERSATION)
        values=pilot.management.prepare.call_args.kwargs
        self.assertEqual(values['conversation_id'],'evaluation-conversation')
        self.assertEqual(values['command_id'],'marketing-conversation-v1-'+EVALUATION_CONVERSATION)
        self.assertNotIn(('pilot','CONVERSATION','default'),pilot._plans)
        self.assertEqual(len(pilot._plans),1)
