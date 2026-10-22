"""Fresh authorized runtime resolution and changes during exact Vertex reads."""
from dataclasses import replace
from datetime import timedelta
from unittest.mock import Mock,patch
import unittest

from src.application.access_policy import AccessDecision,Action,Reason
from src.application.prompt_evaluation_contract import _hash
from src.application.prompt_runtime_service import PromptRuntimeService
from src.application.workflow_contract import ContractError
from tests.test_access_reader import EPOCH
from tests.test_prompt_runtime_contract import capture,profile,behavior


class RuntimeServiceTests(unittest.TestCase):
    def setUp(self):
        self.c=capture();self.identity=Mock();self.identity.authenticate.return_value=self.c.request.identity
        self.access=Mock()
        def decision(identity,action,**kwargs):
            owner=action is Action.REFINE_DRAFT
            return AccessDecision(True,action,Reason.ALLOWED_OWNER if owner else Reason.ALLOWED_WORKSPACE_ACTION,
                kwargs['workspace_id'],identity.principal.principal_id,3,
                kwargs.get('conversation_id'),1 if owner else None)
        self.access.authorize.side_effect=decision
        self.reader=Mock();self.reader.read.return_value=self.c
        self.runtime=PromptRuntimeService(identity_adapter=self.identity,access_reader=self.access,
            reader=self.reader,vertex_transport=Mock(),behaviors=(behavior(),),
            approved_profiles=frozenset({_hash(profile())}),enabled=True)
        patch('src.application.identity.time.time',return_value=EPOCH).start()
        clock=patch('src.application.prompt_runtime_service.datetime').start()
        clock.now.return_value=self.c.observed_at+timedelta(seconds=1)
        self.vertex=patch('src.application.prompt_runtime_service.read_exact_vertex_prompt',
            return_value=self.c.target.head.snapshot).start()
        self.addCleanup(patch.stopall)

    def resolve(self,**changes):
        return self.runtime.resolve({},**(dict(workspace_id='workspace-1',conversation_id='conversation-1',
            request_id='request-1',invocation_id='invocation-1',profile=profile())|changes))

    def test_exact_version_and_second_bigquery_snapshot_with_current_owner_access(self):
        result=self.resolve()
        self.assertEqual(result.snapshot,self.c.target.head.snapshot)
        self.assertEqual(self.reader.read.call_count,2)
        self.assertEqual(self.access.authorize.call_count,6)
        self.assertEqual(self.vertex.call_args.args[1].version_id,'4')
        self.assertEqual(result.binding.invocation_id,'invocation-1')
        self.runtime.revalidate({},result,profile())
        self.assertEqual(self.vertex.call_count,2)

    def test_disabled_defaults_deny_before_auth_or_cloud(self):
        self.runtime.enabled=False
        with self.assertRaises(ContractError):self.resolve()
        self.identity.authenticate.assert_not_called();self.reader.read.assert_not_called()

    def test_unapproved_profile_denies_before_vertex(self):
        with self.assertRaises(ContractError):self.resolve(profile=profile()|{'seed':10})
        self.vertex.assert_not_called()

    def test_changed_control_policy_or_membership_during_vertex_denies(self):
        changed=replace(self.c.control,command_digest='f'*64)
        # A trusted-reader supplied change still cannot silently switch the binding.
        c=replace(self.c,control=changed,control_history=(changed,))
        self.reader.read.side_effect=[self.c,c]
        with self.assertRaises(ContractError):self.resolve()

    def test_same_resource_edited_vertex_text_and_vertex_failure_have_no_fallback(self):
        self.vertex.return_value=replace(self.c.target.head.snapshot,
            system_instruction='Changed {campaign_request}')
        with self.assertRaises(ContractError):self.resolve()
        self.vertex.side_effect=TimeoutError('unavailable')
        with self.assertRaises(TimeoutError):self.resolve()
        self.assertEqual(self.vertex.call_count,2)

    def test_access_revocation_and_wrong_conversation_prevent_release(self):
        allowed=self.access.authorize.side_effect
        def deny(identity,action,**kwargs):
            d=allowed(identity,action,**kwargs)
            return replace(d,allowed=False) if self.vertex.called else d
        self.access.authorize.side_effect=deny
        with self.assertRaises(ContractError):self.resolve()

    def test_revalidation_refuses_changed_invocation_profile_or_actor(self):
        result=self.resolve()
        with self.assertRaises(ContractError):self.runtime.revalidate({},result,profile()|{'prompt_version':'5'})
        from src.application.identity import principal_for_subject
        different=replace(self.c.request.identity,subject='different',
            principal=principal_for_subject(self.c.request.identity.issuer,'different'))
        self.identity.authenticate.return_value=different
        with self.assertRaises(ContractError):self.runtime.revalidate({},result,profile())

    def test_stale_or_future_bigquery_observation_denies(self):
        at=self.c.observed_at+timedelta(seconds=60)
        self.reader.read.return_value=replace(self.c,observed_at=at,
            evaluation=replace(self.c.evaluation,observed_at=at))
        with self.assertRaises(ContractError):self.resolve()

    def test_stored_binding_recheck_and_corruption_deny(self):
        resolved=self.resolve()
        self.reader.read_binding.return_value=dict(observed_at=self.c.observed_at,
            binding_count=1,bindings=[resolved.binding.row()])
        self.runtime.verify_committed_binding({},resolved,profile())
        self.reader.read_binding.return_value['bindings'][0]['binding_sha256']='f'*64
        with self.assertRaises(ContractError):self.runtime.verify_committed_binding({},resolved,profile())

    def test_real_es256_assertion_and_tampering_denial_before_bigquery(self):
        import base64,json
        from types import SimpleNamespace
        from google.auth import jwt
        from src.application.identity import IapIdentityAdapter,IapSettings,ASSERTION_HEADER,IdentityError
        from tests.test_identity import signer_and_public_key
        from tests.test_access_reader import NOW as IDENTITY_NOW
        signer,public=signer_and_public_key();identity=self.c.request.identity
        request=Mock(return_value=SimpleNamespace(status=200,data=json.dumps({'fixture-key':public}).encode()))
        claims=dict(iss=identity.issuer,sub=identity.subject,aud=identity.audience,
                    iat=identity.issued_at,exp=identity.expires_at)
        token=jwt.encode(signer,claims).decode()
        self.runtime.identity_adapter=IapIdentityAdapter(IapSettings(identity.audience))
        with patch('src.application.identity._BoundedKeyRequest',return_value=request), \
                patch('google.auth._helpers.utcnow',return_value=IDENTITY_NOW.replace(tzinfo=None)):
            self.runtime.resolve({ASSERTION_HEADER:token},workspace_id='workspace-1',
                conversation_id='conversation-1',request_id='request-1',invocation_id='invocation-1',profile=profile())
            parts=token.split('.');claims['sub']='attacker'
            parts[1]=base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=')
            self.reader.reset_mock();self.vertex.reset_mock()
            with self.assertRaises(IdentityError):
                self.runtime.resolve({ASSERTION_HEADER:'.'.join(parts)},workspace_id='workspace-1',
                    conversation_id='conversation-1',request_id='request-1',invocation_id='invocation-1',profile=profile())
            self.reader.read.assert_not_called();self.vertex.assert_not_called()

    def test_stale_snapshot_and_membership_revision_changes_deny(self):
        with patch('src.application.prompt_runtime_service.datetime') as clock:
            clock.now.return_value=self.c.observed_at+timedelta(seconds=31)
            with self.assertRaises(ContractError):self.resolve()
        allowed=self.access.authorize.side_effect
        def changed(identity,action,**kwargs):
            decision=allowed(identity,action,**kwargs)
            return replace(decision,membership_revision=4) if self.vertex.called else decision
        self.access.authorize.side_effect=changed
        with self.assertRaises(ContractError):self.resolve()


if __name__=='__main__':unittest.main()
