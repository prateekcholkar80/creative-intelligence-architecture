"""Real ES256 signature tests with ephemeral in-memory keys; no account bootstrap."""
import base64
from datetime import datetime, timezone
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from google.auth import jwt
from google.auth.crypt import ES256Signer

from src.application.access_policy import Principal
from src.application.operation_budget import OperationSettings, OperationStopped, operation_scope
from src.application.identity import (
    ASSERTION_HEADER, CERTS_URL, ISSUER, IapIdentityAdapter, IapSettings,
    IdentityError, VerifiedIdentity, _BoundedKeyRequest, ensure_fresh, principal_for_subject,
)

AUDIENCE = '/projects/123456789/locations/asia-south1/services/test-campaign'
NOW = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
EPOCH = int(NOW.timestamp())


def signer_and_public_key():
    key = ec.generate_private_key(ec.SECP256R1())
    signer = ES256Signer.from_string(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()), key_id='fixture-key')
    public = key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')
    return signer, public


class IdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.signer, cls.public_key = signer_and_public_key()

    def setUp(self):
        self.adapter = IapIdentityAdapter(IapSettings(AUDIENCE))
        self.request = Mock(return_value=SimpleNamespace(status=200,
            data=json.dumps({'fixture-key': self.public_key}).encode()))
        for patcher in (patch('src.application.identity._BoundedKeyRequest', return_value=self.request),
                        patch('src.application.identity.time.time', return_value=EPOCH),
                        patch('google.auth._helpers.utcnow', return_value=NOW.replace(tzinfo=None))):
            patcher.start()
            self.addCleanup(patcher.stop)

    def claims(self, **changes):
        return dict(iss=ISSUER, aud=AUDIENCE, sub='accounts.google.com:test-subject',
                    iat=EPOCH - 10, exp=EPOCH + 590, **changes)

    def token(self, claims=None, header=None, signer=None):
        return jwt.encode(signer or self.signer, claims or self.claims(), header=header).decode('ascii')

    def verify(self, token):
        return self.adapter.authenticate({ASSERTION_HEADER: token})

    def test_valid_signature_subject_binding_and_fixed_key_endpoint(self):
        result = self.adapter.authenticate({'X-Goog-Iap-Jwt-Assertion': self.token()})
        self.assertIsInstance(result, VerifiedIdentity)
        self.assertEqual(result.principal, principal_for_subject(ISSUER, result.subject))
        self.assertEqual(result.audience, AUDIENCE)
        self.request.assert_called_once_with(CERTS_URL, method='GET')

    def test_tampered_payload_rejected_without_signature_mock(self):
        segments = self.token().split('.')
        claims = self.claims()
        claims['sub'] = 'attacker'
        segments[1] = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=')
        with self.assertRaises(IdentityError):
            self.verify('.'.join(segments))

    def test_different_signing_key_and_unknown_key_id_fail(self):
        other, _ = signer_and_public_key()
        for token in (self.token(signer=other),
                      jwt.encode(self.signer, self.claims(), key_id='unknown').decode()):
            with self.subTest(token=token[:10]), self.assertRaises(IdentityError):
                self.verify(token)

    def test_unsupported_algorithms_and_remote_key_headers_fail_before_fetch(self):
        for header in ({'alg': 'none'}, {'alg': 'RS256'}, {'crit': []}, {'b64': True},
                       {'jku': 'https://attacker.invalid'}, {'x5u': 'https://attacker.invalid'}):
            with self.subTest(header=header), self.assertRaises(IdentityError):
                self.verify(self.token(header=header))
        self.request.assert_not_called()

    def test_wrong_issuer_audience_and_list_audience_rejected(self):
        for changes in ({'iss': 'https://accounts.google.com'}, {'aud': AUDIENCE + '-other'},
                        {'aud': [AUDIENCE]}, {'aud': None}):
            claims = self.claims()
            claims.update(changes)
            with self.subTest(changes=changes), self.assertRaises(IdentityError):
                self.verify(self.token(claims))

    def test_required_claims_must_exist(self):
        for name in ('iss', 'aud', 'sub', 'iat', 'exp'):
            claims = self.claims()
            del claims[name]
            with self.subTest(name=name), self.assertRaises(IdentityError):
                self.verify(self.token(claims))

    def test_subject_is_bounded_opaque_string(self):
        for value in ('', 'a b', 'a\n', 'a\x00', 'x' * 257, None, 42, '\ud800'):
            claims = self.claims()
            claims['sub'] = value
            with self.subTest(subject=repr(value)), self.assertRaises(IdentityError):
                self.verify(self.token(claims))

    def test_lifetime_types_expiry_future_and_maximum(self):
        for changes in ({'iat': True}, {'exp': EPOCH + 590.5}, {'exp': str(EPOCH + 590)},
                        {'iat': EPOCH + 31}, {'exp': EPOCH - 30}, {'exp': EPOCH - 10},
                        {'iat': EPOCH - 100, 'exp': EPOCH + 561}):
            claims = self.claims()
            claims.update(changes)
            with self.subTest(changes=changes), self.assertRaises(IdentityError):
                self.verify(self.token(claims))

    def test_clock_skew_boundary_and_maximum_lifetime(self):
        for changes in ({'iat': EPOCH + 30, 'exp': EPOCH + 630},
                        {'iat': EPOCH - 659, 'exp': EPOCH + 1},
                        {'iat': EPOCH - 600, 'exp': EPOCH - 29}):
            claims = self.claims()
            claims.update(changes)
            self.assertIsInstance(self.verify(self.token(claims)), VerifiedIdentity)

    def test_optional_not_before_is_checked(self):
        for value in (EPOCH + 31, True, str(EPOCH), EPOCH + 591):
            with self.subTest(nbf=value), self.assertRaises(IdentityError):
                self.verify(self.token(self.claims(nbf=value)))
        self.verify(self.token(self.claims(nbf=EPOCH + 30)))

    def test_missing_duplicate_untrusted_and_malformed_headers_denied(self):
        for headers in ({}, {'X-Goog-Authenticated-User-Email': 'accounts.google.com:test@example.invalid'},
                        {ASSERTION_HEADER: self.token(), ASSERTION_HEADER.upper(): self.token()},
                        {ASSERTION_HEADER: b'bytes'}, {ASSERTION_HEADER: 'Bearer ' + self.token()},
                        {ASSERTION_HEADER: self.token() + ',' + self.token()},
                        {ASSERTION_HEADER: 'x' * 16385}, {ASSERTION_HEADER: 'a.b.c'}, None):
            with self.subTest(headers_type=type(headers)), self.assertRaises(IdentityError):
                self.adapter.authenticate(headers)

    def test_email_domain_and_role_claims_do_not_change_principal_or_supply_roles(self):
        first = self.verify(self.token(self.claims(email='first@example.invalid', hd='example.invalid', roles=['ADMIN'])))
        second = self.verify(self.token(self.claims(email='changed@gmail.invalid')))
        self.assertEqual(first.principal, second.principal)
        self.assertFalse(hasattr(first, 'roles'))
        self.assertFalse(hasattr(first, 'email'))
        self.assertNotEqual(first.principal, principal_for_subject(ISSUER, 'different-subject'))

    def test_key_fetch_outage_and_malformed_response_fail_safely(self):
        self.request.side_effect = RuntimeError('secret token and user details')
        with self.assertRaises(IdentityError) as caught:
            self.verify(self.token())
        self.assertEqual(str(caught.exception), 'A valid application identity is required.')
        self.assertIsNone(caught.exception.__cause__)
        self.request.side_effect = None
        self.request.return_value = SimpleNamespace(status=200, data=b'not-json')
        with self.assertRaises(IdentityError):
            self.verify(self.token())

    def test_settings_require_explicit_exact_service_audience(self):
        for audience in ('', 'client-id', AUDIENCE.replace('asia-south1', 'us-central1'),
                         AUDIENCE + '/', AUDIENCE.replace('123456789', '0123'), None):
            with self.subTest(audience=audience), self.assertRaises(ValueError):
                IapSettings(audience)
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(ValueError):
            IapSettings.from_environment()
        with patch.dict(os.environ, {'CI_IAP_EXPECTED_AUDIENCE': AUDIENCE}):
            self.assertEqual(IapSettings.from_environment(), IapSettings(AUDIENCE))
        with self.assertRaises(ValueError):
            IapIdentityAdapter({'expected_audience': AUDIENCE})

    def test_typed_identity_is_bound_and_rechecked_for_expiry(self):
        identity = self.verify(self.token())
        for value in (None, identity.principal, {'principal': identity.principal}):
            with self.assertRaises(IdentityError):
                ensure_fresh(value)
        with patch('src.application.identity.time.time', return_value=EPOCH + 620), self.assertRaises(IdentityError):
            ensure_fresh(identity)
        with self.assertRaises(IdentityError):
            VerifiedIdentity(Principal('forged'), ISSUER, identity.subject, AUDIENCE, EPOCH, EPOCH + 600)

    def test_public_key_transport_has_fixed_timeout(self):
        # Patch the transport rather than the wrapper to exercise its timeout contract.
        with patch('src.application.identity.Request') as request:
            transport = _BoundedKeyRequest()
            transport(CERTS_URL, timeout=900)
            request.return_value.assert_called_once_with(url=CERTS_URL, method='GET',
                body=None, headers=None, timeout=5)
            transport.close()

    def test_public_certificate_request_is_capped_by_parent_budget(self):
        tick=[100.0]
        with patch('src.application.identity.Request') as request,\
                patch('src.application.operation_budget.time.monotonic',side_effect=lambda:tick[0]),\
                operation_scope(OperationSettings(10)):
            transport=_BoundedKeyRequest();self.addCleanup(transport.close)
            tick[0]+=8;transport(CERTS_URL,timeout=900)
            self.assertEqual(request.return_value.call_args.kwargs['timeout'],2)

    def test_stop_from_certificate_transport_survives_identity_redaction(self):
        self.request.side_effect=OperationStopped('OPERATION_CANCELLED')
        with self.assertRaises(OperationStopped) as caught:self.verify(self.token())
        self.assertEqual(str(caught.exception),'OPERATION_CANCELLED')
        self.request.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
