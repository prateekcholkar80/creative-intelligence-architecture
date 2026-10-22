"""Server-only IAP assertion verification; not yet wired to the local chat.

Configure CI_IAP_EXPECTED_AUDIENCE only after identifying the protected service.
No development identity fallback, email-derived roles, ADC login or JWT logging.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import os
import re
import time

from google.auth import jwt
from google.auth.transport.requests import Request
from google.oauth2 import id_token

from src.application.access_policy import Principal
from src.application.operation_budget import OperationStopped, operation_call

ISSUER = 'https://cloud.google.com/iap'
CERTS_URL = 'https://www.gstatic.com/iap/verify/public_key'
ASSERTION_HEADER = 'x-goog-iap-jwt-assertion'
CLOCK_SKEW_SECONDS = 30
MAX_LIFETIME_SECONDS = 660


class IdentityError(PermissionError):
    def __init__(self, code: str):
        self.code = code
        super().__init__('A valid application identity is required.')


def _subject(value: str) -> None:
    if (not isinstance(value, str) or not 1 <= len(value) <= 256
            or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value)):
        raise IdentityError('INVALID_ASSERTION')


def principal_for_subject(issuer: str, subject: str) -> Principal:
    """Deterministic binding helper; does not verify or authenticate a subject."""
    if issuer != ISSUER:
        raise IdentityError('INVALID_ASSERTION')
    _subject(subject)
    try:
        digest = hashlib.sha256(f'{issuer}\0{subject}'.encode('utf-8')).hexdigest()
    except UnicodeError:
        raise IdentityError('INVALID_ASSERTION') from None
    return Principal(f'iap-v1:{digest}')


@dataclass(frozen=True, slots=True)
class IapSettings:
    expected_audience: str

    def __post_init__(self):
        if (not isinstance(self.expected_audience, str) or re.fullmatch(
            r'/projects/[1-9][0-9]*/locations/asia-south1/services/[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?',
                self.expected_audience) is None):
            raise ValueError('Configure the exact protected Cloud Run IAP audience in asia-south1')

    @classmethod
    def from_environment(cls) -> IapSettings:
        return cls(os.environ.get('CI_IAP_EXPECTED_AUDIENCE', ''))


@dataclass(frozen=True, slots=True)
class VerifiedIdentity:
    """Trusted verifier result, not an object to deserialize from browser input."""
    principal: Principal
    issuer: str
    subject: str
    audience: str
    issued_at: int
    expires_at: int

    def __post_init__(self):
        if self.principal != principal_for_subject(self.issuer, self.subject):
            raise IdentityError('INVALID_ASSERTION')
        IapSettings(self.audience)
        if (type(self.issued_at) is not int or type(self.expires_at) is not int
                or not 0 < self.expires_at - self.issued_at <= MAX_LIFETIME_SECONDS):
            raise IdentityError('INVALID_ASSERTION')


def ensure_fresh(identity: VerifiedIdentity) -> None:
    if not isinstance(identity, VerifiedIdentity):
        raise IdentityError('AUTHENTICATION_REQUIRED')
    now = time.time()
    if identity.issued_at > now + CLOCK_SKEW_SECONDS or now >= identity.expires_at + CLOCK_SKEW_SECONDS:
        raise IdentityError('ASSERTION_EXPIRED_OR_EARLY')


class _BoundedKeyRequest:
    def __init__(self):
        # Local import keeps identity independent of the cloud composition module.
        from src.application.operation_transport import OperationSession
        self.session = OperationSession(timeout_seconds=5)
        self.transport = Request(self.session)

    def __call__(self, url, method='GET', body=None, headers=None, **kwargs):
        return operation_call(self.transport, url=url, method=method, body=body, headers=headers, timeout=5)

    def close(self):
        self.session.close()


class IapIdentityAdapter:
    def __init__(self, settings: IapSettings):
        if not isinstance(settings, IapSettings):
            raise ValueError('IAP settings must be configured by the server')
        self.settings = settings

    def authenticate(self, headers: Mapping[str, str]) -> VerifiedIdentity:
        if not isinstance(headers, Mapping):
            raise IdentityError('AUTHENTICATION_REQUIRED')
        assertions = [value for key, value in headers.items()
                      if isinstance(key, str) and key.lower() == ASSERTION_HEADER]
        if len(assertions) != 1:
            raise IdentityError('AUTHENTICATION_REQUIRED')
        token = assertions[0]
        if (not isinstance(token, str) or not 1 <= len(token) <= 16384
                or re.fullmatch(r'[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', token) is None):
            raise IdentityError('INVALID_ASSERTION')
        try:
            header = jwt.decode_header(token)  # Untrusted header used only to reject unsupported formats.
            if (header.get('alg') != 'ES256' or not isinstance(header.get('kid'), str)
                    or not 1 <= len(header['kid']) <= 256 or 'crit' in header or 'b64' in header
                    or 'jku' in header or 'x5u' in header):
                raise IdentityError('INVALID_ASSERTION')
            _subject(header['kid'])
            request = _BoundedKeyRequest()
            try:
                claims = id_token.verify_token(token, request,
                    audience=self.settings.expected_audience, certs_url=CERTS_URL,
                    clock_skew_in_seconds=CLOCK_SKEW_SECONDS)
            finally:
                request.close()
            if claims.get('iss') != ISSUER or claims.get('aud') != self.settings.expected_audience:
                raise IdentityError('INVALID_ASSERTION')
            identity = VerifiedIdentity(principal_for_subject(claims['iss'], claims['sub']),
                claims['iss'], claims['sub'], claims['aud'], claims['iat'], claims['exp'])
            ensure_fresh(identity)
            if 'nbf' in claims and (type(claims['nbf']) is not int
                    or claims['nbf'] > time.time() + CLOCK_SKEW_SECONDS or claims['nbf'] > claims['exp']):
                raise IdentityError('INVALID_ASSERTION')
            return identity
        except (OperationStopped, IdentityError):
            raise
        except Exception:
            # Never propagate tokens, claims, email addresses or transport response details.
            raise IdentityError('ASSERTION_VERIFICATION_FAILED') from None
