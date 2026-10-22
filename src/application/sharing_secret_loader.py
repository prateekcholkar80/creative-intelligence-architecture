"""Pinned regional Secret Manager composition; no publication or fallback.

Inputs are trusted server metadata, never tokens or browser settings. The project
number must already be verified as belonging to PROJECT by deployment setup.
This loader neither resolves that association nor refreshes running instances.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
import time

from google.cloud import secretmanager_v1
from google.cloud.secretmanager_v1.services.secret_manager_service.transports.rest import SecretManagerServiceRestTransport
import google_crc32c
from src.application.operation_budget import OperationStopped, check_operation, operation_call

from src.application.sharing_retained_sealer import RetainedSharingSealer
from src.application.sharing_signing_contract import (
    SharingSigningPolicy, _identifier, validate_signing_policy_transition,
)
from src.application.workflow_contract import ContractError, _require
from src.application.workflow_schema import LOCATION
from src.ingestion.setup_workflow_tables import PROJECT

REGIONAL_ENDPOINT = f'secretmanager.{LOCATION}.rep.googleapis.com'


def _regional_client(client) -> bool:
    # The SDK REST transport reports its URL with a scheme; gRPC reports host.
    # Accept only these exact native forms, never arbitrary endpoint rewriting.
    return (isinstance(client, secretmanager_v1.SecretManagerServiceClient)
        and (client.api_endpoint == REGIONAL_ENDPOINT or
             (isinstance(client.transport, SecretManagerServiceRestTransport)
              and client.api_endpoint == 'https://' + REGIONAL_ENDPOINT)))


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class SharingSecretVersion:
    key_id: str
    secret_id: str
    version: int

    def __post_init__(self):
        _identifier(self.key_id)
        _require(type(self.secret_id) is str
                 and re.fullmatch(r'[A-Za-z0-9_-]{1,255}', self.secret_id) is not None,
                 'INVALID_SHARING_SECRET_ID')
        _require(type(self.version) is int and 1 <= self.version < 2**63,
                 'INVALID_SHARING_SECRET_VERSION')


@dataclass(frozen=True, slots=True)
class SharingSecretConfig:
    policy: SharingSigningPolicy
    project_number: str
    versions: tuple[SharingSecretVersion, ...]
    project_id: str = PROJECT
    location: str = LOCATION

    def __post_init__(self):
        _require(isinstance(self.policy, SharingSigningPolicy), 'INVALID_SHARING_SIGNING_POLICY')
        _require(self.project_id == PROJECT and self.location == LOCATION,
                 'INVALID_SHARING_SECRET_SCOPE')
        _require(type(self.project_number) is str
                 and re.fullmatch(r'[1-9][0-9]{0,19}', self.project_number) is not None,
                 'INVALID_SHARING_SECRET_PROJECT')
        _require(type(self.versions) is tuple and len(self.versions) == len(self.policy.keys)
                 and all(isinstance(item, SharingSecretVersion) for item in self.versions)
                 and len({item.key_id for item in self.versions}) == len(self.versions)
                 and {item.key_id for item in self.versions} == {key.key_id for key in self.policy.keys}
                 and len({(item.secret_id, item.version) for item in self.versions}) == len(self.versions),
                 'INVALID_SHARING_SECRET_BINDINGS')

    def name(self, version: SharingSecretVersion) -> str:
        _require(version in self.versions, 'INVALID_SHARING_SECRET_BINDINGS')
        return (f'projects/{self.project_number}/locations/{self.location}/secrets/'
                f'{version.secret_id}/versions/{version.version}')


def validate_secret_config_transition(previous: SharingSecretConfig, proposed: SharingSecretConfig,
                                      *, changed_at: datetime) -> None:
    """Validate a supplied transition, without publishing or rollback protection."""
    _require(isinstance(previous, SharingSecretConfig) and isinstance(proposed, SharingSecretConfig),
             'INVALID_SHARING_SECRET_CONFIG')
    _require(previous.project_number == proposed.project_number, 'SHARING_SECRET_SCOPE_REBOUND')
    validate_signing_policy_transition(previous.policy, proposed.policy, changed_at=changed_at)
    old = {item.key_id: item for item in previous.versions}
    _require(all(item == old[item.key_id] for item in proposed.versions if item.key_id in old),
             'SHARING_SECRET_VERSION_REBOUND')


@dataclass(frozen=True, slots=True)
class SharingSecretLoadSettings:
    rpc_timeout_seconds: int = 5
    total_timeout_seconds: int = 30

    def __post_init__(self):
        _require(type(self.rpc_timeout_seconds) is int and type(self.total_timeout_seconds) is int
                 and 1 <= self.rpc_timeout_seconds <= 20
                 and self.rpc_timeout_seconds <= self.total_timeout_seconds <= 60,
                 'INVALID_SHARING_SECRET_LOAD_SETTINGS')


class SharingSecretLoadError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(f'Sharing secret composition failed: {code}')


def create_regional_secret_client() -> secretmanager_v1.SecretManagerServiceClient:
    """ADC discovery only; no key files, custom endpoint or credential fallback.

    Credential discovery/refresh is not an overall host deadline. The caller
    owns this client and must close its transport when no longer needed.
    """
    try:
        return secretmanager_v1.SecretManagerServiceClient(client_options={'api_endpoint': REGIONAL_ENDPOINT})
    except Exception:
        raise SharingSecretLoadError('SHARING_SECRET_CLIENT_UNAVAILABLE') from None


class SharingSecretLoader:
    def __init__(self, *, client: secretmanager_v1.SecretManagerServiceClient,
                 settings: SharingSecretLoadSettings | None = None):
        _require(_regional_client(client), 'INVALID_SHARING_SECRET_CLIENT')
        self.client = client
        self.settings = SharingSecretLoadSettings() if settings is None else settings
        _require(isinstance(self.settings, SharingSecretLoadSettings), 'INVALID_SHARING_SECRET_LOAD_SETTINGS')

    def _preflight(self, config: SharingSecretConfig) -> None:
        _require(isinstance(config, SharingSecretConfig), 'INVALID_SHARING_SECRET_CONFIG')
        # Also check the client at use; trusted composition must not change it.
        _require(_regional_client(self.client), 'INVALID_SHARING_SECRET_CLIENT')
        active = config.policy.key(config.policy.active_key_id)
        now = _now()
        _require(active.issue_from <= now < active.issue_until, 'SHARING_SIGNING_KEY_NOT_ISSUING')
        for key in config.policy.keys:
            if not key.revoked:
                config.policy.profile(key).check_installed()

    def load(self, config: SharingSecretConfig) -> RetainedSharingSealer:
        """Fresh all-or-nothing composition. Never return partial material/cache.

        One access RPC per nonrevoked key, with no retries. No metadata/list RPC,
        printing, local persistence, provisioning or currently-running swap.
        """
        check_operation()
        deadline = time.monotonic() + self.settings.total_timeout_seconds
        self._preflight(config)
        material = {}
        versions = {item.key_id: item for item in config.versions}
        try:
            for key in config.policy.keys:
                check_operation()
                if key.revoked:
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise SharingSecretLoadError('SHARING_SECRET_LOAD_DEADLINE')
                name = config.name(versions[key.key_id])
                try:
                    response = operation_call(self.client.access_secret_version, request={'name': name}, retry=None,
                        timeout=min(self.settings.rpc_timeout_seconds, remaining))
                except OperationStopped:
                    raise
                except Exception:
                    raise SharingSecretLoadError('SHARING_SECRET_UNAVAILABLE') from None
                if not isinstance(response, secretmanager_v1.AccessSecretVersionResponse) or response.name != name:
                    raise SharingSecretLoadError('SHARING_SECRET_RESPONSE_MISMATCH')
                payload = response.payload
                if not secretmanager_v1.SecretPayload.pb(payload).HasField('data_crc32c'):
                    raise SharingSecretLoadError('SHARING_SECRET_INTEGRITY_FAILED')
                data = payload.data
                if len(data) < 32 or len(data) > 64:
                    raise SharingSecretLoadError('SHARING_SECRET_PAYLOAD_INVALID')
                if not 0 <= payload.data_crc32c <= 2**32 - 1 or google_crc32c.value(data) != payload.data_crc32c:
                    raise SharingSecretLoadError('SHARING_SECRET_INTEGRITY_FAILED')
                material[key.key_id] = data
            if time.monotonic() >= deadline:
                raise SharingSecretLoadError('SHARING_SECRET_LOAD_DEADLINE')
            sealer = RetainedSharingSealer(policy=config.policy, key_material=material)
            self._preflight(config)  # Issuance/profile boundaries may have changed during I/O/composition.
            if time.monotonic() >= deadline:
                raise SharingSecretLoadError('SHARING_SECRET_LOAD_DEADLINE')
            check_operation()
            return sealer
        except (OperationStopped, ContractError, SharingSecretLoadError):
            raise
        except Exception:
            raise SharingSecretLoadError('SHARING_SECRET_LOAD_FAILED') from None
        finally:
            material.clear()  # Remove staging references on failure/success; no zeroization claim.
