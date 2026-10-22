"""4.2 Critical/Must have/shared pilot: strict metadata codec, never secret bytes.

Decoding validates content and provenance shape, not publisher authorization.
Only the fresh, scoped BigQuery reader may supply authoritative observations.
"""
from __future__ import annotations

from datetime import datetime
import json

from google.cloud import bigquery

from src.application.audit_contract import _opaque
from src.application.sharing_coordinator_contract import _utc
from src.application.sharing_publication_contract import (
    POLICY_ID, SigningKeyHistory, SigningPublication, SigningPublicationStatus,
    secret_config_digest, secret_config_payload,
)
from src.application.sharing_publication_read_contract import (
    MAX_READ_HISTORY, MAX_READ_PUBLICATIONS, SigningCheckpointRecord, SigningHeadRecord,
    SigningHistoryRecord, SigningReadLimits, SigningReadObservation, SigningReadRequest, SigningReadSnapshot,
    validate_publication_read,
)
from src.application.sharing_publication_schema import STORAGE_VERSION, TABLES
from src.application.sharing_query import SharingQuerySettings
from src.application.sharing_secret_loader import SharingSecretConfig, SharingSecretVersion
from src.application.sharing_signing_contract import (
    MAX_RETAINED_KEYS, SharingSealProfile, SharingSigningKey, SharingSigningPolicy,
)
from src.application.workflow_contract import (
    MAX_JSON_BYTES, ContractError, _require, canonical_json_bytes,
)


def normalize(value):
    """Convert real SDK rows recursively without accepting arbitrary object serializers."""
    if isinstance(value, bigquery.Row):
        # Row.items() deep-copies nested Row objects in SDK 3.46.1 and can recurse.
        return {key: normalize(value[key]) for key in value.keys()}
    if type(value) is dict:
        return {key: normalize(child) for key, child in value.items()}
    if type(value) in (list, tuple):
        return [normalize(child) for child in value]
    return value


def _shape(value: dict, keys) -> None:
    _require(type(value) is dict and set(value) == set(keys), 'INVALID_SIGNING_STORAGE_SHAPE')


def _fields(row: dict, fields) -> None:
    _shape(row, [field.name for field in fields])
    for field in fields:
        value = row[field.name]
        if value is None and field.mode == 'NULLABLE':
            continue
        if field.field_type == 'RECORD':
            _fields(value, field.fields)
        else:
            kind = {'STRING': str, 'INTEGER': int, 'BOOLEAN': bool, 'TIMESTAMP': datetime}[field.field_type]
            _require(type(value) is kind or (kind is datetime and isinstance(value, datetime)),
                     'INVALID_SIGNING_STORAGE_TYPE')
            if kind is int:
                _require(-(2**63) <= value < 2**63, 'INVALID_SIGNING_STORAGE_TYPE')
            if kind is datetime:
                _utc(value)


def _row(row: dict, table: str) -> None:
    _fields(row, TABLES[table].schema)
    _require(row['schema_version'] == STORAGE_VERSION and row['policy_id'] == POLICY_ID,
             'INVALID_SIGNING_STORAGE_SCOPE')
    for name in ('publisher_id', 'writer_id', 'command_id', 'instance_id', 'consumer_id'):
        if name in row:
            _opaque(row[name])


def encode_config(config: SharingSecretConfig) -> str:
    return canonical_json_bytes(secret_config_payload(config)).decode('utf-8')


def _pairs(items):
    output = {}
    for key, value in items:
        _require(key not in output, 'DUPLICATE_SIGNING_CONFIG_KEY')
        output[key] = value
    return output


def _invalid_constant(_value):
    raise ContractError('INVALID_SIGNING_CONFIG_JSON')


def _timestamp(value: str) -> datetime:
    _require(type(value) is str, 'INVALID_SIGNING_CONFIG_TIMESTAMP')
    try:
        instant = _utc(datetime.fromisoformat(value))
    except (ValueError, TypeError):
        raise ContractError('INVALID_SIGNING_CONFIG_TIMESTAMP') from None
    _require(instant.isoformat() == value, 'NONCANONICAL_SIGNING_CONFIG_TIMESTAMP')
    return instant


def _profile(value: dict) -> SharingSealProfile:
    _shape(value, ('profile_id', 'settings', 'fingerprint', 'handler_version'))
    _shape(value['settings'], ('maximum_bytes_billed', 'job_timeout_ms', 'statement_timeout_ms'))
    return SharingSealProfile(value['profile_id'], SharingQuerySettings(**value['settings']),
                              value['fingerprint'], value['handler_version'])


def decode_config(raw: str) -> SharingSecretConfig:
    _require(type(raw) is str, 'INVALID_SIGNING_CONFIG_JSON')
    try:
        encoded = raw.encode('utf-8')
        _require(0 < len(encoded) <= MAX_JSON_BYTES, 'INVALID_SIGNING_CONFIG_SIZE')
        value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_invalid_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise ContractError('INVALID_SIGNING_CONFIG_JSON') from None
    _shape(value, ('project_id', 'project_number', 'location', 'policy_version', 'active_key_id',
                   'keys', 'profiles', 'versions'))
    for name in ('keys', 'profiles', 'versions'):
        _require(type(value[name]) is list and 1 <= len(value[name]) <= MAX_RETAINED_KEYS,
                 'INVALID_SIGNING_CONFIG_COLLECTION')
    keys = []
    for key in value['keys']:
        _shape(key, ('key_id', 'profile_id', 'issue_from', 'issue_until', 'retain_until', 'revoked'))
        keys.append(SharingSigningKey(key['key_id'], key['profile_id'], _timestamp(key['issue_from']),
                    _timestamp(key['issue_until']), _timestamp(key['retain_until']), key['revoked']))
    versions = []
    for version in value['versions']:
        _shape(version, ('key_id', 'secret_id', 'version'))
        versions.append(SharingSecretVersion(**version))
    config = SharingSecretConfig(SharingSigningPolicy(value['active_key_id'], tuple(keys),
        tuple(_profile(p) for p in value['profiles']), value['policy_version']), value['project_number'],
        tuple(versions), value['project_id'], value['location'])
    _require(encode_config(config).encode('utf-8') == encoded, 'NONCANONICAL_SIGNING_CONFIG_JSON')
    return config


def decode_history(row: dict) -> SigningHistoryRecord:
    _row(row, 'signing_key_history')
    record = SigningKeyHistory(
        SharingSigningKey(row['key_id'], row['profile_id'], row['issue_from'], row['issue_until'],
                          row['retain_until'], row['revoked']),
        SharingSecretVersion(row['key_id'], row['secret_id'], row['secret_version']), _profile(row['profile']))
    return SigningHistoryRecord(row['publication_revision'], row['publication_digest'], record)


def decode_publication(row: dict, history: tuple[SigningHistoryRecord, ...]) -> SigningPublication:
    _row(row, 'signing_publications')
    _require(type(history) is tuple and len(history) == row['history_count']
             and all(isinstance(h, SigningHistoryRecord) and h.publication_revision == row['revision']
                     and h.publication_digest == row['publication_digest'] for h in history),
             'SIGNING_STORAGE_HISTORY_MISMATCH')
    config = decode_config(row['config_canonical_json'])
    try:
        status = SigningPublicationStatus(row['status'])
    except ValueError:
        raise ContractError('INVALID_SIGNING_STORAGE_STATUS') from None
    publication = SigningPublication(row['revision'], row['parent_digest'], config, status,
        row['published_at'], tuple(sorted((h.record for h in history), key=lambda h: h.key.key_id)),
        row['publication_version'], row['policy_id'])
    _require(secret_config_digest(config) == row['config_digest'] and publication.digest == row['publication_digest'],
             'SIGNING_STORAGE_DIGEST_MISMATCH')
    _require(_utc(row['recorded_at']) >= publication.published_at, 'INVALID_SIGNING_STORAGE_TIME')
    return publication


def encode_publication(publication: SigningPublication, *, publisher_id: str, command_id: str,
                       recorded_at: datetime) -> tuple[dict, tuple[dict, ...]]:
    """Pure row proposals for fixtures/future writers; never authority or a write."""
    _require(isinstance(publication, SigningPublication), 'INVALID_SIGNING_PUBLICATION')
    base = dict(schema_version=STORAGE_VERSION, policy_id=POLICY_ID)
    history = []
    for item in publication.history:
        profile = item.profile
        history.append(base | dict(publication_revision=publication.revision, publication_digest=publication.digest,
            key_id=item.key.key_id, profile_id=item.key.profile_id, issue_from=item.key.issue_from,
            issue_until=item.key.issue_until, retain_until=item.key.retain_until, revoked=item.key.revoked,
            secret_id=item.secret.secret_id, secret_version=item.secret.version,
            profile=dict(profile_id=profile.profile_id,
                settings=dict(maximum_bytes_billed=profile.settings.maximum_bytes_billed,
                              job_timeout_ms=profile.settings.job_timeout_ms,
                              statement_timeout_ms=profile.settings.statement_timeout_ms),
                fingerprint=profile.fingerprint, handler_version=profile.handler_version)))
    row = base | dict(publication_version=publication.version, revision=publication.revision,
        parent_digest=publication.parent_digest, publication_digest=publication.digest,
        config_digest=secret_config_digest(publication.config), status=publication.status.value,
        published_at=publication.published_at, config_canonical_json=encode_config(publication.config),
        history_count=len(history), publisher_id=publisher_id, command_id=command_id, recorded_at=_utc(recorded_at))
    decode_publication(row, tuple(decode_history(h) for h in history))
    return row, tuple(history)


RESULT_KEYS = {'observed_at', 'consumer_id', 'project_number', 'instance_id', 'generation', 'base_revision',
               'base_digest', 'heads', 'checkpoints', 'publications', 'history', 'head_count', 'checkpoint_count',
               'publication_count', 'history_count', 'unheaded_publication_count'}


def decode_read_result(request: SigningReadRequest, rows: list[dict], *, now: datetime,
                       limits: SigningReadLimits = SigningReadLimits()) -> SigningReadObservation:
    _require(isinstance(request, SigningReadRequest) and type(rows) is list and len(rows) == 1,
             'INVALID_SIGNING_STORAGE_RESULT')
    row = rows[0]
    _shape(row, RESULT_KEYS)
    ticket = request.ticket
    expected = dict(consumer_id=request.consumer_id, project_number=request.project_number,
                    instance_id=ticket.instance_id, generation=ticket.generation,
                    base_revision=ticket.base_revision, base_digest=ticket.base_digest)
    _require(all(type(row[name]) is type(value) and row[name] == value for name, value in expected.items()),
             'SIGNING_STORAGE_TICKET_MISMATCH')
    # Check full server counts before allocating/decoding potentially truncated collections.
    for name, count, lower, upper in (('heads', 'head_count', 1, 1), ('checkpoints', 'checkpoint_count', 0, 1),
            ('publications', 'publication_count', 1, MAX_READ_PUBLICATIONS),
            ('history', 'history_count', 1, MAX_READ_HISTORY)):
        _require(type(row[count]) is int and lower <= row[count] <= upper
                 and type(row[name]) is list and len(row[name]) == row[count], 'INVALID_SIGNING_READ_COUNTS')
    _require(type(row['unheaded_publication_count']) is int and row['unheaded_publication_count'] == 0,
             'SIGNING_READ_UNHEADED_PUBLICATION')
    observed = _utc(row['observed_at'])
    heads, checkpoints = [], []
    for item in row['heads']:
        _row(item, 'signing_heads')
        heads.append(SigningHeadRecord(item['revision'], item['publication_digest'], item['published_at'],
                                       item['control_revision']))
    for item in row['checkpoints']:
        _row(item, 'signing_checkpoints')
        checkpoints.append(SigningCheckpointRecord(item['consumer_id'], item['checkpoint_revision'],
            item['publication_revision'], item['publication_digest'], item['updated_at']))
    history = tuple(decode_history(item) for item in row['history'])
    publications = []
    for item in row['publications']:
        _row(item, 'signing_publications')
        _require(_utc(item['recorded_at']) <= observed, 'INVALID_SIGNING_STORAGE_TIME')
        publications.append(decode_publication(item, tuple(h for h in history
                                                if h.publication_revision == item['revision'])))
    snapshot = SigningReadSnapshot(request.consumer_id, ticket, observed, tuple(heads), tuple(checkpoints),
        tuple(publications), history, row['head_count'], row['checkpoint_count'], row['publication_count'],
        row['history_count'], row['unheaded_publication_count'])
    return validate_publication_read(request, snapshot, now=now, limits=limits)
