"""Pure one-slot control genesis contract; supplied snapshots are not authority.

No authentication, native job observation, retries, bootstrap dispatch or grants.
The global slot is independent of manifest/owner/time. A future trusted executor
must reconcile that exact original job; UNKNOWN or conflict cannot advance slots.
"""
from dataclasses import dataclass
from datetime import datetime,timedelta
import re
from src.application.identity import VerifiedIdentity,IapSettings
from src.application.campaign_acceptance_contract import _utc,_hash
from src.application.campaign_review_storage import SCHEMAS,PublicationControl,row_json,parse_row
from src.application.campaign_permission_storage import TABLE_SCHEMAS
from src.application.workflow_contract import _require,_id
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,LOCATION

VERSION='campaign-control-bootstrap-v1'
MAXIMUM_BYTES_BILLED=768*1024*1024
JOB_TIMEOUT_MS=120000
TABLES=tuple(sorted(set(SCHEMAS)|set(TABLE_SCHEMAS)))
CONTROL_HEAD='campaign_permission_controls'
CONTROL_HISTORY='campaign_permission_control_history'


def bootstrap_job_id():
    namespace=_hash(dict(project=PROJECT,dataset=DATASET,location=LOCATION,
        control_id='campaign-permissions'))
    return 'campaign_control_bootstrap_'+namespace+'_0'


@dataclass(frozen=True,slots=True)
class BootstrapManifest:
    owner_principal_id: str
    expected_audience: str
    accepted_at: datetime
    deadline_at: datetime
    reason_code: str

    def __post_init__(self):
        _require(type(self.owner_principal_id) is str and re.fullmatch(r'iap-v1:[0-9a-f]{64}',self.owner_principal_id)
            is not None,'INVALID_CAMPAIGN_BOOTSTRAP_OWNER')
        IapSettings(self.expected_audience);_id(self.reason_code)
        start,end=_utc(self.accepted_at),_utc(self.deadline_at)
        _require(timedelta(seconds=1)<=end-start<=timedelta(minutes=5),'INVALID_CAMPAIGN_BOOTSTRAP_WINDOW')
        object.__setattr__(self,'accepted_at',start);object.__setattr__(self,'deadline_at',end)

    @property
    def sha256(self):
        return _hash(dict(version=VERSION,project=PROJECT,dataset=DATASET,location=LOCATION,
            owner_principal_id=self.owner_principal_id,expected_audience=self.expected_audience,
            accepted_at=self.accepted_at.isoformat(),deadline_at=self.deadline_at.isoformat(),
            reason_code=self.reason_code,control_id='campaign-permissions',
            profile=dict(maximum_bytes_billed=MAXIMUM_BYTES_BILLED,job_timeout_ms=JOB_TIMEOUT_MS)))

    @property
    def control(self):
        return PublicationControl(1,None,self.sha256,self.owner_principal_id,self.accepted_at)

    def authorize(self,identity,checked_at):
        """Validate a supplied verified actor, not a JWT or an actual live grant."""
        now=_utc(checked_at)
        _require(isinstance(identity,VerifiedIdentity)
            and identity.principal.principal_id==self.owner_principal_id
            and identity.audience==self.expected_audience,'CAMPAIGN_BOOTSTRAP_OWNER_DENIED')
        _require(self.accepted_at<=now<self.deadline_at
            and identity.issued_at<=now.timestamp()<identity.expires_at
            and identity.expires_at>=self.deadline_at.timestamp(),'CAMPAIGN_BOOTSTRAP_WINDOW_DENIED')


@dataclass(frozen=True,slots=True)
class BootstrapSnapshot:
    # Complete frozen capture of all 13 tables; missing tables cannot imply empty.
    tables: tuple[tuple[str,tuple[str,...]],...]

    def __post_init__(self):
        _require(type(self.tables) is tuple and len(self.tables)==len(TABLES)
            and all(type(pair) is tuple and len(pair)==2 for pair in self.tables)
            and tuple(pair[0] for pair in self.tables)==TABLES,'CAMPAIGN_BOOTSTRAP_SNAPSHOT_SCOPE')
        _require(all(type(rows) is tuple and len(rows)<=2 and all(type(row) is str for row in rows)
            for _,rows in self.tables),'CAMPAIGN_BOOTSTRAP_SNAPSHOT_OVERFLOW')
        _require(sum(len(row.encode()) for _,rows in self.tables for row in rows)<=65536,
            'CAMPAIGN_BOOTSTRAP_SNAPSHOT_OVERFLOW')
        # Any third row is overflow: callers must positively detect truncation.
        for _,rows in self.tables:
            for row in rows:
                _require(row_json(parse_row(row))==row,'CAMPAIGN_BOOTSTRAP_NONCANONICAL_ROW')

    def state_for(self,manifest):
        _require(isinstance(manifest,BootstrapManifest),'INVALID_CAMPAIGN_BOOTSTRAP_MANIFEST')
        rows=dict(self.tables)
        if not any(rows.values()):return 'EMPTY'
        expected=row_json(manifest.control.row())
        _require(all(values==((expected,) if name in (CONTROL_HEAD,CONTROL_HISTORY) else ())
            for name,values in self.tables),'CAMPAIGN_BOOTSTRAP_EXISTING_STATE_DENIED')
        return 'EXACT_GENESIS'
