"""Exact-version BigQuery clarification configuration; no approval or activation."""
from dataclasses import dataclass
from datetime import datetime,timedelta,timezone
from itertools import islice
from uuid import uuid4
import json
from google.cloud import bigquery
from src.application.campaign_evidence_gaps import validate_policy,canonical
from src.application.campaign_acceptance_contract import _hash,_utc
from src.application.workflow_contract import _require,_id,_digest
from src.application.sharing_jobs import matches_sharing_job
from src.application.operation_budget import operation_call,operation_iter
from src.application.control_io import ControlIO,ControlPlan,query_config
from src.ingestion.setup_workflow_tables import PROJECT,LOCATION,validate_table

DATASET=PROJECT+'.ci_brand_memory'
VERSIONS='campaign_clarification_policy_versions'
REVOCATIONS='campaign_clarification_policy_revocations'
VERSION='campaign-clarification-policy-storage-v1'


def table_metadata(name):
    _require(name in {VERSIONS,REVOCATIONS},'CLARIFICATION_POLICY_TABLE')
    fields=[bigquery.SchemaField(k,'STRING',mode='REQUIRED') for k in ('schema_version','policy_id','version','policy_sha256')]
    if name==VERSIONS:
        fields += [bigquery.SchemaField('policy_canonical_json','STRING',mode='REQUIRED'),
            bigquery.SchemaField('recorded_at','TIMESTAMP',mode='REQUIRED')]
    else:
        fields += [bigquery.SchemaField('revocation_id','STRING',mode='REQUIRED'),
            bigquery.SchemaField('revoked_at','TIMESTAMP',mode='REQUIRED')]
    table=bigquery.Table(DATASET+'.'+name,schema=fields);table.clustering_fields=['policy_id','version']
    table.description=VERSION+': immutable configuration; no business approval or serving grant'
    return table


@dataclass(frozen=True,slots=True)
class ClarificationPolicyRequest:
    policy_id: str
    version: str
    policy_sha256: str

    def __post_init__(self):
        _id(self.policy_id);_id(self.version);_digest(self.policy_sha256)


@dataclass(frozen=True,slots=True)
class ClarificationPolicyRead:
    request: ClarificationPolicyRequest
    policy_canonical_json: str
    record_sha256: str
    native_job_id: str
    observed_at: datetime
    valid_until: datetime

    @property
    def approval_verified(self):return False


SQL=f'''SELECT CURRENT_TIMESTAMP() observed_at,
 ARRAY(SELECT IF(BYTE_LENGTH(TO_JSON_STRING(t))<=131072,TO_JSON_STRING(t),NULL)
 FROM `{DATASET}.{VERSIONS}` t WHERE policy_id=@policy_id AND version=@version LIMIT 2) policies,
 (SELECT COUNT(*) FROM `{DATASET}.{VERSIONS}` WHERE policy_id=@policy_id AND version=@version) policy_count,
 (SELECT COUNT(*) FROM `{DATASET}.{REVOCATIONS}` WHERE policy_id=@policy_id AND version=@version) revocation_count'''


def decode_policy(row,request,*,native_job_id,started,ended):
    _require(type(request) is ClarificationPolicyRequest and type(row) is dict
        and set(row)=={'observed_at','policies','policy_count','revocation_count'},'CLARIFICATION_POLICY_RESULT')
    _require(type(row['policy_count']) is int and row['policy_count']==1
        and type(row['policies']) is list and len(row['policies'])==1
        and type(row['revocation_count']) is int and row['revocation_count']==0,'CLARIFICATION_POLICY_MISSING_DUPLICATE_OR_REVOKED')
    _require(type(row['policies'][0]) is str and len(row['policies'][0].encode())<=131072,'CLARIFICATION_POLICY_SIZE')
    value=json.loads(row['policies'][0])
    _require(type(value) is dict and set(value)=={f.name for f in table_metadata(VERSIONS).schema}
        and value['schema_version']==VERSION and value['policy_id']==request.policy_id
        and value['version']==request.version and value['policy_sha256']==request.policy_sha256,'CLARIFICATION_POLICY_PIN_CHANGED')
    policy=validate_policy(value['policy_canonical_json'])
    _require(policy['policy_id']==request.policy_id and policy['version']==request.version
        and _hash(policy)==request.policy_sha256,'CLARIFICATION_POLICY_CONTENT_CHANGED')
    observed=_utc(row['observed_at']);written=_utc(datetime.fromisoformat(value['recorded_at'].replace('Z','+00:00')))
    _require(_utc(started)<=observed<=_utc(ended)<_utc(started)+timedelta(seconds=60)
        and written<=observed,'CLARIFICATION_POLICY_EXPIRED')
    _id(native_job_id)
    return ClarificationPolicyRead(request,value['policy_canonical_json'],_hash(value),native_job_id,observed,
        _utc(started)+timedelta(seconds=60))


def _preflight(client):
    _require(client.project==PROJECT and client.location==LOCATION and client.default_query_job_config is None,
        'CLARIFICATION_POLICY_CLIENT')
    for name in (VERSIONS,REVOCATIONS):
        expected=table_metadata(name)
        validate_table(operation_call(client.get_table,expected.reference,retry=None,timeout=20),expected)


def read_policy(client,request,*,clock=lambda:datetime.now(timezone.utc)):
    _require(type(request) is ClarificationPolicyRequest,'CLARIFICATION_POLICY_REQUEST')
    _preflight(client);started=_utc(clock())
    params=[bigquery.ScalarQueryParameter(k,'STRING',getattr(request,k)) for k in ('policy_id','version')]
    config=query_config(params);native_id='clarification_policy_read_'+uuid4().hex
    job=operation_call(client.query,SQL,job_config=config,job_id=native_id,location=LOCATION,retry=None,job_retry=None,timeout=20)
    operation_call(job.result,max_results=0,retry=None,job_retry=None,timeout=45)
    native=operation_call(client.get_job,native_id,project=PROJECT,location=LOCATION,retry=None,timeout=20)
    _require(matches_sharing_job(native,job_id=native_id,sql=SQL,config=config)
        and native.state=='DONE' and native.error_result is None,'CLARIFICATION_POLICY_NATIVE_MISMATCH')
    rows=list(islice(operation_iter(operation_call(native.result,max_results=2,page_size=2,retry=None,job_retry=None,timeout=20)),2))
    _require(len(rows)==1,'CLARIFICATION_POLICY_RESULT')
    return decode_policy(dict(rows[0]),request,native_job_id=native_id,started=started,ended=clock())


def draft_plan(policy_json):
    """Explicit ingestion proposal only. Cannot approve or revoke any policy."""
    policy=validate_policy(policy_json);_require(policy['status']=='DRAFT','CLARIFICATION_DRAFT_ONLY')
    digest=_hash(policy);request=ClarificationPolicyRequest(policy['policy_id'],policy['version'],digest)
    params=[bigquery.ScalarQueryParameter(k,'STRING',v) for k,v in dict(policy_id=request.policy_id,
        version=request.version,policy_sha256=digest,policy_json=policy_json).items()]
    sql=f'''BEGIN
BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `{DATASET}.{REVOCATIONS}` WHERE policy_id=@policy_id AND version=@version)=0 AS 'POLICY_REVOKED';
IF EXISTS(SELECT 1 FROM `{DATASET}.{VERSIONS}` WHERE policy_id=@policy_id AND version=@version) THEN
 ASSERT (SELECT COUNT(*) FROM `{DATASET}.{VERSIONS}` WHERE policy_id=@policy_id AND version=@version)=1
 AND (SELECT COUNT(*) FROM `{DATASET}.{VERSIONS}` WHERE policy_id=@policy_id AND version=@version
 AND schema_version='{VERSION}' AND policy_sha256=@policy_sha256 AND policy_canonical_json=@policy_json)=1 AS 'POLICY_VERSION_CONFLICT';
ELSE
 INSERT INTO `{DATASET}.{VERSIONS}` (schema_version,policy_id,version,policy_sha256,policy_canonical_json,recorded_at)
 VALUES('{VERSION}',@policy_id,@version,@policy_sha256,@policy_json,CURRENT_TIMESTAMP());
END IF;
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE;
END;
SELECT 'DRAFT_STORED' state,@policy_sha256 policy_sha256;'''
    # Single native slot per exact ID/version, independent of supplied content.
    job_id='clarification_policy_draft_'+_hash([request.policy_id,request.version])
    return ControlPlan('explicit-draft-ingestion',request.policy_id,request.version,job_id,sql,
        json.dumps(query_config(params).to_api_repr(),sort_keys=True),'CLARIFICATION_POLICY_DRAFT',policy_json,'',DATASET+'.'+VERSIONS)


def publish_draft(client,policy_json,*,apply=False):
    _require(type(apply) is bool and apply,'CLARIFICATION_DRAFT_WRITES_DISABLED')
    _preflight(client)
    return ControlIO(client).submit_once(draft_plan(policy_json))
