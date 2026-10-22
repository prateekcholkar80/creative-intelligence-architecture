"""Pure disabled one-slot control genesis SQL; no query submission or retries.

The immutable control history is the genesis audit/receipt and pins the complete
trusted manifest hash. It approves no claims and assigns no reviewer authority.
"""
from dataclasses import dataclass
import json
from google.cloud import bigquery
from src.application.campaign_bootstrap_contract import (
    BootstrapManifest,BootstrapSnapshot,TABLES,CONTROL_HEAD,CONTROL_HISTORY,bootstrap_job_id,
    MAXIMUM_BYTES_BILLED,JOB_TIMEOUT_MS,
)
from src.application.campaign_review_storage import SCHEMAS,row_json
from src.application.campaign_permission_publication import _expr
from src.application.workflow_contract import _require,canonical_json_bytes
from src.ingestion.configure_campaign_acceptance import PROJECT,DATASET,settings


@dataclass(frozen=True,slots=True)
class BootstrapPlan:
    manifest: BootstrapManifest
    native_job_id: str
    sql: str
    config_json: str
    execution_enabled: bool=False

    def config(self):return bigquery.QueryJobConfig.from_api_repr(json.loads(self.config_json))


def bootstrap_spec(manifest):
    _require(isinstance(manifest,BootstrapManifest),'INVALID_CAMPAIGN_BOOTSTRAP_MANIFEST')
    parameters=[bigquery.ScalarQueryParameter('control_json','STRING',row_json(manifest.control.row())),
        bigquery.ScalarQueryParameter('manifest_sha256','STRING',manifest.sha256),
        bigquery.ScalarQueryParameter('accepted_at','TIMESTAMP',manifest.accepted_at),
        bigquery.ScalarQueryParameter('deadline_at','TIMESTAMP',manifest.deadline_at)]
    def table(name):return f'`{PROJECT}.{DATASET}.{name}`'
    def count(name):return f'(SELECT COUNT(*) FROM {table(name)})'
    head,history=table(CONTROL_HEAD),table(CONTROL_HISTORY)
    sql=[f"CREATE TEMP TABLE proposed_control AS SELECT {_expr(SCHEMAS[CONTROL_HEAD],'@control_json')};",
        'CREATE TEMP TABLE bootstrap_result(state STRING,manifest_sha256 STRING,control_sha256 STRING,observed_at TIMESTAMP);',
        'BEGIN TRANSACTION;', 'BEGIN',
        "ASSERT @accepted_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@deadline_at AS 'CAMPAIGN_BOOTSTRAP_WINDOW_DENIED';",
        f'IF {count(CONTROL_HEAD)}=0 AND {count(CONTROL_HISTORY)}=0 THEN']
    # Global emptiness is deliberate: orphan/partial business or review rows deny.
    for name in TABLES:
        sql.append(f"ASSERT {count(name)}=0 AS 'CAMPAIGN_BOOTSTRAP_EXISTING_STATE_DENIED';")
    sql.extend([f'INSERT INTO {head} SELECT * FROM proposed_control;',
        f'INSERT INTO {history} SELECT * FROM proposed_control;',
        "INSERT bootstrap_result SELECT 'APPLIED',@manifest_sha256,control_sha256,CURRENT_TIMESTAMP() FROM proposed_control;",
        'ELSE'])
    for name in TABLES:
        if name not in (CONTROL_HEAD,CONTROL_HISTORY):
            sql.append(f"ASSERT {count(name)}=0 AS 'CAMPAIGN_BOOTSTRAP_EXISTING_STATE_DENIED';")
        else:
            target=table(name)
            sql.append(f"ASSERT {count(name)}=1 AND NOT EXISTS(SELECT * FROM {target} EXCEPT DISTINCT SELECT * FROM proposed_control) AND NOT EXISTS(SELECT * FROM proposed_control EXCEPT DISTINCT SELECT * FROM {target}) AS 'CAMPAIGN_BOOTSTRAP_EXACT_GENESIS_REQUIRED';")
    sql.extend(["INSERT bootstrap_result SELECT 'REUSED',@manifest_sha256,control_sha256,CURRENT_TIMESTAMP() FROM proposed_control;",
        'END IF;', 'COMMIT TRANSACTION;', 'EXCEPTION WHEN ERROR THEN', 'ROLLBACK TRANSACTION;',
        'RAISE;', 'END;', 'SELECT * FROM bootstrap_result;'])
    script='\n'.join(sql)
    _require(script.count(';')<=64 and len(script.encode())<=128*1024,'CAMPAIGN_BOOTSTRAP_SQL_OVERFLOW')
    config=settings(parameters);config.maximum_bytes_billed=MAXIMUM_BYTES_BILLED;config.job_timeout_ms=JOB_TIMEOUT_MS
    return BootstrapPlan(manifest,bootstrap_job_id(),script,canonical_json_bytes(config.to_api_repr()).decode())


def prepare_bootstrap(manifest,identity,snapshot,*,checked_at):
    _require(isinstance(manifest,BootstrapManifest) and isinstance(snapshot,BootstrapSnapshot),
        'INVALID_CAMPAIGN_BOOTSTRAP_CAPTURE')
    manifest.authorize(identity,checked_at);snapshot.state_for(manifest)
    return bootstrap_spec(manifest)


def validate_bootstrap_result(plan,rows):
    _require(isinstance(plan,BootstrapPlan) and type(rows) is tuple and len(rows)==1,
        'INVALID_CAMPAIGN_BOOTSTRAP_RESULT')
    from src.application.campaign_acceptance_contract import _utc
    row=dict(rows[0]);manifest=plan.manifest
    _require(set(row)=={'state','manifest_sha256','control_sha256','observed_at'}
        and row['state'] in {'APPLIED','REUSED'} and row['manifest_sha256']==manifest.sha256
        and row['control_sha256']==manifest.control.sha256,'CAMPAIGN_BOOTSTRAP_RESULT_CONFLICT')
    _require(manifest.accepted_at<=_utc(row['observed_at'])<manifest.deadline_at,
        'CAMPAIGN_BOOTSTRAP_RESULT_WINDOW')
    return row['state']
