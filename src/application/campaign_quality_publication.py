"""Disabled measurement publication plans sharing the permission-control fence.

A plan never approves a policy or releases a response. Supplied capture/identity
must come from the protected adapter; fixtures do not authenticate operators.
"""
from dataclasses import dataclass
from datetime import datetime
import json
from google.cloud import bigquery
from src.application.campaign_quality_qualification import BENCHMARKS, REPORTS, REVOCATIONS, MAX_DOCUMENT_BYTES, qualify_documents
from src.application.campaign_operator_policy_publication import OperatorPublisherManifest
from src.application.campaign_permission_publication import Rows, PublicationPlan, _expr, PLAN_TTL, PUBLICATION_BYTES, PUBLICATION_TIMEOUT_MS
from src.application.campaign_review_storage import PublicationControl, decode_control_state, SCHEMAS, STORAGE_VERSION, row_json
from src.application.campaign_acceptance_contract import _hash, _utc
from src.application.workflow_contract import _require, _digest
from src.ingestion.configure_campaign_acceptance import PROJECT, DATASET, settings

CONTROL='campaign_permission_controls'
HISTORY='campaign_permission_control_history'
AUDIT='campaign_permission_publication_audits'


@dataclass(frozen=True,slots=True)
class MeasurementPublisherManifest:
    capability: OperatorPublisherManifest
    workspace_id: str
    review_sha256: str
    evaluator_sha256: str

    def __post_init__(self):
        from src.application.workflow_contract import _id
        _require(type(self.capability) is OperatorPublisherManifest,'MEASUREMENT_PUBLISHER_CAPABILITY')
        _id(self.workspace_id);_digest(self.review_sha256);_digest(self.evaluator_sha256)

    @property
    def sha256(self):
        return _hash(dict(version='measurement-publisher-v1',capability=self.capability.sha256,
            workspace_id=self.workspace_id,review_sha256=self.review_sha256,evaluator_sha256=self.evaluator_sha256))


def build_publication(*,identity,manifest,control,capture,pins,requirements,checked_at):
    now=_utc(checked_at)
    _require(type(manifest) is MeasurementPublisherManifest and type(control) is Rows
        and control.kind=='CONTROL' and control.record_id=='campaign-permissions','MEASUREMENT_PUBLICATION_CONFIGURATION')
    manifest.capability.require_actor(identity,now)
    _require(manifest.workspace_id==pins.workspace_id and manifest.review_sha256==requirements.review_sha256
        and manifest.evaluator_sha256==requirements.evaluator_sha256,'MEASUREMENT_PUBLICATION_SCOPE')
    old,history=control.decoded();head=decode_control_state(old,history,now)
    _require(head.revision<64,'MEASUREMENT_PUBLICATION_CONTROL_LIMIT')
    # Capture uses native source JSON, preserving exact transaction comparisons.
    groups=('invocations','bindings','requests')
    _require(type(capture) is dict and set(capture)=={'benchmark_json','report_json','existing_benchmarks'}|set(groups),
        'MEASUREMENT_PUBLICATION_CAPTURE')
    benchmark,report=capture['benchmark_json'],capture['report_json']
    _require(type(capture['existing_benchmarks']) is tuple and len(capture['existing_benchmarks'])<=1
        and all(v==benchmark for v in capture['existing_benchmarks']),'MEASUREMENT_PUBLICATION_BENCHMARK_CONFLICT')
    from src.application.campaign_quality_qualification_reader import decode_binding
    qualify_documents(benchmark,report,pins=pins,requirements=requirements,invocations=capture['invocations'],
        requests=capture['requests'],bindings=tuple(decode_binding(v) for v in capture['bindings']),
        native_job_id='supplied-capture-not-authentication',observed_at=now,valid_until=now+PLAN_TTL)
    _require(json.loads(report)['recorded_by']==identity.principal.principal_id,'MEASUREMENT_PUBLICATION_RECORDER')
    for group in groups:
        _require(type(capture[group]) is tuple and all(type(v) is str and len(v.encode())<=65536 for v in capture[group]),
            'MEASUREMENT_PUBLICATION_SOURCE_SIZE')
    _require(sum(len(v.encode()) for g in groups for v in capture[g])+len(benchmark.encode())+len(report.encode())<=4*MAX_DOCUMENT_BYTES,
        'MEASUREMENT_PUBLICATION_CAPTURE_SIZE')
    command=_hash(dict(version='measurement-publication-v1',capture={k:list(v) if type(v) is tuple else v for k,v in capture.items()},control=control.head_json,
        history=list(control.history_json),manifest=manifest.sha256,actor=identity.principal.principal_id,checked_at=now.isoformat(),
        bytes=PUBLICATION_BYTES,timeout=PUBLICATION_TIMEOUT_MS))
    candidate=PublicationControl(head.revision+1,head.sha256,command,identity.principal.principal_id,now)
    audit=dict(schema_version=STORAGE_VERSION,command_sha256=command,action='QUALITY_MEASUREMENT_PUBLISH',
        permission_kind='QUALITY_MEASUREMENT',record_id=pins.evaluation_sha256,previous_revision=0,revision=1,
        previous_record_sha256=None,record_sha256=pins.evaluation_sha256,control_revision=candidate.revision,
        control_sha256=candidate.sha256,grant_id=manifest.capability.manifest_id,grant_sha256=manifest.sha256,
        review_id=pins.benchmark_sha256,review_sha256=requirements.review_sha256,changed_by=identity.principal.principal_id,
        reason_code='MEASURED_RESULT_PUBLICATION',recorded_at=now,outcome='APPLIED')
    params=[];sql=[]
    for name,value in dict(workspace_id=pins.workspace_id,benchmark_sha256=pins.benchmark_sha256,
        evaluation_sha256=pins.evaluation_sha256,benchmark_json=benchmark,report_json=report,
        command_sha256=command,record_sha256=pins.evaluation_sha256,control_sha256=candidate.sha256).items():
        params.append(bigquery.ScalarQueryParameter(name,'STRING',value))
    expires=min(now+PLAN_TTL,manifest.capability.valid_until,datetime.fromtimestamp(identity.expires_at,now.tzinfo))
    for name,value in dict(checked_at=now,expires_at=expires).items():params.append(bigquery.ScalarQueryParameter(name,'TIMESTAMP',value))
    for name,table,rows in (('expected_control',CONTROL,(old,)),('expected_history',HISTORY,history),
        ('candidate_control',CONTROL,(candidate.row(),)),('candidate_audit',AUDIT,(audit,))):
        params.append(bigquery.ArrayQueryParameter(name,'STRING',[row_json(v) for v in rows]))
        sql.append(f'CREATE TEMP TABLE {name} AS SELECT {_expr(SCHEMAS[table],"item")} FROM UNNEST(@{name}) item;')
    for group in groups:
        params.append(bigquery.ArrayQueryParameter('expected_'+group,'STRING',list(capture[group])))
    params.append(bigquery.ScalarQueryParameter('existing_benchmark_count','INT64',len(capture['existing_benchmarks'])))
    def table(name):return f'`{PROJECT}.{DATASET}.{name}`'
    def insert(name,source):
        columns=','.join(n for n,_,_ in SCHEMAS[name]);return f'INSERT {table(name)} ({columns}) SELECT {columns} FROM {source};'
    def exact(name,source):
        scope="control_id='campaign-permissions'"
        return f"ASSERT (SELECT COUNT(*) FROM {table(name)} WHERE {scope})=(SELECT COUNT(*) FROM {source}) AND NOT EXISTS(SELECT * FROM {table(name)} WHERE {scope} EXCEPT DISTINCT SELECT * FROM {source}) AND NOT EXISTS(SELECT * FROM {source} EXCEPT DISTINCT SELECT * FROM {table(name)} WHERE {scope}) AS 'MEASUREMENT_{source.upper()}';"
    sql += ['CREATE TEMP TABLE publication_result(state STRING,command_sha256 STRING,record_sha256 STRING,control_sha256 STRING);',
        'BEGIN\nBEGIN TRANSACTION;',"ASSERT @checked_at<=CURRENT_TIMESTAMP() AND CURRENT_TIMESTAMP()<@expires_at AS 'MEASUREMENT_EXPIRED';",
        exact(CONTROL,'expected_control'),exact(HISTORY,'expected_history')]
    sql += [f"ASSERT (SELECT COUNT(*) FROM {table(REPORTS)} WHERE document_sha256=@evaluation_sha256)=0 AS 'MEASUREMENT_REPORT_EXISTS';",
        f"ASSERT NOT EXISTS(SELECT 1 FROM {table(REVOCATIONS)} WHERE (target_kind='REPORT' AND target_sha256=@evaluation_sha256) OR (target_kind='BENCHMARK' AND target_sha256=@benchmark_sha256)) AS 'MEASUREMENT_REVOKED';",
        f"ASSERT (SELECT COUNT(*) FROM {table(BENCHMARKS)} WHERE document_sha256=@benchmark_sha256)=@existing_benchmark_count AND NOT EXISTS(SELECT 1 FROM {table(BENCHMARKS)} WHERE document_sha256=@benchmark_sha256 AND canonical_json!=@benchmark_json) AS 'MEASUREMENT_BENCHMARK_CHANGED';",
        f"ASSERT NOT EXISTS(SELECT 1 FROM {table(AUDIT)} WHERE permission_kind='QUALITY_MEASUREMENT' AND record_id=@evaluation_sha256) AS 'MEASUREMENT_AUDIT_EXISTS';"]
    ids="SELECT JSON_VALUE(c,'$.provider.invocation_id') FROM UNNEST(JSON_QUERY_ARRAY(@report_json,'$.cases')) c"
    invocation_scope=f'workspace_id=@workspace_id AND invocation_id IN ({ids})'
    request_scope=f'workspace_id=@workspace_id AND request_id IN (SELECT request_id FROM `{PROJECT}.ci_application.model_invocations` WHERE {invocation_scope})'
    for group,name,scope in (('invocations','model_invocations',invocation_scope),('bindings','invocation_prompt_bindings',invocation_scope),('requests','requests',request_scope)):
        source=f'(SELECT TO_JSON_STRING(t) value FROM `{PROJECT}.ci_application.{name}` t WHERE {scope})'
        sql.append(f"ASSERT (SELECT COUNT(*) FROM {source})=ARRAY_LENGTH(@expected_{group}) AND NOT EXISTS(SELECT value FROM {source} EXCEPT DISTINCT SELECT * FROM UNNEST(@expected_{group})) AND NOT EXISTS(SELECT * FROM UNNEST(@expected_{group}) EXCEPT DISTINCT SELECT value FROM {source}) AS 'MEASUREMENT_{group.upper()}_CHANGED';")
    sql += [f"UPDATE {table(CONTROL)} t SET "+','.join(f'{n}=c.{n}' for n,_,_ in SCHEMAS[CONTROL])+" FROM candidate_control c WHERE t.control_id='campaign-permissions' AND t.control_sha256=(SELECT control_sha256 FROM expected_control);",
        "ASSERT @@row_count=1 AS 'MEASUREMENT_CAS';",insert(HISTORY,'candidate_control'),
        f'INSERT {table(BENCHMARKS)} (document_sha256,canonical_json) SELECT @benchmark_sha256,@benchmark_json FROM UNNEST([1]) WHERE @existing_benchmark_count=0;',
        f'INSERT {table(REPORTS)} (document_sha256,canonical_json) VALUES(@evaluation_sha256,@report_json);',insert(AUDIT,'candidate_audit'),
        "INSERT publication_result VALUES('APPLIED',@command_sha256,@record_sha256,@control_sha256);",
        'COMMIT TRANSACTION;\nEXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE;\nEND;','SELECT * FROM publication_result;']
    script='\n'.join(sql);config=settings(params);config.maximum_bytes_billed=PUBLICATION_BYTES;config.job_timeout_ms=PUBLICATION_TIMEOUT_MS
    _require(script.count(';')<=64,'MEASUREMENT_STATEMENT_LIMIT')
    return PublicationPlan('quality_measurement_'+_hash(['measurement-slot-v1',pins.workspace_id,pins.evaluation_sha256]),command,
        pins.evaluation_sha256,candidate.sha256,row_json(audit),script,json.dumps(config.to_api_repr(),sort_keys=True,separators=(',',':')))
