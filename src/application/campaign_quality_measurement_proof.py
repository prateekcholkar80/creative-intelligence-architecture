"""Stored measured-report publication proof; no semantic evaluation or approval."""
import json
from src.application.campaign_review_storage import decode_control_state, SCHEMAS, STORAGE_VERSION
from src.application.campaign_publication_reader import _native_row
from src.application.campaign_quality_publication import AUDIT
from src.application.workflow_contract import _require, _digest

GROUPS={'publication_controls':1,'publication_history':64,'measurement_publications':1}


def verify_measurement_proof(row, *, pins, requirements, publisher_manifest_sha256, report, observed_at):
    _digest(publisher_manifest_sha256)
    decoded={}
    for key,limit in GROUPS.items():
        values=row[key];count=row[key+'_count']
        _require(type(values) is list and type(count) is int and count==len(values) and 0<count<=limit,
            'MEASUREMENT_PROOF_COVERAGE')
        _require(all(type(v) is str and len(v.encode())<=65536 for v in values),'MEASUREMENT_PROOF_SIZE')
        decoded[key]=tuple(_native_row(v) for v in values)
    audits=decoded['measurement_publications'];audit=audits[0]
    _require(set(audit)=={name for name,_,_ in SCHEMAS[AUDIT]},'MEASUREMENT_PROOF_AUDIT_SCHEMA')
    for name,kind,mode in SCHEMAS[AUDIT]:
        value=audit[name]
        _require((value is None and mode=='NULLABLE') or (type(value) is str if kind=='STRING' else
            type(value) is int if kind=='INTEGER' else hasattr(value,'tzinfo')),'MEASUREMENT_PROOF_AUDIT_TYPE')
    _require(audit['schema_version']==STORAGE_VERSION and audit['action']=='QUALITY_MEASUREMENT_PUBLISH'
        and audit['permission_kind']=='QUALITY_MEASUREMENT' and audit['record_id']==pins.evaluation_sha256
        and audit['record_sha256']==pins.evaluation_sha256 and audit['previous_revision']==0
        and audit['revision']==1 and audit['previous_record_sha256'] is None and audit['outcome']=='APPLIED'
        and audit['reason_code']=='MEASURED_RESULT_PUBLICATION'
        and audit['grant_sha256']==publisher_manifest_sha256 and audit['review_id']==pins.benchmark_sha256
        and audit['review_sha256']==requirements.review_sha256 and audit['changed_by']==report['recorded_by'],
        'MEASUREMENT_PROOF_AUDIT_CONFLICT')
    head=decoded['publication_controls'][0];history=tuple(sorted(decoded['publication_history'],key=lambda v:v['revision']))
    decode_control_state(head,history,observed_at)
    matches=[v for v in history if v['revision']==audit['control_revision']]
    _require(len(matches)==1 and all(matches[0][key]==audit[key] for key in
        ('control_sha256','command_sha256','changed_by','recorded_at')),'MEASUREMENT_PROOF_CONTROL_CONFLICT')
    from datetime import datetime
    _require(datetime.fromisoformat(report['completed_at'])<=audit['recorded_at']<=observed_at,
        'MEASUREMENT_PROOF_WINDOW')
