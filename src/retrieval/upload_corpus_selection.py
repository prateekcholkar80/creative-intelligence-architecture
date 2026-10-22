"""Shared server-owned source-selection validation; no approval or dispatch."""
from src.application.workflow_contract import _require,_digest,_id

def validate_selection(value):
    _require(type(value) is dict and set(value)=={'version','scope','image_binding_id','sources'}
        and value['version']=='private-evaluation-source-selection-v1','UPLOAD_ACCESS_SELECTION')
    scope=value['scope']
    _require(type(scope) is dict and set(scope)=={'workspace_id','conversation_id','request_id','sku','country','channel'},
        'UPLOAD_ACCESS_SCOPE')
    for v in scope.values():_id(v)
    _require(type(value['image_binding_id']) is str and value['image_binding_id'].startswith('eval_upload_binding_v1_'),
        'UPLOAD_ACCESS_BINDING')
    _digest(value['image_binding_id'].removeprefix('eval_upload_binding_v1_'))
    sources=value['sources']
    _require(type(sources) is list and 0<len(sources)<=16,'UPLOAD_ACCESS_SOURCES')
    ids=set()
    for s in sources:
        _require(type(s) is dict and set(s)=={'submission_id','content_sha256','generation','content_type','role'}
            and s['role'] in ('BOUND_IMAGE','DOCUMENT') and s['submission_id'] not in ids,
            'UPLOAD_ACCESS_SOURCE')
        _digest(s['submission_id']);_digest(s['content_sha256']);ids.add(s['submission_id'])
        _require(type(s['generation']) is str and s['generation'].isdigit() and int(s['generation'])>0
            and s['content_type'] in ('image/webp','image/png','image/jpeg','text/plain','text/csv','application/json','application/pdf')
            and (s['content_type'].startswith('image/'))==(s['role']=='BOUND_IMAGE'),'UPLOAD_ACCESS_SOURCE_TYPE')
    _require(any(s['role']=='BOUND_IMAGE' for s in sources),'UPLOAD_ACCESS_IMAGE_REQUIRED')
    return value

