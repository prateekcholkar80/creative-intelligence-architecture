"""Versioned exact projection of planner JSON into copy; no approval or repair."""
from dataclasses import dataclass,field
import hashlib,json,unicodedata
from src.application.workflow_contract import ContractError,_require,_digest,canonical_json_bytes
from src.application.campaign_acceptance_contract import _hash
from src.generators.campaign_brief_generator import validate_draft

VERSION='campaign-planner-copy-v1'
FIELDS=frozenset({'concept','layout','headline','body','cta','reference_campaign_id','test_variable',
    'image_treatment','background','text_color','typography'})
COPY_FIELDS=('headline','body','cta')


def parse_json(text):
    _require(type(text) is str and 0<len(text.encode('utf-8'))<=65536,'PLANNER_PROJECTION_SIZE')
    def pairs(items):
        result={}
        for k,v in items:
            _require(k not in result,'PLANNER_PROJECTION_DUPLICATE_KEY');result[k]=v
        return result
    def constant(value):raise ContractError('PLANNER_PROJECTION_JSON')
    try:return json.loads(text,object_pairs_hook=pairs,parse_constant=constant)
    except (ValueError,RecursionError):raise ContractError('PLANNER_PROJECTION_JSON') from None


def validate_options(encoded):
    values=parse_json(encoded)
    _require(type(values) is dict and set(values)==FIELDS and canonical_json_bytes(values).decode()==encoded,
        'PLANNER_PROJECTION_OPTIONS')
    for key,options in values.items():
        _require(type(options) is list and 1<=len(options)<=128
            and all(type(v) is str and len(v)<=8192 and unicodedata.normalize('NFC',v)==v
                and (v!='' or key=='body') for v in options),'PLANNER_PROJECTION_OPTIONS')
        _require(len(set(options))==len(options),'PLANNER_PROJECTION_OPTIONS')
    return values


@dataclass(frozen=True,slots=True)
class PlannerProjection:
    response_sha256: str
    options_canonical_json: str
    draft_canonical_json: str
    sha256: str=field(init=False)

    def __post_init__(self):
        _digest(self.response_sha256);options=validate_options(self.options_canonical_json)
        draft=parse_json(self.draft_canonical_json)
        _require(canonical_json_bytes(draft).decode()==self.draft_canonical_json,'PLANNER_PROJECTION_DRAFT')
        validate_draft(draft,options)
        _require(0<len(self.copy_text)<=8192,'PLANNER_PROJECTION_COPY_SIZE')
        object.__setattr__(self,'sha256',_hash(self.payload()))

    @property
    def copy_fields(self):
        draft=json.loads(self.draft_canonical_json)
        return tuple((key,draft[key]) for key in COPY_FIELDS if draft[key]!='')

    @property
    def copy_text(self):return '\n'.join(value for _,value in self.copy_fields)

    @property
    def options_sha256(self):return _hash(json.loads(self.options_canonical_json))

    def payload(self):
        return dict(version=VERSION,schema_version='campaign-draft-v1',response_sha256=self.response_sha256,
            options_sha256=self.options_sha256,draft_canonical_json=self.draft_canonical_json,
            copy_fields=[dict(field=k,text=v) for k,v in self.copy_fields])


def project_planner_response(response_text,options_canonical_json):
    options=validate_options(options_canonical_json);draft=parse_json(response_text)
    _require(type(draft) is dict and set(draft)==FIELDS,'PLANNER_PROJECTION_DRAFT')
    # The installed Gemini 3 schema permits JSON null only for optional body.
    # Preserve the original response hash; normalize that one representation.
    if draft['body'] is None and '' in options['body']:draft=dict(draft,body='')
    validate_draft(draft,options)
    return PlannerProjection(hashlib.sha256(response_text.encode('utf-8')).hexdigest(),options_canonical_json,
        canonical_json_bytes(draft).decode())


def verify_projection(projection,response_text,*,expected_options_sha256):
    _require(type(projection) is PlannerProjection,'PLANNER_PROJECTION_REQUIRED')
    _digest(expected_options_sha256)
    actual=project_planner_response(response_text,projection.options_canonical_json)
    _require(actual==projection and actual.options_sha256==expected_options_sha256,'PLANNER_PROJECTION_MISMATCH')
    return actual
