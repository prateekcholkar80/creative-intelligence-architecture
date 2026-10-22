"""Pinned product-image transformation; a user crop decision is not claim approval."""
from dataclasses import dataclass
import hashlib,io,json,struct,zlib
import PIL
from PIL import Image
from src.application.campaign_evaluator_store import canonical,sha
from src.application.campaign_attachment_service import inspect_file
from src.application.campaign_evaluation_uploads import EvaluationUploadInputs,VERSION as INPUT_VERSION
from src.application.workflow_contract import _require,_digest

VERSION='private-product-image-derivation-v1'
PREFIX='eval_image_derivation_v1_'
PNG_ENCODING='RGB8_STORED_DEFLATE_V1'


def canonical_rgb_png(image):
    """PNG with fixed filter/block boundaries, independent of compression builds."""
    _require(image.mode=='RGB' and 0<image.width*image.height<=20_000_000,'IMAGE_DERIVATION_FORMAT')
    pixels=image.tobytes();stride=image.width*3
    scanlines=b''.join(b'\x00'+pixels[i:i+stride] for i in range(0,len(pixels),stride))
    # RFC 1950/1951: no dictionary, stored blocks, fixed 65535-byte boundaries.
    blocks=[]
    for start in range(0,len(scanlines),65535):
        block=scanlines[start:start+65535];n=len(block)
        blocks.append(bytes([int(start+n==len(scanlines))])+struct.pack('<HH',n,n^65535)+block)
    stream=b'\x78\x01'+b''.join(blocks)+struct.pack('>I',zlib.adler32(scanlines)&0xffffffff)
    def chunk(kind,data):return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data)&0xffffffff)
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',image.width,image.height,8,2,0,0,0))+chunk(b'IDAT',stream)+chunk(b'IEND',b'')


@dataclass(frozen=True,slots=True)
class ImageDerivationSettings:
    decision_json:str
    decision_sha256:str
    enabled:bool=False

    def __post_init__(self):
        _digest(self.decision_sha256)
        _require(type(self.enabled) is bool and type(self.decision_json) is str
            and len(self.decision_json.encode())<=8192,'IMAGE_DERIVATION_SETTINGS')
        value=json.loads(self.decision_json)
        _require(canonical(value)==self.decision_json and sha(value)==self.decision_sha256
            and set(value)=={'version','decision','source_binding_id','source_submission_id','source_sha256',
                'source_dimensions','crop_box','output_sha256','crop_decoded_rgb_sha256','png_encoding','pillow_version','scope','claim_approval','release_allowed'}
            and value['version']=='private-product-image-transform-decision-v1'
            and value['decision']=='EXPLICIT_USER_PRODUCT_ONLY_CROP'
            and value['claim_approval'] is False and value['release_allowed'] is False,'IMAGE_DERIVATION_DECISION')
        prefix='eval_upload_binding_v1_'
        _require(type(value['source_binding_id']) is str and value['source_binding_id'].startswith(prefix),
            'IMAGE_DERIVATION_SOURCE_BINDING')
        _digest(value['source_binding_id'][len(prefix):])
        for key in ('source_submission_id','source_sha256','output_sha256','crop_decoded_rgb_sha256'):_digest(value[key])
        _require(value['png_encoding']==PNG_ENCODING,'IMAGE_DERIVATION_TRANSFORM')
        _require(type(value['source_dimensions']) is list and len(value['source_dimensions'])==2
            and all(type(n) is int and 1<=n<=20000 for n in value['source_dimensions'])
            and type(value['crop_box']) is list and len(value['crop_box'])==4
            and all(type(n) is int for n in value['crop_box'])
            and 0<=value['crop_box'][0]<value['crop_box'][2]<=value['source_dimensions'][0]
            and 0<=value['crop_box'][1]<value['crop_box'][3]<=value['source_dimensions'][1]
            and type(value['pillow_version']) is str and bool(value['pillow_version'])
            and type(value['scope']) is dict and set(value['scope'])=={'sku','country','channel','audience'},
            'IMAGE_DERIVATION_TRANSFORM')


def prepare_derivation(inputs,settings):
    _require(type(inputs) is EvaluationUploadInputs and type(settings) is ImageDerivationSettings,'IMAGE_DERIVATION_INPUT')
    source=json.loads(inputs.manifest_json);decision=json.loads(settings.decision_json)
    _require(canonical(source)==inputs.manifest_json and source['version']==INPUT_VERSION
        and source['use']=='UNVERIFIED_VISUAL_REFERENCE_FOR_EVALUATION_ONLY'
        and all(source[k] is False for k in ('claim_approval','eligible_for_generation','qualifying','release_allowed'))
        and len(inputs.images)==len(source['image_pins'])==len(source['sources'])==1
        and decision['source_binding_id']=='eval_upload_binding_v1_'+inputs.sha256
        and all(source['context']['scope'][k]==v for k,v in decision['scope'].items()),'IMAGE_DERIVATION_PARENT_CHANGED')
    pin=source['image_pins'][0];parent=source['sources'][0]
    asset,generation,mime,raw=inputs.images[0]
    _require(parent['submission_id']==decision['source_submission_id'] and parent['image_asset_id']==asset
        and pin==dict(asset_id=asset,generation=generation,mime_type=mime,byte_count=len(raw),sha256=decision['source_sha256'])
        and parent['original']['generation']==generation
        and hashlib.sha256(raw).hexdigest()==decision['source_sha256']
        and PIL.__version__==decision['pillow_version'],'IMAGE_DERIVATION_BYTES_OR_PROFILE_CHANGED')
    with Image.open(io.BytesIO(raw)) as image:
        _require(image.mode=='RGB' and list(image.size)==decision['source_dimensions']
            and image.width*image.height<=20_000_000 and getattr(image,'n_frames',1)==1,'IMAGE_DERIVATION_FORMAT')
        image.load();crop=image.crop(tuple(decision['crop_box']));pixels=hashlib.sha256(crop.tobytes()).hexdigest()
        _require(pixels==decision['crop_decoded_rgb_sha256'],'IMAGE_DERIVATION_PIXELS_CHANGED')
        png=canonical_rgb_png(crop)
        with Image.open(io.BytesIO(png)) as saved:
            saved.load();_require(saved.mode=='RGB' and saved.tobytes()==crop.tobytes(),'IMAGE_DERIVATION_PIXELS_CHANGED')
    _require(hashlib.sha256(png).hexdigest()==decision['output_sha256'],'IMAGE_DERIVATION_OUTPUT_CHANGED')
    file=inspect_file('product-only-crop.png',png,'PRODUCT_IMAGE')
    record=dict(version=VERSION,derivation_scope=source['context']['scope'],policy_sha256=source['context']['policy_sha256'],
        principal_id=source['principal_id'],parent_binding_id=decision['source_binding_id'],parent_manifest_sha256=inputs.sha256,
        parent_source=parent,decision=decision,decision_sha256=settings.decision_sha256,
        transform=dict(method='PILLOW_RECTANGULAR_CROP',crop_box=decision['crop_box'],pillow_version=PIL.__version__,png_encoding=PNG_ENCODING,
            resized=False,generative_edit=False,color_conversion=False),
        derived_file=file,width=crop.width,height=crop.height,decoded_rgb_sha256=pixels,
        use='UNVERIFIED_DERIVED_VISUAL_REFERENCE_FOR_PRIVATE_EVALUATION_ONLY',
        review_status='UNVERIFIED',claim_approval=False,eligible_for_generation=False,qualifying=False,release_allowed=False)
    return PREFIX+sha(record),record,png
