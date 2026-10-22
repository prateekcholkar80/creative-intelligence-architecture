"""Bounded, deterministic extraction of untrusted originals; no claim approval."""
import csv
import hashlib
from io import BytesIO,StringIO
import json
import subprocess
import sys

VERSION='campaign-document-text-v1'
MAX_BYTES=5*1024*1024
MAX_TEXT_BYTES=24000
MAX_PAGES=20
MAX_OUTPUT_BYTES=65536
PDF_VERSION='6.21.1'


class ExtractionError(ValueError):
    def __init__(self,code):self.code=code;super().__init__(code)


def require(ok,code):
    if not ok:raise ExtractionError(code)


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def _parse(content,mime):
    require(type(content) is bytes and 0<len(content)<=MAX_BYTES,'DOCUMENT_SIZE')
    pages=[];parser='UTF8'
    if mime in ('image/png','image/jpeg','image/webp'):return 'OCR_REQUIRED',pages,'NO_IMAGE_OCR'
    if mime=='application/pdf':
        import pypdf
        require(pypdf.__version__==PDF_VERSION,'DOCUMENT_PARSER_VERSION')
        parser='pypdf-'+PDF_VERSION
        reader=pypdf.PdfReader(BytesIO(content),strict=True)
        if reader.is_encrypted:return 'ENCRYPTED_DOCUMENT',[],parser
        require(0<len(reader.pages)<=MAX_PAGES,'DOCUMENT_PAGE_LIMIT')
        for number,page in enumerate(reader.pages,1):
            text=page.extract_text() or ''
            if not text.strip():return 'OCR_REQUIRED',[],parser
            pages.append((number,text))
            require(sum(len(t.encode()) for _,t in pages)<=MAX_TEXT_BYTES,'DOCUMENT_TEXT_LIMIT')
    else:
        require(mime in ('text/plain','text/csv','application/json'),'DOCUMENT_FORMAT')
        text=content.decode('utf-8-sig',errors='strict')
        require(len(text.encode())<=MAX_TEXT_BYTES,'DOCUMENT_TEXT_LIMIT')
        require(not any(ord(c)<32 and c not in '\n\r\t' for c in text),'DOCUMENT_CONTROL_CHARACTERS')
        if mime=='application/json':
            def pairs(items):
                result={}
                for k,v in items:
                    require(k not in result,'DOCUMENT_JSON_DUPLICATE_KEY');result[k]=v
                return result
            def constant(_):raise ExtractionError('DOCUMENT_JSON_CONSTANT')
            json.loads(text,object_pairs_hook=pairs,parse_constant=constant)
        if mime=='text/csv':
            count=0
            for row in csv.reader(StringIO(text),strict=True):
                count+=1;require(count<=1000 and len(row)<=64,'DOCUMENT_CSV_LIMIT')
        if not text.strip():return 'NO_TEXT',[],parser
        pages=[(None,text)]
    return 'EXTRACTED_UNVERIFIED',pages,parser


def parse_document(content,mime):
    """Pure parser, invoked in the bounded subprocess by serving callers."""
    state,pages,parser=_parse(content,mime)
    segments=[]
    for page,text in pages:
        for start in range(0,len(text),1000):
            chunk=text[start:start+1000]
            segments.append(dict(segment=len(segments)+1,page=page,start_char=start,end_char=start+len(chunk),
                text=chunk,text_sha256=hashlib.sha256(chunk.encode()).hexdigest()))
    value=dict(version=VERSION,source_sha256=hashlib.sha256(content).hexdigest(),content_type=mime,
        parser=parser,status=state,offset_unit='UNICODE_CODEPOINT',segments=segments,
        text_bytes=sum(len(t.encode()) for _,t in pages),trust='UNTRUSTED_USER_EVIDENCE',
        claim_approval=False,eligible_for_generation=False)
    require(len(encoded(value))<=MAX_OUTPUT_BYTES,'DOCUMENT_OUTPUT_LIMIT')
    return value


def extract_document(content,mime):
    require(type(content) is bytes and 0<len(content)<=MAX_BYTES,'DOCUMENT_SIZE')
    try:
        run=subprocess.run([sys.executable,'-m',__name__,'--worker',mime],input=content,
            stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=12,check=False,
            env={'LANG':'C.UTF-8','LC_ALL':'C.UTF-8','PYTHONIOENCODING':'utf-8'})
    except subprocess.TimeoutExpired:raise ExtractionError('DOCUMENT_PARSER_TIMEOUT') from None
    require(run.returncode==0,'DOCUMENT_PARSER_FAILED')
    require(0<len(run.stdout)<=MAX_OUTPUT_BYTES,'DOCUMENT_OUTPUT_LIMIT')
    value=json.loads(run.stdout)
    if 'error' in value:raise ExtractionError(value['error'])
    require(value['version']==VERSION and value['source_sha256']==hashlib.sha256(content).hexdigest()
        and value['content_type']==mime and value['eligible_for_generation'] is False
        and value['claim_approval'] is False,'DOCUMENT_RESULT_MISMATCH')
    return value


def _worker():
    import resource
    resource.setrlimit(resource.RLIMIT_CPU,(5,5))
    if sys.platform.startswith('linux'):
        resource.setrlimit(resource.RLIMIT_AS,(512*1024*1024,512*1024*1024))
    try:result=parse_document(sys.stdin.buffer.read(MAX_BYTES+1),sys.argv[2])
    except ExtractionError as error:result=dict(error=error.code)
    except Exception:result=dict(error='DOCUMENT_INVALID_OR_UNREADABLE')
    sys.stdout.buffer.write(encoded(result))


if __name__=='__main__':
    if len(sys.argv)!=3 or sys.argv[1]!='--worker':raise SystemExit(2)
    _worker()
