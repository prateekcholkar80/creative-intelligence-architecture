"""Protected product-readiness/chat boundary for a trusted durable pipeline.

No browser-configured pipeline, local generation fallback or approval inference.
Pipeline implementations own native admission/outcomes and fresh artifact release.
"""
from abc import ABC,abstractmethod
from dataclasses import dataclass
import hashlib,re

from src.application.identity import IapIdentityAdapter
from src.application.access_policy import AccessDecision,Action
from src.application.campaign_quality_product_reader import _access
from src.application.workflow_contract import _require,_id,_digest

PRODUCT_STATES=frozenset({'READY','PREPARING','NEEDS_EVIDENCE','NEEDS_REVIEW','UNAVAILABLE'})
JOB_STATES=frozenset({'ACCEPTED','RUNNING','NEEDS_REVIEW','SUCCEEDED','FAILED','UNKNOWN'})
STAGES=frozenset({'QUEUED','CHECKING_EVIDENCE','RETRIEVING','PLANNING','RENDERING','VALIDATING','COMPLETE','STOPPED'})
REASONS=frozenset({'SOURCE_PREPARATION_REQUIRED','PRODUCT_IMAGE_REQUIRED','IMAGE_BINDING_REQUIRED',
    'IMAGE_USE_REVIEW_REQUIRED','CLAIM_REVIEW_REQUIRED','BRAND_REVIEW_REQUIRED','PROMPT_POLICY_REVIEW_REQUIRED',
    'PIPELINE_NOT_INSTALLED','ARTIFACT_CHECKS_REQUIRED','SOURCE_UNAVAILABLE','UNSUPPORTED_PRODUCT_SCOPE'})


@dataclass(frozen=True,slots=True)
class CampaignProduct:
    sku:str
    product_name:str
    country:str
    channel:str
    audience:str
    state:str
    reasons:tuple[str,...]
    readiness_sha256:str

    def __post_init__(self):
        _id(self.sku);_digest(self.readiness_sha256)
        _require(re.fullmatch(r'CI-SKU-[0-9]{4}',self.sku) is not None
            and type(self.product_name) is str and 0<len(self.product_name)<=512
            and type(self.country) is str and re.fullmatch('[A-Z]{2}',self.country) is not None
            and self.channel in {'META','KLAVIYO_EMAIL','TIKTOK','PINTEREST'} and self.audience in {'NEW','RETURNING'}
            and self.state in PRODUCT_STATES and type(self.reasons) is tuple
            and len(set(self.reasons))==len(self.reasons) and set(self.reasons)<=REASONS
            and ((self.state=='READY')==(not self.reasons)),'CAMPAIGN_PRODUCT_READINESS')


@dataclass(frozen=True,slots=True)
class CampaignJob:
    job_id:str
    sku:str
    message_sha256:str
    state:str
    stage:str
    reasons:tuple[str,...]=()

    def __post_init__(self):
        _id(self.job_id);_id(self.sku);_digest(self.message_sha256)
        _require(self.state in JOB_STATES and self.stage in STAGES and type(self.reasons) is tuple
            and set(self.reasons)<=REASONS and (self.state!='SUCCEEDED' or self.stage=='COMPLETE'),
            'CAMPAIGN_JOB_STATUS')


@dataclass(frozen=True,slots=True)
class CampaignImage:
    job_id:str
    sku:str
    png:bytes
    content_sha256:str
    width:int
    height:int
    evidence_summary:str

    def __post_init__(self):
        _id(self.job_id);_id(self.sku);_digest(self.content_sha256)
        _require(type(self.png) is bytes and 0<len(self.png)<=20*1024**2
            and hashlib.sha256(self.png).hexdigest()==self.content_sha256
            and type(self.width) is int and type(self.height) is int
            and 1<=self.width<=4096 and 1<=self.height<=4096
            and type(self.evidence_summary) is str and len(self.evidence_summary)<=8192,'CAMPAIGN_IMAGE_RESPONSE')
        # Decoding is required even when a backend supplies matching hash fields.
        import io
        from PIL import Image
        with Image.open(io.BytesIO(self.png)) as image:
            _require(image.format=='PNG' and image.size==(self.width,self.height),'CAMPAIGN_IMAGE_RESPONSE')
            image.verify()


class CampaignPipeline(ABC):
    """Trusted server adapter; durable/current checks cannot be delegated to UI.

    admit must atomically persist exact scope/brief/retry identity and readiness
    dependencies. Repeating a key must recover the original accepted operation,
    including after a cold restart; changed payloads must conflict. artifact must
    verify current source/policy/quality/release and GCS generation before bytes.
    No implementation is installed merely by defining this interface.
    """
    @abstractmethod
    def products(self,identity,*,workspace_id,conversation_id):raise NotImplementedError
    @abstractmethod
    def admit(self,identity,*,workspace_id,conversation_id,product,message,retry_key):raise NotImplementedError
    @abstractmethod
    def job(self,identity,*,workspace_id,conversation_id,job_id):raise NotImplementedError
    @abstractmethod
    def jobs(self,identity,*,workspace_id,conversation_id):raise NotImplementedError
    @abstractmethod
    def recover_admission(self,identity,*,workspace_id,conversation_id,sku,message_sha256,retry_key):
        """Read only the original admission; never enqueue or invoke a provider."""
        raise NotImplementedError
    @abstractmethod
    def artifact(self,identity,*,workspace_id,conversation_id,job_id):raise NotImplementedError


class ProtectedCampaignCreation:
    def __init__(self,*,verifier,access_reader,pipeline):
        _require(type(verifier) is IapIdentityAdapter and isinstance(pipeline,CampaignPipeline),
            'CAMPAIGN_CREATION_COMPOSITION')
        self.verifier,self.access_reader,self.pipeline=verifier,access_reader,pipeline

    def _identity(self,headers,scope):
        for value in scope.values():_id(value)
        identity=self.verifier.authenticate(headers)
        return identity,_access(self.access_reader,identity,**scope)

    def _fresh(self,headers,scope,identity,access):
        after,current=self._identity(headers,scope)
        _require(after.principal==identity.principal and current==access,'CAMPAIGN_CREATION_ACCESS_CHANGED')

    def _generation_access(self,identity,scope):
        decision=self.access_reader.authorize(identity,Action.GENERATE_DRAFT,workspace_id=scope['workspace_id'])
        _require(type(decision) is AccessDecision and decision.allowed is True
            and decision.action is Action.GENERATE_DRAFT and decision.workspace_id==scope['workspace_id']
            and decision.principal_id==identity.principal.principal_id
            and type(decision.membership_revision) is int and decision.membership_revision>0,
            'CAMPAIGN_GENERATION_ACCESS_DENIED')
        return decision

    def products(self,headers,*,workspace_id,conversation_id):
        scope=dict(workspace_id=workspace_id,conversation_id=conversation_id)
        identity,access=self._identity(headers,scope)
        products=self.pipeline.products(identity,**scope)
        _require(type(products) is tuple and len(products)<=1000
            and all(type(p) is CampaignProduct for p in products)
            and len({p.sku for p in products})==len(products),'CAMPAIGN_CATALOG_UNVERIFIED')
        self._fresh(headers,scope,identity,access);return products

    def submit(self,headers,*,workspace_id,conversation_id,sku,message,retry_key):
        _id(sku);_id(retry_key)
        _require(type(message) is str and 0<len(message.strip()) and len(message)<=2000,'CAMPAIGN_MESSAGE_INVALID')
        scope=dict(workspace_id=workspace_id,conversation_id=conversation_id)
        identity,access=self._identity(headers,scope)
        # Never accept a browser's READY marker, corpus, prompt or approval pins.
        products=self.products(headers,**scope);matches=[p for p in products if p.sku==sku]
        _require(len(matches)==1 and matches[0].state=='READY','CAMPAIGN_PRODUCT_NOT_READY')
        self._fresh(headers,scope,identity,access)
        generation_access=self._generation_access(identity,scope)
        result=self.pipeline.admit(identity,**scope,product=matches[0],message=message,retry_key=retry_key)
        _require(type(result) is CampaignJob and result.sku==sku
            and result.message_sha256==hashlib.sha256(message.encode()).hexdigest(),'CAMPAIGN_ADMISSION_UNVERIFIED')
        self._fresh(headers,scope,identity,access)
        _require(self._generation_access(identity,scope)==generation_access,'CAMPAIGN_GENERATION_ACCESS_CHANGED')
        return result

    def job(self,headers,*,workspace_id,conversation_id,job_id):
        _id(job_id);scope=dict(workspace_id=workspace_id,conversation_id=conversation_id)
        identity,access=self._identity(headers,scope)
        result=self.pipeline.job(identity,**scope,job_id=job_id)
        _require(type(result) is CampaignJob and result.job_id==job_id,'CAMPAIGN_JOB_UNVERIFIED')
        self._fresh(headers,scope,identity,access);return result

    def jobs(self,headers,*,workspace_id,conversation_id):
        scope=dict(workspace_id=workspace_id,conversation_id=conversation_id)
        identity,access=self._identity(headers,scope)
        result=self.pipeline.jobs(identity,**scope)
        _require(type(result) is tuple and len(result)<=50 and all(type(j) is CampaignJob for j in result)
            and len({j.job_id for j in result})==len(result),'CAMPAIGN_HISTORY_UNVERIFIED')
        self._fresh(headers,scope,identity,access);return result

    def recover(self,headers,*,workspace_id,conversation_id,sku,message,retry_key):
        _id(sku);_id(retry_key)
        _require(type(message) is str and 0<len(message.strip()) and len(message)<=2000,'CAMPAIGN_MESSAGE_INVALID')
        scope=dict(workspace_id=workspace_id,conversation_id=conversation_id)
        identity,access=self._identity(headers,scope);digest=hashlib.sha256(message.encode()).hexdigest()
        result=self.pipeline.recover_admission(identity,**scope,sku=sku,message_sha256=digest,retry_key=retry_key)
        _require(result is None or (type(result) is CampaignJob and result.sku==sku
            and result.message_sha256==digest),'CAMPAIGN_ADMISSION_UNVERIFIED')
        self._fresh(headers,scope,identity,access);return result

    def artifact(self,headers,*,workspace_id,conversation_id,job_id):
        _id(job_id);scope=dict(workspace_id=workspace_id,conversation_id=conversation_id)
        identity,access=self._identity(headers,scope)
        job=self.job(headers,**scope,job_id=job_id)
        _require(job.state=='SUCCEEDED','CAMPAIGN_ARTIFACT_WITHHELD')
        # pipeline.artifact performs the authoritative per-artifact release check;
        # SUCCEEDED or the pure CampaignImage type alone never grants release.
        result=self.pipeline.artifact(identity,**scope,job_id=job_id)
        _require(type(result) is CampaignImage and result.job_id==job_id and result.sku==job.sku,
            'CAMPAIGN_ARTIFACT_UNVERIFIED')
        self._fresh(headers,scope,identity,access);return result
