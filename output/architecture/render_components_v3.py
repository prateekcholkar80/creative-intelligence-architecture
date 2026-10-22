from pathlib import Path
from PIL import Image,ImageDraw,ImageFont
import html,math
OUT=Path('output/architecture');W,H=4000,3000
im=Image.new('RGB',(W,H),'#F6F9FE');d=ImageDraw.Draw(im)
fontpath='assets/fonts/campaign/opensans.ttf'
svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">','<rect width="100%" height="100%" fill="#F6F9FE"/>']
errors=[]
def rect(x,y,w,h,fill,stroke=None,r=14):
 d.rounded_rectangle((x,y,x+w,y+h),radius=r,fill=fill,outline=stroke,width=2)
 svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke or "none"}" stroke-width="2"/>')
def text(x,y,s,n=27,c='#18324F',maxw=None):
 f=ImageFont.truetype(fontpath,n)
 if maxw and d.textlength(s,font=f)>maxw:errors.append((s,d.textlength(s,font=f),maxw))
 d.text((x,y),s,font=f,fill=c)
 svg.append(f'<text x="{x}" y="{y+n}" font-family="Open Sans,Arial,sans-serif" font-size="{n}" fill="{c}">{html.escape(s)}</text>')
def line(x1,y1,x2,y2,c='#57779E',dash=False):
 d.line((x1,y1,x2,y2),fill=c,width=4)
 svg.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{c}" stroke-width="4"'+(' stroke-dasharray="10 8"' if dash else '')+'/>')
def arrow(x1,y1,x2,y2,c='#57779E'):
 line(x1,y1,x2,y2,c);a=math.atan2(y2-y1,x2-x1);pts=[(x2,y2),(x2-16*math.cos(a-.5),y2-16*math.sin(a-.5)),(x2-16*math.cos(a+.5),y2-16*math.sin(a+.5))];d.polygon(pts,fill=c);svg.append('<polygon points="'+' '.join(f'{x},{y}' for x,y in pts)+f'" fill="{c}"/>')
colors=['#087FAB','#187F56','#3856BE','#A16A0C','#7646AF','#AB4762'];tints=['#E9F7FF','#EAF8F0','#EFF1FF','#FFF8E4','#F5EFFF','#FFF0F3'];xs=[80+i*644 for i in range(6)];cw=620
text(80,40,'CREATIVE INTELLIGENCE ON GOOGLE CLOUD',62,'#102F5D')
text(80,130,'Component architecture  |  Multimodal RAG + commerce analytics + grounded campaign generation',31,'#496380')
rect(80,205,3840,88,'#102F5D')
text(110,224,'MARKETERS / REVIEWERS',29,'#FFFFFF');text(650,226,'Chat + campaign studio   →   Product / market / channel / objective   →   Protected Cloud Run API',29,'#FFFFFF')
# section panels
heads=['1  Intake + orchestration','2  Data + knowledge','3  Grounding + planning','4  Models + rendering','5  Quality + approval','6  Delivery + outcomes']
for x,h,c,t in zip(xs,heads,colors,tints):
 rect(x,330,cw,1035,t,'#CFDCEB');text(x+20,350,h,31,c,maxw=580)
def box(col,y,title,service,items,icon):
 x=xs[col]+16;w=cw-32;rect(x,y,w,280,'#FFFFFF','#D3DFED')
 rect(x+17,y+18,61,52,colors[col],r=9);text(x+27,y+24,icon,24,'#FFFFFF')
 text(x+92,y+15,title,29,colors[col],maxw=w-110)
 text(x+22,y+76,service,25,'#18324F',maxw=w-44)
 for i,s in enumerate(items):text(x+22,y+123+i*36,'• '+s,25,'#405A74',maxw=w-44)
components=[
 [('Protected ingestion','Cloud Run intake + GCS',['Bulk uploads + scheduled connectors','Validate MIME / size / source identity','SKU mapping, dedupe + immutable bytes','Source hash, generation + ownership'],'IN'),('Preparation scheduler','Cloud Scheduler / Workflows',['Source event starts a durable job','Workflows invokes Cloud Run Jobs','Per-stage status + stable operation IDs','Explicit recovery of uncertain calls'],'WF'),('Processing workers','Cloud Run Jobs + Vertex AI',['Extract documents + segment text','Classify images + prepare pack shots','Create text + image embeddings','Version parser, model + source pins'],'ETL')],
 [('Source + artifact bytes','Cloud Storage (GCS)',['Originals, derived assets + banners','Immutable generation-pinned reads','Lifecycle / retention configuration','Content hashes + lineage references'],'GCS'),('Product knowledge + vectors','BigQuery corpus / evidence tables',['Products, chunks + asset metadata','Text / image vectors + profile versions','Claims, brand rules + scoped reviews','Readiness, expiry + revocation records'],'BQ'),('Commerce analytics marts','BigQuery curated facts + views',['Sales, returns, spend + inventory facts','Product / store / channel dimensions','Net revenue, margin, CVR + ROAS','Currency, time zone + attribution rules'],'SQL')],
 [('Campaign admission','Cloud Run campaign API / worker',['IAP identity + workspace / SKU scope','Persist brief + selected campaign scope','Check readiness and execution limits','Resume durable progress after refresh'],'API'),('Retrieval + analytics tools','BigQuery search + governed SQL',['Eligible lexical + vector candidates','Semantic reranker selects evidence','Typed queries for store / SKU metrics','Read real images; retain query IDs'],'RAG'),('Evidence binder + planner','Application services + Gemini',['Bind facts, claims, assets + metrics','Hash exact evidence + prompt inputs','Produce concept, copy + layout JSON','Pin all model / prompt / policy versions'],'LLM')],
 [('Embedding services','Vertex AI + Google Gen AI SDK',['Text and multimodal embedding profiles','Versioned dimensions + task types','Batch preparation + query embeddings','Provider outcomes + nullable usage'],'VEC'),('Generation + reranking','Vertex AI Gemini models',['Intent / planning and multimodal reasoning','Bounded semantic reranking adapter','Image generation with product reference','Approved endpoint / region profiles'],'AI'),('Source-preserving compositor','Cloud Run render worker + Pillow',['Preserve actual product crop + label','Versioned typography + output layouts','Compose scene, copy and real product','Save image bytes + exact render manifest'],'IMG')],
 [('Input + evidence checks','Application policy / source validators',['Current access + scope + source hashes','Applicable claims and asset permissions','Untrusted document / prompt handling','No approval inferred from similarity'],'POL'),('Artifact quality pipeline','Mechanical + multimodal evaluators',['Product fidelity, OCR + layout checks','Claim support, brand + safety checks','Format constraints + scored results','Unavailable checks withhold clearance'],'EVAL'),('Correction + release gate','Bounded workflow + human review',['Limited correction with recorded reasons','Reviewer decision tied to exact artifact','Fresh access / approval / expiry checks','Generation and release stay separate'],'GATE')],
 [('Campaign experience','Cloud Run chat / campaign studio',['Private preview + approved downloads','Concept, copy and format variants','Evidence and analytics explanations','Saved progress, clarification + feedback'],'UI'),('Durable execution records','BigQuery ledger + GCS artifacts',['Inputs, attempts + provider outcomes','Response hashes, model IDs + usage','Artifact generations + measured scores','Original-job recovery without redispatch'],'AUD'),('Outcome intelligence','BigQuery analytics + feedback pipeline',['Join campaign / creative / SKU IDs','Store, audience and channel outcomes','Experiments + attribution limitations','Controlled evaluation of future changes'],'BI')]
]
for col,items in enumerate(components):
 for y,args in zip([415,725,1035],items):box(col,y,*args)
# arrows across layer gutters; data → app → models → checks → outputs
for col in range(5):arrow(xs[col]+cw+2,690,xs[col+1]-3,690)
# explicit named integration bus
rect(80,1410,3840,112,'#E8EDF9','#BCCBE7')
text(108,1426,'EXECUTION + MODEL ADAPTER CONTRACT',29,'#35487E')
text(108,1471,'Typed inputs / outputs   •   Exact prompt + source binding   •   One provider attempt per durable slot   •   Budgeted timeouts   •   Recovery before retry',27,'#35487E')
# platform services
text(80,1570,'PLATFORM SERVICES / LLMOPS',36,'#102F5D')
platform=[('7  Development + registry','Python + Google SDKs',['Versioned schemas + configurations','Prompt / model / parser definitions','Git-reviewed changes + test fixtures'],'BigQuery registry / review records',['Exact version and history binding','No framework checkpoint as approval']),('8  CI/CD + infrastructure','Cloud Build + Artifact Registry',['Allowlisted source + container hashes','Focused tests + image validation','Immutable Cloud Run deployments'],'Infrastructure as code',['Scoped service configuration + IAM','Staged rollout + rollback contracts']),('9  Evaluation + experiments','Versioned benchmark runner',['Retrieval relevance + source support','Campaign quality + adversarial cases','Unit / native / live evidence separated'],'Human review + experiments',['Artifact-level judgment + feedback','Measured campaign outcome comparisons']),('10  Observability','Cloud Logging + Monitoring',['Correlated run / stage / native-job IDs','Latency, failures + worker progress','Provider identity, tokens + usage'],'Operational dashboards',['Readiness / source freshness alerts','Safe diagnostics without raw secrets']),('11  Security + privacy','IAP + IAM + service accounts',['Least privilege per worker and table','Workspace / owner / market controls','Secret Manager + audited configuration'],'Data governance',['Retention + residency + revocations','Approved model endpoints / regions']),('12  Cost + reliability','Budgets + admission limits',['Provider, query and compute usage','Bounded context / images / concurrency','Estimates separate from billed costs'],'Recovery + resource controls',['Idempotent steps + unknown holds','No repeated call after lost response'])]
for x,(title,svc,items,svc2,items2) in zip(xs,platform):
 rect(x,1640,cw,510,'#FFFFFF','#C7D7EA');text(x+20,1660,title,30,'#1459A3',maxw=580);text(x+20,1720,svc,27,'#18324F',maxw=580)
 for i,s in enumerate(items):text(x+20,1770+i*39,'• '+s,25,'#405A74',maxw=580)
 text(x+20,1910,svc2,27,'#18324F',maxw=580)
 for i,s in enumerate(items2):text(x+20,1964+i*39,'• '+s,25,'#405A74',maxw=580)
# source bottom / feedback / business
bottom=[(80,1220,'DATA SOURCES',['Product catalog / PIM / CMS + brand documents','Original pack shots + historical campaign creatives','Orders / POS / returns + product costs + inventory','Store / ecommerce / ad platforms + experiment data'],'#087FAB'),(1390,1220,'ANALYTICS GROUNDING',['Typed SQL over governed BigQuery views','Product revenue, margin and sales trends','Store, market, channel and cohort comparisons','Metric definition + window + freshness + query lineage'],'#A16A0C'),(2700,1220,'FEEDBACK AND BUSINESS VALUE',['Marketer review + real campaign outcome capture','Product / store / campaign identity reconciliation','Recommendations informed by stock and economics','Evaluated changes; no automatic efficacy approval'],'#7646AF')]
for x,w,title,items,c in bottom:
 rect(x,2250,w,350,'#FFFFFF','#C7D7EA');text(x+25,2272,title,31,c,maxw=w-50)
 for i,s in enumerate(items):text(x+25,2335+i*47,'• '+s,28,'#405A74',maxw=w-50)
# Lower reference panels describe shared inputs and feedback, not sequential stages.
rect(80,2660,3840,152,'#102F5D')
text(110,2680,'KEY PLATFORM CAPABILITIES',31,'#FFFFFF')
text(110,2735,'Product fidelity   |   Evidence-grounded content   |   Commerce intelligence   |   Durable execution   |   Governed delivery   |   Measured quality',30,'#FFFFFF')
text(80,2860,'BigQuery owns data, vectors and durable business records. GCS owns bytes. Orchestration coordinates work and does not grant approval.',27,'#496380')
text(80,2910,'Future-state component design  •  Solid arrows show primary flow; platform services apply across every layer.',26,'#496380')
assert not errors,errors
svg.append('</svg>');(OUT/'creative-intelligence-component-architecture-v3.svg').write_text('\n'.join(svg))
im.save(OUT/'creative-intelligence-component-architecture-v3.png')
print('Saved editable SVG and 4000 × 3000 PNG; all text width checks passed.')
