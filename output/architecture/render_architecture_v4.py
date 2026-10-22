from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import html, base64, io, math, zipfile
ROOT=Path(__file__).resolve().parent
FONT=Path('assets/fonts/campaign/opensans.ttf'); W,H=4000,3000
im=Image.new('RGB',(W,H),'#F7FAFF');d=ImageDraw.Draw(im)
svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}"><rect width="100%" height="100%" fill="#F7FAFF"/>'];errors=[]
C={'ink':'#142F4E','muted':'#4F6680','blue':'#2877CC','teal':'#16866D','gold':'#A87615','purple':'#7752B8'}
def rect(x,y,w,h,fill,stroke=None,r=18):
 d.rounded_rectangle((x,y,x+w,y+h),radius=r,fill=fill,outline=stroke,width=2)
 svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke or "none"}" stroke-width="2"/>')
def text(x,y,s,n=28,c=None,mw=None):
 c=c or C['ink'];f=ImageFont.truetype(str(FONT),n)
 if mw and d.textlength(s,font=f)>mw:errors.append(s)
 d.text((x,y),s,font=f,fill=c)
 svg.append(f'<text x="{x}" y="{y+n}" font-family="Open Sans,Arial,sans-serif" font-size="{n}" fill="{c}">{html.escape(s)}</text>')
def path(points,c='#7391B5',dash=False,arrow=True,width=4):
 if dash:
  for (x1,y1),(x2,y2) in zip(points,points[1:]):
   dist=math.hypot(x2-x1,y2-y1)
   for a in range(0,int(dist),22):
    b=min(a+12,dist);d.line((x1+(x2-x1)*a/dist,y1+(y2-y1)*a/dist,x1+(x2-x1)*b/dist,y1+(y2-y1)*b/dist),fill=c,width=width)
 else:d.line(points,fill=c,width=width)
 svg.append('<polyline points="'+' '.join(f'{x},{y}' for x,y in points)+f'" fill="none" stroke="{c}" stroke-width="{width}"'+(' stroke-dasharray="12 10"' if dash else '')+'/>')
 if arrow:
  (x1,y1),(x2,y2)=points[-2:];a=math.atan2(y2-y1,x2-x1);p=[(x2,y2),(x2-17*math.cos(a-.48),y2-17*math.sin(a-.48)),(x2-17*math.cos(a+.48),y2-17*math.sin(a+.48))];d.polygon(p,fill=c);svg.append('<polygon points="'+' '.join(f'{x},{y}' for x,y in p)+f'" fill="{c}"/>')
def icon(name,x,y,size=72):
 p=ROOT/'gcp-icons'/f'{name}.png'
 a=Image.open(p).convert('RGBA');a.thumbnail((size,size),Image.Resampling.LANCZOS);im.paste(a,(int(x),int(y)),a)
 b=base64.b64encode(p.read_bytes()).decode();svg.append(f'<image x="{x}" y="{y}" width="{size}" height="{size}" href="data:image/png;base64,{b}"/>')
def node(x,y,w,h,title,service,lines,ic,accent='blue'):
 rect(x+4,y+6,w,h,'#E7EEF8');rect(x,y,w,h,'#FFFFFF','#C6D7E9')
 icon(ic,x+22,y+22,70);text(x+112,y+19,title,31,C[accent],mw=w-133);text(x+112,y+66,service,24,C['muted'],mw=w-133)
 for i,s in enumerate(lines):text(x+25,y+126+39*i,s,27,C['muted'],mw=w-50)
def small_source(x,title,lines,kind):
 w=860;y=225;rect(x,y,w,195,'#FFFFFF','#C6D7E9')
 # familiar simple source icons, distinct from cloud service logos
 ix=x+30;iy=y+34;c=C['blue']
 if kind=='doc':
  rect(ix,iy,58,76,'#EDF5FF',c,4)
  for dy in [22,38,54]:path([(ix+12,iy+dy),(ix+46,iy+dy)],c,arrow=False,width=3)
 elif kind=='image':
  rect(ix,iy,82,64,'#EDF5FF',c,5);path([(ix+8,iy+52),(ix+28,iy+31),(ix+43,iy+46),(ix+58,iy+26),(ix+74,iy+52)],c,arrow=False,width=3)
 elif kind=='store':
  rect(ix+8,iy+22,68,55,'#EDF5FF',c,4);path([(ix,iy+22),(ix+12,iy),(ix+72,iy),(ix+84,iy+22),(ix,iy+22)],c,arrow=False,width=3);rect(ix+31,iy+43,20,34,'#FFFFFF',c,2)
 else:
  for j,hh in enumerate([30,56,76]):rect(ix+j*26,iy+80-hh,17,hh,'#84B6EB',None,3)
 text(x+135,y+22,title,31,mw=700)
 for i,s in enumerate(lines):text(x+135,y+78+i*39,s,25,C['muted'],mw=700)
text(80,35,'CREATIVE INTELLIGENCE',62)
text(80,116,'Google Cloud component architecture  |  Multimodal RAG + product and commerce analytics',32,C['muted'])
for x,t,l,k in [(100,'Product + brand knowledge',['PIM / catalog, ingredients, usage, claims','Brand documents and market rules'],'doc'),(1080,'Images + creative assets',['Original product photos and pack shots','Campaign creatives and visual references'],'image'),(2060,'Commerce + store systems',['Orders, returns, stock, costs and revenue','SKU / store / market / channel keys'],'store'),(3040,'Marketing + performance',['Ad spend, audiences and conversions','Experiments, attribution and feedback'],'chart')]:small_source(x,t,l,k)
# Cloud project boundary
rect(60,480,3880,2050,'#FFFFFF','#AFC7E0',25)
text(1500,488,'GOOGLE CLOUD PROJECT  /  DOMAIN SERVICES AND DATA PLANE',23,C['muted'])
text(100,540,'01  BACKGROUND PREPARATION',32,C['teal'])
# preparation layer
px=[100,880,1660,2440,3220]; pw=680;py=620
node(px[0],py,pw,270,'Intake service','Cloud Run + GCS',['Protected upload / source connectors','Type validation, dedupe and SKU mapping','Original bytes + source identity'], 'cloud_run','teal')
node(px[1],py,pw,270,'Preparation orchestration','Workflows + Cloud Scheduler',['Scheduled sync and source-change jobs','Invoke workers with stable execution IDs','Stage status, bounded retry and recovery'],'workflows','teal')
node(px[2],py,pw,270,'Document + image workers','Cloud Run Jobs',['Extract, segment and classify sources','Prepare product crops / image variants','Record parser and asset lineage'],'cloud_run','teal')
node(px[3],py,pw,270,'Embedding workers','Vertex AI',['Versioned text + multimodal profiles','Document / image vectors and metadata','Persist provider outcomes and usage'],'vertexai','teal')
node(px[4],py,pw,270,'Analytics transformations','BigQuery SQL + Cloud Run Jobs',['Normalize sales, returns, spend and stock','Reconcile dimensions, currency and time','Build curated facts and metric views'],'bigquery','gold')
# prep arrows: source bars to intake and analytics
path([(530,420),(530,455),(440,455),(440,620)],C['teal'])
path([(1510,420),(1510,445),(455,445),(455,620)],C['teal'])
path([(2490,420),(2490,455),(3540,455),(3540,620)],C['gold'])
path([(3470,420),(3470,445),(3560,445),(3560,620)],C['gold'])
for i in range(3):path([(px[i]+pw,755),(px[i+1],755)],C['teal'])
# workflow controls analytics using top lane
path([(1220,620),(1220,595),(3500,595),(3500,620)],C['teal'],dash=True)
# storage layer
text(100,960,'02  AUTHORITATIVE STORAGE',32,C['blue'])
sy=1040;sw=1200
node(100,sy,sw,270,'Cloud Storage','Original and derived bytes',['Source documents + product images + campaign assets','Immutable generations, content hashes and retention','Actual image bytes remain available to model / renderer'],'cloud_storage')
node(1400,sy,sw,270,'BigQuery knowledge + evidence','Corpus, vectors and permission records',['Product facts, chunks, image metadata and vector profiles','Scoped claims / asset reviews, readiness and revocations','Source bindings, prompt versions and durable job records'],'bigquery')
node(2700,sy,sw,270,'BigQuery commerce marts','Governed analytical facts and views',['Product / store / campaign dimensions; sales and ad facts','Net revenue, margin, returns, availability, CVR and ROAS','Metric definitions, attribution windows and freshness'],'bigquery','gold')
path([(440,890),(440,930),(800,930),(800,1040)],C['teal']);path([(2780,890),(2780,925),(2050,925),(2050,1040)],C['teal']);path([(3560,890),(3560,1040)],C['gold'])
# main online flow
text(100,1380,'03  CAMPAIGN REQUEST AND EXECUTION',32,C['blue'])
text(2300,1383,'Solid: data / request flow     Dashed: orchestration or feedback',25,C['muted'])
ox=[100,750,1400,2050,2700,3350];ow=550;oy=1540
node(ox[0],oy,ow,320,'Campaign studio','Cloud Run + IAP',['Product + campaign brief','Audience, market and channel','Clarification + saved progress','Authenticated marketer'],'identity-aware_proxy')
node(ox[1],oy,ow,320,'Campaign worker','Cloud Run + Workflows',['Readiness and scope checks','Durable job + execution budget','Coordinate bounded stages','Reconnect and original recovery'],'cloud_run')
node(ox[2],oy,ow,320,'Grounding service','BigQuery + Vertex AI',['Eligible lexical / vector search','Semantic reranking + typed SQL','SKU / store / time filters','Bound evidence + metric bundle'],'bigquery')
node(ox[3],oy,ow,320,'Campaign planner','Vertex AI Gemini',['Evidence + original images','Concept, copy and layout JSON','Exact prompt / model versions','Traceable source references'],'vertexai')
node(ox[4],oy,ow,320,'Visual production','Vertex AI + Cloud Run',['Generate scene from references','Preserve actual product pixels','Compose copy, fonts and layout','Persist banner + render manifest'],'vertexai')
node(ox[5],oy,ow,320,'Validate + deliver','Evaluators + review service',['Fidelity, claims, safety and brand','Format checks + bounded fixes','Fresh access and approval gate','Private preview + download'],'domain_validation')
for i in range(5):path([(ox[i]+ow,1700),(ox[i+1],1700)],C['blue'])
# store-to-grounding routes; labels in open band
path([(2000,1310),(2000,1450),(1630,1450),(1630,1540)],C['blue']);text(1700,1415,'RAG evidence',24,C['blue'])
path([(3300,1310),(3300,1485),(1760,1485),(1760,1540)],C['gold']);text(2640,1448,'Scoped analytics / governed SQL',24,C['gold'])
path([(1100,1310),(1100,1510),(2950,1510),(2950,1540)],C['teal']);text(100,1470,'Generation-pinned product image bytes',24,C['teal'])
# bounded corrections rail
path([(3630,1860),(3630,1910),(2325,1910),(2325,1860)],C['purple'],dash=True);text(2550,1871,'Bounded correction with recorded reasons',24,C['purple'])
# execution records and outputs
node(100,2010,1830,260,'Execution evidence','BigQuery ledger + GCS artifacts',['Input manifests, provider responses, model IDs and usage','Original-job recovery, artifact generations and measured scores','Shared record authority across every execution stage'],'bigquery')
node(2070,2010,1830,260,'Campaign outputs','Protected campaign studio',['Campaign concepts, copy, banners and format variants','Source citations + business-metric context + validation results','Human review and approved downloads'],'cloud_run')
path([(1025,1860),(1025,2010)],C['blue']);text(1050,1945,'Stage records',24,C['blue'])
path([(3750,1860),(3750,2010)],C['blue']);text(3450,1945,'Validated artifacts',24,C['blue'])
rect(100,2340,3800,130,'#EEF3FC','#C7D6EA')
icon('cloud_monitoring',126,2365,65);text(220,2354,'FEEDBACK AND OUTCOME ANALYTICS',29,C['purple'])
text(220,2408,'Marketer feedback + campaign / creative / SKU IDs + measured store and channel results feed evaluation and subsequent analytics.',27,C['muted'],mw=3620)
path([(2985,2270),(2985,2340)],C['purple'],dash=True)
path([(3900,2405),(3920,2405),(3920,1190),(3900,1190)],C['purple'],dash=True)
# platform plane
text(80,2590,'PLATFORM SERVICES  /  APPLIES ACROSS ALL FLOWS',32)
ps=[('Identity + privacy','identity_and_access_management',['IAM / IAP / service accounts','Scope, retention and region rules']),('Secrets + configuration','secret_manager',['Secret Manager + versioned settings','Prompt / model / policy bindings']),('CI/CD + deployment','cloud_build',['Cloud Build + Artifact Registry','Immutable Cloud Run releases']),('Evaluation + review','vertexai',['Retrieval and artifact benchmarks','Human review + quality gates']),('Observability','cloud_logging',['Cloud Logging + Monitoring','Stage latency, failures and usage']),('Cost + reliability','billing',['Budgets, limits and query costs','Idempotency + uncertainty recovery'])]
for i,(title,ic,ls) in enumerate(ps):
 x=80+i*650;rect(x,2660,590,220,'#FFFFFF','#C6D7E9');icon(ic,x+18,2678,58);text(x+94,2675,title,26,mw=475)
 for j,l in enumerate(ls):text(x+22,2752+j*42,l,23,C['muted'],mw=550)
text(80,2930,'Component architecture  •  Google Cloud service icons + domain symbols  •  BigQuery: data and vector authority  •  GCS: source and artifact bytes',25,C['muted'])
assert not errors,errors
svg.append('</svg>');(ROOT/'creative-intelligence-architecture-icons-v4.svg').write_text('\n'.join(svg));im.save(ROOT/'creative-intelligence-architecture-icons-v4.png')
print('Exported PNG + editable SVG; text fit verified.')
