from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
import html, base64, io, math, zipfile
ROOT=Path(__file__).resolve().parent
FONT=Path('assets/fonts/campaign/opensans.ttf'); W,H=3600,2360
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

# Distinct functional icons supplement official Google Cloud service icons.
def symbol(kind,x,y,c='#3166A2',size=76):
    s=size/80
    def pts(a):return [(x+u*s,y+v*s) for u,v in a]
    def ln(a):path(pts(a),c,arrow=False,width=4)
    def rr(a,b,w,h):rect(x+a*s,y+b*s,w*s,h*s,'#F8FBFF',c,5)
    if kind=='document':
        rr(16,4,49,69)
        for yy in (25,39,53):ln([(27,yy),(54,yy)])
    elif kind=='image':
        rr(3,12,74,57);ln([(10,59),(29,36),(43,50),(56,30),(72,59)])
    elif kind=='store':
        rr(10,31,60,44);ln([(3,31),(14,6),(66,6),(77,31),(3,31)]);rr(31,49,18,26)
    elif kind=='chart':
        for a,b in [(8,33),(30,53),(52,71)]:rect(x+a*s,y+(76-b)*s,14*s,b*s,c,None,3)
    elif kind=='extract':
        rr(3,4,40,58)
        for yy in (20,32,44):ln([(11,yy),(34,yy)])
        ln([(41,61),(72,61)]);ln([(61,49),(73,61),(61,73)])
    elif kind=='chat':
        rr(2,7,74,51);ln([(20,58),(20,75),(38,58)])
        for yy in (24,39):ln([(15,yy),(62,yy)])
    elif kind=='search':
        # SVG and PNG circles share the same geometry.
        d.ellipse((x+5*s,y+5*s,x+55*s,y+55*s),outline=c,width=5)
        svg.append(f'<circle cx="{x+30*s}" cy="{y+30*s}" r="{25*s}" fill="none" stroke="{c}" stroke-width="5"/>')
        ln([(48,48),(75,75)]);ln([(16,40),(16,29),(27,29),(27,20),(39,20)])
    elif kind=='render':
        rr(3,8,74,62);ln([(10,60),(27,41),(40,54),(56,33),(69,48)])
        ln([(52,4),(52,22)]);ln([(43,13),(61,13)])
    elif kind=='check':
        ln([(40,3),(73,16),(67,51),(40,76),(13,51),(7,16),(40,3)])
        ln([(24,37),(36,49),(58,25)])
    elif kind=='download':
        ln([(40,4),(40,52),(25,37)]);ln([(40,52),(55,37)]);ln([(9,49),(9,72),(71,72),(71,49)])
    elif kind=='ledger':
        rr(9,6,62,68)
        for yy in (24,40,56):ln([(18,yy),(25,yy)]);ln([(35,yy),(61,yy)])
    elif kind=='feedback':
        ln([(14,25),(26,10),(60,10),(72,25),(72,42)]);ln([(61,31),(72,42),(80,28)])
        ln([(66,56),(54,71),(20,71),(8,56),(8,39)]);ln([(19,50),(8,39),(0,53)])
    else:raise ValueError(kind)

pal={
 'sources':('#F0F4F8','#8CA1B8','#405A74'),
 'prep':('#E4F5EE','#36A17B','#16775A'),
 'data':('#E7F1FF','#4F8ED1','#2465A8'),
 'campaign':('#F0E9FC','#9870CB','#6942A1'),
 'outputs':('#FFF2D8','#D9AA42','#966B14'),
 'platform':('#FCEAF0','#CE7F9B','#9C4568')}

def layer(y,h,key,label):
    fill,edge,ink=pal[key];rect(65,y,3470,h,fill,edge,22)
    rect(84,y+20,42,34,edge,None,8)
    text(143,y+17,label,26,ink)

blocks=[]
def card(x,y,w,h,bid,title,sub,ic,key,custom=False,description=''):
    fill,edge,ink=pal[key];rect(x+3,y+4,w,h,'#DDE6F0',None,15);rect(x,y,w,h,'#FFFFFF',edge,15)
    if custom:symbol(ic,x+22,y+(h-76)/2,ink)
    else:icon(ic,x+22,y+(h-76)/2,76)
    text(x+122,y+35,title,27,ink,mw=w-140)
    text(x+122,y+80,sub,21,C['muted'],mw=w-140)
    text(x+w-49,y+9,bid,17,'#75879A')
    blocks.append((bid,title,description or sub))

text(70,30,'CREATIVE INTELLIGENCE',52)
text(70,100,'Multimodal RAG + commerce analytics  |  Component architecture',28,C['muted'])
layer(165,210,'sources','01   DATA SOURCES')
for x,b,t,s,ic,desc in [
 (100,'S1','Product + brand','Catalog / documents','document','Product metadata, ingredients, usage instructions, brand rules and supporting documents.'),
 (970,'S2','Visual assets','Products / creatives','image','Original pack shots, approved derived imagery and historical campaign creatives.'),
 (1840,'S3','Commerce','Orders / stores / stock','store','Orders, order lines, refunds, discounts, product costs, store mappings and inventory.'),
 (2710,'S4','Performance','Spend / conversions','chart','Ad spend, audience and channel performance, attribution and experiment results.')]:card(x,235,790,120,b,t,s,ic,'sources',True,desc)

layer(440,280,'prep','02   BACKGROUND PREPARATION')
pre=[
 (100,'P1','Intake','Cloud Run','cloud_run',False,'Protected source intake validates file types and scope, maps SKU identities, deduplicates and stores immutable originals.'),
 (790,'P2','Orchestrate','Workflows / Scheduler','workflows',False,'Workflows coordinates durable preparation jobs; Cloud Scheduler starts scheduled sync. Stable job identities and recovery prevent blind repeat execution.'),
 (1480,'P3','Extract + classify','Cloud Run Jobs','extract',True,'Workers extract and segment text, classify images and prepare authorized derivatives while retaining provenance.'),
 (2170,'P4','Embed','Vertex AI','vertexai',False,'Versioned text and multimodal embeddings retain source, task, dimension, provider and usage records.'),
 (2860,'P5','Transform metrics','BigQuery SQL','chart',True,'Transforms commerce and marketing data into reconciled dimensions, facts and governed metric views.')]
for x,b,t,s,ic,cu,de in pre:card(x,535,640,150,b,t,s,ic,'prep',cu,de)
for i in range(3):path([(pre[i][0]+640,610),(pre[i+1][0],610)],pal['prep'][2])
# Data sources to preparation: connectors occupy clear gaps.
path([(495,375),(495,410),(420,410),(420,535)],pal['prep'][2])
path([(1365,375),(1365,397),(440,397),(440,535)],pal['prep'][2])
path([(2235,375),(2235,410),(3160,410),(3160,535)],pal['outputs'][2])
path([(3105,375),(3105,397),(3180,397),(3180,535)],pal['outputs'][2])
path([(1110,535),(1110,509),(3135,509),(3135,535)],pal['prep'][2],dash=True)

layer(810,240,'data','03   AUTHORITATIVE STORAGE')
data=[
 (100,'D1','Source + artifact bytes','Cloud Storage','cloud_storage','Stores original files, generation-pinned product images, derived assets and generated campaign artifacts.'),
 (1270,'D2','Knowledge + vectors','BigQuery','bigquery','Holds product facts, text chunks, asset metadata, embedding profiles, source bindings, scoped claims and review records.'),
 (2440,'D3','Commerce marts','BigQuery','bigquery','Curated product/store/campaign dimensions and sales, returns, stock and spend facts support revenue, margin, CVR and ROAS with explicit definitions.')]
for x,b,t,s,ic,de in data:card(x,880,1060,140,b,t,s,ic,'data',False,de)
path([(420,685),(420,760),(600,760),(600,880)],pal['prep'][2])
path([(2490,685),(2490,756),(1800,756),(1800,880)],pal['prep'][2])
path([(3180,685),(3180,880)],pal['outputs'][2])

layer(1190,290,'campaign','04   CAMPAIGN EXECUTION')
cs=[
 (100,'C1','Campaign studio','Chat / IAP','chat',True,'Marketers select a product and provide the brief, market, audience and formats; authenticated users can clarify and reconnect to saved work.'),
 (675,'C2','Coordinate','Cloud Run / Workflows','cloud_run',False,'Checks scope, readiness and execution limits, then coordinates durable generation stages and recovery.'),
 (1250,'C3','Ground','RAG + governed SQL','search',True,'Retrieves eligible lexical/vector candidates, reranks them and runs typed analytics queries. Binds selected evidence, metrics and query lineage to the brief.'),
 (1825,'C4','Plan','Vertex AI Gemini','vertexai',False,'Produces structured concepts, copy and layout using exact evidence, source images and versioned prompts.'),
 (2400,'C5','Render','Vertex AI + compositor','render',True,'Generates visual scenes, preserves the actual product asset and composes approved copy and format-specific layouts.'),
 (2975,'C6','Validate + release','Checks / human review','check',True,'Checks product fidelity, claims, safety, brand and format; supports bounded correction and scoped approval before delivery.')]
for x,b,t,s,ic,cu,de in cs:card(x,1290,525,150,b,t,s,ic,'campaign',cu,de)
for i in range(5):path([(cs[i][0]+525,1365),(cs[i+1][0],1365)],pal['campaign'][2])
# Three grounding inputs, with distinct routes and labels.
path([(1800,1020),(1800,1100),(1430,1100),(1430,1290)],pal['data'][2]);text(1480,1063,'Evidence',22,pal['data'][2])
path([(2970,1020),(2970,1140),(1565,1140),(1565,1290)],pal['outputs'][2]);text(2280,1104,'Revenue / margin / store metrics',22,pal['outputs'][2])
path([(600,1020),(600,1170),(2650,1170),(2650,1290)],pal['prep'][2]);text(700,1133,'Original product images',22,pal['prep'][2])
# Post-check correction branch.
path([(3237,1440),(3237,1530),(2087,1530),(2087,1440)],pal['campaign'][2],dash=True);text(2460,1491,'Bounded correction',22,pal['campaign'][2])

layer(1610,240,'outputs','05   RESULTS AND LEARNING')
outs=[
 (100,'O1','Execution evidence','BigQuery / GCS','ledger','Records exact inputs, attempts, provider responses, usage, original job IDs, artifact generations and measured scores.'),
 (1270,'O2','Campaign package','Preview / approved files','download','Delivers campaign concepts, copy, banners, format variants, source references, analytics context and validation records through protected access.'),
 (2440,'O3','Outcome feedback','Review / campaign results','feedback','Links human feedback and measured campaign results to campaign, creative and SKU IDs. Feeds governed analytics and evaluation of future changes.')]
for x,b,t,s,ic,de in outs:card(x,1685,1060,140,b,t,s,ic,'outputs',True,de)
path([(937,1440),(937,1565),(600,1565),(600,1685)],pal['campaign'][2])
path([(3390,1440),(3390,1580),(1800,1580),(1800,1685)],pal['campaign'][2]);text(1850,1543,'Approved artifacts',22,pal['campaign'][2])
path([(2330,1755),(2440,1755)],pal['outputs'][2])
path([(3500,1755),(3555,1755),(3555,950),(3500,950)],pal['outputs'][2],dash=True)

layer(1980,240,'platform','06   PLATFORM SERVICES')
services=[
 ('X1','Identity','IAM / IAP','identity_and_access_management','Identity, workspace and resource authorization; narrow service-account permissions and data access boundaries.'),
 ('X2','Secrets','Secret Manager','secret_manager','Protected configuration and secrets, with versioned prompt/model/policy bindings.'),
 ('X3','CI/CD','Build / Registry','cloud_build','Focused verification, controlled source packaging, immutable images and Cloud Run deployment/rollback.'),
 ('X4','Evaluation','Quality benchmarks','domain_validation','Independent retrieval and artifact evaluation, adversarial cases, human review and version-specific quality gates.'),
 ('X5','Observability','Logging / Monitoring','cloud_logging','Correlated stage progress, latency, failures, safe diagnostics, readiness and freshness alerts.'),
 ('X6','Cost + recovery','Budgets / durable jobs','billing','Tracks provider/query/compute usage, bounds resource use and preserves uncertain operation recovery.')]
for i,(b,t,s,ic,de) in enumerate(services):card(100+i*575,2050,525,140,b,t,s,ic,'platform',False,de)
text(90,2278,'Solid arrows: data / request flow     Dashed arrows: orchestration, correction or feedback',23,C['muted'])
text(90,2320,'Google Cloud service icons + domain symbols  |  Block IDs map to the separate component reference.',21,C['muted'])
assert not errors,errors
svg.append('</svg>')
(ROOT/'creative-intelligence-architecture-compact-v5.svg').write_text('\n'.join(svg))
im.save(ROOT/'creative-intelligence-architecture-compact-v5.png')
with (ROOT/'creative-intelligence-component-reference-v5.txt').open('w') as out:
    out.write('CREATIVE INTELLIGENCE — COMPONENT REFERENCE\nCompanion to creative-intelligence-architecture-compact-v5.png / .svg\n\n')
    for bid,title,desc in blocks:out.write(f'{bid} | {title}\n{desc}\n\n')
    out.write('FLOW NOTES\nEvidence retrieval and governed analytics supply different inputs to campaign grounding.\nActual generation-pinned product images feed visual production.\nAnalytics retain their SKU/store/market scope, time window, metric definition and freshness.\nCommercial outcomes do not substantiate product efficacy claims.\nThe orchestrator coordinates execution; BigQuery remains the business-record and vector authority, and GCS stores bytes.\n')
print('Saved compact PNG, editable SVG and separate component reference; text-fit checks passed.')
