from PIL import Image,ImageDraw,ImageFont
from pathlib import Path
OUT=Path('output/architecture');OUT.mkdir(parents=True,exist_ok=True)
FONT='assets/fonts/campaign/opensans.ttf'
BG='#0B1424';INK='#EAF0FA';MUTED='#BCCCE0';TEAL='#57D9C2';BLUE='#82B8FF';GOLD='#F1C978';PURPLE='#D3ACFF'
def canvas(w,h):
 im=Image.new('RGB',(w,h),BG);return im,ImageDraw.Draw(im)
def txt(d,x,y,s,n=28,c=INK):d.text((x,y),s,font=ImageFont.truetype(FONT,n),fill=c,spacing=10)
def card(d,x,y,w,h,title,lines,c=TEAL,n=25):
 d.rounded_rectangle((x,y,x+w,y+h),radius=18,fill='#152238',outline='#2A3C55',width=2)
 d.rectangle((x,y+16,x+6,y+h-16),fill=c)
 txt(d,x+23,y+18,title,29,c)
 for i,line in enumerate(lines):txt(d,x+23,y+72+i*37,line,n,MUTED)
def arrow(d,x1,y1,x2,y2,c='#7891B4'):
 d.line((x1,y1,x2,y2),fill=c,width=4)
 if x1==x2:d.polygon([(x2-9,y2-13),(x2+9,y2-13),(x2,y2)],fill=c)
 else:d.polygon([(x2-13,y2-9),(x2-13,y2+9),(x2,y2)],fill=c)
def bar(d,y,title,body,c=BLUE):
 d.rounded_rectangle((80,y,1920,y+108),radius=17,fill='#1B3049',outline='#355273',width=2)
 txt(d,105,y+13,title,27,c);txt(d,105,y+59,body,25)
# Capability view
im,d=canvas(2000,2350)
txt(d,80,55,'CREATIVE INTELLIGENCE',26,TEAL)
txt(d,80,105,'Product knowledge + business intelligence',60)
txt(d,80,185,'Grounded campaigns with measurable business context',34,MUTED)
txt(d,80,290,'01   BUILD A READY PRODUCT AND BUSINESS FOUNDATION',31,TEAL)
xs=[80,550,1020,1490];w=430
cards=[('Product + brand',['Catalog, ingredients + usage','Product images + documents','Brand rules + claim reviews']),('Commerce + stores',['Orders, returns + discounts','Revenue and margin by SKU','Store, channel + inventory']),('Campaign performance',['Creative history + audiences','Spend, CTR, CVR + ROAS','Attribution + experiment data']),('Background preparation',['Extract + classify + embed','Normalize commerce metrics','Review use + track freshness'])]
for x,(t,l) in zip(xs,cards):card(d,x,350,w,235,t,l,TEAL)
bar(d,635,'SHARED DATA FOUNDATION','BigQuery: knowledge, vectors, commerce and evidence   |   GCS: source files, images and artifacts')
arrow(d,1000,758,1000,800)
txt(d,80,825,'02   GROUND THE CAMPAIGN IN TWO COMPLEMENTARY WAYS',31,BLUE)
card(d,80,890,890,245,'Evidence retrieval',['Find relevant product facts, brand rules and creative references','Filter by SKU, market, channel and permitted use','Use hybrid search + semantic reranking'],BLUE)
card(d,1030,890,890,245,'Business analytics',['Query product revenue, margin, trends and stock availability','Compare stores, markets, channels and campaign cohorts','Retain time window, currency, metric definition and freshness'],GOLD)
arrow(d,520,1150,520,1190);arrow(d,1475,1150,1475,1190)
bar(d,1210,'BOUND CAMPAIGN CONTEXT','Brief + eligible evidence + real product assets + scoped, timestamped business metrics',GOLD)
txt(d,80,1370,'03   CREATE, VALIDATE AND DELIVER',31,BLUE)
cards=[('Understand + plan',['Product, objective + audience','Evidence-backed concepts','Copy + layout + rationale']),('Generate visuals',['Vertex visual generation','Preserve real product identity','Format-specific composition']),('Validate + review',['Claims, brand + safety','Product fidelity + layout','Bounded fixes + approval']),('Deliver + measure',['Chat preview + download','Sources + metric references','Feedback + real outcomes'])]
for x,(t,l) in zip(xs,cards):card(d,x,1430,w,235,t,l,BLUE)
txt(d,80,1725,'BUSINESS QUESTIONS THIS CAN SUPPORT',30,GOLD)
card(d,80,1785,890,185,'What should we promote?',['Products with relevant demand, stock and margin signals','Recommendations constrained by approved evidence'],GOLD)
card(d,1030,1785,890,185,'What should inform the creative?',['Relevant audience, store and channel performance','Measured results with explicit attribution limits'],GOLD)
bar(d,2020,'PLATFORM CONTROLS','Access + privacy  •  Durable jobs + recovery  •  Versions + approvals  •  Quality, latency + cost',PURPLE)
txt(d,80,2180,'Choose product  →  Describe objective  →  Review grounded campaign',36,TEAL)
txt(d,80,2270,'Business metrics inform decisions. They do not substantiate product efficacy claims.',25,MUTED)
im.save(OUT/'creative-intelligence-capabilities-v2.png')
# Technical view
im,d=canvas(2400,2650)
txt(d,70,45,'CREATIVE INTELLIGENCE  /  TECHNICAL ARCHITECTURE',31,TEAL)
txt(d,70,103,'Multimodal RAG + governed business analytics',57)
txt(d,70,185,'Background ingestion and preparation  •  Request-time grounding  •  Durable campaign execution',28,MUTED)
# columns width 720 x 3
xs=[70,840,1610];w=720
for x,t,c in zip(xs,['SOURCE SYSTEMS','PREPARATION AND STORAGE','CAMPAIGN SERVING'],[TEAL,GOLD,BLUE]):txt(d,x,280,t,30,c)
card(d,70,340,w,255,'Product and content sources',['PIM / catalog / CMS / brand documents','Original pack shots and creative assets','Claims, formula versions and usage rules','Source IDs, owners and SKU mappings'],TEAL)
card(d,70,640,w,290,'Commerce and marketing sources',['Orders, order lines, returns and discounts','Product costs, inventory and store catalog','Ad platforms, spend and creative IDs','Web events, conversions and experiments','Product, store, channel and campaign keys'],GOLD)
card(d,70,975,w,240,'Ingestion contracts',['Protected uploads + scheduled connectors','Schema checks, dedupe and reconciliation','Event time, source version and lineage','Immutable original bytes in GCS'],TEAL)
arrow(d,790,470,830,470);arrow(d,790,780,830,780)
card(d,840,340,w,255,'Durable preparation orchestration',['Workflows + Cloud Run workers (proposed)','Versioned jobs, retries and unknown holds','Extract text, classify and prepare images','Text / image embeddings through Vertex AI'],TEAL)
card(d,840,640,w,290,'BigQuery analytical foundation',['Raw / staging / curated data layers','Product, store and campaign dimensions','Sales, returns, inventory and ad-spend facts','Governed revenue, margin and ROAS views','Currency, grain, window and attribution rules'],GOLD)
card(d,840,975,w,240,'Knowledge and evidence authority',['BigQuery facts, chunks and vector tables','Scoped claims, asset use and revocations','Readiness, freshness and source versions','GCS originals, derivatives and artifacts'],TEAL)
card(d,1610,340,w,255,'Protected campaign API + chat',['IAP / application authorization','Product + market + channel + brief','Admit durable job and execution budget','Reconnect to saved progress and results'],BLUE)
card(d,1610,640,w,290,'Two governed grounding paths',['RAG: scoped lexical + vector candidates','Semantic reranking + original image fetch','Analytics: typed parameters / vetted SQL','SKU, store, cohort and time-window filters','BigQuery query IDs + result snapshots'],GOLD)
card(d,1610,975,w,240,'Immutable campaign input manifest',['Brief + evidence IDs + source generations','Approved wording + image bytes / hashes','Metric values, definitions and query IDs','Prompt / model versions + freshness pins'],BLUE)
arrow(d,1560,780,1600,780);arrow(d,1560,1090,1600,1090)
# lower pipeline
for x in [430,1200,1970]:arrow(d,x,1235,x,1280)
# full width crossline
liney=1320
d.line((430,liney,1970,liney),fill='#7891B4',width=4);arrow(d,1200,1320,1200,1370)
txt(d,70,1385,'DURABLE GENERATION AND QUALITY PIPELINE',31,BLUE)
# 4 cards width 535
x4=[70,645,1220,1795];w4=535
cards=[('Plan',['Gemini campaign planner','Bound evidence + business context','Structured concept, copy + layout','No inferred claim permissions']),('Generate + compose',['Vertex image generation','Real product asset preservation','Versioned fonts and render config','Generation-pinned GCS output']),('Validate + correct',['Claims, fidelity, brand + safety','OCR, geometry + format checks','Bounded correction / human review','Measured scoring + durable records']),('Serve + learn',['Fresh access + delivery checks','Private preview + download','Marketer feedback and experiments','Observed outcomes, not promises'])]
for x,(t,l) in zip(x4,cards):card(d,x,1450,w4,270,t,l,BLUE,23)
for x in [610,1185,1760]:arrow(d,x,1585,x+25,1585)
txt(d,70,1780,'PLATFORM SERVICES AND CROSS-CUTTING CONTROLS',31,PURPLE)
cards=[('Execution authority',['BigQuery jobs, intents + outcomes','Stable IDs and original-job recovery','One orchestration owner','No blind repeat of uncertain calls']),('Security + governance',['IAM / IAP + narrow service roles','Secret Manager + retention controls','Market, region and use restrictions','Review expiry + revocation checks']),('LLMOps + evaluation',['Versioned prompts and profiles','Retrieval + artifact benchmarks','Provider identity, usage + hashes','Quality holds and release decisions']),('Operations + feedback',['Cloud Logging / Monitoring','Stage timings, failures + budgets','Source drift + readiness alerts','Controlled evaluation of changes'])]
for x,(t,l) in zip(x4,cards):card(d,x,1845,w4,270,t,l,PURPLE,23)
d.rounded_rectangle((70,2175,2330,2325),radius=18,fill='#1B3049',outline='#355273',width=2)
txt(d,95,2194,'AUTHORITY BOUNDARIES',27,GOLD)
txt(d,95,2242,'BigQuery = data, vectors and durable business records   |   GCS = immutable source and artifact bytes',26)
txt(d,95,2283,'Orchestration coordinates execution. Retrieved similarity and commercial success do not approve claims.',25,MUTED)
txt(d,70,2395,'Analytics remains structured and queryable; daily transaction rows are not embedded as narrative knowledge.',26,MUTED)
txt(d,70,2450,'Use aggregate metrics at request time, with explicit scope, provenance, freshness and attribution limitations.',26,MUTED)
txt(d,70,2550,'Google Cloud  •  BigQuery  •  GCS  •  Cloud Run  •  Workflows  •  Vertex AI',29,TEAL)
im.save(OUT/'creative-intelligence-technical-architecture-v1.png')
print('Saved capability and technical PNG diagrams.')
