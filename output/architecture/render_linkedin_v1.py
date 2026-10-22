from PIL import Image,ImageDraw,ImageFont
from pathlib import Path
W,H=1800,1960
im=Image.new('RGB',(W,H),'#0B1424');d=ImageDraw.Draw(im)
font='assets/fonts/campaign/opensans.ttf'
def f(n):return ImageFont.truetype(font,n)
def text(x,y,s,n=30,c='#EAF0FA'):d.text((x,y),s,font=f(n),fill=c,spacing=12)
def card(x,y,w,h,title,lines,accent):
 d.rounded_rectangle((x,y,x+w,y+h),radius=20,fill='#152238',outline='#2A3C55',width=2)
 d.rounded_rectangle((x,y,x+7,y+h),radius=3,fill=accent)
 text(x+24,y+22,title,29)
 for i,line in enumerate(lines):text(x+24,y+78+i*39,line,24,'#BFCDE0')
def arrow(y):
 d.line((900,y,900,y+40),fill='#7790B4',width=5);d.polygon([(889,y+30),(911,y+30),(900,y+44)],fill='#7790B4')
text(80,62,'CREATIVE INTELLIGENCE',25,'#57D9C2')
text(80,111,'From product knowledge',65)
text(80,193,'to grounded campaigns',65)
text(80,292,'Select a product. Describe the brief. Receive a campaign.',30,'#BFCDE0')
d.rounded_rectangle((80,362,1720,435),radius=18,fill='#20364A')
text(108,379,'TARGET ARCHITECTURE  /  Proposed capabilities, not a delivery claim',26,'#77E5D1')
text(80,482,'01   PREPARE KNOWLEDGE IN THE BACKGROUND',31,'#57D9C2')
xs=[80,500,920,1340];cw=380
items=[('Ingest',['Product facts + documents','Images + brand rules','Creatives + performance']),('Process',['Extract + classify','Prepare product imagery','Version text/image vectors']),('Review',['Verify source provenance','Scope claims + asset use','Track changes + expiry']),('Make ready',['Searchable product corpus','Readiness by campaign use','Clear gaps for review'])]
for x,(a,b) in zip(xs,items):card(x,548,cw,235,a,b,'#57D9C2')
arrow(800)
d.rounded_rectangle((80,862,1720,1017),radius=20,fill='#182D46',outline='#385579',width=2)
text(110,881,'SHARED DATA FOUNDATION',25,'#82B8FF')
text(110,930,'BigQuery: facts, vectors, evidence, permissions, analytics and job records',26)
text(110,971,'GCS: original files, processed assets and generated campaign artifacts',24,'#BFCDE0')
arrow(1033)
text(80,1103,'02   GENERATE FOR THE CAMPAIGN BRIEF',31,'#82B8FF')
items=[('Understand',['Product + campaign brief','Necessary clarification','Current readiness check']),('Ground',['Eligible hybrid retrieval','Semantic reranking','Traceable evidence bundle']),('Create',['Gemini plans copy + layout','Vertex generates visuals','Preserve real product']),('Validate + deliver',['Claims, fidelity + safety','Brand + format checks','Preview + evidence'])]
for x,(a,b) in zip(xs,items):card(x,1170,cw,235,a,b,'#82B8FF')
text(80,1460,'03   RUN RELIABLY. MEASURE. IMPROVE.',31,'#D3ACFF')
items=[('Durable execution',['Background workers','Reconnect + recovery']),('Governance',['Access + privacy','Versioned approvals']),('Quality + cost',['Retrieval + artifact evals','Latency + provider usage']),('Feedback',['Marketer review','Measured real outcomes'])]
for x,(a,b) in zip(xs,items):card(x,1527,cw,194,a,b,'#D3ACFF')
text(80,1770,'Marketer experience',28,'#57D9C2')
text(80,1820,'Choose product  →  Enter brief  →  Review campaign',37)
text(80,1900,'Google Cloud  •  BigQuery  •  GCS  •  Cloud Run  •  Vertex AI',25,'#8EA5C3')
im.save('output/architecture/creative-intelligence-future-state-linkedin.png')
