from pathlib import Path
from PIL import Image,ImageDraw,ImageFont
import html,math
ROOT=Path(__file__).resolve().parent;W,H=3600,2950
im=Image.new('RGB',(W,H),'#F7FAFF');d=ImageDraw.Draw(im);font='assets/fonts/campaign/opensans.ttf'
svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}"><rect width="100%" height="100%" fill="#F7FAFF"/>'];errors=[]
colors=['#216BC3','#13866C','#7953B5','#AC7417'];xs=[240,1070,1900,2730];cw=740

def rect(x,y,w,h,fill,stroke=None,r=16):
 d.rounded_rectangle((x,y,x+w,y+h),radius=r,fill=fill,outline=stroke,width=2);svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke or "none"}" stroke-width="2"/>')
def text(x,y,s,n=27,c='#18334F',mw=None):
 f=ImageFont.truetype(font,n)
 if mw and d.textlength(s,font=f)>mw:errors.append(s)
 d.text((x,y),s,font=f,fill=c);svg.append(f'<text x="{x}" y="{y+n}" font-family="Open Sans,Arial,sans-serif" font-size="{n}" fill="{c}">{html.escape(s)}</text>')
def path(points,c='#6F8BAA',dash=False,arrow=True):
 for (x1,y1),(x2,y2) in zip(points,points[1:]):
  dist=math.hypot(x2-x1,y2-y1)
  if dash:
   for a in range(0,int(dist),22):
    b=min(a+12,dist);d.line((x1+(x2-x1)*a/dist,y1+(y2-y1)*a/dist,x1+(x2-x1)*b/dist,y1+(y2-y1)*b/dist),fill=c,width=4)
  else:d.line((x1,y1,x2,y2),fill=c,width=4)
 svg.append('<polyline points="'+' '.join(f'{x},{y}' for x,y in points)+f'" fill="none" stroke="{c}" stroke-width="4"'+(' stroke-dasharray="12 10"' if dash else '')+'/>')
 if arrow:
  (a,b),(x,y)=points[-2:];t=math.atan2(y-b,x-a);pts=[(x,y),(x-17*math.cos(t-.5),y-17*math.sin(t-.5)),(x-17*math.cos(t+.5),y-17*math.sin(t+.5))];d.polygon(pts,fill=c);svg.append('<polygon points="'+' '.join(f'{x},{y}' for x,y in pts)+f'" fill="{c}"/>')
def role_icon(x,y,c,kind):
 if kind in ('person','creative'):
  d.ellipse((x+22,y,x+57,y+35),outline=c,width=4);svg.append(f'<circle cx="{x+39.5}" cy="{y+17.5}" r="17.5" fill="none" stroke="{c}" stroke-width="4"/>');rect(x+6,y+45,68,38,'#FFFFFF',c,14)
  if kind=='creative':path([(x+72,y+74),(x+96,y+40)],c,arrow=False)
 elif kind=='platform':
  for dx,dy in [(0,0),(55,0),(0,55),(55,55)]:rect(x+dx,y+dy,25,25,'#FFFFFF',c,5)
  path([(x+25,y+12),(x+55,y+12)],c,arrow=False);path([(x+12,y+25),(x+12,y+55)],c,arrow=False);path([(x+67,y+25),(x+67,y+55)],c,arrow=False)
 else:
  pts=[(x+40,y),(x+78,y+17),(x+70,y+58),(x+40,y+85),(x+10,y+58),(x+2,y+17),(x+40,y)];path(pts,c,arrow=False);path([(x+21,y+41),(x+35,y+56),(x+59,y+28)],c,arrow=False)
def node(col,y,title,lines,step=None,h=230):
 x=xs[col]+20;w=cw-40;rect(x+3,y+5,w,h,'#E7EEF7');rect(x,y,w,h,'#FFFFFF','#CBD9E9')
 off=25
 if step is not None:rect(x+22,y+20,48,48,colors[col],r=24);text(x+35,y+25,str(step),28,'#FFFFFF');off=88
 text(x+off,y+23,title,31,colors[col],mw=w-off-20)
 for i,line in enumerate(lines):text(x+25,y+86+i*39,line,26,'#4D657D',mw=w-50)
def note(col,y,title,lines):
 x=xs[col]+30;text(x,y,title,27,colors[col],mw=cw-60)
 for i,s in enumerate(lines):text(x,y+49+i*37,s,24,'#5B7087',mw=cw-60)
text(75,36,'FROM BRIEF TO GROUNDED CAMPAIGN',60)
text(75,118,'How marketing, creative and brand teams collaborate through the platform',32,'#526A84')
# lane backgrounds and role headings
roles=[('MARKETING TEAM','Objective, audience and business approval','person'),('CREATIVE INTELLIGENCE','Grounding, generation and validation','platform'),('CREATIVE TEAM','Direction, design and revisions','creative'),('BRAND / CLAIMS REVIEWER','Evidence scope and policy decisions','shield')]
for i,(t,sub,kind) in enumerate(roles):
 rect(xs[i],220,cw,2440,['#EEF5FD','#ECF8F3','#F4F0FC','#FFF8E9'][i],None,22)
 role_icon(xs[i]+26,247,colors[i],kind);text(xs[i]+145,250,t,29,colors[i],mw=cw-165);text(xs[i]+145,298,sub,23,'#526A84',mw=cw-165)
# prepared foundation strip
rect(260,375,3190,83,'#173C65');text(290,394,'PREPARED FOUNDATION   Product facts • Original images • Approved claims • Brand rules • Scoped product / store analytics',27,'#FFFFFF',mw=3120)
for y,label in [(540,'BRIEF'),(940,'CREATE'),(1340,'REVIEW'),(1780,'APPROVE'),(2220,'DELIVER')]:
 text(35,y,label,25,'#607994');path([(35,y+49),(205,y+49)],'#CAD6E5',arrow=False)
# 1 brief and readiness, conditional evidence resolution
node(0,520,'Define the campaign',['Select product, market and channel','Set objective, audience and formats','Describe the message and constraints'],1)
node(1,520,'Check readiness',['Confirm current access and permitted use','Identify missing or restricted evidence','Ask only necessary clarification'],2)
node(2,520,'Resolve asset gaps',['Choose or upload a suitable pack shot','Provide brand assets / creative references','Confirm product and asset identity'])
node(3,520,'Resolve evidence gaps',['Review requested claims and source support','Approve exact wording and usage scope','Or request omission / further evidence'])
path([(960,635),(1090,635)],colors[0]);path([(1790,635),(1920,635)],colors[3],dash=True);path([(2620,635),(2750,635)],colors[3],dash=True)
# return evidence to readiness via gap below row
path([(3100,750),(3100,815),(1500,815),(1500,750)],colors[3],dash=True);text(1900,772,'If needed: resolve evidence, then recheck readiness',24,colors[3])
path([(1440,750),(1440,920)],colors[1]);text(1470,855,'Ready for the agreed scope',23,colors[1])
# 2 creation
node(1,920,'Ground and generate',['Retrieve eligible facts, images and metrics','Rerank and bind evidence to the brief','Generate concepts, copy and visual drafts'],3)
node(2,920,'Shape the creative',['Review concepts and product fidelity','Select a direction and request edits','Adjust copy, layout and channel variants'],4)
note(0,938,'What marketing sees',['Draft concepts with their rationale','Supporting sources and metric context','Questions about business preferences'])
note(3,938,'Evidence stays attached',['Exact wording and qualifications','Market / channel / formula scope','Source references and review history'])
path([(1790,1035),(1920,1035)],colors[1]);path([(1090,1035),(990,1035)],colors[1]);path([(2270,1150),(2270,1230),(1490,1230),(1490,1320)],colors[2]);text(1600,1188,'Selected draft for validation',24,colors[2])
path([(1920,1100),(1830,1100),(1830,1195),(1560,1195),(1560,1150)],colors[2],dash=True)
text(1110,1155,'Edit request: regenerate draft',23,colors[2])
# 3 automated checks and scoped review
node(1,1320,'Validate the exact draft',['Check claims, product fidelity and safety','Check brand rules, text and format','Save results against this artifact version'],5)
node(3,1320,'Review flagged content',['Inspect evidence and unresolved findings','Approve permitted use or request changes','Record reviewer, scope and exact version'],6)
note(0,1335,'A visible verification summary',['Pass, issue or missing evidence per check','Reasons for any hold or required action','Business metrics retain their time window'])
note(2,1335,'Resolve creative findings',['Correct layout, copy or product depiction','Regenerate only the affected creative','Every revision returns to validation'])
path([(1790,1470),(1840,1470),(1840,1600),(2690,1600),(2690,1435),(2750,1435)],colors[3]);text(1930,1562,'Required brand / claims review',24,colors[3])
# correction path avoids nodes; up creative lane margin
path([(1790,1380),(1860,1380),(1860,990),(1920,990)],colors[2],dash=True)
text(1915,1500,'Changes trigger revalidation',24,colors[2])
# business signoff row
node(0,1760,'Approve the campaign',['Confirm strategy, audience and message','Review creative and evidence summary','Approve this exact campaign version'],7)
node(1,1760,'Lock the approved version',['Require applicable checks and sign-offs','Recheck access, evidence and permissions','Save an immutable approved package'],8)
node(2,1760,'Creative sign-off',['Confirm final design and channel variants','Approve product appearance and layout','Sign-off attaches to the exact version'])
node(3,1760,'Scoped review decision',['Record approval conditions and expiry','Retain restricted wording / asset rules','Revisions invalidate affected sign-offs'])
# reviewer to signoff note then left to lock belowrow
path([(3100,1550),(3100,1760)],colors[3]);path([(1440,1550),(1440,1685),(610,1685),(610,1760)],colors[1]);text(700,1647,'Checks + review decisions available',24,colors[1])
path([(960,1875),(1090,1875)],colors[0]);path([(1920,1875),(1790,1875)],colors[2]);path([(3100,1990),(3100,2040),(1560,2040),(1560,1990)],colors[3]);text(2300,2002,'Applicable review approval',24,colors[3])
# delivery
node(0,2200,'Launch and measure',['Download / hand off approved assets','Launch through the campaign channel','Return campaign IDs and real outcomes'],9)
node(1,2200,'Deliver and learn',['Show preview, files and evidence pack','Keep reconnectable history and versions','Use feedback and outcomes in evaluation'])
node(2,2200,'Reuse and adapt',['Download approved format variants','Create a new version for further edits','Preserve source and approval lineage'])
node(3,2200,'Audit and maintain',['Review campaign and evidence history','Manage review expiry or revocation','Update permitted-use decisions'])
path([(1440,1990),(1440,2200)],colors[1]);text(1110,2110,'Only after required checks and approvals',23,colors[1])
path([(1090,2315),(960,2315)],colors[1]);path([(1790,2315),(1920,2315)],colors[1]);path([(610,2430),(610,2500),(1440,2500),(1440,2430)],colors[2],dash=True);text(720,2459,'Feedback + measured outcomes',24,colors[2])
text(280,2587,'Solid arrows: primary interaction and hand-off     Dashed arrows: clarification, correction or feedback',25,'#526A84',mw=3100)
# output artifact
rect(75,2720,3450,140,'#173C65');text(105,2740,'THE CAMPAIGN PACKAGE',31,'#FFFFFF');text(105,2797,'Approved creative + copy + format variants + source evidence + analytics context + validation results + versioned sign-offs',29,'#FFFFFF',mw=3390)
text(75,2900,'The team works in chat and review screens; background preparation, retrieval, job recovery and record keeping remain platform responsibilities.',25,'#526A84',mw=3450)
assert not errors,errors
svg.append('</svg>');(ROOT/'creative-intelligence-user-interaction-v1.svg').write_text('\n'.join(svg));im.save(ROOT/'creative-intelligence-user-interaction-v1.png');print('Exported PNG + editable SVG with text-fit checks.')
