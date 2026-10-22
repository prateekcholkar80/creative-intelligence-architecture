"""Owned native E2 observations only; no real identities, content or approvals."""
from copy import deepcopy
from datetime import datetime,timezone
from src.application.campaign_acceptance_contract import Stage
from src.application.campaign_quality_authority import VERSION
from src.application.identity import ISSUER,VerifiedIdentity,principal_for_subject
from src.application.workflow_contract import request_fingerprint
from src.validation.control_fixtures import SUBJECT,workflow,controls

SCOPES=('core','lost','rollback','postcommit','stale','revoked','catalog','global','race')


def fixtures():
    rows={name:[] for name in ('workspaces','memberships','conversations','requests')};contexts={}
    for name in SCOPES:
        workspace='fixture-authority-'+name;seed=workflow(workspace)
        r=seed['requests'][0];r['canonical_sku']='CI-SKU-0021'
        r['submitted_preferences']['preferences']['canonical_sku']='CI-SKU-0021'
        r['request_sha256']=request_fingerprint(r,catalog=frozenset({'CI-SKU-0021'}))
        for table in rows:rows[table].extend(seed[table])
        contexts[name]=dict(version=VERSION,workspace_id=workspace,conversation_id=r['conversation_id'],
            request_id=r['request_id'],principal_id=r['principal_id'],request_sha256=r['request_sha256'],
            sku=r['canonical_sku'],country='US',channel='META',stage=Stage.GENERATION.value,
            audience=r['audience'],formula_version=None,transcript=[],claims=[])
    sentinel=controls('fixture-authority-unrelated')
    for name in ('workspaces','memberships','conversations'):rows[name].extend(sentinel[name])
    rows['product_master']=[dict(sku='CI-SKU-0021',source_system='NAKED_AND_THRIVING_OFFICIAL',product_status='ACTIVE')]
    return rows,contexts


def identity():
    now=int(datetime.now(timezone.utc).timestamp())
    return VerifiedIdentity(principal_for_subject(ISSUER,SUBJECT),ISSUER,SUBJECT,
        '/projects/1042059373096/locations/asia-south1/services/fixture-authority',now-1,now+599)
