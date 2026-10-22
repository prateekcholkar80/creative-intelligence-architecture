"""Deployment policy guards; fixtures do not prove live IAP access."""
from copy import deepcopy
from unittest import TestCase
from src.validation import measurement_runtime_deployment as checks


class RuntimeDeploymentTests(TestCase):
    def fixture(self):
        env={'CI_MEASUREMENT_PUBLISHER_PRINCIPAL_ID':checks.PILOT,'CI_MEASUREMENT_WORKSPACE_ID':'ci-poc-marketing',
            'CI_MEASUREMENT_RUNTIME_ENABLED':'true','CI_MEASUREMENT_REVIEW_SHA256':'dc80755c1873d0e586431fe5bc551e03fdb84b6b46a335b8c296099096738862',
            'CI_PROMPT_REGISTRATION_WRITES_ENABLED':'false'}
        service=dict(spec={'template':{'spec':{'serviceAccountName':'ci-prompt-registrar@'+checks.PROJECT+'.iam.gserviceaccount.com',
            'containers':[{'image':checks.IMAGE,'env':[{'name':k,'value':v} for k,v in env.items()]}]}}},
            metadata={'annotations':{'run.googleapis.com/iap-enabled':'true'}},status={'conditions':[{'type':'Ready','status':'True'}]})
        iam={'bindings':[{'role':'roles/run.invoker','members':['serviceAccount:service-1042059373096@gcp-sa-iap.iam.gserviceaccount.com']}]}
        iap={'bindings':[{'role':'roles/iap.httpsResourceAccessor','members':['user:Prateek.Cholkar@gmail.com']}]}
        return service,iam,iap

    def test_public_invocation_and_wrong_image_deny(self):
        service,iam,iap=self.fixture();self.assertTrue(all(checks.verify(service,iam,iap).values()))
        changed=deepcopy(iam);changed['bindings'][0]['members'].append('allUsers')
        self.assertFalse(checks.verify(service,changed,iap)['iap_only_invoker'])
        service['spec']['template']['spec']['containers'][0]['image']='unreviewed-image'
        self.assertFalse(checks.verify(service,iam,iap)['image_pinned'])

    def test_pilot_revocation_and_write_enablement_fail_checks(self):
        service,iam,iap=self.fixture();iap['bindings'][0]['members']=[]
        self.assertFalse(checks.verify(service,iam,iap)['pilot_iap'])
        service['spec']['template']['spec']['containers'][0]['env'].append({'name':'CI_UNEXPECTED_WRITES_ENABLED','value':'true'})
        self.assertFalse(checks.verify(service,iam,iap)['no_write_or_bootstrap_enablement'])
