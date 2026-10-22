from unittest import TestCase
from copy import deepcopy
from src.validation.attachment_deployment import checks,CONFIG_PATH,PROJECT


class AttachmentDeploymentTests(TestCase):
    def test_preserves_secrets_iap_and_capacity_and_rejects_unrelated_changes(self):
        container=dict(image='old',env=[dict(name='secret',valueFrom=dict(secretKeyRef=dict(name='config',key='2')))],
            resources=dict(limits=dict(memory='2Gi')))
        service=dict(metadata=dict(annotations={'run.googleapis.com/iap-enabled':'true'}),
            spec=dict(template=dict(spec=dict(serviceAccountName='runtime',containerConcurrency=8,containers=[container]))),
            status=dict(conditions=[dict(type='Ready',status='True')]))
        iam=dict(bindings=[dict(role='roles/run.invoker',members=['serviceAccount:service-1042059373096@gcp-sa-iap.iam.gserviceaccount.com'])])
        before=dict(service=deepcopy(service),run_iam=iam,iap_iam={},release_enabled=False,interpreter_enabled=False)
        image='asia-south1-docker.pkg.dev/'+PROJECT+'/ci-operator-images/marketing-pilot@sha256:'+'a'*64
        service['spec']['template']['spec']['containers'][0].update(image=image)
        service['spec']['template']['spec']['containers'][0]['env'].append(dict(name='CI_CAMPAIGN_ATTACHMENTS_CONFIG',value=CONFIG_PATH))
        self.assertTrue(all(checks(before,service,iam,{},image).values()))
        service['spec']['template']['spec']['containerConcurrency']=99
        self.assertFalse(checks(before,service,iam,{},image)['runtime_secrets_resources_capacity_preserved'])
