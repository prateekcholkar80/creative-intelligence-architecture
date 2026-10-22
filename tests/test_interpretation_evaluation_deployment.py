from copy import deepcopy
from unittest import TestCase
import json
from pathlib import Path
from src.validation.interpretation_evaluation_deployment import checks,CONFIG


class EvaluationDeploymentTests(TestCase):
    def fixture(self):
        # Minimal service-shaped fixture, not a live acceptance observation.
        spec=dict(containers=[dict(image='old',env=[dict(name='CI_PROMPT_REGISTRATION_WRITES_ENABLED',value='false'),dict(name='CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED',value='false')])],serviceAccountName='runtime',containerConcurrency=16)
        service=dict(spec=dict(template=dict(spec=spec,metadata=dict(annotations={'autoscaling.knative.dev/maxScale':'1'}))),metadata=dict(annotations={'run.googleapis.com/iap-enabled':'true','run.googleapis.com/maxScale':'1'}),status=dict(conditions=[dict(type='Ready',status='True')]))
        iam=dict(bindings=[dict(role='roles/run.invoker',members=['serviceAccount:service-1042059373096@gcp-sa-iap.iam.gserviceaccount.com'])])
        return dict(service=service,run_iam=iam,iap_iam=dict(bindings=[]))
    def test_only_exact_evaluation_changes_are_allowed(self):
        before=self.fixture();new=deepcopy(before['service']);image='asia-south1-docker.pkg.dev/project-661d501a-e709-4276-a26/ci-operator-images/prompt-registration@sha256:'+'a'*64
        container=new['spec']['template']['spec']['containers'][0];container['image']=image
        container['env'] += [dict(name='CI_INTERPRETATION_EVALUATION_CONFIG',value=CONFIG),dict(name='CI_MEASUREMENT_EVALUATOR_SHA256',value='b'*64)]
        self.assertTrue(all(checks(before,new,before['run_iam'],before['iap_iam'],image,'b'*64).values()))
        new['spec']['template']['spec']['containerConcurrency']=32
        self.assertFalse(checks(before,new,before['run_iam'],before['iap_iam'],image,'b'*64)['runtime_resources_secrets_concurrency_preserved'])
