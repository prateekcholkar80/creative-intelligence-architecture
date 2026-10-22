from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from src.validation.clarification_console_preflight import REQUIRED, RUNTIME, package_report, service_report

class ConsolePreflightTests(TestCase):
    def test_package_requires_routes_and_rejects_business_or_secret_exports(self):
        with TemporaryDirectory() as d:
            root=Path(d)
            for name in REQUIRED:
                p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('fixture')
            report=package_report(root,REQUIRED)
            self.assertEqual(report['status'],'PASSED')
            self.assertEqual(report['source_sha256'],package_report(root,reversed(REQUIRED))['source_sha256'])
            for names in (REQUIRED[:-1],(*REQUIRED,'.env'),(*REQUIRED,'data/private.json'),(*REQUIRED,'src/__pycache__/x.pyc')):
                with self.assertRaises(ValueError):package_report(root,names)
            p=root/REQUIRED[0];p.unlink();p.symlink_to(root/REQUIRED[1])
            with self.assertRaises(ValueError):package_report(root,REQUIRED)

    def service(self):
        from src.validation.clarification_console_preflight import SERVICE
        env=dict(CI_PROMPT_REGISTRATION_WRITES_ENABLED='false',CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED='false',
            CI_IAP_EXPECTED_AUDIENCE='/projects/1042059373096/locations/asia-south1/services/'+SERVICE,
            CI_PROMPT_REGISTRATION_ORIGIN='https://ci-prompt-registrar-1042059373096.asia-south1.run.app',
            CI_PROMPT_REGISTRATION_WORKSPACE_ID='ci-poc-marketing')
        return dict(metadata=dict(annotations={'run.googleapis.com/iap-enabled':'true','run.googleapis.com/maxScale':'1'}),
            spec=dict(template=dict(metadata=dict(annotations={'autoscaling.knative.dev/maxScale':'1'}),
                spec=dict(serviceAccountName=RUNTIME,containerConcurrency=16,containers=[dict(image='digest',env=[dict(name=k,value=v) for k,v in env.items()])]))),
            status=dict(latestReadyRevisionName='previous'))

    def test_service_requires_disabled_writes_iap_and_exact_scope(self):
        self.assertEqual(service_report(self.service())['status'],'PASSED')
        for field in ('CI_PROMPT_REGISTRATION_WRITES_ENABLED','CI_WORKSPACE_BOOTSTRAP_CONSOLE_ENABLED','CI_IAP_EXPECTED_AUDIENCE'):
            value=self.service()
            for env in value['spec']['template']['spec']['containers'][0]['env']:
                if env['name']==field:env['value']='wrong'
            self.assertEqual(service_report(value)['status'],'FAILED')
        value=self.service();value['metadata']['annotations']['run.googleapis.com/iap-enabled']='false'
        self.assertEqual(service_report(value)['status'],'FAILED')
