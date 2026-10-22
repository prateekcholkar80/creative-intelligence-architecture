from dataclasses import FrozenInstanceError,replace
import hashlib,json
from unittest import TestCase
from src.application.campaign_planner_projection import project_planner_response,verify_projection,FIELDS
from src.application.workflow_contract import ContractError,canonical_json_bytes


def options():
    result={key:[key+'-fixture'] for key in FIELDS}
    result.update(headline=['Exact product name'],body=['','Explore the product.'],cta=['View product'])
    return canonical_json_bytes(result).decode()


def response(body=None):
    result={key:values[0] for key,values in json.loads(options()).items()}
    result['body']=body
    return json.dumps(result)


class PlannerProjectionTests(TestCase):
    def test_exact_copy_order_and_original_bytes_are_pinned(self):
        raw=response();p=project_planner_response(raw,options())
        self.assertEqual(p.copy_text,'Exact product name\nView product')
        self.assertEqual(p.response_sha256,hashlib.sha256(raw.encode()).hexdigest())
        self.assertEqual(tuple(k for k,_ in p.copy_fields),('headline','cta'))
        self.assertEqual(verify_projection(p,raw,expected_options_sha256=p.options_sha256),p)
        with self.assertRaises(FrozenInstanceError):p.response_sha256='a'*64

    def test_body_is_retained_and_no_copy_is_invented_from_other_fields(self):
        p=project_planner_response(response('Explore the product.'),options())
        self.assertEqual(p.copy_text,'Exact product name\nExplore the product.\nView product')
        self.assertNotIn('layout-fixture',p.copy_text)

    def test_duplicate_extra_missing_wrong_enum_and_wrong_types_deny(self):
        raw=response();draft=json.loads(raw)
        for text in (raw.replace('"body": null','"body": null, "body": ""'),
            json.dumps(draft|dict(extra='x')),json.dumps({k:v for k,v in draft.items() if k!='cta'}),
            json.dumps(draft|dict(headline='Invented benefit')),json.dumps(draft|dict(cta=None)),
            json.dumps(draft|dict(body=True)),'NaN'):
            with self.assertRaises(ValueError):project_planner_response(text,options())

    def test_options_must_be_complete_canonical_unique_and_nfc(self):
        values=json.loads(options())
        for changed in (values|dict(cta=[]),values|dict(cta=['x','x']),values|dict(cta=['e\u0301']),
            {k:v for k,v in values.items() if k!='layout'}):
            with self.assertRaises(ValueError):project_planner_response(response(),canonical_json_bytes(changed).decode())
        with self.assertRaises(ValueError):project_planner_response(response(),json.dumps(values))

    def test_raw_whitespace_changes_hash_and_cannot_reuse_projection_or_other_options(self):
        raw=response();p=project_planner_response(raw,options())
        self.assertNotEqual(p.response_sha256,project_planner_response(raw+' ',options()).response_sha256)
        for text,pin in ((raw+' ',p.options_sha256),(raw,'a'*64)):
            with self.assertRaises(ContractError):verify_projection(p,text,expected_options_sha256=pin)

    def test_optional_null_requires_the_dispatched_empty_body_option(self):
        values=json.loads(options());values['body']=['Explore the product.']
        with self.assertRaises(ValueError):project_planner_response(response(),canonical_json_bytes(values).decode())
        with self.assertRaises(ValueError):project_planner_response('x'*65537,options())
