from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch
from copy import deepcopy
from google.cloud import bigquery

from src.application import campaign_quality_product_reader as reader
from src.application.access_policy import AccessDecision, Action, Reason
from src.application.campaign_acceptance_contract import Decision
from src.application.workflow_contract import ContractError
from tests.test_campaign_quality_checks import NOW, candidate


def client(row_changes=None, rows=None, native_changes=None):
    row = dict(sku='CI-SKU-0021', brand='Naked & Thriving', product_name='Renew Resurfacing Night Serum',
        size_value=Decimal('30.000000000'), size_unit='mL', product_status='ACTIVE',
        source_system='NAKED_AND_THRIVING_OFFICIAL')
    row.update(row_changes or {})
    c = Mock(project=reader.PROJECT)
    def query(sql, **kw):
        config=deepcopy(kw['job_config'].to_api_repr())
        config['query']['query']=sql
        native = SimpleNamespace(project=reader.PROJECT, location=reader.LOCATION, job_id=kw['job_id'],
            query=sql, state='DONE', error_result=None, to_api_repr=lambda: dict(configuration=config))
        for key, value in (native_changes or {}).items():setattr(native, key, value)
        c.get_job.return_value = native
        return Mock(result=Mock(return_value=[row] if rows is None else rows))
    c.query.side_effect = query
    return c


def access():
    decision = AccessDecision(True, Action.READ_DRAFT, Reason.ALLOWED_OWNER, 'workspace', 'principal', 1, 'conversation', 1)
    return Mock(authorize=Mock(return_value=decision))


class ProductReaderTests(TestCase):
    def read(self, c=None, **kw):
        return reader.read_product_fact(c or client(), 'CI-SKU-0021', clock=lambda: NOW, **kw)

    def protected(self, c=None, a=None, builder=candidate, clock=lambda: NOW):
        with patch.object(reader, 'ensure_fresh'):
            return reader.check_current_product('CI-SKU-0021', builder, client=c or client(),
                access_reader=a or access(), identity=SimpleNamespace(principal=SimpleNamespace(principal_id='principal')),
                workspace_id='workspace', conversation_id='conversation', clock=clock)

    def test_exact_current_identity_fields_only_no_prices_or_claims(self):
        read = self.read()
        self.assertEqual(read.fact.fields_json, '{"full_size_ml":"30","product_name":"Renew Resurfacing Night Serum"}')
        self.assertEqual(read.native_job_id, read.fact.source_id)
        self.assertEqual(read.fact.valid_until, NOW+timedelta(seconds=60))

    def test_parameterized_read_has_billing_timeout_no_retries_or_cache(self):
        c=client();self.read(c)
        args,kw=c.query.call_args
        self.assertIn('WHERE sku=@sku LIMIT 2',args[0])
        cfg=kw['job_config'];self.assertFalse(cfg.use_query_cache)
        self.assertEqual(cfg.maximum_bytes_billed,64*1024*1024)
        self.assertEqual(int(cfg.job_timeout_ms),60000)
        self.assertIsNone(kw['retry']);self.assertIsNone(kw['job_retry'])

    def test_real_sdk_job_configuration_includes_sql_and_is_accepted(self):
        c=client();submit=c.query.side_effect
        def query(sql,**kw):
            job=submit(sql,**kw)
            resource=dict(
                jobReference=dict(projectId=reader.PROJECT,location=reader.LOCATION,jobId=kw['job_id']),
                configuration=c.get_job.return_value.to_api_repr()['configuration'],status=dict(state='DONE'))
            # BigQuery also returns server-generated destination/default metadata.
            resource['configuration']['query'].update(priority='INTERACTIVE',destinationTable=dict(
                projectId=reader.PROJECT,datasetId='_anonymous',tableId='query_result'))
            c.get_job.return_value=bigquery.QueryJob.from_api_repr(resource,c)
            return job
        c.query.side_effect=query
        self.assertEqual(self.read(c).fact.sku,'CI-SKU-0021')

    def test_provider_mutation_of_submitted_config_cannot_redefine_expected_pin(self):
        c=client();submit=c.query.side_effect
        def mutate(sql,**kw):
            kw['job_config'].maximum_bytes_billed=1
            return submit(sql,**kw)
        c.query.side_effect=mutate
        with self.assertRaisesRegex(ContractError,'QUALITY_PRODUCT_NATIVE_MISMATCH'):self.read(c)

    def test_native_omitted_false_defaults_match_but_true_defaults_deny(self):
        for changed in (None,'createSession','useLegacySql'):
            c=client();submit=c.query.side_effect
            def query(sql,**kw):
                job=submit(sql,**kw)
                config=c.get_job.return_value.to_api_repr()['configuration']
                config['query'].pop('createSession');config['query'].pop('useLegacySql')
                if changed:config['query'][changed]=True
                return job
            c.query.side_effect=query
            if changed:
                with self.assertRaisesRegex(ContractError,'QUALITY_PRODUCT_NATIVE_MISMATCH'):self.read(c)
            else:self.assertEqual(self.read(c).fact.sku,'CI-SKU-0021')

    def test_unsupported_or_injected_sku_denies_before_provider(self):
        for sku in ('CI-SKU-0000','CI-SKU-0024',"x' OR TRUE",None):
            c=client()
            with self.assertRaises(ContractError):reader.read_product_fact(c,sku)
            c.query.assert_not_called()

    def test_original_twenty_identity_only_without_invented_size(self):
        for sku in reader.ORIGINAL_SKUS:
            c=client(dict(sku=sku,brand='Original catalog brand',source_system='BEAUTYFEEDS',
                product_name='Catalog serum 30ml',size_value=None,size_unit=None))
            result=reader.read_product_fact(c,sku,clock=lambda:NOW)
            self.assertEqual(result.fact.fields_json,'{"product_name":"Catalog serum 30ml"}')
            self.assertEqual(result.fact.sku,sku)

    def test_original_source_conflicts_partial_size_and_blank_brand_deny(self):
        base=dict(sku='CI-SKU-0001',source_system='BEAUTYFEEDS',brand='Catalog brand',size_value=None,size_unit=None)
        for change in (dict(source_system='NAKED_AND_THRIVING_OFFICIAL'),dict(source_system='OTHER'),
                dict(brand=''),dict(size_unit='mL'),dict(size_value=Decimal('30'))):
            with self.subTest(change=change),self.assertRaises(ContractError):
                reader.read_product_fact(client(base|change),'CI-SKU-0001',clock=lambda:NOW)

    def test_official_products_still_require_independent_size(self):
        with self.assertRaises(ContractError):self.read(client(dict(size_value=None,size_unit=None)))

    def test_missing_duplicate_or_extra_columns_deny(self):
        for rows in ([],[{},{}],[{}],[dict(extra='x')]):
            with self.assertRaises(ContractError):self.read(client(rows=rows))

    def test_invalid_product_status_provenance_scope_and_values_deny(self):
        for changes in (dict(sku='CI-SKU-0022'),dict(brand='Other'),dict(source_system='BEAUTYFEEDS'),
            dict(product_status='INACTIVE'),dict(size_unit='oz'),dict(size_value=30),
            dict(size_value=Decimal('NaN')),dict(size_value=Decimal('0')),dict(product_name=' ')):
            with self.subTest(changes=changes),self.assertRaises(ContractError):self.read(client(changes))

    def test_independent_original_job_identity_sql_and_config_must_match(self):
        for changes in (dict(project='other'),dict(location='us'),dict(job_id='other'),dict(query='SELECT 1'),
            dict(state='RUNNING'),dict(error_result={'reason':'failed'}),dict(to_api_repr=lambda: {})):
            with self.subTest(changes=changes),self.assertRaises(ContractError):self.read(client(native_changes=changes))

    def test_slow_or_regressed_read_window_does_not_extend_freshness(self):
        for end in (NOW-timedelta(seconds=1),NOW+timedelta(seconds=60)):
            times=iter((NOW,end))
            with self.assertRaises(ContractError):reader.read_product_fact(client(),'CI-SKU-0021',clock=lambda: next(times))

    def test_initial_denial_prevents_source_read_and_candidate_builder(self):
        a=access();a.authorize.return_value=replace(a.authorize.return_value,allowed=False)
        c=client();builder=Mock()
        with self.assertRaises(ContractError):self.protected(c,a,builder)
        c.query.assert_not_called();builder.assert_not_called()

    def test_unverified_identity_stops_before_access_or_product_queries(self):
        c=client();a=access()
        with self.assertRaises(PermissionError):
            reader.check_current_product('CI-SKU-0021',candidate,client=c,access_reader=a,
                identity=None,workspace_id='workspace',conversation_id='conversation')
        a.authorize.assert_not_called();c.query.assert_not_called()

    def test_revocation_or_revision_change_after_read_withholds_result(self):
        for changes in (dict(allowed=False),dict(membership_revision=2),dict(resource_revision=2),
            dict(resource_id='other'),dict(principal_id='other')):
            a=access();d=a.authorize.return_value;a.authorize.side_effect=[d,replace(d,**changes)]
            with self.assertRaises(ContractError):self.protected(a=a)
            self.assertEqual(a.authorize.call_count,2)

    def test_current_pins_execute_checker_without_granting_release(self):
        result=self.protected()
        self.assertIs(result['report'].decision,Decision.NEEDS_REVIEW)
        self.assertFalse(result['release_allowed'])
        self.assertEqual(len(result['context_sha256']),64)
        self.assertEqual(result['report'].candidate_sha256,candidate(result['source']).sha256)

    def test_server_annotation_cannot_silently_certify_wrong_text_or_scope(self):
        result=self.protected(builder=lambda fact:candidate(fact,text='Invented product'))
        self.assertIs(result['report'].decision,Decision.BLOCK)
        with self.assertRaises(ContractError):self.protected(builder=lambda fact:candidate(fact,sku='CI-SKU-0022'))

    def test_provider_failures_do_not_fallback_or_invoke_builder(self):
        c=client();c.query.side_effect=RuntimeError('unavailable');builder=Mock()
        with self.assertRaises(RuntimeError):self.protected(c=c,builder=builder)
        builder.assert_not_called()

    def test_expiry_during_access_checks_blocks_fact_support(self):
        times=iter((NOW,NOW,NOW+timedelta(seconds=60)))
        result=self.protected(clock=lambda:next(times))
        self.assertIs(result['report'].decision,Decision.BLOCK)
