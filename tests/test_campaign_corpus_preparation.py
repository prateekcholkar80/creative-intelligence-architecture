from copy import deepcopy
from datetime import datetime,timezone,timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock
import json,time
from google.cloud import bigquery

from src.application.campaign_corpus_preparation import *
from src.application.operation_budget import worker_operation_scope
from src.retrieval.upload_corpus import UploadCorpusCapture


class CorpusPreparationTests(TestCase):
    def test_expired_snapshot_has_distinct_code_and_does_not_read_again(self):
        p,d,*_=self.fixture();capture=p.reader.capture.return_value
        p.clock=lambda:capture.valid_until
        with self.assertRaisesRegex(ValueError,'CORPUS_PREPARATION_CAPTURE_EXPIRED'):
            p._fresh({},None,None,{}, {},capture)
        d._capture.assert_not_called()

    def fixture(self):
        selected=json.loads(Path('config/evaluation/rejuvenate_upload_corpus.json').read_text())
        settings=CorpusPreparationSettings('private-corpus',canonical(selected),digest(selected),True)
        now=datetime.now(timezone.utc);actor=SimpleNamespace(principal=SimpleNamespace(principal_id='owner'))
        scope=dict(selected['scope'],request_sha256='f'*64);sources={};extractions={};bindings={}
        for pin in selected['sources']:
            sources[pin['submission_id']]=dict(scope=scope,file=dict(content_sha256=pin['content_sha256'],
                content_type=pin['content_type']),original=dict(generation=pin['generation']),
                review_status='UNVERIFIED',eligible_for_generation=False)
            if pin['role']=='DOCUMENT':extractions[pin['submission_id']]='b'*64
        docs=ProtectedDocumentService(intake=Mock(),store=Mock())
        docs.intake._actor.return_value=(actor,'access')
        docs._capture=Mock(return_value=(actor,'access',dict(scope=scope,policy_sha256='e'*64)))
        docs.store.submission.side_effect=lambda sid,**kw:sources[sid]
        missing=selected['sources'][1]['submission_id'];extractions.pop(missing)
        docs.store.find_extraction.side_effect=lambda sid,**kw:extractions.get(sid)
        def extract(headers,**kw):
            extractions[kw['submission_id']]='a'*64
            return SimpleNamespace(submission_id=kw['submission_id'],status='EXTRACTED_UNVERIFIED',
                generation_authorized=False,extraction_id='a'*64)
        docs.extract=Mock(side_effect=extract)
        manifest=dict(scope=scope,policy_sha256='e'*64,principal_id='owner',image_pins=[dict(asset_id='image')],
            text_chunks=[dict(text='unverified')],text_bytes=10,text_sources=[dict(submission_id=p['submission_id'],
                source_record_sha256=digest(sources[p['submission_id']])) for p in selected['sources'] if p['role']=='DOCUMENT'])
        captured=UploadCorpusCapture(canonical(manifest),(),now+timedelta(seconds=120))
        with worker_operation_scope(time.monotonic()+60):p=ProtectedCorpusPreparation(docs,settings,clock=lambda:now)
        p.reader=Mock();p.reader.capture.return_value=captured
        docs.store.query.side_effect=lambda sql,params: (() if params[0].value not in bindings else
            ({'record_json':canonical(bindings[params[0].value])},))
        docs.intake.store.record.side_effect=lambda key,record:bindings.setdefault(key,record)
        def native(jid,**kw):
            key=jid.removeprefix('evidence_submission_v1_');raw=canonical(bindings[key])
            cfg=bigquery.QueryJobConfig(use_legacy_sql=False,use_query_cache=False,create_session=False,
                maximum_bytes_billed=128*1024**2,job_timeout_ms=60000,query_parameters=[
                    bigquery.ScalarQueryParameter('id','STRING',key),bigquery.ScalarQueryParameter('record','STRING',raw)])
            config=cfg.to_api_repr();config['query']['query']=SQL
            return bigquery.QueryJob.from_api_repr(dict(jobReference=dict(projectId=docs.store.client.project,
                location=LOCATION,jobId=jid),configuration=config,status=dict(state='DONE')),docs.store.client)
        from src.ingestion.setup_workflow_tables import PROJECT
        docs.store.client.project=PROJECT;docs.store.client.get_job.side_effect=native
        return p,docs,bindings,selected,missing

    def action(self,p,**kw):
        with worker_operation_scope(time.monotonic()+60):return p.prepare({},**kw)

    def test_prepare_only_missing_extract_and_recover_original_binding_without_rewrites(self):
        p,d,bindings,selected,missing=self.fixture();receipt=self.action(p)
        d.extract.assert_called_once_with({},workspace_id=selected['scope']['workspace_id'],
            conversation_id=selected['scope']['conversation_id'],submission_id=missing)
        self.assertFalse(receipt['generation_authorized']);self.assertEqual(receipt['source_count'],3)
        self.assertEqual(len(bindings),1)
        stored=bindings[receipt['binding_id']]
        self.assertNotIn('scope',stored)  # Ordinary upload history reads only $.scope.
        self.assertEqual(stored['binding_scope']['request_id'],selected['scope']['request_id'])
        self.assertTrue(all('text' not in c for c in stored['corpus']['text_chunks']))
        self.assertEqual(stored['corpus']['text_materialization'],'RECONSTRUCT_FROM_EXACT_STORED_EXTRACTION')
        self.assertEqual(stored['corpus_sha256'],p.reader.capture.return_value.sha256)
        self.assertEqual(self.action(p,recover_only=True),receipt)
        self.assertEqual(self.action(p),receipt)
        d.intake.store.record.assert_called_once();d.extract.assert_called_once()
        # A new service instance holds no pending plan or response cache.
        with worker_operation_scope(time.monotonic()+60):cold=ProtectedCorpusPreparation(d,p.settings,clock=p.clock)
        cold.reader=p.reader
        self.assertEqual(self.action(cold,recover_only=True),receipt)

    def test_recovery_cannot_create_missing_extraction_or_binding(self):
        p,d,bindings,*_=self.fixture()
        with self.assertRaisesRegex(ValueError,'CORPUS_EXTRACTION_REQUIRED'):self.action(p,recover_only=True)
        d.extract.assert_not_called();d.intake.store.record.assert_not_called()

    def test_lost_binding_reply_recovers_exact_original_and_does_not_reextract(self):
        p,d,bindings,*_=self.fixture();record=d.intake.store.record.side_effect
        def lost(key,value):record(key,value);raise TimeoutError()
        d.intake.store.record.side_effect=lost
        with self.assertRaises(TimeoutError):self.action(p)
        result=self.action(p,recover_only=True)
        self.assertEqual(len(bindings),1);self.assertIn(result['binding_id'],bindings)
        d.extract.assert_called_once();d.intake.store.record.assert_called_once()

    def test_changed_selected_source_denies_before_any_extraction(self):
        p,d,*_=self.fixture();original=d.store.submission.side_effect
        d.store.submission.side_effect=lambda sid,**kw:dict(original(sid,**kw),review_status='APPROVED')
        with self.assertRaises(ValueError):self.action(p)
        d.extract.assert_not_called();d.intake.store.record.assert_not_called()

    def test_policy_or_access_change_withholds_binding(self):
        for fault in ('policy','access'):
            p,d,bindings,*_=self.fixture();actor,access,context=d._capture.return_value
            d._capture.return_value=(actor,'changed' if fault=='access' else access,
                dict(context,policy_sha256='a'*64) if fault=='policy' else context)
            with self.subTest(fault=fault),self.assertRaises(ValueError):self.action(p)
            self.assertFalse(bindings)

    def test_wrong_original_binding_native_job_denies_recovery(self):
        p,d,*_=self.fixture();self.action(p)
        d.store.client.get_job.side_effect=None;d.store.client.get_job.return_value=Mock()
        with self.assertRaisesRegex(ValueError,'CORPUS_BINDING_NATIVE_UNVERIFIED'):self.action(p,recover_only=True)

    def test_changed_or_disabled_settings_deny(self):
        from dataclasses import replace
        p,d,*_=self.fixture()
        with self.assertRaises(ValueError):replace(p.settings,selection_sha256='0'*64)
        p.settings=replace(p.settings,enabled=False)
        with self.assertRaises(ValueError):self.action(p)
        d.store.submission.assert_not_called()
