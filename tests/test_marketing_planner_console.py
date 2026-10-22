"""Offline marketing evaluation scope; no live authority or model execution."""
import json
import tempfile
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock,patch

from src.ui import campaign_operator_console as console
from src.application.identity import IapSettings
from tests.test_campaign_permission_lifecycle import identity

AUDIENCE='/projects/1042059373096/locations/asia-south1/services/ci-marketing'


class MarketingPlannerConsoleTests(TestCase):
    def test_measured_retrieval_controls_roundtrip_and_recovery_are_explicit(self):
        from src.application.campaign_operator_host import ProtectedOperatorHost,config_from_wire,request_values
        from src.application.campaign_operator_worker import execute
        from src.application.operation_channel import ChannelError
        from src.retrieval.private_retrieval_metrics import ranking_metrics
        self.config['enabled']=True;self.save();root=self.path.parent
        paths={}
        for name in ('rejuvenate_upload_corpus','rejuvenate_text_embeddings','rejuvenate_retrieval_measurement'):
            path=root/(name+'.json');path.write_text(Path('config/evaluation/'+name+'.json').read_text());paths[name]=str(path)
        environment=dict(self.environment,CI_UPLOAD_CORPUS_PREPARATION_ENABLED='true',CI_UPLOAD_CORPUS_SELECTION_CONFIG=paths['rejuvenate_upload_corpus'],
            CI_UPLOAD_TEXT_EMBEDDING_ENABLED='true',CI_UPLOAD_TEXT_EMBEDDING_CONFIG=paths['rejuvenate_text_embeddings'],
            CI_UPLOAD_RETRIEVAL_MEASUREMENT_ENABLED='true',CI_UPLOAD_RETRIEVAL_MEASUREMENT_CONFIG=paths['rejuvenate_retrieval_measurement'])
        class Host:
            def __init__(self,config,*args):
                self.config=config;self.close=Mock(return_value=True)
                for name in ('start','planner_run','planner_recover','retrieval_measure','retrieval_recover','corpus_prepare','corpus_recover',
                        'embedding_run','embedding_recover','corpus_retrieval_run','corpus_retrieval_recover'):
                    setattr(self,name,Mock(return_value={}))
        with patch.dict('os.environ',environment,clear=True),patch.object(console,'ProtectedOperatorHost',Host):
            self.render();host=self.ui.session_state[console.SESSION_KEY];host.corpus_retrieval_run.assert_not_called()
            self.ui.button.side_effect=lambda label:label=='Run configured retrieval measurement'
            self.render();host.corpus_retrieval_run.assert_called_once_with({})
            self.ui.button.side_effect=lambda label:label=='Recover original retrieval measurement'
            self.render();host.corpus_retrieval_recover.assert_called_once_with({})
            host.embedding_run.assert_not_called();host.planner_run.assert_not_called()
        real=ProtectedOperatorHost(host.config);self.assertEqual(config_from_wire(real._config_values()),host.config)
        cfg=host.config.corpus_retrieval;case=json.loads(cfg.case_json);grades={r['chunk_id']:r['grade'] for r in case['labels']};ids=sorted(grades)
        receipt=dict(run_id=cfg.run_id,case_id=cfg.query_id,measurement_record_id='a'*64,outcome_sha256='b'*64,
            query_embedding_record_id='c'*64,index_record_id=cfg.index_record_id,index_sha256=cfg.index_sha256,
            corpus_sha256=case['corpus_sha256'],native_ranking_job_id='private_retrieval_rank_v1_'+'d'*64,
            observation_sha256='e'*64,measurement_sha256='f'*64,rankings={n:ids for n in ('lexical','vector','rrf')},
            metrics={n:[ranking_metrics(tuple(ids),grades,k) for k in (1,3)] for n in ('lexical','vector','rrf')},
            label_authority=case['label_authority'],semantic_reranking='NOT_EXECUTED',image_embedding_status='NOT_EXECUTED',
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
        for method in ('corpus_retrieval_run','corpus_retrieval_recover'):
            self.assertEqual(real._decode_result(method,receipt),receipt);real._validate_result_scope(method,{},receipt)
            with self.assertRaises(ChannelError):request_values(method,{},dict(query='override'))
            with self.assertRaises(ChannelError):real._decode_result(method,dict(receipt,qualifying=True))
            with self.assertRaises(ChannelError):real._validate_result_scope(method,{},dict(receipt,index_sha256='f'*64))
            worker=Mock();execute(None,host.config,method,{}, {},corpus_retrieval=worker)
            worker.execute.assert_called_once_with({},recover_only=method=='corpus_retrieval_recover')

    def test_measured_retrieval_failure_names_own_recovery_and_redacts_exception(self):
        from src.ui.marketing_pilot import show_planner_failure
        from src.application.sharing_operation_host import SharingHostError
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING'):
            show_planner_failure(self.ui,SharingHostError('PRIVATE_MEASUREMENT_OUTCOME_UNUSABLE'),measured_retrieval=True)
        self.assertIn('Recover original retrieval measurement',self.ui.error.call_args.args[0])
        self.assertIn('PRIVATE_MEASUREMENT_OUTCOME_UNUSABLE',self.ui.caption.call_args.args[0])
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING') as logs:
            show_planner_failure(self.ui,ValueError('secret-token identity@example.com'),measured_retrieval=True)
        self.assertNotIn('identity@example.com',str(logs.output));self.assertIn('RUNTIME_UNAVAILABLE',self.ui.caption.call_args.args[0])

    def test_embedding_failure_logs_only_safe_code_and_names_own_recovery(self):
        from src.ui.marketing_pilot import show_planner_failure
        from src.application.sharing_operation_host import SharingHostError
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING') as logs:
            show_planner_failure(self.ui,SharingHostError('CORPUS_EXTRACTION_CHANGED'),embedding=True)
        self.assertIn('CORPUS_EXTRACTION_CHANGED',self.ui.caption.call_args.args[0])
        self.assertIn('Recover original text embeddings and index',self.ui.error.call_args.args[0])
        self.assertNotIn('planner evaluation records',self.ui.error.call_args.args[0])
        self.assertIn('"action": "embedding"',logs.output[0])
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING') as logs:
            show_planner_failure(self.ui,ValueError('secret-token identity@example.com'),embedding=True)
        self.assertNotIn('secret-token',str(logs.output));self.assertNotIn('identity@example.com',str(logs.output))
        self.assertIn('RUNTIME_UNAVAILABLE',self.ui.caption.call_args.args[0])
    def test_embedding_controls_are_scoped_opt_in_and_cannot_accept_browser_inputs(self):
        from src.application.campaign_corpus_embedding import CorpusEmbeddingSettings
        from src.application.campaign_operator_host import ProtectedOperatorHost,config_from_wire,request_values
        from src.application.campaign_operator_worker import execute
        from src.application.operation_channel import ChannelError
        self.config['enabled']=True;self.save()
        root=Path(self.directory.name);selection=root/'sources.json'
        selection.write_text(Path('config/evaluation/rejuvenate_upload_corpus.json').read_text())
        selected=json.loads(selection.read_text());selection_sha=console.digest(selected)
        settings=CorpusEmbeddingSettings('text-index-v1','eval_corpus_binding_v1_'+'a'*64,'b'*64,selection_sha,True)
        target=root/'embedding.json';target.write_text(json.dumps(dict(version='private-text-embedding-operation-v1',
            run_id=settings.run_id,binding_id=settings.binding_id,corpus_sha256=settings.corpus_sha256,selection_sha256=selection_sha)))
        environment=dict(self.environment,CI_UPLOAD_CORPUS_PREPARATION_ENABLED='true',CI_UPLOAD_CORPUS_SELECTION_CONFIG=str(selection),
            CI_UPLOAD_TEXT_EMBEDDING_ENABLED='true',CI_UPLOAD_TEXT_EMBEDDING_CONFIG=str(target))
        class Host:
            def __init__(self,config,*args):
                self.config=config;self.close=Mock(return_value=True)
                for name in ('start','planner_run','planner_recover','retrieval_measure','retrieval_recover','corpus_prepare','corpus_recover','embedding_run','embedding_recover'):
                    setattr(self,name,Mock(return_value={}))
        with patch.dict('os.environ',environment,clear=True),patch.object(console,'ProtectedOperatorHost',Host):
            self.render();host=self.ui.session_state[console.SESSION_KEY]
            self.assertEqual(host.config.embedding,settings);host.embedding_run.assert_not_called()
            self.ui.button.side_effect=lambda label:label=='Run selected text embeddings and index'
            self.render();host.embedding_run.assert_called_once_with({})
            self.ui.button.side_effect=lambda label:label=='Recover original text embeddings and index'
            self.render();host.embedding_recover.assert_called_once_with({});host.planner_run.assert_not_called()
            configured=host.config
        real=ProtectedOperatorHost(configured);self.assertEqual(config_from_wire(real._config_values()),configured)
        for method in ('embedding_run','embedding_recover'):
            with self.assertRaises(ChannelError):request_values(method,{},dict(text='browser override'))
            worker=Mock();execute(None,configured,method,{}, {},embedding=worker)
            worker.execute.assert_called_once_with({},recover_only=method=='embedding_recover')
        with self.assertRaises(ChannelError):replace(configured,embedding=replace(settings,selection_sha256='f'*64))
        with self.assertRaises(ChannelError):replace(configured,corpus=None)
        row=dict(record_id='a'*64,outcome_sha256='b'*64,response_sha256='c'*64,dimension=768,
            input_tokens=None,returned_model_id=None,qualifying=False,release_allowed=False)
        index=dict(index_record_id='d'*64,index_sha256='e'*64,corpus_sha256=settings.corpus_sha256,
            chunk_count=1,dimension=768,claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
        receipt=dict(run_id=settings.run_id,corpus_binding_id=settings.binding_id,corpus_sha256=settings.corpus_sha256,
            text_embeddings=[row],index=index,image_embedding_status='NOT_EXECUTED',retrieval_quality='NOT_MEASURED',
            claim_approval=False,generation_authorized=False,qualifying=False,release_allowed=False)
        for method in ('embedding_run','embedding_recover'):
            self.assertEqual(real._decode_result(method,receipt),receipt);real._validate_result_scope(method,{},receipt)
            with self.assertRaises(ChannelError):real._decode_result(method,dict(receipt,qualifying=True))
            with self.assertRaises(ChannelError):real._validate_result_scope(method,{},dict(receipt,corpus_sha256='f'*64))

    def test_selected_corpus_controls_are_explicit_and_config_is_pinned(self):
        self.config['enabled']=True;self.save()
        selection=Path(self.directory.name)/'sources.json'
        selection.write_text(Path('config/evaluation/rejuvenate_upload_corpus.json').read_text())
        environment=dict(self.environment,CI_UPLOAD_CORPUS_PREPARATION_ENABLED='true',
            CI_UPLOAD_CORPUS_SELECTION_CONFIG=str(selection))
        class Host:
            def __init__(self,config,*args):
                self.config=config;self.close=Mock(return_value=True)
                for name in ('start','planner_run','planner_recover','retrieval_measure','retrieval_recover','corpus_prepare','corpus_recover'):
                    setattr(self,name,Mock(return_value={}))
        with patch.dict('os.environ',environment,clear=True),patch.object(console,'ProtectedOperatorHost',Host):
            self.render();host=self.ui.session_state[console.SESSION_KEY]
            self.assertIsNotNone(host.config.corpus);host.corpus_prepare.assert_not_called()
            self.ui.button.side_effect=lambda label:label=='Prepare selected retrieval corpus'
            self.render();host.corpus_prepare.assert_called_once_with({})
            self.ui.button.side_effect=lambda label:label=='Recover saved retrieval corpus'
            self.render();host.corpus_recover.assert_called_once_with({})
            host.planner_run.assert_not_called();host.retrieval_measure.assert_not_called()
            configured=host.config
        from src.application.campaign_operator_host import ProtectedOperatorHost,config_from_wire,request_values
        from src.application.campaign_operator_worker import execute
        from src.application.operation_channel import ChannelError
        real=ProtectedOperatorHost(configured)
        self.assertEqual(config_from_wire(real._config_values()),configured)
        receipt=dict(binding_id='eval_corpus_binding_v1_'+'a'*64,corpus_sha256='b'*64,
            selection_sha256=configured.corpus.selection_sha256,run_id=configured.corpus.run_id,
            source_count=3,chunk_count=3,image_count=1,text_bytes=1690,claim_approval=False,
            generation_authorized=False,qualifying=False,release_allowed=False)
        for method in ('corpus_prepare','corpus_recover'):
            self.assertEqual(real._decode_result(method,receipt),receipt)
            real._validate_result_scope(method,{},receipt)
            with self.assertRaises(ChannelError):real._validate_result_scope(method,{},dict(receipt,selection_sha256='c'*64))
            with self.assertRaises(ChannelError):request_values(method,{},dict(selection_json='browser override'))
            worker=Mock();execute(None,configured,method,{}, {},corpus=worker)
            worker.prepare.assert_called_once_with({},recover_only=method=='corpus_recover')
        from dataclasses import replace
        with self.assertRaises(ValueError):replace(configured.corpus,selection_sha256='c'*64)
        selected=json.loads(configured.corpus.selection_json);selected['image_binding_id']='eval_upload_binding_v1_'+'c'*64
        altered=replace(configured.corpus,selection_json=console.canonical(selected),selection_sha256=console.digest(selected))
        with self.assertRaises(ChannelError):replace(configured,corpus=altered)
        self.assertFalse(configured.writes_enabled);self.assertIsNone(configured.policy)

    def test_corpus_failure_does_not_suggest_a_model_run(self):
        from src.ui.marketing_pilot import show_planner_failure
        from src.application.sharing_operation_host import SharingHostError
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING'):
            show_planner_failure(self.ui,SharingHostError('CORPUS_BINDING_NOT_VERIFIED'),corpus=True)
        self.assertIn('reuse saved extractions',self.ui.error.call_args.args[0])
        self.assertNotIn('click Run',self.ui.error.call_args.args[0])

    def test_delivery_error_offers_recovery_and_logs_only_safe_category(self):
        from src.ui.marketing_pilot import show_planner_failure
        from src.application.sharing_operation_host import SharingHostError
        ui=Mock()
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING') as logs:
            show_planner_failure(ui,SharingHostError('OPERATION_DEADLINE_EXCEEDED'))
        self.assertIn('Do not click Run again',ui.error.call_args.args[0])
        self.assertIn('OPERATION_DEADLINE_EXCEEDED',logs.output[0])
        self.assertIn('OPERATION_DEADLINE_EXCEEDED',ui.caption.call_args.args[0])
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING') as logs:
            show_planner_failure(ui,ValueError('secret-token identity@example.com'))
        self.assertNotIn('secret-token',str(logs.output));self.assertNotIn('identity@example.com',str(logs.output))
        self.assertIn('RUNTIME_UNAVAILABLE',ui.caption.call_args.args[0])

    def test_session_refresh_message_never_suggests_replaying_original_run(self):
        from src.ui.marketing_pilot import show_planner_failure
        from src.application.sharing_operation_host import SharingHostError
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING'):
            show_planner_failure(self.ui,SharingHostError('PLANNER_SESSION_REFRESH_REQUIRED'))
        self.assertIn('No model was dispatched by this denied attempt',self.ui.error.call_args.args[0])

    def test_retrieval_failure_names_only_its_own_recovery_action(self):
        from src.ui.marketing_pilot import show_planner_failure
        from src.application.sharing_operation_host import SharingHostError
        with self.assertLogs('src.ui.marketing_pilot',level='WARNING') as logs:
            show_planner_failure(self.ui,SharingHostError('ACCESS_DENIED'),retrieval=True)
        message=self.ui.error.call_args.args[0]
        self.assertIn('Recover the original retrieval corpus measurement',message)
        self.assertIn('Do not repeat Measure',message)
        self.assertNotIn('click Run',message)
        self.assertIn('"action": "retrieval"',logs.output[0])

    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.config=json.loads(Path('config/evaluation/rejuvenate_planner_pilot.json').read_text())
        self.path=Path(self.directory.name)/'planner.json';self.save()
        self.actor=replace(identity(),audience=AUDIENCE)
        principal=self.actor.principal.principal_id
        self.environment=dict(CI_OPERATOR_PUBLISHER_PRINCIPAL_ID=principal,
            CI_MARKETING_PLANNER_EVALUATION_ENABLED='true',CI_MEASUREMENT_RUNTIME_ENABLED='true',
            CI_MEASUREMENT_WORKSPACE_ID='ci-poc-marketing',CI_MEASUREMENT_PUBLISHER_PRINCIPAL_ID=principal,
            CI_MEASUREMENT_REVIEW_SHA256='a'*64,CI_MEASUREMENT_EVALUATOR_SHA256='b'*64,
            CI_PLANNER_EVALUATION_CONFIG=str(self.path))
        self.scope=('ci-poc-marketing',self.config['conversation_id'])
        self.ui=Mock();self.ui.session_state={};self.ui.button.return_value=False
        self.settings=SimpleNamespace(iap=IapSettings(AUDIENCE))

    def save(self):self.path.write_text(json.dumps(self.config))

    def render(self):
        return console.render(self.ui,{},self.actor,self.settings,planner_scope=self.scope)

    def test_pinned_configuration_stays_disabled_and_never_constructs_worker(self):
        self.assertFalse(self.config['enabled'])
        self.assertEqual(self.config['binding_id'],
            'eval_upload_binding_v1_81feff98013b48e40fddb1fca01ec31e08c5cfdf9db537096697d29dee5131df')
        with patch.dict('os.environ',self.environment,clear=True),patch.object(console,'ProtectedOperatorHost') as host:
            self.render()
        host.assert_not_called();self.assertIn('execution is disabled',self.ui.info.call_args.args[0])

    def test_other_conversation_and_workspace_deny_before_worker_start(self):
        for scope in (('other',self.config['conversation_id']),('ci-poc-marketing','other')):
            self.scope=scope
            with patch.dict('os.environ',self.environment,clear=True),patch.object(console,'ProtectedOperatorHost') as host:
                with self.assertRaisesRegex(ValueError,'MARKETING_PLANNER_SCOPE_DENIED'):self.render()
            host.assert_not_called()

    def test_foreign_publisher_and_disabled_marketing_flag_deny(self):
        for change in (dict(CI_OPERATOR_PUBLISHER_PRINCIPAL_ID='other'),
                       dict(CI_MARKETING_PLANNER_EVALUATION_ENABLED='false')):
            with patch.dict('os.environ',dict(self.environment,**change),clear=True),patch.object(console,'ProtectedOperatorHost') as host:
                with self.assertRaises(ValueError):self.render()
            host.assert_not_called()

    def test_explicit_run_uses_marketing_audience_without_admin_or_interpretation_actions(self):
        self.config['enabled']=True;self.save()
        class Host:
            def __init__(self,config,*args):
                self.config=config;self.start=Mock();self.close=Mock(return_value=True)
                self.planner_run=Mock(return_value={});self.planner_recover=Mock(return_value={})
                self.retrieval_measure=Mock(return_value={});self.retrieval_recover=Mock(return_value={})
        with patch.dict('os.environ',self.environment,clear=True),patch.object(console,'ProtectedOperatorHost',Host):
            self.render();host=self.ui.session_state[console.SESSION_KEY]
            host.planner_run.assert_not_called();host.planner_recover.assert_not_called()
            self.assertEqual(host.config.iap.expected_audience,AUDIENCE)
            self.assertFalse(host.config.writes_enabled);self.assertIsNone(host.config.policy)
            self.ui.button.side_effect=lambda label:label=='Run the configured real-image planner evaluation'
            self.render();host.planner_run.assert_called_once_with({})
            self.ui.button.side_effect=lambda label:label=='Recover the original planner evaluation records'
            self.render();host.planner_run.assert_called_once();host.planner_recover.assert_called_once_with({})
            self.ui.button.side_effect=lambda label:label=='Measure retrieval corpus for the saved planner stage'
            self.render();host.retrieval_measure.assert_not_called()
            self.ui.button.side_effect=lambda label:label=='Recover the original retrieval corpus measurement'
            self.render();host.retrieval_recover.assert_not_called();host.planner_run.assert_called_once()
        labels=[c.args[0] for c in self.ui.button.call_args_list]
        self.assertNotIn('Read current operator authority',labels)
        self.assertNotIn('Verify measurement runtime and authority',labels)
        self.assertNotIn('Run the configured interpretation evaluation',labels)

    def test_disabled_configuration_closes_retained_enabled_host(self):
        class Host:
            def __init__(self):self.close=Mock(return_value=True)
        retained=Host();self.ui.session_state[console.SESSION_KEY]=retained
        with patch.dict('os.environ',self.environment,clear=True),patch.object(console,'ProtectedOperatorHost',Host):self.render()
        retained.close.assert_called_once();self.assertNotIn(console.SESSION_KEY,self.ui.session_state)

    def test_real_marketing_ui_requires_explicit_view_and_keeps_normal_runtime_separate(self):
        from streamlit.testing.v1 import AppTest
        pilot=Mock();pilot.inspect.return_value=(self.actor,None,dict(roles=['CREATOR']),{'conversation_id':self.scope[1]},self.scope[1])
        pilot.manifest={'workspace_id':self.scope[0]};pilot.verifier.settings=self.settings.iap
        app=AppTest.from_string('from src.ui.marketing_pilot import main\nmain()')
        app.query_params['campaign']='rejuvenate-evaluation-v1'
        with patch.dict('os.environ',self.environment,clear=True),patch('src.ui.marketing_pilot.pilot',return_value=pilot),\
                patch('src.ui.marketing_pilot.iap_headers',return_value={}),patch('src.ui.campaign_chat.main') as chat,\
                patch.object(console,'render') as render:
            app.run();self.assertFalse(app.exception);render.assert_not_called();chat.assert_called_once()
            pilot.runtime.reset_mock();chat.reset_mock()
            next(c for c in app.checkbox if c.label=='Private planner evaluation').check().run()
            self.assertFalse(app.exception);render.assert_called_once()
            self.assertEqual(render.call_args.kwargs,dict(planner_scope=self.scope))
            self.assertEqual(render.call_args.args[3].iap,self.settings.iap)
            pilot.runtime.assert_not_called();chat.assert_not_called()
