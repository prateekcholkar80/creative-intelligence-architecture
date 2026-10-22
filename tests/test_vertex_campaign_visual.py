from dataclasses import replace
import hashlib,io,json,time
from unittest import TestCase
from unittest.mock import Mock,patch
from PIL import Image
from google.genai import types
from src.application.campaign_evaluator_store import canonical
from src.application.campaign_product_image_service import DerivedImageCapture
from src.application.campaign_visual_request import *
from src.generators.vertex_campaign_visual import VertexCampaignVisualTransport,extract_visual
from src.application.operation_budget import worker_operation_scope
from tests.test_campaign_artifact_execution import ArtifactExecutionTests
from tests.test_campaign_product_image_service import fixture as crop_fixture


class VertexVisualTests(TestCase):
    def request(self,authorized=False):
        p,engine,*_=ArtifactExecutionTests().fixture()
        with worker_operation_scope(time.monotonic()+60):
            crop,_,records,data,inputs=crop_fixture()
            r=crop.prepare({},workspace_id='workspace',conversation_id='conversation',request_id='request')
        captured=DerivedImageCapture(r['binding_id'],canonical(records[r['binding_id']]),data[r['binding_id']],inputs.valid_until)
        return prepare_visual_request(run_id='visual-pilot',case_id='rejuvenate',artifact_input=json.loads(p.intent_json),
            crop=captured,planner_outcome_sha256='a'*64,checked_at=engine.clock(),profile=VisualProfile(endpoint_authorized=authorized))

    def response(self):
        out=io.BytesIO();Image.new('RGB',(768,432),'#e8d9c2').save(out,format='PNG')
        return types.GenerateContentResponse(model_version='gemini-3.1-flash-image',candidates=[types.Candidate(
            finish_reason='STOP',content=types.Content(parts=[types.Part.from_text(text='Private scene'),
                types.Part.from_bytes(data=out.getvalue(),mime_type='image/png')]))])

    def test_region_authorization_is_required_before_client_creation(self):
        request=self.request()
        with patch('src.generators.vertex_campaign_visual.genai.Client') as client,worker_operation_scope(time.monotonic()+180):
            with self.assertRaisesRegex(ValueError,'VISUAL_ENDPOINT_AUTHORIZATION_REQUIRED'):VertexCampaignVisualTransport()(request)
        client.assert_not_called()

    def test_sdk_uses_one_attempt_explicit_vertex_image_modality_and_exact_crop(self):
        request=self.request(True);response=self.response()
        with patch('src.generators.vertex_campaign_visual.genai.Client') as client,worker_operation_scope(time.monotonic()+180):
            model=client.return_value.__enter__.return_value.models;model.generate_content.return_value=response
            result=VertexCampaignVisualTransport()(request)
        model.generate_content.assert_called_once()
        kwargs=client.call_args.kwargs;self.assertIs(kwargs['vertexai'],True);self.assertEqual(kwargs['location'],'global')
        self.assertEqual(kwargs['http_options'].retry_options.attempts,1)
        self.assertLessEqual(kwargs['http_options'].timeout,120000)
        call=model.generate_content.call_args.kwargs
        self.assertEqual(call['model'],'gemini-3.1-flash-image')
        self.assertEqual(call['contents'][0].parts[1].inline_data.data,request.crop_png)
        self.assertEqual(call['config'].response_modalities,['TEXT','IMAGE'])
        self.assertEqual(call['config'].candidate_count,1)
        self.assertEqual(result.observation.response_sha256,hashlib.sha256(result.response_manifest_json.encode()).hexdigest())
        self.assertIsNone(result.observation.input_tokens);self.assertIsNone(result.observation.output_tokens)

    def test_blocked_partial_missing_multiple_or_badly_shaped_images_are_rejected(self):
        for fault in ('finish','blocked','missing','multiple','mime','model','shape'):
            response=self.response();candidate=response.candidates[0]
            if fault=='finish':candidate.finish_reason='MAX_TOKENS'
            if fault=='blocked':candidate.safety_ratings=[types.SafetyRating(category='HARM_CATEGORY_DANGEROUS_CONTENT',blocked=True)]
            if fault=='missing':candidate.content.parts=candidate.content.parts[:1]
            if fault=='multiple':candidate.content.parts.append(candidate.content.parts[-1])
            if fault=='mime':candidate.content.parts[-1].inline_data.mime_type='image/webp'
            if fault=='model':response.model_version=None
            if fault=='shape':
                out=io.BytesIO();Image.new('RGB',(512,512),'white').save(out,format='PNG')
                candidate.content.parts[-1].inline_data.data=out.getvalue()
            with self.subTest(fault=fault),self.assertRaises(ValueError):extract_visual(response,'gemini-3.1-flash-image')

    def test_transport_does_not_retry_an_uncertain_provider_call(self):
        request=self.request(True)
        with patch('src.generators.vertex_campaign_visual.genai.Client') as client,worker_operation_scope(time.monotonic()+180):
            model=client.return_value.__enter__.return_value.models;model.generate_content.side_effect=TimeoutError()
            with self.assertRaises(TimeoutError):VertexCampaignVisualTransport()(request)
        model.generate_content.assert_called_once()

    def test_changed_source_prompt_or_input_shape_denies_before_dispatch(self):
        request=self.request(True)
        for fault in ('bytes','system','slot','content'):
            value=request.intent;raw=request.crop_png
            if fault=='bytes':raw=b'changed'
            if fault=='system':value['system']='different'
            if fault=='slot':value['record_id']='0'*64
            if fault=='content':
                value['content']=canonical(dict(product_identity='Rejuvenate',reference_use='CLAIMS',output_use='release'))
                value['content_sha256']=hashlib.sha256(value['content'].encode()).hexdigest()
            with patch('src.generators.vertex_campaign_visual.genai.Client') as client,worker_operation_scope(time.monotonic()+60):
                with self.subTest(fault=fault),self.assertRaises(ValueError):VertexCampaignVisualTransport()(VisualRequest(canonical(value),raw))
            client.assert_not_called()
