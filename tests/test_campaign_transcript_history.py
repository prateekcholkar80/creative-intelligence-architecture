from copy import deepcopy
from datetime import datetime,timezone,timedelta
import hashlib,json
from unittest import TestCase
from src.application.campaign_transcript_history import decode_history,TABLES
from src.application.grounded_interpreter_submission import prepare_grounded_submission
from src.application.interpreter_outcome import prepare_interpreter_outcome
from src.application.workflow_invocation_result_query import ModelResultObservation
from src.application.workflow_codec import encode_row,decode_row
from src.application.conversation_contract import validate_conversation
from src.validation.control_fixtures import controls,profile,ACTOR

SELECT=dict(sku='CI-SKU-0001',country='US',channel='META',audience='Shoppers')

class TranscriptHistoryTests(TestCase):
    def setUp(self):
        self.rows=controls('history');self.conv=self.rows['conversations'][0]
        self.resource=validate_conversation(self.conv,workspace_id='history',conversation_id=self.conv['conversation_id'])
    def submission(self,retry,history=None):
        return prepare_grounded_submission(selection=SELECT,history=history,workspace=self.rows['workspaces'][0],
            membership=self.rows['memberships'][0],conversation=self.conv,resource=self.resource,principal_id=ACTOR,
            message='User '+retry,retry_key=retry,profile=profile('INTERPRETER'),observed_at=datetime.now(timezone.utc),worker_id='worker')
    def completed(self,submission):
        values={p.name:p.value for p in submission.config().query_parameters}
        request=decode_row(values['request_candidate'],'requests');call=decode_row(values['invocation_candidate'],'model_invocations')
        call.update(state='DISPATCHED',revision=2,worker_id='worker',dispatch_receipt_id='dispatch-'+call['invocation_id'])
        raw='{"features":[]}'
        outcome=prepare_interpreter_outcome(workspace=self.rows['workspaces'][0],membership=self.rows['memberships'][0],
            conversation=self.conv,request=request,invocation=call,response_text=raw,
            observation=ModelResultObservation('SUCCEEDED',response_sha256=hashlib.sha256(raw.encode()).hexdigest(),
                finish_reason='STOP',safety_outcome='PASSED',returned_model_id=call['profile']['model_id']),observed_at=datetime.now(timezone.utc))
        result={p.name:p.value for p in outcome.config().query_parameters}
        return dict(requests=result['request_candidate'],calls=result['invocation_candidate'],bodies=values['transcript_candidate'],responses=result['response_candidate'])
    def row(self,*completed):
        row=dict(observed_at=datetime.now(timezone.utc))
        for key in TABLES:row[key]=[r[key] for r in completed] if key not in {'jobs','attempts'} else [];row[key+'_count']=len(row[key])
        return row
    def decode(self,row):return decode_history(row,resource=self.resource,observed_at=datetime.now(timezone.utc))
    def test_two_turns_preserve_raw_users_and_exact_responses(self):
        first=self.completed(self.submission('turn1'));history=self.decode(self.row(first))
        self.assertEqual([t['role'] for t in history.turns],['USER','ASSISTANT'])
        second_plan=self.submission('turn2',history)
        self.assertEqual(second_plan.sql.count('CHAT_TRANSCRIPT_SOURCE_CHANGED'),6)
        second=self.completed(second_plan);full=self.decode(self.row(first,second))
        self.assertEqual([t['role'] for t in full.turns],['USER','ASSISTANT','USER','ASSISTANT'])
        self.assertEqual(full.texts,('User turn1','{"features":[]}','User turn2','{"features":[]}'))
    def test_empty_history_is_explicit_and_captured(self):
        history=self.decode(self.row());plan=self.submission('initial',history)
        self.assertEqual(history.texts,());self.assertIn('history_responses',plan.sql)
    def test_missing_changed_unknown_duplicate_or_expired_history_denies(self):
        first=self.completed(self.submission('turn1'));base=self.row(first)
        for kind in ('missing','changed','unknown','duplicate','expired','count'):
            row=deepcopy(base)
            if kind=='missing':row['responses']=[];row['responses_count']=0
            elif kind=='changed':
                body=json.loads(row['responses'][0]);body['response_text']='changed';row['responses'][0]=json.dumps(body)
            elif kind=='unknown':
                call=decode_row(row['calls'][0],'model_invocations');call['state']='UNKNOWN';row['calls'][0]=encode_row(call,'model_invocations')
            elif kind=='duplicate':row['requests']*=2;row['requests_count']=2
            elif kind=='expired':
                body=json.loads(row['bodies'][0]);body['recorded_at']=(datetime.now(timezone.utc)-timedelta(days=31)).isoformat();row['bodies'][0]=json.dumps(body)
            else:row['requests_count']=2
            with self.subTest(kind=kind),self.assertRaises(ValueError):self.decode(row)
    def test_input_reconstruction_keeps_original_age_and_checks_every_body(self):
        from unittest.mock import Mock,patch
        from src.application.campaign_transcript_history import read_bound_texts
        first=self.completed(self.submission('turn1'));captured=self.decode(self.row(first))
        second_plan=self.submission('turn2',captured)
        values={p.name:p.value for p in second_plan.config().query_parameters}
        second_body=json.loads(values['transcript_candidate'])
        self.assertEqual([i['text'] for i in json.loads(second_body['batch_canonical_json'])],['User turn2'])
        row=self.row(first);row['bodies'].append(values['transcript_candidate']);row['bodies_count']=2
        content=dict(authority=json.loads(values['authority_payload']))
        io=Mock();io.read.return_value=row
        with patch('src.application.campaign_transcript_history.ControlIO',return_value=io):
            result=read_bound_texts(Mock(),content,workspace_id='history',conversation_id=self.conv['conversation_id'],clock=lambda:datetime.now(timezone.utc))
            self.assertEqual(result,('User turn1','{"features":[]}','User turn2'))
            response=json.loads(row['responses'][0]);response['response_text']='changed';row['responses'][0]=json.dumps(response)
            with self.assertRaises(ValueError):read_bound_texts(Mock(),content,workspace_id='history',conversation_id=self.conv['conversation_id'],clock=lambda:datetime.now(timezone.utc))
