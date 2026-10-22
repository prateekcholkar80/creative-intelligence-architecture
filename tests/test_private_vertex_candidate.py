from unittest import TestCase
from unittest.mock import MagicMock
from src.ui.private_vertex_candidate import render,failure
from src.application.sharing_operation_host import SharingHostError


class PrivateVertexUiTests(TestCase):
    def fixture(self,button):
        st=MagicMock();st.session_state={};st.button.side_effect=lambda label,**kw:label==button and not kw.get('disabled',False)
        host=MagicMock();return st,host

    def test_one_action_generates_visual_then_banner_and_never_recovers_implicitly(self):
        st,host=self.fixture('Generate private Vertex candidate');render(st,host,{'iap':'current'},'fixed-run')
        host.vertex_visual_run.assert_called_once_with({'iap':'current'})
        host.vertex_banner_run.assert_called_once_with({'iap':'current'})
        host.vertex_visual_recover.assert_not_called();self.assertFalse(next(iter(st.session_state.values())))

    def test_uncertain_image_disables_run_and_does_not_start_banner(self):
        st,host=self.fixture('Generate private Vertex candidate');host.vertex_visual_run.side_effect=SharingHostError('OPERATION_DEADLINE_EXCEEDED')
        with self.assertLogs('src.ui.private_vertex_candidate',level='WARNING'):render(st,host,{},'fixed-run')
        host.vertex_banner_run.assert_not_called();self.assertTrue(next(iter(st.session_state.values())))
        host.vertex_visual_run.reset_mock();render(st,host,{},'fixed-run');host.vertex_visual_run.assert_not_called()

    def test_recovery_is_read_only_and_missing_banner_allows_explicit_resume(self):
        st,host=self.fixture('Recover original Vertex candidate')
        host.vertex_banner_recover.side_effect=SharingHostError('ARTIFACT_ORIGINAL_MISSING')
        render(st,host,{},'fixed-run')
        host.vertex_visual_recover.assert_called_once();host.vertex_banner_recover.assert_called_once()
        host.vertex_visual_run.assert_not_called();host.vertex_banner_run.assert_not_called()
        self.assertFalse(next(iter(st.session_state.values())));st.info.assert_called_once()

    def test_unrecognized_error_values_never_appear_in_ui_or_logs(self):
        st=MagicMock()
        with self.assertLogs('src.ui.private_vertex_candidate',level='WARNING') as log:
            failure(st,SharingHostError('secret@example.com'))
        self.assertNotIn('secret@example.com',str(log.output));st.caption.assert_called_with('Diagnostic reference: RUNTIME_UNAVAILABLE')
