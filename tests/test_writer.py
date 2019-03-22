import mock
from datarobot_batch_scoring.writer import (RunContext, NewRunContext,
                                            OldRunContext)


def test_no_resume_existing_no_ask_new_context(run_context_file):
    """
    If --no-resume provided for call, and there was
    old run already, it should create new run context
    without asking
    """
    ui_mock = mock.Mock()
    with mock.patch('datarobot_batch_scoring.writer.csv'):
        ctx = RunContext.create(
            resume=False,
            n_samples=10,
            out_file='out.csv',
            pid='pid',
            lid='lid',
            keep_cols=None,
            n_retry=3,
            delimiter=',', dataset='dataset.csv',
            pred_name='pred_name', ui=ui_mock,
            fast_mode=None, encoding=None, skip_row_id=None,
            output_delimiter=None,
            pred_decision_name=None)
        assert ui_mock.call_count == 0
        assert isinstance(ctx, NewRunContext)


def test_resume_if_it_was_run_already(run_context_file):
    """
    If --resume provided and there was old run_context
    it should continue to run (provide oldruncontext) without
    asking
    """
    ui_mock = mock.Mock()
    with mock.patch('datarobot_batch_scoring.writer.csv'):
        ctx = RunContext.create(
            resume=True,
            n_samples=10,
            out_file='out.csv',
            pid='pid',
            lid='lid',
            keep_cols=None,
            n_retry=3,
            delimiter=',', dataset='dataset.csv',
            pred_name='pred_name', ui=ui_mock,
            fast_mode=None, encoding=None, skip_row_id=None,
            output_delimiter=None,
            pred_decision_name=None)
        assert ui_mock.call_count == 0
        assert isinstance(ctx, OldRunContext)


def test_asking_if_resume_not_provided(run_context_file):
    """
    If no resume args was provided and there found new context
    it should ask for a question
    """
    ui_mock = mock.Mock()
    with mock.patch('datarobot_batch_scoring.writer.csv'):
        RunContext.create(
            resume=None,
            n_samples=10,
            out_file='out.csv',
            pid='pid',
            lid='lid',
            keep_cols=None,
            n_retry=3,
            delimiter=',', dataset='dataset.csv',
            pred_name='pred_name', ui=ui_mock,
            fast_mode=None, encoding=None, skip_row_id=None,
            output_delimiter=None,
            pred_decision_name=None)
        ui_mock.prompt_yesno.assert_called_once_with(
            'Existing run found. Resume')
