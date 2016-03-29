import logging
import mock
import pytest
from datarobot_batch_scoring.utils import verify_objectid, UI


def test_invalid_objectid():
    with pytest.raises(ValueError):
        verify_objectid('123')


class TestUi(object):
    def test_prompt_yesno_always_yes(self):
        ui = UI(True, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            assert ui.prompt_yesno('msg')
            assert not m_input.called

    def test_prompt_yesno_always_no(self):
        ui = UI(False, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            assert not ui.prompt_yesno('msg')
            assert not m_input.called

    def test_prompt_yesno_user_input_yes(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'yEs'
            assert ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_no(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'nO'
            assert not ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_invalid(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.side_effect = ['invalid', 'yes']
            assert ui.prompt_yesno('msg')
            m_input.assert_has_calls([mock.call('msg (Yes/No)> '),
                                      mock.call('Please type (Yes/No)> ')])

    def test_prompt_yesno_user_input_y(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'y'
            assert ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_n(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'n'
            assert not ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_user(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'Andrew'
            assert ui.prompt_user() == 'Andrew'
            m_input.assert_called_with('user name> ')

    def test_debug(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            ui.debug('text')
            m_log.debug.assert_called_with('text')

    def test_info(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            ui.info('text')
            m_log.info.assert_called_with('text')

    def test_warning(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            ui.warning('text')
            m_log.warning.assert_called_with('text')

    def test_error(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            with mock.patch(
                    'datarobot_batch_scoring.utils.root_logger') as m_root:
                ui.error('text')
                m_log.error.assert_called_with('text')
                m_root.error.assert_called_with('text', exc_info=False)

    def test_error_with_excinfo(self):
        ui = UI(None, logging.DEBUG)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            with mock.patch(
                    'datarobot_batch_scoring.utils.root_logger') as m_root:
                try:
                    1 / 0
                except:
                    ui.error('text')
                m_log.error.assert_called_with('text')
                m_root.error.assert_called_with('text', exc_info=True)

    def test_fatal(self):
        ui = UI(None, logging.DEBUG)
        msg = ('{}\nIf you need assistance please send the log \n'
               'file {} to support@datarobot.com .').format(
                   'text', ui.root_logger_filename)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            with mock.patch(
                    'datarobot_batch_scoring.utils.root_logger') as m_root:
                with mock.patch(
                        'datarobot_batch_scoring.utils.sys.exit') as m_exit:
                    ui.fatal('text')
                m_log.error.assert_called_with(msg)
                m_root.error.assert_called_with(msg,
                                                exc_info=(None, None, None))
                m_exit.assert_called_with(1)
