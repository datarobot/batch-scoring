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
