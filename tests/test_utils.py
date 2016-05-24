import logging
import mock
import pytest
from datarobot_batch_scoring.utils import (verify_objectid, UI,
                                           iter_chunks, acquire_api_token)


def test_invalid_objectid():
    with pytest.raises(ValueError):
        verify_objectid('123')


class TestUi(object):
    def test_prompt_yesno_always_yes(self):
        ui = UI(True, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            assert ui.prompt_yesno('msg')
            assert not m_input.called

    def test_prompt_yesno_always_no(self):
        ui = UI(False, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            assert not ui.prompt_yesno('msg')
            assert not m_input.called

    def test_prompt_yesno_user_input_yes(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'yEs'
            assert ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_no(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'nO'
            assert not ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_invalid(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.side_effect = ['invalid', 'yes']
            assert ui.prompt_yesno('msg')
            m_input.assert_has_calls([mock.call('msg (Yes/No)> '),
                                      mock.call('Please type (Yes/No)> ')])

    def test_prompt_yesno_user_input_y(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'y'
            assert ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_n(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'n'
            assert not ui.prompt_yesno('msg')
            m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_user(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
            m_input.return_value = 'Andrew'
            assert ui.prompt_user() == 'Andrew'
            m_input.assert_called_with('user name> ')

    def test_debug(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            ui.debug('text')
            m_log.debug.assert_called_with('text')

    def test_info(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            ui.info('text')
            m_log.info.assert_called_with('text')

    def test_warning(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            ui.warning('text')
            m_log.warning.assert_called_with('text')

    def test_error(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            with mock.patch(
                    'datarobot_batch_scoring.utils.root_logger') as m_root:
                ui.error('text')
                m_log.error.assert_called_with('text')
                m_root.error.assert_called_with('text', exc_info=False)

    def test_error_with_excinfo(self):
        ui = UI(None, logging.DEBUG, stdout=False)
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
        ui = UI(None, logging.DEBUG, stdout=False)
        msg = ('{}\nIf you need assistance please send the log \n'
               'file {} to support@datarobot.com .').format(
                   'text', ui.root_logger_filename)
        with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
            with mock.patch(
                    'datarobot_batch_scoring.utils.root_logger') as m_root:
                with mock.patch(
                        'datarobot_batch_scoring.utils.os._exit') as m_exit:
                    ui.fatal('text')
                m_log.error.assert_called_with(msg)
                m_root.error.assert_called_with(msg,
                                                exc_info=(None, None, None))
                m_exit.assert_called_with(1)

    def test_getpass(self):
        ui = UI(None, logging.DEBUG, stdout=False)
        with mock.patch(
                'datarobot_batch_scoring.utils.getpass.getpass') as m_getpass:
            m_getpass.return_value = 'passwd'
            assert 'passwd' == ui.getpass()
            m_getpass.assert_called_with('password> ')

    def test_getpass_noninteractive(self):
        ui = UI(True, logging.DEBUG, stdout=False)
        with mock.patch(
                'datarobot_batch_scoring.utils.getpass.getpass') as m_getpass:
            with pytest.raises(RuntimeError) as exc:
                ui.getpass()
            assert str(exc.value) == "Non-interactive session"
            assert not m_getpass.called


def test_iter_chunks():
    csvfile = [[1, 'a'],
               [2, 'b'],
               [3, 'c']]

    it = iter_chunks(csvfile, 2)
    chunk1 = next(it)
    assert [[1, 'a'], [2, 'b']] == chunk1
    chunk2 = next(it)
    assert [[3, 'c']] == chunk2
    with pytest.raises(StopIteration):
        next(it)


def test_acquire_api_token(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    ret = acquire_api_token(base_url, {}, 'username', 'password', False, ui)
    assert ret == 'Som3tok3n'
    ui.info.assert_called_with('api-token acquired')
    ui.debug.assert_called_with('api-token: Som3tok3n')


def test_acquire_api_token_unauthorized(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'unknown', 'passwd', False, ui)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == 'wrong credentials'


def test_acquire_api_token_bad_status(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'bad_status', 'passwd', False, ui)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == 'api_token request returned status code 500'


def test_acquire_api_token_no_token1(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'no_token1', 'passwd', False, ui)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == ('no api-token registered; '
                              'please run with --create_api_token flag.')


def test_acquire_api_token_no_token2(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'no_token2', 'passwd', False, ui)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == ('no api-token registered; '
                              'please run with --create_api_token flag.')


def test_create_and_acquire_api_token(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    ret = acquire_api_token(base_url, {}, 'username', 'password', True, ui)
    assert ret == 'Som3tok3n'
    ui.info.assert_called_with('api-token acquired')
    ui.debug.assert_called_with('api-token: Som3tok3n')
