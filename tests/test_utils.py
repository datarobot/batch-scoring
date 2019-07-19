import csv
import logging
import mock
import pytest
from datarobot_batch_scoring.utils import (verify_objectid, UI,
                                           acquire_api_token,
                                           parse_host,
                                           get_endpoint,
                                           SerializableDialect)
from datarobot_batch_scoring.reader import (iter_chunks,
                                            investigate_encoding_and_dialect,
                                            auto_sampler)
from utils import PickableMock


def test_invalid_objectid():
    with pytest.raises(ValueError):
        verify_objectid('123')


class TestUi(object):
    def test_prompt_yesno_always_yes(self):
        with UI(True, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
                assert ui.prompt_yesno('msg')
                assert not m_input.called

    def test_prompt_yesno_always_no(self):
        with UI(False, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
                assert not ui.prompt_yesno('msg')
                assert not m_input.called

    def test_prompt_yesno_user_input_yes(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
                m_input.return_value = 'yEs'
                assert ui.prompt_yesno('msg')
                m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_no(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
                m_input.return_value = 'nO'
                assert not ui.prompt_yesno('msg')
                m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_invalid(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
                m_input.side_effect = ['invalid', 'yes']
                assert ui.prompt_yesno('msg')
                m_input.assert_has_calls([mock.call('msg (Yes/No)> '),
                                          mock.call('Please type (Yes/No)> ')])

    def test_prompt_yesno_user_input_y(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
                m_input.return_value = 'y'
                assert ui.prompt_yesno('msg')
                m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_yesno_user_input_n(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.input') as m_input:
                m_input.return_value = 'n'
                assert not ui.prompt_yesno('msg')
                m_input.assert_called_with('msg (Yes/No)> ')

    def test_prompt_user(self):
            with UI(None, logging.DEBUG, stdout=False) as ui:
                with mock.patch('datarobot_batch_scoring.utils.input'
                                '') as m_input:
                    m_input.return_value = 'Andrew'
                    assert ui.prompt_user() == 'Andrew'
                    m_input.assert_called_with('user name> ')

    def test_debug(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
                ui.debug('text')
                m_log.debug.assert_called_with('text')

    def test_info(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
                ui.info('text')
                m_log.info.assert_called_with('text')

    def test_warning(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
                ui.warning('text')
                m_log.warning.assert_called_with('text')

    def test_error(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
                with mock.patch(
                        'datarobot_batch_scoring.utils.root_logger') as m_root:
                    ui.error('text')
                    m_log.error.assert_called_with('text')
                    m_root.error.assert_called_with('text', exc_info=False)

    def test_error_with_excinfo(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
                with mock.patch('datarobot_batch_scoring.utils.root_logger'
                                '') as m_root:
                    try:
                        1 / 0
                    except:
                        ui.error('text')
                m_log.error.assert_called_with('text')
                m_root.error.assert_called_with('text', exc_info=True)

    def test_fatal(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.logger') as m_log:
                with mock.patch(
                        'datarobot_batch_scoring.utils.root_logger') as m_root:
                    with mock.patch(
                            'datarobot_batch_scoring.utils.sys.exit'
                            '') as m_exit:
                        msg = ('{}\nIf you need assistance please send the '
                               'log file/s:\n{}to support@datarobot.com.'
                               '').format('text', ui.get_all_logfiles())
                        ui.fatal('text')
                        m_log.error.assert_called_with(msg)
                        m_root.error.assert_called_with(msg,
                                                        exc_info=(None, None,
                                                                  None))
                        m_exit.assert_called_with(1)

    def test_getpass(self):
        with UI(None, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.getpass.getpass'
                            '') as m_getpass:
                m_getpass.return_value = 'passwd'
                assert 'passwd' == ui.getpass()
                m_getpass.assert_called_with('password> ')

    def test_getpass_noninteractive(self):
        with UI(True, logging.DEBUG, stdout=False) as ui:
            with mock.patch('datarobot_batch_scoring.utils.getpass.getpass'
                            '') as m_getpass:
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


def test_investigate_encoding_and_dialect():
    with UI(None, logging.DEBUG, stdout=False) as ui:
        data = 'tests/fixtures/windows_encoded.csv'
        encoding = investigate_encoding_and_dialect(data, None, ui)
        dialect = csv.get_dialect('dataset_dialect')
        assert encoding == 'windows-1252'
        assert dialect.lineterminator == '\r\n'
        assert dialect.quotechar == '"'
        assert dialect.delimiter == ','


def test_investigate_encoding_and_dialect_submit_encoding():

    with UI(None, logging.DEBUG, stdout=False) as ui:
        with mock.patch('datarobot_batch_scoring.reader.chardet.detect') as cd:
            data = 'tests/fixtures/windows_encoded.csv'
            encoding = investigate_encoding_and_dialect(data, None, ui,
                                                        fast=False,
                                                        encoding='iso-8859-2',
                                                        skip_dialect=False)
        assert encoding == 'iso-8859-2'
        assert not cd.called


def test_investigate_encoding_and_dialect_skip_dialect():

    with UI(None, logging.DEBUG, stdout=False) as ui:
        with mock.patch('datarobot_batch_scoring.reader.csv.Sniffer') as sn:
            data = 'tests/fixtures/windows_encoded.csv'
            encoding = investigate_encoding_and_dialect(data, None, ui,
                                                        fast=False,
                                                        encoding='',
                                                        skip_dialect=True)
        assert encoding == 'windows-1252'
        assert not sn.called
        dialect = csv.get_dialect('dataset_dialect')
        assert dialect.delimiter == ','


def test_investigate_encoding_and_dialect_substitute_delimiter():

    with UI(None, logging.DEBUG, stdout=False) as ui:
        with mock.patch('datarobot_batch_scoring.reader.csv.Sniffer') as sn:
            data = 'tests/fixtures/windows_encoded.csv'
            encoding = investigate_encoding_and_dialect(data, '|', ui,
                                                        fast=False,
                                                        encoding='',
                                                        skip_dialect=True)
        assert encoding == 'windows-1252'
        assert not sn.called
        dialect = csv.get_dialect('dataset_dialect')
        assert dialect.delimiter == '|'


def test_stdout_logging_and_csv_module_fail(capsys):
    with UI(None, logging.DEBUG, stdout=True) as ui:
        data = 'tests/fixtures/unparsable.csv'
        exc = str("""[ERROR] The csv module failed to detect the CSV """ +
                  """dialect. Try giving hints with the --delimiter """ +
                  """argument, E.g  --delimiter=','""")
        msg = ('{}\nIf you need assistance please send the output of this '
               'script to support@datarobot.com.').format(exc)
        with mock.patch('datarobot_batch_scoring.utils.sys.exit') as m_exit:
            with pytest.raises(csv.Error):
                investigate_encoding_and_dialect(data, None, ui)
            m_exit.assert_called_with(1)
        out, err = capsys.readouterr()
        assert msg in out.strip('\n')


def test_auto_sample():
    with UI(None, logging.DEBUG, stdout=False) as ui:
        data = 'tests/fixtures/criteo_top30_1m.csv.gz'
        encoding = investigate_encoding_and_dialect(data, None, ui)
        assert auto_sampler(data, encoding, ui) == 14980
        ui.close()


def test_auto_small_dataset():
    with UI(None, logging.DEBUG, stdout=False) as ui:
        data = 'tests/fixtures/regression_jp.csv.gz'
        encoding = investigate_encoding_and_dialect(data, None, ui)
        assert auto_sampler(data, encoding, ui) == 500


def test_acquire_api_token(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    ret = acquire_api_token(base_url, {}, 'username', 'password', False, ui,
                            False)
    assert ret == 'Som3tok3n'
    ui.info.assert_called_with('api-token acquired')
    ui.debug.assert_called_with('api-token: Som3tok3n')


def test_acquire_api_token_unauthorized(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'unknown', 'passwd', False, ui,
                          False)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == 'wrong credentials'


def test_acquire_api_token_bad_status(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'bad_status', 'passwd', False, ui,
                          False)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == 'api_token request returned status code 500'


def test_acquire_api_token_no_token1(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'no_token1', 'passwd', False, ui,
                          False)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == ('no api-token registered; '
                              'please run with --create_api_token flag.')


def test_acquire_api_token_no_token2(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        acquire_api_token(base_url, {}, 'no_token2', 'passwd', False, ui,
                          False)
        assert not ui.info.called
        assert not ui.debug.called
    assert str(ctx.value) == ('no api-token registered; '
                              'please run with --create_api_token flag.')


def test_create_and_acquire_api_token(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    ret = acquire_api_token(base_url, {}, 'username', 'password', True, ui,
                            False)
    assert ret == 'Som3tok3n'
    ui.info.assert_called_with('api-token acquired')
    ui.debug.assert_called_with('api-token: Som3tok3n')


@pytest.fixture
def test_parse_host(input, expected):
    with UI(None, logging.DEBUG, stdout=False) as ui:
        assert parse_host(input, ui) == expected


def test_parse_host_success():
    test_parse_host('http://dr.com', 'http://dr.com/predApi/v1.0/')
    test_parse_host('https://dr.com', 'https://dr.com/predApi/v1.0/')
    test_parse_host('https://dr.com/api', 'https://dr.com/predApi/v1.0/')
    test_parse_host('https://dr.com/predApi/v1.0',
                    'https://dr.com/predApi/v1.0/')
    test_parse_host('https://dr.com/predApi/v1.0/',
                    'https://dr.com/predApi/v1.0/')
    test_parse_host('http://dr.com/predApi/v1.0/',
                    'http://dr.com/predApi/v1.0/')
    test_parse_host('http://dr.com:8080', 'http://dr.com:8080/predApi/v1.0/')
    test_parse_host('https://127.0.0.1/', 'https://127.0.0.1/predApi/v1.0/')


@pytest.mark.parametrize('host, api_version, result', (
    ('https://dr.com/', 'api/v1', 'https://dr.com/api/v1/'),
    ('https://dr.com', 'predApi/v1.0', 'https://dr.com/predApi/v1.0/')
))
def test_get_endpoint(host, api_version, result):
    assert get_endpoint(host, api_version) == result


def test_get_endpoint_raises():
    with pytest.raises(ValueError):
        get_endpoint('localhost', 'api/v2.0')


def test_parse_host_no_protocol_fatal():
    host = '57a2a9eac808914f2fb8f717.com/api'
    with UI(None, logging.INFO, stdout=False) as ui:
        with mock.patch('datarobot_batch_scoring.utils.UI.fatal') as ui_fatal:
            msg = ('Cannot parse "--host" argument. Host address must start '
                   'with a protocol such as "http://" or "https://".'
                   ' Value given: {}').format(host)
            parse_host(host, ui)
            ui_fatal.assert_called_with(msg)


def test_serializable_dialect_fields():
    def same_attr(key, coll1, coll2):
        return getattr(coll1, key) == getattr(coll2, key)
    original_dialect = csv.excel()
    serializable_dialect = SerializableDialect.from_dialect(original_dialect)
    converted_dialect = serializable_dialect.to_dialect()
    attributes = 'delimiter doublequote escapechar ' \
                 'lineterminator quotechar quoting skipinitialspace'.split(' ')
    for key in attributes:
        assert hasattr(serializable_dialect, key)
        assert same_attr(key, serializable_dialect, original_dialect)
        assert same_attr(key, original_dialect, converted_dialect)
