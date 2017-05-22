import uuid
import pytest
import six

from datarobot_batch_scoring.utils import UI

pytest_plugins = ['liveserver_fixtures']


@pytest.fixture(scope='session')
def session_id():
    '''Unique session identifier, random string.'''
    return str(uuid.uuid4())


@pytest.yield_fixture(scope='function')
def ui():
    '''Unique session identifier, random string.'''
    ui = UI(True, 'DEBUG', False)
    yield ui
    ui.close()


@pytest.fixture
def csv_file_handle_with_wide_field():
    s = six.StringIO()
    s.write('idx,data\n')
    s.write('1,one\n')
    s.write('2,two\n')
    s.write('3,three\n')
    s.write('4,')
    for idx in six.moves.range(50000):
        s.write('spam{}'.format(idx))
    s.seek(0)
    return s


@pytest.fixture
def csv_file_with_wide_dataset():
    """The auto_sampler will only look at a half-MB to estimate a good sample size
    to send per batch. If the dataset doesn't get to one full line before this value
    then the auto_sampler would fail (PRED-1240).

    """
    s = six.StringIO()
    # write header
    for i in range(1024 * 128):
        s.write('column_{:0>8},'.format(i))
    s.write('end\n')
    for i in range(1024 * 128):
        s.write('1,')
    s.write('0\n')
    s.seek(0)
    return s
