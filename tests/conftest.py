import os
import tempfile
import uuid
import pytest
import six

from datarobot_batch_scoring.utils import UI
from datarobot_batch_scoring.writer import ContextFile

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
def csv_data_with_wide_dataset():
    """Data of a very wide dataset, whose first line does not fit within
    the threshold for the auto_sampler
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


@pytest.fixture
def csv_data_with_cr():
    """ Data where each line is terminated by \r """
    s = six.StringIO()
    # write header
    s.write('idx,data\r')
    # write 3 values
    s.write('1,one\r')
    s.write('2,two\r')
    s.write('3,three\r')
    s.seek(0)
    return s


@pytest.yield_fixture
def csv_file_with_wide_dataset(csv_data_with_wide_dataset):
    """Path to a very wide dataset"""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
        f.write(csv_data_with_wide_dataset.getvalue().encode('utf-8'))
    yield f.name
    os.remove(f.name)


@pytest.yield_fixture
def csv_file_with_cr(csv_data_with_cr):
    """Path to dataset terminated by \r only"""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
        f.write(csv_data_with_cr.getvalue().encode('utf-8'))
    yield f.name
    os.remove(f.name)


@pytest.yield_fixture
def run_context_file():
    c_file = ContextFile('pid', 'lid', 10, None)
    open(c_file.file_name, 'a')
    yield c_file
    c_file.clean()
