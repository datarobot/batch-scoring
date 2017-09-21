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
    stream = six.StringIO()
    stream.write('idx,data\n')
    stream.write('1,one\n')
    stream.write('2,two\n')
    stream.write('3,three\n')
    stream.write('4,')
    for idx in six.moves.range(50000):
        stream.write('spam{}'.format(idx))
    stream.seek(0)
    return stream


@pytest.fixture
def csv_data_with_wide_dataset():
    """Data of a very wide dataset, whose first line does not fit within
    the threshold for the auto_sampler
    """
    stream = six.StringIO()
    # write header
    for i in range(1024 * 128):
        stream.write('column_{:0>8},'.format(i))
    stream.write('end\n')
    for i in range(1024 * 128):
        stream.write('1,')
    stream.write('0\n')
    stream.seek(0)
    return stream


def csv_data_with_term(term):
    """ Data where each line is terminated by term """
    stream = six.StringIO()
    data = [
        'idx,data',
        '1,one',
        '2,two',
        '3,three',
    ]
    for line in data:
        stream.write(line + term)
    stream.seek(0)
    return stream


@pytest.fixture
def csv_data_with_cr():
    """ Data where each line is terminated by \r """
    return csv_data_with_term('\r')


@pytest.fixture
def csv_data_with_crlf():
    """ Data where each line is terminated by \r\n """
    return csv_data_with_term('\r\n')


@pytest.fixture
def csv_data_with_lf():
    """ Data where each line is terminated by \n """
    return csv_data_with_term('\n')


@pytest.yield_fixture
def csv_file_with_wide_dataset(csv_data_with_wide_dataset):
    """Path to a very wide dataset"""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
        f.write(csv_data_with_wide_dataset.getvalue().encode('utf-8'))
    yield f.name
    os.remove(f.name)


@pytest.yield_fixture
def csv_file_with_cr(csv_data_with_cr):
    """Path to a file terminated with CR"""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
        f.write(csv_data_with_cr.getvalue().encode('utf-8'))
    yield f.name
    os.remove(f.name)


@pytest.yield_fixture
def csv_file_with_lf(csv_data_with_lf):
    """Path to a file terminated with LF"""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
        f.write(csv_data_with_lf.getvalue().encode('utf-8'))
    yield f.name
    os.remove(f.name)


@pytest.yield_fixture
def csv_file_with_crlf(csv_data_with_crlf):
    """Path to a file terminated with CRLF"""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
        f.write(csv_data_with_crlf.getvalue().encode('utf-8'))
    yield f.name
    os.remove(f.name)


@pytest.yield_fixture
def run_context_file():
    c_file = ContextFile('pid', 'lid', 10, None)
    open(c_file.file_name, 'a')
    yield c_file
    c_file.clean()
