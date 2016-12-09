import uuid
import pytest

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
