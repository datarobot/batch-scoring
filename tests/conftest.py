import uuid
import pytest
import pymongo

import datarobot_sdk as dr

pytest_plugins = ['liveserver_fixtures']


@pytest.fixture(scope='session')
def session_id():
    '''Unique session identifier, random string.'''
    return str(uuid.uuid4())


def pytest_addoption(parser):
    parser.addoption('--web-host', action='store', type=str,
                     default='127.0.0.1', dest='webhost',
                     help=("Webserver host:port for testing. "
                           "Used for creation project, managing autopilot. "
                           "Using Prediction Resource Management on app side"))
    parser.addoption('--job-router-management-api-endpoint', action='store', type=str,
                     default='127.0.0.1:8008', dest='jrmngapi',
                     help=("Job Router Management api host:port for testing"
                           "Used for checking how well main app integrated"
                           " with job router management api"))
    parser.addoption('--predapi', action='store', type=str,
                     default='127.0.0.1', dest='predapi',
                     help=("Dedicated prediction api endpoint"
                           "Used for performing requests to dedicicated prediction"
                           " api cluster"))
    parser.addoption('--mongo', action='store', type=str,
                     default='127.0.0.1:27017', dest='mongo_opt',
                     help=("Mongo endpoint for creation initial user. "
                           "Supports replicaset if it has ',' in str"))


@pytest.yield_fixture(scope='function')
def mongo_client(request):
    mongo_host = request.config.getoption("mongo_opt")
    if ',' in mongo_host:
        mongo_client = pymongo.MongoReplicaSetClient(
            mongo_host,
            replicaset='rs0',
            read_preference=pymongo.ReadPreference.PRIMARY_PREFERRED)
    else:
        # assuming that mongo_host is X.X.X.X:YYYY format
        host, port = mongo_host.split(':')
        mongo_client = pymongo.MongoClient(host=host, port=int(port))
    yield mongo_client


@pytest.yield_fixture(scope='function')
def create_user(mongo_client):
    password = 'testing123'
    non_local_username = {}

    def create(username, token, admin=False):
        password_hash = '$pbkdf2-sha512$7725$uhciZMxZK6XUuhcCYOz9Hw$fZgflFNhTAC7GhaadoVoP2JwMdY' \
                        'XqwaHv4Vn7Hnwwzl6K2meZv52RNXnFTmnrlLJ2/QSId5UL3ZovTblPx/qqg'
        non_local_username['username'] = username
        insert_dict = {
            '$set': {
                'account_permissions.API_ACCESS': True,
                'account_permissions.IS_OFFLINE_QUEUE_ENABLED': True,
                'account_permissions.ENABLE_FULL_DATASET_PARTITIONING_AND_SAMPLING': True,
                'activated': 1,
                'invite_code': 'code',
                'api_token': token,
                'password': password_hash
            }
        }
        if admin:
            insert_dict['$set'].update({'account_permissions.PREDICTIONS_ADMIN': True})
        mongo_client.MMApp.users.update({'username': username}, insert_dict, upsert=True)
        return username, password, token
    yield create
    # clean username
    mongo_client.MMApp.users.remove({'username': non_local_username['username']})


@pytest.fixture(scope='function')
def user(session_id, create_user):
    username = 'batch_user_{}@datarobot.com'.format(session_id)
    return create_user(username, session_id)


@pytest.yield_fixture(scope='function')
def temp_project(v2_client):
    project = dr.Project.start('tests/fixtures/temperatura.csv', project_name='Test project',
                               autopilot_on=False, target='y')
    yield project
    project.delete()


@pytest.fixture(scope='function')
def v2_client(user, request):
    """The v2 client is a singleton, but this is a pretty decent way to make
    sure that it gets configured before a test that needs it
    """
    webhost = request.config.getoption("webhost")
    user, _, token = user
    client = dr.Client(token=token,
                       endpoint='http://{}/api/v2'.format(webhost))
    return client


@pytest.fixture(scope='function')
def v2_client_admin(prmadmin_user, request):
    """The v2 client is a singleton, but this is a pretty decent way to make
    sure that it gets configured before a test that needs it
    """
    webhost = request.config.getoption("webhost")
    _, _, token = prmadmin_user
    client = dr.Client(token=token,
                       endpoint='http://{}/api/v2'.format(webhost))
    return client


@pytest.yield_fixture(scope='function')
def temp_project_admin(v2_client_admin):
    project = dr.Project.start('fixtures/regression.csv', project_name='Test project',
                               autopilot_on=False, target='z')
    yield project
    project.delete()
