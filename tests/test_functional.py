import subprocess
import requests
from bson import ObjectId


def test_args_from_subprocess(live_server):
    # train one model in project
    arguments = ('batch_scoring --host {webhost}/api'
                 ' --user {username}'
                 ' --password {password}'
                 ' {project_id}'
                 ' {model_id}'
                 ' tests/fixtures/temperatura_predict.csv'
                 ' --n_samples 10'
                 ' --n_concurrent 1').format(webhost=live_server.url(),
                                             username='username',
                                             password='passowrd',
                                             project_id=ObjectId(),
                                             model_id=ObjectId())

    assert 0 == subprocess.call(arguments.split(' '))


def test_live_server(live_server):
    resp = requests.get(live_server.url() + '/ping').json()
    assert resp['ping'] == 'pong'
    resp = requests.get(live_server.url() + '/api/v1/api_token').json()
    assert resp['api_token'] == 'Som3tok3n'
    resp = requests.post(live_server.url() + '/api/v1/pid/lid/predict').json()
    # assert resp['api_token'] == 'Som3tok3n'
