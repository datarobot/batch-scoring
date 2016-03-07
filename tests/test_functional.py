from __future__ import print_function
import subprocess
import sys
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
                 ' --n_concurrent 1'
                 ' --no').format(webhost=live_server.url(),
                                 username='username',
                                 password='password',
                                 project_id=ObjectId(),
                                 model_id=ObjectId())

    assert 0 == subprocess.call(arguments.split(' '), stdout=sys.stdout,
                                stderr=subprocess.STDOUT)
