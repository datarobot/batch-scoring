from __future__ import print_function
import subprocess
import sys
from bson import ObjectId


def test_args_from_subprocess(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')
    arguments = ('batch_scoring --host {webhost}/api'
                 ' --user {username}'
                 ' --password {password}'
                 ' {project_id}'
                 ' {model_id}'
                 ' tests/fixtures/temperatura_predict.csv'
                 ' --n_samples 10'
                 ' --n_concurrent 1'
                 ' --out {out}'
                 ' --no').format(webhost=live_server.url(),
                                 username='username',
                                 password='password',
                                 project_id=ObjectId(),
                                 model_id=ObjectId(),
                                 out=str(out))

    assert 0 == subprocess.call(arguments.split(' '), stdout=sys.stdout,
                                stderr=subprocess.STDOUT)
    expected = out.read_text('utf-8')
    with open('tests/fixtures/temperatura_output.csv', 'r') as f:
        assert expected == f.read()
