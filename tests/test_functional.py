from __future__ import print_function
import mock
import subprocess
import sys
from bson import ObjectId

from datarobot_batch_scoring.batch_scoring import run_batch_predictions_v1


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


def test_simple(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    ret = run_batch_predictions_v1(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        pid='56dd9570018e213242dfa93c',
        lid='56dd9570018e213242dfa93d',
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
        timeout=30,
        ui=ui,
    )

    assert ret is None

    expected = out.read_text('utf-8')
    with open('tests/fixtures/temperatura_output.csv', 'r') as f:
        assert expected == f.read()


def test_keep_cols(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    ret = run_batch_predictions_v1(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        pid='56dd9570018e213242dfa93c',
        lid='56dd9570018e213242dfa93d',
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=['x'],
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv',
        pred_name=None,
        timeout=30,
        ui=ui
    )

    assert ret is None

    expected = out.read_text('utf-8')
    with open('tests/fixtures/temperatura_output_keep_x.csv', 'r') as f:
        assert expected == f.read()
