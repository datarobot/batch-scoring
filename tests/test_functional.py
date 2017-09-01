from __future__ import print_function
import os
import sys
import subprocess
import tempfile

import mock
import pytest

from datarobot_batch_scoring.batch_scoring import run_batch_predictions
from datarobot_batch_scoring.utils import UI
from utils import PickableMock, print_logs


def test_args_from_subprocess(live_server):
    # train one model in project
    with tempfile.NamedTemporaryFile(prefix='test_',
                                     suffix='.csv',
                                     delete=True) as fd:
        pass
    bscore_name = 'batch_scoring'
    if os.name is 'nt':
        exe = sys.executable
        head = os.path.split(exe)[0]
        bscore_name = os.path.normpath(os.path.join(head, 'scripts',
                                       'batch_scoring.exe'))
        assert os.path.isfile(bscore_name) is True
        assert os.path.supports_unicode_filenames is True
    arguments = ('{bscore_name} --host={webhost}/api'
                 ' --user={username}'
                 ' --password={password}'
                 ' --verbose'
                 ' --n_samples=10'
                 ' --n_concurrent=1'
                 ' --out={out}'
                 ' --no'
                 ' {project_id}'
                 ' {model_id}'
                 ' tests/fixtures/temperatura_predict.csv').format(
                    webhost=live_server.url(),
                    bscore_name=bscore_name,
                    username='username',
                    password='password',
                    project_id='56dd9570018e213242dfa93c',
                    model_id='56dd9570018e213242dfa93d',
                    out=fd.name)
    try:
        spc = subprocess.check_call(arguments.split(' '))
    except subprocess.CalledProcessError as e:
        print(e)
        print_logs()

    #  newlines will be '\r\n on windows and \n on linux. using 'rU' should
    #  resolve differences on different platforms
    with open(fd.name, 'rU') as o:
        actual = o.read()
    with open('tests/fixtures/temperatura_output.csv', 'rU') as f:
        expected = f.read()
    assert str(actual) == str(expected), str(actual)
    assert spc is 0


def test_simple(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    ret = run_batch_predictions(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        pid='56dd9570018e213242dfa93c',
        lid='56dd9570018e213242dfa93d',
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )

    assert ret is None
    actual = out.read_text('utf-8')
    with open('tests/fixtures/temperatura_output.csv', 'rU') as f:
        expected = f.read()
    assert str(actual) == str(expected), expected


def test_simple_api_v1(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui = PickableMock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    ret = run_batch_predictions(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        pid='56dd9570018e213242dfa93c',
        lid='56dd9570018e213242dfa93f',
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )

    assert ret is None
    actual = out.read_text('utf-8')
    with open('tests/fixtures/temperatura_api_v1_output.csv', 'rU') as f:
        expected = f.read()
    assert str(actual) == str(expected), expected


def test_simple_transferable(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    ret = run_batch_predictions(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        import_id='0ec5bcea7f0f45918fa88257bfe42c09',
        pid=None,
        lid=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_predict.csv',
        pred_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )

    assert ret is None
    actual = out.read_text('utf-8')
    with open('tests/fixtures/regression_output.csv', 'rU') as f:
        expected = f.read()
    assert str(actual) == str(expected), expected


def test_keep_cols(live_server, tmpdir, ui, fast_mode=False):
    # train one model in project
    out = tmpdir.join('out.csv')

    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    ret = run_batch_predictions(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        pid='56dd9570018e213242dfa93c',
        lid='56dd9570018e213242dfa93d',
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=['x'],
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv',
        pred_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=fast_mode,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )

    assert ret is None

    expected = out.read_text('utf-8')
    with open('tests/fixtures/temperatura_output_keep_x.csv', 'rU') as f:
        assert expected == f.read(), expected


def test_keep_cols_fast_mode(live_server, tmpdir, ui):
    test_keep_cols(live_server, tmpdir, ui, True)


def test_keep_wrong_cols(live_server, tmpdir, fast_mode=False):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui_class = mock.Mock(spec=UI)
    ui = ui_class.return_value
    ui.fatal.side_effect = SystemExit

    with pytest.raises(SystemExit):
        base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
        ret = run_batch_predictions(
            base_url=base_url,
            base_headers={},
            user='username',
            pwd='password',
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            import_id=None,
            n_retry=3,
            concurrent=1,
            resume=False,
            n_samples=10,
            out_file=str(out),
            keep_cols=['not_present', 'x'],
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=ui,
            auto_sample=False,
            fast_mode=fast_mode,
            dry_run=False,
            encoding='',
            skip_dialect=False
        )

        assert ret is None

    ui.fatal.assert_called()
    ui.fatal.assert_called_with(
        '''keep_cols "['not_present']" not in columns ['', 'x'].'''
    )


def test_pred_name_classification(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    ret = run_batch_predictions(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        pid='56dd9570018e213242dfa93c',
        lid='56dd9570018e213242dfa93d',
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv',
        pred_name='healthy',
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )

    assert ret is None

    expected = out.read_text('utf-8')
    with open('tests/fixtures/temperatura_output_healthy.csv', 'rU') as f:
        assert expected == f.read(), expected
