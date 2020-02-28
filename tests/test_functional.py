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


@pytest.fixture(params=['pid/lid', 'deployment_id'])
def cli_args(request):
    pid = '56dd9570018e213242dfa93c'
    lid = '56dd9570018e213242dfa93d'
    deployment_id = '56dd9570018e213242dfa93d'

    if request.param == 'deployment_id':
        return 'batch_scoring_deployment_aware', deployment_id
    else:
        return 'batch_scoring', pid + ' ' + lid


def test_args_from_subprocess(live_server, cli_args):
    # train one model in project
    with tempfile.NamedTemporaryFile(prefix='test_',
                                     suffix='.csv',
                                     delete=True) as fd:
        pass
    bscore_name, params = cli_args
    if os.name is 'nt':
        exe = sys.executable
        head = os.path.split(exe)[0]
        bscore_name = os.path.normpath(os.path.join(head, 'scripts',
                                       bscore_name + '.exe'))
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
                 ' {params}'
                 ' tests/fixtures/temperatura_predict.csv').format(
                    webhost=live_server.url(),
                    bscore_name=bscore_name,
                    username='username',
                    password='password',
                    out=fd.name, params=params)
    spc = None
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


@pytest.fixture(params=['pid/lid', 'deployment_id'])
def func_params(request):
    pid = '56dd9570018e213242dfa93c'
    lid = '56dd9570018e213242dfa93d'
    deployment_id = '56dd9570018e213242dfa93d'

    if request.param == 'deployment_id':
        return {'deployment_id': deployment_id, 'pid': None, 'lid': None}
    return {'pid': pid, 'lid': lid, 'deployment_id': None}


def test_simple(live_server, tmpdir, func_params):
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
        deployment_id=func_params['deployment_id'],
        pid=func_params['pid'],
        lid=func_params['lid'],
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
        pred_threshold_name=None,
        pred_decision_name=None,
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


@pytest.mark.parametrize(
    'dataset_name', ['jpReview_books_reg.csv', 'jpReview_books_reg.csv.gz'])
def test_simple_with_unicode(live_server, tmpdir, func_params, dataset_name):
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
        deployment_id=func_params['deployment_id'],
        pid=func_params['pid'],
        lid=func_params['lid'],
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/{}'.format(dataset_name),
        pred_name=None,
        pred_threshold_name=None,
        pred_decision_name=None,
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
    with open('tests/fixtures/jpReview_books_reg_out.csv', 'rU') as f:
        expected = f.read()
    assert str(actual) == str(expected), expected


def test_simple_with_wrong_encoding(live_server, tmpdir, func_params):
    out = tmpdir.join('out.csv')
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    with pytest.raises(UnicodeDecodeError) as execinfo:
        run_batch_predictions(
            base_url=base_url,
            base_headers={},
            user='username',
            pwd='password',
            api_token=None,
            create_api_token=False,
            deployment_id=func_params['deployment_id'],
            pid=func_params['pid'],
            lid=func_params['lid'],
            import_id=None,
            n_retry=3,
            concurrent=1,
            resume=False,
            n_samples=10,
            out_file=str(out),
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/jpReview_books_reg.csv',
            pred_name=None,
            pred_threshold_name=None,
            pred_decision_name=None,
            timeout=None,
            ui=ui,
            auto_sample=False,
            fast_mode=False,
            dry_run=False,
            encoding='cp932',
            skip_dialect=False
        )

    # Fixture dataset encoding 'utf-8' and we trying to decode it with 'cp932'
    assert "'cp932' codec can't decode byte" in str(execinfo.value)


def test_prediction_explanations(live_server, tmpdir):
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
        pid='5afb150782c7dd45fcc03951',
        lid='5b2cad28aa1d12847310acf4',
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/10kDiabetes.csv',
        pred_name=None,
        pred_threshold_name=None,
        pred_decision_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False,
        max_prediction_explanations=5
    )

    assert ret is None
    actual = out.read_text('utf-8')
    with open('tests/fixtures/10kDiabetes_5explanations.csv', 'rU') as f:
        expected = f.read()
    assert str(actual) == str(expected), expected


def test_prediction_explanations_not_all_explanations(live_server, tmpdir):
    """Test scenario when some prediction explanations are missing"""
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
        pid='5afb150782c7dd45fcc03951',
        lid='5b2cad28aa1d12847310acf5',
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/10kDiabetes.csv',
        pred_name=None,
        pred_threshold_name=None,
        pred_decision_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False,
        max_prediction_explanations=5
    )

    assert ret is None
    actual = out.read_text('utf-8')
    with open('tests/fixtures/10kDiabetes_mixed_explanations.csv', 'rU') as f:
        expected = f.read()
    assert str(actual) == str(expected), actual


def test_prediction_explanations_keepcols(live_server, tmpdir):
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
        pid='5afb150782c7dd45fcc03951',
        lid='5b2cad28aa1d12847310acf4',
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=['medical_specialty', 'number_diagnoses'],
        delimiter=None,
        dataset='tests/fixtures/10kDiabetes.csv',
        pred_name=None,
        pred_threshold_name=None,
        pred_decision_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False,
        max_prediction_explanations=5
    )

    assert ret is None
    actual = out.read_text('utf-8')
    file_path = 'tests/fixtures/10kDiabetes_5explanations_keepcols.csv'
    with open(file_path, 'rU') as f:
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
        pred_threshold_name=None,
        pred_decision_name=None,
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
        pred_threshold_name=None,
        pred_decision_name=None,
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


def test_keep_cols(live_server, tmpdir, ui, func_params, fast_mode=False):
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
        deployment_id=func_params['deployment_id'],
        pid=func_params['pid'],
        lid=func_params['lid'],
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
        pred_threshold_name=None,
        pred_decision_name=None,
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


def test_keep_cols_fast_mode(live_server, tmpdir, ui, func_params):
    test_keep_cols(live_server, tmpdir, ui, func_params, True)


def test_keep_wrong_cols(live_server, tmpdir, func_params, fast_mode=False):
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
            deployment_id=func_params['deployment_id'],
            pid=func_params['pid'],
            lid=func_params['lid'],
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
            pred_threshold_name=None,
            pred_decision_name=None,
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


def test_pred_name_classification(live_server, tmpdir, func_params):
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
        deployment_id=func_params['deployment_id'],
        pid=func_params['pid'],
        lid=func_params['lid'],
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
        pred_threshold_name=None,
        pred_decision_name=None,
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


def test_pred_threshold_classification(live_server, tmpdir, func_params):
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
        deployment_id=func_params['deployment_id'],
        pid=func_params['pid'],
        lid=func_params['lid'],
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
        pred_threshold_name='threshold',
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
    with open(
        'tests/fixtures/temperatura_output_healthy_threshold.csv', 'rU'
    ) as f:
        assert expected == f.read(), expected


def test_pred_decision_name_classification(live_server, tmpdir, func_params):
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
        deployment_id=func_params['deployment_id'],
        pid=func_params['pid'],
        lid=func_params['lid'],
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv',
        pred_name=None,
        pred_decision_name='label',
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
    with open('tests/fixtures/temperatura_output_decision.csv', 'rU') as f:
        assert expected == f.read(), expected


def test_422(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

    ui_class = mock.Mock(spec=UI)
    ui = ui_class.return_value
    ui.fatal.side_effect = SystemExit
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    with pytest.raises(SystemExit):
        run_batch_predictions(
            base_url=base_url,
            base_headers={},
            user='username',
            pwd='password',
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242eee422',
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
    ui.fatal.assert_called()
    ui.fatal.assert_called_with(
        '''Predictions are not available because: "Server raised 422.".'''
    )
