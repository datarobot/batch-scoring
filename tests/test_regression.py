import pytest
import six
import os
from datarobot_batch_scoring.batch_scoring import run_batch_predictions
from datarobot_batch_scoring.utils import UI
from utils import PickableMock


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression(live_server, tmpdir, keep_cols=None,
                    in_fixture='tests/fixtures/regression_predict.csv',
                    out_fixture='tests/fixtures/regression_output.csv',
                    fast_mode=False):
    # train one model in project
    out = tmpdir.join('out.csv')

    with UI(False, 'DEBUG', False) as ui:
        base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
        ret = run_batch_predictions(
            base_url=base_url,
            base_headers={},
            user='username',
            pwd='password',
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93e',
            n_retry=3,
            concurrent=1,
            resume=False,
            n_samples=10,
            out_file=str(out),
            keep_cols=keep_cols,
            delimiter=None,
            dataset=in_fixture,
            pred_name=None,
            timeout=30,
            ui=ui,
            auto_sample=False,
            fast_mode=fast_mode,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            max_batch_size=100
        )
        assert ret is None
        actual = out.read_text('utf-8')
        with open(out_fixture, 'rU') as f:
            assert actual == f.read()


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression_rename(live_server, tmpdir):
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
        lid='56dd9570018e213242dfa93e',
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_predict.csv',
        pred_name='new_name',
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )
    assert ret is None

    actual = out.read_text('utf-8')
    with open('tests/fixtures/regression_output_rename.csv', 'rU') as f:
        assert actual == f.read()


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression_rename_fast(live_server, tmpdir):
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
        lid='56dd9570018e213242dfa93e',
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_predict.csv',
        pred_name='new_name',
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=True,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )
    assert ret is None

    actual = out.read_text('utf-8')
    with open('tests/fixtures/regression_output_rename.csv', 'rU') as f:
        assert actual == f.read()


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def check_regression_jp(live_server, tmpdir, fast_mode, gzipped):
    """Use utf8 encoded input data.

    """
    if fast_mode:
        out_fname = 'out_fast.csv'
    else:
        out_fname = 'out.csv'
    out = tmpdir.join(out_fname)

    dataset_suffix = '.gz' if gzipped else ''

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
        lid='56dd9570018e213242dfa93e',
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_jp.csv' + dataset_suffix,
        pred_name='new_name',
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=fast_mode,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )
    assert ret is None

    actual = out.read_text('utf-8')

    with open('tests/fixtures/regression_output_jp.csv', 'rU') as f:
        assert actual == f.read()


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_fast_mode_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, True, False)


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_wo_fast_mode_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, False, False)


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_fast_mode_gzipped_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, True, True)


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_wo_fast_mode_gzipped_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, False, True)


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression_keep_cols(live_server, tmpdir):
    test_regression(live_server, tmpdir, keep_cols=['x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_x.csv')


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression_keep_cols_multi(live_server, tmpdir):
    test_regression(live_server, tmpdir, keep_cols=['y', 'x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_yx.csv')


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression_keep_cols_fast(live_server, tmpdir):
    test_regression(live_server, tmpdir, keep_cols=['x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_x.csv',
                    fast_mode=True)


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression_keep_cols_multi_fast(live_server, tmpdir):
    test_regression(live_server, tmpdir, keep_cols=['y', 'x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_yx.csv',
                    fast_mode=True)


@pytest.mark.skipif(six.PY3 and os.name is 'nt',
                    reason="py3 on windows appveyor fails unexpectedly. Cannot"
                           " reproduce on actual Windows machine.")
def test_regression_bad_csv(live_server, tmpdir):
    test_regression(live_server, tmpdir,
                    in_fixture='tests/fixtures/regression_bad.csv',
                    out_fixture='tests/fixtures/regression_output_bad.csv',
                    fast_mode=False)
