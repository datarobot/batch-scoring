import pytest
from datarobot_batch_scoring.batch_scoring import run_batch_predictions
from utils import PickableMock

from utils import read_logs


def test_regression(live_server, tmpdir, ui, keep_cols=None,
                    in_fixture='tests/fixtures/regression_predict.csv',
                    out_fixture='tests/fixtures/regression_output.csv',
                    fast_mode=False, skip_row_id=False, output_delimiter=None,
                    skip_dialect=False,
                    n_samples=500,
                    max_batch_size=None,
                    expected_ret=None):
    # train one model in project
    out = tmpdir.join('out.csv')

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
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=n_samples,
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
        skip_dialect=skip_dialect,
        skip_row_id=skip_row_id,
        output_delimiter=output_delimiter,
        max_batch_size=max_batch_size
    )
    assert ret is expected_ret

    if out_fixture:
        actual = out.read_text('utf-8')
        with open(out_fixture, 'rU') as f:
            expected = f.read()
            print(len(actual), len(expected))
            assert actual == expected


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
        import_id=None,
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
        import_id=None,
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
        import_id=None,
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=500,
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
        skip_dialect=False,
        compression=True
    )
    assert ret is None

    actual = out.read_text('utf-8')

    with open('tests/fixtures/regression_output_jp.csv', 'rU') as f:
        assert actual == f.read()


def test_fast_mode_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, True, False)


def test_wo_fast_mode_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, False, False)


def test_fast_mode_gzipped_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, True, True)


def test_wo_fast_mode_gzipped_regression_jp(live_server, tmpdir):
    check_regression_jp(live_server, tmpdir, False, True)


def test_regression_keep_cols(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_x.csv')


def test_regression_keep_cols_multi(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['y', 'x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_yx.csv')


def test_regression_keep_cols_fast(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_x.csv',
                    fast_mode=True)


def test_regression_keep_cols_multi_fast(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['y', 'x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_yx.csv',
                    fast_mode=True)


def test_regression_keep_cols_multi_fast_max_batch(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['y', 'x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_yx.csv',
                    fast_mode=True,
                    max_batch_size=100)

    logs = read_logs()
    assert "bytes, splitting" in logs


def test_regression_bad_csv(live_server, tmpdir, ui):

    test_regression(live_server, tmpdir, ui,
                    in_fixture='tests/fixtures/regression_bad.csv',
                    out_fixture=None,
                    fast_mode=False,
                    expected_ret=1)

    logs = read_logs()
    assert "Error parsing CSV file after line 1000, error: " in logs


def test_regression_bad2_csv(live_server, tmpdir, monkeypatch, ui):
    def sys_exit(code):
        raise RuntimeError

    monkeypatch.setattr("sys.exit", sys_exit)
    with pytest.raises(RuntimeError):
        test_regression(live_server, tmpdir, ui,
                        in_fixture='tests/fixtures/regression_bad2.csv',
                        out_fixture=None,
                        fast_mode=True,
                        expected_ret=1)


def test_regression_keep_cols_wo_row_id(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_x_rid.csv',
                    skip_row_id=True)


def test_regression_keep_cols_multi_wo_row_id(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['y', 'x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_yx_rid.csv',
                    skip_row_id=True)


def test_regression_keep_cols_fast_wo_row_id(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_x_rid.csv',
                    fast_mode=True,
                    skip_row_id=True)


def test_regression_keep_cols_multi_fast_wo_row_id(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui, keep_cols=['y', 'x'],
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_yx_rid.csv',
                    fast_mode=True,
                    skip_row_id=True)


def test_regression_fast_wo_row_id(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui,
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_rid.csv',
                    fast_mode=True,
                    skip_row_id=True)


def test_regression_wo_row_id(live_server, tmpdir, ui):
    test_regression(live_server, tmpdir, ui,
                    in_fixture='tests/fixtures/regression.csv',
                    out_fixture='tests/fixtures/regression_output_rid.csv',
                    fast_mode=False,
                    skip_row_id=True)


def test_regression_keep_cols_multi_output(live_server, tmpdir, ui):
    test_regression(
        live_server, tmpdir, ui, keep_cols=['y', 'x'],
        in_fixture='tests/fixtures/regression.csv',
        out_fixture='tests/fixtures/regression_output_yx_output.csv',
        output_delimiter='|')


def test_regression_keep_cols_multi_output_skip_dialect(live_server,
                                                        tmpdir, ui):
    test_regression(
        live_server, tmpdir, ui, keep_cols=['y', 'x'],
        skip_dialect=True,
        in_fixture='tests/fixtures/regression.csv',
        out_fixture='tests/fixtures/regression_output_yx_output.csv',
        output_delimiter='|')


def test_regression_keep_cols_multi_skip_dialect(live_server, tmpdir, ui):
    test_regression(
        live_server, tmpdir, ui, keep_cols=['y', 'x'],
        skip_dialect=True,
        in_fixture='tests/fixtures/regression.csv',
        out_fixture='tests/fixtures/regression_output_yx.csv')
