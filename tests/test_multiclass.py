import pytest

from datarobot_batch_scoring.batch_scoring import run_batch_predictions


def test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=None,
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture='tests/fixtures/iris_out.csv',
                            fast_mode=False, output_delimiter=None,
                            skip_row_id=False, skip_dialect=False,
                            n_samples=500,
                            max_batch_size=None,
                            expected_ret=None):
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
        lid='5a29097f962d7465d1a81946',
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
        timeout=None,
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
            assert actual == expected


def test_multiclass_import_id(live_server, tmpdir, ui):
    out = tmpdir.join('out.csv')

    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())

    ret = run_batch_predictions(
        base_url=base_url,
        base_headers={},
        user='username',
        pwd='password',
        api_token=None,
        create_api_token=False,
        pid=None,
        lid=None,
        import_id='098fa761405d1c9d8a5ea71dc0f3d2bb5ce898b5',
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=500,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/iris_predict.csv',
        pred_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False,
        skip_row_id=False,
        output_delimiter=None,
        max_batch_size=None
    )

    assert not ret

    actual = out.read_text('utf-8')
    with open('tests/fixtures/iris_out.csv', 'rU') as f:
        expected = f.read()
        assert actual == expected


def test_multiclass_keep_cols(live_server, tmpdir, ui):
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1'],
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture='tests/fixtures/iris_out_x1.csv')


def test_multiclass_keep_cols_multi(live_server, tmpdir, ui):
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1', 'X2'],
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture='tests/fixtures/iris_out_x1x2.csv')


def test_multiclass_keep_cols_fast(live_server, tmpdir, ui):
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1'],
                            fast_mode=True,
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture='tests/fixtures/iris_out_x1.csv')


def test_multiclass_keep_cols_multi_fast(live_server, tmpdir, ui):
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1', 'X2'],
                            fast_mode=True,
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture='tests/fixtures/iris_out_x1x2.csv')


def test_multiclass_keep_cols_wo_row_id(live_server, tmpdir, ui):
    out_fixture = 'tests/fixtures/iris_out_x1_no_rid.csv'
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1'],
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture=out_fixture,
                            skip_row_id=True)


def test_multiclass_keep_cols_multi_wo_row_id(live_server, tmpdir, ui):
    out_fixture = 'tests/fixtures/iris_out_x1x2_no_rid.csv'
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1', 'X2'],
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture=out_fixture,
                            skip_row_id=True)


def test_multiclass_keep_cols_fast_wo_row_id(live_server, tmpdir, ui):
    out_fixture = 'tests/fixtures/iris_out_x1_no_rid.csv'
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1'],
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture=out_fixture,
                            skip_row_id=True, fast_mode=True)


def test_multiclass_keep_cols_multi_fast_wo_row_id(live_server, tmpdir, ui):
    out_fixture = 'tests/fixtures/iris_out_x1x2_no_rid.csv'
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1', 'X2'],
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture=out_fixture,
                            skip_row_id=True, fast_mode=True)


def test_multiclass_with_custom_delimiter(live_server, tmpdir, ui):
    out_fixture = 'tests/fixtures/iris_out_delimiter.csv'
    test_multiclass_pid_lid(live_server, tmpdir, ui,
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture=out_fixture,
                            output_delimiter='|')


def test_multiclass_keep_cols_multi_custom_delimiter(live_server, tmpdir, ui):
    out_fixture = 'tests/fixtures/iris_out_x1x2_delimiter.csv'
    test_multiclass_pid_lid(live_server, tmpdir, ui, keep_cols=['X1', 'X2'],
                            in_fixture='tests/fixtures/iris_predict.csv',
                            out_fixture=out_fixture,
                            output_delimiter='|')


def test_multiclass_bad_csv(live_server, tmpdir, monkeypatch, ui):
    def sys_exit(code):
        raise RuntimeError

    monkeypatch.setattr("sys.exit", sys_exit)

    in_fixture = 'tests/fixtures/iris_predict_bad.csv'
    with pytest.raises(RuntimeError):
        test_multiclass_pid_lid(live_server, tmpdir, ui,
                                in_fixture=in_fixture,
                                out_fixture=None,
                                fast_mode=True,
                                expected_ret=1)
