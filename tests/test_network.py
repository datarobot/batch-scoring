import textwrap

import pytest

from datarobot_batch_scoring.batch_scoring import run_batch_predictions

from utils import read_logs


def test_request_client_timeout(live_server, tmpdir, ui):
    live_server.app.config['PREDICTION_DELAY'] = 3
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
        timeout=1,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )

    assert ret is 1
    returned = out.read_text('utf-8')
    assert '' in returned, returned
    logs = read_logs()
    assert textwrap.dedent("""The server did not send any data
in the allotted amount of time.
You might want to decrease the "--n_concurrent" parameters
or
increase "--timeout" parameter.
""") in logs


def test_request_pool_is_full(live_server, tmpdir, ui):
    live_server.app.config["PREDICTION_DELAY"] = 1

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
        lid='56dd9570018e213242dfa93d',
        n_retry=3,
        concurrent=30,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/criteo_top30_1m.csv.gz',
        pred_name=None,
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )
    assert ret is None

    logs = read_logs()
    assert "Connection pool is full" not in logs


def test_request_retry(live_server, tmpdir, ui):
    live_server.app.config["FAIL_AT"] = [8]

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
        lid='56dd9570018e213242dfa93d',
        n_retry=3,
        concurrent=2,
        resume=False,
        n_samples=5,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
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
    assert len(actual.splitlines()) == 101

    logs = read_logs()
    assert "failed with status code: 500" in logs


def test_compression(live_server, tmpdir, ui):
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
        lid='56dd9570018e213242dfa93d',
        n_retry=3,
        concurrent=2,
        resume=False,
        n_samples=100,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_jp.csv.gz',
        pred_name=None,
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False,
        compression=True
    )
    assert ret is None

    actual = out.read_text('utf-8')
    assert len(actual.splitlines()) == 1411

    logs = read_logs()
    assert "space savings" in logs


@pytest.mark.xfail(reason="Results are written in response order")
def test_wrong_result_order(live_server, tmpdir, ui):
    out = tmpdir.join('out.csv')
    live_server.app.config["DELAY_AT"] = {
        8: 3.0,
        9: 2.0,
        10: 1.0
    }

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
        concurrent=4,
        resume=False,
        n_samples=100,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_jp.csv',
        pred_name='new_name',
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False,
        compression=True
    )
    assert ret is None

    actual = out.read_text('utf-8')

    with open('tests/fixtures/regression_output_jp.csv', 'rU') as f:
        assert actual == f.read()


@pytest.mark.xfail(reason="Last retries are lost")
def test_lost_retry(live_server, tmpdir, monkeypatch, ui):
    out = tmpdir.join('out.csv')
    live_server.app.config["PREDICTION_DELAY"] = 1.0
    live_server.app.config["FAIL_AT"] = [14]

    def sys_exit(code):
        raise RuntimeError

    monkeypatch.setattr("sys.exit", sys_exit)

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
        concurrent=4,
        resume=False,
        n_samples=100,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_jp.csv',
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

    actual = out.read_text('utf-8').splitlines()
    actual.sort()

    with open('tests/fixtures/regression_output_jp.csv', 'rU') as f:
        expected = f.read().decode('utf-8').splitlines()
        expected.sort()
        assert actual == expected
