import csv
import mock
import pytest
from datarobot_batch_scoring.batch_scoring import run_batch_predictions


def test_gzipped_csv(live_server):
    ui = mock.Mock()
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False
    )

    assert ret is None


def test_explicit_delimiter(live_server):
    ui = mock.Mock()
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=',',
        dataset='tests/fixtures/temperatura_predict.csv',
        pred_name=None,
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False
    )

    assert ret is None


def test_explicit_delimiter_gzip(live_server):
    ui = mock.Mock()
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=',',
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False
    )

    assert ret is None


def test_tab_delimiter(live_server):
    ui = mock.Mock()
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
        out_file='out.csv',
        keep_cols=None,
        delimiter='\t',
        dataset='tests/fixtures/temperatura_predict_tab.csv',
        pred_name=None,
        timeout=30,
        ui=ui,
        auto_sample=False,
        fast_mode=False
    )

    assert ret is None


def test_empty_file(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with pytest.raises(csv.Error) as ctx:
        run_batch_predictions(
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
            out_file='out.csv',
            keep_cols=None,
            delimiter=',',
            dataset='tests/fixtures/empty.csv',
            pred_name=None,
            timeout=30,
            ui=ui,
            auto_sample=False,
            fast_mode=False
        )
    assert str(ctx.value) == ("Could not determine delimiter")


def test_no_delimiter(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with pytest.raises(csv.Error) as ctx:
        run_batch_predictions(
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
            out_file='out.csv',
            keep_cols=None,
            delimiter=';',
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=30,
            ui=ui,
            auto_sample=False,
            fast_mode=False
        )
    assert str(ctx.value) == ("Could not determine delimiter")


def test_header_only(live_server):
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with pytest.raises(ValueError) as ctx:
        run_batch_predictions(
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
            out_file='out.csv',
            keep_cols=None,
            delimiter=',',
            dataset='tests/fixtures/header_only.csv',
            pred_name=None,
            timeout=30,
            ui=ui,
            auto_sample=False,
            fast_mode=False
        )
    assert str(ctx.value) == ("Input file 'tests/fixtures/header_only.csv' "
                              "is empty.")
