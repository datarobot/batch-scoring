import mock
import pytest
import sys
from datarobot_batch_scoring.batch_scoring import run_batch_predictions_v1


@pytest.mark.xfail(sys.version_info < (3, 0),
                   reason="Python 2 doesn't support both encoding and "
                   "compression, gzipped files are disabled")
def test_gzipped_csv(live_server):
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
        timeout=30,
        ui=ui
    )

    assert ret is None


def test_explicit_delimiter(live_server):
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=',',
        dataset='tests/fixtures/temperatura_predict.csv',
        pred_name=None,
        timeout=30,
        ui=ui
    )

    assert ret is None


def test_explicit_delimiter_gzip(live_server):
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=',',
        dataset='tests/fixtures/temperatura_predict.csv.gz',
        pred_name=None,
        timeout=30,
        ui=ui
    )

    assert ret is None


def test_tab_delimiter(live_server):
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
        out_file='out.csv',
        keep_cols=None,
        delimiter='\\t',
        dataset='tests/fixtures/temperatura_predict_tab.csv',
        pred_name=None,
        timeout=30,
        ui=ui
    )

    assert ret is None
