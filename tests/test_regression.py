import mock

from datarobot_batch_scoring.batch_scoring import run_batch_predictions


def test_regression(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

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
        lid='56dd9570018e213242dfa93e',
        n_retry=3,
        concurrent=1,
        resume=False,
        n_samples=10,
        out_file=str(out),
        keep_cols=None,
        delimiter=None,
        dataset='tests/fixtures/regression_predict.csv',
        pred_name=None,
        timeout=30,
        ui=ui
    )

    assert ret is None

    actual = out.read_text('utf-8')
    with open('tests/fixtures/regression_output.csv', 'r') as f:
        assert actual == f.read()


def test_regression_rename(live_server, tmpdir):
    # train one model in project
    out = tmpdir.join('out.csv')

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
        ui=ui
    )

    assert ret is None

    actual = out.read_text('utf-8')
    with open('tests/fixtures/regression_output_rename.csv', 'r') as f:
        assert actual == f.read()
