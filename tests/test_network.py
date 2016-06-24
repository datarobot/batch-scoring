import mock
import textwrap

import requests

from datarobot_batch_scoring.batch_scoring import run_batch_predictions


def test_request_client_timeout(live_server, tmpdir):
    out = tmpdir.join('out.csv')
    ui = mock.Mock()
    base_url = '{webhost}/api/v1/'.format(webhost=live_server.url())
    with mock.patch('datarobot_batch_scoring.'
                    'network.requests.Session') as nw_mock:
        nw_mock.return_value.send = mock.Mock(
            side_effect=requests.exceptions.ReadTimeout)

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
            timeout=30,
            ui=ui,
            auto_sample=False,
            fast_mode=False,
        )

    assert ret is None
    returned = out.read_text('utf-8')
    assert '' in returned, returned
    ui.warning.assert_called_with(textwrap.dedent("""The server did not send any data
in the allotted amount of time.
You might want to increase --timeout parameter
or
decrease --n_samples --n_concurrent parameters
"""))
