import csv
import tempfile

import pytest
from datarobot_batch_scoring.batch_scoring import run_batch_predictions
from utils import PickableMock

from datarobot_batch_scoring.reader import DETECT_SAMPLE_SIZE_SLOW


def test_gzipped_csv(live_server, ui):
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
        out_file='out.csv',
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
        skip_dialect=False,
        max_batch_size=1000
    )

    assert ret is None


def test_explicit_delimiter(live_server):
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=',',
        dataset='tests/fixtures/temperatura_predict.csv',
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


def test_explicit_delimiter_gzip(live_server):
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=',',
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


def test_tab_delimiter(live_server):
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
        out_file='out.csv',
        keep_cols=None,
        delimiter='\t',
        dataset='tests/fixtures/temperatura_predict_tab.csv',
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


def test_empty_file(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
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
            import_id=None,
            n_retry=3,
            concurrent=1,
            resume=False,
            n_samples=10,
            out_file='out.csv',
            keep_cols=None,
            delimiter=',',
            dataset='tests/fixtures/empty.csv',
            pred_name=None,
            timeout=None,
            ui=ui,
            auto_sample=False,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False
        )
    assert "The csv module failed to detect the CSV dialect." in str(ctx.value)


def test_no_delimiter(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
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
            import_id=None,
            n_retry=3,
            concurrent=1,
            resume=False,
            n_samples=10,
            out_file='out.csv',
            keep_cols=None,
            delimiter=';',
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=ui,
            auto_sample=False,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False
        )
    assert str(ctx.value) == ("Could not determine delimiter")


def test_bad_newline(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())

    run_batch_predictions(
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
        out_file='out.csv',
        keep_cols=None,
        delimiter=',',
        dataset='tests/fixtures/diabetes_bad_newline.csv',
        pred_name=None,
        timeout=None,
        ui=ui,
        auto_sample=False,
        fast_mode=False,
        dry_run=False,
        encoding='',
        skip_dialect=False
    )

    lines = len(open("out.csv", "rb").readlines())

    assert lines == 9


def test_header_only(live_server):
    ui = PickableMock()
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
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
            import_id=None,
            n_retry=3,
            concurrent=1,
            resume=False,
            n_samples=10,
            out_file='out.csv',
            keep_cols=None,
            delimiter=',',
            dataset='tests/fixtures/header_only.csv',
            pred_name=None,
            timeout=None,
            ui=ui,
            auto_sample=False,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False
        )
    assert str(ctx.value) == ("Input file 'tests/fixtures/header_only.csv' "
                              "is empty.")


def test_quotechar_in_keep_cols(live_server):
    base_url = '{webhost}/predApi/v1.0/'.format(webhost=live_server.url())
    ui = PickableMock()
    with tempfile.NamedTemporaryFile(prefix='test_',
                                     suffix='.csv',
                                     delete=False) as fd:
        head = open("tests/fixtures/quotes_input_head.csv",
                    "rb").read()
        body_1 = open("tests/fixtures/quotes_input_first_part.csv",
                      "rb").read()
        body_2 = open("tests/fixtures/quotes_input_bad_part.csv",
                      "rb").read()
        fd.file.write(head)
        size = 0
        while size < DETECT_SAMPLE_SIZE_SLOW:
            fd.file.write(body_1)
            size += len(body_1)
        fd.file.write(body_2)
        fd.close()

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
            out_file='out.csv',
            keep_cols=["b", "c"],
            delimiter=None,
            dataset=fd.name,
            pred_name=None,
            timeout=None,
            ui=ui,
            auto_sample=True,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False
        )
        assert ret is None

        last_line = open("out.csv", "rb").readlines()[-1]
        expected_last_line = b'1044,2,"eeeeeeee ""eeeeee"" eeeeeeeeeeee'
        assert last_line[:len(expected_last_line)] == expected_last_line
