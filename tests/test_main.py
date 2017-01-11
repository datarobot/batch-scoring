import mock
import pytest
from datarobot_batch_scoring.main import main, UI


def test_without_passed_user_and_passwd(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--n_samples',
                 '10',
                 '--n_concurrent', '1', '--no']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/api/v1/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
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
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=30,
            ui=mock.ANY,
            auto_sample=False,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False
        )


def test_keep_cols(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--keep_cols', 'a, b, c']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/api/v1/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=False,
            n_samples=False,
            out_file='out.csv',
            keep_cols=['a', 'b', 'c'],
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=30,
            ui=mock.ANY,
            auto_sample=True,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False
        )


def test_input_dataset_doesnt_exist(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'file-not-exists.csv']

    ui_class = mock.Mock(spec=UI)
    ui = ui_class.return_value
    ui.fatal.side_effect = SystemExit
    monkeypatch.setattr('datarobot_batch_scoring.main.UI', ui_class)

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        with pytest.raises(SystemExit):
            main(argv=main_args)
        assert not mock_method.called
    ui.fatal.assert_called_with('file file-not-exists.csv does not exist.')


def test_bad_objectid(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93caa',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv']

    ui_class = mock.Mock(spec=UI)
    ui = ui_class.return_value
    ui.fatal.side_effect = SystemExit
    monkeypatch.setattr('datarobot_batch_scoring.main.UI', ui_class)
    monkeypatch.setattr('datarobot_batch_scoring.main.verify_objectid',
                        mock.Mock(side_effect=ValueError('bad objectid')))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        with pytest.raises(SystemExit):
            main(argv=main_args)
        assert not mock_method.called
    ui.fatal.assert_called_with('bad objectid')


def test_datarobot_key(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--datarobot_key', 'the_key']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/api/v1/',
            base_headers={'datarobot-key': 'the_key'},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=False,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=30,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False
        )


def test_encoding_options(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--delimiter=tab',
                 '--encoding=utf-8', '--skip_dialect']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/api/v1/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=False,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter='\t',
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=30,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='utf-8',
            skip_dialect=True,
            skip_row_id=False,
            output_delimiter=None,
            compression=False
        )


def test_invalid_delimiter(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '--delimiter', 'INVALID',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv']

    ui_class = mock.Mock(spec=UI)
    ui = ui_class.return_value
    ui.fatal.side_effect = SystemExit
    monkeypatch.setattr('datarobot_batch_scoring.main.UI', ui_class)

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        with pytest.raises(SystemExit):
            main(argv=main_args)
        assert not mock_method.called
    ui.fatal.assert_called_with(
        'Delimiter "INVALID" is not a valid delimiter.')


def test_no_required_params(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '--n_samples',
                 '10',
                 '--n_concurrent', '1', '--no']
    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        with pytest.raises(SystemExit):
            main(argv=main_args)
    assert not mock_method.called


def test_output_delimiter(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--output_delimiter=tab',
                 '--encoding=utf-8', '--skip_dialect']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/api/v1/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=False,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=30,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='utf-8',
            skip_dialect=True,
            skip_row_id=False,
            output_delimiter='\t',
            compression=False
        )


def test_skip_row_id(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--skip_row_id',
                 '--encoding=utf-8', '--skip_dialect']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/api/v1/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=False,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=30,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='utf-8',
            skip_dialect=True,
            skip_row_id=True,
            output_delimiter=None,
            compression=False
        )
