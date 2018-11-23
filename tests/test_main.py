import mock
import pytest
from datarobot_batch_scoring.main import (
    main, UI, main_standalone, parse_args, main_deployment_aware
)


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
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
            import_id=None,
            n_retry=3,
            concurrent=1,
            resume=None,
            n_samples=10,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            auto_sample=False,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0,
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
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=None,
            n_samples=False,
            out_file='out.csv',
            keep_cols=['a', 'b', 'c'],
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            auto_sample=True,
            fast_mode=False,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0,
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
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={'datarobot-key': 'the_key'},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=None,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0
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
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=None,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter='\t',
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='utf-8',
            skip_dialect=True,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0,
        )


def test_unicode_decode_error_message_fast(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--encoding=ascii', '--fast', '--dry_run']

    ui_class = mock.Mock(spec=UI)
    ui = ui_class.return_value
    monkeypatch.setattr('datarobot_batch_scoring.main.UI', ui_class)

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        mock_method.side_effect = UnicodeDecodeError('test',
                                                     b'', 1, 1, 'test')
        assert main(argv=main_args) == 1

    ui.error.assert_has_calls([
        mock.call("'test' codec can't decode bytes in position 1-0: test"),
        mock.call("You are using --fast option, which uses a small sample "
                  "of data to figuring out the encoding of your file. You "
                  "can try to specify the encoding directly for this file "
                  "by using the encoding flag (e.g. --encoding utf-8). "
                  "You could also try to remove the --fast mode to auto-"
                  "detect the encoding with a larger sample size")
    ])


def test_unicode_decode_error_message_slow(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--dry_run']

    ui_class = mock.Mock(spec=UI)
    ui = ui_class.return_value
    monkeypatch.setattr('datarobot_batch_scoring.main.UI', ui_class)

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        mock_method.side_effect = UnicodeDecodeError('test',
                                                     b'', 1, 1, 'test')
        assert main(argv=main_args) == 1

    # Without fast flag, we don't show the verbose error message
    ui.error.assert_called_with(
        "'test' codec can't decode bytes in position 1-0: test"
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
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=None,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='utf-8',
            skip_dialect=True,
            skip_row_id=False,
            output_delimiter='\t',
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0
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
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=None,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='utf-8',
            skip_dialect=True,
            skip_row_id=True,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0
        )


def test_batch_scoring_deployment_aware_call(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--encoding=utf-8', '--skip_dialect']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main_deployment_aware(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid=None,
            lid=None,
            deployment_id='56dd9570018e213242dfa93d',
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=None,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='utf-8',
            skip_dialect=True,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0
        )


def test_datarobot_transferable_call(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/api',
                 '0ec5bcea7f0f45918fa88257bfe42c09',
                 'tests/fixtures/temperatura_predict.csv']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main_standalone(argv=main_args)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid=None,
            lid=None,
            import_id='0ec5bcea7f0f45918fa88257bfe42c09',
            n_retry=3,
            concurrent=4,
            resume=None,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0
        )


def test_resume(monkeypatch):
    main_args_yes = ['--host',
                     'http://localhost:53646/api',
                     '56dd9570018e213242dfa93c',
                     '56dd9570018e213242dfa93d',
                     'tests/fixtures/temperatura_predict.csv',
                     '--resume']

    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))

    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args_yes)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
            import_id=None,
            n_retry=3,
            concurrent=4,
            resume=True,
            n_samples=False,
            out_file='out.csv',
            keep_cols=None,
            delimiter=None,
            dataset='tests/fixtures/temperatura_predict.csv',
            pred_name=None,
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0
        )


def test_resume_no(monkeypatch):
    main_args_no = ['--host',
                    'http://localhost:53646/api',
                    '56dd9570018e213242dfa93c',
                    '56dd9570018e213242dfa93d',
                    'tests/fixtures/temperatura_predict.csv',
                    '--no-resume']
    monkeypatch.setattr('datarobot_batch_scoring.main.UI', mock.Mock(spec=UI))
    with mock.patch(
            'datarobot_batch_scoring.main'
            '.run_batch_predictions') as mock_method:
        main(argv=main_args_no)
        mock_method.assert_called_once_with(
            base_url='http://localhost:53646/predApi/v1.0/',
            base_headers={},
            user=mock.ANY,
            pwd=mock.ANY,
            api_token=None,
            create_api_token=False,
            pid='56dd9570018e213242dfa93c',
            lid='56dd9570018e213242dfa93d',
            deployment_id=None,
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
            timeout=None,
            ui=mock.ANY,
            fast_mode=False,
            auto_sample=True,
            dry_run=False,
            encoding='',
            skip_dialect=False,
            skip_row_id=False,
            output_delimiter=None,
            compression=False,
            field_size_limit=None,
            verify_ssl=True,
            max_prediction_explanations=0
        )


@pytest.mark.parametrize('ssl_argvs, verify_ssl_value', [
    ('', True),
    ('--no_verify_ssl', False),
    ('--ca_bundle /path/to/cert', '/path/to/cert'),
    ('--ca_bundle /path/to/cert --no_verify_ssl', False)
])
def test_verify_ssl_parameter(ssl_argvs, verify_ssl_value):
    argvs = (
        '--host http://localhost:53646/api '
        '56dd9570018e213242dfa93c 56dd9570018e213242dfa93d '
        'tests/fixtures/temperatura_predict.csv ' + ssl_argvs
    ).strip().split(' ')
    parsed_args = parse_args(argvs)
    assert parsed_args['verify_ssl'] == verify_ssl_value


def test_reason_codes_not_compatible_wit_old_api(monkeypatch):
    main_args = ['--host',
                 'http://localhost:53646/',
                 '56dd9570018e213242dfa93c',
                 '56dd9570018e213242dfa93d',
                 'tests/fixtures/temperatura_predict.csv',
                 '--api_version', 'api/v1',
                 '--max_prediction_explanations', '3']

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
        'Prediction explanation is not available for '
        'api_version `api/v1` please use the '
        '`predApi/v1.0` or deployments endpoint')
