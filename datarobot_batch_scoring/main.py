import argparse
import copy
import logging
import os
import sys
import warnings

from . import __version__
from .batch_scoring import (run_batch_predictions_v1, run_batch_predictions_v2,
                            ShelveError)
from .utils import (UI,
                    get_config_file,
                    parse_config_file,
                    verify_objectid)

VERSION_TEMPLATE = '%(prog)s {}'.format(__version__)


def main(argv=sys.argv[1:]):
    global ui  # global variable hack, will get rid of a bit later
    warnings.simplefilter('ignore')
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument('--host', type=str,
                        help='Specifies the hostname of the prediction '
                        'API endpoint (the location of the data from where '
                        'to get predictions)')
    parser.add_argument('--user', type=str,
                        help='Specifies the username used to acquire '
                        'the api-token. '
                        'Use quotes if the name contains spaces.')
    parser.add_argument('--password', type=str, nargs='?',
                        help='Specifies the password used to acquire '
                        'the api-token.'
                        ' Use quotes if the name contains spaces.')
    parser.add_argument('--api_token', type=str, nargs='?',
                        help='Specifies the api token for the requests; '
                        'if you do not have a token, '
                        'you must specify the password argument.')
    parser.add_argument('project_id', type=str,
                        help='Specifies the project identification string.')
    parser.add_argument('model_id', type=str,
                        help='Specifies the model identification string.')
    parser.add_argument('dataset', type=str,
                        help='Specifies the .csv input file that '
                        'the script scores.')
    parser.add_argument('--datarobot_key', type=str,
                        nargs='?',
                        help='An additional datarobot_key '
                        'for dedicated prediction instances.')
    parser.add_argument('--out', type=str,
                        nargs='?', default='out.csv',
                        help='Specifies the file name, and optionally path, '
                        'to which the results are written. '
                        'If not specified, the default file name is out.csv, '
                        'written to the directory containing the script.')
    parser.add_argument('--verbose', '-v', action="store_true",
                        help='Provides status updates while '
                        'the script is running.')
    parser.add_argument('--n_samples', type=int,
                        nargs='?',
                        default=1000,
                        help='Specifies the number of samples to use '
                        'per batch. Default sample size is 1000.')
    parser.add_argument('--n_concurrent', type=int,
                        nargs='?',
                        default=4,
                        help='Specifies the number of concurrent requests '
                        'to submit. '
                        'By default, 4 concurrent requests are submitted.')
    parser.add_argument('--api_version', type=str,
                        nargs='?',
                        default='v1',
                        help='Specifies the API version, either v1 or v2. '
                        'The default is v1.')
    parser.add_argument('--create_api_token', action="store_true",
                        default=False,
                        help='Requests a new API token. To use this option, '
                        'you must specify the '
                        'password argument for this request '
                        '(not the api_token argument).')
    parser.add_argument('--n_retry', type=int,
                        default=3,
                        help='Specifies the number of times DataRobot '
                        'will retry if a request fails. '
                        'A value of -1, the default, specifies '
                        'an infinite number of retries.')
    parser.add_argument('--keep_cols', type=str,
                        nargs='?',
                        help='Specifies the column names to append '
                        'to the predictions. '
                        'Enter as a comma-separated list.')
    parser.add_argument('--delimiter', type=str,
                        nargs='?',
                        help='Specifies the delimiter to recognize in '
                        'the input .csv file. '
                        'If not specified, the script tries to automatically '
                        'determine the delimiter, and if it cannot, '
                        'defaults to comma ( , ).')
    parser.add_argument('--resume', action='store_true',
                        default=False,
                        help='Starts the prediction from the point at which '
                        'it was halted. '
                        'If the prediction stopped, for example due '
                        'to error or network connection issue, you can run '
                        'the same command with all the same '
                        'all arguments plus this resume argument.')
    parser.add_argument('--version', action='version',
                        version=VERSION_TEMPLATE, help='Show version')
    parser.add_argument('--timeout', type=int,
                        default=30, help='The timeout for each post request')
    parser.add_argument('--pred_name', type=str,
                        nargs='?')
    parser.set_defaults(prompt=None)
    parser.add_argument('-y', '--yes', dest='prompt', action='store_true',
                        help="Always answer 'yes' for user prompts")
    parser.add_argument('-n', '--no', dest='prompt', action='store_false',
                        help="Always answer 'no' for user prompts")

    parsed_args = {}
    conf_file = get_config_file()
    if conf_file:
        file_args = parse_config_file(conf_file)
        parsed_args.update(file_args)
    pre_parsed_args = {k: v
                       for k, v in vars(parser.parse_args(argv)).items()
                       if v is not None}
    parsed_args.update(pre_parsed_args)
    loglevel = logging.DEBUG if parsed_args['verbose'] else logging.INFO
    ui = UI(parsed_args['prompt'], loglevel)
    printed_args = copy.copy(parsed_args)
    printed_args.pop('password')
    ui.debug(printed_args)
    ui.info('platform: {} {}'.format(sys.platform, sys.version))

    # parse args
    host = parsed_args['host']
    pid = parsed_args['project_id']
    lid = parsed_args['model_id']
    n_retry = int(parsed_args['n_retry'])
    if parsed_args.get('keep_cols'):
        keep_cols = [s.strip() for s in parsed_args['keep_cols'].split(',')]
    else:
        keep_cols = None
    concurrent = int(parsed_args['n_concurrent'])
    dataset = parsed_args['dataset']
    n_samples = int(parsed_args['n_samples'])
    delimiter = parsed_args.get('delimiter')
    resume = parsed_args['resume']
    out_file = parsed_args['out']
    datarobot_key = parsed_args.get('datarobot_key')
    pwd = parsed_args['password']
    timeout = int(parsed_args['timeout'])

    if 'user' not in parsed_args:
        user = ui.prompt_user()
    else:
        user = parsed_args['user'].strip()

    if not os.path.exists(parsed_args['dataset']):
        ui.fatal('file {} does not exist.'.format(parsed_args['dataset']))

    try:
        verify_objectid(pid)
        verify_objectid(lid)
    except ValueError as e:
        ui.fatal('{}'.format(e))

    api_token = parsed_args.get('api_token')
    create_api_token = parsed_args.get('create_api_token')
    pwd = parsed_args['password']
    pred_name = parsed_args.get('pred_name')

    api_version = parsed_args['api_version']

    base_url = '{}/{}/'.format(host, api_version)
    base_headers = {}
    if datarobot_key:
        base_headers['datarobot-key'] = datarobot_key

    ui.info('connecting to {}'.format(base_url))
    try:
        if api_version == 'v1':
            run_batch_predictions_v1(
                base_url=base_url, base_headers=base_headers,
                user=user, pwd=pwd,
                api_token=api_token, create_api_token=create_api_token,
                pid=pid, lid=lid, n_retry=n_retry, concurrent=concurrent,
                resume=resume, n_samples=n_samples,
                out_file=out_file, keep_cols=keep_cols, delimiter=delimiter,
                dataset=dataset, pred_name=pred_name, timeout=timeout,
                ui=ui)
        elif api_version == 'v2':
            run_batch_predictions_v2(base_url, base_headers, user, pwd,
                                     api_token, create_api_token,
                                     pid, lid, concurrent, n_samples,
                                     out_file, dataset, timeout, ui)
        else:
            ui.fatal('API Version {} is not supported'.format(api_version))
    except SystemError:
        pass
    except ShelveError as e:
        ui.error(str(e))
    except KeyboardInterrupt:
        ui.info('Keyboard interrupt')
    except Exception as e:
        ui.fatal(str(e))


if __name__ == '__main__':
    main()
