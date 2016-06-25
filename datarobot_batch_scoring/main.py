import argparse
import copy
import logging
import os
import sys
import warnings

from datarobot_batch_scoring import __version__
from datarobot_batch_scoring.batch_scoring import (run_batch_predictions,
                                                   ShelveError)
from datarobot_batch_scoring.utils import (UI, get_config_file,
                                           parse_config_file,
                                           verify_objectid)

VERSION_TEMPLATE = '%(prog)s {}'.format(__version__)


DESCRIPTION = """
Batch score DATASET by submitting prediction requests against HOST
using PROJECT_ID and MODEL_ID.

It will send batches of size N_SAMPLES.
Set N_SAMPLES such that the round-trip is roughly 10sec (see verbose output).

Set N_CONCURRENT to match the number of cores in the prediction API endpoint.

The dataset has to be a single CSV file that can be gzipped (extension '.gz').
The OUT will be a single CSV file but remember that records
might be unordered.
"""


EPILOG = """
Example:
  $ batch_scoring --host https://beta.datarobot.com/api --user="<username>" \
--password="<password>" 5545eb20b4912911244d4835 5545eb71b4912911244d4847 \
~/Downloads/diabetes_test.csv
"""


VALID_DELIMITERS = {';', ',', '|', '\t', ' ', '!', '  '}


def main(argv=sys.argv[1:]):
    global ui  # global variable hack, will get rid of a bit later
    warnings.simplefilter('ignore')
    parser = argparse.ArgumentParser(
        description=DESCRIPTION, epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--verbose', '-v', action="store_true",
                        help='Provides status updates while '
                        'the script is running.')
    parser.add_argument('--version', action='version',
                        version=VERSION_TEMPLATE, help='Show version')
    dataset_gr = parser.add_argument_group('Dataset and server')
    dataset_gr.add_argument('--host', type=str,
                            help='Specifies the hostname of the prediction '
                            'API endpoint (the location of the data '
                            'from where '
                            'to get predictions)')
    dataset_gr.add_argument('project_id', type=str,
                            help='Specifies the project '
                            'identification string.')
    dataset_gr.add_argument('model_id', type=str,
                            help='Specifies the model identification string.')
    dataset_gr.add_argument('dataset', type=str,
                            help='Specifies the .csv input file that '
                            'the script scores.')
    dataset_gr.add_argument('--out', type=str,
                            nargs='?', default='out.csv',
                            help='Specifies the file name, '
                            'and optionally path, '
                            'to which the results are written. '
                            'If not specified, '
                            'the default file name is out.csv, '
                            'written to the directory containing the script. '
                            '(default: %(default)r)')
    auth_gr = parser.add_argument_group('Authentication parameters')
    auth_gr.add_argument('--user', type=str,
                         help='Specifies the username used to acquire '
                         'the api-token. '
                         'Use quotes if the name contains spaces.')
    auth_gr.add_argument('--password', type=str, nargs='?',
                         help='Specifies the password used to acquire '
                         'the api-token. '
                         'Use quotes if the name contains spaces.')
    auth_gr.add_argument('--api_token', type=str, nargs='?',
                         help='Specifies the api token for the requests; '
                         'if you do not have a token, '
                         'you must specify the password argument.')
    auth_gr.add_argument('--create_api_token', action="store_true",
                         default=False,
                         help='Requests a new API token. To use this option, '
                         'you must specify the '
                         'password argument for this request '
                         '(not the api_token argument). '
                         '(default: %(default)r)')
    auth_gr.add_argument('--datarobot_key', type=str,
                         nargs='?',
                         help='An additional datarobot_key '
                         'for dedicated prediction instances.')
    conn_gr = parser.add_argument_group('Connection control')
    conn_gr.add_argument('--timeout', type=int,
                         default=30,
                         help='The timeout for each post request. '
                         '(default: %(default)r)')
    conn_gr.add_argument('--n_samples', type=int,
                         nargs='?',
                         default=1000,
                         help='Specifies the number of samples to use '
                         'per batch. (default: %(default)r)')
    conn_gr.add_argument('--n_concurrent', type=int,
                         nargs='?',
                         default=4,
                         help='Specifies the number of concurrent requests '
                         'to submit. (default: %(default)r)')
    conn_gr.add_argument('--n_retry', type=int,
                         default=3,
                         help='Specifies the number of times DataRobot '
                         'will retry if a request fails. '
                         'A value of -1, the default, specifies '
                         'an infinite number of retries.'
                         '(default: %(default)r)')
    conn_gr.add_argument('--resume', action='store_true',
                         default=False,
                         help='Starts the prediction from the point at which '
                         'it was halted. '
                         'If the prediction stopped, for example due '
                         'to error or network connection issue, you can run '
                         'the same command with all the same '
                         'all arguments plus this resume argument.')
    csv_gr = parser.add_argument_group('CVS parameters')
    csv_gr.add_argument('--keep_cols', type=str,
                        nargs='?',
                        help='Specifies the column names to append '
                        'to the predictions. '
                        'Enter as a comma-separated list.')
    csv_gr.add_argument('--delimiter', type=str,
                        nargs='?', default=None,
                        help='Specifies the delimiter to recognize in '
                        'the input .csv file. '
                        'If not specified, the script tries to automatically '
                        'determine the delimiter.')
    csv_gr.add_argument('--pred_name', type=str,
                        nargs='?')
    csv_gr.add_argument('--fast', action='store_true',
                        default=False,
                        help='Experimental: faster CSV processor. '
                        'Note: does not support multiline csv. ')
    csv_gr.add_argument('--auto_sample', action='store_true',
                        default=False,
                        help='Override "n_samples" and instead '
                        'use chunks of about 1.5 MB.')
    misc_gr = parser.add_argument_group('Miscellaneous')
    misc_gr.add_argument('-y', '--yes', dest='prompt', action='store_true',
                         help="Always answer 'yes' for user prompts")
    misc_gr.add_argument('-n', '--no', dest='prompt', action='store_false',
                         help="Always answer 'no' for user prompts")
    misc_gr.add_argument('--dry_run', dest='dry_run', action='store_true',
                         help="Only read/chunk input data but dont send "
                         "requests.")
    misc_gr.add_argument('--stdout', action='store_true', dest='stdout',
                         default=False,
                         help='Send all log messages to stdout.')

    defaults = {
        'prompt': None,
        'out': 'out.csv',
        'create_api_token': False,
        'timeout': 30,
        'n_samples': 1000,
        'n_concurrent': 4,
        'n_retry': 3,
        'resume': False,
        'fast': False,
        'stdout': False,
        'auto_sample': False
    }

    conf_file = get_config_file()
    if conf_file:
        file_args = parse_config_file(conf_file)
        defaults.update(file_args)
    parser.set_defaults(**defaults)
    for action in parser._actions:
        if action.dest in defaults and action.required:
            action.required = False
            if '--' + action.dest not in argv:
                action.nargs = '?'
    parsed_args = {k: v
                   for k, v in vars(parser.parse_args(argv)).items()
                   if v is not None}
    loglevel = logging.DEBUG if parsed_args['verbose'] else logging.INFO
    stdout = parsed_args['stdout']
    ui = UI(parsed_args.get('prompt'), loglevel, stdout)
    printed_args = copy.copy(parsed_args)
    printed_args.pop('password', None)
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
    timeout = int(parsed_args['timeout'])
    fast_mode = parsed_args['fast']
    auto_sample = parsed_args['auto_sample']

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
        ui.fatal(str(e))

    if delimiter == '\\t':
        # NOTE: on bash you have to use Ctrl-V + TAB
        delimiter = '\t'

    if delimiter and delimiter not in VALID_DELIMITERS:
        ui.fatal('Delimiter "{}" is not a valid delimiter.'
                 .format(delimiter))

    api_token = parsed_args.get('api_token')
    create_api_token = parsed_args.get('create_api_token')
    pwd = parsed_args.get('password')
    pred_name = parsed_args.get('pred_name')
    dry_run = parsed_args.get('dry_run', False)

    base_url = '{}/{}/'.format(host, 'v1')
    base_headers = {}
    if datarobot_key:
        base_headers['datarobot-key'] = datarobot_key

    ui.info('connecting to {}'.format(base_url))

    try:
        run_batch_predictions(
            base_url=base_url, base_headers=base_headers,
            user=user, pwd=pwd,
            api_token=api_token, create_api_token=create_api_token,
            pid=pid, lid=lid, n_retry=n_retry, concurrent=concurrent,
            resume=resume, n_samples=n_samples,
            out_file=out_file, keep_cols=keep_cols, delimiter=delimiter,
            dataset=dataset, pred_name=pred_name, timeout=timeout,
            ui=ui, fast_mode=fast_mode, auto_sample=auto_sample,
            dry_run=dry_run)
    except SystemError:
        pass
    except ShelveError as e:
        ui.error(str(e))
    except KeyboardInterrupt:
        ui.info('Keyboard interrupt')
    except Exception as e:
        ui.fatal(str(e))
    finally:
        ui.close()


if __name__ == '__main__':
    main()  # pragma: no cover
