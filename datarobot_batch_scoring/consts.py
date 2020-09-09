import collections

Batch = collections.namedtuple('Batch', 'id rows fieldnames data rty_cnt')
SENTINEL = Batch(-1, 0, None, '', -1)
ERROR_SENTINEL = Batch(-1, 1, None, '', -1)

REPORT_INTERVAL = 5

DEPRECATION_WARNING = (
    '{yellow}{bold}Deprecation Warning!{reset} '
    'The Batch Scoring script is deprecated. It will continue functioning '
    'indefinitely, but it will not receive any new bug fixes and new '
    'functionality. Please, use the Batch Prediction command-line tools '
    'instead: '
    '({underline}https://app.datarobot.com/docs/predictions'
    '/batch/cli-scripts.html{reset}).'
    .format(
        yellow='\033[93m',
        bold='\033[1m',
        reset='\033[0m',
        underline='\033[4m'
    )
   )


class WriterQueueMsg(object):
    CTX_WARNING = 'CTX_WARNING'
    CTX_ERROR = 'CTX_ERROR'
    RESPONSE = 'RESPONSE'
    SENTINEL = 'SENTINEL'


class ProgressQueueMsg(object):
    SHOVEL_DONE = 'SHOVEL_DONE'
    SHOVEL_ERROR = 'SHOVEL_ERROR'
    SHOVEL_CSV_ERROR = 'SHOVEL_CSV_ERROR'
    SHOVEL_PROGRESS = 'SHOVEL_PROGRESS'
    NETWORK_DONE = 'NETWORK_DONE'
    NETWORK_ERROR = 'NETWORK_ERROR'
    NETWORK_PROGRESS = 'NETWORK_PROGRESS'
    WRITER_DONE = 'WRITER_DONE'
    WRITER_ERROR = 'WRITER_ERROR'
    WRITER_PROGRESS = 'WRITER_PROGRESS'


class TargetType(object):
    REGRESSION = 'Regression'
    BINARY = 'Binary'
