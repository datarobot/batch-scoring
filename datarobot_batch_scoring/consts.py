import collections

Batch = collections.namedtuple('Batch', 'id rows fieldnames data rty_cnt')
SENTINEL = Batch(-1, 0, None, '', -1)
ERROR_SENTINEL = Batch(-1, 1, None, '', -1)

REPORT_INTERVAL = 5


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
