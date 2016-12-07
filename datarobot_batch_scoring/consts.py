import collections

Batch = collections.namedtuple('Batch', 'id rows fieldnames data rty_cnt')
SENTINEL = Batch(-1, 0, None, '', -1)
ERROR_SENTINEL = Batch(-1, 1, None, '', -1)


class WriterQueueMsg(object):
    CTX_WARNING = 'CTX_WARNING'
    CTX_ERROR = 'CTX_ERROR'
    RESPONSE = 'RESPONSE'
    SENTINEL = 'SENTINEL'


class ProgressQueueMsg(object):
    SHELVE_DONE = 'SHELVE_DONE'
    SHELVE_ERROR = 'SHELVE_ERROR'
    SHELVE_CSV_ERROR = 'SHELVE_CSV_ERROR'
    SHELVE_PROGRESS = 'SHELVE_PROGRESS'
    NETWORK_DONE = 'NETWORK_DONE'
    NETWORK_ERROR = 'NETWORK_ERROR'
    NETWORK_PROGRESS = 'NETWORK_PROGRESS'
    WRITER_DONE = 'WRITER_DONE'
    WRITER_ERROR = 'WRITER_ERROR'
    WRITER_PROGRESS = 'WRITER_PROGRESS'


class TargetType(object):
    REGRESSION = 'Regression'
    BINARY = 'Binary'
