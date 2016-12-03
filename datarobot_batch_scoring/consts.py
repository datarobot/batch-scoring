import collections

Batch = collections.namedtuple('Batch', 'id rows fieldnames data rty_cnt')
SENTINEL = Batch(-1, 0, None, '', -1)
ERROR_SENTINEL = Batch(-1, 1, None, '', -1)


class QueueMsg(object):
    WARNING = 'WARNING'
    ERROR = 'ERROR'


class TargetType(object):
    REGRESSION = 'Regression'
    BINARY = 'Binary'
