import os
from mock import Mock


log_files = ['datarobot_batch_scoring_main.log',
             'datarobot_batch_scoring_batcher.log',
             'datarobot_batch_scoring_network.log',
             'datarobot_batch_scoring_writer.log']


class PickableMock(Mock):
    def __reduce__(self):
        return (Mock, ())


def print_logs():
    """
    debug tests by sending the contents of the log files to stdout
    """
    for file in log_files:
        if os.path.isfile(file):
            with open(file, 'r') as o:
                print('>>> {} >>>'.format(file))
                print(o.read())
                print('<<< {} <<<'.format(file))


def read_logs():
    """
    debug tests by sending the contents of the log files to stdout
    """
    output = []
    for file in log_files:
        if os.path.isfile(file):
            output.append(open(file, 'r').read())

    return "\n".join(output)
