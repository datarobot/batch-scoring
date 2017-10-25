import collections
import logging
import os
import sys

from datarobot_batch_scoring.consts import (WriterQueueMsg, Batch,
                                            ProgressQueueMsg)
from datarobot_batch_scoring.reader import (fast_to_csv_chunk,
                                            slow_to_csv_chunk)
from datarobot_batch_scoring.utils import compress, get_rusage, Worker


logger = logging.getLogger(__name__)


FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')


class BaseNetworkWorker(Worker):
    """A network worker

    Work for the worker is read off of the network_queue; failures are put into
    the network_deque from which they are also read and sometimes retried.

    Successfully returned responses are put into the writer_queue for
    processing by another worker.

    Occasional progress updates are occasionally put into the progress_queue
    for processing by another worker

    A properly functioning Worker implementation must read data from the
    network_queue until it finds the SENTINEL message, which is the indicator
    that no more work will be put onto the network queue. At this point the
    worker should finish any remaining work and put a NETWORK_PROGRESS message
    into the progress_queue with the following details:

    ret : boolean
        The return status (success flag) of the process
    processed : int
        The number of requests processed
    retried : int
        The number of requests retried (read from the network_deque)
    consumed : int
        The number of consumed requests from the network_queue
    rusage : dict
        The resource usage. See the function `get_rusage` in `utils`

    In addition, it is helpful for debugging purposes if the worker is diligent
    about minding its state transitions. As an instance of `Worker`, any time
    the `self.state` attribute is updated, a message is sent to the interface
    logging this fact. Valid values for the state are defined in this class in
    the `state_names` class attribute.
    """

    state_names = {
        b"-": "Initial",
        b"I": "Idle",
        b"e": "PreIdle",
        b"E": "PrePreIdle",
        b"R": "Doing Requests",
        b"F": "Pool is Full",
        b"W": "Waiting for Finish",
        b"D": "Done"
    }

    def __init__(self, concurrency, timeout, ui,
                 network_queue,
                 network_deque,
                 writer_queue,
                 progress_queue,
                 abort_flag,
                 network_status,
                 endpoint,
                 headers,
                 user,
                 api_token,
                 pred_name,
                 fast_mode,
                 max_batch_size,
                 compression,
                 verify_ssl):

        Worker.__init__(self, network_status)

        self.concurrency = concurrency
        self.timeout = timeout
        self.ui = ui or logger
        self.network_queue = network_queue
        self.network_deque = network_deque
        self.writer_queue = writer_queue
        self.progress_queue = progress_queue
        self.abort_flag = abort_flag
        self.endpoint = endpoint
        self.headers = headers
        self.user = user
        self.api_token = api_token
        self.pred_name = pred_name
        self.fast_mode = fast_mode
        self.max_batch_size = max_batch_size
        self.compression = compression
        self.verify_ssl = verify_ssl

        self._timeout = timeout
        self.futures = []
        self.concurrency = concurrency

        self._executor = None
        self.session = None
        self.proc = None

        self.n_consumed = 0
        self.n_retried = 0
        self.n_requests = 0

    def send_warning_to_ctx(self, batch, message):
        self.ui.info('CTX WARNING batch_id {} , '
                     'message {}'.format(batch.id, message))
        self.writer_queue.put((WriterQueueMsg.CTX_WARNING, {
            "batch": batch,
            "error": message
        }))

    def send_error_to_ctx(self, batch, message):
        self.ui.info('CTX ERROR batch_id {} , '
                     'message {}'.format(batch.id, message))

        self.writer_queue.put((WriterQueueMsg.CTX_ERROR, {
            "batch": batch,
            "error": message
        }))

    def split_batch(self, batch):
        if self.fast_mode:
            chunk_formatter = fast_to_csv_chunk
        else:
            chunk_formatter = slow_to_csv_chunk

        todo = [batch]
        while todo:
            batch = todo.pop(0)
            data = chunk_formatter(batch.data, batch.fieldnames)
            starting_size = sys.getsizeof(data)
            if starting_size < self.max_batch_size:
                if self.compression:
                    data = compress(data)
                    self.ui.debug(
                        'batch {}-{} transmitting {} byte - space savings '
                        '{}%'.format(batch.id, batch.rows,
                                     sys.getsizeof(data),
                                     '%.2f' % float(1 -
                                                    (sys.getsizeof(data) /
                                                     starting_size))))
                else:
                    self.ui.debug('batch {}-{} transmitting {} bytes'
                                  ''.format(batch.id, batch.rows,
                                            starting_size))

                yield (batch, data)
            else:
                if batch.rows < 2:
                    msg = ('batch {} is single row but bigger '
                           'than limit, skipping. We lost {} '
                           'records'.format(batch.id,
                                            len(batch.data)))
                    self.ui.error(msg)
                    self.send_error_to_ctx(batch, msg)
                    continue

                msg = ('batch {}-{} is too long: {} bytes,'
                       ' splitting'.format(batch.id, batch.rows,
                                           len(data)))
                self.ui.debug(msg)
                self.send_warning_to_ctx(batch, msg)
                split_point = int(batch.rows/2)

                data1 = batch.data[:split_point]
                batch1 = Batch(batch.id, split_point, batch.fieldnames,
                               data1, batch.rty_cnt)
                todo.append(batch1)

                data2 = batch.data[split_point:]
                batch2 = Batch(batch.id + split_point,
                               batch.rows - split_point,
                               batch.fieldnames, data2, batch.rty_cnt)
                todo.append(batch2)
                todo.sort()

    def exit_fast(self, a, b):
        self.state = b'D'
        os._exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.proc and self.proc.is_alive():
            self.proc.terminate()

    def run(self):
        """This is a dummy implementation. It will return immediately."""
        self.progress_queue.put((ProgressQueueMsg.NETWORK_DONE, {
            "ret": True,
            "processed": self.n_requests,
            "retried": self.n_retried,
            "consumed": self.n_consumed,
            "rusage": get_rusage(),
        }))

    def go(self):
        """This is a dummy implementation. It will return immediately."""
        return self.run()
