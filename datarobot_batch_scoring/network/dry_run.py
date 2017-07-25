import collections
import json
import logging
import os
import signal
import sys
import textwrap

import requests
import requests.adapters
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from datarobot_batch_scoring.consts import (SENTINEL,
                                            WriterQueueMsg, Batch)
from datarobot_batch_scoring.reader import (fast_to_csv_chunk,
                                            slow_to_csv_chunk)
from datarobot_batch_scoring.utils import compress, Worker
from six.moves import queue

try:
    from futures import ThreadPoolExecutor
except ImportError:
    from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')


class DryRunNetworkWorker(Worker):
    """A worker that will drain the network_queue, but doesn't actually send any
    requests or put anything into the writer_queue
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
                 compression):

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

    def push_retry(self, batch):
        # we retry a batch - decrement retry counter
        batch = batch._replace(rty_cnt=batch.rty_cnt - 1)
        try:
            self.network_deque.put(batch, block=False)
        except queue.Full:
            msg = 'Dropping {} due to backfill queue full.'.format(
                batch)
            self.ui.error(msg)
            self.send_error_to_ctx(batch, msg)

    def _response_callback(self, r, batch=None, *args, **kw):
        try:
            if r.status_code == 200:
                pickleable_resp = {'elapsed': r.elapsed.total_seconds(),
                                   'text': r.text,
                                   'headers': r.headers}
                self.writer_queue.put((WriterQueueMsg.RESPONSE, {
                    "request": pickleable_resp,
                    "batch": batch
                }))
                return
            elif isinstance(r, FakeResponse):
                if r.status_code == 499:
                    msg = ('batch {} timed out, dropping; '
                           'we lost {} records'
                           ''.format(batch.id, len(batch.data)))
                    self.ui.error(msg)
                    self.send_error_to_ctx(batch, msg)
                    return

                self.ui.debug('Skipping processing response '
                              'because of FakeResponse')
            else:
                try:
                    self.ui.warning('batch {} failed with status code '
                                    '{} message: {}'
                                    ''.format(
                                         batch.id,
                                         r.status_code,
                                         json.loads(r.text)['message']))
                except ValueError:
                    self.ui.warning('batch {} failed with status code: {}'
                                    ''.format(batch.id, r.status_code))

                text = r.text
                msg = ('batch {} failed, status_code:{} '
                       'text:{}'.format(batch.id, r.status_code, text))
                self.ui.error(msg)
                self.send_warning_to_ctx(batch, msg)

            if batch.rty_cnt == 1:
                msg = ('batch {} exceeded retry limit; '
                       'we lost {} records'
                       ''.format(batch.id, len(batch.data)))
                self.ui.error(msg)
                self.send_error_to_ctx(batch, msg)
            else:
                self.ui.warning('retrying failed batch {}, attempts left: {}'
                                .format(batch.id, batch.rty_cnt - 1))
                self.push_retry(batch)

        except Exception as e:
            msg = 'batch {} - dropping due to: {}, {} records lost'.format(
                batch.id, e, batch.rows)
            self.ui.error(msg)
            self.send_error_to_ctx(batch, msg)

    def _request(self, request):

        prepared = self.session.prepare_request(request)
        try:
            self.session.send(prepared, timeout=self._timeout)
        except Exception as exc:
            code = 400
            if isinstance(exc, requests.exceptions.ReadTimeout):
                self.ui.warning(textwrap.dedent("""The server did not send any data
in the allotted amount of time.
You might want to decrease the "--n_concurrent" parameters
or
increase "--timeout" parameter.
"""))
                code = 499
            else:
                self.ui.debug('Exception {}: {}'.format(type(exc), exc))
                raise

            try:
                callback = request.kwargs['hooks']['response']
            except AttributeError:
                callback = request.hooks['response'][0]
            response = FakeResponse(code, 'No Response')
            callback(response)

    def get_batch(self):
        while True:
            if self.abort_flag.value:
                self.exit_fast(None, None)
                break
            try:
                r = self.network_deque.get_nowait()
                self.ui.debug('Got batch from dequeu: {}'.format(r.id))
                self.n_retried += 1
                yield r
            except queue.Empty:
                try:
                    r = self.network_queue.get(timeout=1)
                    if r.id == SENTINEL.id:
                        break
                    self.n_consumed += 1
                    yield r
                except queue.Empty:
                    if self.state in (b"-", b"R"):
                        self.state = b'E'
                    elif self.state == b"E":
                        self.state = b'e'
                    elif self.state == b"e":
                        self.state = b'I'
                        break

                except OSError:
                    self.ui.error('OS Error')
                    break

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

    def request_cb(self, f):
        futures = [i for i in self.futures if not i.done()]
        self.ui.debug('request finished, pending futures: {}'
                      ''.format(len(futures)))

        if len(futures) == 0:
            self.state = b'E'

    def exit_fast(self, a, b):
        self.state = b'D'
        os._exit(1)

    def perform_requests(self):
        signal.signal(signal.SIGINT, self.exit_fast)
        signal.signal(signal.SIGTERM, self.exit_fast)

        self.state = b'E'
        for q_batch in self.get_batch():
            for (_, _) in self.split_batch(q_batch):
                if self.state != b"R":
                    self.state = b'R'
                yield
                continue

        #  wait for all batches to finish before returning
        self.state = b'W'
        while self.futures:
            f_len = len(self.futures)
            self.futures = [i for i in self.futures if not i.done()]
            if f_len != len(self.futures):
                self.ui.debug('Waiting for final requests to finish. '
                              'remaining requests: {}'
                              ''.format(len(self.futures)))
            wait(self.futures, return_when=FIRST_COMPLETED)
        self.state = b'D'
        yield True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.proc and self.proc.is_alive():
            self.proc.terminate()

    def run(self):
        i = 0
        for _ in self.perform_requests():
            i += 1
        return i

    def go(self):
        return self.run()
