import collections
import json
import logging
import sys
import textwrap
from functools import partial
from time import time

import requests
import requests.adapters
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from six.moves import queue

from datarobot_batch_scoring.consts import (SENTINEL, ERROR_SENTINEL,
                                            WriterQueueMsg, Batch)
from datarobot_batch_scoring.reader import fast_to_csv_chunk, slow_to_csv_chunk
from datarobot_batch_scoring.utils import compress

try:
    from futures import ThreadPoolExecutor
except ImportError:
    from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')


class MultiprocessingGeneratorBackedQueue(object):
    """A queue that is backed by a generator.

    When the queue is exhausted it repopulates from the generator.
    """
    def __init__(self, ui, queue, deque):
        self.n_consumed = 0
        self.queue = queue
        self.deque = deque
        self._ui = ui

    def __iter__(self):
        return self

    def __next__(self):
        try:
            r = self.deque.get_nowait()
            self._ui.debug('Got batch from dequeu: {}'.format(r.id))
            return r
        except queue.Empty:
            try:
                r = self.queue.get()
                if r.id == SENTINEL.id:
                    if r.rows == ERROR_SENTINEL.rows:
                        self._ui.error('Error parsing CSV file, '
                                       'check logs for exact error')
                    raise StopIteration
                self.n_consumed += 1
                return r
            except OSError:
                raise StopIteration

    def __len__(self):
        return self.queue.qsize() + self.deque.qsize()

    def next(self):
        return self.__next__()

    def push(self, batch):
        # we retry a batch - decrement retry counter
        batch = batch._replace(rty_cnt=batch.rty_cnt - 1)
        try:
            self.deque.put(batch, block=False)
        except queue.Empty:
            self._ui.error('Dropping {} due to backfill queue full.'.format(
                batch))


class WorkUnitGenerator(object):
    """Generates async requests with completion or retry callbacks.

    It uses a queue backed by a batch generator.
    It will pop items for the queue and if its exhausted it will populate the
    queue from the batch generator.
    If a submitted async request was not successfull it gets enqueued again.
    """

    def __init__(self, queue, endpoint, headers, user, api_token, pred_name,
                 fast_mode, ui, max_batch_size, writer_queue, compression):
        self.endpoint = endpoint
        self.headers = headers
        self.user = user
        self.api_token = api_token
        self.queue = queue
        self.pred_name = pred_name
        self.fast_mode = fast_mode
        self._ui = ui
        self.max_batch_size = max_batch_size
        self.writer_queue = writer_queue
        self.compression = compression

    def _response_callback(self, r, batch=None, *args, **kw):
        try:
            if r.status_code == 200:
                pickleable_resp = {'elapsed': r.elapsed.total_seconds(),
                                   'text': r.text}
                self.writer_queue.put((WriterQueueMsg.RESPONSE, {
                    "request": pickleable_resp,
                    "batch": batch
                }))
                return
            elif isinstance(r, FakeResponse):
                self._ui.debug('Skipping processing response '
                               'because of FakeResponse')
                self.queue.push(batch)
            else:
                try:
                    self._ui.warning('batch {} failed with status: {}'
                                     .format(batch.id,
                                             json.loads(r.text)['status']))
                except ValueError:
                    self._ui.warning('batch {} failed with status code: {}'
                                     .format(batch.id, r.status_code))

                text = r.text
                msg = ('batch {} failed, queued to retry, status_code:{} '
                       'text:{}'.format(batch.id, r.status_code, text))
                self._ui.error(msg)
                self.send_warning_to_ctx(batch, msg)
                self.queue.push(batch)
        except Exception as e:
            msg = 'batch {} - dropping due to: {}, {} records lost'.format(
                batch.id, e, batch.rows)
            self._ui.error(msg)
            self.send_error_to_ctx(batch, msg)

    def send_warning_to_ctx(self, batch, message):
        self._ui.info('WorkUnitGenerator sending WARNING batch_id {} , '
                      'message {}'.format(batch.id, message))
        self.writer_queue.put((WriterQueueMsg.CTX_WARNING, {
            "batch": batch,
            "error": message
        }))

    def send_error_to_ctx(self, batch, message):
        self._ui.info('WorkUnitGenerator sending ERROR batch_id {} , '
                      'message {}'.format(batch.id, message))

        self.writer_queue.put((WriterQueueMsg.CTX_ERROR, {
            "batch": batch,
            "error": message
        }))

    def __iter__(self):
        for batch in self.queue:
            if batch.id == -1:  # sentinel
                raise StopIteration()
            # if we exhaused our retries we drop the batch
            if batch.rty_cnt == 0:
                msg = ('batch {} exceeded retry limit; '
                       'we lost {} records'.format(
                            batch.id, len(batch.data)))
                self._ui.error(msg)
                self.send_error_to_ctx(batch, msg)
                continue

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
                        self._ui.debug(
                            'batch {}-{} transmitting {} byte - space savings '
                            '{}%'.format(batch.id, batch.rows,
                                         sys.getsizeof(data),
                                         '%.2f' % float(1 -
                                                        (sys.getsizeof(data) /
                                                         starting_size))))
                    else:
                        self._ui.debug('batch {}-{} transmitting {} bytes'
                                       .format(batch.id, batch.rows,
                                               starting_size))
                    hook = partial(self._response_callback, batch=batch)

                    yield requests.Request(
                        method='POST',
                        url=self.endpoint,
                        headers=self.headers,
                        data=data,
                        auth=(self.user, self.api_token),
                        hooks={'response': hook})
                else:
                    if batch.rows < 2:
                        msg = ('batch {} is single row but bigger '
                               'than limit, skipping. We lost {} '
                               'records'.format(batch.id,
                                                len(batch.data)))
                        self._ui.error(msg)
                        self.send_error_to_ctx(batch, msg)
                        continue

                    msg = ('batch {}-{} is too long: {} bytes,'
                           ' splitting'.format(batch.id, batch.rows,
                                               len(data)))
                    self._ui.debug(msg)
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


class Network(object):
    def __init__(self, concurrency, timeout, ui,
                 network_queue,
                 network_deque,
                 writer_queue,
                 endpoint,
                 headers,
                 user,
                 api_token,
                 pred_name,
                 fast_mode,
                 max_batch_size,
                 compression):

        self.concurrency = concurrency
        self.timeout = timeout
        self.ui = ui
        self.network_queue = network_queue
        self.network_deque = network_deque
        self.writer_queue = writer_queue
        self.endpoint = endpoint
        self.headers = headers
        self.user = user
        self.api_token = api_token
        self.pred_name = pred_name
        self.fast_mode = fast_mode
        self.max_batch_size = max_batch_size
        self.compression = compression

        self.work_unit_gen = None
        self._executor = ThreadPoolExecutor(concurrency)
        self._timeout = timeout
        self._ui = ui or logger
        self.futures = []
        self.concurrency = concurrency

        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=concurrency, pool_maxsize=concurrency)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def _request(self, request):
        prepared = self.session.prepare_request(request)
        try:
            self.session.send(prepared, timeout=self._timeout)
        except requests.exceptions.ReadTimeout:
            self._ui.warning(textwrap.dedent("""The server did not send any data
in the allotted amount of time.
You might want to decrease the "--n_concurrent" parameters
or
increase "--timeout" parameter.
"""))

        except Exception as exc:
            self._ui.debug('Exception {}: {}'.format(type(exc), exc))
            try:
                callback = request.kwargs['hooks']['response']
            except AttributeError:
                callback = request.hooks['response'][0]
            response = FakeResponse(400, 'No Response')
            callback(response)

    def perform_requests(self, requests):
        for r in requests:
            while True:
                self.futures = [i for i in self.futures if not i.done()]
                if len(self.futures) < self.concurrency:
                    self.futures.append(self._executor.submit(self._request,
                                                              r))
                    break
                else:
                    wait(self.futures, return_when=FIRST_COMPLETED)
            yield
        #  wait for all batches to finish before returning
        while self.futures:
            f_len = len(self.futures)
            self.futures = [i for i in self.futures if not i.done()]
            if f_len != len(self.futures):
                self._ui.debug('Waiting for final requests to finish. '
                               'remaining requests: {}'
                               ''.format(len(self.futures)))
            wait(self.futures, return_when=FIRST_COMPLETED)
        yield True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=True)

    def go(self, dry_run=False):
        MGBQ = MultiprocessingGeneratorBackedQueue(ui=self.ui,
                                                   queue=self.network_queue,
                                                   deque=self.network_deque)
        self.work_unit_gen = WorkUnitGenerator(
            queue=MGBQ,
            endpoint=self.endpoint,
            headers=self.headers,
            user=self.user,
            api_token=self.api_token,
            pred_name=self.pred_name,
            fast_mode=self.fast_mode,
            ui=self.ui,
            max_batch_size=self.max_batch_size,
            writer_queue=self.writer_queue,
            compression=self.compression)

        if dry_run:
            i = 0
            for _ in self.work_unit_gen:
                i += 1

            return i

        t0 = time()
        i = 0
        r = None
        for r in self.perform_requests(self.work_unit_gen):
            i += 1
            self.ui.info('{} responses sent | time elapsed {}s'
                         .format(i, time() - t0))

        return r, i, MGBQ.n_consumed
