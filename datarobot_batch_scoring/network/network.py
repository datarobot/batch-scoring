import collections
import json
import logging
import multiprocessing
import signal
import textwrap
from functools import partial
from time import time
import threading
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
try:
    from futures import ThreadPoolExecutor
except ImportError:
    from concurrent.futures import ThreadPoolExecutor

from six.moves import queue
import requests
import requests.adapters

from datarobot_batch_scoring.consts import (SENTINEL,
                                            REPORT_INTERVAL,
                                            ProgressQueueMsg, WriterQueueMsg)
from datarobot_batch_scoring.utils import get_rusage

from .base_network_worker import BaseNetworkWorker


logger = logging.getLogger(__name__)
FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')
lock = threading.Lock()

try:
    from functools import lru_cache
except ImportError:
    # Python 2.7 compatible lru_cache implementation
    # https://stackoverflow.com/questions/17119154/python-decorator-optional-argument
    import functools
    def lru_cache(*setting_args, **setting_kwargs):
        cache = {}
        no_args = (
            len(setting_args) == 1 and
            not setting_kwargs and
            callable(setting_args[0])
        )
        if no_args:
            # We were called without args
            func = setting_args[0]
        def outer(func):
            @functools.wraps(func)
            def cacher(*args, **kwargs):
                with lock:
                    key = tuple(args) + tuple(sorted(kwargs.items()))
                    if key not in cache:
                        cache[key] = func(*args, **kwargs)
                    return cache[key]
            return cacher
        return outer(func) if no_args else outer


# Monkey-patch getaddrinfo with an LRU cache to minimize conflicting calls
from requests.packages.urllib3.util.connection import socket as r_socket
old_getaddrinfo = r_socket.getaddrinfo

@lru_cache()
def my_getaddrinfo(*args, **kwargs):
    return old_getaddrinfo(*args, **kwargs)

r_socket.getaddrinfo = my_getaddrinfo

class Network(BaseNetworkWorker):

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

    def _request(self, request):

        try:
            prepared = self.session.prepare_request(request)
            self.session.send(prepared, timeout=self._timeout)
        except Exception as exc:
            code = 400
            if isinstance(exc, requests.exceptions.ReadTimeout):
                self.ui.warning(textwrap.dedent(
                    """The server did not send any data
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

    def request_cb(self, f):
        futures = [i for i in self.futures if not i.done()]
        self.ui.debug('request finished, pending futures: {}'
                      ''.format(len(futures)))

        if len(futures) == 0:
            self.state = b'E'

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
                    if self.state == b"E":
                        self.state = b'e'
                    elif self.state == b"e":
                        self.state = b'I'

                except OSError:
                    self.ui.error('OS Error')
                    break

    def perform_requests(self):
        signal.signal(signal.SIGINT, self.exit_fast)
        signal.signal(signal.SIGTERM, self.exit_fast)

        self.state = b'E'
        for q_batch in self.get_batch():
            for (batch, data) in self.split_batch(q_batch):

                hook = partial(self._response_callback, batch=batch)
                r = requests.Request(
                    method='POST',
                    url=self.endpoint,
                    headers=self.headers,
                    data=data,
                    auth=(self.user, self.api_token),
                    hooks={'response': hook})

                self.n_requests += 1

                while True:
                    self.futures = [i for i in self.futures if not i.done()]
                    if len(self.futures) < self.concurrency:
                        self.state = b'R'
                        f = self._executor.submit(self._request, r)
                        f.add_done_callback(self.request_cb)
                        self.futures.append(f)
                        break
                    else:
                        self.state = b'F'
                        wait(self.futures, return_when=FIRST_COMPLETED)
                yield
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

    def run(self):
        self._executor = ThreadPoolExecutor(self.concurrency)
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=self.concurrency, pool_maxsize=self.concurrency)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        t0 = time()
        last_report = time()
        i = 0
        r = None
        for r in self.perform_requests():
            if r is not True:
                i += 1
                self.ui.info('{} responses sent | time elapsed {}s'
                             .format(i, time() - t0))

                if time() - last_report > REPORT_INTERVAL:
                    self.progress_queue.put((
                        ProgressQueueMsg.NETWORK_PROGRESS, {
                            "processed": self.n_requests,
                            "retried": self.n_retried,
                            "consumed": self.n_consumed,
                            "rusage": get_rusage(),
                        }))
                    last_report = time()

        self.progress_queue.put((ProgressQueueMsg.NETWORK_DONE, {
            "ret": r,
            "processed": self.n_requests,
            "retried": self.n_retried,
            "consumed": self.n_consumed,
            "rusage": get_rusage(),
        }))

    def go(self):
        self.ui.set_next_UI_name('network')
        self.proc = \
            multiprocessing.Process(target=self.run,
                                    name='Netwrk_Proc')
        self.proc.start()
        return self.proc
