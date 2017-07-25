import collections
import logging
import multiprocessing
import signal
from functools import partial
from time import time

import requests
import requests.adapters
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from datarobot_batch_scoring.consts import (SENTINEL,
                                            REPORT_INTERVAL,
                                            ProgressQueueMsg)
from datarobot_batch_scoring.utils import get_rusage
from six.moves import queue

from .base_network_worker import BaseNetworkWorker

try:
    from futures import ThreadPoolExecutor
except ImportError:
    from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')


class Network(BaseNetworkWorker):

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
