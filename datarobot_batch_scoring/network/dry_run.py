import collections
import logging
import signal

from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import wait
from datarobot_batch_scoring.consts import (SENTINEL)
from six.moves import queue

from .base_network_worker import BaseNetworkWorker

logger = logging.getLogger(__name__)


FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')


class DryRunNetworkWorker(BaseNetworkWorker):
    """A worker that will drain the network_queue, but doesn't actually send
    any requests or put anything into the writer_queue
    """

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

    def run(self):
        i = 0
        for _ in self.perform_requests():
            i += 1
        return i

    def go(self):
        return self.run()
