import collections
import logging
import textwrap
from time import sleep
import requests
import requests.adapters

try:
    from futures import ThreadPoolExecutor
except ImportError:
    from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')


class Network(object):

    def __init__(self, concurrency, timeout, ui=None):
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
                    sleep(0.1)
            yield
        #  wait for all batches to finish before returning
        while self.futures:
            f_len = len(self.futures)
            self.futures = [i for i in self.futures if not i.done()]
            if f_len != len(self.futures):
                self._ui.debug('Waiting for final requests to finish. '
                               'remaining requests: {}'
                               ''.format(len(self.futures)))
            sleep(0.1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=False)
