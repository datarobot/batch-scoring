import collections
import logging
import textwrap
import gc

import requests

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
        self.session = requests.Session()
        self._ui = ui or logger

    def _request(self, request):
        prepared = self.session.prepare_request(request)
        try:
            self.session.send(prepared, timeout=self._timeout)
        except requests.exceptions.ReadTimeout:
            self._ui.warning(textwrap.dedent("""The server did not send any data
in the allotted amount of time.
You might want to increase --timeout parameter
or
decrease --n_samples --n_concurrent parameters
"""))

        except Exception as exc:
            self._ui.debug('Exception {}: {}'.format(type(exc), exc))
            try:
                callback = request.kwargs['hooks']['response']
            except AttributeError:
                callback = request.hooks['response'][0]
            response = FakeResponse(400, 'No Response')
            callback(response)
        finally:
            request.data = None
            prepared.data = None
            del request
            del prepared
            gc.collect()

    def perform_requests(self, requests):
        return self._executor.map(self._request, requests)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=False)
