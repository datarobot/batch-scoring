import collections
import logging
import requests

try:
    from futures import ThreadPoolExecutor
except ImportError:
    from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


FakeResponse = collections.namedtuple('FakeResponse', 'status_code, text')


class Network(object):

    def __init__(self, concurrency, timeout):
        self._executor = ThreadPoolExecutor(concurrency)
        self._timeout = timeout

    def _request(self, request):
        try:
            session = requests.Session()
            prepared = session.prepare_request(request)
            response = session.send(prepared, timeout=self._timeout)
        except Exception as exc:
            logger.warning('Exception {}: {}'.format(type(exc), exc))
            try:
                callback = request.kwargs['hooks']['response']
            except AttributeError:
                callback = request.hooks['response'][0]
            response = FakeResponse(400, 'No Response')
            callback(response)

    def perform_requests(self, requests):
        return self._executor.map(self._request, requests)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=False)
