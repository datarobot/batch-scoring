import logging
import requests
import six

if six.PY2:
    from futures import ThreadPoolExecutor
else:
    from concurrent.futures import ThreadPoolExecutor


logger = logging.getLogger(__name__)


class Network(object):

    def __init__(self, concurrency, timeout):
        self._executor = ThreadPoolExecutor(concurrency)
        self._timeout = timeout

    def exception_handler(request, *args):
        response = getattr(request, 'response', None)
        exc = args[0] if len(args) else None
        if exc:
            logger.warning('Exception: {} {}'.format(exc, type(exc)))
        else:
            logger.warn('Request failed -- retrying')

        if response is None:
            response = FakeResponse(400, 'No Response')

        callback = request.kwargs['hooks']['response']

        callback(response)

    def _request(self, request):
        try:
            session = requests.Session()
            prepared = session.prepare_request(request)
            response = session.send(prepared, timeout=self._timeout)
        except Exception as exc:
            logger.warning('Exception {}: {}'.format(type(exc), exc))
            callback = request.kwargs['hooks']['response']

            callback(response)

    def perform_requests(self, requests, exception_handler):
        fut = self._executor.submit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=False)
