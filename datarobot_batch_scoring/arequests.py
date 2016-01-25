import datetime
import asyncio
import aiohttp
import json
import logging


logger = logging.getLogger('main')
root_logger = logging.getLogger()

TIMEOUT = 5.5

r_counter = 0
sem = None


def _map(requests, loop, *, size=4, exception_handler=None):
    """
    requests should be list of tuples where first element
    is a prepared aiohttp.request couroutine and second is
    callback_handler function that we'll be have handling logic
    after we'll have response.
    :requests: list of tuples.
        [(<generator object some at 0x10156d9d8>, <callback_handler>), ...]
    """
    tasks = []
    it = iter(requests)
    connector = aiohttp.TCPConnector(force_close=True,
                                     verify_ssl=False,
                                     loop=loop)
    with aiohttp.ClientSession(connector=connector) as session:
        for i in range(size):
            task = asyncio.async(fetcher(it, session, loop, exception_handler),
                                 loop=loop)
            tasks.append(task)
        yield from asyncio.gather(*tasks)


def imap(requests, size=4, exception_handler=None):
    """
    proxy function for _map
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)
    loop.run_until_complete(_map(requests,
                                 loop,
                                 size=size,
                                 exception_handler=exception_handler))
    loop.close()
    return range(r_counter)


class patched_response(object):
    status_code = None
    elapsed = None
    text = ''

    def __init__(self, status_code, raw, elapsed):
        self.status_code = status_code
        self.text = raw
        self.elapsed = elapsed

    def json(self):
        return json.loads(self.text)


@asyncio.coroutine
def fetcher(requests, session, loop, exception_handler=None):
    while True:
        try:
            try:
                request = next(requests)
            except StopIteration:
                return
            timeout = request[0].pop('timeout', TIMEOUT)
            yield from asyncio.wait_for(fetch_data(request, session),
                                        timeout=timeout, loop=loop)
        except Exception as exc:
            logger.debug('Exception {}'.format(type(exc)))
            if exception_handler:
                exception_handler(request, exc)
            else:
                logger.fatal('{}'.format(exc))
                root_logger.error(exc, exc_info=True)


@asyncio.coroutine
def fetch_data(request, session):
    """
    :request: tuple of dict of configuration for request
              and callback_handler function
    """
    request_data, callback_handler = request
    # drop response key in case of requeued request
    request_data.pop('response', None)

    st = datetime.datetime.now()
    r = yield from session.request(**request_data)
    raw = yield from r.read()
    raw = raw.decode('utf-8')
    rq = patched_response(r.status, raw,
                          (datetime.datetime.now() - st))
    request_data['response'] = rq
    callback_handler(rq)
