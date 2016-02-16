import time
import multiprocessing
import socket
import pytest

try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen
import flask


class LiveServer(object):
    """The helper class uses to manage live server. Handles creation and
    stopping application in a separate process.
    :param app: The application to run.
    :param port: The port to run application.
    """

    def __init__(self, app, port):
        self.app = app
        self.port = port
        self._process = None

    def start(self):
        """Start application in a separate process."""
        def worker(app, port):
            return app.run(port=port, use_reloader=False)
        self._process = multiprocessing.Process(
            target=worker,
            args=(self.app, self.port)
        )
        self._process.start()

        # We must wait for the server to start listening with a maximum
        # timeout of 5 seconds.
        timeout = 5
        while timeout > 0:
            time.sleep(1)
            try:
                urlopen(self.url())
                timeout = 0
            except:
                timeout -= 1

    def url(self, url=''):
        """Returns the complete url based on server options."""
        return 'http://localhost:%d%s' % (self.port, url)

    def stop(self):
        """Stop application process."""
        if self._process:
            self._process.terminate()

    def __repr__(self):
        return '<LiveServer listening at %s>' % self.url()


def _rewrite_server_name(server_name, new_port):
    """Rewrite server port in ``server_name`` with ``new_port`` value."""
    sep = ':'
    if sep in server_name:
        server_name, port = server_name.split(sep, 1)
    return sep.join((server_name, new_port))


@pytest.yield_fixture(scope='function')
def live_server(app, monkeypatch):
    """Run application in a separate process.
    When the ``live_server`` fixture is applyed, the ``url_for`` function
    works as expected::
        def test_server_is_up_and_running(live_server):
            index_url = url_for('index', _external=True)
            assert index_url == 'http://localhost:5000/'
            res = urllib2.urlopen(index_url)
            assert res.code == 200
    """
    # Bind to an open port
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    port = s.getsockname()[1]
    s.close()

    # Explicitly set application ``SERVER_NAME`` for test suite
    # and restore original value on test teardown.
    server_name = app.config['SERVER_NAME'] or 'localhost'
    monkeypatch.setitem(app.config, 'SERVER_NAME',
                        _rewrite_server_name(server_name, str(port)))

    server = LiveServer(app, port)
    server.start()

    yield server
    server.stop()


@pytest.fixture(scope='function')
def app():
    app = flask.Flask(__name__)
    app.config['SECRET_KEY'] = '42'

    @app.route('/ping')
    def ping():
        return '{"ping": "pong"}'

    @app.route('/api/v1/api_token')
    def auth():
        return '{"api_token": "Som3tok3n"}'

    @app.route('/api/v1/<pid>/<lid>/predict', methods=["POST"])
    def predict(pid, lid):
        with open('tests/fixtures/response.json', 'r') as f:
            return f.read()

    return app
