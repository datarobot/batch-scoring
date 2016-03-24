import threading
import time
import os
import os.path
import socket
import pytest

try:
    from urllib2 import urlopen, HTTPError
except ImportError:
    from urllib.request import urlopen, HTTPError
import flask
from flask import request


class LiveServer(object):
    """The helper class uses to manage live server. Handles creation and
    stopping application in a separate process.
    :param app: The application to run.
    :param port: The port to run application.
    """

    def __init__(self, app, port):
        self.app = app
        self.port = port
        self._thread = None

    def start(self):
        """Start application in a separate process."""
        def worker(app, port):
            return app.run(port=port, use_reloader=False)
        self._thread = threading.Thread(
            target=worker,
            args=(self.app, self.port)
        )
        self._thread.start()

        delay = 0.01
        for i in range(100):
            try:
                urlopen(self.url('/ping'))
                break
            except:
                time.sleep(delay)
                delay *= 2
        else:
            raise RuntimeError("Cannot start flask server")

    def url(self, url=''):
        """Returns the complete url based on server options."""
        return 'http://localhost:%d%s' % (self.port, url)

    def stop(self):
        """Stop application process."""
        if self._thread:
            try:
                urlopen(self.url('/shutdown'))
            except HTTPError:
                pass  # 500 server closed
            self._thread.join()
        self._thread = None

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
    if os.path.exists('.shelve'):
        os.unlink('.shelve')
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
    MAPPING = {'56dd9570018e213242dfa93d': 'tests/fixtures/temperatura.json',
               '56dd9570018e213242dfa93e': 'tests/fixtures/regression.json',
               None: 'tests/fixtures/temperatura.json'}

    app = flask.Flask(__name__)
    app.config['SECRET_KEY'] = '42'

    @app.route('/ping')
    def ping():
        return '{"ping": "pong"}'

    @app.route('/shutdown')
    def shutdown():
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()

    @app.route('/api/v1/api_token')
    def auth():
        return '{"api_token": "Som3tok3n"}'

    @app.route('/api/v1/<pid>/<lid>/predict', methods=["POST"])
    def predict_sinc(pid, lid):
        with open(MAPPING.get(lid), 'r') as f:
            return f.read()

    return app
