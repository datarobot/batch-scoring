import gzip
import json
import threading
import time
import os
import os.path
import traceback
from glob import glob
import socket
import pytest
import io

import sys

try:
    from urllib2 import urlopen, HTTPError
except ImportError:
    from urllib.request import urlopen, HTTPError
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
        self._thread = None

    def start(self):
        """Start application in a separate process."""
        def worker(app, port):
            return app.run(port=port, use_reloader=False, threaded=True)
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
    files = glob('*shelve*')
    for file in files:
        os.unlink(file)
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
    #           LID                         FILE
    MAPPING = {
        '56dd9570018e213242dfa93d': 'tests/fixtures/temperatura.json',
        '56dd9570018e213242dfa93e': 'tests/fixtures/regression.json',
        '56dd9570018e213242dfa93f': 'tests/fixtures/temperatura_api_v1.json',
        '5a29097f962d7465d1a81946': 'tests/fixtures/iris.json',
        '098fa761405d1c9d8a5ea71dc0f3d2bb5ce898b5': 'tests/fixtures/iris.json',
        '0ec5bcea7f0f45918fa88257bfe42c09': 'tests/fixtures/regression.json',
        None: 'tests/fixtures/temperatura.json'}

    app = flask.Flask(__name__)
    app.config['SECRET_KEY'] = '42'
    app.config['PREDICTION_DELAY'] = 0
    app.config['FAIL_AT'] = []
    app.config['DELAY_AT'] = []
    app.config['FAIL_GRACEFULLY_AT'] = []

    app.request_number = 0

    @app.route('/ping')
    def ping():
        return '{"ping": "pong"}'

    @app.route('/shutdown')
    def shutdown():
        func = flask.request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        func()

    @app.errorhandler(500)
    def error_handler(error):
        exc_type, exc_value, exc_traceback = sys.exc_info()
        return ''.join(traceback.format_exception(exc_type, exc_value,
                                                  exc_traceback)), 500

    @app.after_request
    def after_request(response):
        response.headers['X-Datarobot-Execution-Time'] = 0.03
        return response

    @app.route('/predApi/v1.0/api_token')
    @app.route('/api/v1/api_token')
    def auth():
        auth = flask.request.authorization
        if not auth:
            flask.abort(401)
        if auth.username == 'bad_status':
            flask.abort(500)
        if auth.username == 'no_token1':
            return '{"api_token": null}'
        if auth.username == 'no_token2':
            return '{}'
        if auth.username != 'username' or auth.password != 'password':
            flask.abort(401)
        else:
            return '{"api_token": "Som3tok3n"}'

    @app.route('/predApi/v1.0/api_token', methods=['POST'])
    @app.route('/api/v1/api_token', methods=['POST'])
    def post_auth():
        auth = flask.request.authorization
        if not auth:
            flask.abort(401)
        if auth.username != 'username' or auth.password != 'password':
            flask.abort(401)
        else:
            return '{"api_token": "Som3tok3n"}'

    @app.route('/predApi/v1.0/<pid>/<lid>/predict', methods=["POST"])
    @app.route('/api/v1/<pid>/<lid>/predict', methods=["POST"])
    def predict_sinc(pid, lid):
        return _predict(lid)

    @app.route('/predApi/v1.0/<import_id>/predict', methods=["POST"])
    @app.route('/api/v1/<import_id>/predict', methods=["POST"])
    def predict_transferable(import_id):
        return _predict(import_id)

    def _predict(uid):
        body = flask.request.data

        if flask.request.content_encoding == "gzip":
            str_io = io.BytesIO(body)
            with gzip.GzipFile(fileobj=str_io, mode='rb') as f:
                body = f.read()

        body = body.decode('utf-8').splitlines()
        rows_number = len(body) - 1

        if app.config["PREDICTION_DELAY"]:
            time.sleep(app.config["PREDICTION_DELAY"])

        app.request_number += 1
        if app.config["FAIL_AT"]:
            if app.request_number in app.config["FAIL_AT"]:
                raise RuntimeError("Requested failure")

        if app.config["FAIL_GRACEFULLY_AT"]:
            if app.request_number in app.config["FAIL_GRACEFULLY_AT"]:
                return json.dumps({"message": "Requested failure"}), 400

        if app.config["DELAY_AT"]:
            if app.request_number in app.config["DELAY_AT"]:
                time.sleep(app.config["DELAY_AT"][app.request_number])

        try:
            # for files with row_id as first column
            first_row = int(body[1].split(',')[0]) % 10
        except ValueError:
            first_row = 0

        def preprocess_response(response_data, pred_key):
            std_predictions = response_data[pred_key]
            response_data[pred_key] = []
            for i in range(rows_number):
                row = std_predictions[(i + first_row) % 10].copy()
                row["rowId"] = i
                response_data[pred_key].append(row)
            return response_data

        with open(MAPPING.get(uid), 'r') as f:
            resp_data = json.load(f)
            pred_key = 'data' if 'data' in resp_data else 'predictions'
            return json.dumps(
                preprocess_response(resp_data, pred_key)).encode('utf-8')

    return app
