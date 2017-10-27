import csv
import getpass
import io
import logging
import os
import sys
from collections import namedtuple
from functools import partial
from gzip import GzipFile
from os import getcwd
from os.path import expanduser, isfile, join as path_join

import requests
import six
import trafaret as t
from six.moves.configparser import ConfigParser
from six.moves import input

if six.PY2:
    from urlparse import urlparse
elif six.PY3:
    from urllib.parse import urlparse

OptKey = partial(t.Key, optional=True)


CONFIG_FILENAME = 'batch_scoring.ini'


def verify_objectid(value):
    """Verify if id_ is a proper ObjectId. """
    try:
        t.String(regex='^[A-Fa-f0-9]{24}$').check(value)
    except t.DataError:
        raise ValueError('id {} not a valid project/model id'.format(value))


config_validator = t.Dict({
    OptKey('host'): t.String,
    OptKey('project_id'): t.String(regex='^[A-Fa-f0-9]{24}$'),
    OptKey('model_id'): t.String(regex='^[A-Fa-f0-9]{24}$'),
    OptKey('import_id'): t.String,
    OptKey('n_retry'): t.Int,
    OptKey('keep_cols'): t.String,
    OptKey('n_concurrent'): t.Int,
    OptKey('dataset'): t.String,
    OptKey('n_samples'): t.Int,
    OptKey('delimiter'): t.String,
    OptKey('out'): t.String,
    OptKey('user'): t.String,
    OptKey('password'): t.String,
    OptKey('datarobot_key'): t.String,
    OptKey('timeout'): t.Int,
    OptKey('api_token'): t.String,
    OptKey('create_api_token'): t.String,
    OptKey('pred_name'): t.String,
    OptKey('skip_row_id'): t.StrBool,
    OptKey('output_delimiter'): t.String,
    OptKey('field_size_limit'): t.Int,
    OptKey('ca_bundle'): t.String,
    OptKey('no_verify_ssl'): t.StrBool,
}).allow_extra('*')


logger = logging.getLogger('main')
root_logger = logging.getLogger()


class UI(object):

    def __init__(self, prompt, loglevel, stdout, file_name_suffix='main'):
        self._prompt = prompt
        self.loglevel = loglevel
        self.stdout = stdout
        self.log_files = []
        self.file_name_suffix = file_name_suffix
        self._configure_logging(loglevel, stdout)

    def _configure_logging(self, level, stdout):
        """Configures logging for user and debug logging. """
        self.root_logger_filename = self.get_file_name(self.file_name_suffix)
        if isfile(self.root_logger_filename):
            os.unlink(self.root_logger_filename)
        if self.file_name_suffix is 'main':
            self.log_files.append(str(self.root_logger_filename))

        # user logger
        if self.file_name_suffix != 'main':
            fs = '%(processName)s %(asctime)-15s [%(levelname)s] %(message)s'
            hdlr = logging.FileHandler(self.root_logger_filename, 'w+')
        elif stdout:
            fs = '%(processName)s [%(levelname)s] %(message)s'
            hdlr = logging.StreamHandler(sys.stdout)
        else:
            fs = '%(processName)s [%(levelname)s] %(message)s'
            hdlr = logging.StreamHandler()
        dfs = None
        fmt = logging.Formatter(fs, dfs)
        hdlr.setFormatter(fmt)
        logger.setLevel(level)
        logger.addHandler(hdlr)

        # root logger
        if stdout is False and self.file_name_suffix == 'main':
            fs = '%(processName)s %(asctime)-15s [%(levelname)s] %(message)s'
            hdlr = logging.FileHandler(self.root_logger_filename, 'w+')
            dfs = None
            fmt = logging.Formatter(fs, dfs)
            hdlr.setFormatter(fmt)
            root_logger.setLevel(logging.DEBUG)
            root_logger.addHandler(hdlr)

    def prompt_yesno(self, msg):
        if self._prompt is not None:
            return self._prompt
        cmd = input('{} (Yes/No)> '.format(msg)).strip().lower()
        while cmd not in ('yes', 'no', 'y', 'n'):
            cmd = input('Please type (Yes/No)> ').strip().lower()
        return cmd in ('yes', 'y')

    def prompt_user(self):
        return input('user name> ').strip()

    def debug(self, msg):
        logger.debug(msg)

    def info(self, msg):
        logger.info(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        if sys.exc_info()[0]:
            exc_info = True
        else:
            exc_info = False
        if self.file_name_suffix != 'main':
            logger.error(msg, exc_info=exc_info)
        elif self.stdout:
            logger.error(msg, exc_info=exc_info)
        else:
            logger.error(msg)
            root_logger.error(msg, exc_info=exc_info)

    def fatal(self, msg):
        exc_info = sys.exc_info()
        if self.file_name_suffix != 'main':
            msg = ('{}\nIf you need assistance please send the log file/s:\n'
                   '{}to support@datarobot.com.').format(
                           msg, self.get_all_logfiles())
            logger.error(msg, exc_info=exc_info)
        elif self.stdout:
            msg = ('{}\nIf you need assistance please send the output of this '
                   'script to support@datarobot.com.').format(
                   msg)
            logger.error(msg, exc_info=exc_info)
        else:
            msg = ('{}\nIf you need assistance please send the log file/s:\n'
                   '{}to support@datarobot.com.').format(
                           msg, self.get_all_logfiles())
            logger.error(msg)
            root_logger.error(msg, exc_info=exc_info)
        self.close()
        sys.exit(1)

    def close(self):
        for l in [logger, root_logger]:
            handlers = l.handlers[:]
            for h in handlers:
                if hasattr(h, 'close'):
                    h.close()
                l.removeHandler(h)
            if hasattr(l, 'shutdown'):
                l.shutdown()

    def get_file_name(self, suffix):
        return os.path.join(os.getcwd(), 'datarobot_batch_scoring_{}.log'
                                         ''.format(suffix))

    def get_all_logfiles(self):
        file_names = ''
        for logfile in self.log_files:
            file_names += '\t{}\n'.format(logfile)
        return file_names

    def set_next_UI_name(self, suffix):
        """
        On the Windows platform we want to init a new UI inside each subproc
        This allows us to set the name new log file name after pickling
        On *NIX we can just write to a single file
        """
        if self.file_name_suffix != 'main':
            #  For now let's only init new UI's from the main process
            self.error('set_next_UI_name() called by "{}" UI instance. This '
                       'should only be called by "main".'
                       ''.format(self.file_name_suffix))
        if os.name is 'nt':
            self._next_suffix = suffix
            self.log_files.append(self.get_file_name(suffix))
        else:
            self._next_suffix = 'main'
            self.log_files.append(self.get_file_name(self._next_suffix))
            self.log_files = list(set(self.log_files))  # dedupe list

    def __getstate__(self):
        """
        On windows we need to pickle the UI instances or create new
        instances inside the subprocesses since there's no fork.
        """
        d = self.__dict__.copy()
        #  replace the old file suffix with a separate log file
        d['file_name_suffix'] = self._next_suffix
        #  remove args not needed for __new__
        for k in [i for i in d.keys()]:
            if k not in ['_prompt', 'loglevel', 'stdout', 'file_name_suffix']:
                del d[k]
        return d

    def __setstate__(self, d):
        """
        On windows we need to pickle the UI instances or create new
        instances inside the subprocesses since there's no fork.
        This method is called when unpickling a UI instance.
        It actually creates a new UI that logs to a separate file.
        """
        if os.name is not 'nt':
            raise SystemError('__getstate__() should not be called in '
                              'non-windows environments.')
        self.__dict__.update(d)
        self._configure_logging(self.loglevel, self.stdout)

    def getpass(self):
        if self._prompt is not None:
            raise RuntimeError("Non-interactive session")
        return getpass.getpass('password> ')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def get_config_file():
    """
    Lookup for config file at user home directory or working directory.
    Returns
    -------
    str or None
        Path to config file or None if it not exists.
    """
    home_path = path_join(expanduser('~'), CONFIG_FILENAME)
    cwd_path = path_join(getcwd(), CONFIG_FILENAME)
    if isfile(home_path):
        return home_path
    elif isfile(cwd_path):
        return cwd_path
    return None


def parse_config_file(file_path):
    config = ConfigParser()
    config.read(file_path)
    if 'batch_scoring' not in config.sections():
        #  We are return empty dict, because there is nothing in this file
        #  that related to arguments to batch scoring.
        return {}
    parsed_dict = dict(config.items('batch_scoring'))
    return config_validator(parsed_dict)


def acquire_api_token(base_url, base_headers, user, pwd, create_api_token, ui):
    """Get the api token.

    Either supplied by user or requested from the API with username and pwd.
    Optionally, create a new one.
    """

    auth = (user, pwd)

    if create_api_token:
        request_meth = requests.post
    else:
        request_meth = requests.get

    r = request_meth(base_url + 'api_token', auth=auth, headers=base_headers)
    if r.status_code == 401:
        raise ValueError('wrong credentials')
    elif r.status_code != 200:
        raise ValueError('api_token request returned status code {}'
                         .format(r.status_code))
    else:
        ui.info('api-token acquired')

    api_token = r.json().get('api_token')

    if api_token is None:
        raise ValueError('no api-token registered; '
                         'please run with --create_api_token flag.')

    ui.debug('api-token: {}'.format(api_token))

    return api_token


def parse_host(host, ui):
    parsed = urlparse(host)
    ui.debug('urlparse.urlparse result: {}'.format(parsed))
    if not parsed.scheme and not parsed.netloc:
        #  protocol missing
        ui.fatal('Cannot parse "--host" argument. Host address must start '
                 'with a protocol such as "http://" or "https://". '
                 'Value given: {}'.format(host))
    base_url = '{}://{}/predApi/v1.0/'.format(parsed.scheme,
                                              parsed.netloc)
    ui.debug('parse_host return value: {}'.format(base_url))
    return base_url


def compress(data):
    buf = io.BytesIO()
    with GzipFile(fileobj=buf, mode='wb', compresslevel=2) as f:
        f.write(data)
    return buf.getvalue()


def warn_if_redirected(req, ui):
    """
    test whether a request was redirect.
    Log a warning to the user if it was redirected
    """
    history = req.history
    if history:
        first = history[0]
        if first.is_redirect:
            starting_endpoint = first.url  # Requested url
            redirect_endpoint = first.headers.get('Location')  # redirect
            if str(starting_endpoint) != str(redirect_endpoint):
                ui.warning('The requested URL:\n\t{}\n\twas redirected '
                           'by the webserver to:\n\t{}'
                           ''.format(starting_endpoint, redirect_endpoint))


def get_endpoint(host, api_version):
    parsed = urlparse(host)
    if not parsed.scheme and not parsed.netloc:
        raise ValueError(
            'Cannot parse "--host" argument. Host address must start '
            'with a protocol such as "http://" or "https://". '
            'Value given: {}'.format(host))

    return '{}://{}/{}/'.format(parsed.scheme, parsed.netloc, api_version)


def make_validation_call(user, api_token, n_retry, endpoint, base_headers,
                         batch, ui, compression=None, verify_ssl=True):
    """Check if user is authorized for the given model and that schema is
    correct.

    This function will make a sync request to the api endpoint with a single
    row just to make sure that the schema is correct and the user
    is authorized.
    """
    r = None

    while n_retry:
        ui.debug('request authorization')
        if compression:
            data = compress(batch.data)
        else:
            data = batch.data
        try:
            if user and api_token:
                r = requests.post(endpoint, headers=base_headers,
                                  data=data,
                                  auth=(user, api_token),
                                  verify=verify_ssl)
            elif not (user and api_token):
                r = requests.post(endpoint, headers=base_headers,
                                  data=data,
                                  verify=verify_ssl)
            else:
                ui.fatal("Aborting: no auth credentials passed")

            ui.debug('authorization request response: {}|{}'
                     .format(r.status_code, r.text))
            if r.status_code == 200:
                # all good
                break

            warn_if_redirected(r, ui)
            if r.status_code == 400:
                # client error -- maybe schema is wrong
                try:
                    msg = r.json()['message']
                except:
                    msg = r.text
                ui.fatal('failed with client error: {}'.format(msg))
            elif r.status_code == 403:
                #  This is usually a bad API token. E.g.
                #  {"status": "API token not valid", "code": 403}
                ui.fatal('Failed with message:\n\t{}'.format(r.text))
            elif r.status_code == 401:
                #  This can be caused by having the wrong datarobot_key
                ui.fatal('failed to authenticate -- '
                         'please check your: datarobot_key (if required), '
                         'username/password and/or api token. Contact '
                         'customer support if the problem persists '
                         'message:\n{}'
                         ''.format(r.__dict__.get('_content')))
            elif r.status_code == 405:
                ui.fatal('failed to request endpoint -- please check your '
                         '"--host" argument')
            elif r.status_code == 502:
                ui.fatal('problem with the gateway -- please check your '
                         '"--host" argument and contact customer support'
                         'if the problem persists.')
        except requests.exceptions.SSLError as e:
            ui.error('SSL verification failed, reason: {}:'.format(e))
        except requests.exceptions.ConnectionError:
            ui.error('cannot connect to {}'.format(endpoint))
        n_retry -= 1

    if n_retry == 0:
        status = r.text if r is not None else 'UNKNOWN'
        try:
            status = r.json()['message']
        except:
            pass  # fall back to r.text
        content = r.content if r is not None else 'NO CONTENT'
        warn_if_redirected(r, ui)
        ui.debug("Failed authorization response \n{!r}".format(content))
        ui.fatal('authorization failed -- please check project id and model '
                 'id permissions: {}'.format(status))
    else:
        ui.debug('authorization has succeeded')


try:
    import resource

    def get_rusage():
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return {
            "utime": usage.ru_utime,
            "stime": usage.ru_stime,
            "rss": usage.ru_maxrss,
        }

except ImportError:
    def get_rusage():
        return {}


DialectTuple = namedtuple('DialectTuple',
                          'delimiter doublequote escapechar lineterminator '
                          'quotechar quoting skipinitialspace strict')


class SerializableDialect(DialectTuple):
    @classmethod
    def from_dialect(cls, dialect):
        return cls(
            delimiter=dialect.delimiter,
            doublequote=dialect.doublequote,
            escapechar=dialect.escapechar,
            lineterminator=dialect.lineterminator,
            quotechar=dialect.quotechar,
            quoting=dialect.quoting,
            skipinitialspace=dialect.skipinitialspace,
            strict=getattr(dialect, 'strict', False))

    def to_dialect(self):
        class _dialect(csv.Dialect):
            delimiter = self.delimiter
            doublequote = self.doublequote
            escapechar = self.escapechar
            lineterminator = self.lineterminator
            quotechar = self.quotechar
            quoting = self.quoting
            skipinitialspace = self.skipinitialspace
            strict = self.strict
        return _dialect()


class Worker(object):
    state_names = {}

    def __init__(self, status_value):
        self.status_value = status_value

    @property
    def state(self):
        return self.status_value.value

    @state.setter
    def state(self, status):
        self.ui.debug('state: {} -> {}'
                      ''.format(self.state_name(), self.state_name(status)))
        self.status_value.value = status

    def state_name(self, s=None):
        return self.state_names[s or self.state]
