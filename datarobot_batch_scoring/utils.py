from functools import partial
import getpass
from os.path import expanduser, isfile, join as path_join
from os import getcwd
from time import time
import codecs
import io
import gzip
import csv
import logging
import os
import requests
import six
import sys
import trafaret as t
from six.moves.configparser import ConfigParser
import chardet

if six.PY2:
    import StringIO

OptKey = partial(t.Key, optional=True)

input = six.moves.input

CONFIG_FILENAME = 'batch_scoring.ini'


def verify_objectid(id_):
    """Verify if id_ is a proper ObjectId. """
    if not len(id_) == 24:
        raise ValueError('id {} not a valid project/model id'.format(id_))


config_validator = t.Dict({
    OptKey('host'): t.String,
    OptKey('project_id'): t.contrib.object_id.MongoId >> str,
    OptKey('model_id'): t.contrib.object_id.MongoId >> str,
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
            fs = '%(asctime)-15s [%(levelname)s] %(message)s'
            hdlr = logging.FileHandler(self.root_logger_filename, 'w+')
        elif stdout:
            fs = '[%(levelname)s] %(message)s'
            hdlr = logging.StreamHandler(sys.stdout)
        else:
            fs = '[%(levelname)s] %(message)s'
            hdlr = logging.StreamHandler()
        dfs = None
        fmt = logging.Formatter(fs, dfs)
        hdlr.setFormatter(fmt)
        logger.setLevel(level)
        logger.addHandler(hdlr)

        # root logger
        if stdout is False and self.file_name_suffix == 'main':
            fs = '%(asctime)-15s [%(levelname)s] %(message)s'
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
        os._exit(1)

    def close(self):
        for l in [logger, root_logger]:
            handlers = l.handlers[:]
            for h in handlers:
                if isinstance(h, logging.FileHandler):
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
        """
        # if self.get_file_name('main') is not self.root_logger_filename:
        #     raise SystemError('set_next_UI_name() should not be called in '
        #                       'non-windows environments.')
        if self.file_name_suffix != 'main':
            #  For now let's only init new UI's from the main process
            self.error('set_next_UI_name() called by "{}" UI instance. This '
                       'should only be called by "main".'
                       ''.format(self.file_name_suffix))
        self.log_files.append(self.get_file_name(suffix))
        self._next_suffix = suffix

    def __getstate__(self):
        """
        On windows we need to pickle the UI instances or create new
        instances inside the subprocesses since there's no fork.
        """
        if os.name is not 'nt':
            raise SystemError('__getstate__() should not be called in '
                              'non-windows environments.')
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


class Recoder:
    """
    Iterator that reads an encoded stream and decodes the input to UTF-8
    for Python 2. In Python 3 the open function decodes the file.
    """
    def __init__(self, f, encoding):
        f.seek(0)
        if six.PY3:
            self.reader = f
        if six.PY2:
            self.reader = codecs.StreamRecoder(f,
                                               codecs.getencoder('utf-8'),
                                               codecs.getdecoder('utf-8'),
                                               codecs.getreader(encoding),
                                               codecs.getwriter(encoding))

    def __iter__(self):
        return self

    def next(self):   # python 3
        return self.reader.next()

    def __next__(self):  # python 2
        return self.reader.__next__()


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


def iter_chunks(csvfile, chunk_size):
    chunk = []
    for row in csvfile:
        chunk.append(row)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


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


def investigate_encoding_and_dialect(dataset, sep, ui):
    """Try to identify encoding and dialect.
    Providing a delimiter may help with smaller datasets.
    Running this is costly so run it once per dataset."""
    t0 = time()
    if dataset.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open
    with opener(dataset, 'rb') as dfile:
        sample = dfile.read(2*1024**2)
    chardet_result = chardet.detect(sample)
    encoding = chardet_result['encoding'].lower()
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample.decode(encoding), delimiters=sep)
    except csv.Error:
        if len(sample) < 10:
            ui.fatal('Input file "%s" is less than 10 chars long '
                     'and this is the possible cause of a csv.Error.'
                     ' Check the file and try again.' % dataset)
        elif sep is not None:
            ui.fatal('The csv module failed to detect the CSV '
                     'dialect. Check that you provided the correct '
                     'delimiter, or try the script without the '
                     '--delimiter flag.')
        else:
            ui.fatal('The csv module failed to detect the CSV '
                     'dialect. Try giving hints with the '
                     '--delimiter argument, E.g  '
                     """--delimiter=','""")
        raise
    #  in Python 2, csv.dialect sometimes returns unicode which the
    #  PY2 csv.reader cannot handle. This may be from the Recoder
    if six.PY2:
        dialect.lineterminator = str(dialect.lineterminator)
        for a in ['delimiter', 'lineterminator', 'quotechar']:
            if isinstance(getattr(dialect, a, None), type(u'')):
                recast = str(getattr(dialect, a))
                setattr(dialect, a, recast)
    csv.register_dialect('dataset_dialect', dialect)
    #  the csv writer should use the systems newline char
    csv.register_dialect('writer_dialect', dialect,
                         lineterminator=os.linesep)
    ui.info('investigate_encoding_and_dialect - total time seconds -'
            ' {}'.format(time() - t0))
    ui.debug('investigate_encoding_and_dialect - encoding detected -'
             ' {}'.format(encoding))
    ui.debug('investigate_encoding_and_dialect - vars(dialect) - {}'
             ''.format(vars(dialect)))
    return encoding


def auto_sampler(dataset, encoding, ui):
    """
    Automatically find an appropriate number of rows to send per batch based
    on the average row size.
    :return:
    """

    t0 = time()

    sample_size = int(0.5 * 1024 ** 2)
    if dataset.endswith('.gz'):
        opener = gzip.open
    else:
        opener = open
    with opener(dataset, 'rb') as dfile:
        sample = dfile.read(sample_size)
    ingestable_sample = sample.decode(encoding)
    size_bytes = sys.getsizeof(ingestable_sample.encode('utf-8'))

    if size_bytes < (sample_size * 0.75):
        #  if dataset is tiny, don't bother auto sampling.
        ui.info('auto_sampler: total time seconds - {}'.format(time() - t0))
        ui.info('auto_sampler: defaulting to 500 samples for small dataset')
        return 500

    if six.PY3:
        buf = io.StringIO()
        buf.write(ingestable_sample)
    else:
        buf = StringIO.StringIO()
        buf.write(sample)
    buf.seek(0)
    file_lines, csv_lines = 0, 0
    dialect = csv.get_dialect('dataset_dialect')
    fd = Recoder(buf, encoding)
    reader = csv.reader(fd, dialect=dialect, delimiter=dialect.delimiter)
    line_pos = []
    for _ in buf:
        file_lines += 1
        line_pos.append(buf.tell())
    #  remove the last line since it's probably not fully formed
    buf.truncate(line_pos[-2])
    buf.seek(0)
    file_lines -= 1
    try:
        for _ in reader:
            csv_lines += 1
    except csv.Error:
        if buf.tell() in line_pos[-3:]:
            ui.debug('auto_sampler: caught csv.Error at end of sample. '
                     'seek_position: {}, csv_line: {}'.format(buf.tell(),
                                                              line_pos))
        else:
            ui.fatal('--auto_sample failed to parse the csv file. Try again '
                     'without --auto_sample. seek_position: {}, '
                     'csv_line: {}'.format(buf.tell(), line_pos))
            raise
    else:
        ui.debug('auto_sampler: analyzed {} csv rows'.format(csv_lines))

    buf.close()
    avg_line = int(size_bytes / csv_lines)
    chunk_size_goal = int(1.5 * 1024 ** 2)  # size we want per batch
    lines_per_sample = int(chunk_size_goal / avg_line) + 1
    ui.debug('auto_sampler: lines counted: {},  avgerage line size: {}, '
             'recommended lines per sample: {}'.format(csv_lines, avg_line,
                                                       lines_per_sample))
    ui.info('auto_sampler: total time seconds - {}'.format(time() - t0))

    return lines_per_sample
