# -*- coding: utf-8 -*-
"""
  batch_scoring --host=<host>  --user=<user>
                {--password=<pwd> | --api_token=<api_token>}
                <project_id>  <model_id>  <dataset_filepath>
                [--verbose]  [--keep_cols=<keep_cols>] [--n_samples=<n_samples>]
                [--n_concurrent=<n_concurrent>]
                [--out=<filepath>] [--api_version=<api_version>]
                [—create_api_token] [--n_retry=<n_retry>]
                [--delimiter=<delimiter>] [--resume]
  batch_scoring -h | --help
  batch_scoring --version

Batch score ``dataset`` by submitting prediction requests against ``host``
using model ``model_id``. It will send batches of size ``n_samples``.
Set ``n_samples`` such that the round-trip is roughly 10sec
(see verbose output).
Set ``n_concurrent`` to match the number of cores in the prediction
API endpoint.

The dataset has to be a single CSV file that can be gzipped (extension '.gz').
The output ``out`` will be a single CSV files but remember that records
might be unordered.


Example:

  $ batch_scoring --host https://beta.datarobot.com/api --user="<username>" \
--password="<password>" 5545eb20b4912911244d4835 5545eb71b4912911244d4847 \
~/Downloads/diabetes_test.csv

"""

from __future__ import print_function

import collections
import copy
import csv
import getpass
import glob
import gzip
import json
import logging
import os
import shelve
import sys
import tempfile
import threading
import warnings
from functools import partial
from itertools import filterfalse
from pprint import pformat
from time import time

import requests
import six
import argparse

from . import __version__
from .network import Network
from .utils import prompt_yesno


if six.PY2:
    from contextlib2 import ExitStack
    import dumbdbm  # noqa
elif six.PY3:
    from contextlib import ExitStack
    # for successful py2exe dist package
    from dbm import dumb  # noqa




class ShelveError(Exception):
    pass


Batch = collections.namedtuple('Batch', 'id fieldnames data rty_cnt')
Prediction = collections.namedtuple('Prediction', 'fieldnames data')


class TargetType(object):
    REGRESSION = 'Regression'
    BINARY = 'Binary'


VALID_DELIMITERS = {';', ',', '|', '\t', ' ', '!', '  '}


logger = logging.getLogger('main')
root_logger = logging.getLogger()

with tempfile.NamedTemporaryFile(prefix='datarobot_batch_scoring_',
                                 suffix='.log', delete=False) as fd:
    pass
root_logger_filename = fd.name


def configure_logging(level):
    """Configures logging for user and debug logging. """
    # user logger
    fs = '[%(levelname)s] %(message)s'
    hdlr = logging.StreamHandler()
    dfs = None
    fmt = logging.Formatter(fs, dfs)
    hdlr.setFormatter(fmt)
    logger.setLevel(level)
    logger.addHandler(hdlr)

    # root logger
    fs = '%(asctime)-15s [%(levelname)s] %(message)s'
    hdlr = logging.FileHandler(root_logger_filename, 'w+')
    dfs = None
    fmt = logging.Formatter(fs, dfs)
    hdlr.setFormatter(fmt)
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(hdlr)


def error(msg, exit=True):
    if exit:
        msg = ('{}\nIf you need assistance please send the log \n'
               'file {} to support@datarobot.com .').format(
                   msg, root_logger_filename)
    logger.error(msg)
    if sys.exc_info()[0]:
        exc_info = True
    else:
        exc_info = False
    root_logger.error(msg, exc_info=exc_info)
    if exit:
        sys.exit(1)


def acquire_api_token(base_url, base_headers, user, pwd, create_api_token):
    """Get the api token.

    Either supplied by user or requested from the API with username and pwd.
    Optionally, create a new one.
    """

    auth = (user, pwd)

    request_meth = requests.get
    if create_api_token:
        request_meth = requests.post

    r = request_meth(base_url + 'api_token', auth=auth, headers=base_headers)
    if r.status_code == 401:
        raise ValueError('wrong credentials')
    elif r.status_code != 200:
        raise ValueError('api_token request returned status code {}'
                         .format(r.status_code))
    else:
        logger.info('api-token acquired')

    api_token = r.json()['api_token']

    if api_token is None:
        raise ValueError('no api-token registered; '
                         'please run with --create_api_token flag.')

    logger.debug('api-token: {}'.format(api_token))

    return api_token


def verify_objectid(id_):
    """Verify if id_ is a proper ObjectId. """
    if not len(id_) == 24:
        raise ValueError('id {} not a valid project/model id'.format(id_))


def iter_chunks(csvfile, chunk_size):
    chunk = []
    for line in csvfile:
        chunk.append(line)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class BatchGenerator(object):
    """Class to chunk a large csv files into a stream
    of batches of size ``--n_samples``.

    Yields
    ------
    batch : Batch
        The next batch
    """

    def __init__(self, dataset, n_samples, n_retry, delimiter):
        self.dataset = dataset
        self.chunksize = n_samples
        self.rty_cnt = n_retry
        self.sep = delimiter

    def open(self):
        if self.dataset.endswith('.gz'):
            return gzip.open(self.dataset)
        else:
            if six.PY2:
                return open(self.dataset, 'rb')
            else:
                return open(self.dataset, newline='')

    def __iter__(self):
        rows_read = 0
        sep = self.sep

        # handle unix tabs

        with self.open() as csvfile:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(csvfile.read(1024))

        if sep is not None:
            if sep not in VALID_DELIMITERS:
                raise ValueError('Delimiter "{}" is not a valid delimiter.'
                                 .format(sep))

            # if fixed sep check if we have at least one occurrence.
            with self.open() as fd:
                header = fd.readline()
                if not header.strip():
                    raise ValueError("Input file '{}' is empty."
                                     .format(self.dataset))
                if len(header.split(sep)) == 1:
                    raise ValueError(
                        ("Delimiter '{}' not found. "
                         "Please check your input file "
                         "or consider the flag `--delimiter=''`.").format(sep))
        if sep is None:
            sep = dialect.delimiter
        elif sep == '\\t':
            # NOTE: on bash you have to use Ctrl-V + TAB
            sep = '\t'

        csvfile = self.open()
        reader = csv.reader(csvfile, dialect, delimiter=sep)
        reader.fieldnames = [c.strip() for c in reader.fieldnames]

        for batch_num, chunk in enumerate(iter_chunks(csvfile,
                                                      self.chunksize)):
            if batch_num == 0:
                root_logger.debug('input head: %r', pformat(chunk[:2]))

            yield Batch(rows_read, reader.fieldnames, chunk, self.rty_cnt)
            rows_read += len(chunk)
        else:
            raise ValueError("Input file '{}' is empty.".format(self.dataset))


def peek_row(dataset, delimiter):
    """Peeks at the first row in `dataset`. """
    batches = BatchGenerator(dataset, 1, 1, delimiter)
    try:
        row = next(iter(batches))
    except StopIteration:
        raise ValueError('Cannot peek first row from {}'.format(dataset))
    return row.df


class GeneratorBackedQueue(object):
    """A queue that is backed by a generator.

    When the queue is exhausted it repopulates from the generator.
    """

    def __init__(self, gen):
        self.gen = gen
        self.n_consumed = 0
        self.deque = collections.deque()
        self.lock = threading.RLock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            if len(self.deque):
                return self.deque.popleft()
            else:
                out = next(self.gen)
                self.n_consumed += 1
                return out

    def next(self):
        return self.__next__()

    def push(self, batch):
        # we retry a batch - decrement retry counter
        with self.lock:
            batch = batch._replace(rty_cnt=batch.rty_cnt - 1)
            self.deque.append(batch)

    def has_next(self):
        with self.lock:
            try:
                item = self.next()
                self.push(item)
                return True
            except StopIteration:
                return False


def unique_everseen(iterable, key=None):
    "List unique elements, preserving order. Remember all elements ever seen."
    # unique_everseen('AAAABBBCCDAABBB') --> A B C D
    # unique_everseen('ABBCcAD', str.lower) --> A B C D
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in filterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element


def dataframe_from_predictions(result, pred_name):
    """Convert DR prediction api v1 into dataframe.

    Returns
    -------
    pred : DataFrame
         A dataframe that holds one prediction per row;
         as many columns as class labels.
         Class labels are ordered in lexical order (asc).
    """
    predictions = result['predictions']
    if result['task'] == TargetType.BINARY:
        sorted_classes = unique_everseen()pd.np.unique(pred.columns.tolist())
        pred = pd.DataFrame([p['class_probabilities'] for p in
                             sorted(predictions, key=lambda p: p['row_id'])])
        pred = pred[sorted_classes]
    elif result['task'] == TargetType.REGRESSION:
        pred = pd.DataFrame({pred_name: [p["prediction"] for p in
                             sorted(predictions, key=lambda p: p['row_id'])]})
    else:
        ValueError('task {} not supported'.format(result['task']))

    return pred


def process_successful_request(result, batch, ctx, pred_name):
    """Process a successful request. """
    pred = dataframe_from_predictions(result, pred_name)
    if pred.fieldnames != batch.fieldnames:
        raise ValueError('Shape mismatch {}!={}'.format(
            pred.fieldnames, batch.fieldnames))
    # offset index by batch.id
    pred.index = batch.df.index + batch.id
    pred.index.name = 'row_id'
    ctx.checkpoint_batch(batch, pred)


class WorkUnitGenerator(object):
    """Generates async requests with completion or retry callbacks.

    It uses a queue backed by a batch generator.
    It will pop items for the queue and if its exhausted it will populate the
    queue from the batch generator.
    If a submitted async request was not successfull it gets enqueued again.
    """

    def __init__(self, batches, endpoint, headers, user, api_token,
                 ctx, pred_name):
        self.endpoint = endpoint
        self.headers = headers
        self.user = user
        self.api_token = api_token
        self.ctx = ctx
        self.queue = GeneratorBackedQueue(batches)
        self.pred_name = pred_name

    def _response_callback(self, r, batch=None, *args, **kw):
        try:
            if r.status_code == 200:
                try:
                    result = r.json()
                    exec_time = result['execution_time']
                    logger.debug(('successful response: exec time {:.0f}msec |'
                                  ' round-trip: {:.0f}msec').format(
                                      exec_time,
                                      r.elapsed.total_seconds() * 1000))

                    process_successful_request(result, batch,
                                               self.ctx, self.pred_name)
                except Exception as e:
                    logger.warn('{} response error: {} -- retry'
                                .format(batch.id, e))
                    self.queue.push(batch)
            else:
                try:
                    logger.warn('batch {} failed with status: {}'
                                .format(batch.id,
                                        json.loads(r.text)['status']))
                except ValueError:
                    logger.warn('batch {} failed with status code: {}'
                                .format(batch.id, r.status_code))

                text = r.text
                root_logger.error('batch {} failed status_code:{} text:{}'
                                  .format(batch.id,
                                          r.status_code,
                                          text))
                self.queue.push(batch)
        except Exception as e:
            logger.fatal('batch {} - dropping due to: {}'
                         .format(batch.id, e), exc_info=True)

    def has_next(self):
        return self.queue.has_next()

    def __iter__(self):
        for batch in self.queue:
            # if we exhaused our retries we drop the batch
            if batch.rty_cnt == 0:
                logger.error('batch {} exceeded retry limit; '
                             'we lost {} records'.format(
                                 batch.id, batch.df.shape[0]))
                continue
            # otherwise we make an async request
            data = batch.df.to_csv(encoding='utf8', index=False)
            logger.debug('batch {} transmitting {} bytes'
                         .format(batch.id, len(data)))
            yield requests.Request(
                method='POST',
                url=self.endpoint,
                headers=self.headers,
                data=data,
                auth=(self.user, self.api_token),
                hooks = {'response': partial(self._response_callback,
                                             batch=batch)})


class RunContext(object):
    """A context for a run backed by a persistant store.

    We use a shelve to store the state of the run including
    a journal of processed batches that have been checkpointed.

    Note: we use globs for the shelve files because different
    versions of Python have different file layouts.
    """
    FILENAME = '.shelve'

    def __init__(self, n_samples, out_file, pid, lid, keep_cols,
                 n_retry, delimiter, dataset, pred_name):
        self.n_samples = n_samples
        self.out_file = out_file
        self.project_id = pid
        self.model_id = lid
        self.keep_cols = keep_cols
        self.n_retry = n_retry
        self.delimiter = delimiter
        self.dataset = dataset
        self.pred_name = pred_name
        self.out_stream = None
        self.lock = threading.Lock()

    def __enter__(self):
        self.db = shelve.open(self.FILENAME, writeback=True)
        self.partitions = []
        return self

    def __exit__(self, type, value, traceback):
        self.db.close()
        if self.out_stream is not None:
            self.out_stream.close()
        if type is None:
            # success - remove shelve
            self.clean()

    def checkpoint_batch(self, batch, pred):
        if self.keep_cols and self.first_write:
            # import ipdb; ipdb.set_trace()
            mask = [c in batch.df.columns for c in self.keep_cols]
            if not all(mask):
                error('keep_cols "{}" not in columns {}.'.format(
                    self.keep_cols[mask.index(False)], batch.df.columns))

        if self.keep_cols:
            # stack columns
            ddf = batch.df[self.keep_cols]
            ddf.index = pred.index
            comb = pd.concat((ddf, pred), axis=1)
            assert comb.shape[0] == ddf.shape[0]
        else:
            comb = pred
        with self.lock:
            # if an error happends during/after the append we
            # might end up with inconsistent state
            # TODO write partition files instead of appending
            #  store checksum of each partition and back-check
            comb.to_csv(self.out_stream, mode='aU', header=self.first_write)
            self.out_stream.flush()

            self.db['checkpoints'].append(batch.id)

            if self.first_write:
                self.db['first_write'] = False
            self.first_write = False
            logger.info('batch {} checkpointed'.format(batch.id))
            self.db.sync()

    def batch_generator(self):
        return iter(BatchGenerator(self.dataset, self.n_samples,
                                   self.n_retry, self.delimiter))

    @classmethod
    def exists(cls):
        """Does shelve exist. """
        return any(glob.glob(cls.FILENAME + '*'))

    @classmethod
    def clean(cls):
        """Clean the shelve. """
        for fname in glob.glob(cls.FILENAME + '*'):
            os.remove(fname)


class NewRunContext(RunContext):
    """RunContext for a new run.

    It creates a shelve file and adds a checkpoint journal.
    """

    def __enter__(self):
        if self.exists():
            logger.info('Removing old run shelve')
            self.clean()
        if os.path.exists(self.out_file):
            logger.warn('File {} exists.'.format(self.out_file))
            rm = prompt_yesno('Do you want to remove {}'.format(self.out_file))
            if rm:
                os.remove(self.out_file)
            else:
                sys.exit(0)

        super(NewRunContext, self).__enter__()

        self.db['n_samples'] = self.n_samples
        self.db['project_id'] = self.project_id
        self.db['model_id'] = self.model_id
        self.db['keep_cols'] = self.keep_cols
        # list of batch ids that have been processed
        self.db['checkpoints'] = []
        # used to check if output file is dirty (ie first write op)
        self.db['first_write'] = True
        self.first_write = True
        self.db.sync()

        self.out_stream = open(self.out_file, 'w+')
        return self

    def __exit__(self, type, value, traceback):
        super(NewRunContext, self).__exit__(type, value, traceback)


class OldRunContext(RunContext):
    """RunContext for a resume run.

    It requires a shelve file and plays back the checkpoint journal.
    Checks if inputs are consistent.

    TODO: add md5sum of dataset otherwise they might
    use a different file for resume.
    """

    def __enter__(self):
        if not self.exists():
            raise ValueError('Cannot resume a project without {}'
                             .format(self.FILENAME))
        super(OldRunContext, self).__enter__()

        if self.db['n_samples'] != self.n_samples:
            raise ShelveError('n_samples mismatch: should be {} but was {}'
                              .format(self.db['n_samples'], self.n_samples))
        if self.db['project_id'] != self.project_id:
            raise ShelveError('project id mismatch: should be {} but was {}'
                              .format(self.db['project_id'], self.project_id))
        if self.db['model_id'] != self.model_id:
            raise ShelveError('model id mismatch: should be {} but was {}'
                              .format(self.db['model_id'], self.model_id))
        if self.db['keep_cols'] != self.keep_cols:
            raise ShelveError('keep_cols mismatch: should be {} but was {}'
                              .format(self.db['keep_cols'], self.keep_cols))

        self.first_write = self.db['first_write']
        self.out_stream = open(self.out_file, 'a')

        logger.info('resuming a shelved run with {} checkpointed batches'
                    .format(len(self.db['checkpoints'])))
        return self

    def __exit__(self, type, value, traceback):
        super(OldRunContext, self).__exit__(type, value, traceback)

    def batch_generator(self):
        """We filter everything that has not been checkpointed yet. """
        logger.info('playing checkpoint log forward.')
        already_processed_batches = set(self.db['checkpoints'])
        return (b for b in BatchGenerator(self.dataset,
                                          self.n_samples,
                                          self.n_retry,
                                          self.delimiter)
                if b.id not in already_processed_batches)


def context_factory(resume, n_samples, out_file, pid, lid,
                    keep_cols, n_retry,
                    delimiter, dataset, pred_name):
    """Factory method for run contexts.

    Either resume or start a new one.
    """
    if RunContext.exists():
        is_resume = None
        if resume:
            is_resume = True
        if is_resume is None:
            is_resume = prompt_yesno('Existing run found. Resume')
    else:
        is_resume = False
    if is_resume:
        return OldRunContext(n_samples, out_file, pid, lid, keep_cols, n_retry,
                             delimiter, dataset, pred_name)
    else:
        return NewRunContext(n_samples, out_file, pid, lid, keep_cols, n_retry,
                             delimiter, dataset, pred_name)


def authorized(user, api_token, n_retry, endpoint, base_headers, row):
    """Check if user is authorized for the given model and that schema is correct.

    This function will make a sync request to the api endpoint with a single
    row just to make sure that the schema is correct and the user
    is authorized.
    """
    while n_retry:
        logger.debug('request authorization')
        try:
            data = row.to_csv(encoding='utf8', index=False)
            r = requests.post(endpoint, headers=base_headers,
                              data=data,
                              auth=(user, api_token))
            root_logger.debug('authorization request response: {}|{}'
                              .format(r.status_code, r.text))
        except requests.exceptions.ConnectionError:
            error('cannot connect to {}'.format(endpoint))
        if r.status_code == 200:
            # all good
            break
        if r.status_code == 400:
            # client error -- maybe schema is wrong
            try:
                msg = r.json()['status']
            except:
                msg = r.text

            error('failed with client error: {}'.format(msg))
        elif r.status_code == 401:
            error('failed to authenticate -- '
                  'please check your username and/or api token.')
        elif r.status_code == 405:
            error('failed to request endpoint -- '
                  'please check your --host argument.')
        else:
            n_retry -= 1
    if n_retry == 0:
        status = r.text
        try:
            status = r.json()['status']
        except:
            pass  # fall back to r.text
        logger.error(('authorization failed -- '
                      'please check project id and model id permissions: {}')
                     .format(status))
        logger.debug(r.content)
        rval = False
    else:
        logger.debug('authorization successfully')
        rval = True

    return rval


def run_batch_predictions_v1(base_url, base_headers, user, pwd,
                             api_token, create_api_token,
                             pid, lid, n_retry, concurrent,
                             resume, n_samples,
                             out_file, keep_cols, delimiter,
                             dataset, pred_name,
                             timeout):
    if not api_token:
        if not pwd:
            pwd = getpass.getpass('password> ')
        try:
            api_token = acquire_api_token(base_url, base_headers, user, pwd,
                                          create_api_token)
        except Exception as e:
            error('{}'.format(e))

    base_headers['content-type'] = 'text/csv; charset=utf8'
    endpoint = base_url + '/'.join((pid, lid, 'predict'))

    first_row = peek_row(dataset, delimiter)
    root_logger.debug('First row for auth request: %s', first_row)

    # Make a sync request to check authentication and fail early
    if not authorized(user, api_token, n_retry, endpoint,
                      base_headers, first_row):
        sys.exit(1)

    try:
        with ExitStack() as stack:
            ctx = stack.enter_context(
                context_factory(resume, n_samples, out_file, pid,
                                lid, keep_cols, n_retry, delimiter,
                                dataset, pred_name))
            network = stack.enter_context(Network(concurrent, timeout))
            n_batches_checkpointed_init = len(ctx.db['checkpoints'])
            root_logger.debug('number of batches checkpointed initially: {}'
                              .format(n_batches_checkpointed_init))
            batches = ctx.batch_generator()
            work_unit_gen = WorkUnitGenerator(batches,
                                              endpoint,
                                              headers=base_headers,
                                              user=user,
                                              api_token=api_token,
                                              ctx=ctx,
                                              pred_name=pred_name)
            t0 = time()
            i = 0
            while work_unit_gen.has_next():
                responses = network.perform_requests(
                    work_unit_gen)
                for r in responses:
                    i += 1
                    logger.info('{} responses sent | time elapsed {}s'
                                .format(i, time() - t0))

                logger.debug('{} items still in the queue'
                             .format(len(work_unit_gen.queue.deque)))

            root_logger.debug('list of checkpointed batches: {}'
                              .format(sorted(ctx.db['checkpoints'])))
            n_batches_checkpointed = (len(ctx.db['checkpoints']) -
                                      n_batches_checkpointed_init)
            root_logger.debug('number of batches checkpointed: {}'
                              .format(n_batches_checkpointed))
            n_batches_not_checkpointed = (work_unit_gen.queue.n_consumed -
                                          n_batches_checkpointed)
            batches_missing = n_batches_not_checkpointed > 0
            if batches_missing:
                logger.fatal(('scoring incomplete, {} batches were dropped | '
                             'time elapsed {}s')
                             .format(n_batches_not_checkpointed, time() - t0))
            else:
                logger.info('scoring complete | time elapsed {}s'
                            .format(time() - t0))
                os.remove(root_logger_filename)

    except ShelveError as e:
        error('{}'.format(e), exit=False)
    except KeyboardInterrupt:
        logger.info('Keyboard interrupt')
    except Exception as oe:
        error('{}'.format(oe), exit=False)


# FIXME: broken alpha version
def run_batch_predictions_v2(base_url, base_headers, user, pwd,
                             api_token, create_api_token,
                             pid, lid, concurrent, n_samples,
                             out_file, dataset, timeout):

    from datarobot_sdk.client import Client
    if api_token:
        Client(token=api_token, endpoint=base_url)
    elif pwd:
        Client(username=user, password=pwd, endpoint=base_url)
    else:
        error('Please provide a password or api token')
        sys.exit(1)

    from datarobot_sdk import Model
    model = Model.get(pid, lid)
    try:
        model.predict_batch(dataset, out_file + ".tmp",
                            n_jobs=concurrent, batch_size=n_samples)

        import csv
        # swap order of prediction CSV schema to match api/v1
        with open(out_file + ".tmp", "rb") as input_file:
            with open(out_file, "wb") as output_file:
                rdr = csv.DictReader(input_file)
                wrtr = csv.DictWriter(output_file, ["row_id", "prediction"],
                                      extrasaction='ignore')
                wrtr.writeheader()
                for a in rdr:
                    wrtr.writerow(a)

    except ShelveError as e:
        error('{}'.format(e), exit=False)
    except KeyboardInterrupt:
        logger.info('Keyboard interrupt')
    except Exception as oe:
        error('{}'.format(oe), exit=False)


def main(argv=sys.argv[1:]):
    warnings.simplefilter('ignore')
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument('--host', type=str,
                        help='Specifies the hostname of the prediction '
                        'API endpoint (the location of the data from where to get predictions)')
    parser.add_argument('--user', type=str,
                        help='Specifies the username used to acquire the api-token.'
                        ' Use quotes if the name contains spaces.')
    parser.add_argument('--password', type=str, nargs='?',
                        help='Specifies the password used to acquire the api-token.'
                        ' Use quotes if the name contains spaces.')
    parser.add_argument('--api_token', type=str, nargs='?',
                        help='Specifies the api token for the requests; if you do not have a token,'
                        ' you must specify the password argument.')
    parser.add_argument('project_id', type=str,
                        help='Specifies the project identification string.')
    parser.add_argument('model_id', type=str,
                        help='Specifies the model identification string.')
    parser.add_argument('dataset', type=str,
                        help='Specifies the .csv input file that the script scores.')
    parser.add_argument('--datarobot_key', type=str,
                        nargs='?',
                        help='An additional datarobot_key for dedicated prediction instances.')
    parser.add_argument('--out', type=str,
                        nargs='?',
                        help='Specifies the file name, and optionally path, '
                             'to which the results are written. '
                             'If not specified, the default file name is out.csv,'
                             ' written to the directory containing the script.')
    parser.add_argument('--verbose', '-v', action="store_true",
                        help='Provides status updates while the script is running.')
    parser.add_argument('--n_samples', type=int,
                        nargs='?',
                        default=1000,
                        help='Specifies the number of samples to use per batch.'
                             ' Default sample size is 1000.')
    parser.add_argument('--n_concurrent', type=int,
                        nargs='?',
                        default=4,
                        help='Specifies the number of concurrent requests to submit.'
                             ' By default, 4 concurrent requests are submitted.')
    parser.add_argument('--api_version', type=str,
                        nargs='?',
                        default='v1',
                        help='Specifies the API version, either v1 or v2. The default is v1.')
    parser.add_argument('--create_api_token', action="store_true",
                        default=False,
                        help='Requests a new API token. To use this option, you must specify the '
                             ' password argument for this request (not the api_token argument).')
    parser.add_argument('--n_retry', type=int,
                        default=3,
                        help='Specifies the number of times DataRobot will retry '
                             'if a request fails. '
                             'A value of -1, the default, specifies an infinite number of retries.')
    parser.add_argument('--keep_cols', type=str,
                        nargs='?',
                        help='Specifies the column names to append to the predictions. '
                             'Enter as a comma-separated list.')
    parser.add_argument('--delimiter', type=str,
                        nargs='?',
                        help='Specifies the delimiter to recognize in the input .csv file. '
                             'If not specified, the script tries to automatically determine the '
                             'delimiter, and if it cannot, defaults to comma ( , ).')
    parser.add_argument('--resume', action='store_true',
                        default=False,
                        help='Starts the prediction from the point at which it was halted.'
                             ' If the prediction stopped, for example due to error or network '
                             'connection issue, you can run the same command with all the same '
                             'all arguments plus this resume argument.')
    parser.add_argument('--version', action='store_true',
                        default=False,
                        help='Show version')
    parser.add_argument('--timeout', type=int,
                        default=30, help='The timeout for each post request')
    parser.add_argument('--pred_name', type=str,
                        nargs='?')

    parsed_args = parser.parse_args()
    level = logging.DEBUG if parsed_args.verbose else logging.INFO
    configure_logging(level)
    printed_args = copy.copy(vars(parsed_args))
    printed_args.pop('password')
    root_logger.debug(printed_args)
    root_logger.info('platform: {} {}'.format(sys.platform, sys.version))

    if parsed_args.version:
        print('batch_scoring {}'.format(__version__))
        sys.exit(0)

    # parse args
    host = parsed_args.host
    pid = parsed_args.project_id
    lid = parsed_args.model_id
    n_retry = int(parsed_args.n_retry)
    if parsed_args.keep_cols:
        keep_cols = [s.strip() for s in parsed_args.keep_cols.split(',')]
    else:
        keep_cols = None
    concurrent = int(parsed_args.n_concurrent)
    dataset = parsed_args.dataset
    n_samples = int(parsed_args.n_samples)
    delimiter = parsed_args.delimiter
    resume = parsed_args.resume
    out_file = parsed_args.out
    datarobot_key = parsed_args.datarobot_key
    pwd = parsed_args.password
    timeout = int(parsed_args.timeout)

    if not hasattr(parsed_args, 'user'):
        user = input('user name> ').strip()
    else:
        user = parsed_args.user.strip()

    if not os.path.exists(parsed_args.dataset):
        error('file {} does not exist.'.format(parsed_args.dataset))
        sys.exit(1)

    pid = parsed_args.project_id
    lid = parsed_args.model_id

    try:
        verify_objectid(pid)
        verify_objectid(lid)
    except ValueError as e:
        error('{}'.format(e))
        sys.exit(1)

    api_token = parsed_args.api_token
    create_api_token = parsed_args.create_api_token
    pwd = parsed_args.password
    pred_name = parsed_args.pred_name

    api_version = parsed_args.api_version

    base_url = '{}/{}/'.format(host, api_version)
    base_headers = {}
    if datarobot_key:
        base_headers['datarobot-key'] = datarobot_key

    logger.info('connecting to {}'.format(base_url))

    if api_version == 'v1':
        run_batch_predictions_v1(base_url, base_headers, user, pwd,
                                 api_token, create_api_token,
                                 pid, lid, n_retry, concurrent,
                                 resume, n_samples,
                                 out_file, keep_cols, delimiter,
                                 dataset, pred_name, timeout)
    elif api_version == 'v2':
        run_batch_predictions_v2(base_url, base_headers, user, pwd,
                                 api_token, create_api_token,
                                 pid, lid, concurrent, n_samples,
                                 out_file, dataset, timeout)
    else:
        error('API Version {} is not supported'.format(api_version))
        sys.exit(1)


if __name__ == '__main__':
    main()
