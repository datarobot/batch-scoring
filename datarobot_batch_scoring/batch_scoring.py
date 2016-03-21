# -*- coding: utf-8 -*-
from __future__ import print_function

import collections
import getpass
import glob
import gzip
import json
import os
import shelve
import sys
import threading
from functools import partial
from time import time

import pandas as pd
import requests
import six

from .network import Network


if six.PY2:  # pragma: no cover
    from contextlib2 import ExitStack
    import dumbdbm  # noqa
elif six.PY3:  # pragma: no cover
    from contextlib import ExitStack
    # for successful py2exe dist package
    from dbm import dumb  # noqa


class ShelveError(Exception):
    pass


Batch = collections.namedtuple('Batch', 'id, df, rty_cnt')


class TargetType(object):
    REGRESSION = 'Regression'
    BINARY = 'Binary'


def acquire_api_token(base_url, base_headers, user, pwd, create_api_token, ui):
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
        ui.info('api-token acquired')

    api_token = r.json()['api_token']

    if api_token is None:
        raise ValueError('no api-token registered; '
                         'please run with --create_api_token flag.')

    ui.debug('api-token: {}'.format(api_token))

    return api_token


class BatchGenerator(object):
    """Class to chunk a large csv files into a stream
    of batches of size ``--n_samples``.

    Yields
    ------
    batch : Batch
        The next batch
    """

    def __init__(self, dataset, n_samples, n_retry, delimiter, ui):
        self.dataset = dataset
        self.chunksize = n_samples
        self.rty_cnt = n_retry
        self.sep = delimiter
        self._ui = ui

    def __iter__(self):
        compression = None
        if self.dataset.endswith('.gz'):
            self._ui.debug('using gzip compression')
            compression = 'gzip'
        rows_read = 0
        sep = self.sep

        engine = 'c'
        engine_params = {'error_bad_lines': False,
                         'warn_bad_lines': True}

        if not sep:
            sep = None
            engine = 'python'
            engine_params = {}
            self._ui.warning('Guessing delimiter will result '
                             'in slower parsing.')

        # handle unix tabs
        # NOTE: on bash you have to use Ctrl-V + TAB
        if sep == '\\t':
            sep = '\t'

        def _file_handle(fname):
            self._ui.debug('Opening file name {}.'.format(fname))
            return gzip.open(fname) if compression == 'gzip' else open(fname)

        if sep is not None:
            # if fixed sep check if we have at least one occurrence.
            with _file_handle(self.dataset) as fd:
                header = fd.readline()
                if isinstance(header, bytes):
                    bsep = sep.encode('utf-8')
                else:
                    bsep = sep
                if not header.strip():
                    raise ValueError("Input file '{}' is empty."
                                     .format(self.dataset))
                if len(header.split(bsep)) == 1:
                    raise ValueError(
                        ("Delimiter '{}' not found. "
                         "Please check your input file "
                         "or consider the flag `--delimiter=''`.").format(sep))

        # TODO for some reason c parser bombs on python 3.4 wo latin1
        batches = pd.read_csv(self.dataset, encoding='latin-1',
                              sep=sep,
                              iterator=True,
                              chunksize=self.chunksize,
                              compression=compression,
                              engine=engine,
                              **engine_params
                              )
        i = -1
        for i, chunk in enumerate(batches):
            if chunk.shape[0] == 0:
                raise ValueError(
                    "Input file '{}' is empty.".format(self.dataset))
            if i == 0:
                self._ui.debug('input columns: {!r}'
                               .format(chunk.columns.tolist()))
                self._ui.debug('input dtypes: {!r}'
                               .format(chunk.dtypes))
                self._ui.debug('input head: {!r}'
                               .format(chunk.head(2)))

            # strip white spaces
            chunk.columns = [c.strip() for c in chunk.columns]
            yield Batch(rows_read, chunk, self.rty_cnt)
            rows_read += self.chunksize

        if i == -1:
            raise ValueError("Input file '{}' is empty.".format(self.dataset))


def peek_row(dataset, delimiter, ui):
    """Peeks at the first row in `dataset`. """
    batches = BatchGenerator(dataset, 1, 1, delimiter, ui)
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
        pred = pd.DataFrame([p['class_probabilities'] for p in
                             sorted(predictions, key=lambda p: p['row_id'])])
        sorted_classes = pd.np.unique(pred.columns.tolist())
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
    if pred.shape[0] != batch.df.shape[0]:
        raise ValueError('Shape mismatch {}!={}'.format(
            pred.shape[0], batch.df.shape[0]))
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
                 ctx, pred_name, ui):
        self.endpoint = endpoint
        self.headers = headers
        self.user = user
        self.api_token = api_token
        self.ctx = ctx
        self.queue = GeneratorBackedQueue(batches)
        self.pred_name = pred_name
        self._ui = ui

    def _response_callback(self, r, batch=None, *args, **kw):
        try:
            if r.status_code == 200:
                try:
                    result = r.json()
                    exec_time = result['execution_time']
                    self._ui.debug(('successful response: exec time '
                                    '{:.0f}msec |'
                                    ' round-trip: {:.0f}msec').format(
                                        exec_time,
                                        r.elapsed.total_seconds() * 1000))

                    process_successful_request(result, batch,
                                               self.ctx, self.pred_name)
                except Exception as e:
                    self._ui.warning('{} response error: {} -- retry'
                                     .format(batch.id, e))
                    self.queue.push(batch)
            else:
                try:
                    self._ui.warning('batch {} failed with status: {}'
                                     .format(batch.id,
                                             json.loads(r.text)['status']))
                except ValueError:
                    self._ui.warning('batch {} failed with status code: {}'
                                     .format(batch.id, r.status_code))

                text = r.text
                self._ui.error('batch {} failed status_code:{} text:{}'
                               .format(batch.id,
                                       r.status_code,
                                       text))
                self.queue.push(batch)
        except Exception as e:
            self._ui.error('batch {} - dropping due to: {}'
                           .format(batch.id, e))

    def has_next(self):
        return self.queue.has_next()

    def __iter__(self):
        for batch in self.queue:
            # if we exhaused our retries we drop the batch
            if batch.rty_cnt == 0:
                self._ui.error('batch {} exceeded retry limit; '
                               'we lost {} records'.format(
                                   batch.id, batch.df.shape[0]))
                continue
            # otherwise we make an async request
            data = batch.df.to_csv(encoding='utf8', index=False)
            self._ui.debug('batch {} transmitting {} bytes'
                           .format(batch.id, len(data)))
            hook = partial(self._response_callback, batch=batch)
            yield requests.Request(
                method='POST',
                url=self.endpoint,
                headers=self.headers,
                data=data,
                auth=(self.user, self.api_token),
                hooks={'response': hook})


class RunContext(object):
    """A context for a run backed by a persistant store.

    We use a shelve to store the state of the run including
    a journal of processed batches that have been checkpointed.

    Note: we use globs for the shelve files because different
    versions of Python have different file layouts.
    """
    FILENAME = '.shelve'

    def __init__(self, n_samples, out_file, pid, lid, keep_cols,
                 n_retry, delimiter, dataset, pred_name, ui):
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
        self._ui = ui

    @classmethod
    def create(cls, resume, n_samples, out_file, pid, lid,
               keep_cols, n_retry,
               delimiter, dataset, pred_name, ui):
        """Factory method for run contexts.

        Either resume or start a new one.
        """
        if RunContext.exists():
            is_resume = None
            if resume:
                is_resume = True
            if is_resume is None:
                is_resume = ui.prompt_yesno('Existing run found. Resume')
        else:
            is_resume = False
        if is_resume:
            ctx_class = OldRunContext
        else:
            ctx_class = NewRunContext

        return ctx_class(n_samples, out_file, pid, lid, keep_cols, n_retry,
                         delimiter, dataset, pred_name, ui)

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
            mask = [c in batch.df.columns for c in self.keep_cols]
            if not all(mask):
                self._ui.fatal('keep_cols "{}" not in columns {}.'.format(
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
            self._ui.info('batch {} checkpointed'.format(batch.id))
            self.db.sync()

    def batch_generator(self):
        return iter(BatchGenerator(self.dataset, self.n_samples,
                                   self.n_retry, self.delimiter, self._ui))

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
            self._ui.info('Removing old run shelve')
            self.clean()
        if os.path.exists(self.out_file):
            self._ui.warning('File {} exists.'.format(self.out_file))
            rm = self._ui.prompt_yesno('Do you want to remove {}'.format(
                self.out_file))
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

        self._ui.info('resuming a shelved run with {} checkpointed batches'
                      .format(len(self.db['checkpoints'])))
        return self

    def __exit__(self, type, value, traceback):
        super(OldRunContext, self).__exit__(type, value, traceback)

    def batch_generator(self):
        """We filter everything that has not been checkpointed yet. """
        self._ui.info('playing checkpoint log forward.')
        already_processed_batches = set(self.db['checkpoints'])
        return (b for b in BatchGenerator(self.dataset,
                                          self.n_samples,
                                          self.n_retry,
                                          self.delimiter,
                                          self._ui)
                if b.id not in already_processed_batches)


def authorize(user, api_token, n_retry, endpoint, base_headers, row, ui):
    """Check if user is authorized for and that schema is correct.

    This function will make a sync request to the api endpoint with a single
    row just to make sure that the schema is correct and the user
    is authorized.
    """
    r = None
    while n_retry:
        ui.debug('request authorization')
        try:
            data = row.to_csv(encoding='utf8', index=False)
            r = requests.post(endpoint, headers=base_headers,
                              data=data,
                              auth=(user, api_token))
            ui.debug('authorization request response: {}|{}'
                     .format(r.status_code, r.text))
            if r.status_code == 200:
                # all good
                break
            if r.status_code == 400:
                # client error -- maybe schema is wrong
                try:
                    msg = r.json()['status']
                except:
                    msg = r.text

                ui.fatal('failed with client error: {}'.format(msg))
            elif r.status_code == 401:
                ui.fatal('failed to authenticate -- '
                         'please check your username and/or api token.')
            elif r.status_code == 405:
                ui.fatal('failed to request endpoint -- '
                         'please check your --host argument.')
        except requests.exceptions.ConnectionError:
            ui.error('cannot connect to {}'.format(endpoint))
        n_retry -= 1

    if n_retry == 0:
        status = r.text if r is not None else 'UNKNOWN'
        try:
            status = r.json()['status']
        except:
            pass  # fall back to r.text
        content = r.content if r is not None else 'NO CONTENT'
        ui.debug("Failed authorization response \n{!r}".format(content))
        ui.fatal(('authorization failed -- '
                  'please check project id and model id permissions: {}')
                 .format(status))
    else:
        ui.debug('authorization has succeeded')


def run_batch_predictions_v1(base_url, base_headers, user, pwd,
                             api_token, create_api_token,
                             pid, lid, n_retry, concurrent,
                             resume, n_samples,
                             out_file, keep_cols, delimiter,
                             dataset, pred_name,
                             timeout, ui):
    if not api_token:
        if not pwd:
            pwd = getpass.getpass('password> ')
        try:
            api_token = acquire_api_token(base_url, base_headers, user, pwd,
                                          create_api_token, ui)
        except Exception as e:
            ui.fatal(str(e))

    base_headers['content-type'] = 'text/csv; charset=utf8'
    endpoint = base_url + '/'.join((pid, lid, 'predict'))

    first_row = peek_row(dataset, delimiter, ui)
    ui.debug('First row for auth request: {}'.format(first_row))

    # Make a sync request to check authentication and fail early
    authorize(user, api_token, n_retry, endpoint, base_headers, first_row, ui)

    with ExitStack() as stack:
        ctx = stack.enter_context(
            RunContext.create(resume, n_samples, out_file, pid,
                              lid, keep_cols, n_retry, delimiter,
                              dataset, pred_name, ui))
        network = stack.enter_context(Network(concurrent, timeout))
        n_batches_checkpointed_init = len(ctx.db['checkpoints'])
        ui.debug('number of batches checkpointed initially: {}'
                 .format(n_batches_checkpointed_init))
        batches = ctx.batch_generator()
        work_unit_gen = WorkUnitGenerator(batches,
                                          endpoint,
                                          headers=base_headers,
                                          user=user,
                                          api_token=api_token,
                                          ctx=ctx,
                                          pred_name=pred_name,
                                          ui=ui)
        t0 = time()
        i = 0
        while work_unit_gen.has_next():
            responses = network.perform_requests(
                work_unit_gen)
            for r in responses:
                i += 1
                ui.info('{} responses sent | time elapsed {}s'
                        .format(i, time() - t0))

            ui.debug('{} items still in the queue'
                     .format(len(work_unit_gen.queue.deque)))

        ui.debug('list of checkpointed batches: {}'
                 .format(sorted(ctx.db['checkpoints'])))
        n_batches_checkpointed = (len(ctx.db['checkpoints']) -
                                  n_batches_checkpointed_init)
        ui.debug('number of batches checkpointed: {}'
                 .format(n_batches_checkpointed))
        n_batches_not_checkpointed = (work_unit_gen.queue.n_consumed -
                                      n_batches_checkpointed)
        batches_missing = n_batches_not_checkpointed > 0
        if batches_missing:
            ui.fatal(('scoring incomplete, {} batches were dropped | '
                      'time elapsed {}s')
                     .format(n_batches_not_checkpointed, time() - t0))
        else:
            ui.info('scoring complete | time elapsed {}s'
                    .format(time() - t0))
            ui.close()


# FIXME: broken alpha version
def run_batch_predictions_v2(base_url, base_headers, user, pwd,
                             api_token, create_api_token,
                             pid, lid, concurrent, n_samples,
                             out_file, dataset, timeout, ui):

    from datarobot_sdk.client import Client
    if api_token:
        Client(token=api_token, endpoint=base_url)
    elif pwd:
        Client(username=user, password=pwd, endpoint=base_url)
    else:
        ui.fatal('Please provide a password or api token')

    from datarobot_sdk import Model
    model = Model.get(pid, lid)
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
    ui.close()
