# -*- coding: utf-8 -*-
from __future__ import print_function

import io
import collections
import csv
import glob
import gzip
import json
import operator
import os
import shelve
import sys
import threading
import hashlib
import codecs
from functools import partial
from functools import reduce
from itertools import chain
from time import time
from multiprocessing import Queue
from multiprocessing import Process
from six.moves import queue
from six.moves import zip
import requests
import six
import chardet

from .network import Network
from .utils import acquire_api_token, iter_chunks


if six.PY2:  # pragma: no cover
    from contextlib2 import ExitStack
    from itertools import ifilter
    import dumbdbm  # noqa
elif six.PY3:  # pragma: no cover
    from contextlib import ExitStack
    ifilter = filter
    # for successful py2exe dist package
    from dbm import dumb  # noqa


class ShelveError(Exception):
    pass


Batch = collections.namedtuple('Batch', 'id fieldnames data rty_cnt')
Prediction = collections.namedtuple('Prediction', 'fieldnames data')

SENTINEL = Batch(-1, None, '', -1)


class TargetType(object):
    REGRESSION = 'Regression'
    BINARY = 'Binary'


def fast_to_csv_chunk(data, header):
    """Fast routine to format data for prediction api.

    Returns data in unicode.
    """
    header = ','.join(header)
    chunk = ''.join(chain((header, '\n'), data))
    if six.PY3:
        return chunk.encode('utf-8')
    else:
        return chunk


def slow_to_csv_chunk(data, header):
    """Slow routine to format data for prediction api.
    Returns data in unicode.
    """
    if six.PY3:
        buf = io.StringIO()
    else:
        buf = io.BytesIO()

    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(data)
    if six.PY3:
        return buf.getvalue().encode('utf-8')
    else:
        return buf.getvalue()


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


class CSVReader(object):
    def __init__(self, fd, sep, encoding=None, dialect=None):
        self.fd = fd
        self.sep = sep
        self.dialect = dialect
        self.encoding = encoding

    def _create_reader(self):
        if self.sep is not None:
            self.sep = str(self.sep)
        #  in Python 2 csv.dialect encodes these inconsistently.
        self.dialect.delimiter = str(self.dialect.delimiter)
        self.dialect.lineterminator = str(self.dialect.lineterminator)
        self.dialect.quotechar = str(self.dialect.quotechar)
        fd = Recoder(self.fd, self.encoding)
        return csv.reader(fd, self.dialect, delimiter=self.sep)


class FastReader(CSVReader):
    """A reader that only reads the file in text mode but not parses it. """

    def __init__(self, fd, sep, encoding=None, dialect=None):
        super(FastReader, self).__init__(fd, sep, encoding=encoding,
                                         dialect=dialect)
        self._check_for_multiline_input()
        reader = self._create_reader()
        self.header = next(reader)
        self.fieldnames = [c.strip() for c in self.header]

    def __iter__(self):
        fd = Recoder(self.fd, self.encoding)
        it = iter(fd)
        next(it)  # skip header
        return it

    def _check_for_multiline_input(self, peek_size=100):
        # peek the first `peek_size` records for multiline CSV
        reader = self._create_reader()
        i = 0
        for line in reader:
            i += 1
            if i == peek_size:
                break

        peek_size = min(i, peek_size)

        if reader.line_num != peek_size:
            self._ui.fatal('Detected multiline CSV format'
                           ' -- dont use flag `--fast` '
                           'to force CSV parsing. '
                           'Note that this will slow down scoring.')


class SlowReader(CSVReader):
    """The slow reader does actual CSV parsing.
    It supports multiline csv and can be a factor of 50 slower. """

    def __init__(self, fd, sep, encoding=None, dialect=None):
        super(SlowReader, self).__init__(fd, sep, encoding=encoding,
                                         dialect=dialect)
        reader = self._create_reader()
        self.header = next(reader)
        self.fieldnames = [c.strip() for c in self.header]

    def __iter__(self):
        fd = Recoder(self.fd, self.encoding)
        self.reader = csv.reader(fd, self.dialect, delimiter=self.sep)
        for i, row in enumerate(self.reader):
            if i == 0:
                # skip header
                continue
            yield row


class BatchGenerator(object):
    """Class to chunk a large csv files into a stream
    of batches of size ``--n_samples``.

    Yields
    ------
    batch : Batch
        The next batch. A batch holds the data to be send already
        in the form that can be passed to the HTTP request.
    """

    def __init__(self, dataset, n_samples, n_retry, delimiter, ui,
                 fast_mode, encoding=None, dialect=None):
        self.dataset = dataset
        self.chunksize = n_samples
        self.rty_cnt = n_retry
        self.sep = delimiter
        self._ui = ui
        self.fast_mode = fast_mode
        self.encoding = encoding
        self.dialect = dialect

    def csv_input_file_reader(self):
        if self.dataset.endswith('.gz'):
            opener = gzip.open
        else:
            opener = open

        if six.PY3:
            fd = opener(self.dataset, 'rt',
                        encoding=self.encoding)
        else:
            fd = opener(self.dataset, 'rb')
        return fd

    def __iter__(self):
        if self.fast_mode:
            reader_factory = FastReader
        else:
            reader_factory = SlowReader

        with self.csv_input_file_reader() as csvfile:
            reader = reader_factory(csvfile, dialect=self.dialect,
                                    sep=self.dialect.delimiter,
                                    encoding=self.encoding)
            fieldnames = reader.fieldnames

            has_content = False
            t0 = time()
            rows_read = 0
            for chunk in iter_chunks(reader, self.chunksize):
                has_content = True
                n_rows = len(chunk)
                yield Batch(rows_read, fieldnames, chunk, self.rty_cnt)
                rows_read += n_rows
            if not has_content:
                raise ValueError("Input file '{}' is empty.".format(
                    self.dataset))

            self._ui.error('chunking {} rows took {}'.format(
                rows_read, time() - t0))

    def investigate_encoding_and_dialect(self):
        """Try to identify encoding and dialect.
        Providing a delimiter may help with smaller datasets.
        Running this is costly so run it once per dataset."""
        if self.encoding and self.dialect:
            return (self.encoding, self.dialect)
        if self.dataset.endswith('.gz'):
            opener = gzip.open
        else:
            opener = open
        with opener(self.dataset, 'rb') as dfile:
            sample = dfile.read(2*1024**2)
        chardet_result = chardet.detect(sample)
        self.encoding = chardet_result['encoding'].lower()
        sniffer = csv.Sniffer()
        try:
            self.dialect = sniffer.sniff(sample.decode(self.encoding),
                                         delimiters=self.sep)
        except csv.Error:
            if len(sample) < 10:
                self._ui.fatal('Input file "%s" is less than 10 chars long '
                               'and this is the possible cause of a csv.Error.'
                               ' Check the file and try again.' % self.dataset)
            elif self.sep is not None:
                self._ui.fatal('The csv module failed to detect the CSV '
                               'dialect. Check that you provided the correct '
                               'delimiter, or try the script without the '
                               '--delimiter flag.')
            else:
                self._ui.fatal('The csv module failed to detect the CSV '
                               'dialect. Try giving hints with the '
                               '--delimiter argument, E.g  '
                               """--delimiter=','""")
            raise
        return self.encoding, self.dialect


def peek_row(dataset, delimiter, ui, fast_mode, encoding, dialect):
    """Peeks at the first row in `dataset`. """
    batches = BatchGenerator(dataset, 1, 1, delimiter, ui, fast_mode,
                             encoding, dialect)
    try:
        batch = next(iter(batches))
    except StopIteration:
        raise ValueError('Cannot peek first row from {}'.format(dataset))
    return batch


class MultiprocessingGeneratorBackedQueue(object):
    """A queue that is backed by a generator.

    When the queue is exhausted it repopulates from the generator.
    """
    def __init__(self, queue_size, ui, queue=None):
        self.n_consumed = 0
        self.queue = Queue(queue_size)
        self.deque = Queue(queue_size)
        self.lock = threading.RLock()
        self._ui = ui

    def __iter__(self):
        return self

    def __next__(self):
        try:
            r = self.deque.get_nowait()
            return r
        except queue.Empty:
            try:
                r = self.queue.get()
                if r.id == SENTINEL.id:
                    self.queue.close()
                    raise StopIteration
                self.n_consumed += 1
                return r
            except OSError:
                raise StopIteration

    def __len__(self):
        return self.queue.qsize() + self.deque.qsize()

    def next(self):
        return self.__next__()

    def push(self, batch):
        # we retry a batch - decrement retry counter
        batch = batch._replace(rty_cnt=batch.rty_cnt - 1)
        try:
            self.deque.put(batch, block=False)
        except queue.Empty:
            self._ui.error('Dropping {} due to backfill queue full.'.format(
                batch))

    def has_next(self):
        with self.lock:
            try:
                item = self.next()
                self.push(item)
                return True
            except StopIteration:
                return False


class Shovel(object):

    def __init__(self, ctx, queue, ui):
        self.ctx = ctx
        self._ui = ui
        self.queue = queue

    def _shove(self, q, ctx):
        for batch in ctx.batch_generator():
            q.put(batch)

        q.put(SENTINEL)

    def go(self):
        self.p = Process(target=self._shove,
                         args=(self.queue.queue, self.ctx),
                         name='shovel')
        self.p.start()

    def all(self):
        self.queue.queue = Queue(0)
        self._shove(self.queue.queue, self.ctx)
        self._ui.info('QSIZE:{}'.format(self.queue.queue.qsize()))


def process_successful_request(result, batch, ctx, pred_name):
    """Process a successful request. """
    predictions = result['predictions']
    if result['task'] == TargetType.BINARY:
        sorted_classes = list(
            sorted(predictions[0]['class_probabilities'].keys()))
        out_fields = ['row_id'] + sorted_classes
        if pred_name is not None and '1.0' in sorted_classes:
            sorted_classes = ['1.0']
            out_fields = ['row_id'] + [pred_name]
        pred = [[p['row_id'] + batch.id] +
                [p['class_probabilities'][c] for c in sorted_classes]
                for p in
                sorted(predictions, key=operator.itemgetter('row_id'))]
    elif result['task'] == TargetType.REGRESSION:
        pred = [[p['row_id'] + batch.id, p['prediction']]
                for p in
                sorted(predictions, key=operator.itemgetter('row_id'))]
        out_fields = ['row_id', pred_name if pred_name else '']
    else:
        ValueError('task {} not supported'.format(result['task']))

    ctx.checkpoint_batch(batch, out_fields, pred)


class WorkUnitGenerator(object):
    """Generates async requests with completion or retry callbacks.

    It uses a queue backed by a batch generator.
    It will pop items for the queue and if its exhausted it will populate the
    queue from the batch generator.
    If a submitted async request was not successfull it gets enqueued again.
    """

    def __init__(self, queue, endpoint, headers, user, api_token,
                 ctx, pred_name, ui):
        self.endpoint = endpoint
        self.headers = headers
        self.user = user
        self.api_token = api_token
        self.ctx = ctx
        self.queue = queue
        self.pred_name = pred_name
        self._ui = ui

    def _response_callback(self, r, batch=None, *args, **kw):
        try:
            if r.status_code == 200:
                try:
                    try:
                        result = r.json()
                    except Exception as e:
                        self._ui.warning('{} response error: {} -- retry'
                                         .format(batch.id, e))
                        self.queue.push(batch)
                        return
                    exec_time = result['execution_time']
                    self._ui.debug(('successful response: exec time '
                                    '{:.0f}msec |'
                                    ' round-trip: {:.0f}msec').format(
                                        exec_time,
                                        r.elapsed.total_seconds() * 1000))
                    process_successful_request(result, batch, self.ctx,
                                               self.pred_name)
                except Exception as e:
                    self._ui.fatal('{} response error: {}'.format(batch.id, e))
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
            if batch.id == -1:  # sentinel
                raise StopIteration()
            # if we exhaused our retries we drop the batch
            if batch.rty_cnt == 0:
                self._ui.error('batch {} exceeded retry limit; '
                               'we lost {} records'.format(
                                   batch.id, len(batch.data)))
                continue
            hook = partial(self._response_callback, batch=batch)

            if self.ctx.fast_mode:
                chunk_formatter = fast_to_csv_chunk
            else:
                chunk_formatter = slow_to_csv_chunk

            data = chunk_formatter(batch.data, batch.fieldnames)
            self._ui.debug('batch {} transmitting {} bytes'
                           .format(batch.id, len(data)))
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

    def __init__(self, n_samples, out_file, pid, lid, keep_cols,
                 n_retry, delimiter, dataset, pred_name, ui, file_context,
                 fast_mode, encoding, dialect):
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
        self.file_context = file_context
        self.fast_mode = fast_mode
        self.encoding = encoding
        self.dialect = dialect

    @classmethod
    def create(cls, resume, n_samples, out_file, pid, lid,
               keep_cols, n_retry,
               delimiter, dataset, pred_name, ui,
               fast_mode, encoding, dialect):
        """Factory method for run contexts.

        Either resume or start a new one.
        """
        file_context = ContextFile(pid, lid, n_samples, keep_cols)
        if file_context.exists():
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
                         delimiter, dataset, pred_name, ui, file_context,
                         fast_mode, encoding, dialect)

    def __enter__(self):
        self.db = shelve.open(self.file_context.file_name, writeback=True)
        self.partitions = []
        return self

    def __exit__(self, type, value, traceback):
        self.db.close()
        if self.out_stream is not None:
            self.out_stream.close()
        if type is None:
            # success - remove shelve
            self.file_context.clean()

    def checkpoint_batch(self, batch, out_fields, pred):
        """Mark a batch as being processed:
           - write it to the output stream (if necessary pull out columns).
           - put the batch_id into the journal.
        """
        delimiter = self.dialect.delimiter
        if self.keep_cols:
            # stack columns
            if self.db['first_write']:
                if not all(c in batch.fieldnames for c in self.keep_cols):
                    self._ui.fatal('keep_cols "{}" not in columns {}.'.format(
                        [c for c in self.keep_cols
                         if c not in batch.fieldnames], batch.fieldnames))

            feature_indices = {col: i for i, col in
                               enumerate(batch.fieldnames)}
            indices = [feature_indices[col] for col in self.keep_cols]
            written_fields = ['row_id'] + self.keep_cols + out_fields[1:]

            # first column is row_id
            comb = []
            for row, predicted in zip(batch.data, pred):
                if self.fast_mode:
                    # row is a full line, we need to cut it into fields
                    # FIXME this will fail on quoted fields!
                    row = row.rstrip().split(delimiter)
                keeps = [row[i] for i in indices]
                comb.append([predicted[0]] + keeps + predicted[1:])
        else:
            comb = pred
            written_fields = out_fields
        with self.lock:
            # if an error happends during/after the append we
            # might end up with inconsistent state
            # TODO write partition files instead of appending
            #  store checksum of each partition and back-check
            writer = csv.writer(self.out_stream)
            if self.db['first_write']:
                writer.writerow(written_fields)
            writer.writerows(comb)
            self.out_stream.flush()

            self.db['checkpoints'].append(batch.id)

            self.db['first_write'] = False
            self._ui.info('batch {} checkpointed'.format(batch.id))
            self.db.sync()

    def batch_generator(self):
        return iter(BatchGenerator(self.dataset, self.n_samples,
                                   self.n_retry, self.delimiter, self._ui,
                                   self.fast_mode, self.encoding,
                                   self.dialect))


class ContextFile(object):
    def __init__(self, project_id, model_id, n_samples, keep_cols):
        hashable = reduce(operator.add, map(str,
                                            [project_id,
                                             model_id,
                                             n_samples,
                                             keep_cols]))
        digest = hashlib.md5(hashable.encode('utf8')).hexdigest()
        self.file_name = digest + '.shelve'

    def exists(self):
        """Does shelve exist. """
        return any(glob.glob(self.file_name + '*'))

    def clean(self):
        """Clean the shelve. """
        for fname in glob.glob(self.file_name + '*'):
            os.remove(fname)


class NewRunContext(RunContext):
    """RunContext for a new run.

    It creates a shelve file and adds a checkpoint journal.
    """

    def __enter__(self):
        if self.file_context.exists():
            self._ui.info('Removing old run shelve')
            self.file_context.clean()
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
        if not self.file_context.exists():
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
                                          self._ui,
                                          self.fast_mode,
                                          self.encoding,
                                          self.dialect)
                if b.id not in already_processed_batches)


def authorize(user, api_token, n_retry, endpoint, base_headers, batch, ui):
    """Check if user is authorized for the given model and that schema is
    correct.

    This function will make a sync request to the api endpoint with a single
    row just to make sure that the schema is correct and the user
    is authorized.
    """
    r = None

    while n_retry:
        ui.debug('request authorization')
        try:
            r = requests.post(endpoint, headers=base_headers,
                              data=batch.data,
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


def run_batch_predictions(base_url, base_headers, user, pwd,
                          api_token, create_api_token,
                          pid, lid, n_retry, concurrent,
                          resume, n_samples,
                          out_file, keep_cols, delimiter,
                          dataset, pred_name,
                          timeout, ui, fast_mode,
                          dry_run=False):
    t1 = time()
    if not api_token:
        if not pwd:
            pwd = ui.getpass()
        try:
            api_token = acquire_api_token(base_url, base_headers, user, pwd,
                                          create_api_token, ui)
        except Exception as e:
            ui.fatal(str(e))

    base_headers['content-type'] = 'text/csv; charset=utf8'
    endpoint = base_url + '/'.join((pid, lid, 'predict'))
    (encoding, dialect) = BatchGenerator(
        dataset, 1, 1, delimiter, ui, fast_mode
        ).investigate_encoding_and_dialect()
    # Make a sync request to check authentication and fail early
    first_row = peek_row(dataset, delimiter, ui, fast_mode, encoding, dialect)
    ui.debug('First row for auth request: {}'.format(first_row))
    if fast_mode:
        chunk_formatter = fast_to_csv_chunk
    else:
        chunk_formatter = slow_to_csv_chunk

    first_row_data = chunk_formatter(first_row.data, first_row.fieldnames)
    first_row = first_row._replace(data=first_row_data)
    authorize(user, api_token, n_retry, endpoint, base_headers, first_row, ui)

    with ExitStack() as stack:
        ctx = stack.enter_context(
            RunContext.create(resume, n_samples, out_file, pid,
                              lid, keep_cols, n_retry, delimiter,
                              dataset, pred_name, ui, fast_mode,
                              encoding, dialect))
        network = stack.enter_context(Network(concurrent, timeout))
        n_batches_checkpointed_init = len(ctx.db['checkpoints'])
        ui.debug('number of batches checkpointed initially: {}'
                 .format(n_batches_checkpointed_init))

        # make the queue twice as big as the
        queue = MultiprocessingGeneratorBackedQueue(concurrent * 2, ui)
        shovel = Shovel(ctx, queue, ui)
        ui.info('Shovel go...')
        t2 = time()
        shovel.go()
        ui.info('shoveling complete | total time elapsed {}s'
                .format(time() - t2))

        work_unit_gen = WorkUnitGenerator(queue,
                                          endpoint,
                                          headers=base_headers,
                                          user=user,
                                          api_token=api_token,
                                          ctx=ctx,
                                          pred_name=pred_name,
                                          ui=ui)
        t0 = time()
        i = 0

        if dry_run:
            for _ in work_unit_gen:
                pass
            ui.info('dry-run complete | time elapsed {}s'.format(time() - t0))
            ui.info('dry-run complete | total time elapsed {}s'.format(
                time() - t1))
            ui.close()
        else:
            responses = network.perform_requests(work_unit_gen)
            for r in responses:
                i += 1
                ui.info('{} responses sent | time elapsed {}s'
                        .format(i, time() - t0))

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
                ui.info('scoring complete | total time elapsed {}s'
                        .format(time() - t1))
                ui.close()
