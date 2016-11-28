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
import hashlib
from functools import partial
from functools import reduce
from itertools import chain
from time import time, sleep
import multiprocessing
from six.moves import queue
from six.moves import zip
import requests
import six
import platform

from datarobot_batch_scoring.network import Network, FakeResponse
from datarobot_batch_scoring.utils import acquire_api_token, iter_chunks, \
    auto_sampler, Recoder, investigate_encoding_and_dialect
from datarobot_batch_scoring import __version__

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


Batch = collections.namedtuple('Batch', 'id rows fieldnames data rty_cnt')
Prediction = collections.namedtuple('Prediction', 'fieldnames data')

SENTINEL = Batch(-1, 0, None, '', -1)
ERROR_SENTINEL = Batch(-1, 1, None, '', -1)
MAX_BATCH_SIZE = 5 * 1024 ** 2


class QueueMsg(object):
    WARNING = 'WARNING'
    ERROR = 'ERROR'


class TargetType(object):
    REGRESSION = 'Regression'
    BINARY = 'Binary'


def fast_to_csv_chunk(data, header):
    """Fast routine to format data for prediction api.

    Returns data in unicode.
    """
    header = ','.join(header)
    chunk = ''.join(chain((header, os.linesep), data))
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


class CSVReader(object):
    def __init__(self, fd, encoding, ui):
        self.fd = fd
        #  dataset_dialect is set by investigate_encoding_and_dialect in utils
        self.dialect = csv.get_dialect('dataset_dialect')
        self.encoding = encoding
        self._ui = ui

    def _create_reader(self):
        fd = Recoder(self.fd, self.encoding)
        return csv.reader(fd, self.dialect, delimiter=self.dialect.delimiter)


class FastReader(CSVReader):
    """A reader that only reads the file in text mode but not parses it. """

    def __init__(self, fd, encoding, ui):
        super(FastReader, self).__init__(fd, encoding, ui)
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

    def __init__(self, fd, encoding, ui):
        super(SlowReader, self).__init__(fd, encoding, ui)
        reader = self._create_reader()
        self.header = next(reader)
        self.fieldnames = [c.strip() for c in self.header]

    def __iter__(self):
        self.reader = self._create_reader()
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
                 fast_mode, encoding, already_processed_batches=set()):
        self.dataset = dataset
        self.chunksize = n_samples
        self.rty_cnt = n_retry
        self._ui = ui
        self.fast_mode = fast_mode
        self.encoding = encoding
        self.already_processed_batches = already_processed_batches

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
            reader = reader_factory(csvfile, self.encoding, self._ui)
            fieldnames = reader.fieldnames

            has_content = False
            t0 = time()
            rows_read = 0
            for chunk in iter_chunks(reader, self.chunksize):
                has_content = True
                n_rows = len(chunk)
                if (rows_read, n_rows) not in self.already_processed_batches:
                    yield Batch(rows_read, n_rows, fieldnames,
                                chunk, self.rty_cnt)
                rows_read += n_rows
            if not has_content:
                raise ValueError("Input file '{}' is empty.".format(
                    self.dataset))
            self._ui.info('chunking {} rows took {}'.format(rows_read,
                                                            time() - t0))


def peek_row(dataset, delimiter, ui, fast_mode, encoding):
    """Peeks at the first row in `dataset`. """
    batches = BatchGenerator(dataset, 1, 1, delimiter, ui, fast_mode,
                             encoding)
    try:
        batch = next(iter(batches))
    except StopIteration:
        raise ValueError('Cannot peek first row from {}'.format(dataset))
    return batch


class MultiprocessingGeneratorBackedQueue(object):
    """A queue that is backed by a generator.

    When the queue is exhausted it repopulates from the generator.
    """
    def __init__(self, ui, queue, deque, rlock):
        self.n_consumed = 0
        self.queue = queue
        self.deque = deque
        self.rlock = rlock
        self._ui = ui

    def __iter__(self):
        return self

    def __next__(self):
        try:
            r = self.deque.get_nowait()
            self._ui.debug('Got batch from dequeu: {}'.format(r.id))
            return r
        except queue.Empty:
            try:
                r = self.queue.get()
                if r.id == SENTINEL.id:
                    if r.rows == ERROR_SENTINEL.rows:
                        self._ui.error('Error parsing CSV file, '
                                       'check logs for exact error')
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
        with self.rlock:
            try:
                item = self.next()
                self.push(item)
                return True
            except StopIteration:
                return False


class Shovel(object):

    def __init__(self, queue, batch_gen_args, ui):
        self._ui = ui
        self.queue = queue
        self.batch_gen_args = batch_gen_args
        self.dialect = csv.get_dialect('dataset_dialect')
        #  The following should only impact Windows
        self._ui.set_next_UI_name('batcher')

    def _shove(self, args, dialect, queue):
        t2 = time()
        _ui = args[4]
        _ui.info('Shovel process started')
        csv.register_dialect('dataset_dialect', dialect)
        batch_generator = BatchGenerator(*args)
        try:
            for batch in batch_generator:
                _ui.debug('queueing batch {}'.format(batch.id))
                queue.put(batch)

            _ui.info('shoveling complete | total time elapsed {}s'
                     ''.format(time() - t2))
            queue.put(SENTINEL)
        except csv.Error:
            queue.put(ERROR_SENTINEL)
            raise
        finally:
            queue.put(SENTINEL)
            if os.name is 'nt':
                _ui.close()

    def go(self):
        self.p = multiprocessing.Process(target=self._shove,
                                         args=([self.batch_gen_args,
                                                self.dialect, self.queue]),
                                         name='shovel')
        self.p.start()


class WriterProcess(object):
    def __init__(self, ui, ctx, writer_queue, queue, deque):
            self._ui = ui
            self._ui.set_next_UI_name('writer')
            self.reader_dialect = csv.get_dialect('dataset_dialect')
            self.writer_dialect = csv.get_dialect('writer_dialect')
            self.ctx = ctx
            self.writer_queue = writer_queue
            self.queue = queue
            self.deque = deque

    @staticmethod
    def push(batch, deque, ui):
        # we retry a batch - decrement retry counter
        batch = batch._replace(rty_cnt=batch.rty_cnt - 1)
        try:
            deque.put(batch, block=True)
        except queue.Empty:
            ui.error('Dropping {} due to backfill queue full.'.format(
                batch))

    @staticmethod
    def unpack_request_object(request, batch, ui, queue, deque):
        try:
            result = json.loads(request['text'])  # replace with r.content
            elapsed_total_seconds = request['elapsed']
        except Exception as e:
            ui.warning('{} response error: {} -- retry'.format(batch.id, e))
            WriterProcess.push(batch, deque, ui)
            return False
        exec_time = result['execution_time']
        ui.debug(('successful response {}-{}: exec time {:.0f}msec | '
                  'round-trip: {:.0f}msec'
                  '').format(batch.id, batch.rows, exec_time,
                             elapsed_total_seconds * 1000))
        return result

    @staticmethod
    def process_response(ui, ctx, writer_queue, queue, deque, reader_dialect,
                         writer_dialect):
        """Process a successful request. """
        csv.register_dialect('dataset_dialect', reader_dialect)
        csv.register_dialect('writer_dialect', writer_dialect)
        ui.info(csv.list_dialects())
        ui.debug('Writer Process started - {}'
                 ''.format(multiprocessing.current_process().name))
        success = False
        try:
            ctx.open()
            while True:
                (request, batch, pred_name) = writer_queue.get()
                if request == QueueMsg.ERROR:
                    # pred_name is a message if ERROR or WARNING
                    ui.debug('Writer ERROR')
                    ctx.save_error(batch, error=pred_name)
                    continue
                if request == QueueMsg.WARNING:
                    ui.debug('Writer WARNING')
                    ctx.save_warning(batch, error=pred_name)
                    continue
                if batch.id == SENTINEL.id:
                    ui.debug('Writer recieved SENTINEL')
                    break
                result = WriterProcess.unpack_request_object(request, batch,
                                                             ui, queue, deque)
                if result is False:
                    continue
                predictions = result['predictions']
                if result['task'] == TargetType.BINARY:
                    sorted_classes = list(
                        sorted(predictions[0]['class_probabilities'].keys()))
                    out_fields = ['row_id'] + sorted_classes
                    if pred_name is not None:
                        sorted_classes = [sorted_classes[-1]]
                        out_fields = ['row_id'] + [pred_name]
                    pred = [[p['row_id'] + batch.id] +
                            [p['class_probabilities'][c] for c in
                             sorted_classes]
                            for p in
                            sorted(predictions,
                                   key=operator.itemgetter('row_id'))]
                elif result['task'] == TargetType.REGRESSION:
                    pred = [[p['row_id'] + batch.id, p['prediction']]
                            for p in
                            sorted(predictions,
                                   key=operator.itemgetter('row_id'))]
                    out_fields = ['row_id', pred_name if pred_name else '']
                else:
                    ValueError('task "{}" not supported'
                               ''.format(result['task']))
                ctx.checkpoint_batch(batch, out_fields, pred)
                ui.debug('Writer Queue queue length: {}'
                         ''.format(writer_queue.qsize()))

            ui.info('---Writer Exiting---')
            checkpointed = ctx.db['checkpoints']
            ui.info('---Checkpointed--- {}'.format(checkpointed))
            writer_queue.put(True)
            success = True
        except Exception as e:
            ui.error('Writer Process error: batch={}, error={}'
                     ''.format(batch.id, e))

        finally:
            ctx.close()
            queue.close()
            writer_queue.close()
            deque.close()
            ui.close()
            if success:
                sys.exit(0)
            else:
                sys.exit(1)

    def go(self):
        self.w = multiprocessing.Process(target=self.process_response,
                                         args=([self._ui, self.ctx,
                                                self.writer_queue,
                                                self.queue, self.deque,
                                                self.reader_dialect,
                                                self.writer_dialect]),
                                         name='process_response')
        self.w.start()
        return self.w

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if hasattr(self, 'w'):
            if self.w.is_alive():
                self._ui.debug('Terminating Writer')
                self.writer_queue.put_nowait((None, SENTINEL, None))


class WorkUnitGenerator(object):
    """Generates async requests with completion or retry callbacks.

    It uses a queue backed by a batch generator.
    It will pop items for the queue and if its exhausted it will populate the
    queue from the batch generator.
    If a submitted async request was not successfull it gets enqueued again.
    """

    def __init__(self, queue, endpoint, headers, user, api_token, pred_name,
                 fast_mode, ui, max_batch_size, writer_queue):
        self.endpoint = endpoint
        self.headers = headers
        self.user = user
        self.api_token = api_token
        self.queue = queue
        self.pred_name = pred_name
        self.fast_mode = fast_mode
        self._ui = ui
        self.max_batch_size = max_batch_size
        self.writer_queue = writer_queue

    def _response_callback(self, r, batch=None, *args, **kw):
        try:
            if r.status_code == 200:
                try:
                    pickleable_resp = {'elapsed': r.elapsed.total_seconds(),
                                       'text': r.text}
                    self.writer_queue.put((pickleable_resp, batch,
                                           self.pred_name))
                    return
                except Exception as e:
                    self._ui.fatal('{} response error: {}'.format(batch.id, e))
            elif isinstance(r, FakeResponse):
                self._ui.debug('Skipping processing response '
                               'because of FakeResponse')
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
                msg = ('batch {} failed, queued to retry, status_code:{} '
                       'text:{}'.format(batch.id, r.status_code, text))
                self._ui.error(msg)
                self.send_warning_to_ctx(batch, msg)
                self.queue.push(batch)
        except Exception as e:
            msg = 'batch {} - dropping due to: {}, {} records lost'.format(
                batch.id, e, batch.rows)
            self._ui.error(msg)
            self.send_error_to_ctx(batch, msg)

    def has_next(self):
        return self.queue.has_next()

    def send_warning_to_ctx(self, batch, message):
        self._ui.info('WorkUnitGenerator sending WARNING batch_id {} , '
                      'message {}'.format(batch.id, message))
        self.writer_queue.put((QueueMsg.WARNING, batch, message))

    def send_error_to_ctx(self, batch, message):
        self._ui.info('WorkUnitGenerator sending ERROR batch_id {} , '
                      'message {}'.format(batch.id, message))

        self.writer_queue.put((QueueMsg.ERROR, batch, message))

    def __iter__(self):
        for batch in self.queue:
            if batch.id == -1:  # sentinel
                raise StopIteration()
            # if we exhaused our retries we drop the batch
            if batch.rty_cnt == 0:
                msg = ('batch {} exceeded retry limit; '
                       'we lost {} records'.format(
                            batch.id, len(batch.data)))
                self._ui.error(msg)
                self.send_error_to_ctx(batch, msg)
                continue

            if self.fast_mode:
                chunk_formatter = fast_to_csv_chunk
            else:
                chunk_formatter = slow_to_csv_chunk

            todo = [batch]
            while todo:
                batch = todo.pop(0)
                data = chunk_formatter(batch.data, batch.fieldnames)
                if len(data) < self.max_batch_size:
                    self._ui.debug('batch {}-{} transmitting {} bytes'
                                   .format(batch.id, batch.rows, len(data)))

                    hook = partial(self._response_callback, batch=batch)

                    yield requests.Request(
                        method='POST',
                        url=self.endpoint,
                        headers=self.headers,
                        data=data,
                        auth=(self.user, self.api_token),
                        hooks={'response': hook})
                else:
                    if batch.rows < 2:
                        msg = ('batch {} is single row but bigger '
                               'than limit, skipping. We lost {} '
                               'records'.format(batch.id,
                                                len(batch.data)))
                        self._ui.error(msg)
                        self.send_error_to_ctx(batch, msg)
                        continue

                    msg = ('batch {}-{} is too long: {} bytes,'
                           ' splitting'.format(batch.id, batch.rows,
                                               len(data)))
                    self._ui.debug(msg)
                    self.send_warning_to_ctx(batch, msg)
                    split_point = int(batch.rows/2)

                    data1 = batch.data[:split_point]
                    batch1 = Batch(batch.id, split_point, batch.fieldnames,
                                   data1, batch.rty_cnt)
                    todo.append(batch1)

                    data2 = batch.data[split_point:]
                    batch2 = Batch(batch.id + split_point,
                                   batch.rows - split_point,
                                   batch.fieldnames, data2, batch.rty_cnt)
                    todo.append(batch2)


class RunContext(object):
    """A context for a run backed by a persistant store.

    We use a shelve to store the state of the run including
    a journal of processed batches that have been checkpointed.

    Note: we use globs for the shelve files because different
    versions of Python have different file layouts.
    """

    def __init__(self, n_samples, out_file, pid, lid, keep_cols,
                 n_retry, delimiter, dataset, pred_name, ui, file_context,
                 fast_mode, encoding, skip_row_id, output_delimiter):
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
        self._ui = ui
        self.file_context = file_context
        self.fast_mode = fast_mode
        self.encoding = encoding
        self.skip_row_id = skip_row_id
        self.output_delimiter = output_delimiter
        #  dataset_dialect and writer_dialect are set by
        #  investigate_encoding_and_dialect in utils
        self.dialect = csv.get_dialect('dataset_dialect')
        self.writer_dialect = csv.get_dialect('writer_dialect')
        self.scoring_succeeded = False  # Removes shelves when True

    @classmethod
    def create(cls, resume, n_samples, out_file, pid, lid,
               keep_cols, n_retry,
               delimiter, dataset, pred_name, ui,
               fast_mode, encoding, skip_row_id, output_delimiter):
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
                         fast_mode, encoding, skip_row_id, output_delimiter)

    def __enter__(self):
        self._ui.debug('ENTER CALLED ON RUNCONTEXT')
        self.db = shelve.open(self.file_context.file_name, writeback=True)
        if not hasattr(self, 'partitions'):
            self.partitions = []
        return self

    def __exit__(self, type, value, traceback):
        self._ui.debug('EXIT CALLED ON RUNCONTEXT: successes={}'
                         ''.format(self.scoring_succeeded))
        self.db.close()
        if self.out_stream is not None:
            self.out_stream.close()
        if self.scoring_succeeded:
            # success - remove shelve
            self.file_context.clean()

    def open(self):
        self._ui.debug('OPEN CALLED ON RUNCONTEXT')
        self.db = shelve.open(self.file_context.file_name, writeback=True)
        if six.PY2:
            self.out_stream = open(self.out_file, 'ab')
        elif six.PY3:
            self.out_stream = open(self.out_file, 'a', newline='')

    def close(self):
        self._ui.debug('CLOSE CALLED ON RUNCONTEXT')
        self.db.sync()
        self.db.close()
        if self.out_stream is not None:
            self.out_stream.close()

    def checkpoint_batch(self, batch, out_fields, pred):
        """Mark a batch as being processed:
           - write it to the output stream (if necessary pull out columns).
           - put the batch_id into the journal.
        """
        input_delimiter = self.dialect.delimiter
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

            written_fields = []

            if not self.skip_row_id:
                written_fields.append('row_id')

            written_fields += self.keep_cols + out_fields[1:]

            # first column is row_id
            comb = []
            for row, predicted in zip(batch.data, pred):
                if self.fast_mode:
                    # row is a full line, we need to cut it into fields
                    # FIXME this will fail on quoted fields!
                    row = row.rstrip().split(input_delimiter)
                keeps = [row[i] for i in indices]
                output_row = []
                if not self.skip_row_id:
                    output_row.append(predicted[0])
                output_row += keeps + predicted[1:]
                comb.append(output_row)
        else:
            if not self.skip_row_id:
                comb = pred
                written_fields = out_fields
            else:
                written_fields = out_fields[1:]
                comb = [row[1:] for row in pred]

        # if an error happends during/after the append we
        # might end up with inconsistent state
        # TODO write partition files instead of appending
        #  store checksum of each partition and back-check
        writer = csv.writer(self.out_stream, dialect=self.writer_dialect)
        if self.db['first_write']:
            writer.writerow(written_fields)
        writer.writerows(comb)
        self.out_stream.flush()

        self.db['checkpoints'].append((batch.id, batch.rows))

        self.db['first_write'] = False
        self._ui.info('batch {}-{} checkpointed'
                      .format(batch.id, batch.rows))
        self.db.sync()

    def save_error(self, batch, error, bucket="errors"):
        # with self.lock:
        msgs = self.db[bucket].setdefault((batch.id, batch.rows), [])
        msgs.append(error)
        self.db.sync()

    def save_warning(self, batch, error):
        self.save_error(batch, error, "warnings")

    def __getstate__(self):
        """
        On windows we need to pickle the UI instances or create new
        instances inside the subprocesses since there's no fork.
        """
        self.close()
        self.out_stream = None
        # self.db.sync()
        # self.db.close()
        self._ui._next_suffix = 'writer'
        d = self.__dict__.copy()
        return d

    def __setstate__(self, d):
        """
        On windows we need to pickle the UI instances or create new
        instances inside the subprocesses since there's no fork.
        This method is called when unpickling a UI instance.
        It actually creates a new UI that logs to a separate file.
        """
        # from IPython import embed; embed()
        csv.register_dialect('dataset_dialect', d['dialect'])
        csv.register_dialect('writer_dialect', d['writer_dialect'])
        self.__dict__.update(d)
        self.open()


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
        self.db['skip_row_id'] = self.skip_row_id
        self.db['output_delimiter'] = self.output_delimiter
        # list of batch ids that have been processed
        self.db['checkpoints'] = []
        # dictionary of error messages per batch
        self.db['warnings'] = {}
        self.db['errors'] = {}
        # used to check if output file is dirty (ie first write op)
        self.db['first_write'] = True
        self.db.sync()
        if six.PY2:
            self.out_stream = open(self.out_file, 'w+b')
        elif six.PY3:
            self.out_stream = open(self.out_file, 'w+', newline='')
        return self

    def __exit__(self, type, value, traceback):
        super(NewRunContext, self).__exit__(type, value, traceback)

    def batch_generator_args(self):
        """
        returns the arguments needed to set up a BatchGenerator
        In this case it's a fresh start
        """
        already_processed_batches = set()
        args = [self.dataset, self.n_samples, self.n_retry, self.delimiter,
                self._ui, self.fast_mode, self.encoding,
                already_processed_batches]
        return args


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
        if self.db['skip_row_id'] != self.skip_row_id:
            raise ShelveError('skip_row_id mismatch: should be {} but was {}'
                              .format(self.db['skip_row_id'],
                                      self.skip_row_id))
        if self.db['output_delimiter'] != self.output_delimiter:
            raise ShelveError('output_delimiter mismatch: should be {}'
                              ' but was {}'.format(
                                    self.db['output_delimiter'],
                                    self.output_delimiter))

        if six.PY2:
            self.out_stream = open(self.out_file, 'ab')
        elif six.PY3:
            self.out_stream = open(self.out_file, 'a', newline='')

        self._ui.info('resuming a shelved run with {} checkpointed batches'
                      .format(len(self.db['checkpoints'])))
        return self

    def __exit__(self, type, value, traceback):
        super(OldRunContext, self).__exit__(type, value, traceback)

    def batch_generator_args(self):
        """
        returns the arguments needed to set up a BatchGenerator
        In this case some batches may have already run
        """
        already_processed_batches = set(self.db['checkpoints'])
        args = [self.dataset, self.n_samples, self.n_retry, self.delimiter,
                self._ui, self.fast_mode, self.encoding,
                already_processed_batches]
        return args


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

            warn_if_redirected(r, ui)
            if r.status_code == 400:
                # client error -- maybe schema is wrong
                try:
                    msg = r.json()['status']
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
        warn_if_redirected(r, ui)
        ui.debug("Failed authorization response \n{!r}".format(content))
        ui.fatal('authorization failed -- please check project id and model '
                 'id permissions: {}'.format(status))
    else:
        ui.debug('authorization has succeeded')


def run_batch_predictions(base_url, base_headers, user, pwd,
                          api_token, create_api_token,
                          pid, lid, n_retry, concurrent,
                          resume, n_samples,
                          out_file, keep_cols, delimiter,
                          dataset, pred_name,
                          timeout, ui, fast_mode, auto_sample,
                          dry_run, encoding, skip_dialect,
                          skip_row_id=False,
                          output_delimiter=None,
                          max_batch_size=None):

    if max_batch_size is None:
        max_batch_size = MAX_BATCH_SIZE

    multiprocessing.freeze_support()
    t1 = time()
    queue_size = concurrent * 2
    #  provide version info and system info in user-agent
    base_headers['User-Agent'] = 'datarobot_batch_scoring/{}|' \
                                 'Python/{}|{}|system/{}|concurrency/{}' \
                                 ''.format(__version__,
                                           sys.version.split(' ')[0],
                                           requests.utils.default_user_agent(),
                                           platform.system(),
                                           concurrent)

    with ExitStack() as stack:
        if os.name is 'nt':
            #  Windows requires an additional manager process. The locks
            #  and queues it creates are proxies for objects that exist within
            #  the manager itself. It does not perform as well so we only
            #  use it when necessary.
            conc_manager = stack.enter_context(multiprocessing.Manager())
        else:
            #  You're on a nix of some sort and don't need a manager process.
            conc_manager = multiprocessing
        queue = conc_manager.Queue(queue_size)
        deque = conc_manager.Queue(queue_size)
        rlock = conc_manager.RLock()
        writer_queue = conc_manager.Queue(queue_size * 2)
        if not api_token:
            if not pwd:
                pwd = ui.getpass()
            try:
                api_token = acquire_api_token(base_url, base_headers, user,
                                              pwd, create_api_token, ui)
            except Exception as e:
                ui.fatal(str(e))

        base_headers['content-type'] = 'text/csv; charset=utf8'
        endpoint = base_url + '/'.join((pid, lid, 'predict'))
        encoding = investigate_encoding_and_dialect(
            dataset=dataset,
            sep=delimiter, ui=ui,
            fast=fast_mode,
            encoding=encoding,
            skip_dialect=skip_dialect,
            output_delimiter=output_delimiter)
        if auto_sample:
            #  override n_sample
            n_samples = auto_sampler(dataset, encoding, ui)
            ui.info('auto_sample: will use batches of {} rows'
                    ''.format(n_samples))
        # Make a sync request to check authentication and fail early
        first_row = peek_row(dataset, delimiter, ui, fast_mode, encoding)
        ui.debug('First row for auth request: {}'.format(first_row))
        if fast_mode:
            chunk_formatter = fast_to_csv_chunk
        else:
            chunk_formatter = slow_to_csv_chunk
        first_row_data = chunk_formatter(first_row.data, first_row.fieldnames)
        first_row = first_row._replace(data=first_row_data)
        if not dry_run:
            authorize(user, api_token, n_retry, endpoint, base_headers,
                      first_row, ui)

        ctx = stack.enter_context(
            RunContext.create(resume, n_samples, out_file, pid,
                              lid, keep_cols, n_retry, delimiter,
                              dataset, pred_name, ui, fast_mode,
                              encoding, skip_row_id, output_delimiter))
        network = stack.enter_context(Network(concurrent, timeout, ui))
        n_batches_checkpointed_init = len(ctx.db['checkpoints'])
        ui.debug('number of batches checkpointed initially: {}'
                 .format(n_batches_checkpointed_init))

        # make the queue twice as big as the

        MGBQ = MultiprocessingGeneratorBackedQueue(ui, queue, deque, rlock)
        batch_generator_args = ctx.batch_generator_args()
        shovel = Shovel(queue, batch_generator_args, ui)
        ui.info('Shovel go...')
        ctx.close()
        shovel.go()
        writer = stack.enter_context(WriterProcess(ui, ctx, writer_queue,
                                                   queue, deque))
        writer_proc = writer.go()
        work_unit_gen = WorkUnitGenerator(MGBQ,
                                          endpoint,
                                          headers=base_headers,
                                          user=user,
                                          api_token=api_token,
                                          pred_name=pred_name,
                                          fast_mode=fast_mode,
                                          ui=ui,
                                          max_batch_size=max_batch_size,
                                          writer_queue=writer_queue)
        t0 = time()
        i = 0

        if dry_run:
            for _ in work_unit_gen:
                pass
            ui.info('dry-run complete | time elapsed {}s'.format(time() - t0))
            ui.info('dry-run complete | total time elapsed {}s'.format(
                time() - t1))
        else:
            for r in network.perform_requests(work_unit_gen):
                if r is True:
                    ui.debug('Network requests finished')
                    break
                i += 1
                ui.info('{} responses sent | time elapsed {}s'
                        .format(i, time() - t0))

            sleep(0.5)
            ui.debug('sending Sentinel to writer process')
            writer_queue.put((None, SENTINEL, None))
            writer_proc.join(20)
            if writer_proc.exitcode is 0:
                ui.debug('writer process exited successfully')
            else:
                ui.debug('writer process did not exit properly: '
                         'returncode="{}"'.format(writer_proc.exitcode))

            ctx.open()
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

            total_done = 0
            for _, batch_len in ctx.db["checkpoints"]:
                total_done += batch_len

            total_lost = 0
            for bucket in ("warnings", "errors"):
                ui.info('==== Scoring {} ===='.format(bucket))
                if ctx.db[bucket]:
                    msg_data = ctx.db[bucket]
                    msg_keys = sorted(msg_data.keys())
                    for batch_id in msg_keys:
                        first = True
                        for msg in msg_data[batch_id]:
                            if first:
                                first = False
                                ui.info("{}: {}".format(batch_id, msg))
                            else:
                                ui.info("        {}".format(msg))

                        if bucket == "errors":
                            total_lost += batch_id[1]

            ui.info('==== Total stats ===='.format(bucket))
            ui.info("done: {} lost: {}".format(total_done, total_lost))
            if total_lost is 0:
                ctx.scoring_succeeded = True
