import csv
import glob
import hashlib
import json
import multiprocessing
import operator
import os
import shelve
import signal
import sys
from functools import reduce

import six
from six.moves import queue

from datarobot_batch_scoring.consts import SENTINEL, QueueMsg, TargetType


class ShelveError(Exception):
    pass


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

    def checkpoint_batch(self, batch, out_fields, pred):
        """Mark a batch as being processed:
           - write it to the output stream (if necessary pull out columns).
           - put the batch_id into the journal.
        """
        input_delimiter = self.dialect.delimiter
        if self.keep_cols:
            # stack columns

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
        msgs = self.db[bucket].setdefault((batch.id, batch.rows), [])
        msgs.append(error)
        self.db.sync()

    def save_warning(self, batch, error):
        self.save_error(batch, error, "warnings")

    def __getstate__(self):
        self.close()
        self.out_stream = None
        d = self.__dict__.copy()
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.open()

    def open(self):
        self._ui.debug('OPEN CALLED ON RUNCONTEXT')
        csv.register_dialect('dataset_dialect', **self.dialect)
        csv.register_dialect('writer_dialect', **self.writer_dialect)
        self.dialect = csv.get_dialect('dataset_dialect')
        self.writer_dialect = csv.get_dialect('writer_dialect')
        self.db = shelve.open(self.file_context.file_name, writeback=True)
        if six.PY2:
            self.out_stream = open(self.out_file, 'ab')
        elif six.PY3:
            self.out_stream = open(self.out_file, 'a', newline='')

    def close(self):
        self._ui.debug('CLOSE CALLED ON RUNCONTEXT')
        self.dialect = csv.get_dialect('dataset_dialect')
        self.writer_dialect = csv.get_dialect('writer_dialect')
        values = ['delimiter', 'doublequote', 'escapechar', 'lineterminator',
                  'quotechar', 'quoting', 'skipinitialspace', 'strict']
        self.dialect = {k: getattr(self.dialect, k) for k in values if
                        hasattr(self.dialect, k)}
        self.writer_dialect = {k: getattr(self.writer_dialect, k) for k
                               in values if hasattr(self.writer_dialect, k)}
        self.db.sync()
        self.db.close()
        if self.out_stream is not None:
            self.out_stream.close()


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


class WriterProcess(object):
    def __init__(self, ui, ctx, writer_queue, queue, deque):
        self._ui = ui
        self.ctx = ctx
        self.writer_queue = writer_queue
        self.queue = queue
        self.deque = deque

    def deque_failed_batch(self, batch):
        # we retry a batch - decrement retry counter
        batch = batch._replace(rty_cnt=batch.rty_cnt - 1)
        try:
            self.deque.put(batch, block=True)
        except queue.Empty:
            self._ui.error('Dropping {} due to backfill queue full.'.format(
                batch))

    def unpack_request_object(self, request, batch):
        """

        :param request: text of response from request object. contains JSON
        :param batch: batch NamedTuple
        :return: deserialize text to JSON and returns JSON as python objects
        """
        try:
            result = json.loads(request['text'])  # replace with r.content
            elapsed_total_seconds = request['elapsed']
        except Exception as e:
            self._ui.warning('{} response error: {} -- retry'
                             ''.format(batch.id, e))
            self.deque_failed_batch(batch)
            return False  # unpack failed for this batch
        exec_time = result['execution_time']
        self._ui.debug(('successful response {}-{}: exec time {:.0f}msec | '
                        'round-trip: {:.0f}msec'
                        '').format(batch.id, batch.rows, exec_time,
                                   elapsed_total_seconds * 1000))
        return result

    def format_result_data(self, result, batch, pred_name):
        """
        :param result: JSON data returned from pred server
        :param batch: batch NamedTuple
        :param pred_name: name of prediction column
        :return: out_fields, pred

        Takes the result data and formats it.
        """
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
        return (out_fields, pred)

    def sigterm_handler(self, _signo, _stack_frame):
        self._ui.debug('WriterProcess.sigterm_handler received signal '
                       '"{}". Exiting 1'.format(_signo))
        sys.exit(1)

    def process_response(self):
        """Process a successful request. """
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)

        self._ui.debug('Writer Process started - {}'
                       ''.format(multiprocessing.current_process().name))
        success = False
        try:
            while True:
                try:
                    (request, batch, pred_name) = \
                        self.writer_queue.get_nowait()
                except queue.Empty:
                    (request, batch, pred_name) = self.writer_queue.get()

                if request == QueueMsg.ERROR:
                    # pred_name is a message if ERROR or WARNING
                    self._ui.debug('Writer ERROR')
                    self.ctx.save_error(batch, error=pred_name)
                    continue
                if request == QueueMsg.WARNING:
                    self._ui.debug('Writer WARNING')
                    self.ctx.save_warning(batch, error=pred_name)
                    continue
                if batch.id == SENTINEL.id:
                    self._ui.debug('Writer received SENTINEL')
                    break
                result = self.unpack_request_object(request, batch)
                if result is False:  # unpack_request_object failed
                    continue

                (out_fields, pred) = self.format_result_data(result, batch,
                                                             pred_name)

                self.ctx.checkpoint_batch(batch, out_fields, pred)
                self._ui.debug('Writer Queue queue length: {}'
                               ''.format(self.writer_queue.qsize()))

            self._ui.debug('---Writer Exiting---')
            success = True
        except Exception as e:
            # Note this won't catch SystemExit which is raised by
            # sigterm_handler because it's based on BaseException
            self._ui.error('Writer Process error: batch={}, error={}'
                           ''.format(batch.id, e))

        finally:
            for o in [self.ctx, self.queue, self.writer_queue, self.deque,
                      self._ui]:
                if hasattr(o, 'close'):
                    # On Windows the Queue doesn't have a close attr
                    o.close()
            if success:
                sys.exit(0)
            else:
                sys.exit(1)

    def go(self):
        self._ui.set_next_UI_name('writer')
        if os.name is not 'nt':  # happens when pickled on Windows
            self.ctx.close()
        self.proc = \
            multiprocessing.Process(target=run_subproc_cls_inst,
                                    args=([self._ui, self.ctx,
                                           self.writer_queue,
                                           self.queue, self.deque]),
                                    name='Writer_Proc')
        self.proc.start()
        return self.proc

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if hasattr(self, 'proc'):
            if self.proc.is_alive():
                self._ui.debug('Terminating Writer')
                self.writer_queue.put_nowait((None, SENTINEL, None))


def run_subproc_cls_inst(_ui, ctx, writer_queue, queue, deque):
    """
    This was intended to be a staticmethod of WriterProcess but
    python 2.7 on Windows can't find it unless it's at module level

    This should only be run in the spawned process to launch the
    WriterProcess class instance.
    """
    if str(multiprocessing.current_process().name) != 'Writer_Proc':
        _ui.warning('WriterProcess.run_subproc_cls_inst called in '
                    'process named: "{}"'
                    ''.format(multiprocessing.current_process().name))
    if os.name is not 'nt':  # this happens automatically on Windows
        ctx.open()
    WriterProcess(_ui, ctx, writer_queue, queue, deque).process_response()