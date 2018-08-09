import csv
import glob
import hashlib
import multiprocessing
import operator
import os
import shelve
import signal
import sys
from functools import reduce
from time import time

import six
from six.moves import queue

from datarobot_batch_scoring.consts import SENTINEL, \
    WriterQueueMsg, ProgressQueueMsg, REPORT_INTERVAL
from datarobot_batch_scoring.utils import get_rusage


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
        self.is_open = False  # Removes shelves when True

    @classmethod
    def create(cls, resume, n_samples, out_file, pid, lid,
               keep_cols, n_retry,
               delimiter, dataset, pred_name, ui,
               fast_mode, encoding, skip_row_id, output_delimiter):
        """Factory method for run contexts.

        Either resume or start a new one.
        """
        file_context = ContextFile(pid, lid, n_samples, keep_cols)
        if resume is None and file_context.exists():
            resume = ui.prompt_yesno('Existing run found. Resume')
        if resume:
            ctx_class = OldRunContext
        else:
            ctx_class = NewRunContext

        return ctx_class(n_samples, out_file, pid, lid, keep_cols, n_retry,
                         delimiter, dataset, pred_name, ui, file_context,
                         fast_mode, encoding, skip_row_id, output_delimiter)

    def __enter__(self):
        assert(not self.is_open)
        self.is_open = True
        self._ui.debug('ENTER CALLED ON RUNCONTEXT')
        self.db = shelve.open(self.file_context.file_name, writeback=True)
        if not hasattr(self, 'partitions'):
            self.partitions = []
        return self

    def __exit__(self, type, value, traceback):
        if not self.is_open:
            self._ui.debug('EXIT CALLED ON CLOSED RUNCONTEXT: successes={}'
                           ''.format(self.scoring_succeeded))
            return
        self.is_open = False
        self._ui.debug('EXIT CALLED ON RUNCONTEXT: successes={}'
                       ''.format(self.scoring_succeeded))
        self.db.close()
        if self.out_stream is not None:
            self.out_stream.close()
        if self.scoring_succeeded:
            # success - remove shelve
            self.file_context.clean()

    def checkpoint_batch(self, batch, written_fields, comb):
        """Mark a batch as being processed:
           - write it to the output stream (if necessary pull out columns).
           - put the batch_id into the journal.
        """

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
        assert(not self.is_open)
        self.out_stream = None
        d = self.__dict__.copy()
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.open()

    def open(self):
        if self.is_open:
            self._ui.debug('OPEN CALLED ON ALREADY OPEN RUNCONTEXT')
            return
        self.is_open = True
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
        if not self.is_open:
            self._ui.debug('CLOSE CALLED ON CLOSED RUNCONTEXT')
            return
        self.is_open = False
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
                self._ui.info('Removed {}'.format(self.out_file))
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


def decode_writer_state(ch):
    return {
        b"-": "Initial",
        b"I": "Idle",
        b"G": "Getting from queue",
        b"D": "Done",
        b"W": "Writing",
    }.get(ch)


class WriterProcess(object):
    def __init__(self, ui, ctx, writer_queue, queue, deque, progress_queue,
                 abort_flag, writer_status, response_handlers):
        self._ui = ui
        self.ctx = ctx
        self.writer_queue = writer_queue
        self.queue = queue
        self.deque = deque
        self.progress_queue = progress_queue
        self.abort_flag = abort_flag
        self.response_handlers = response_handlers
        self.local_abort_flag = False
        self.writer_status = writer_status
        self.proc = None

    def deque_failed_batch(self, batch):
        # we retry a batch - decrement retry counter
        batch = batch._replace(rty_cnt=batch.rty_cnt - 1)
        try:
            self.deque.put(batch, block=True)
        except queue.Empty:
            self._ui.error('Dropping {} due to backfill queue full.'.format(
                batch))
            self.ctx.save_error(batch, error="Backfill queue full")

    def exit_fast(self, a, b):
        self.local_abort_flag = True

    def process_response(self):
        signal.signal(signal.SIGINT, self.exit_fast)
        signal.signal(signal.SIGTERM, self.exit_fast)

        """Process a successful request. """
        self._ui.debug('Writer Process started - {}'
                       ''.format(multiprocessing.current_process().name))

        rows_done = sum(rows for _, rows in self.ctx.db['checkpoints'])

        success = False
        processed = 0
        written = 0
        idle_cycles = 0
        last_report = time()

        try:
            while True:
                if self.abort_flag.value or self.local_abort_flag:
                    self._ui.debug('abort requested')
                    break
                try:
                    if idle_cycles > 2:
                        self.writer_status.value = b"I"
                    else:
                        self.writer_status.value = b"G"
                    msg, args = self.writer_queue.get(timeout=1)
                except queue.Empty:
                    idle_cycles += 1
                    continue
                idle_cycles = 0
                self.writer_status.value = b"W"

                if msg == WriterQueueMsg.CTX_ERROR:
                    # pred_name is a message if ERROR or WARNING
                    self._ui.debug('Writer ERROR')
                    self.ctx.save_error(args["batch"], error=args["error"])
                    continue
                elif msg == WriterQueueMsg.CTX_WARNING:
                    self._ui.debug('Writer WARNING')
                    self.ctx.save_warning(args["batch"], error=args["error"])
                    continue
                elif msg == WriterQueueMsg.SENTINEL:
                    self._ui.debug('Writer received SENTINEL')
                    break
                elif msg == WriterQueueMsg.RESPONSE:
                    processed += 1
                    batch = args["batch"]

                    unpack_data, format_data = self.response_handlers
                    try:
                        data, exec_time, elapsed_seconds = \
                            unpack_data(args['request'])
                        debug_msg = ('successful response {}-{}: exec time '
                                     '{:.0f}msec | round-trip: {:.0f}msec')
                        self._ui.debug(
                            debug_msg.format(batch.id, batch.rows, exec_time,
                                             elapsed_seconds * 1000))
                    except Exception as e:
                        self._ui.warning('{} response parse error: {} -- retry'
                                         ''.format(batch.id, e))
                        self.deque_failed_batch(batch)
                        continue

                    try:
                        written_fields, comb = format_data(
                            data, batch,
                            pred_name=self.ctx.pred_name,
                            keep_cols=self.ctx.keep_cols,
                            skip_row_id=self.ctx.skip_row_id,
                            fast_mode=self.ctx.fast_mode,
                            delimiter=self.ctx.dialect.delimiter)
                    except Exception as e:
                        self._ui.fatal(e)

                    self.ctx.checkpoint_batch(batch, written_fields, comb)
                    written += 1
                    rows_done += batch.rows

                    if time() - last_report > REPORT_INTERVAL:
                        self.progress_queue.put((
                            ProgressQueueMsg.WRITER_PROGRESS, {
                                "processed": processed,
                                "written": written,
                                "rows": rows_done,
                                "rusage": get_rusage()
                            }))
                        last_report = time()
                else:
                    self._ui.error('Unknown Writer Queue msg: "{}", args={}'
                                   ''.format(msg, args))

            self._ui.debug('---Writer Exiting---')

            success = True
            if self.local_abort_flag:
                success = False

        except Exception as e:
            # Note this won't catch SystemExit which is raised by
            # sigterm_handler because it's based on BaseException
            self._ui.error('Writer Process error: batch={}, error={}'
                           ''.format(batch.id, e))

        finally:
            self.writer_status.value = b"D"
            self.progress_queue.put((ProgressQueueMsg.WRITER_DONE, {
                "ret": success,
                "processed": processed,
                "written": written,
                "rows": rows_done,
                "rusage": get_rusage()
            }))
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
        self.ctx.close()      # handover it to Writer from top-level
        self.proc = \
            multiprocessing.Process(target=run_subproc_cls_inst,
                                    args=([self._ui, self.ctx,
                                           self.writer_queue,
                                           self.queue,
                                           self.deque,
                                           self.progress_queue,
                                           self.abort_flag,
                                           self.writer_status,
                                           self.response_handlers]),
                                    name='Writer_Proc')
        self.proc.start()
        return self.proc

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.proc and self.proc.is_alive():
            self._ui.debug('Terminating Writer')
            self.writer_queue.put_nowait((None, SENTINEL, None))


def run_subproc_cls_inst(_ui, ctx, writer_queue, queue, deque,
                         progress_queue, abort_flag, writer_status,
                         response_handlers):
    """
    This was intended to be a staticmethod of WriterProcess but
    python 2.7 on Windows can't find it unless it's at module level

    This should only be run in the spawned process to launch the
    WriterProcess class instance.
    """
    process_name = str(multiprocessing.current_process().name)
    if process_name != 'Writer_Proc':
        _ui.warning('WriterProcess.run_subproc_cls_inst called in '
                    'process named: "{}"'
                    ''.format(process_name))
    ctx.open()
    WriterProcess(_ui, ctx, writer_queue, queue, deque,
                  progress_queue, abort_flag, writer_status,
                  response_handlers).process_response()
