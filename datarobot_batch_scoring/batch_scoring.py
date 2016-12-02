# -*- coding: utf-8 -*-
from __future__ import print_function

import collections
import io
import json
import multiprocessing
import os
import platform
import sys
from functools import partial
from gzip import GzipFile
from time import time

import requests
import six
from six.moves import queue

from datarobot_batch_scoring import __version__
from datarobot_batch_scoring.consts import Batch, SENTINEL, ERROR_SENTINEL
from datarobot_batch_scoring.network import Network, FakeResponse
from datarobot_batch_scoring.reader import (fast_to_csv_chunk,
                                            slow_to_csv_chunk, peek_row,
                                            Shovel, auto_sampler,
                                            investigate_encoding_and_dialect)
from datarobot_batch_scoring.utils import acquire_api_token
from datarobot_batch_scoring.writer import QueueMsg, WriterProcess, RunContext

if six.PY2:  # pragma: no cover
    from contextlib2 import ExitStack
elif six.PY3:  # pragma: no cover
    from contextlib import ExitStack
    ifilter = filter
    # for successful py2exe dist package

Prediction = collections.namedtuple('Prediction', 'fieldnames data')

MAX_BATCH_SIZE = 5 * 1024 ** 2


def compress(data):
    buf = io.BytesIO()
    with GzipFile(fileobj=buf, mode='wb', compresslevel=2) as f:
        f.write(data)
    return buf.getvalue()


class MultiprocessingGeneratorBackedQueue(object):
    """A queue that is backed by a generator.

    When the queue is exhausted it repopulates from the generator.
    """
    def __init__(self, ui, queue, deque):
        self.n_consumed = 0
        self.queue = queue
        self.deque = deque
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


class WorkUnitGenerator(object):
    """Generates async requests with completion or retry callbacks.

    It uses a queue backed by a batch generator.
    It will pop items for the queue and if its exhausted it will populate the
    queue from the batch generator.
    If a submitted async request was not successfull it gets enqueued again.
    """

    def __init__(self, queue, endpoint, headers, user, api_token, pred_name,
                 fast_mode, ui, max_batch_size, writer_queue, compression):
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
        self.compression = compression

    def _response_callback(self, r, batch=None, *args, **kw):
        try:
            if r.status_code == 200:
                pickleable_resp = {'elapsed': r.elapsed.total_seconds(),
                                   'text': r.text}
                self.writer_queue.put((pickleable_resp, batch,
                                       self.pred_name))
                return
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
                starting_size = sys.getsizeof(data)
                if starting_size < self.max_batch_size:
                    if self.compression:
                        data = compress(data)
                        self._ui.debug(
                            'batch {}-{} transmitting {} byte - space savings '
                            '{}%'.format(batch.id, batch.rows,
                                         sys.getsizeof(data),
                                         '%.2f' % float(1 -
                                                        (sys.getsizeof(data) /
                                                         starting_size))))
                    else:
                        self._ui.debug('batch {}-{} transmitting {} bytes'
                                       .format(batch.id, batch.rows,
                                               starting_size))
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
                    todo.sort()


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


def authorize(user, api_token, n_retry, endpoint, base_headers, batch, ui,
              compression=None):
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
            r = requests.post(endpoint, headers=base_headers,
                              data=data,
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
                          max_batch_size=None, compression=None):

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
        network_queue = conc_manager.Queue(queue_size)
        network_deque = conc_manager.Queue(queue_size)
        writer_queue = conc_manager.Queue(queue_size)
        if not api_token:
            if not pwd:
                pwd = ui.getpass()
            try:
                api_token = acquire_api_token(base_url, base_headers, user,
                                              pwd, create_api_token, ui)
            except Exception as e:
                ui.fatal(str(e))
        base_headers['content-type'] = 'text/csv; charset=utf8'
        if compression:
            base_headers['Content-Encoding'] = 'gzip'
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
        if keep_cols:
            if not all(c in first_row.fieldnames for c in keep_cols):
                ui.fatal('keep_cols "{}" not in columns {}.'.format(
                    [c for c in keep_cols if c not in first_row.fieldnames],
                    first_row.fieldnames))
        if not dry_run:
            authorize(user, api_token, n_retry, endpoint, base_headers,
                      first_row, ui, compression=compression)

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

        MGBQ = MultiprocessingGeneratorBackedQueue(ui, network_queue,
                                                   network_deque)
        batch_generator_args = ctx.batch_generator_args()
        shovel = Shovel(network_queue, batch_generator_args, ui)
        ui.info('Shovel go...')
        shovel.go()
        writer = stack.enter_context(WriterProcess(ui, ctx, writer_queue,
                                                   network_queue,
                                                   network_deque))
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
                                          writer_queue=writer_queue,
                                          compression=compression)
        t0 = time()
        i = 0

        if dry_run:
            for _ in work_unit_gen:
                pass
            ui.info('dry-run complete | time elapsed {}s'.format(time() - t0))
            ui.info('dry-run complete | total time elapsed {}s'.format(
                time() - t1))
            ctx.scoring_succeeded = True
        else:
            for r in network.perform_requests(work_unit_gen):
                if r is True:
                    ui.debug('Network requests finished')
                    break
                i += 1
                ui.info('{} responses sent | time elapsed {}s'
                        .format(i, time() - t0))

            ui.debug('sending Sentinel to writer process')
            writer_queue.put((None, SENTINEL, None))
            writer_proc.join(30)
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
                ui.error(('scoring incomplete, {} batches were dropped | '
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
