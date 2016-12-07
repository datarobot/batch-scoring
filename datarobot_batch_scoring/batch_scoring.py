# -*- coding: utf-8 -*-
from __future__ import print_function

import multiprocessing
import os
import platform
import sys
from time import time

import requests
import six

from datarobot_batch_scoring import __version__
from datarobot_batch_scoring.consts import WriterQueueMsg
from datarobot_batch_scoring.network import Network
from datarobot_batch_scoring.reader import (fast_to_csv_chunk,
                                            slow_to_csv_chunk, peek_row,
                                            Shovel, auto_sampler,
                                            investigate_encoding_and_dialect)
from datarobot_batch_scoring.utils import (acquire_api_token, authorize)
from datarobot_batch_scoring.writer import WriterProcess, RunContext

if six.PY2:  # pragma: no cover
    from contextlib2 import ExitStack
elif six.PY3:  # pragma: no cover
    from contextlib import ExitStack


MAX_BATCH_SIZE = 5 * 1024 ** 2


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
        # progress_queue = conc_manager.Queue()

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

        n_batches_checkpointed_init = len(ctx.db['checkpoints'])
        ui.debug('number of batches checkpointed initially: {}'
                 .format(n_batches_checkpointed_init))

        batch_generator_args = ctx.batch_generator_args()
        shovel = stack.enter_context(Shovel(network_queue,
                                            batch_generator_args,
                                            ui))
        ui.info('Shovel go...')
        shovel.go()

        network = stack.enter_context(Network(concurrency=concurrent,
                                              timeout=timeout,
                                              ui=ui,
                                              network_queue=network_queue,
                                              network_deque=network_deque,
                                              writer_queue=writer_queue,
                                              endpoint=endpoint,
                                              headers=base_headers,
                                              user=user,
                                              api_token=api_token,
                                              pred_name=pred_name,
                                              fast_mode=fast_mode,
                                              max_batch_size=max_batch_size,
                                              compression=compression
                                              ))
        t0 = time()

        if dry_run:
            network.go(dry_run=True)
            ui.info('dry-run complete | time elapsed {}s'.format(time() - t0))
            ui.info('dry-run complete | total time elapsed {}s'.format(
                time() - t1))
            ctx.scoring_succeeded = True
            return

        exit_code = None
        writer = stack.enter_context(WriterProcess(ui, ctx, writer_queue,
                                                   network_queue,
                                                   network_deque))
        ui.info('Writer go...')
        writer_proc = writer.go()

        ret, n_requests, n_consumed = network.go()
        if ret is True:
            ui.debug('Network requests successfully finished')
        else:
            exit_code = 1

        ui.debug('sending Sentinel to writer process')
        writer_queue.put((WriterQueueMsg.SENTINEL, {}))
        writer_proc.join(30)
        if writer_proc.exitcode is 0:
            ui.debug('writer process exited successfully')
        else:
            ui.debug('writer process did not exit properly: '
                     'returncode="{}"'.format(writer_proc.exitcode))

        ctx.open()
        ui.debug('number of batches checkpointed initially: {}'
                 .format(n_batches_checkpointed_init))
        ui.debug('list of checkpointed batches: {}'
                 .format(sorted(ctx.db['checkpoints'])))
        n_batches_checkpointed = (len(ctx.db['checkpoints']) -
                                  n_batches_checkpointed_init)
        ui.debug('number of batches checkpointed: {}'
                 .format(n_batches_checkpointed))
        n_batches_not_checkpointed = (n_consumed -
                                      n_batches_checkpointed)
        batches_missing = n_batches_not_checkpointed > 0
        if batches_missing:
            ui.error(('scoring incomplete, {} batches were dropped | '
                      'time elapsed {}s')
                     .format(n_batches_not_checkpointed, time() - t0))
            exit_code = 1
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
        else:
            exit_code = 1

        return exit_code
