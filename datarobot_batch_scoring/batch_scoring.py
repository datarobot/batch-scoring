# -*- coding: utf-8 -*-
from __future__ import print_function

import csv
import multiprocessing
import os
import platform
import signal
import sys
import threading
from multiprocessing.managers import SyncManager
from time import time

import requests
import six
from six.moves import queue

from datarobot_batch_scoring import __version__
from datarobot_batch_scoring.api_response_handlers import \
    get_response_handlers_from_url
from datarobot_batch_scoring.consts import (WriterQueueMsg,
                                            ProgressQueueMsg,
                                            SENTINEL,
                                            REPORT_INTERVAL)
from datarobot_batch_scoring.network import Network, DryRunNetworkWorker
from datarobot_batch_scoring.reader import (fast_to_csv_chunk,
                                            slow_to_csv_chunk, peek_row,
                                            Shovel, auto_sampler,
                                            investigate_encoding_and_dialect,
                                            decode_reader_state)
from datarobot_batch_scoring.utils import (acquire_api_token,
                                           make_validation_call)
from datarobot_batch_scoring.writer import (WriterProcess, RunContext,
                                            decode_writer_state)

if six.PY2:  # pragma: no cover
    from contextlib2 import ExitStack
elif six.PY3:  # pragma: no cover
    from contextlib import ExitStack


MAX_BATCH_SIZE = 5 * 1024 ** 2


def manager_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def format_usage(rusage):
    if not rusage:
        return ""
    else:
        return " User time: {utime:.3f} System time: {stime:.3f} " \
               "RSS: {rss}".format(**rusage)


def my_os_cannot_handle_life_in_the_fast_lane():
    if os.name == 'nt':
        return True
    else:
        return False


def run_batch_predictions(base_url, base_headers, user, pwd,
                          api_token, create_api_token,
                          pid, lid, import_id, n_retry, concurrent,
                          resume, n_samples,
                          out_file, keep_cols, delimiter,
                          dataset, pred_name,
                          timeout, ui, fast_mode, auto_sample,
                          dry_run, encoding, skip_dialect,
                          skip_row_id=False,
                          output_delimiter=None,
                          max_batch_size=None, compression=None,
                          field_size_limit=None,
                          verify_ssl=True):

    if field_size_limit is not None:
        csv.field_size_limit(field_size_limit)

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
        if my_os_cannot_handle_life_in_the_fast_lane():
            #  Windows requires an additional manager process. The locks
            #  and queues it creates are proxies for objects that exist within
            #  the manager itself. It does not perform as well so we only
            #  use it when necessary.

            manager = SyncManager()
            manager.start(initializer=manager_init)

            conc_manager = stack.enter_context(manager)
        else:
            #  You're on a nix of some sort and don't need a manager process.
            conc_manager = multiprocessing
        network_queue = conc_manager.Queue(queue_size)
        network_deque = conc_manager.Queue(queue_size)
        writer_queue = conc_manager.Queue(queue_size)
        progress_queue = conc_manager.Queue()

        shovel_status = conc_manager.Value('c', b'-', lock=False)
        network_status = conc_manager.Value('c', b'-', lock=False)
        writer_status = conc_manager.Value('c', b'-', lock=False)
        abort_flag = conc_manager.Value('b', 0, lock=False)

        base_headers['content-type'] = 'text/csv; charset=utf8'
        if compression:
            base_headers['Content-Encoding'] = 'gzip'
        if import_id:
            endpoint = base_url + '/'.join((import_id, 'predict'))
        else:
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
            # If any columns appear in `keep_cols` that are not in
            # `first_row.fieldnames`, it is a fatal error.
            extra_cols = set(keep_cols) - set(first_row.fieldnames)
            if extra_cols:
                msg = 'keep_cols "{}" not in columns {}.'.format(
                    list(sorted(extra_cols)),
                    first_row.fieldnames)
                ui.fatal(msg)

        if not dry_run:

            if not (api_token or import_id):
                try:
                    api_token = acquire_api_token(base_url, base_headers, user,
                                                  pwd, create_api_token, ui)
                except Exception as e:
                    ui.fatal(str(e))

            make_validation_call(user, api_token, n_retry, endpoint,
                                 base_headers, first_row, ui,
                                 compression=compression,
                                 verify_ssl=verify_ssl)

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
                                            progress_queue,
                                            shovel_status,
                                            abort_flag,
                                            batch_generator_args,
                                            ui))
        ui.info('Reader go...')
        shovel_proc = shovel.go()

        t0 = time()

        if dry_run:
            network = stack.enter_context(DryRunNetworkWorker(
                concurrency=concurrent,
                timeout=timeout,
                ui=ui,
                network_queue=network_queue,
                network_deque=network_deque,
                writer_queue=writer_queue,
                progress_queue=progress_queue,
                abort_flag=abort_flag,
                network_status=network_status,
                endpoint=endpoint,
                headers=base_headers,
                user=user,
                api_token=api_token,
                pred_name=pred_name,
                fast_mode=fast_mode,
                max_batch_size=max_batch_size,
                compression=compression,
                verify_ssl=verify_ssl))
            network.go()
            ui.info('dry-run complete | time elapsed {}s'.format(time() - t0))
            ui.info('dry-run complete | total time elapsed {}s'.format(
                time() - t1))
            i = 0
            while True:
                if not shovel_proc.is_alive():
                    break

                if i == 0:
                    ui.info("Waiting for shovel process exit")
                elif i == 10:
                    ui.info("Sending terminate signal to shovel process")
                    shovel_proc.terminate()
                elif i == 20:
                    ui.error("Sending kill signal to shovel process")
                    os.kill(shovel_proc.pid, 9)
                elif i == 30:
                    ui.error("Reader process was not exited,"
                             " processing anyway.")
                    break
                i += 1
                try:
                    msg, args = progress_queue.get(timeout=1)
                    ui.debug("got progress: {} args: {}".format(msg, args))
                except queue.Empty:
                    continue

            ctx.scoring_succeeded = True
            return

        network = stack.enter_context(Network(concurrency=concurrent,
                                              timeout=timeout,
                                              ui=ui,
                                              network_queue=network_queue,
                                              network_deque=network_deque,
                                              writer_queue=writer_queue,
                                              progress_queue=progress_queue,
                                              abort_flag=abort_flag,
                                              network_status=network_status,
                                              endpoint=endpoint,
                                              headers=base_headers,
                                              user=user,
                                              api_token=api_token,
                                              pred_name=pred_name,
                                              fast_mode=fast_mode,
                                              max_batch_size=max_batch_size,
                                              compression=compression,
                                              verify_ssl=verify_ssl
                                              ))

        exit_code = None

        try:
            response_handlers = get_response_handlers_from_url(base_url)
        except ValueError as e:
            ui.fatal(str(e))

        writer = stack.enter_context(WriterProcess(ui, ctx, writer_queue,
                                                   network_queue,
                                                   network_deque,
                                                   progress_queue,
                                                   abort_flag,
                                                   writer_status,
                                                   response_handlers))
        ui.info('Writer go...')
        writer_proc = writer.go()

        ui.info('Network go...')
        network_proc = network.go()

        shovel_done = False
        network_done = False
        writer_done = False

        writer_exitcode = None

        n_ret = False
        n_consumed = 0
        n_requests = 0
        n_retried = 0
        n_rusage = None

        s_produced = 0
        s_rusage = None
        s_read = 0
        s_skipped = 0

        w_ret = 0
        w_requests = 0
        w_written = 0
        w_rows = 0
        w_rusage = None

        aborting_phase = 0
        phase_start = time()

        local_abort_flag = [False]

        def exit_fast(*_):
            local_abort_flag[0] = True

        signal.signal(signal.SIGINT, exit_fast)
        signal.signal(signal.SIGTERM, exit_fast)

        while True:
            progress_empty = False
            try:
                msg, args = progress_queue.get(timeout=1)
                ui.debug("got progress: {} args={}".format(msg, args))
            except queue.Empty:
                progress_empty = True
                ui.debug("get progress timed out")
                ui.debug(" aborting_phase: {} ({} seconds)".format(
                    aborting_phase, time() - phase_start))
                ui.debug(" shovel_status: '{}' shovel_done: {} "
                         "shovel_proc: {}({})"
                         "".format(decode_reader_state(shovel_status.value),
                                   shovel_done,
                                   shovel_proc,
                                   shovel_proc and shovel_proc.pid))
                ui.debug(" network_status: '{}' network_done: {} "
                         "network_proc: {}({})"
                         "".format(network.state_name(),
                                   network_done,
                                   network_proc,
                                   network_proc and network_proc.pid))
                ui.debug(" writer_status: '{}' writer_done: {} "
                         "writer_proc: {}({})"
                         "".format(decode_writer_state(writer_status.value),
                                   writer_done,
                                   writer_proc,
                                   writer_proc and writer_proc.pid))
            except KeyboardInterrupt:
                local_abort_flag[0] = True
            else:
                if msg == ProgressQueueMsg.NETWORK_DONE:
                    n_ret = args["ret"]
                    n_requests = args["processed"]
                    n_retried = args["retried"]
                    n_consumed = args["consumed"]
                    n_rusage = args["rusage"]
                    network_done = "ok"
                    if not n_ret:
                        network_done = "with error"
                        exit_code = 1
                        aborting_phase = 1

                elif msg == ProgressQueueMsg.WRITER_DONE:
                    w_ret = args["ret"]
                    w_requests = args["processed"]
                    w_written = args["written"]
                    w_rows = args["rows"]
                    w_rusage = args["rusage"]
                    writer_done = "ok"
                    if not w_ret:
                        writer_done = "with error"
                        exit_code = 1
                        aborting_phase = 1

                elif msg == ProgressQueueMsg.NETWORK_PROGRESS:
                    n_requests = args["processed"]
                    n_retried = args["retried"]
                    n_consumed = args["consumed"]
                    n_rusage = args["rusage"]
                    ui.info("Network progress: Chunks: {}"
                            " Requests: {} Retries: {}{}"
                            "".format(n_consumed,
                                      n_requests, n_retried,
                                      format_usage(n_rusage)))

                elif msg == ProgressQueueMsg.WRITER_PROGRESS:
                    w_requests = args["processed"]
                    w_written = args["written"]
                    w_rows = args["rows"]
                    w_rusage = args["rusage"]
                    ui.info("Writer progress:"
                            " Results: {} Written: {}"
                            " Rows done: {}{}"
                            "".format(w_requests,
                                      w_written,
                                      w_rows,
                                      format_usage(w_rusage)))

                elif msg == ProgressQueueMsg.SHOVEL_PROGRESS:
                    s_produced = args["produced"]
                    s_read = args["read"]
                    s_skipped = args["skipped"]
                    s_rusage = args["rusage"]
                    ui.info("Reader progress: Read: {} Skipped: {}"
                            " Produced: {}{}"
                            "".format(s_read, s_skipped, s_produced,
                                      format_usage(s_rusage)))

                elif msg == ProgressQueueMsg.SHOVEL_DONE:
                    s_produced = args["produced"]
                    s_read = args["read"]
                    s_skipped = args["skipped"]
                    s_rusage = args["rusage"]
                    shovel_done = "ok"

                elif msg in (ProgressQueueMsg.SHOVEL_CSV_ERROR,
                             ProgressQueueMsg.SHOVEL_ERROR):
                    batch = args["batch"]
                    error = args["error"]
                    s_produced = args["produced"]
                    s_read = args["read"]
                    s_skipped = args["skipped"]
                    s_rusage = args["rusage"]

                    if msg == ProgressQueueMsg.SHOVEL_CSV_ERROR:
                        shovel_done = "with csv format error"
                        ui.error("Error parsing CSV file after line {},"
                                 " error: {}, aborting".format(
                                    batch.id + batch.rows, error))
                    else:
                        shovel_done = "with error"
                        ui.error("Unexpected reader error after line {},"
                                 " error: {}, aborting".format(
                                    batch.id + batch.rows, error))

                    exit_code = 1
                    aborting_phase = 1
                else:
                    ui.error("got unknown progress message: {} args={}"
                             "".format(msg, args))

            if local_abort_flag[0]:
                exit_code = 2
                if aborting_phase == 0:
                    ui.info("Keyboard Interrupt, abort sequence started")
                    aborting_phase = 1
                else:
                    ui.info("Aborting is already in progress")

            some_worker_exited = False
            if shovel_proc and not shovel_proc.is_alive():
                shovel_exitcode = shovel_proc.exitcode
                ui.info("shovel proc finished, exit code: {}"
                        .format(shovel_exitcode))
                shovel_proc = None
                some_worker_exited = True
                if shovel_exitcode != 0:
                    exit_code = 1
                    aborting_phase = 1

            if network_proc and not network_proc.is_alive():
                network_exitcode = network_proc.exitcode
                ui.info("network proc finished, exit code: {}"
                        .format(network_exitcode))
                network_proc = None
                some_worker_exited = True
                if network_exitcode != 0:
                    exit_code = 1
                    aborting_phase = 1

            if writer_proc and not writer_proc.is_alive():
                writer_exitcode = writer_proc.exitcode
                ui.info("writer proc finished, exit code: {}"
                        .format(writer_exitcode))
                writer_proc = None
                some_worker_exited = True
                if writer_exitcode != 0:
                    exit_code = 1
                    aborting_phase = 1

            if aborting_phase == 0:
                if progress_empty and not some_worker_exited:
                    if time() - phase_start > REPORT_INTERVAL:
                        if network_proc is None and not network_done:
                            ui.warning("network process finished without "
                                       "posting results, aborting")
                            network_done = "exited"
                            exit_code = 1
                            aborting_phase = 1
                        if shovel_proc is None and not shovel_done:
                            ui.warning("shovel process finished without "
                                       "posting results, aborting")
                            shovel_done = "exited"
                            exit_code = 1
                            aborting_phase = 1
                        if writer_proc is None and not writer_done:
                            ui.warning("writer process finished without "
                                       "posting results, aborting")
                            writer_done = "exited"
                            exit_code = 1
                            aborting_phase = 1

                        phase_start = time()
                else:
                    phase_start = time()

                if shovel_done and \
                        network_status.value == b"I" and \
                        writer_status.value == b"I":
                    ui.info("All requests done, waiting for writer")
                    if writer_proc:
                        writer_queue.put((WriterQueueMsg.SENTINEL, {}))
                    if network_proc:
                        network_queue.put(SENTINEL)
                    aborting_phase = -1
                    phase_start = time()

            elif aborting_phase == -1:
                procs = [shovel_proc, network_proc, writer_proc]
                not_done = [a is False for a in [shovel_done,
                                                 network_done,
                                                 writer_done]]
                if (procs == [None, None, None] and
                        not_done == [False, False, False]):
                    ui.info("all workers exited successfully")
                    break
                elif time() - phase_start > 30:
                    ui.info("some of workers are still active, aborting")
                    if writer_done != "ok":
                        exit_code = 1
                    aborting_phase = 1

            elif aborting_phase == 1:
                abort_flag.value = 1
                aborting_phase = 2
                phase_start = time()
                ui.info("abort sequence started, waiting for workers exit")

            elif aborting_phase == 2:
                procs = [shovel_proc, network_proc, writer_proc]
                if procs == [None, None, None]:
                    ui.info("all workers exited")
                    break
                elif time() - phase_start > 10:
                    for proc in procs:
                        if proc and proc.is_alive():
                            proc.terminate()

                    aborting_phase = 3
                    phase_start = time()

            elif aborting_phase == 3:
                procs = [shovel_proc, network_proc, writer_proc]
                if procs == [None, None, None]:
                    ui.info("all workers exited")
                    break
                elif time() - phase_start > 10:
                    for proc in procs:
                        if proc and proc.is_alive():
                            os.kill(proc.pid, 9)
                aborting_phase = 4

            elif aborting_phase == 4:
                procs = [shovel_proc, network_proc, writer_proc]
                if procs == [None, None, None]:
                    ui.info("all workers exited")
                    break
                elif time() - phase_start > 10:
                    ui.error("some workers are not exited, ignoring")
                    break

        if shovel_done:
            ui.info("Reader is finished: Read: {} Skipped: {}"
                    " Produced: {}{}"
                    "".format(s_read, s_skipped, s_produced,
                              format_usage(s_rusage)))

        if network_done:
            ui.info("Network is finished {}. Chunks: {}"
                    " Requests: {} Retries: {}{}"
                    "".format(network_done, n_consumed,
                              n_requests, n_retried,
                              format_usage(n_rusage)))

        if writer_done:
            ui.info("Writer is finished {}. Result: {}"
                    " Results: {} Written: {} Rows done: {}{}"
                    "".format(writer_done, w_ret, w_requests,
                              w_written, w_rows,
                              format_usage(w_rusage)))

        if n_ret is not True:
            ui.debug('Network finished with error')
            exit_code = 1

        if writer_exitcode == 0:
            ui.debug('writer process exited successfully')
        else:
            ui.debug('writer process did not exit properly: '
                     'returncode="{}"'.format(writer_exitcode))
            exit_code = 1

        ui.debug("active threads: {}".format(threading.enumerate()))
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
                    suffix = ""
                    if bucket == "errors":
                        if batch_id not in ctx.db['checkpoints']:
                            total_lost += batch_id[1]
                        else:
                            suffix = " (fixed)"

                    first = True
                    for msg in msg_data[batch_id]:
                        if first:
                            first = False
                            ui.info("{}: {}{}".format(batch_id, msg, suffix))
                        else:
                            ui.info("        {}".format(msg, suffix))

        ui.info('==== Total stats ===='.format(bucket))
        ui.info("done: {} lost: {}".format(total_done, total_lost))
        if exit_code is None and total_lost == 0:
            ctx.scoring_succeeded = True
        else:
            exit_code = 1

        return exit_code
