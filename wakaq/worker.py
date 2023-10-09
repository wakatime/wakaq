# -*- coding: utf-8 -*-


import logging
import os
import random
import signal
import sys
import time
import traceback

import psutil

from .exceptions import SoftTimeout
from .logger import setup_logging
from .serializer import deserialize
from .utils import (
    close_fd,
    current_task,
    exception_in_chain,
    flush_fh,
    kill,
    read_fd,
    write_fd,
    write_fd_or_raise,
)

ZRANGEPOP = """
local results = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
return results
"""


log = logging.getLogger("wakaq")


class Child:
    __slots__ = [
        "pid",
        "stdin",
        "pingin",
        "broadcastout",
        "last_ping",
        "soft_timeout_reached",
        "done",
        "soft_timeout",
        "hard_timeout",
    ]

    def __init__(self, pid, stdin, pingin, broadcastout):
        os.set_blocking(stdin, False)
        os.set_blocking(pingin, False)
        os.set_blocking(broadcastout, False)
        self.pid = pid
        self.stdin = stdin
        self.pingin = pingin
        self.broadcastout = broadcastout
        self.soft_timeout_reached = False
        self.last_ping = time.time()
        self.done = False
        self.soft_timeout = None
        self.hard_timeout = None

    def close(self):
        close_fd(self.pingin)
        close_fd(self.stdin)
        close_fd(self.broadcastout)

    def set_timeouts(self, wakaq, task=None, queue=None):
        self.soft_timeout = wakaq.soft_timeout
        self.hard_timeout = wakaq.hard_timeout
        if task and task.soft_timeout:
            self.soft_timeout = task.soft_timeout
        elif queue and queue.soft_timeout:
            self.soft_timeout = queue.soft_timeout
        if task and task.hard_timeout:
            self.hard_timeout = task.hard_timeout
        elif queue and queue.hard_timeout:
            self.hard_timeout = queue.hard_timeout


class Worker:
    __slots__ = [
        "wakaq",
        "children",
        "_stop_processing",
        "_max_mem_reached_at",
        "_pubsub",
        "_pingout",
        "_broadcastin",
        "_stdout",
        "_num_tasks_processed",
    ]

    def __init__(self, wakaq=None):
        self.wakaq = wakaq

    def start(self):
        setup_logging(self.wakaq)
        log.info(f"concurrency={self.wakaq.concurrency}")
        log.info(f"soft_timeout={self.wakaq.soft_timeout}")
        log.info(f"hard_timeout={self.wakaq.hard_timeout}")
        log.info(f"wait_timeout={self.wakaq.wait_timeout}")
        log.info(f"exclude_queues={self.wakaq.exclude_queues}")
        log.info(f"max_retries={self.wakaq.max_retries}")
        log.info(f"max_mem_percent={self.wakaq.max_mem_percent}")
        log.info(f"max_tasks_per_worker={self.wakaq.max_tasks_per_worker}")
        log.info(f"worker_log_file={self.wakaq.worker_log_file}")
        log.info(f"scheduler_log_file={self.wakaq.scheduler_log_file}")
        log.info(f"worker_log_level={self.wakaq.worker_log_level}")
        log.info(f"scheduler_log_level={self.wakaq.scheduler_log_level}")
        log.info(f"starting {self.wakaq.concurrency} workers...")
        self._run()

    def _stop(self):
        self._stop_processing = True
        for child in self.children:
            kill(child.pid, signal.SIGTERM)

    def _run(self):
        self.children = []
        self._stop_processing = False
        self._max_mem_reached_at = 0

        pid = None
        for i in range(self.wakaq.concurrency):
            pid = self._fork()
            if pid == 0:
                break

        if pid != 0:  # parent
            self._parent()

    def _fork(self) -> int:
        pingin, pingout = os.pipe()
        broadcastin, broadcastout = os.pipe()
        stdin, stdout = os.pipe()
        pid = os.fork()
        if pid == 0:  # child worker process
            close_fd(stdin)
            close_fd(pingin)
            close_fd(broadcastout)
            self._child(stdout, pingout, broadcastin)
        else:  # parent process
            close_fd(stdout)
            close_fd(pingout)
            close_fd(broadcastin)
            self._add_child(pid, stdin, pingin, broadcastout)
        return pid

    def _parent(self):
        signal.signal(signal.SIGCHLD, self._on_child_exited)
        signal.signal(signal.SIGINT, self._on_exit_parent)
        signal.signal(signal.SIGTERM, self._on_exit_parent)
        signal.signal(signal.SIGQUIT, self._on_exit_parent)

        log.info("finished forking all workers")

        try:
            self._pubsub = self.wakaq.broker.pubsub()
            self._pubsub.subscribe(self.wakaq.broadcast_key)

            while not self._stop_processing:
                self._read_child_logs()
                self._check_max_mem_percent()
                self._refork_missing_children()
                self._enqueue_ready_eta_tasks()
                self._cleanup_children()
                self._check_child_runtimes()
                self._listen_for_broadcast_task()

            if len(self.children) > 0:
                log.info("shutting down...")
                while len(self.children) > 0:
                    self._cleanup_children()
                    self._check_child_runtimes()
                    time.sleep(0.05)

        except:
            try:
                log.error(traceback.format_exc())
            except:
                print(traceback.format_exc())
            self._stop()

    def _child(self, stdout, pingout, broadcastin):
        os.set_blocking(pingout, False)
        os.set_blocking(broadcastin, False)
        os.set_blocking(stdout, False)
        self._pingout = pingout
        self._broadcastin = broadcastin
        self._stdout = stdout

        fh = os.fdopen(stdout, "w")
        sys.stdout = fh
        sys.stderr = fh

        # reset sigchld
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

        # stop processing and gracefully shutdown
        signal.signal(signal.SIGTERM, self._on_exit_child)

        # ignore ctrl-c sent to process group from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # raise SoftTimeout
        signal.signal(signal.SIGQUIT, self._on_soft_timeout_child)

        setup_logging(self.wakaq, is_child=True)

        try:

            # redis should eventually detect pid change and reset, but we force it
            self.wakaq.broker.connection_pool.reset()

            # cleanup file descriptors opened by parent process
            self._remove_all_children()

            log.debug("started worker process")

            if self.wakaq.after_worker_started_callback:
                self.wakaq.after_worker_started_callback()

            self._num_tasks_processed = 0
            while not self._stop_processing:
                self._send_ping_to_parent()
                queue_broker_key, payload = self._blocking_dequeue()
                if payload is not None:
                    try:
                        task = self.wakaq.tasks[payload["name"]]
                    except KeyError:
                        log.error(f'Task not found: {payload["name"]}')
                        task = None

                    if task is not None:
                        queue = self.wakaq.queues_by_key[queue_broker_key]
                        current_task.set((task, payload))
                        retry = payload.get("retry") or 0

                        try:
                            self._execute_task(task, payload, queue=queue)

                        except SoftTimeout:
                            retry += 1
                            max_retries = task.max_retries
                            if max_retries is None:
                                max_retries = (
                                    queue.max_retries if queue.max_retries is not None else self.wakaq.max_retries
                                )
                            if retry > max_retries:
                                log.error(traceback.format_exc())
                            else:
                                log.warning(traceback.format_exc())
                                self.wakaq._enqueue_at_end(
                                    task.name, queue.name, payload["args"], payload["kwargs"], retry=retry
                                )

                        except Exception as e:
                            if exception_in_chain(e, SoftTimeout):
                                retry += 1
                                max_retries = task.max_retries
                                if max_retries is None:
                                    max_retries = (
                                        queue.max_retries if queue.max_retries is not None else self.wakaq.max_retries
                                    )
                                if retry > max_retries:
                                    log.error(traceback.format_exc())
                                else:
                                    log.warning(traceback.format_exc())
                                    self.wakaq._enqueue_at_end(
                                        task.name, queue.name, payload["args"], payload["kwargs"], retry=retry
                                    )
                            else:
                                log.error(traceback.format_exc())

                        except:  # catch BaseException, SystemExit, KeyboardInterrupt, and GeneratorExit
                            log.error(traceback.format_exc())

                        finally:
                            current_task.set(None)

                    else:
                        self._send_ping_to_parent()

                else:
                    self._send_ping_to_parent()
                flush_fh(sys.stdout)
                self._execute_broadcast_tasks()
                if self.wakaq.max_tasks_per_worker and self._num_tasks_processed >= self.wakaq.max_tasks_per_worker:
                    log.info(f"restarting worker after {self._num_tasks_processed} tasks")
                    self._stop_processing = True
                flush_fh(sys.stdout)

        # re-raise the timeout if we were processing a task, otherwise just exit and let
        # parent re-fork another child
        except SoftTimeout:
            if current_task.get():
                raise

        except Exception as e:
            if exception_in_chain(e, SoftTimeout):
                if current_task.get():
                    raise
            else:
                log.error(traceback.format_exc())

        except:  # catch BaseException, SystemExit, KeyboardInterrupt, and GeneratorExit
            log.error(traceback.format_exc())

        finally:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            flush_fh(fh)
            close_fd(self._broadcastin)
            close_fd(self._stdout)
            close_fd(self._pingout)

    def _send_ping_to_parent(self, task_name=None, queue_name=None):
        msg = task_name or ""
        if msg:
            msg = f"{msg}:{queue_name or ''}"
        write_fd_or_raise(self._pingout, f"{msg}\n")

    def _add_child(self, pid, stdin, pingin, broadcastout):
        self.children.append(Child(pid, stdin, pingin, broadcastout))

    def _remove_all_children(self):
        for child in self.children:
            self._remove_child(child)

    def _cleanup_children(self):
        for child in self.children:
            if child.done:
                self._remove_child(child)

    def _remove_child(self, child):
        child.close()
        self.children = [c for c in self.children if c.pid != child.pid]

    def _on_exit_parent(self, signum, frame):
        log.debug(f"Received signal {signum}")
        self._stop()

    def _on_exit_child(self, signum, frame):
        self._stop_processing = True

    def _on_soft_timeout_child(self, signum, frame):
        raise SoftTimeout("SoftTimeout")

    def _on_child_exited(self, signum, frame):
        for child in self.children:
            try:
                pid, _ = os.waitpid(child.pid, os.WNOHANG)
                if pid != 0:  # child exited
                    child.done = True
            except InterruptedError:  # child exited while calling os.waitpid
                child.done = True
            except ChildProcessError:  # child pid no longer valid
                child.done = True

    def _enqueue_ready_eta_tasks(self):
        script = self.wakaq.broker.register_script(ZRANGEPOP)
        for queue in self.wakaq.queues:
            results = script(keys=[queue.broker_eta_key], args=[int(round(time.time()))])
            for payload in results:
                payload = deserialize(payload)
                task_name = payload.pop("name")
                args = payload.pop("args")
                kwargs = payload.pop("kwargs")
                self.wakaq._enqueue_at_front(task_name, queue.name, args, kwargs)

    def _execute_task(self, task, payload, queue=None):
        self._send_ping_to_parent(task_name=task.name, queue_name=queue.name if queue else None)
        log.debug(f"running with payload {payload}")
        if self.wakaq.before_task_started_callback:
            self.wakaq.before_task_started_callback()
        try:
            if self.wakaq.wrap_tasks_function:
                self.wakaq.wrap_tasks_function(task.fn)(*payload["args"], **payload["kwargs"])
            else:
                task.fn(*payload["args"], **payload["kwargs"])
        finally:
            self._send_ping_to_parent()
            self._num_tasks_processed += 1
            if self.wakaq.after_task_finished_callback:
                self.wakaq.after_task_finished_callback()

    def _execute_broadcast_tasks(self):
        payloads = read_fd(self._broadcastin)
        if payloads == "":
            return
        for payload in payloads.splitlines():
            payload = deserialize(payload)
            try:
                task = self.wakaq.tasks[payload["name"]]
            except KeyError:
                log.error(f'Task not found: {payload["name"]}')
                continue
            retry = 0
            current_task.set((task, payload))
            try:
                while True:
                    try:
                        self._execute_task(task, payload)
                        break

                    except SoftTimeout:
                        retry += 1
                        max_retries = task.max_retries
                        if max_retries is None:
                            max_retries = self.wakaq.max_retries
                        if retry > max_retries:
                            log.error(traceback.format_exc())
                            break
                        else:
                            log.warning(traceback.format_exc())

                    except Exception as e:
                        if exception_in_chain(e, SoftTimeout):
                            retry += 1
                            max_retries = task.max_retries
                            if max_retries is None:
                                max_retries = self.wakaq.max_retries
                            if retry > max_retries:
                                log.error(traceback.format_exc())
                                break
                            else:
                                log.warning(traceback.format_exc())
                        else:
                            log.error(traceback.format_exc())
                            break

                    except:  # catch BaseException, SystemExit, KeyboardInterrupt, and GeneratorExit
                        log.error(traceback.format_exc())
                        break

            finally:
                current_task.set(None)

    def _read_child_logs(self):
        for child in self.children:
            logs = read_fd(child.stdin)
            if logs != "":
                log.handlers[0].stream.write(logs)

    def _check_max_mem_percent(self):
        if not self.wakaq.max_mem_percent:
            return
        task_timeout = self.wakaq.hard_timeout or self.wakaq.soft_timeout or 120
        if time.time() - self._max_mem_reached_at < task_timeout:
            return
        if len(self.children) == 0:
            return
        percent_used = int(round(psutil.virtual_memory().percent))
        if percent_used < self.wakaq.max_mem_percent:
            return
        self._max_mem_reached_at = time.time()
        child = self._child_using_most_mem()
        if child:
            log.info(
                f"Mem usage {percent_used}% is more than max_mem_percent threshold ({self.wakaq.max_mem_percent}%)... stopping child process {child.pid}"
            )
            child.soft_timeout_reached = True  # prevent raising SoftTimeout twice for same child
            kill(child.pid, signal.SIGQUIT)

    def _child_using_most_mem(self):
        try:
            return max(self.children, lambda c: psutil.Process(c.pid).memory_percent())
        except:
            return random.choice(self.children)

    def _check_child_runtimes(self):
        for child in self.children:
            ping = read_fd(child.pingin)
            if ping != "":
                log.debug(f"received ping from child process {child.pid}")
                child.last_ping = time.time()
                child.soft_timeout_reached = False
                ping = ping[:-1] if ping[-1] == "\n" else ping
                ping = ping.rsplit("\n", 1)[-1]
                task, queue = None, None
                if ping != "":
                    task_name, queue_name = ping.split(":", 1)
                    task = self.wakaq.tasks[task_name]
                    queue = self.wakaq.queues_by_name.get(queue_name)
                child.set_timeouts(self.wakaq, task=task, queue=queue)
            else:
                soft_timeout = child.soft_timeout or self.wakaq.soft_timeout
                hard_timeout = child.hard_timeout or self.wakaq.hard_timeout
                if soft_timeout or hard_timeout:
                    runtime = time.time() - child.last_ping
                    if hard_timeout and runtime > hard_timeout:
                        log.debug(f"child process {child.pid} runtime {runtime} reached hard timeout, sending sigkill")
                        kill(child.pid, signal.SIGKILL)
                    elif not child.soft_timeout_reached and soft_timeout and runtime > soft_timeout:
                        log.debug(f"child process {child.pid} runtime {runtime} reached soft timeout, sending sigquit")
                        child.soft_timeout_reached = True  # prevent raising SoftTimeout twice for same child
                        kill(child.pid, signal.SIGQUIT)

    def _listen_for_broadcast_task(self):
        msg = self._pubsub.get_message(ignore_subscribe_messages=True, timeout=self.wakaq.wait_timeout)
        if msg:
            payload = msg["data"]
            for child in self.children:
                if child.done:
                    continue
                log.debug(f"run broadcast task: {payload}")
                write_fd(child.broadcastout, f"{payload}\n")
                break

    def _blocking_dequeue(self):
        if len(self.wakaq.broker_keys) == 0:
            time.sleep(self.wakaq.wait_timeout)
            return None, None
        data = self.wakaq.broker.blpop(self.wakaq.broker_keys, self.wakaq.wait_timeout)
        if data is None:
            return None, None
        return data[0], deserialize(data[1])

    def _refork_missing_children(self):
        if self._stop_processing:
            return
        for i in range(self.wakaq.concurrency - len(self.children)):
            log.debug("restarting a crashed worker")
            self._fork()
