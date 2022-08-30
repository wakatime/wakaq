# -*- coding: utf-8 -*-


import logging
import math
import os
import random
import signal
import sys
import time
import traceback

import daemon
import psutil

from .exceptions import SoftTimeout
from .logger import setup_logging
from .serializer import deserialize
from .utils import current_task, kill, read_fd

ZRANGEPOP = """
local results = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
return results
"""


log = logging.getLogger("wakaq")


class Child:
    __slots__ = [
        "pid",
        "pingin",
        "stdin",
        "last_ping",
        "soft_timeout_reached",
        "done",
    ]

    def __init__(self, pid, pingin, stdin):
        self.pid = pid
        self.pingin = pingin
        self.stdin = stdin
        self.soft_timeout_reached = False
        self.last_ping = time.time()
        self.done = False

    def close(self):
        try:
            os.close(self.pingin)
        except:
            pass
        try:
            os.close(self.stdin)
        except:
            pass


class Worker:
    __slots__ = [
        "wakaq",
        "children",
        "_stop_processing",
        "_max_mem_reached_at",
        "_pubsub",
        "_pingout",
        "_stdout",
        "_num_tasks_processed",
    ]

    def __init__(self, wakaq=None):
        self.wakaq = wakaq

    def start(self, foreground=False):
        setup_logging(self.wakaq)
        log.info(f"starting {self.wakaq.concurrency} workers")

        if foreground:
            self._run()
            return

        with daemon.DaemonContext():
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
        stdin, stdout = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(pingin)
            os.close(stdin)
            self._child(pingout, stdout)
        else:
            os.close(pingout)
            os.close(stdout)
            self._add_child(pid, pingin, stdin)
        return pid

    def _parent(self):
        signal.signal(signal.SIGCHLD, self._on_child_exited)
        signal.signal(signal.SIGINT, self._on_exit_parent)
        signal.signal(signal.SIGTERM, self._on_exit_parent)

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

        if self._stop_processing:
            if len(self.children) > 0:
                log.info("shutting down...")
            while len(self.children) > 0:
                self._cleanup_children()
                self._check_child_runtimes()
                time.sleep(0.05)

    def _child(self, pingout, stdout):
        self._pingout = pingout
        self._stdout = stdout

        fh = os.fdopen(stdout, "w")
        sys.stdout = fh
        sys.stderr = fh

        # ignore ctrl-c sent to process group from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # reset sigchld
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

        # stop processing and gracefully shutdown
        signal.signal(signal.SIGTERM, self._on_exit_child)

        # raise SoftTimeout
        signal.signal(signal.SIGQUIT, self._on_soft_timeout_child)

        setup_logging(self.wakaq, is_child=True)

        try:

            # redis should eventually detect pid change and reset, but we force it
            self.wakaq.broker.connection_pool.reset()

            # cleanup file descriptors opened by parent process
            self._remove_all_children()

            log.debug("started worker process")
            if self.wakaq.after_worker_startup:
                self.wakaq.after_worker_startup()

            self._num_tasks_processed = 0
            while not self._stop_processing:
                self._execute_next_task_from_queue()

        except:
            log.error(traceback.format_exc())
            return

        finally:
            for handler in log.handlers:
                handler.flush()
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__
            fh.close()

    def _add_child(self, pid, pingin, stdin):
        self.children.append(Child(pid, pingin, stdin))

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
        self._stop()

    def _on_exit_child(self, signum, frame):
        self._stop_processing = True

    def _on_soft_timeout_child(self, signum, frame):
        self._stop_processing = True
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
        results = script(keys=[self.wakaq.eta_task_key], args=[int(round(time.time()))])
        for payload in results:
            payload = deserialize(payload)
            task_name = payload.pop("name")
            queue = payload.pop("queue")
            args = payload.pop("args")
            kwargs = payload.pop("kwargs")
            self.wakaq._enqueue_at_front(task_name, queue, args, kwargs)

    def _execute_next_task_from_queue(self):
        payload = self.wakaq._blocking_dequeue()
        if payload is not None:
            task = self.wakaq.tasks[payload["name"]]
            current_task.set(task)
            log.debug(f"running with payload {payload}")
            task.fn(*payload["args"], **payload["kwargs"])
            current_task.set(None)
            self._num_tasks_processed += 1
            if self.wakaq.max_tasks_per_worker and self._num_tasks_processed >= self.wakaq.max_tasks_per_worker:
                log.debug(f"restarting worker after {self._num_tasks_processed} tasks")
                self._stop_processing = True
        os.write(self._pingout, b".")

    def _read_child_logs(self):
        for child in self.children:
            logs = read_fd(child.stdin)
            if logs != b"":
                log.handlers[0].stream.write(logs.decode("utf8"))

    def _check_max_mem_percent(self):
        if not self.wakaq.max_mem_percent:
            return
        task_timeout = self.wakaq.hard_timeout or self.wakaq.soft_timeout or 120
        if time.time() - self._max_mem_reached_at < task_timeout:
            return
        if len(self.children) == 0:
            return
        percent_used = int(math.ceil(psutil.virtual_memory().percent))
        if percent_used < self.wakaq.max_mem_percent:
            return
        self._max_mem_reached_at = time.time()
        child = self._child_using_most_mem()
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
            if ping != b"":
                child.last_ping = time.time()
                child.soft_timeout_reached = False
            elif self.wakaq.soft_timeout or self.wakaq.hard_timeout:
                runtime = time.time() - child.last_ping
                if self.wakaq.hard_timeout and runtime > self.wakaq.hard_timeout:
                    kill(child.pid, signal.SIGKILL)
                elif not child.soft_timeout_reached and self.wakaq.soft_timeout and runtime > self.wakaq.soft_timeout:
                    child.soft_timeout_reached = True  # prevent raising SoftTimeout twice for same child
                    kill(child.pid, signal.SIGQUIT)

    def _listen_for_broadcast_task(self):
        msg = self._pubsub.get_message(ignore_subscribe_messages=True, timeout=self.wakaq.wait_timeout)
        if msg:
            payload = msg["data"]
            payload = deserialize(payload)
            task_name = payload.pop("name")
            queue = payload.pop("queue")
            args = payload.pop("args")
            kwargs = payload.pop("kwargs")
            self.wakaq._enqueue_at_front(task_name, queue, args, kwargs)

    def _refork_missing_children(self):
        if self._stop_processing:
            return
        for i in range(self.wakaq.concurrency - len(self.children)):
            log.debug("restarting a crashed worker")
            self._fork()
