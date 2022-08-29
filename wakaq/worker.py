# -*- coding: utf-8 -*-


import os
import signal
import time

import daemon

from .exceptions import SoftTimeout
from .serializer import deserialize
from .utils import kill, read_fd

ZRANGEPOP = """
local results = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
return results
"""


class Child:
    __slots__ = [
        "pid",
        "fd",
        "last_ping",
        "soft_timeout_reached",
    ]

    def __init__(self, pid, fd):
        self.pid = pid
        self.fd = fd
        self.soft_timeout_reached = False
        self.last_ping = time.time()


class Worker:
    __slots__ = [
        "wakaq",
        "children",
        "_stop_processing",
        "_pubsub",
        "_write_fd",
    ]

    def __init__(self, wakaq=None):
        self.wakaq = wakaq

    def start(self, foreground=False):
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

        pid = None
        for i in range(self.wakaq.concurrency):
            pid = self._fork()

        if pid != 0:  # parent
            self._parent()

    def _fork(self) -> int:
        r, w = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(r)
            self._child(w)
        else:
            self._add_child(pid, r)
        return pid

    def _parent(self):
        signal.signal(signal.SIGCHLD, self._on_child_exited)
        signal.signal(signal.SIGINT, self._on_exit_parent)
        signal.signal(signal.SIGTERM, self._on_exit_parent)

        self._pubsub = self.wakaq.broker.pubsub()
        self._pubsub.subscribe(self.wakaq.broadcast_key)

        while not self._stop_processing:
            self._enqueue_ready_eta_tasks()
            self._check_child_runtimes()
            self._listen_for_broadcast_task()

        if self._stop_processing:
            while len(self.children) > 0:
                print("shutting down...")
                self._check_child_runtimes()
                time.sleep(1)

    def _child(self, fd):
        self._write_fd = fd

        # ignore ctrl-c sent to process group from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # reset sigchld
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

        # stop processing and gracefully shutdown
        signal.signal(signal.SIGTERM, self._on_exit_child)

        # raise SoftTimeout
        signal.signal(signal.SIGQUIT, self._on_soft_timeout_child)

        # redis should eventually detect pid change and reset, but we force it
        self.wakaq.broker.connection_pool.reset()

        while not self._stop_processing:
            self._execute_next_task_from_queue()

    def _add_child(self, pid, fd):
        self.children.append(Child(pid, fd))

    def _remove_child(self, child):
        i = 0
        for c in self.children:
            if child.pid == c.pid:
                try:
                    os.close(child.fd)
                except:
                    pass
                del self.children[i]
                break
            i += 1

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
                    self._remove_child(child)
            except InterruptedError:  # child exited while calling os.waitpid
                self._remove_child(child)
            except ChildProcessError:  # child pid no longer valid
                self._remove_child(child)
        self._refork_missing_children()

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
        print("Checking for tasks...")
        payload = self.wakaq._blocking_dequeue()
        if payload is not None:
            print(f"got task: {payload}")
            task = self.wakaq.tasks[payload["name"]]
            task.fn(*payload["args"], **payload["kwargs"])
        os.write(self._write_fd, b".")

    def _check_child_runtimes(self):
        for child in self.children:
            ping = read_fd(child.fd)
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
            self._fork()
