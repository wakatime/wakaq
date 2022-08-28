# -*- coding: utf-8 -*-


import daemon
import os
import signal
import time

from .serializer import deserialize


ZRANGEPOP = """
local results = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
return results
"""


class Worker:
    __slots__ = [
        "wakaq",
        "children",
        "_stop_processing",
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
            os.kill(child, signal.SIGTERM)

    def _run(self):
        self.children = []
        self._stop_processing = False

        pid = None
        for i in range(self.wakaq.concurrency):
            pid = self._fork()

        if pid != 0:  # parent
            self._parent()

    def _parent(self):
        signal.signal(signal.SIGCHLD, self._on_child_exited)
        signal.signal(signal.SIGINT, self._on_exit_parent)
        signal.signal(signal.SIGTERM, self._on_exit_parent)
        self.wakaq.broker.close()
        pubsub = self.wakaq.broker.pubsub()
        pubsub.subscribe(self.wakaq.broadcast_key)
        while not self._stop_processing:
            msg = pubsub.get_message(
                ignore_subscribe_messages=True, timeout=self.wakaq.wait_timeout
            )
            if msg:
                payload = msg["data"]
                payload = deserialize(payload)
                task_name = payload.pop("name")
                queue = payload.pop("queue")
                args = payload.pop("args")
                kwargs = payload.pop("kwargs")
                self.wakaq._enqueue_at_front(task_name, queue, args, kwargs)

        if self._stop_processing:
            while len(self.children) > 0:
                print("shutting down...")
                time.sleep(1)

    def _child(self):
        # ignore ctrl-c sent to process group from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # reset sigchld
        signal.signal(signal.SIGCHLD, signal.SIG_DFL)

        # stop processing and gracefully shutdown
        signal.signal(signal.SIGTERM, self._on_exit_child)

        # redis should eventually detect pid change and reset, but we force it
        self.wakaq.broker.connection_pool.reset()

        while not self._stop_processing:
            self._enqueue_ready_eta_tasks()
            self._execute_next_task_from_queue()

    def _fork(self) -> int:
        pid = os.fork()
        if pid == 0:
            self._child()
        else:
            self._add_child(pid)
        return pid

    def _add_child(self, child):
        self.children.append(child)

    def _remove_child(self, child):
        try:
            self.children.remove(child)
        except ValueError:
            pass

    def _on_exit_parent(self, signum, frame):
        self._stop()

    def _on_exit_child(self, signum, frame):
        self._stop_processing = True

    def _on_child_exited(self, signum, frame):
        for child in self.children:
            try:
                pid, _ = os.waitpid(child, os.WNOHANG)
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

    def _refork_missing_children(self):
        if self._stop_processing:
            return
        for i in range(self.wakaq.concurrency - len(self.children)):
            self._fork()
