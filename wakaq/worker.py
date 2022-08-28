# -*- coding: utf-8 -*-


import daemon
import os
import signal
import time

from .queue import Queue
from .serializer import deserialize, serialize


ZRANGEPOP = """
local results = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
return results
"""

ZRANGEPOP2 = """
local results = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'BYSCORE')
local removed = redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
if #results ~= tonumber(removed) then
    error('Removed different number of tasks than retrieved')
end
return results"""


class Worker:
    __slots__ = [
        "wakaq",
        "children",
    ]

    def __init__(self, wakaq=None):
        self.wakaq = wakaq

    def start(self, foreground=False):
        self.children = []

        if foreground:
            self._run()
            return

        with daemon.DaemonContext():
            self._run()

    def _run(self):
        pid = None
        for i in range(self.wakaq.concurrency):
            pid = self._fork()

        if pid != 0:  # parent
            self._parent()

    def _parent(self):
        signal.signal(signal.SIGCHLD, self._on_child_exit)
        self.wakaq.broker.close()
        pubsub = self.wakaq.broker.pubsub()
        pubsub.subscribe(self.wakaq.broadcast_key)
        while True:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=10)
            if msg:
                payload = msg["data"]
                payload = deserialize(payload)
                queue = payload.pop("queue")
                queue = Queue.create(queue, queues_by_name=self.wakaq.queues_by_name)
                payload = serialize(payload)
                self.wakaq.broker.lpush(queue.broker_key, payload)

    def _child(self):
        # ignore ctrl-c sent to process group from terminal
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        # redis should eventually detect pid change and reset, but we force it
        self.wakaq.broker.connection_pool.reset()

        while True:
            self._enqueue_ready_eta_tasks()
            self._execute_next_task_from_queue()

    def _fork(self) -> int:
        pid = os.fork()
        if pid == 0:
            self._child()
        else:
            self.children.append(pid)
        return pid

    def _on_child_exit(self, signum, frame):
        for child in self.children:
            try:
                pid, _ = os.waitpid(child, os.WNOHANG)
                if pid != 0:  # child exited
                    self.children.remove(child)
            except InterruptedError:  # child exited while calling os.waitpid
                self.children.remove(child)
            except ChildProcessError:  # child pid no longer valid
                self.children.remove(child)
        self._refork_missing_children()

    def _enqueue_ready_eta_tasks(self):
        script = self.wakaq.broker.register_script(ZRANGEPOP)
        results = script(keys=[self.wakaq.eta_task_key], args=[int(round(time.time()))])
        for payload in results:
            payload = deserialize(payload)
            queue = payload.pop("queue")
            queue = Queue.create(queue, queues_by_name=self.wakaq.queues_by_name)
            payload = serialize(payload)
            self.wakaq.broker.lpush(queue.broker_key, payload)

    def _execute_next_task_from_queue(self):
        queues = [x.broker_key for x in self.wakaq.queues]
        print("Checking for tasks...")
        payload = self.wakaq.broker.blpop(queues, self.wakaq.wait_timeout)
        if payload is not None:
            queue, payload = payload
            payload = deserialize(payload)
            print(f"got task: {payload}")
            task = self.wakaq.tasks[payload["name"]]
            task.fn(*payload["args"], **payload["kwargs"])

    def _refork_missing_children(self):
        for i in range(self.wakaq.concurrency - len(self.children)):
            self._fork()
