# -*- coding: utf-8 -*-


import daemon
import os
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
        'wakaq',
        'concurrency',
        'exclude_queues',
    ]

    def __init__(self, wakaq=None, concurrency=1, exclude_queues='', foreground=False):
        self.wakaq = wakaq
        self.concurrency = concurrency
        self.exclude_queues = exclude_queues

        if foreground:
            self._run()
            return

        with daemon.DaemonContext():
            self._run()

    def _run(self):
        is_parent = True
        for i in range(self.concurrency):
            pid = os.fork()
            if pid == 0:  # child
                is_parent = False
                while True:
                    self._enqueue_ready_eta_tasks()
                    self._execute_next_task_from_queue()

        if is_parent:
            while True:
                time.sleep(10)

    def _enqueue_ready_eta_tasks(self):
        script = self.wakaq.broker.register_script(ZRANGEPOP)
        results = script(keys=[self.wakaq.eta_task_key], args=[int(round(time.time()))])
        for payload in results:
            payload = deserialize(payload)
            queue = payload.pop('queue')
            queue = Queue.create(queue, queues_by_name=self.wakaq.queues_by_name)
            payload = serialize(payload)
            self.wakaq.broker.lpush(queue.broker_key, payload)

    def _execute_next_task_from_queue(self):
        queues = [x.broker_key for x in self.wakaq.queues]
        print('Checking for tasks...')
        payload = self.wakaq.broker.blpop(queues, self.wakaq.wait_timeout)
        if payload is not None:
            queue, payload = payload
            payload = deserialize(payload)
            print(f'got task: {payload}')
            task = self.wakaq.tasks[payload['name']]
            task.fn(*payload['args'], **payload['kwargs'])
