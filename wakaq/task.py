# -*- coding: utf-8 -*-


import time
from functools import wraps

from .queue import Queue
from .serializer import serialize


class Task:
    __slots__ = [
        'name',
        'fn',
        'wakaq',
        'queue',
    ]

    def __init__(self, fn=None, wakaq=None, queue=None):
        self.name = fn.__name__
        self.wakaq = wakaq
        if queue:
            self.queue = self._create_queue(queue)
        else:
            self.queue = None

        @wraps(fn)
        def inner(*args, **kwargs):
            return fn(*args, **kwargs)

        inner.delay = self.delay
        self.fn = inner

    def delay(self, *args, **kwargs):
        queue = kwargs.pop('queue', None)
        eta = kwargs.pop('eta', None)
        if queue:
            queue = self._create_queue(queue)
        else:
            if self.queue:
                queue = self.queue
            else:
                queue = self.wakaq.queues[-1]

        payload = {
            'name': self.name,
            'args': args,
            'kwargs': kwargs,
        }
        if eta:
            payload['queue'] = queue.name
        payload = serialize(payload)
        if eta:
            self.wakaq.broker.zadd(self.wakaq.eta_task_key, {payload: time.time()}, nx=True)
        else:
            self.wakaq.broker.rpush(queue.broker_key, payload)

    def _create_queue(self, queue):
        return Queue.create(queue, queues_by_name=self.wakaq.queues_by_name)
