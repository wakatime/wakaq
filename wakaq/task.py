# -*- coding: utf-8 -*-


from functools import wraps

from .queue import Queue


class Task:
    __slots__ = [
        "name",
        "fn",
        "wakaq",
        "queue",
        "soft_timeout",
        "hard_timeout",
        "max_retries",
    ]

    def __init__(self, fn=None, wakaq=None, queue=None, soft_timeout=None, hard_timeout=None, max_retries=None):
        self.name = fn.__name__
        self.wakaq = wakaq
        if queue:
            self.queue = Queue.create(queue, queues_by_name=self.wakaq.queues_by_name)
        else:
            self.queue = None

        self.soft_timeout = int(soft_timeout) if soft_timeout else None
        self.hard_timeout = int(hard_timeout) if hard_timeout else None
        self.max_retries = int(max_retries) if max_retries else None

        @wraps(fn)
        def inner(*args, **kwargs):
            return fn(*args, **kwargs)

        inner.delay = self._delay
        inner.broadcast = self._broadcast

        self.fn = inner

    def _delay(self, *args, **kwargs):
        """Run task in the background."""

        queue = kwargs.pop("queue", None) or self.queue
        eta = kwargs.pop("eta", None)
        if eta:
            self.wakaq._enqueue_with_eta(self.name, queue, args, kwargs, eta)
        else:
            self.wakaq._enqueue_at_end(self.name, queue, args, kwargs)

    def _broadcast(self, *args, **kwargs) -> int:
        """Run task in the background on all workers.

        Only runs the task once per worker parent daemon, no matter the worker's concurrency.

        Returns the number of workers the task was sent to.
        """

        return self.wakaq._broadcast(self.name, args, kwargs)
