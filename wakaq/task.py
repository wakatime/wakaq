# -*- coding: utf-8 -*-


from functools import wraps


class Task:
    __slots__ = [
        "name",
        "fn",
        "wakaq",
        "queue",
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
        inner.broadcast = self.broadcast

        self.fn = inner

    def delay(self, *args, **kwargs):
        """Run task in the background."""

        queue = kwargs.pop("queue", None)
        eta = kwargs.pop("eta", None)
        if eta:
            self.wakaq._enqueue_with_eta(self.name, queue, args, kwargs, eta)
        else:
            self.wakaq._enqueue_at_end(self.name, queue, args, kwargs)

    def broadcast(self, *args, **kwargs) -> int:
        """Run task in the background on all workers.

        Only runs the task once per worker parent daemon, no matter the worker's concurrency.

        Returns the number of workers the task was sent to.
        """

        queue = kwargs.pop("queue", None)
        return self.wakaq._broadcast(self.name, queue, args, kwargs)
