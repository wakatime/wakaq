# -*- coding: utf-8 -*-


import calendar
import logging
import multiprocessing
from datetime import datetime, timedelta

import redis

from .queue import Queue
from .scheduler import CronTask
from .serializer import deserialize, serialize
from .task import Task

__all__ = [
    "WakaQ",
    "Queue",
    "CronTask",
]


class WakaQ:
    queues = []
    soft_timeout = None
    hard_timeout = None
    concurrency = 0
    schedules = []
    exclude_queues = []
    wait_timeout = None
    max_mem_percent = None
    max_tasks_per_worker = None
    log_file = None
    log_level = None
    after_worker_startup = None

    eta_task_key = "wakaq-eta"
    broadcast_key = "wakaq-broadcast"
    log_format = "[%(asctime)s] %(levelname)s: %(message)s"
    task_log_format = "[%(asctime)s] %(levelname)s in %(task)s: %(message)s"

    def __init__(
        self,
        queues=[],
        schedules=[],
        host="localhost",
        port=6379,
        concurrency=0,
        exclude_queues=[],
        soft_timeout=None,
        hard_timeout=None,
        max_mem_percent=None,
        max_tasks_per_worker=None,
        after_worker_startup=None,
        log_file=None,
        log_level=None,
        socket_timeout=15,
        socket_connect_timeout=15,
        health_check_interval=30,
        wait_timeout=1,
    ):
        self.queues = [Queue.create(x) for x in queues]
        lowest_priority = max(self.queues, key=lambda q: q.priority)
        self.queues = list(map(lambda q: self._default_priority(q, lowest_priority.priority), self.queues))
        self.queues.sort(key=lambda q: q.priority)
        self.queues_by_name = dict([(x.name, x) for x in self.queues])
        self.broker_keys = [x.broker_key for x in self.queues]
        self.schedules = [CronTask.create(x) for x in schedules]
        self.concurrency = abs(int(concurrency)) or multiprocessing.cpu_count()
        self.exclude_queues = self._validate_queue_names(exclude_queues)
        self.soft_timeout = soft_timeout.total_seconds() if isinstance(soft_timeout, timedelta) else soft_timeout
        self.hard_timeout = hard_timeout.total_seconds() if isinstance(hard_timeout, timedelta) else hard_timeout
        self.wait_timeout = wait_timeout.total_seconds() if isinstance(wait_timeout, timedelta) else wait_timeout

        if self.soft_timeout and self.soft_timeout <= wait_timeout:
            raise Exception(
                f"Soft timeout ({self.soft_timeout}) can not be less than or equal to wait timeout ({self.wait_timeout})."
            )
        if self.hard_timeout and self.hard_timeout <= self.wait_timeout:
            raise Exception(
                f"Hard timeout ({self.hard_timeout}) can not be less than or equal to wait timeout ({self.wait_timeout})."
            )
        if self.soft_timeout and self.hard_timeout and self.hard_timeout <= self.soft_timeout:
            raise Exception(
                f"Hard timeout ({self.hard_timeout}) can not be less than or equal to soft timeout ({self.soft_timeout})."
            )

        if self.max_mem_percent:
            self.max_mem_percent = int(max_mem_percent)
            if self.max_mem_percent < 1 or self.max_mem_percent > 99:
                raise Exception(f"Max memory percent must be between 1 and 99: {self.max_mem_percent}")
        else:
            self.max_mem_percent = None

        self.max_tasks_per_worker = abs(int(max_tasks_per_worker)) if max_tasks_per_worker else None
        self.log_file = log_file if isinstance(log_file, str) else None
        self.log_level = log_level if isinstance(log_level, int) else logging.INFO

        if after_worker_startup and not callable(after_worker_startup):
            raise Exception("after_worker_startup must be a function")
        self.after_worker_startup = after_worker_startup

        self.tasks = {}
        self.broker = redis.Redis(
            host=host,
            port=port,
            charset="utf-8",
            decode_responses=True,
            health_check_interval=health_check_interval,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
        )

    def task(self, fn=None, queue=None):
        def wrap(f):
            t = Task(fn=f, wakaq=self, queue=queue)
            if t.name in self.tasks:
                raise Exception(f"Duplicate task name: {t.name}")
            self.tasks[t.name] = t
            return t.fn

        return wrap(fn) if fn else wrap

    def _validate_queue_names(self, queue_names: list) -> list:
        try:
            queue_names = [x for x in queue_names]
        except:
            return []
        for queue_name in queue_names:
            if queue_name not in self.queues_by_name:
                raise Exception(f"Invalid queue: {queue_name}")
        return queue_names

    def _enqueue_at_front(self, task_name: str, queue: str, args: list, kwargs: dict):
        queue = self._queue_or_default(queue)
        payload = serialize(
            {
                "name": task_name,
                "args": args,
                "kwargs": kwargs,
            }
        )
        self.broker.lpush(queue.broker_key, payload)

    def _enqueue_at_end(self, task_name: str, queue: str, args: list, kwargs: dict):
        queue = self._queue_or_default(queue)
        payload = serialize(
            {
                "name": task_name,
                "args": args,
                "kwargs": kwargs,
            }
        )
        self.broker.rpush(queue.broker_key, payload)

    def _enqueue_with_eta(self, task_name: str, queue: str, args: list, kwargs: dict, eta: timedelta):
        queue = self._queue_or_default(queue)
        payload = serialize(
            {
                "name": task_name,
                "args": args,
                "kwargs": kwargs,
                "queue": queue.name,
            }
        )
        timestamp = calendar.timegm((datetime.utcnow() + eta).utctimetuple())
        self.broker.zadd(self.eta_task_key, {payload: timestamp}, nx=True)

    def _broadcast(self, task_name: str, queue: str, args: list, kwargs: dict):
        queue = self._queue_or_default(queue)
        payload = serialize(
            {
                "name": task_name,
                "queue": queue.name,
                "args": args,
                "kwargs": kwargs,
            }
        )
        return self.broker.publish(self.broadcast_key, payload)

    def _blocking_dequeue(self):
        data = self.broker.blpop(self.broker_keys, self.wait_timeout)
        return deserialize(data[1]) if data is not None else None

    def _queue_or_default(self, queue_name: str):
        if queue_name:
            return Queue.create(queue_name, queues_by_name=self.queues_by_name)

        # return lowest priority queue by default
        return self.queues[-1]

    def _default_priority(self, queue, lowest_priority):
        if queue.priority < 0:
            queue.priority = lowest_priority + 1
        return queue
