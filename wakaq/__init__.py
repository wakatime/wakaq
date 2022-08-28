# -*- coding: utf-8 -*-


import calendar
import multiprocessing
import redis
from datetime import datetime, timedelta

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

    eta_task_key = "wakaq-eta"
    broadcast_key = "wakaq-broadcast"

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
        socket_timeout=15,
        socket_connect_timeout=15,
        health_check_interval=30,
        wait_timeout=1,
    ):
        self.queues = [Queue.create(x) for x in queues]
        self.queues.sort(key=lambda q: q.priority)
        self.queues_by_name = dict([(x.name, x) for x in self.queues])
        self.broker_keys = [x.broker_key for x in self.queues]
        self.schedules = [CronTask.create(x) for x in schedules]
        self.concurrency = abs(int(concurrency)) or multiprocessing.cpu_count()
        self.exclude_queues = self._validate_queue_names(exclude_queues)
        self.soft_timeout = soft_timeout
        self.hard_timeout = hard_timeout
        self.wait_timeout = wait_timeout
        if soft_timeout and int(soft_timeout) <= int(wait_timeout):
            raise Exception(
                f"Soft timeout ({soft_timeout}) can not be less than or equal to wait timeout ({wait_timeout})."
            )
        if hard_timeout and int(hard_timeout) <= int(wait_timeout):
            raise Exception(
                f"Hard timeout ({hard_timeout}) can not be less than or equal to wait timeout ({wait_timeout})."
            )
        if soft_timeout and hard_timeout and int(hard_timeout) <= int(soft_timeout):
            raise Exception(
                f"Hard timeout ({hard_timeout}) can not be less than or equal to soft timeout ({soft_timeout})."
            )
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
