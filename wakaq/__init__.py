# -*- coding: utf-8 -*-


import calendar
import logging
import multiprocessing
from datetime import datetime, timedelta

import redis

from .queue import Queue
from .scheduler import CronTask
from .serializer import serialize
from .task import Task
from .utils import safe_eval

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
    max_retries = None
    wait_timeout = None
    max_mem_percent = None
    max_tasks_per_worker = None
    worker_log_file = None
    scheduler_log_file = None
    worker_log_level = None
    scheduler_log_level = None

    after_worker_started_callback = None
    before_task_started_callback = None
    after_task_finished_callback = None
    wrap_tasks_function = None

    broadcast_key = "wakaq-broadcast"
    log_format = "[%(asctime)s] %(levelname)s: %(message)s"
    task_log_format = "[%(asctime)s] %(levelname)s in %(task)s: %(message)s"

    def __init__(
        self,
        queues=[],
        schedules=[],
        host="localhost",
        port=6379,
        username=None,
        password=None,
        db=0,
        concurrency=0,
        exclude_queues=[],
        max_retries=None,
        soft_timeout=None,
        hard_timeout=None,
        max_mem_percent=None,
        max_tasks_per_worker=None,
        worker_log_file=None,
        scheduler_log_file=None,
        worker_log_level=None,
        scheduler_log_level=None,
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
        self.queues_by_key = dict([(x.broker_key, x) for x in self.queues])
        self.exclude_queues = self._validate_queue_names(exclude_queues)
        self.max_retries = int(max_retries or 0)
        self.broker_keys = [x.broker_key for x in self.queues if x.name not in self.exclude_queues]
        self.schedules = [CronTask.create(x) for x in schedules]
        self.concurrency = self._format_concurrency(concurrency)
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

        if max_mem_percent:
            self.max_mem_percent = int(max_mem_percent)
            if self.max_mem_percent < 1 or self.max_mem_percent > 99:
                raise Exception(f"Max memory percent must be between 1 and 99: {self.max_mem_percent}")
        else:
            self.max_mem_percent = None

        self.max_tasks_per_worker = abs(int(max_tasks_per_worker)) if max_tasks_per_worker else None
        self.worker_log_file = worker_log_file if isinstance(worker_log_file, str) else None
        self.scheduler_log_file = scheduler_log_file if isinstance(scheduler_log_file, str) else None
        self.worker_log_level = worker_log_level if isinstance(worker_log_level, int) else logging.INFO
        self.scheduler_log_level = scheduler_log_level if isinstance(scheduler_log_level, int) else logging.DEBUG

        self.tasks = {}
        self.broker = redis.Redis(
            host=host,
            port=port,
            username=username,
            password=password,
            db=db,
            charset="utf-8",
            decode_responses=True,
            health_check_interval=health_check_interval,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
        )

    def task(self, fn=None, queue=None, max_retries=None, soft_timeout=None, hard_timeout=None):
        def wrap(f):
            t = Task(
                fn=f,
                wakaq=self,
                queue=queue,
                max_retries=max_retries,
                soft_timeout=soft_timeout,
                hard_timeout=hard_timeout,
            )
            if t.name in self.tasks:
                raise Exception(f"Duplicate task name: {t.name}")
            self.tasks[t.name] = t
            return t.fn

        return wrap(fn) if fn else wrap

    def after_worker_started(self, callback):
        self.after_worker_started_callback = callback
        return callback

    def before_task_started(self, callback):
        self.before_task_started_callback = callback
        return callback

    def after_task_finished(self, callback):
        self.after_task_finished_callback = callback
        return callback

    def wrap_tasks_with(self, callback):
        self.wrap_tasks_function = callback
        return callback

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

    def _enqueue_at_end(self, task_name: str, queue: str, args: list, kwargs: dict, retry=0):
        queue = self._queue_or_default(queue)
        payload = serialize(
            {
                "name": task_name,
                "args": args,
                "kwargs": kwargs,
                "retry": retry,
            }
        )
        self.broker.rpush(queue.broker_key, payload)

    def _enqueue_with_eta(self, task_name: str, queue: str, args: list, kwargs: dict, eta):
        queue = self._queue_or_default(queue)
        payload = serialize(
            {
                "name": task_name,
                "args": args,
                "kwargs": kwargs,
            }
        )
        if isinstance(eta, timedelta):
            eta = datetime.utcnow() + eta
        timestamp = calendar.timegm(eta.utctimetuple())
        self.broker.zadd(queue.broker_eta_key, {payload: timestamp}, nx=True)

    def _broadcast(self, task_name: str, args: list, kwargs: dict):
        payload = serialize(
            {
                "name": task_name,
                "args": args,
                "kwargs": kwargs,
            }
        )
        return self.broker.publish(self.broadcast_key, payload)

    def _queue_or_default(self, queue_name: str):
        if queue_name:
            return Queue.create(queue_name, queues_by_name=self.queues_by_name)

        # return lowest priority queue by default
        return self.queues[-1]

    def _default_priority(self, queue, lowest_priority):
        if queue.priority < 0:
            queue.priority = lowest_priority + 1
        return queue

    def _format_concurrency(self, concurrency):
        if not concurrency:
            return 0
        try:
            return safe_eval(str(concurrency), {"cores": multiprocessing.cpu_count()})
        except Exception as e:
            raise Exception(f"Error parsing concurrency: {e}")
