# -*- coding: utf-8 -*-


import logging
import time
from datetime import datetime, timedelta

from croniter import croniter

from .logger import setup_logging
from .serializer import serialize

log = logging.getLogger("wakaq")


class CronTask:
    __slots__ = [
        "schedule",
        "task_name",
        "queue",
        "args",
        "kwargs",
    ]

    def __init__(self, schedule=None, task_name=None, queue=None, args=None, kwargs=None):
        if not croniter.is_valid(schedule):
            log.error(f"Invalid cron schedule (min hour dom month dow): {schedule}")
            raise Exception(f"Invalid cron schedule (min hour dom month dow): {schedule}")

        self.schedule = schedule
        self.task_name = task_name
        self.queue = queue
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def create(cls, obj, queues_by_name=None):
        if isinstance(obj, cls):
            if queues_by_name is not None and obj.queue and obj.queue not in queues_by_name:
                log.error(f"Unknown queue: {obj.queue}")
                raise Exception(f"Unknown queue: {obj.queue}")
            return obj
        elif isinstance(obj, (list, tuple)) and len(obj) == 2:
            return cls(schedule=obj[0], task_name=obj[1])
        elif isinstance(obj, (list, tuple)) and len(obj) == 4:
            return cls(schedule=obj[0], task_name=obj[1], args=obj[2], kwargs=obj[3])
        else:
            log.error(f"Invalid schedule: {obj}")
            raise Exception(f"Invalid schedule: {obj}")

    @property
    def payload(self):
        return serialize(
            {
                "name": self.task_name,
                "args": self.args if self.args is not None else [],
                "kwargs": self.kwargs if self.kwargs is not None else {},
            }
        )


class Scheduler:
    __slots__ = [
        "wakaq",
        "schedules",
    ]

    def __init__(self, wakaq=None):
        self.wakaq = wakaq

    def start(self):
        setup_logging(self.wakaq, is_scheduler=True)
        log.info("starting scheduler")

        if len(self.wakaq.schedules) == 0:
            log.error("no scheduled tasks found")
            raise Exception("No scheduled tasks found.")

        self.schedules = []
        for schedule in self.wakaq.schedules:
            self.schedules.append(CronTask.create(schedule, queues_by_name=self.wakaq.queues_by_name))

        self._run()

    def _run(self):
        base = datetime.utcnow()
        upcoming_tasks = []

        while True:
            for cron_task in upcoming_tasks:
                task = self.wakaq.tasks[cron_task.task_name]
                if cron_task.queue:
                    queue = self.wakaq.queues_by_name[cron_task.queue]
                elif task.queue:
                    queue = task.queue
                else:
                    queue = self.wakaq.queues[-1]
                log.debug(f"run scheduled task on queue {queue.name}: {task.name}")
                self.wakaq.broker.lpush(queue.broker_key, cron_task.payload)

            upcoming_tasks = []
            crons = [(croniter(x.schedule, base).get_next(datetime), x) for x in self.schedules]
            sleep_until = base + timedelta(days=1)

            for dt, cron_task in crons:
                if self._is_same_minute_precision(dt, sleep_until):
                    upcoming_tasks.append(cron_task)
                elif dt < sleep_until:
                    sleep_until = dt
                    upcoming_tasks = [cron_task]

            # sleep until the next scheduled task
            time.sleep((sleep_until - base).total_seconds())

            base = sleep_until

    def _is_same_minute_precision(self, a, b):
        return a.strftime("%Y%m%d%H%M") == b.strftime("%Y%m%d%H%M")
