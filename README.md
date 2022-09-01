# WakaQ
Distributed background task queue for Python backed by Redis, a super minimal Celery.

## Features

* Queue priority
* Delayed tasks (run tasks after a timedelta eta)
* Scheduled periodic tasks
* [Broadcast][broadcast] a task to all workers
* Task [soft][soft timeout] and [hard][hard timeout] timeout limits
* Combat memory leaks with `max_mem_percent` or `max_tasks_per_worker`
* Super minimal

Want more features like rate limiting, task deduplication, etc? Too bad, feature PRs are not accepted. Maximal features belong in your app’s worker tasks.

## Installing

    pip install wakaq

## Example

```python
import logging
from wakaq import WakaQ, Queue, CronTask
from datetime import timedelta


wakaq = WakaQ(

    # List your queues and their priorities.
    # Queues can be defined as Queue instances, tuples, or just a str.
    queues=[
        (0, 'a-high-priority-queue'),
        (1, 'a-medium-priority-queue'),
        (2, 'a-low-priority-queue'),
        'default-lowest-priority-queue',
        Queue('another-queue', priority=3),
    ],

    # Number of worker processes. Must be an int or str which evaluates to an
    # int. The variable "cores" is replaced with the number of processors on
    # the current machine.
    concurrency="cores*4",

    # Raise SoftTimeout in a task if it runs longer than 30 seconds.
    soft_timeout=30,  # seconds

    # SIGKILL a task if it runs longer than 1 minute.
    hard_timeout=timedelta(minutes=1),

    # Combat memory leaks by reloading a worker (the one using the most RAM),
    # when the total machine RAM usage is at or greater than 98%.
    max_mem_percent=98,

    # Combat memory leaks by reloading a worker after it's processed 5000 tasks.
    max_tasks_per_worker=5000,

    # Schedule two tasks, the first runs every minute, the second once every ten minutes.
    # Scheduled tasks can be passed as CronTask instances or tuples.
    schedules=[

        # Runs mytask on the queue with priority 1.
        CronTask('* * * * *', 'mytask', queue='a-medium-priority-queue', args=[2, 2], kwargs={}),

        # Runs mytask once every 5 minutes.
        ('*/5 * * * *', 'mytask', [1, 1], {}),

        # Runs anothertask on the default lowest priority queue.
        ('*/10 * * * *', 'anothertask'),
    ],
)


@wakaq.task(queue='a-medium-priority-queue')
def mytask(x, y):
    print(x + y)


@wakaq.task
def anothertask():
    print("hello world")


@wakaq.after_worker_started
def after_worker_started():
    logger = logging.getLogger("wakaq")
    # runs once in each forked worker process
    # check the source code for more callback decorators


if __name__ == '__main__':
    # add 1 plus 1 on a worker somewhere, overwriting the task's queue from medium to high
    mytask.delay(1, 1, queue='a-high-priority-queue')
    # add 1 plus 1 on a worker somewhere, running on the default lowest priority queue
    anothertask.delay()
```


[broadcast]: https://github.com/wakatime/wakaq/blob/996af0ea337c53161ce78a4650ef7d08d44f6038/wakaq/task.py#L30
[soft timeout]: https://github.com/wakatime/wakaq/blob/996af0ea337c53161ce78a4650ef7d08d44f6038/wakaq/exceptions.py#L8
[hard timeout]: https://github.com/wakatime/wakaq/blob/996af0ea337c53161ce78a4650ef7d08d44f6038/wakaq/worker.py#L316
