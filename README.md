# WakaQ
Distributed background task queue for Python backed by Redis, a super minimal Celery.

## Features

* Queue priority
* Delayed tasks (run tasks after a timedelta eta)
* Scheduled periodic tasks
* [Broadcast][broadcast] a task to all workers
* Task [soft][soft timeout] and [hard][hard timeout] timeout limits
* Combat memory leaks by reloading the worker using most RAM, when machine RAM usage at `max_mem_percent`
* Combat memory leaks by reloading workers after `max_tasks_per_worker` processed
* Super minimal

Want more features like rate limiting, task deduplication, etc? Too bad, feature PRs are not accepted. Maximal features belong in your app’s worker tasks.

## ToDo

* [x] scheduler
* [x] admin info inspection, purging queues, etc
* [x] broadcast task
* [x] handle child process crash/exception and re-fork
* [x] timeouts
* [x] combat memory leaks with `max_mem_percent` and `max_tasks_per_worker`
* [ ] logging
* [ ] error exception handlers (maybe skip this because logging captures it)
* [ ] signals
* [ ] pre_fork(parent only) and post_fork(children only) hooks/signals

## Example

```python
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

    # Raise SoftTimeout in a task if it runs longer than 30 seconds.
    soft_timeout=30,  # seconds

    # SIGKILL a task if it runs longer than 1 minute.
    hard_timeout=timedelta(minutes=1),

    # Combat memory leaks by reloading a child worker (the one using the most RAM),
    # when the total machine RAM usage is at or greater than 98%.
    max_mem_percent=98,

    # Combat memory leaks by reloading a worker after it's processed 5000 tasks.
    max_tasks_per_worker=5000,

    # Schedule two tasks, the first runs every minute, the second once every ten minutes.
    # Scheduled tasks can be passed as CronTask instances or tuples.
    schedules=[

        # Runs mytask on the queue with priority 1.
        CronTask('* * * * *', 'mytask', queue='a-medium-priority-queue', args=[2, 2], kwargs={}),

        # Runs anothertask on the default lowest priority queue.
        ('*/10 * * * *', 'anothertask', [1, 1], {}),
    ],
)


@wakaq.task(queue='a-medium-priority-queue')
def mytask(x, y):
    print(x + y)


@wakaq.task
def anothertask(x, y):
    print(x + y)


if __name__ == '__main__':
    # add 1 plus 1 on a worker somewhere, overwriting the task's queue from medium to high
    mytask.delay(1, 1, queue='a-high-priority-queue')
    # add 1 plus 1 on a worker somewhere, running on the default lowest priority queue
    anothertask.delay(1, 1)
```


[broadcast]: https://github.com/wakatime/wakaq/blob/804a4afd6c66a9eafa0bdbe7e49d7484079cb25e/wakaq/task.py#L44
[soft timeout]: https://github.com/wakatime/wakaq/blob/804a4afd6c66a9eafa0bdbe7e49d7484079cb25e/wakaq/exceptions.py#L4
[hard timeout]: https://github.com/wakatime/wakaq/blob/804a4afd6c66a9eafa0bdbe7e49d7484079cb25e/wakaq/worker.py#L199
