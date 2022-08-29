# WakaQ
Distributed background task queue for Python backed by Redis, a super minimal Celery.

## Features

* Queue priority
* Delayed tasks (run tasks after a timedelta eta)
* Scheduled periodic tasks
* [Broadcast][broadcast] a task to all workers
* Task [soft][soft timeout] and [hard][hard timeout] timeout limits
* Super minimal

Want more features like rate limiting, task deduplication, etc? Too bad, feature PRs are not accepted. Maximal features belong in your app’s worker tasks.

## ToDo

* [x] scheduler
* [x] admin info inspection, purging queues, etc
* [x] broadcast task
* [x] handle child process crash/exception and re-fork
* [x] timeouts
* [ ] logging
* [ ] error exception handlers (maybe skip this because logging captures it)
* [ ] signals
* [ ] pre_fork(parent only) and post_fork(children only) hooks/signals
* [ ] combat memory leaks with `max_mem_per_worker` and `max_tasks_per_worker` which re-fork worker child processes

## Example

```python
from wakaq import WakaQ, Queue, CronTask
from datetime import timedelta


wakaq = WakaQ(
    queues=[
        (0, 'a-high-priority-queue'),
        (1, 'a-medium-priority-queue'),
        (2, 'a-low-priority-queue'),
        'default-lowest-priority-queue',
        Queue('another-queue', priority=3),
    ],
    soft_timeout=30,  # seconds
    hard_timeout=timedelta(minutes=1),
    schedules=[
        CronTask('* * * * *', 'mytask', queue='a-medium-priority-queue', args=[2, 2], kwargs={}),
        ('*/10 * * * *', 'mytask', [1, 1], {}),  # runs on default lowest priority queue
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
