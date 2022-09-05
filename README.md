# WakaQ
Distributed background task queue for Python backed by Redis, a super minimal Celery.

## Features

* Queue priority
* Delayed tasks (run tasks after a timedelta eta)
* Scheduled periodic tasks
* [Broadcast][broadcast] a task to all workers
* Task [soft][soft timeout] and [hard][hard timeout] timeout limits
* Optionally retry tasks on soft timeout
* Combat memory leaks with `max_mem_percent` or `max_tasks_per_worker`
* Super minimal

Want more features like rate limiting, task deduplication, etc? Too bad, feature PRs are not accepted. Maximal features belong in your app’s worker tasks.

## Installing

    pip install wakaq

## Using

```python
import logging
from datetime import timedelta
from wakaq import WakaQ, Queue, CronTask


wakaq = WakaQ(

    # List your queues and their priorities.
    # Queues can be defined as Queue instances, tuples, or just a str.
    queues=[
        (0, 'a-high-priority-queue'),
        (1, 'a-medium-priority-queue'),
        (2, 'a-low-priority-queue'),
        'default-lowest-priority-queue',
        Queue('another-queue', priority=3, default_max_retries=5),
    ],

    # Number of worker processes. Must be an int or str which evaluates to an
    # int. The variable "cores" is replaced with the number of processors on
    # the current machine.
    concurrency="cores*4",

    # Raise SoftTimeout in a task if it runs longer than 30 seconds.
    soft_timeout=30,  # seconds

    # SIGKILL a task if it runs longer than 1 minute.
    hard_timeout=timedelta(minutes=1),

    # If the task soft timeouts, retry up to 3 times. Max retries comes first
    # from the task decorator if set, next from the Queue's default_max_retries,
    # lastly from the option below. If No default_max_retries is found, the task
    # is not retried on a soft timeout.
    default_max_retries=3,

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


@wakaq.task(queue='a-medium-priority-queue', max_retries=7)
def mytask(x, y):
    print(x + y)


@wakaq.task
def anothertask():
    print("hello world")


@wakaq.wrap_tasks_with
def custom_task_decorator(fn):
    def inner(*args, **kwargs):
        # do something before each task runs
        fn(*args, **kwargs)
        # do something after each task runs
    return inner


if __name__ == '__main__':
    # add 1 plus 1 on a worker somewhere, overwriting the task's queue from medium to high
    mytask.delay(1, 1, queue='a-high-priority-queue')
    # add 1 plus 1 on a worker somewhere, running on the default lowest priority queue
    anothertask.delay()
```

## Deploying

#### Optimizing

When using in production, make sure to [increase the max open ports][max open ports] allowed for your Redis server process.

#### Running as a Daemon

Here’s an example systemd config to run `wakaq-worker` as a daemon:

```systemd
[Unit]
Description=WakaQ Worker Service

[Service]
WorkingDirectory=/opt/yourapp
ExecStart=/opt/yourapp/venv/bin/python /opt/yourapp/venv/bin/wakaq-worker --app=yourapp.wakaq
RemainAfterExit=no
Restart=always
RestartSec=30s
KillSignal=SIGQUIT
LimitNOFILE=99999

[Install]
WantedBy=multi-user.target
```

Create a file at `/etc/systemd/system/wakaqworker.service` with the above contents, then run:

    systemctl daemon-reload && systemctl enable wakaqworker



[broadcast]: https://github.com/wakatime/wakaq/blob/2186334f0917ab11ad21e39617ad8a3116169593/wakaq/task.py#L33
[soft timeout]: https://github.com/wakatime/wakaq/blob/2186334f0917ab11ad21e39617ad8a3116169593/wakaq/exceptions.py#L8
[hard timeout]: https://github.com/wakatime/wakaq/blob/2186334f0917ab11ad21e39617ad8a3116169593/wakaq/worker.py#L360
[max open ports]: https://wakatime.com/blog/47-maximize-your-concurrent-web-server-connections
