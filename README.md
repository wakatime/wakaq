# ![logo](https://raw.githubusercontent.com/wakatime/wakaq/main/wakatime-logo.png "WakaQ") WakaQ

[![wakatime](https://wakatime.com/badge/github/wakatime/wakaq.svg)](https://wakatime.com/badge/github/wakatime/wakaq)

Background task queue for Python backed by Redis, a super minimal Celery.
Read about the motivation behind this project on [this blog post][blog launch] and the accompanying [Hacker News discussion][hacker news].
WakaQ is currently used in production at [WakaTime.com][wakatime].
WakaQ is also available in [TypeScript][wakaq-ts].

## Features

* Queue priority
* Delayed tasks (run tasks after a timedelta eta)
* Scheduled periodic tasks
* Tasks can be [async][asyncio] or normal synchronous functions
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


# use constants to prevent misspelling queue names
Q_HIGH = 'a-high-priority-queue'
Q_MED = 'a-medium-priority-queue'
Q_LOW = 'a-low-priority-queue'
Q_OTHER = 'another-queue'
Q_DEFAULT = 'default-lowest-priority-queue'


wakaq = WakaQ(

    # List your queues and their priorities.
    # Queues can be defined as Queue instances, tuples, or just a str.
    queues=[
        (0, Q_HIGH),
        (1, Q_MED),
        (2, Q_LOW),
        Queue(Q_OTHER, priority=3, max_retries=5, soft_timeout=300, hard_timeout=360),
        Q_DEFAULT,
    ],

    # Number of worker processes. Must be an int or str which evaluates to an
    # int. The variable "cores" is replaced with the number of processors on
    # the current machine.
    concurrency="cores*4",

    # Number of concurrent asyncio tasks per worker process. Must be an int or
    # str which evaluates to an int. The variable "cores" is replaced with the
    # number of processors on the current machine. Default is zero for no limit.
    async_concurrency=0,

    # Raise SoftTimeout or asyncio.CancelledError in a task if it runs longer
    # than 30 seconds. Can also be set per task or queue. If no soft timeout
    # set, tasks can run forever.
    soft_timeout=30,  # seconds

    # SIGKILL a task if it runs longer than 1 minute. Can be set per task or queue.
    hard_timeout=timedelta(minutes=1),

    # If the task soft timeouts, retry up to 3 times. Max retries comes first
    # from the task decorator if set, next from the Queue's max_retries,
    # lastly from the option below. If No max_retries is found, the task
    # is not retried on a soft timeout.
    max_retries=3,

    # Combat memory leaks by reloading a worker (the one using the most RAM),
    # when the total machine RAM usage is at or greater than 98%.
    max_mem_percent=98,

    # Combat memory leaks by reloading a worker after it's processed 5000 tasks.
    max_tasks_per_worker=5000,

    # Schedule two tasks, the first runs every minute, the second once every ten minutes.
    # Scheduled tasks can be passed as CronTask instances or tuples. To run scheduled
    # tasks you must keep a wakaq scheduler running as a daemon.
    schedules=[

        # Runs mytask on the queue with priority 1.
        CronTask('* * * * *', 'mytask', queue=Q_MED, args=[2, 2], kwargs={}),

        # Runs mytask once every 5 minutes.
        ('*/5 * * * *', 'mytask', [1, 1], {}),

        # Runs anothertask on the default lowest priority queue.
        ('*/10 * * * *', 'anothertask'),
    ],
)


# timeouts can be customized per task with a timedelta or integer seconds
@wakaq.task(queue=Q_MED, max_retries=7, soft_timeout=420, hard_timeout=480)
def mytask(x, y):
    print(x + y)


@wakaq.task
def a_cpu_intensive_task():
    print("hello world")


@wakaq.task
async def an_io_intensive_task():
    print("hello world")


@wakaq.wrap_tasks_with
def custom_task_decorator(fn, args, kwargs):
    # do something before each task runs, for ex: `with app.app_context():`
    if inspect.iscoroutinefunction(fn):
        await fn(*payload["args"], **payload["kwargs"])
    else:
        fn(*payload["args"], **payload["kwargs"])
    # do something after each task runs


if __name__ == '__main__':

    # add 1 plus 1 on a worker somewhere
    mytask.delay(1, 1)

    # add 1 plus 1 on a worker somewhere, overwriting the task's queue from medium to high
    mytask.delay(1, 1, queue=Q_HIGH)

    # print hello world on a worker somewhere, running on the default lowest priority queue
    anothertask.delay()

    # print hello world on a worker somewhere, after 10 seconds from now
    anothertask.delay(eta=timedelta(seconds=10))

    # print hello world on a worker concurrently, even if you only have 1 worker process
    an_io_intensive_task.delay()
```

## Deploying

#### Optimizing

See the [WakaQ init params][wakaq init] for a full list of options, like Redis host and Redis socket timeout values.

When using in production, make sure to [increase the max open ports][max open ports] allowed for your Redis server process.

When using eta tasks a Redis sorted set is used, so eta tasks are automatically deduped based on task name, args, and kwargs.
If you want multiple pending eta tasks with the same arguments, just add a throwaway random string to the task’s kwargs for ex: `str(uuid.uuid1())`.

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
KillSignal=SIGINT
LimitNOFILE=99999

[Install]
WantedBy=multi-user.target
```

Create a file at `/etc/systemd/system/wakaqworker.service` with the above contents, then run:

    systemctl daemon-reload && systemctl enable wakaqworker

## Running synchronously in tests or local dev environment

In dev and test environments, it’s easier to run tasks synchronously so you don’t need Redis or any worker processes.
The recommended way is mocking WakaQ:

```python
class WakaQMock:
    def __init__(self):
        self.task = TaskMock

    def wrap_tasks_with(self, fn):
        return fn


class TaskMock(object):
    fn = None
    name = None
    args = ()
    kwargs = {}

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0:
            self.fn = args[0]
            self.name = args[0].__name__
        else:
            self.args = args
            self.kwargs = kwargs

    def delay(self, *args, **kwargs):
        kwargs.pop("queue", None)
        kwargs.pop("eta", None)
        return self.fn(*args, **kwargs)

    def broadcast(self, *args, **kwargs):
        return

    def __call__(self, *args, **kwargs):
        if not self.fn:
            task = TaskMock(args[0])
            task.args = self.args
            task.kwargs = self.kwargs
            return task
        else:
            return self.fn(*args, **kwargs)
```

Then in dev and test environments instead of using `wakaq.WakaQ` use `WakaQMock`.


[wakatime]: https://wakatime.com
[broadcast]: https://github.com/wakatime/wakaq/blob/58a7e4ce29d9be928b16ffbf5c00c7106aab9360/wakaq/task.py#L65
[soft timeout]: https://github.com/wakatime/wakaq/blob/58a7e4ce29d9be928b16ffbf5c00c7106aab9360/wakaq/exceptions.py#L5
[hard timeout]: https://github.com/wakatime/wakaq/blob/58a7e4ce29d9be928b16ffbf5c00c7106aab9360/wakaq/worker.py#L590
[wakaq init]: https://github.com/wakatime/wakaq/blob/58a7e4ce29d9be928b16ffbf5c00c7106aab9360/wakaq/__init__.py#L47
[max open ports]: https://wakatime.com/blog/47-maximize-your-concurrent-web-server-connections
[blog launch]: https://wakatime.com/blog/56-building-a-distributed-task-queue-in-python
[hacker news]: https://news.ycombinator.com/item?id=32730038
[wakaq-ts]: https://github.com/wakatime/wakaq-ts
[asyncio]: https://docs.python.org/3/library/asyncio.html
