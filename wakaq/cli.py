# -*- coding: utf-8 -*-


import json

import click

from .scheduler import Scheduler
from .utils import (
    import_app,
    inspect,
    pending_eta_tasks,
    pending_tasks_in_queue,
    purge_eta_tasks,
    purge_queue,
)
from .worker import Worker


@click.command()
@click.option("--app", required=True, help="Import path of the WakaQ instance.")
@click.option("--foreground", is_flag=True, help="Run in foreground; Default is to run as daemon in background.")
def worker(**options):
    """Run worker(s) to process tasks from queue(s) defined in your app."""
    wakaq = import_app(options.pop("app"))
    worker = Worker(wakaq=wakaq)
    worker.start(**options)


@click.command()
@click.option("--app", required=True, help="Import path of the WakaQ instance.")
@click.option("--foreground", is_flag=True, help="Run in foreground; Default is to run as daemon in background.")
def scheduler(**options):
    """Run a scheduler to enqueue periodic tasks based on a schedule defined in your app."""
    wakaq = import_app(options.pop("app"))
    scheduler = Scheduler(wakaq=wakaq)
    scheduler.start(**options)


@click.command()
@click.option("--app", required=True, help="Import path of the WakaQ instance.")
def info(**options):
    """Inspect and print info about your queues."""
    wakaq = import_app(options.pop("app"))
    click.echo(json.dumps(inspect(wakaq), indent=2, sort_keys=True))


@click.command()
@click.option("--app", required=True, help="Import path of the WakaQ instance.")
@click.option("--queue", help="Name of queue to purge.")
@click.option("--eta-tasks", is_flag=True, help="Purge all pending eta tasks.")
def purge(**options):
    """Remove and empty all pending tasks in a queue."""
    wakaq = import_app(options.pop("app"))
    queue_name = options.pop("queue", None)
    if queue_name is not None:
        count = pending_tasks_in_queue(wakaq, queue_name=queue_name)
        purge_queue(wakaq, queue_name=queue_name)
        click.echo(f"Purged {count} tasks from {queue_name}.")
    if options.get("eta_tasks"):
        count = pending_eta_tasks(wakaq)
        purge_eta_tasks(wakaq)
        click.echo(f"Purged {count} eta tasks.")
