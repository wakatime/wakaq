# -*- coding: utf-8 -*-


import click
import json

from .scheduler import Scheduler
from .worker import Worker
from .utils import import_app, inspect, purge_queue, purge_eta_tasks


@click.command()
@click.option('--app', required=True, help='Import path of the WakaQ instance.')
@click.option('--concurrency', type=click.INT, default=1, help='Number of worker processes.')
@click.option('--exclude-queues', default='', help='Comma separated list of queue names to not process.')
@click.option('--foreground', is_flag=True, help='Run in foreground; Default is to run as daemon in background.')
def worker(**options):
    """Run worker(s) to process tasks from queue(s) defined in your app."""
    try:
        options['exclude_queues'] = [x.strip().lower() for x in options['exclude_queues'].split(',')]
    except:
        click.fail(f'Invalid value for exclude_queues. Must be a list of queue names separated by periods: {options["exclude_queues"]}')

    wakaq = import_app(options.pop('app'))
    Worker(wakaq=wakaq, **options)


@click.command()
@click.option('--app', required=True, help='Import path of the WakaQ instance.')
@click.option('--foreground', is_flag=True, help='Run in foreground; Default is to run as daemon in background.')
def scheduler(**options):
    """Run a scheduler to enqueue periodic tasks based on a schedule defined in your app."""
    wakaq = import_app(options.pop('app'))
    result = Scheduler(wakaq=wakaq, **options)
    if result:
        click.fail(result)


@click.command()
@click.option('--app', required=True, help='Import path of the WakaQ instance.')
def info(**options):
    """Inspect and print info about your queues."""
    wakaq = import_app(options.pop('app'))
    click.echo(json.dumps(inspect(wakaq), indent=2, sort_keys=True))


@click.command()
@click.option('--app', required=True, help='Import path of the WakaQ instance.')
@click.option('--queue', help='Name of queue to purge.')
@click.option('--eta-tasks', is_flag=True, help='Purge all pending eta tasks.')
def purge(**options):
    """Remove and empty all pending tasks in a queue."""
    wakaq = import_app(options.pop('app'))
    queue_name = options.pop('queue', None)
    if queue_name is not None:
        count = purge_queue(wakaq, queue_name=queue_name)
        click.echo(f'Purged {count} tasks from {queue_name}.')
    if options.get('eta_tasks'):
        count = purge_eta_tasks(wakaq)
        click.echo(f'Purged {count} eta tasks.')


"""
TODO

1. DONE scheduler
2. signals
3. timeouts
4. logging
5. DONE admin info inspection, purging queues, etc
6.
7. handle child process crash/exception and re-fork

"""
