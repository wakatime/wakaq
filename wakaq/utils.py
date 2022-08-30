# -*- coding: utf-8 -*-


import os
import sys
from importlib import import_module


def import_app(app):
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    module_path, class_name = app.rsplit(".", 1)
    module = import_module(module_path)
    wakaq = getattr(module, class_name)
    return wakaq


def inspect(app):
    result = {
        "queues": {},
        "eta": {
            "pending_tasks": pending_eta_tasks(app),
        },
    }
    for queue in app.queues:
        result["queues"][queue.name] = {
            "name": queue.name,
            "priority": queue.priority,
            "broker_key": queue.broker_key,
            "pending_tasks": pending_tasks_in_queue(app, queue),
        }
    return result


def pending_tasks_in_queue(app, queue=None, queue_name: str = None) -> int:
    if not queue:
        if queue_name is None:
            return 0
        queue = app.queues_by_name.get(queue_name)
        if not queue:
            return 0
    return app.broker.llen(queue.broker_key)


def purge_queue(app, queue_name: str):
    if queue_name is None:
        return
    queue = app.queues_by_name.get(queue_name)
    if not queue:
        return
    app.broker.delete(queue.broker_key)


def pending_eta_tasks(app) -> int:
    return app.broker.zcount(app.eta_task_key, "-inf", "+inf")


def purge_eta_tasks(app):
    app.broker.delete(app.eta_task_key)


def kill(pid, signum):
    try:
        os.kill(pid, signum)
    except IOError:
        pass


def read_fd(fd):
    try:
        os.set_blocking(fd, False)
        return os.read(fd, 64000)
    except OSError:
        return b""


class Context:
    __slots__ = ["value"]

    def __init__(self):
        self.value = None

    def get(self):
        return self.value

    def set(self, val):
        self.value = val


current_task = Context()
