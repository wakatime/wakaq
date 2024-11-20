import ast
import calendar
import operator
import os
import sys
from datetime import datetime, timedelta
from importlib import import_module
from typing import Union

import psutil

from .serializer import deserialize


def import_app(app):
    """Import and return the WakaQ instance from the specified module path."""

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    try:
        module_path, class_name = app.rsplit(".", 1)
    except ValueError:
        raise Exception(
            f"Invalid app path: {app}. App must point to a WakaQ instance. For ex: yourapp.background.wakaq"
        )

    module = import_module(module_path)
    wakaq = getattr(module, class_name)
    from . import WakaQ

    if not isinstance(wakaq, WakaQ):
        raise Exception(f"Invalid app path: {app}. App must point to a WakaQ instance.")
    return wakaq


def inspect(app):
    """Return the queues and their respective pending task counts, and the number of workers connected."""

    queues = {}
    for queue in app.queues:
        queues[queue.name] = {
            "name": queue.name,
            "priority": queue.priority,
            "broker_key": queue.broker_key,
            "broker_eta_key": queue.broker_eta_key,
            "pending_tasks": num_pending_tasks_in_queue(app, queue),
            "pending_eta_tasks": num_pending_eta_tasks_in_queue(app, queue),
        }
    return {
        "queues": queues,
        "workers": num_workers_connected(app),
    }


def pending_tasks_in_queue(app, queue=None, queue_name: str = None, limit: int = 20) -> list:
    """Retrieve a list of pending tasks from a queue, without removing them from the queue."""

    if not queue:
        if queue_name is None:
            return []
        queue = app.queues_by_name.get(queue_name)
        if not queue:
            return []

    if not limit:
        limit = 0

    tasks = app.broker.lrange(queue.broker_key, 0, limit - 1)
    return [deserialize(task) for task in tasks]


def pending_eta_tasks_in_queue(
    app,
    queue=None,
    queue_name: str = None,
    before: Union[datetime, timedelta, int] = None,
    limit: int = 20,
) -> list:
    """Retrieve a list of pending eta tasks from a queue, without removing them from the queue."""

    if not queue:
        if queue_name is None:
            return []
        queue = app.queues_by_name.get(queue_name)
        if not queue:
            return []
    params = []
    if before:
        cmd = "ZRANGEBYSCORE"
        if isinstance(before, timedelta):
            before = datetime.utcnow() + before
        if isinstance(before, datetime):
            before = calendar.timegm(before.utctimetuple())
        params.extend([0, before])
        params.append("WITHSCORES")
        if limit:
            params.extend(["LIMIT", 0, limit])
    else:
        cmd = "ZRANGE"
        if not limit:
            limit = 0
        params.extend([0, limit - 1])
        params.append("WITHSCORES")
    tasks = app.broker.execute_command(cmd, queue.broker_eta_key, *params)
    payloads = []
    for n in range(0, len(tasks), 2):
        payload = deserialize(tasks[n])
        payload["eta"] = datetime.utcfromtimestamp(int(tasks[n + 1]))
        payloads.append(payload)
    return payloads


def num_pending_tasks_in_queue(app, queue=None, queue_name: str = None) -> int:
    """Count and return the number of pending tasks in a queue."""

    if not queue:
        if queue_name is None:
            return 0
        queue = app.queues_by_name.get(queue_name)
        if not queue:
            return 0
    return app.broker.llen(queue.broker_key)


def num_pending_eta_tasks_in_queue(app, queue=None, queue_name: str = None) -> int:
    """Count and return the number of pending eta tasks in a queue."""

    if not queue:
        if queue_name is None:
            return 0
        queue = app.queues_by_name.get(queue_name)
        if not queue:
            return 0
    return app.broker.zcount(queue.broker_eta_key, "-inf", "+inf")


def num_workers_connected(app) -> int:
    """Count and return the number of connected workers."""

    return app.broker.pubsub_numsub(app.broadcast_key)[0][1]


def purge_queue(app, queue_name: str):
    """Empty a queue, discarding any pending tasks."""

    if queue_name is None:
        return
    queue = app.queues_by_name.get(queue_name)
    if not queue:
        return
    app.broker.delete(queue.broker_key)


def purge_eta_queue(app, queue_name: str):
    """Empty a queue of any pending eta tasks."""

    if queue_name is None:
        return
    queue = app.queues_by_name.get(queue_name)
    if not queue:
        return
    app.broker.delete(queue.broker_eta_key)


def kill(pid, signum):
    try:
        os.kill(pid, signum)
    except IOError:
        pass


def read_fd(fd):
    try:
        return os.read(fd, 64000).decode("utf8")
    except OSError:
        return ""


def write_fd_or_raise(fd, s):
    os.write(fd, s.encode("utf8"))


def write_fd(fd, s):
    try:
        write_fd_or_raise(fd, s)
    except:
        pass


def close_fd(fd):
    try:
        os.close(fd)
    except:
        pass


def flush_fh(fh):
    try:
        fh.flush()
    except:
        pass


def mem_usage_percent():
    return int(round(psutil.virtual_memory().percent))


_operations = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Pow: operator.pow,
}


def _safe_eval(node, variables, functions):
    if isinstance(node, ast.Num):
        return node.n
    elif isinstance(node, ast.Name):
        try:
            return variables[node.id]
        except KeyError:
            raise Exception(f"Unknown variable: {node.id}")
    elif isinstance(node, ast.BinOp):
        try:
            op = _operations[node.op.__class__]
        except KeyError:
            raise Exception(f"Unknown operation: {node.op.__class__}")
        left = _safe_eval(node.left, variables, functions)
        right = _safe_eval(node.right, variables, functions)
        if isinstance(node.op, ast.Pow):
            assert right < 100
        return op(left, right)
    elif isinstance(node, ast.Call):
        assert not node.keywords and not node.starargs and not node.kwargs
        assert isinstance(node.func, ast.Name), "Unsafe function derivation"
        try:
            func = functions[node.func.id]
        except KeyError:
            raise Exception(f"Unknown function: {node.func.id}")
        args = [_safe_eval(arg, variables, functions) for arg in node.args]
        return func(*args)
    assert False, "Unsafe operation"


def safe_eval(expr, variables={}, functions={}):
    node = ast.parse(expr, "<string>", "eval").body
    return _safe_eval(node, variables, functions)


class Context:
    __slots__ = ["value"]

    def __init__(self):
        self.value = None

    def get(self):
        return self.value

    def set(self, val):
        self.value = val


current_task = Context()


def exception_in_chain(e, exception_type):
    if isinstance(e, exception_type):
        return True
    while (e.__cause__ or e.__context__) is not None:
        if isinstance((e.__cause__ or e.__context__), exception_type):
            return True
        e = e.__cause__ or e.__context__
    return False


def get_timeouts(app, task=None, queue=None):
    soft_timeout = app.soft_timeout
    hard_timeout = app.hard_timeout
    if task and task.soft_timeout:
        soft_timeout = task.soft_timeout
    elif queue and queue.soft_timeout:
        soft_timeout = queue.soft_timeout
    if task and task.hard_timeout:
        hard_timeout = task.hard_timeout
    elif queue and queue.hard_timeout:
        hard_timeout = queue.hard_timeout
    return soft_timeout, hard_timeout
