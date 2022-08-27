# -*- coding: utf-8 -*-


from importlib import import_module


def import_app(app):
    module_path, class_name = app.rsplit('.', 1)
    module = import_module(module_path)
    wakaq = getattr(module, class_name)
    return wakaq


def inspect(app):
    result = {
        'queues': {},
        'eta_tasks': {},
    }
    for queue in app.queues:
        result['queues'][queue.name] = {
            'name': queue.name,
            'priority': queue.priority,
            'broker_key': queue.broker_key,
            'pending_tasks': app.broker.llen(queue.broker_key),
        }
    result['eta_tasks']['pending_tasks'] = app.broker.zcount(app.eta_task_key, '-inf', '+inf')
    return result


def purge_queue(app, queue_name: str) -> int:
    if queue_name is None:
        return 0
    queue = app.queues_by_name.get(queue_name)
    if not queue:
        return 0
    count = app.broker.llen(queue.broker_key)
    app.broker.delete(queue.broker_key)
    return count


def purge_eta_tasks(app) -> int:
    count = app.broker.zcount(app.eta_task_key, '-inf', '+inf')
    app.broker.delete(app.eta_task_key)
    return count
