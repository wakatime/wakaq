# -*- coding: utf-8 -*-


import re


class Queue:
    __slots__ = [
        "name",
        "priority",
        "prefix",
        "soft_timeout",
        "hard_timeout",
        "max_retries",
    ]

    def __init__(self, name=None, priority=-1, prefix=None, soft_timeout=None, hard_timeout=None, max_retries=None):
        self.prefix = re.sub(r"[^a-zA-Z0-9_.-]", "", prefix or "wakaq")
        self.name = re.sub(r"[^a-zA-Z0-9_.-]", "", name)

        try:
            self.priority = int(priority)
        except:
            raise Exception(f"Invalid queue priority: {priority}")

        if soft_timeout:
            try:
                self.soft_timeout = int(soft_timeout)
            except:
                raise Exception(f"Invalid queue soft timeout: {soft_timeout}")
        else:
            self.soft_timeout = None

        if hard_timeout:
            try:
                self.hard_timeout = int(hard_timeout)
            except:
                raise Exception(f"Invalid queue hard timeout: {hard_timeout}")
        else:
            self.hard_timeout = None

        if max_retries:
            try:
                self.max_retries = int(max_retries)
            except:
                raise Exception(f"Invalid queue max retries: {max_retries}")
        else:
            self.max_retries = None

    @classmethod
    def create(cls, obj, queues_by_name=None):
        if isinstance(obj, cls):
            if queues_by_name is not None and obj.name not in queues_by_name:
                raise Exception(f"Unknown queue: {obj.name}")
            return obj
        elif isinstance(obj, (list, tuple)) and len(obj) == 2:
            if isinstance(obj[0], int):
                if queues_by_name is not None and obj[1] not in queues_by_name:
                    raise Exception(f"Unknown queue: {obj[1]}")
                return cls(priority=obj[0], name=obj[1])
            else:
                if queues_by_name is not None and obj[0] not in queues_by_name:
                    raise Exception(f"Unknown queue: {obj[0]}")
                return cls(name=obj[0], priority=obj[1])
        else:
            if queues_by_name is not None and obj not in queues_by_name:
                raise Exception(f"Unknown queue: {obj}")
            return cls(name=obj)

    @property
    def broker_key(self):
        return f"{self.prefix}:{self.name}"

    @property
    def broker_eta_key(self):
        return f"{self.prefix}:eta:{self.name}"
