# -*- coding: utf-8 -*-


import sys
from logging import Formatter as FormatterBase
from logging import StreamHandler, captureWarnings, getLogger
from logging.handlers import WatchedFileHandler

from .utils import current_task


class Formatter(FormatterBase):
    def __init__(self, wakaq):
        self.wakaq = wakaq
        super().__init__(wakaq.log_format)

    def format(self, record):
        task = current_task.get()
        if task:
            self._fmt = self.wakaq.task_log_format
            self._style._fmt = self.wakaq.task_log_format
            record.__dict__.update(task=task.name)
        else:
            self._fmt = self.wakaq.log_format
            self._style._fmt = self.wakaq.log_format
            record.__dict__.setdefault("task", None)
        return super().format(record)


def setup_logging(wakaq, is_child=None, is_scheduler=None):
    logger = getLogger("wakaq")

    for handler in logger.handlers:
        logger.removeHandler(handler)

    log_file = wakaq.scheduler_log_file if is_scheduler else wakaq.worker_log_file
    log_level = wakaq.scheduler_log_level if is_scheduler else wakaq.worker_log_level

    logger.setLevel(log_level)
    captureWarnings(True)

    out = sys.stdout if is_child or not log_file else log_file
    options = {}
    if not is_child and log_file:
        options["encoding"] = "utf8"
    handler = (StreamHandler if is_child or not log_file else WatchedFileHandler)(out, **options)
    handler.setLevel(log_level)

    formatter = Formatter(wakaq)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
