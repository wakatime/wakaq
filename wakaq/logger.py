# -*- coding: utf-8 -*-


import sys
from logging import getLogger, captureWarnings, StreamHandler, Formatter as FormatterBase
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
            record.__dict__.setdefault('task', None)
        return super().format(record)


class Logger:
    def __init__(self, wakaq):
        self.wakaq = wakaq
        captureWarnings(True)
        handler = WatchedFileHandler(wakaq.log_file, encoding='utf-8')
        handler.setFormatter(Formatter(format))


def setup_logging(wakaq, is_child=None):
    logger = getLogger('wakaq')

    for handler in logger.handlers:
        logger.removeHandler(handler)

    logger.setLevel(wakaq.log_level)

    out = sys.stdout if is_child or not wakaq.log_file else wakaq.log_file
    handler = (StreamHandler if is_child or not wakaq.log_file else WatchedFileHandler)(out)
    handler.setLevel(wakaq.log_level)

    formatter = Formatter(wakaq)
    handler.setFormatter(formatter)

    logger.addHandler(handler)
