# -*- coding: utf-8 -*-


class WakaQException(Exception):
    pass


class SoftTimeout(WakaQException):
    pass


class ReloadWorkerChild(WakaQException):
    pass
