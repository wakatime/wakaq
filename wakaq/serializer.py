# -*- coding: utf-8 -*-


import math
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from json import JSONDecoder, JSONEncoder, dumps, loads

ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_DATETIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


class CustomJSONEncoder(JSONEncoder):
    def __init__(self, *args, allow_nan=False, **kwargs):
        kwargs["allow_nan"] = allow_nan
        super().__init__(*args, **kwargs)

    def default(self, o):
        if isinstance(o, set):
            return list(o)
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, datetime):
            if o.tzinfo is not None:
                o = o.astimezone(timezone.utc)
            return o.strftime("%Y-%m-%dT%H:%M:%SZ")
        if isinstance(o, date):
            return o.strftime("%Y-%m-%d")
        if isinstance(o, timedelta):
            if not o.microseconds:
                return o.total_seconds()
            prefix = "-" if o < timedelta(0) else ""
            left = str(int(math.floor(abs(o).total_seconds())))
            right = str(abs(o).microseconds).zfill(6)
            return Decimal(prefix + left + "." + right)
        return str(o)


class CustomJSONDecoder(JSONDecoder):
    def decode(self, obj):
        if isinstance(obj, str) and ISO_DATETIME_PATTERN.match(str(obj)):
            return datetime.strptime(str(obj), "%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(obj, str) and ISO_DATE_PATTERN.match(str(obj)):
            return datetime.strptime(str(obj), "%Y-%m-%d").date()
        else:
            return super().decode(obj)


def serialize(*args, **kwargs):
    kwargs["cls"] = CustomJSONEncoder
    return dumps(*args, **kwargs)


def deserialize(*args, **kwargs):
    kwargs["cls"] = CustomJSONDecoder
    return loads(*args, **kwargs)
