# -*- coding: utf-8 -*-


from base64 import b64decode, b64encode
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from json import JSONEncoder, dumps, loads


class CustomJSONEncoder(JSONEncoder):
    def __init__(self, *args, allow_nan=False, **kwargs):
        kwargs["allow_nan"] = allow_nan
        super().__init__(*args, **kwargs)

    def default(self, o):
        if isinstance(o, set):
            return list(o)
        if isinstance(o, Decimal):
            return {
                "__class__": "Decimal",
                "value": str(o),
            }
        if isinstance(o, datetime):
            if o.tzinfo is not None:
                o = o.astimezone(timezone.utc)
            return {
                "__class__": "datetime",
                "value": o.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        if isinstance(o, date):
            return {
                "__class__": "date",
                "value": o.strftime("%Y-%m-%d"),
            }
        if isinstance(o, timedelta):
            return {
                "__class__": "timedelta",
                "kwargs": {
                    "days": o.days,
                    "seconds": o.seconds,
                    "microseconds": o.microseconds,
                },
            }
        if isinstance(o, bytes):
            return {
                "__class__": "bytes",
                "value": b64encode(o).decode("ascii"),
            }
        return str(o)


def object_hook(obj):
    cls = obj.get("__class__")
    if not cls:
        return obj

    if cls == "Decimal":
        return Decimal(obj["value"])
    if cls == "datetime":
        return datetime.strptime(obj["value"], "%Y-%m-%dT%H:%M:%SZ")
    if cls == "date":
        return datetime.strptime(obj["value"], "%Y-%m-%d").date()
    if cls == "timedelta":
        return timedelta(**obj["kwargs"])
    if cls == "bytes":
        return b64decode(obj["value"])

    return obj


def serialize(*args, **kwargs):
    kwargs["cls"] = CustomJSONEncoder
    return dumps(*args, **kwargs)


def deserialize(*args, **kwargs):
    return loads(*args, object_hook=object_hook, **kwargs)
