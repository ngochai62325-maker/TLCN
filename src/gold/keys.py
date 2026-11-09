"""K1 keys and lossless canonical serialization shared by Spark and tests."""
import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal


def json_value(value):
    if hasattr(value, "asDict"):
        value = value.asDict(recursive=True)
    if isinstance(value, dict):
        return {k: json_value(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [json_value(v) for v in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        # Spark returns naive UTC timestamps with session timezone UTC.
        return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        raise TypeError("Binary floats are forbidden in Gold canonical JSON")
    if value is None or type(value) in (str, int, bool):
        return value
    raise TypeError(f"Unsupported canonical JSON type: {type(value).__name__}")


def canonical_json(value):
    return json.dumps(json_value(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)


def key(domain, values):
    def validate(v):
        if isinstance(v, (list, tuple)):
            for item in v:
                validate(item)
        elif v is not None and type(v) not in (str, int, bool) and type(v) is not date:
            raise TypeError("K1 natural keys allow only strings, integers, booleans, dates, NULL and arrays")
    validate(values)
    return hashlib.sha256(canonical_json(["K1", domain, values]).encode("utf-8")).hexdigest()


def fingerprint(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def spark_json(*columns, names=None):
    """Python UDF deliberately avoids Spark JSON decimal/NULL configuration drift."""
    from pyspark.sql import functions as F, types as T
    if names is None:
        return F.udf(lambda *xs: canonical_json(list(xs)), T.StringType())(*columns)
    return F.udf(lambda *xs: canonical_json(dict(zip(names, xs))), T.StringType())(*columns)


def spark_value_json(column):
    from pyspark.sql import functions as F, types as T
    return F.udf(canonical_json, T.StringType())(column)


def spark_key(domain, *columns, nested_source=None):
    from pyspark.sql import functions as F, types as T
    if nested_source is not None:
        return F.udf(lambda *xs: key(domain, [nested_source, list(xs)]), T.StringType())(*columns)
    return F.udf(lambda *xs: key(domain, list(xs)), T.StringType())(*columns)
