"""K1 keys and lossless canonical serialization shared by Spark and tests."""
import hashlib
import json
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal

_CLEAN_PART = re.compile(r"\A[a-z0-9]+(?:_[a-z0-9]+)*\Z")
_KEY_PATTERN = re.compile(r"\A[a-z0-9_]{1,64}\Z")
_VI_MAP = (
    ("àáảãạăằắẳẵặâầấẩẫậÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬäåæ", "a" * 37),
    ("èéẻẽẹêềếểễệÈÉẺẼẸÊỀẾỂỄỆë", "e" * 23),
    ("ìíỉĩịÌÍỈĨỊîï", "i" * 12),
    ("òóỏõọôồốổỗộơờớởỡợÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢöø", "o" * 36),
    ("ùúủũụưừứửữựÙÚỦŨỤƯỪỨỬỮỰüû", "u" * 24),
    ("ỳýỷỹỵỲÝỶỸỴÿ", "y" * 11),
    ("đĐ", "dd"),
    ("çÇ", "cc"),
    ("ñÑ", "nn"),
)
VI_SRC = "".join(s for s, _ in _VI_MAP)
VI_DST = "".join(d for _, d in _VI_MAP)
assert len(VI_SRC) == len(VI_DST) and len(set(VI_SRC)) == len(VI_SRC)
_VI_TABLE = str.maketrans(VI_SRC, VI_DST)


def _slug_part(text):
    folded = text.translate(_VI_TABLE).lower()
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9_]+", "_", folded)).strip("_")


def _flatten_items(v):
    if isinstance(v, (list, tuple)):
        for item in v:
            yield from _flatten_items(item)
    else:
        yield v


def _format_human_key(slug, digest, clean):
    slug = slug or "k"
    if clean and len(slug) <= 64:
        return slug
    prefix = slug[:57].rstrip("_") or "k"
    return f"{prefix}_{digest[:6]}"


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
    if domain == "dim_source_system" and isinstance(values, (list, tuple)) and len(values) == 1:
        source_id = values[0]
        if isinstance(source_id, str) and _KEY_PATTERN.fullmatch(source_id):
            return source_id
    digest = hashlib.sha256(canonical_json(["K1", domain, values]).encode("utf-8")).hexdigest()
    items = list(_flatten_items(values))
    clean = bool(items)
    tokens = []
    for item in items:
        if item is None:
            clean = False
            continue
        if isinstance(item, str):
            clean = clean and bool(_CLEAN_PART.fullmatch(item))
            tok = _slug_part(item)
        else:
            clean = False
            raw = item.isoformat() if isinstance(item, date) else ("true" if item is True else "false" if item is False else str(item))
            tok = _slug_part(raw)
        if tok:
            tokens.append(tok)
    slug = re.sub(r"_+", "_", "_".join(tokens)).strip("_")
    return _format_human_key(slug, digest, clean)


def fingerprint(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _make_converter():
    import numpy as np
    import pandas as pd

    def to_py(v):
        if v is None or v is pd.NA or v is pd.NaT:
            return None
        t = type(v)
        if t in (str, int, bool, Decimal, date, datetime):
            return v
        if t is float:
            if math.isnan(v):
                return None
            if v.is_integer():
                return int(v)
            return v
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        if isinstance(v, np.generic):
            return to_py(v.item())
        if isinstance(v, (list, tuple, np.ndarray)):
            return [to_py(x) for x in v]
        if isinstance(v, dict):
            return {k: to_py(val) for k, val in v.items()}
        if hasattr(v, "asDict"):
            return to_py(v.asDict(recursive=True))
        return v

    def series_to_pylist(s):
        if s.dtype.kind in ("i", "u", "b"):
            return s.tolist()
        return [to_py(v) for v in s.tolist()]

    return pd, np, to_py, series_to_pylist


def _catalyst_json_elem(col):
    from pyspark.sql import functions as F
    return F.regexp_extract(F.to_json(F.array(col)), r"^\[(.*)\]$", 1)


def spark_json(*columns, names=None):
    """Vectorized Catalyst expression preserving exact canonical_json semantics."""
    from pyspark.sql import functions as F
    if names is None:
        return F.concat(F.lit("["), F.concat_ws(",", *(_catalyst_json_elem(c) for c in columns)), F.lit("]"))
    pairs = sorted(zip(names, columns), key=lambda kv: kv[0])
    fields = [F.concat(F.lit(f"{canonical_json(k)}:"), _catalyst_json_elem(c)) for k, c in pairs]
    return F.concat(F.lit("{"), F.concat_ws(",", *fields), F.lit("}"))


def spark_value_json(column, struct_fields=None):
    from pyspark.sql import functions as F, types as T
    if struct_fields is not None:
        ordered = sorted(struct_fields)
        return F.coalesce(
            F.to_json(
                F.transform(column, lambda x: F.struct(*(x[f].alias(f) for f in ordered))),
                options={"ignoreNullFields": "false"},
            ),
            F.lit("null"),
        )
    pd, np, to_py, _ = _make_converter()

    @F.pandas_udf(T.StringType())
    def _vec_val_json(s: pd.Series) -> pd.Series:
        if len(s) == 0:
            return pd.Series([], dtype="object")
        out = []
        for v in s.tolist():
            if v is None:
                out.append("null")
            elif isinstance(v, (list, tuple, np.ndarray)) and len(v) == 0:
                out.append("[]")
            else:
                out.append(canonical_json(to_py(v)))
        return pd.Series(out, dtype="object")
    return _vec_val_json(column)


def spark_key(domain, *columns, nested_source=None):
    from pyspark.sql import functions as F
    if domain == "dim_source_system" and len(columns) == 1 and nested_source is None:
        return columns[0].cast("string")
    elems = F.concat_ws(",", *(_catalyst_json_elem(c) for c in columns))
    if nested_source is not None:
        prefix = f'["K1",{canonical_json(domain)},[{canonical_json(nested_source)},['
        suffix = "]]]"
        slug_cols = [F.lit(nested_source), *columns]
    else:
        prefix = f'["K1",{canonical_json(domain)},['
        suffix = "]]"
        slug_cols = list(columns)
    digest = F.sha2(F.concat(F.lit(prefix), elems, F.lit(suffix)), 256)
    tokens = []
    clean = F.lit(bool(slug_cols))
    for col in slug_cols:
        json_elem = _catalyst_json_elem(col)
        s = col.cast("string")
        clean = clean & col.isNotNull() & json_elem.startswith('"') & s.rlike(r"\A[a-z0-9]+(?:_[a-z0-9]+)*\z")
        folded = F.lower(F.translate(s, VI_SRC, VI_DST))
        tok = F.regexp_replace(F.regexp_replace(F.regexp_replace(folded, r"[^a-z0-9_]+", "_"), r"_+", "_"), r"^_+|_+$", "")
        tokens.append(F.when(col.isNotNull() & (F.length(tok) > 0), tok))
    slug = F.concat_ws("_", *tokens)
    slug = F.when(F.length(slug) > 0, slug).otherwise(F.lit("k"))
    trunc = F.regexp_replace(F.substring(slug, 1, 57), r"_+$", "")
    trunc = F.when(F.length(trunc) > 0, trunc).otherwise(F.lit("k"))
    return F.when(clean & (F.length(slug) <= 64), slug).otherwise(F.concat(trunc, F.lit("_"), F.substring(digest, 1, 6)))
