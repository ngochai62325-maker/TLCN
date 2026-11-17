#!/usr/bin/env python3
"""Extract World Bank and USDA rice price series directly from iceberg.gold_local via Trino HTTP."""
import argparse
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import subprocess
import time
from urllib.parse import urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRINO_URL = "http://localhost:18080"
DEFAULT_TRINO_USER = "tlcn_forecast"
DEFAULT_RUNNER_CONTAINER = "tlcn-local-runner-1"
DEFAULT_OUTPUT_DIR = ROOT / "exports" / "forecast"
GOLD_NAMESPACE = "iceberg.gold_local"
START_MONTH_KEY = 200901
END_MONTH_KEY = 202512
EVAL_START_MONTH_KEY = 201001
MAX_HTTP_BYTES = 10 * 1024 * 1024
MAX_ROWS = 10000
QUERY_TIMEOUT_SECONDS = 30.0
MIN_WB_USDA_CORRELATION = 0.97

TARGET_TABLES = ("dim_price_series", "fact_price_monthly", "fact_usda_price_reference")

MONTH_NUMBERS = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}

SERIES_SPECS = {
    "wb_vn5_usd_t": {
        "source": "worldbank_pinksheet",
        "price_series_key": "worldbank_pinksheet_rice_viet_namese_5",
        "identity": ["rice_viet_namese_5"],
        "unit": "USD/tonne",
        "role": "target",
    },
    "wb_urea_usd_t": {
        "source": "worldbank_pinksheet",
        "price_series_key": "worldbank_pinksheet_urea",
        "identity": ["urea"],
        "unit": "USD/tonne",
        "role": "cost_feature",
    },
    "wb_crude_oil_usd_bbl": {
        "source": "worldbank_pinksheet",
        "price_series_key": "worldbank_pinksheet_crude_oil_average",
        "identity": ["crude_oil_average"],
        "unit": "USD/barrel",
        "role": "cost_feature",
    },
    "usda_vn5_usd_t": {
        "source": "usda_rice_yearbook",
        "price_series_key": "usda_rice_yearbook_vietnam_5_percent_broken_freight_on_bo_dd22fa",
        "identity": ["VIETNAM", "5-PERCENT BROKEN", "FREIGHT ON BOARD PRICE"],
        "unit": "USD/tonne",
        "role": "cross_source_reference",
    },
    "usda_thai5_usd_t": {
        "source": "usda_rice_yearbook",
        "price_series_key": "usda_rice_yearbook_thailand_5_percent_broken_freight_on_b_10e286",
        "identity": ["THAILAND", "5-PERCENT BROKEN", "FREIGHT ON BOARD PRICE"],
        "unit": "USD/tonne",
        "role": "competitor_feature",
    },
    "usda_india_broken5_usd_t": {
        "source": "usda_rice_yearbook",
        "price_series_key": "usda_rice_yearbook_india_5_percent_broken_freight_on_boar_3c14de",
        "identity": ["INDIA", "5-PERCENT BROKEN", "FREIGHT ON BOARD PRICE"],
        "unit": "USD/tonne",
        "role": "competitor_shock_indicator",
    },
    "usda_india_parb5_usd_t": {
        "source": "usda_rice_yearbook",
        "price_series_key": "usda_rice_yearbook_india_5_percent_parboiled_freight_on_b_51dde4",
        "identity": ["INDIA", "5-PERCENT PARBOILED", "FREIGHT ON BOARD PRICE"],
        "unit": "USD/tonne",
        "role": "competitor_feature",
    },
    "usda_pak5_usd_t": {
        "source": "usda_rice_yearbook",
        "price_series_key": "usda_rice_yearbook_pakistan_5_percent_broken_freight_on_b_5d49f7",
        "identity": ["PAKISTAN", "5-PERCENT BROKEN", "FREIGHT ON BOARD PRICE"],
        "unit": "USD/tonne",
        "role": "competitor_feature",
    },
}

SERIES_BY_KEY = {spec["price_series_key"]: (col, spec) for col, spec in SERIES_SPECS.items()}


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_month_key(month_key: int) -> tuple[int, int]:
    if not isinstance(month_key, int) or isinstance(month_key, bool):
        raise ValueError(f"Invalid integer month_key: {month_key!r}")
    year, month = divmod(month_key, 100)
    if not (1900 <= year <= 2100 and 1 <= month <= 12):
        raise ValueError(f"Invalid YYYYMM month_key: {month_key}")
    return year, month


def build_calendar_spine(start_month_key: int = START_MONTH_KEY, end_month_key: int = END_MONTH_KEY) -> list[int]:
    """Return dense YYYYMM calendar month keys inclusive of both endpoints."""
    sy, sm = _parse_month_key(start_month_key)
    ey, em = _parse_month_key(end_month_key)
    start_idx = sy * 12 + (sm - 1)
    end_idx = ey * 12 + (em - 1)
    if start_idx > end_idx:
        raise ValueError(f"Reversed month range: {start_month_key}..{end_month_key}")
    return [(idx // 12) * 100 + (idx % 12) + 1 for idx in range(start_idx, end_idx + 1)]


def convert_usda_to_month_key(reference_year: int, reference_period: str) -> int:
    """Convert USDA August-July marketing-year period to calendar YYYYMM month_key."""
    if type(reference_year) is not int:
        raise ValueError(f"Invalid USDA reference_year: {reference_year!r}")
    if reference_period not in MONTH_NUMBERS:
        raise ValueError(f"Unexpected USDA reference_period (expected month name): {reference_period!r}")
    month_num = MONTH_NUMBERS[reference_period]
    calendar_year = reference_year if month_num >= 8 else reference_year + 1
    return calendar_year * 100 + month_num


def parse_decimal(value) -> Decimal | None:
    """Parse a nullable numeric cell into an exact finite Decimal."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Unexpected boolean price value: {value!r}")
    try:
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid decimal value: {value!r}") from exc
    if not dec.is_finite():
        raise ValueError(f"Non-finite decimal value: {value!r}")
    return dec


def trino_execute(
    sql: str,
    base_url: str = DEFAULT_TRINO_URL,
    user: str = DEFAULT_TRINO_USER,
    opener=urlopen,
    clock=time.monotonic,
    timeout_seconds: float = QUERY_TIMEOUT_SECONDS,
) -> dict:
    """Execute a read-only SQL query directly against Trino HTTP /v1/statement."""
    base = base_url.rstrip("/")
    parsed_base = urlparse(base)
    if parsed_base.scheme not in {"http", "https"} or not parsed_base.hostname:
        raise ValueError(f"Invalid Trino URL: {base_url!r}")

    def checked_uri(uri: str) -> str:
        target = urlparse(uri)
        if (target.scheme, target.hostname, target.port) != (parsed_base.scheme, parsed_base.hostname, parsed_base.port):
            raise RuntimeError(f"Unexpected Trino polling destination: {uri}")
        return uri

    headers = {
        "X-Trino-User": user,
        "X-Trino-Client-Capabilities": "PARAMETRIC_DATETIME",
        "X-Trino-Session": "query_max_memory=128MB,query_max_execution_time=30s,query_max_run_time=30s",
    }
    request = Request(base + "/v1/statement", data=sql.encode("utf-8"), headers=headers)
    started = clock()
    consumed = 0
    columns = []
    rows = []
    next_uri = None
    last_page = {}

    try:
        while True:
            remaining = timeout_seconds - (clock() - started)
            if remaining <= 0:
                raise TimeoutError("Trino HTTP query time budget exceeded")
            with opener(request, timeout=min(10.0, remaining)) as response:
                raw = response.read(MAX_HTTP_BYTES - consumed + 1)
            consumed += len(raw)
            if consumed > MAX_HTTP_BYTES:
                raise RuntimeError("Trino HTTP response byte budget exceeded")
            last_page = json.loads(raw, parse_float=Decimal)
            next_uri = checked_uri(last_page["nextUri"]) if last_page.get("nextUri") else None
            if last_page.get("error"):
                err = last_page["error"]
                raise RuntimeError(f"Trino {last_page.get('id')} {err.get('errorName', '')}: {err.get('message')}")
            if last_page.get("columns"):
                columns = last_page["columns"]
            if last_page.get("data"):
                rows.extend(last_page["data"])
            if len(rows) > MAX_ROWS:
                raise RuntimeError("Trino output row budget exceeded")
            if not next_uri:
                break
            request = Request(next_uri, headers={"X-Trino-User": user})
        return {
            "columns": columns,
            "rows": rows,
            "query_id": last_page.get("id"),
            "query_seconds": clock() - started,
            "http_bytes": consumed,
        }
    except Exception:
        if next_uri:
            try:
                cancel_req = Request(checked_uri(next_uri), method="DELETE", headers={"X-Trino-User": user})
                with opener(cancel_req, timeout=3):
                    pass
            except Exception:
                pass
        raise


def fetch_table_snapshot(
    table: str,
    trino_url: str = DEFAULT_TRINO_URL,
    user: str = DEFAULT_TRINO_USER,
    opener=urlopen,
    clock=time.monotonic,
) -> dict:
    """Fetch the latest Iceberg snapshot_id from Trino metadata table <table>$snapshots."""
    if table not in TARGET_TABLES:
        raise ValueError(f"Unsupported Gold table: {table!r}")
    sql = (
        f'SELECT snapshot_id, committed_at, operation '
        f'FROM {GOLD_NAMESPACE}."{table}$snapshots" '
        f'ORDER BY committed_at DESC LIMIT 1'
    )
    result = trino_execute(sql, base_url=trino_url, user=user, opener=opener, clock=clock)
    if len(result["rows"]) != 1:
        raise RuntimeError(f"Missing Iceberg snapshot metadata for {GOLD_NAMESPACE}.{table}")
    snapshot_id, committed_at, operation = result["rows"][0]
    if type(snapshot_id) is not int or snapshot_id <= 0:
        raise RuntimeError(f"Invalid snapshot_id for {table}: {snapshot_id!r}")
    return {
        "snapshot_id": str(snapshot_id),
        "committed_at": str(committed_at),
        "operation": str(operation),
    }


def fetch_published_batch(container: str = DEFAULT_RUNNER_CONTAINER, runner=subprocess.run) -> dict:
    """Read the active published Gold batch manifest from MinIO via the runner container."""
    probe = (
        "import json, sys; sys.path.insert(0, '/workspace/src'); "
        "from gold.publication import Publication; "
        "store = Publication(); "
        "ptr = store.get('published.json'); "
        "manifest = store.get(ptr['batch'] + '.json') if ptr else None; "
        "print(json.dumps({'pointer': ptr, 'manifest': manifest}, ensure_ascii=False))"
    )
    proc = runner(
        ["docker", "exec", "-i", container, "python3", "-c", probe],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to read Gold publication metadata from {container}: {proc.stderr.strip()}")
    payload = json.loads(proc.stdout)
    pointer = payload.get("pointer") or {}
    manifest = payload.get("manifest") or {}
    if manifest.get("publication_status") != "PUBLISHED" or manifest.get("dq") != "ACCEPT":
        raise RuntimeError("Gold batch is not PUBLISHED/ACCEPT")
    batch = pointer.get("batch")
    if not batch or manifest.get("batch") != batch:
        raise RuntimeError("Gold publication pointer and manifest batch mismatch")
    return manifest


def reconcile_snapshots_with_batch(trino_snapshots: dict, manifest: dict) -> dict:
    """Verify live Trino snapshots match the published Gold manifest and return pinned UUIDs."""
    outputs = manifest.get("outputs") or {}
    pinned = {}
    for table, snap_info in trino_snapshots.items():
        manifest_entry = outputs.get(table)
        if not manifest_entry:
            raise RuntimeError(f"Table {table} missing from published Gold manifest")
        if str(manifest_entry.get("snapshot_id")) != snap_info["snapshot_id"]:
            raise RuntimeError(
                f"Snapshot mismatch for {table}: Trino={snap_info['snapshot_id']} "
                f"vs Gold batch={manifest_entry.get('snapshot_id')}"
            )
        pinned[table] = {
            **snap_info,
            "uuid": manifest_entry.get("uuid"),
            "rows": manifest_entry.get("rows"),
        }
    return pinned


def verify_series_dimension(
    dim_snapshot_id: str,
    trino_url: str = DEFAULT_TRINO_URL,
    user: str = DEFAULT_TRINO_USER,
    opener=urlopen,
    clock=time.monotonic,
) -> dict:
    """Verify all 8 target price_series_key identities and units in dim_price_series."""
    keys_sql = ", ".join(_sql_quote(k) for k in SERIES_BY_KEY)
    sql = (
        f"SELECT price_series_key, source_system_key, series_identity_json, unit, currency, price_kind "
        f"FROM {GOLD_NAMESPACE}.dim_price_series FOR VERSION AS OF {int(dim_snapshot_id)} "
        f"WHERE price_series_key IN ({keys_sql}) "
        f"ORDER BY price_series_key"
    )
    result = trino_execute(sql, base_url=trino_url, user=user, opener=opener, clock=clock)
    found = {}
    for series_key, source_system_key, identity_json, unit, currency, price_kind in result["rows"]:
        if series_key in found:
            raise RuntimeError(f"Duplicate dim_price_series key: {series_key}")
        col_name, spec = SERIES_BY_KEY[series_key]
        parsed_identity = json.loads(identity_json)
        if (
            source_system_key != spec["source"]
            or parsed_identity != spec["identity"]
            or unit != spec["unit"]
        ):
            raise RuntimeError(
                f"Series metadata drift for {series_key}: "
                f"got ({source_system_key}, {parsed_identity}, {unit})"
            )
        found[col_name] = {
            "price_series_key": series_key,
            "source_system_key": source_system_key,
            "series_identity_json": identity_json,
            "unit": unit,
            "currency": currency,
            "price_kind": price_kind,
            "role": spec["role"],
        }
    missing_cols = sorted(set(SERIES_SPECS) - set(found))
    if missing_cols:
        raise RuntimeError(f"Missing expected series in dim_price_series: {missing_cols}")
    return found


def extract_wb_series(
    fact_snapshot_id: str,
    start_month_key: int = START_MONTH_KEY,
    end_month_key: int = END_MONTH_KEY,
    trino_url: str = DEFAULT_TRINO_URL,
    user: str = DEFAULT_TRINO_USER,
    opener=urlopen,
    clock=time.monotonic,
) -> dict:
    """Query World Bank target and cost series from fact_price_monthly."""
    wb_cols = [col for col, spec in SERIES_SPECS.items() if spec["source"] == "worldbank_pinksheet"]
    keys_sql = ", ".join(_sql_quote(SERIES_SPECS[c]["price_series_key"]) for c in wb_cols)
    sql = (
        f"SELECT month_key, price_series_key, value, missing_reason "
        f"FROM {GOLD_NAMESPACE}.fact_price_monthly FOR VERSION AS OF {int(fact_snapshot_id)} "
        f"WHERE price_series_key IN ({keys_sql}) "
        f"AND month_key BETWEEN {int(start_month_key)} AND {int(end_month_key)} "
        f"ORDER BY price_series_key, month_key"
    )
    result = trino_execute(sql, base_url=trino_url, user=user, opener=opener, clock=clock)
    series_data = {col: {} for col in wb_cols}
    for month_key, series_key, raw_value, _missing_reason in result["rows"]:
        col_name, _ = SERIES_BY_KEY[series_key]
        _parse_month_key(month_key)
        if month_key in series_data[col_name]:
            raise RuntimeError(f"Duplicate WB observation at ({series_key}, {month_key})")
        series_data[col_name][month_key] = parse_decimal(raw_value)
    return series_data


def extract_usda_series(
    fact_snapshot_id: str,
    start_month_key: int = START_MONTH_KEY,
    end_month_key: int = END_MONTH_KEY,
    trino_url: str = DEFAULT_TRINO_URL,
    user: str = DEFAULT_TRINO_USER,
    opener=urlopen,
    clock=time.monotonic,
) -> dict:
    """Query USDA reference prices, exclude MARKETING rows, and map Aug-Jul years to YYYYMM."""
    usda_cols = [col for col, spec in SERIES_SPECS.items() if spec["source"] == "usda_rice_yearbook"]
    keys_sql = ", ".join(_sql_quote(SERIES_SPECS[c]["price_series_key"]) for c in usda_cols)
    start_ref_year = (start_month_key // 100) - 1
    end_ref_year = end_month_key // 100
    sql = (
        f"SELECT price_series_key, reference_year, reference_period, value, missing_reason "
        f"FROM {GOLD_NAMESPACE}.fact_usda_price_reference FOR VERSION AS OF {int(fact_snapshot_id)} "
        f"WHERE price_series_key IN ({keys_sql}) "
        f"AND reference_period <> 'MARKETING' "
        f"AND reference_year BETWEEN {start_ref_year} AND {end_ref_year} "
        f"ORDER BY price_series_key, reference_year, reference_period"
    )
    result = trino_execute(sql, base_url=trino_url, user=user, opener=opener, clock=clock)
    series_data = {col: {} for col in usda_cols}
    for series_key, ref_year, ref_period, raw_value, _missing_reason in result["rows"]:
        col_name, _ = SERIES_BY_KEY[series_key]
        month_key = convert_usda_to_month_key(ref_year, ref_period)
        if start_month_key <= month_key <= end_month_key:
            if month_key in series_data[col_name]:
                raise RuntimeError(f"Duplicate USDA calendar month at ({series_key}, {month_key})")
            series_data[col_name][month_key] = parse_decimal(raw_value)
    return series_data


def verify_extracted_alignment(spine: list[int], series_data: dict) -> dict:
    """Audit missing months and verify WB vs USDA VN 5% alignment on 201001..202512."""
    eval_months = [mk for mk in spine if mk >= EVAL_START_MONTH_KEY]
    audit = {}
    for col, values in series_data.items():
        missing_full = [mk for mk in spine if values.get(mk) is None]
        missing_eval = [mk for mk in eval_months if values.get(mk) is None]
        audit[col] = {
            "rows_present_in_source": sum(1 for mk in spine if mk in values),
            "non_null_2009_2025": len(spine) - len(missing_full),
            "missing_2009_2025_count": len(missing_full),
            "missing_2009_2025_months": missing_full,
            "non_null_2010_2025": len(eval_months) - len(missing_eval),
            "missing_2010_2025_count": len(missing_eval),
            "missing_2010_2025_months": missing_eval,
        }

    if not eval_months:
        return {
            "series_audit": audit,
            "wb_vs_usda_vn5_2010_2025": None,
        }

    pairs = [
        (float(series_data["wb_vn5_usd_t"][mk]), float(series_data["usda_vn5_usd_t"][mk]))
        for mk in eval_months
        if series_data["wb_vn5_usd_t"].get(mk) is not None
        and series_data["usda_vn5_usd_t"].get(mk) is not None
    ]
    if len(pairs) != len(eval_months):
        raise RuntimeError(
            f"Expected complete paired WB & USDA VN 5% series for {EVAL_START_MONTH_KEY}..{spine[-1]}, "
            f"got {len(pairs)}/{len(eval_months)} months"
        )
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    denom = math.sqrt(sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys))
    if denom <= 0:
        raise RuntimeError("Zero variance when computing WB vs USDA correlation")
    pearson_r = sum((x - mean_x) * (y - mean_y) for x, y in pairs) / denom
    mae_usd_t = sum(abs(x - y) for x, y in pairs) / len(pairs)
    if pearson_r < MIN_WB_USDA_CORRELATION:
        raise RuntimeError(
            f"WB vs USDA VN 5% correlation too low ({pearson_r:.6f} < {MIN_WB_USDA_CORRELATION}); "
            "verify August-July marketing-year calendar conversion"
        )
    return {
        "series_audit": audit,
        "wb_vs_usda_vn5_2010_2025": {
            "n_months": len(pairs),
            "pearson_r": round(pearson_r, 6),
            "mae_usd_t": round(mae_usd_t, 4),
        },
    }


def write_outputs(
    output_dir: Path,
    spine: list[int],
    series_data: dict,
    provenance: dict,
) -> tuple[Path, Path]:
    """Write extracted monthly CSV and provenance JSON atomically-friendly to output_dir."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "gold_prices_monthly.csv"
    provenance_path = output_dir / "gold_extraction_provenance.json"

    series_cols = list(SERIES_SPECS)
    headers = ["month_key", "month_date", *series_cols, "gold_batch", "wb_snapshot_id", "usda_snapshot_id"]
    gold_batch = provenance.get("gold_batch") or ""
    wb_snap = provenance["snapshots"]["fact_price_monthly"]["snapshot_id"]
    usda_snap = provenance["snapshots"]["fact_usda_price_reference"]["snapshot_id"]

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(headers)
        for mk in spine:
            y, m = divmod(mk, 100)
            row = [str(mk), date(y, m, 1).isoformat()]
            for col in series_cols:
                val = series_data[col].get(mk)
                row.append("" if val is None else format(val, "f"))
            row.extend([gold_batch, wb_snap, usda_snap])
            writer.writerow(row)

    provenance["output_csv"] = {
        "filename": csv_path.name,
        "rows": len(spine),
        "columns": headers,
        "sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
    }
    provenance_path.write_text(json.dumps(provenance, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return csv_path, provenance_path


def extract_gold_prices(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    trino_url: str = DEFAULT_TRINO_URL,
    trino_user: str = DEFAULT_TRINO_USER,
    start_month_key: int = START_MONTH_KEY,
    end_month_key: int = END_MONTH_KEY,
    runner_container: str = DEFAULT_RUNNER_CONTAINER,
    verify_batch: bool = True,
    opener=urlopen,
    clock=time.monotonic,
    docker_runner=subprocess.run,
) -> dict:
    """Extract pinned World Bank and USDA price series from Gold via Trino HTTP."""
    spine = build_calendar_spine(start_month_key, end_month_key)
    trino_snapshots = {
        table: fetch_table_snapshot(table, trino_url=trino_url, user=trino_user, opener=opener, clock=clock)
        for table in TARGET_TABLES
    }

    gold_batch = None
    gold_model = None
    publication_status = None
    dq_status = None
    if verify_batch:
        manifest = fetch_published_batch(container=runner_container, runner=docker_runner)
        snapshots = reconcile_snapshots_with_batch(trino_snapshots, manifest)
        gold_batch = manifest["batch"]
        gold_model = manifest.get("model")
        publication_status = manifest.get("publication_status")
        dq_status = manifest.get("dq")
    else:
        snapshots = trino_snapshots

    series_metadata = verify_series_dimension(
        snapshots["dim_price_series"]["snapshot_id"],
        trino_url=trino_url,
        user=trino_user,
        opener=opener,
        clock=clock,
    )
    wb_data = extract_wb_series(
        snapshots["fact_price_monthly"]["snapshot_id"],
        start_month_key=start_month_key,
        end_month_key=end_month_key,
        trino_url=trino_url,
        user=trino_user,
        opener=opener,
        clock=clock,
    )
    usda_data = extract_usda_series(
        snapshots["fact_usda_price_reference"]["snapshot_id"],
        start_month_key=start_month_key,
        end_month_key=end_month_key,
        trino_url=trino_url,
        user=trino_user,
        opener=opener,
        clock=clock,
    )

    series_data = {**wb_data, **usda_data}
    checks = verify_extracted_alignment(spine, series_data)
    provenance = {
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        "trino_url": trino_url.rstrip("/"),
        "source_namespace": GOLD_NAMESPACE,
        "period_range": {"start_month_key": start_month_key, "end_month_key": end_month_key},
        "gold_batch": gold_batch,
        "gold_model": gold_model,
        "publication_status": publication_status,
        "dq": dq_status,
        "batch_verified": verify_batch,
        "snapshots": snapshots,
        "series": series_metadata,
        "checks": checks,
    }
    csv_path, provenance_path = write_outputs(output_dir, spine, series_data, provenance)
    return {
        "status": "GOLD_PRICES_EXTRACTED",
        "csv_path": str(csv_path),
        "provenance_path": str(provenance_path),
        "rows": len(spine),
        "gold_batch": gold_batch,
        "snapshots": {tbl: info["snapshot_id"] for tbl, info in snapshots.items()},
        "wb_vs_usda_vn5_2010_2025": checks["wb_vs_usda_vn5_2010_2025"],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trino-url", default=DEFAULT_TRINO_URL, help=f"Trino HTTP URL (default: {DEFAULT_TRINO_URL})")
    parser.add_argument("--trino-user", default=DEFAULT_TRINO_USER, help=f"X-Trino-User header (default: {DEFAULT_TRINO_USER})")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help=f"Output folder (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--start-month", type=int, default=START_MONTH_KEY, help=f"Start YYYYMM (default: {START_MONTH_KEY})")
    parser.add_argument("--end-month", type=int, default=END_MONTH_KEY, help=f"End YYYYMM (default: {END_MONTH_KEY})")
    parser.add_argument("--runner-container", default=DEFAULT_RUNNER_CONTAINER, help="Docker container for Gold batch lookup")
    parser.add_argument("--skip-batch-check", action="store_true", help="Skip Docker Gold batch cross-check; pin Trino $snapshots only")
    args = parser.parse_args(argv)

    summary = extract_gold_prices(
        output_dir=args.output_dir,
        trino_url=args.trino_url,
        trino_user=args.trino_user,
        start_month_key=args.start_month,
        end_month_key=args.end_month,
        runner_container=args.runner_container,
        verify_batch=not args.skip_batch_check,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
