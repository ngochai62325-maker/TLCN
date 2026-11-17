from decimal import Decimal
import hashlib
import importlib
import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from jobs.forecasting import extract_gold_prices as mod


def _trino_pages(*items):
    calls = []

    def open_page(request, timeout):
        calls.append(request)
        if request.get_method() == "DELETE":
            return io.BytesIO(b"")
        index = len([c for c in calls if c.get_method() != "DELETE"]) - 1
        return io.BytesIO(json.dumps(items[index]).encode("utf-8"))

    return open_page, calls


def test_import_has_no_side_effects_and_cli_help():
    reloaded = importlib.import_module("jobs.forecasting.extract_gold_prices")
    assert callable(reloaded.main)
    proc = subprocess.run(
        [sys.executable, "-m", "jobs.forecasting.extract_gold_prices", "--help"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert proc.returncode == 0
    assert "usage:" in proc.stdout


def test_build_calendar_spine():
    spine = mod.build_calendar_spine(200901, 202512)
    assert len(spine) == 204
    assert spine[0] == 200901 and spine[11] == 200912 and spine[12] == 201001 and spine[-1] == 202512
    with pytest.raises(ValueError):
        mod.build_calendar_spine(202501, 202401)
    with pytest.raises(ValueError):
        mod.build_calendar_spine(202413, 202501)


def test_convert_usda_to_month_key_aug_jul_marketing_year():
    assert mod.convert_usda_to_month_key(2008, "August") == 200808
    assert mod.convert_usda_to_month_key(2008, "December") == 200812
    assert mod.convert_usda_to_month_key(2008, "January") == 200901
    assert mod.convert_usda_to_month_key(2008, "July") == 200907
    assert mod.convert_usda_to_month_key(2024, "January") == 202501
    for bad in ("MARKETING", "YEAR", "Aug", ""):
        with pytest.raises(ValueError):
            mod.convert_usda_to_month_key(2023, bad)
    with pytest.raises(ValueError):
        mod.convert_usda_to_month_key(True, "January")


def test_trino_execute_pagination_and_cancel_on_error():
    opener, calls = _trino_pages(
        {
            "id": "q1",
            "columns": [{"name": "a"}, {"name": "b"}],
            "data": [[200901, "450.1250"]],
            "nextUri": "http://localhost:18080/v1/statement/q1/1",
        },
        {"id": "q1", "data": None, "nextUri": "http://localhost:18080/v1/statement/q1/2"},
        {"id": "q1", "data": [[200902, "455.0000"]]},
    )
    res = mod.trino_execute("SELECT 1", base_url="http://localhost:18080", opener=opener)
    assert res["rows"] == [[200901, "450.1250"], [200902, "455.0000"]]
    assert [c.get_method() for c in calls] == ["POST", "GET", "GET"]

    bad_opener, bad_calls = _trino_pages(
        {"id": "q2", "nextUri": "http://evil:9999/v1/statement/q2/1"},
    )
    with pytest.raises(RuntimeError, match="Unexpected Trino polling destination"):
        mod.trino_execute("SELECT 1", base_url="http://localhost:18080", opener=bad_opener)
    assert len(bad_calls) == 1

    err_opener, err_calls = _trino_pages(
        {"id": "q3", "nextUri": "http://localhost:18080/v1/statement/q3/1"},
        {"id": "q3", "nextUri": "http://localhost:18080/v1/statement/q3/2", "error": {"errorName": "SYNTAX", "message": "bad"}},
    )
    with pytest.raises(RuntimeError, match="SYNTAX: bad"):
        mod.trino_execute("SELECT 1", base_url="http://localhost:18080", opener=err_opener)
    assert err_calls[-1].get_method() == "DELETE"


def _synthetic_gold_fixtures(tmp_path, snapshot_mismatch=False, break_usda_order=False):
    spine = mod.build_calendar_spine(200901, 202512)
    snapshots = {
        "dim_price_series": "111",
        "fact_price_monthly": "222",
        "fact_usda_price_reference": "333",
    }
    manifest_snaps = dict(snapshots)
    if snapshot_mismatch:
        manifest_snaps["fact_price_monthly"] = "999"

    batch = "a" * 64
    manifest = {
        "batch": batch,
        "model": "gold-20261025.1",
        "publication_status": "PUBLISHED",
        "dq": "ACCEPT",
        "outputs": {
            t: {"snapshot_id": sid, "uuid": f"uuid-{t}", "rows": 100}
            for t, sid in manifest_snaps.items()
        },
    }

    dim_rows = [
        [
            spec["price_series_key"],
            spec["source"],
            json.dumps(spec["identity"]),
            spec["unit"],
            "USD",
            "benchmark_price" if spec["source"] == "worldbank_pinksheet" else "reported_export_price",
        ]
        for spec in mod.SERIES_SPECS.values()
    ]

    wb_rows = []
    for idx, mk in enumerate(spine):
        vn_price = None if mk == 200903 else f"{400 + (idx % 36) * 5:.4f}"
        wb_rows.append([mk, mod.SERIES_SPECS["wb_vn5_usd_t"]["price_series_key"], vn_price, "source_marker" if vn_price is None else None])
        wb_rows.append([mk, mod.SERIES_SPECS["wb_urea_usd_t"]["price_series_key"], f"{300 + (idx % 12) * 3:.4f}", None])
        wb_rows.append([mk, mod.SERIES_SPECS["wb_crude_oil_usd_bbl"]["price_series_key"], f"{60 + (idx % 12) * 2:.4f}", None])

    month_names = {v: k for k, v in mod.MONTH_NUMBERS.items()}
    usda_rows = []
    for ry in range(2008, 2026):
        for m in range(1, 13):
            cy = ry if m >= 8 else ry + 1
            mk = cy * 100 + m
            if not (200901 <= mk <= 202512):
                continue
            idx = spine.index(mk)
            base_price = 400 + (((len(spine) - 1 - idx) if break_usda_order else idx) % 36) * 5 + 10
            rp = month_names[m]
            usda_rows.append([mod.SERIES_SPECS["usda_vn5_usd_t"]["price_series_key"], ry, rp, f"{base_price:.2f}", None])
            usda_rows.append([mod.SERIES_SPECS["usda_thai5_usd_t"]["price_series_key"], ry, rp, f"{base_price + 20:.2f}", None])
            ib_val = None if 202308 <= mk <= 202408 else f"{base_price - 15:.2f}"
            usda_rows.append([mod.SERIES_SPECS["usda_india_broken5_usd_t"]["price_series_key"], ry, rp, ib_val, "source_null" if ib_val is None else None])
            ip_val = None if (201001 <= mk <= 201106 or mk == 202208) else f"{base_price - 10:.2f}"
            usda_rows.append([mod.SERIES_SPECS["usda_india_parb5_usd_t"]["price_series_key"], ry, rp, ip_val, "source_null" if ip_val is None else None])
            pk_val = None if mk == 201009 else f"{base_price - 5:.2f}"
            usda_rows.append([mod.SERIES_SPECS["usda_pak5_usd_t"]["price_series_key"], ry, rp, pk_val, "source_null" if pk_val is None else None])

    seen_sqls = []

    def fake_opener(request, timeout):
        sql = request.data.decode("utf-8")
        seen_sqls.append(sql)
        if "$snapshots" in sql:
            for tbl, sid in snapshots.items():
                if f'"{tbl}$snapshots"' in sql:
                    return io.BytesIO(json.dumps({"id": tbl, "data": [[int(sid), "2026-11-17 10:00:00 UTC", "overwrite"]]}).encode("utf-8"))
        if "FROM iceberg.gold_local.dim_price_series FOR VERSION AS OF 111" in sql:
            return io.BytesIO(json.dumps({"id": "dim", "data": dim_rows}).encode("utf-8"))
        if "FROM iceberg.gold_local.fact_price_monthly FOR VERSION AS OF 222" in sql:
            return io.BytesIO(json.dumps({"id": "wb", "data": wb_rows}).encode("utf-8"))
        if "FROM iceberg.gold_local.fact_usda_price_reference FOR VERSION AS OF 333" in sql:
            return io.BytesIO(json.dumps({"id": "usda", "data": usda_rows}).encode("utf-8"))
        raise AssertionError(f"Unexpected SQL: {sql}")

    def fake_docker(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=json.dumps({"pointer": {"batch": batch}, "manifest": manifest}),
            stderr="",
        )

    return tmp_path, fake_opener, fake_docker, seen_sqls, batch


def test_extract_gold_prices_end_to_end_and_provenance(tmp_path):
    out_dir, fake_opener, fake_docker, seen_sqls, batch = _synthetic_gold_fixtures(tmp_path)
    summary = mod.extract_gold_prices(
        output_dir=out_dir,
        opener=fake_opener,
        docker_runner=fake_docker,
    )
    assert summary["status"] == "GOLD_PRICES_EXTRACTED"
    assert summary["rows"] == 204
    assert summary["gold_batch"] == batch
    assert summary["wb_vs_usda_vn5_2010_2025"]["pearson_r"] == 1.0

    csv_path = Path(summary["csv_path"])
    prov_path = Path(summary["provenance_path"])
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 205
    assert lines[0].endswith(",gold_batch,wb_snapshot_id,usda_snapshot_id")
    # 200903 is row index 3 (header + 200901 + 200902 + 200903)
    row_200903 = lines[3].split(",")
    assert row_200903[0] == "200903" and row_200903[2] == "" and row_200903[-3:] == [batch, "222", "333"]

    prov = json.loads(prov_path.read_text(encoding="utf-8"))
    assert prov["output_csv"]["sha256"] == hashlib.sha256(csv_path.read_bytes()).hexdigest()
    assert prov["checks"]["series_audit"]["wb_vn5_usd_t"]["missing_2009_2025_months"] == [200903]
    assert prov["checks"]["series_audit"]["usda_pak5_usd_t"]["missing_2010_2025_months"] == [201009]
    assert prov["checks"]["series_audit"]["usda_india_broken5_usd_t"]["missing_2010_2025_count"] == 13
    assert any("reference_period <> 'MARKETING'" in s for s in seen_sqls)


def test_extract_gold_prices_fails_closed_on_snapshot_or_correlation_drift(tmp_path):
    out_dir, fake_opener, fake_docker, _, _ = _synthetic_gold_fixtures(tmp_path, snapshot_mismatch=True)
    with pytest.raises(RuntimeError, match="Snapshot mismatch"):
        mod.extract_gold_prices(output_dir=out_dir, opener=fake_opener, docker_runner=fake_docker)

    out_dir2, fake_opener2, fake_docker2, _, _ = _synthetic_gold_fixtures(tmp_path / "bad_corr", break_usda_order=True)
    with pytest.raises(RuntimeError, match="correlation too low"):
        mod.extract_gold_prices(output_dir=out_dir2, opener=fake_opener2, docker_runner=fake_docker2)
