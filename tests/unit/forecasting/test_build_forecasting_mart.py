import csv
from decimal import Decimal
import importlib
import json
import statistics
import subprocess
import sys
from pathlib import Path

import pytest

from jobs.forecasting import build_forecasting_mart as mod


def _write_synthetic_extracted(tmp_path: Path):
    csv_path = tmp_path / "gold_prices_monthly.csv"
    prov_path = tmp_path / "gold_extraction_provenance.json"
    months = mod._calendar_range(200901, 202512)
    headers = [
        "month_key",
        "month_date",
        "wb_vn5_usd_t",
        "wb_urea_usd_t",
        "wb_crude_oil_usd_bbl",
        "usda_vn5_usd_t",
        "usda_thai5_usd_t",
        "usda_india_broken5_usd_t",
        "usda_india_parb5_usd_t",
        "usda_pak5_usd_t",
        "gold_batch",
        "wb_snapshot_id",
        "usda_snapshot_id",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(headers)
        for idx, mk in enumerate(months):
            y, m = divmod(mk, 100)
            vn5 = "" if mk == 200903 else f"{400 + idx * 1.5:.4f}"
            urea = f"{250 + idx * 0.5:.4f}"
            oil = f"{50 + idx * 0.25:.4f}"
            usda_vn5 = f"{405 + idx * 1.5:.2f}"
            thai5 = f"{430 + idx * 1.5:.2f}"
            ind_brk = "" if (202308 <= mk <= 202408 or mk <= 201010) else f"{390 + idx * 1.5:.2f}"
            ind_parb = "" if (mk <= 201106 or mk == 202208) else f"{395 + idx * 1.5:.2f}"
            pak5 = "" if mk == 201009 else f"{410 + idx * 1.5:.2f}"
            writer.writerow(
                [
                    str(mk),
                    f"{y:04d}-{m:02d}-01",
                    vn5,
                    urea,
                    oil,
                    usda_vn5,
                    thai5,
                    ind_brk,
                    ind_parb,
                    pak5,
                    "b" * 64,
                    "101",
                    "202",
                ]
            )
    import hashlib

    prov = {
        "gold_batch": "b" * 64,
        "snapshots": {
            "dim_price_series": {"snapshot_id": "100"},
            "fact_price_monthly": {"snapshot_id": "101"},
            "fact_usda_price_reference": {"snapshot_id": "202"},
        },
        "output_csv": {"sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest()},
    }
    prov_path.write_text(json.dumps(prov), encoding="utf-8")
    return csv_path, prov_path


def test_import_and_cli_help():
    reloaded = importlib.import_module("jobs.forecasting.build_forecasting_mart")
    assert callable(reloaded.main)
    proc = subprocess.run(
        [sys.executable, "-m", "jobs.forecasting.build_forecasting_mart", "--help"],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert proc.returncode == 0
    assert "usage:" in proc.stdout


def test_build_forecasting_mart_d1_to_d5_and_dq19(tmp_path):
    csv_path, prov_path = _write_synthetic_extracted(tmp_path)
    out_dir = tmp_path / "out"
    summary = mod.build_forecasting_mart(
        input_csv=csv_path,
        input_provenance=prov_path,
        output_dir=out_dir,
    )
    assert summary["status"] == "FORECASTING_MART_BUILT"
    assert summary["dq19_status"] == "PASS"
    assert summary["leakage_errors"] == 0
    assert summary["total_rows"] == 192
    assert summary["train_rows"] == 156
    assert summary["test_rows"] == 36
    assert summary["test_india_parb_non_null_rows"] == 36
    assert summary["test_india_white_quote_missing_rows"] == 13

    with (out_dir / "forecasting_mart.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 192
    assert rows[0]["month_key"] == "201001" and rows[0]["split"] == "train"
    assert rows[155]["month_key"] == "202212" and rows[155]["split"] == "train"
    assert rows[156]["month_key"] == "202301" and rows[156]["split"] == "test"
    assert rows[-1]["month_key"] == "202512" and rows[-1]["split"] == "test"

    # Verify D2: 200903 linearly interpolated from 200902 (401.5) and 200904 (404.5) -> 403.0
    # At 201003 (row index 2), y_lag_12 corresponds to 200903!
    row_201003 = rows[2]
    assert row_201003["month_key"] == "201003"
    assert Decimal(row_201003["y_lag_12"]) == Decimal("403.0000")

    # Verify D1 & rolling at 201001 (uses 200901..200912 only)
    row_201001 = rows[0]
    assert Decimal(row_201001["target_delta"]) == Decimal(row_201001["y"]) - Decimal(row_201001["y_lag_1"])
    expected_2009_vals = [400.0 + i * 1.5 for i in range(12)]
    assert abs(float(row_201001["y_roll_mean_12"]) - statistics.fmean(expected_2009_vals)) < 1e-6
    assert abs(float(row_201001["y_roll_std_12"]) - statistics.stdev(expected_2009_vals)) < 1e-6

    # Verify D5: 201010 has pak5_lag_1 == "" (NaN from t=201009) and pak5_missing_lag_1 == "1"
    row_201010 = next(r for r in rows if r["month_key"] == "201010")
    assert row_201010["pak5_lag_1"] == "" and row_201010["pak5_missing_lag_1"] == "1"
    # 202209 has india_parb_lag_1 == "" and spread_vn_india_lag_1 == "" (from t=202208)
    row_202209 = next(r for r in rows if r["month_key"] == "202209")
    assert row_202209["india_parb_lag_1"] == "" and row_202209["spread_vn_india_lag_1"] == ""
    assert row_202209["india_parb_missing_lag_1"] == "1"

    # 202309 has india_white_quote_missing_lag_1 == "1" (ban started t=202308)
    row_202309 = next(r for r in rows if r["month_key"] == "202309")
    assert row_202309["india_white_quote_missing_lag_1"] == "1"


def test_dq19_detects_leakage_or_imputation_and_checksum_drift(tmp_path):
    csv_path, prov_path = _write_synthetic_extracted(tmp_path)
    rows_by_month, _ = mod.load_extracted_prices(csv_path, prov_path)
    warmup_y, _ = mod.build_warmup_target_series(rows_by_month)
    mart_rows = mod.build_mart_rows(rows_by_month, warmup_y)

    # Inject future leakage at row 201001: set thai5_lag_1 to month 201001 instead of 200912
    leaked = [dict(r) for r in mart_rows]
    leaked[0]["thai5_lag_1"] = rows_by_month[201001]["usda_thai5_usd_t"]
    with pytest.raises(RuntimeError, match="DQ19 failed at 201001"):
        mod.verify_no_leakage_dq19(rows_by_month, warmup_y, leaked)

    # Inject silent imputation where D5 requires NaN
    imputed = [dict(r) for r in mart_rows]
    row_201010 = next(r for r in imputed if r["month_key"] == 201010)
    row_201010["pak5_lag_1"] = Decimal("400.00")
    with pytest.raises(RuntimeError, match="DQ19 failed at 201010"):
        mod.verify_no_leakage_dq19(rows_by_month, warmup_y, imputed)

    # Tamper CSV checksum relative to provenance JSON
    csv_path.write_text(csv_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        mod.build_forecasting_mart(input_csv=csv_path, input_provenance=prov_path, output_dir=tmp_path / "out")
