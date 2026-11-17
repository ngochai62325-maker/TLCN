import csv
import importlib
import json
import subprocess
import sys

import pytest

from jobs.forecasting import build_forecasting_mart as builder
from jobs.forecasting import verify_dq19 as mod
from tests.unit.forecasting.test_build_forecasting_mart import _write_synthetic_extracted


def test_import_and_cli_help():
    reloaded = importlib.import_module("jobs.forecasting.verify_dq19")
    assert callable(reloaded.main)
    for cmd in (
        [sys.executable, "-m", "jobs.forecasting.verify_dq19", "--help"],
        [sys.executable, "jobs/forecasting/verify_dq19.py", "--help"],
    ):
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        assert proc.returncode == 0
        assert "usage:" in proc.stdout


def test_run_dq19_audit_passes_on_clean_disk_mart(tmp_path):
    raw_csv, raw_prov = _write_synthetic_extracted(tmp_path)
    out_dir = tmp_path / "out"
    builder.build_forecasting_mart(input_csv=raw_csv, input_provenance=raw_prov, output_dir=out_dir)

    report_path = out_dir / "dq19_mart_report.json"
    report = mod.run_dq19_audit(
        mart_csv=out_dir / "forecasting_mart.csv",
        raw_csv=raw_csv,
        mart_provenance=out_dir / "forecasting_mart_provenance.json",
        raw_provenance=raw_prov,
        report_path=report_path,
    )
    assert report["dq19_status"] == "PASS"
    assert report["leakage_errors"] == 0
    assert report["rows_checked"] == 192
    assert len(report["feature_columns_checked"]) == 22
    assert report_path.is_file()
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["dq19_status"] == "PASS"


def test_run_dq19_audit_fails_on_future_leakage_or_header_drift(tmp_path):
    raw_csv, raw_prov = _write_synthetic_extracted(tmp_path)
    out_dir = tmp_path / "out"
    builder.build_forecasting_mart(input_csv=raw_csv, input_provenance=raw_prov, output_dir=out_dir)
    mart_csv = out_dir / "forecasting_mart.csv"

    # Tamper thai5_lag_1 on row 201001 to leak contemporaneous month 201001 value
    with mart_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows[0]["thai5_lag_1"] = "448.00"
    with mart_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=builder.MART_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        mod.run_dq19_audit(
            mart_csv=mart_csv,
            raw_csv=raw_csv,
            mart_provenance=out_dir / "forecasting_mart_provenance.json",
            raw_provenance=raw_prov,
            report_path=None,
        )

    # Even with provenance disabled, causal recomputation catches future leakage
    with pytest.raises(RuntimeError, match="DQ19 failed at 201001"):
        mod.run_dq19_audit(
            mart_csv=mart_csv,
            raw_csv=raw_csv,
            mart_provenance=None,
            raw_provenance=None,
            report_path=None,
        )
