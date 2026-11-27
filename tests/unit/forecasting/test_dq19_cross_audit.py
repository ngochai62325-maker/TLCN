from pathlib import Path
import pandas as pd
import pytest

from forecasting.audit import audit_mart_dq19, audit_xgboost_logic
from jobs.forecasting import audit_dq19_cross_check as audit_cli


def test_audit_mart_dq19_rejects_india_white_flag_leak():
    raw_df = pd.read_csv(audit_cli.DEFAULT_RAW_CSV)
    mart_df = pd.read_csv(audit_cli.DEFAULT_MART_CSV)
    leaked = mart_df.copy()
    idx = leaked.index[leaked["month_key"] == 202308][0]
    leaked.loc[idx, "india_white_quote_missing_lag_1"] = 1
    with pytest.raises(AssertionError, match="india_white_quote_missing_lag_1"):
        audit_mart_dq19(raw_df, leaked)


def test_audit_xgboost_rejects_corrupted_target_delta():
    mart_df = pd.read_csv(audit_cli.DEFAULT_MART_CSV)
    bad_mart = mart_df.copy()
    bad_mart.loc[0, "target_delta"] = bad_mart.loc[0, "target_delta"] + 5.0
    with pytest.raises(AssertionError, match="target_delta mismatch"):
        audit_xgboost_logic(
            bad_mart,
            audit_cli.DEFAULT_EXPORT_DIR / "models" / "xgb_tuning_summary.json",
            audit_cli.DEFAULT_EXPORT_DIR / "models" / "xgb_predictions.csv",
        )


def test_full_cross_audit_cli_passes_on_repo_exports(tmp_path: Path):
    report_path = tmp_path / "audit_report.json"
    code = audit_cli.main(["--report-path", str(report_path)])
    assert code == 0
    assert report_path.is_file()