import json
import subprocess
import sys
import pandas as pd
import pytest

from jobs.forecasting import build_forecasting_mart as mart_mod
from jobs.forecasting import extract_gold_prices as ext_mod


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _make_fake_http():
    spine = ext_mod.build_spine()
    wb_rows, usda_rows = [], []
    months_inv = {v: k for k, v in ext_mod.MONTH_MAP.items()}
    for i, mk in enumerate(spine):
        wb_rows.append([mk, "worldbank_pinksheet_rice_viet_namese_5", None if mk == 200903 else str(400 + i)])
        wb_rows.append([mk, "worldbank_pinksheet_urea", str(250 + i * 0.5)])
        wb_rows.append([mk, "worldbank_pinksheet_crude_oil_average", str(50 + i * 0.25)])
        y, m = divmod(mk, 100)
        ry = y if m >= 8 else y - 1
        rp = months_inv[m]
        usda_rows.append(["usda_rice_yearbook_vietnam_5_percent_broken_freight_on_bo_dd22fa", ry, rp, str(410 + i)])
        usda_rows.append(["usda_rice_yearbook_thailand_5_percent_broken_freight_on_b_10e286", ry, rp, str(430 + i)])
        usda_rows.append(["usda_rice_yearbook_india_5_percent_broken_freight_on_boar_3c14de", ry, rp, None if 202308 <= mk <= 202408 else str(390 + i)])
        usda_rows.append(["usda_rice_yearbook_india_5_percent_parboiled_freight_on_b_51dde4", ry, rp, None if (mk <= 201106 or mk == 202208) else str(395 + i)])
        usda_rows.append(["usda_rice_yearbook_pakistan_5_percent_broken_freight_on_b_5d49f7", ry, rp, None if mk == 201009 else str(405 + i)])

    class FakeHTTP:
        def post(self, url, data, headers, timeout):
            sql = data.decode("utf-8")
            if "fact_price_monthly$snapshots" in sql:
                return _FakeResp({"columns": [{"name": "snapshot_id"}], "data": [[222]]})
            if "fact_usda_price_reference$snapshots" in sql:
                return _FakeResp({"columns": [{"name": "snapshot_id"}], "data": [[333]]})
            if "fact_price_monthly FOR VERSION AS OF 222" in sql:
                return _FakeResp({"columns": [{"name": "month_key"}, {"name": "price_series_key"}, {"name": "value"}], "data": wb_rows})
            if "fact_usda_price_reference FOR VERSION AS OF 333" in sql:
                return _FakeResp({"columns": [{"name": "price_series_key"}, {"name": "reference_year"}, {"name": "reference_period"}, {"name": "value"}], "data": usda_rows})
            raise AssertionError(sql)

        def get(self, url, headers, timeout):
            raise AssertionError(url)

    def fake_runner(*args, **kwargs):
        manifest = {
            "batch": "b" * 64,
            "publication_status": "PUBLISHED",
            "dq": "ACCEPT",
            "outputs": {"fact_price_monthly": {"snapshot_id": "222"}, "fact_usda_price_reference": {"snapshot_id": "333"}},
        }
        return subprocess.CompletedProcess(args[0], 0, stdout=json.dumps(manifest), stderr="")

    return FakeHTTP(), fake_runner


def test_cli_help():
    for path in ("jobs/forecasting/extract_gold_prices.py", "jobs/forecasting/build_forecasting_mart.py"):
        res = subprocess.run([sys.executable, path, "--help"], capture_output=True, text=True, timeout=15)
        assert res.returncode == 0 and "usage:" in res.stdout


def test_extract_build_and_dq19_end_to_end(tmp_path):
    http, runner = _make_fake_http()
    prov = ext_mod.extract_all(out_dir=tmp_path, http=http, runner=runner)
    assert prov["rows"] == 204 and prov["wb_vs_usda_vn5_2010_2025"]["pearson_r"] == 1.0

    raw = pd.read_csv(tmp_path / "gold_prices_monthly.csv")
    mart = mart_mod.build_mart(raw)
    report = mart_mod.check_dq19(raw, mart)
    assert report["dq19_status"] == "PASS" and report["rows"] == 192
    assert report["train_rows"] == 156 and report["test_rows"] == 36
    assert mart.loc[mart["month_key"] == 201003, "y_lag_12"].iloc[0] == 402.0
    assert pd.isna(mart.loc[mart["month_key"] == 201010, "pak5_lag_1"].iloc[0])
    assert mart.loc[mart["month_key"] == 201010, "pak5_missing_lag_1"].iloc[0] == 1

    leaked = mart.copy()
    leaked.loc[0, "thai5_lag_1"] = raw.loc[raw["month_key"] == 201001, "usda_thai5_usd_t"].iloc[0]
    with pytest.raises(AssertionError):
        mart_mod.check_dq19(raw, leaked)
