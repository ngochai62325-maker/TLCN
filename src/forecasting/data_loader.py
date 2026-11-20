"""Bộ nạp dữ liệu chuỗi thời gian giá gạo hàng tháng (World Bank Pink Sheet / Trino Gold).

Thiết kế đơn giản bằng các hàm thuần (plain functions) để dễ đọc hiểu và giải thích khi bảo vệ đồ án.
"""
from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW_WB_PATH = ROOT / "data/raw/world_bank/CMO-Historical-Data-Monthly.xlsx"


def load_from_raw_excel(
    series_name: str = "Rice, Viet Namese 5%",
    start_date: str | None = "2009-04-01",
    excel_path: str | Path | None = None,
) -> tuple[pd.Series, dict]:
    """Đọc chuỗi giá gạo hàng tháng trực tiếp từ file Excel World Bank Pink Sheet.

    - Sheet 'Monthly Prices', bỏ 4 dòng tiêu đề đầu (`skiprows=4`) và dòng đơn vị (index 0).
    - Chuyển cột thời gian dạng '2003M12' sang `DatetimeIndex` chuẩn đầu tháng (`freq='MS'`).
    - Thay ký hiệu khuyết dữ liệu ('…', '..', 'NA') bằng `NaN` và cắt từ tháng đầu tiên có dữ liệu.
    - Mặc định lọc từ `2009-04-01` (201 tháng liên tiếp không có NaN) để tránh biến dạng
      giai đoạn khủng hoảng lương thực 2008 (6 tháng khuyết liên tiếp do lệnh cấm xuất khẩu).
    """
    path = Path(excel_path) if excel_path else RAW_WB_PATH
    df = pd.read_excel(path, sheet_name="Monthly Prices", skiprows=4)
    matching = [c for c in df.columns if str(c).strip() == series_name.strip()]
    if not matching:
        raise ValueError(f"Không tìm thấy chuỗi '{series_name}' trong {path.name}")
    col_name = matching[0]
    unit = str(df.iloc[0][col_name]).strip()

    # Bỏ dòng đơn vị ở vị trí 0, chỉ giữ các dòng có mã kỳ dạng YYYYMmm
    data = df.iloc[1:].copy()
    period_col = df.columns[0]
    data = data[data[period_col].astype(str).str.match(r"^\d{4}M\d{2}$")].copy()
    dates = pd.to_datetime(data[period_col].astype(str).str.replace("M", "-") + "-01")

    series = pd.Series(pd.to_numeric(data[col_name], errors="coerce").values, index=dates, name=series_name.strip(), dtype=float)
    series = series.sort_index()

    # Cắt từ tháng đầu tiên đến tháng cuối cùng có dữ liệu thực tế (ví dụ 2003-12 đến 2025-12)
    first_valid, last_valid = series.first_valid_index(), series.last_valid_index()
    series = series.loc[first_valid:last_valid]
    full_idx = pd.date_range(first_valid, last_valid, freq="MS")
    series = series.reindex(full_idx)
    series.index.name = "month_start"
    series.name = series_name.strip()

    raw_missing = [d.strftime("%Y-%m") for d in series.index[series.isna()]]
    if start_date:
        series = series.loc[pd.to_datetime(start_date):].copy()
        series.index = pd.date_range(series.index[0], series.index[-1], freq="MS")
        series.index.name = "month_start"

    active_missing = [d.strftime("%Y-%m") for d in series.index[series.isna()]]
    metadata = {
        "source": "raw_excel",
        "series_name": series.name,
        "unit": unit,
        "raw_start_month": first_valid.strftime("%Y-%m"),
        "raw_end_month": last_valid.strftime("%Y-%m"),
        "raw_total_months": int(len(full_idx)),
        "raw_missing_count": len(raw_missing),
        "raw_missing_months": raw_missing,
        "start_month": series.index[0].strftime("%Y-%m"),
        "end_month": series.index[-1].strftime("%Y-%m"),
        "total_months": int(len(series)),
        "missing_count": len(active_missing),
        "missing_months": active_missing,
    }
    return series, metadata


def load_monthly_rice_price(
    series_name: str = "Rice, Viet Namese 5%",
    source: str = "raw",
    start_date: str | None = "2009-04-01",
) -> tuple[pd.Series, dict]:
    """Hàm nạp dữ liệu thống nhất: hỗ trợ đọc từ file `raw` hoặc từ tầng Gold qua `trino`."""
    if source == "raw":
        return load_from_raw_excel(series_name=series_name, start_date=start_date)
    if source != "trino":
        raise ValueError("Tham số source chỉ nhận 'raw' hoặc 'trino'")

    from analytics.client import EDAClient
    client = EDAClient()
    cat = client.query("catalogue", kind="series")
    wb = cat[cat["source_system_key"].eq("worldbank_pinksheet") & cat["series_identity_json"].str.contains(series_name.strip(), regex=False)]
    if len(wb) != 1:
        raise ValueError(f"Không tìm thấy duy nhất chuỗi '{series_name}' trên Trino Gold")
    series_key = wb.iloc[0]["price_series_key"]

    obs = client.query("price_observations", fact="fact_price_monthly", series=series_key, start_period=196001, end_period=203012)
    obs["month_key"] = obs["month_key"].astype(int)
    obs["date"] = pd.to_datetime(obs["month_key"].astype(str) + "01", format="%Y%m%d")
    obs["val"] = pd.to_numeric(obs["value"], errors="coerce")
    series = pd.Series(obs["val"].values, index=obs["date"], name=series_name.strip(), dtype=float).sort_index()

    first_valid, last_valid = series.first_valid_index(), series.last_valid_index()
    series = series.loc[first_valid:last_valid]
    full_idx = pd.date_range(first_valid, last_valid, freq="MS")
    series = series.reindex(full_idx)
    raw_missing = [d.strftime("%Y-%m") for d in series.index[series.isna()]]
    if start_date:
        series = series.loc[pd.to_datetime(start_date):].copy()
        series.index = pd.date_range(series.index[0], series.index[-1], freq="MS")
    series.index.name = "month_start"
    active_missing = [d.strftime("%Y-%m") for d in series.index[series.isna()]]
    return series, {
        "source": "trino_gold",
        "series_name": series.name,
        "series_key": series_key,
        "unit": "USD/mt",
        "raw_start_month": first_valid.strftime("%Y-%m"),
        "raw_end_month": last_valid.strftime("%Y-%m"),
        "raw_total_months": int(len(full_idx)),
        "raw_missing_count": len(raw_missing),
        "raw_missing_months": raw_missing,
        "start_month": series.index[0].strftime("%Y-%m"),
        "end_month": series.index[-1].strftime("%Y-%m"),
        "total_months": int(len(series)),
        "missing_count": len(active_missing),
        "missing_months": active_missing,
    }
