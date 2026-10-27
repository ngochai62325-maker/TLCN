"""Small explicit EDA methods; calculations remain visible in notebooks."""
import json
import math


def source_labels(text):
    items = json.loads(text)
    return " | ".join(dict.fromkeys(str(x["label"]) for x in items if x.get("label") is not None))


def calendar_yoy(frame, group, year="year_key", value="value"):
    """Exact year Y-1 join, not previous available row. Zero denominator stays undefined."""
    import pandas as pd
    keys = list(group) + [year]
    if frame.duplicated(keys).any():
        raise ValueError("Ambiguous publication/grain; do not resolve with MAX/FIRST")
    out = frame.copy()
    out[value] = pd.to_numeric(out[value], errors="raise")
    previous = out[keys+[value]].rename(columns={value: "previous_value"})
    previous[year] = previous[year] + 1
    out = out.merge(previous, on=keys, how="left", validate="one_to_one")
    out["yoy_pct"] = (out[value] / out["previous_value"].where(out["previous_value"] > 0)-1)*100
    return out


def endpoint_change(frame, year="year_key", value="value"):
    import pandas as pd
    valid = frame.dropna(subset=[value]).sort_values(year)
    if len(valid)<2:
        raise ValueError("Two numeric endpoints required")
    a,b=valid.iloc[0],valid.iloc[-1]
    lo,hi=float(a[value]),float(b[value])
    years=int(b[year])-int(a[year])
    return dict(first_year=int(a[year]),last_year=int(b[year]),first_value=lo,last_value=hi,
                change_pct=(hi/lo-1)*100 if lo>0 else None,
                cagr_pct=((hi/lo)**(1/years)-1)*100 if lo>0 and hi>0 and years>0 else None,
                numeric_periods=len(valid), calendar_span=years+1)


def monthly_calendar(frame, period="period", value="value"):
    """Reindex only for analysis; preserve absent-row vs present-NULL indicators."""
    import pandas as pd
    if frame.duplicated(period).any():
        raise ValueError("Select exactly one series/reference grain")
    df=frame.copy()
    df.index=pd.to_datetime(df[period].astype(str),format="%Y%m")
    df[value]=pd.to_numeric(df[value],errors="raise")
    df["observed_row"]=True
    df=df.reindex(pd.date_range(df.index.min(),df.index.max(),freq="MS"))
    df["observed_row"]=df["observed_row"].eq(True)
    df["change_pct"]=(df[value]/df[value].shift(1).where(df[value].shift(1)>0)-1)*100
    df["rolling_mean_12m"]=df[value].rolling(12,min_periods=12).mean()
    returns=df[value].where(df[value]>0).apply(lambda v: math.log(v) if pd.notna(v) else float("nan")).diff()
    df["volatility_12m"]=returns.rolling(12,min_periods=12).std(ddof=1)*100
    return df
