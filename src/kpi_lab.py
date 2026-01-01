import datetime as dt

import numpy as np
import pandas as pd

from src.business_logic import _net_to_positive


def _coerce_bool(s: pd.Series) -> pd.Series:
    try:
        return s.fillna(False).astype(bool)
    except Exception:
        return pd.Series([False] * len(s), index=s.index)


def _addbacks_by_month(df: pd.DataFrame) -> pd.DataFrame:
    if "sde_addback_flag" not in df.columns:
        return pd.DataFrame({"month": pd.PeriodIndex([], freq="M"), "addbacks": []})

    flag = _coerce_bool(df["sde_addback_flag"])
    subset = df.loc[flag].copy()
    if subset.empty:
        return pd.DataFrame({"month": pd.PeriodIndex([], freq="M"), "addbacks": []})

    # Spec: sum magnitudes (robust to sign convention)
    out_rows: list[dict] = []
    for m, g in subset.groupby("month"):
        addbacks = float(pd.to_numeric(g["amount"], errors="coerce").fillna(0.0).abs().sum())
        out_rows.append({"month": m, "addbacks": addbacks})

    return pd.DataFrame(out_rows)


def compute_monthly_kpis(
    df: pd.DataFrame,
    owner_revenue_start: dt.date,
) -> pd.DataFrame:
    """Compute monthly P&L + margins + MoM deltas.

    Uses the same month + classification aggregation pattern as KPI Explorer.
    Revenue is included only for dates >= owner_revenue_start.

    Expects columns (best-effort):
    - date (datetime64)
    - classification ("Revenue"/"COGS"/"Overhead"/"Other" or legacy "Other Expense")
    - amount (signed)
    - is_revenue (bool)
    - sde_addback_flag (bool)

    Returns a dataframe sorted by month with:
    - revenue, cogs, overhead, other_expense, net_profit, addbacks, sde
    - gross_profit
    - margin percent columns (as decimals)
    - MoM deltas for revenue/net_profit/sde
    """

    if df is None or len(df) == 0:
        return pd.DataFrame(
            columns=[
                "month",
                "month_str",
                "revenue",
                "cogs",
                "overhead",
                "other_expense",
                "gross_profit",
                "net_profit",
                "addbacks",
                "sde",
                "gross_margin_pct",
                "net_margin_pct",
                "sde_margin_pct",
                "overhead_pct",
                "cogs_pct",
                "revenue_mom_delta",
                "revenue_mom_pct",
                "net_profit_mom_delta",
                "net_profit_mom_pct",
                "sde_mom_delta",
                "sde_mom_pct",
            ]
        )

    d = df.copy()
    if "date" not in d.columns:
        raise ValueError("compute_monthly_kpis requires a 'date' column")
    if "amount" not in d.columns:
        raise ValueError("compute_monthly_kpis requires an 'amount' column")

    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d = d.dropna(subset=["date"]).copy()
    if d.empty:
        return pd.DataFrame()

    d["month"] = d["date"].dt.to_period("M")

    if "classification" not in d.columns:
        d["classification"] = "Other"

    is_revenue = _coerce_bool(d.get("is_revenue", pd.Series([False] * len(d), index=d.index)))
    owner_start = pd.to_datetime(owner_revenue_start)

    # Apply revenue boundary: pre-owner revenue rows contribute 0 to Revenue aggregation.
    d["amount_kpi"] = d["amount"]
    pre_owner_rev = is_revenue & (d["date"] < owner_start)
    d.loc[pre_owner_rev, "amount_kpi"] = 0.0

    by_class = (
        d.groupby(["month", "classification"], dropna=False)
        .agg(amount=("amount_kpi", "sum"))
        .reset_index()
    )

    pivot = by_class.pivot_table(
        index="month",
        columns="classification",
        values="amount",
        aggfunc="sum",
        fill_value=0.0,
    ).reset_index()

    rename_map = {
        "Revenue": "revenue_raw",
        "COGS": "cogs_raw",
        "Overhead": "overhead_raw",
        "Other Expense": "other_expense_raw",
        "Other": "other_expense_raw",
    }
    pivot.rename(columns=rename_map, inplace=True)

    for col in ["revenue_raw", "cogs_raw", "overhead_raw", "other_expense_raw"]:
        if col not in pivot.columns:
            pivot[col] = 0.0

    # Normalize signs into positive magnitudes for display
    pivot["revenue"] = pivot["revenue_raw"].map(_net_to_positive)
    pivot["cogs"] = pivot["cogs_raw"].map(_net_to_positive)
    pivot["overhead"] = pivot["overhead_raw"].map(_net_to_positive)
    pivot["other_expense"] = pivot["other_expense_raw"].map(_net_to_positive)

    pivot["gross_profit"] = pivot["revenue"] - pivot["cogs"]
    pivot["net_profit"] = pivot["gross_profit"] - pivot["overhead"] - pivot["other_expense"]

    addbacks = _addbacks_by_month(d[["month", "amount", "sde_addback_flag"]].copy())
    pivot = pivot.merge(addbacks, on="month", how="left")
    pivot["addbacks"] = pivot["addbacks"].fillna(0.0)

    pivot["sde"] = pivot["net_profit"] + pivot["addbacks"]

    pivot = pivot.sort_values("month").reset_index(drop=True)
    pivot["month_str"] = pivot["month"].dt.to_timestamp().dt.strftime("%b %Y")

    # Margins (as decimals), avoid divide by zero (return None)
    rev = pivot["revenue"].astype(float)

    def _safe_divide(numer: pd.Series, denom: pd.Series) -> pd.Series:
        out: list[float | None] = []
        for n, d0 in zip(numer.tolist(), denom.tolist()):
            try:
                d_f = float(d0)
            except Exception:
                d_f = 0.0
            if d_f == 0.0:
                out.append(None)
                continue
            try:
                out.append(float(n) / d_f)
            except Exception:
                out.append(None)
        return pd.Series(out, index=numer.index)

    pivot["gross_margin_pct"] = _safe_divide(pivot["gross_profit"], rev)
    pivot["net_margin_pct"] = _safe_divide(pivot["net_profit"], rev)
    pivot["sde_margin_pct"] = _safe_divide(pivot["sde"], rev)
    pivot["overhead_pct"] = _safe_divide(pivot["overhead"], rev)
    pivot["cogs_pct"] = _safe_divide(pivot["cogs"], rev)

    # MoM deltas
    for metric in ["revenue", "net_profit", "sde"]:
        prev = pivot[metric].shift(1)
        delta = pivot[metric] - prev
        pct_vals: list[float | None] = []
        for dlt, p in zip(delta.tolist(), prev.tolist()):
            try:
                p_f = float(p)
            except Exception:
                p_f = 0.0
            if p_f == 0.0:
                pct_vals.append(None)
                continue
            pct_vals.append(float(dlt) / p_f)
        pct = pd.Series(pct_vals, index=pivot.index)
        pivot[f"{metric}_mom_delta"] = delta
        pivot[f"{metric}_mom_pct"] = pct

    return pivot
