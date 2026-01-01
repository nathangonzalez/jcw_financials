import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np
import json
import sys
import os
from pathlib import Path

from src.utils import currency
from src.data_loader import load_ledger
from src.business_logic import (
    classify_transactions,
    detect_addbacks,
    get_owner_metrics,
    get_period_metrics,
    compute_legacy_overhead_addins,
    apply_legacy_overhead_addins,
)
from src.kpi_lab import compute_monthly_kpis
from src.forecasting import calculate_run_rates, forecast_year_1
from src.billing import load_green_sheets, JobBillingConfig, compute_period_billing
from src.account_view import build_account_summary
from src.addback_rules import default_rules, load_rules, rules_to_jsonable
from src.reconciliation import (
    normalize_bank_register,
    normalize_qb_ledger_for_bank,
    match_qb_and_bank,
    compute_cash_vs_accrual_summary
)


APP_SETTINGS_PATH = Path(__file__).resolve().parent / "config" / "app_settings.json"
ADDBACK_RULES_PATH = Path(__file__).resolve().parent / "data" / "addback_rules.json"


def _load_app_settings() -> dict:
    try:
        if APP_SETTINGS_PATH.exists():
            return json.loads(APP_SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {}


def _persist_app_settings(update: dict) -> None:
    try:
        APP_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        current = _load_app_settings()
        current.update(update)
        APP_SETTINGS_PATH.write_text(json.dumps(current, indent=2), encoding="utf-8")
    except Exception:
        # Optional persistence: never crash the app if file I/O fails.
        return


def make_arrow_safe(
    d: pd.DataFrame,
    debug_label: str | None = None,
    debug_mode: bool = False,
) -> pd.DataFrame:
    """Make a dataframe safe for Streamlit Arrow serialization.

    Streamlit uses Arrow internally for `st.dataframe` (and some chart APIs).
    Certain dtypes (Period, tz-aware datetimes, mixed object columns containing
    Timestamp/date/datetime, or even an object-typed *index* containing
    Timestamps) can trigger Arrow conversion warnings.

    This function coerces common problematic columns into Arrow-friendly types.
    """
    out = d.copy()

    converted_cols: list[str] = []

    # Index can also be serialized; make it safe as well.
    try:
        if getattr(out.index, "dtype", None) == "object":
            idx_sample = pd.Series(out.index).dropna().head(25).tolist()
            idx_has_dt = any(isinstance(v, (pd.Timestamp, dt.datetime, dt.date)) for v in idx_sample)
            idx_name = str(out.index.name or "").lower()
            idx_looks_like_date = "date" in idx_name

            if idx_has_dt or idx_looks_like_date:
                new_index = pd.to_datetime(out.index, errors="coerce")
                if pd.Series(new_index).notna().any():
                    out.index = new_index
    except Exception:
        # Never fail rendering due to index normalization
        pass

    for c in out.columns:
        s = out[c]

        # Datetime dtype -> convert to tz-naive then to ISO-like string.
        # Streamlit's Arrow+Styler path can be picky about datetime columns,
        # so representing datetimes as strings is the most robust display strategy.
        if pd.api.types.is_datetime64_any_dtype(s):
            dt_ser = pd.to_datetime(s, errors="coerce")
            try:
                dt_ser = dt_ser.dt.tz_localize(None)
            except Exception:
                pass

            # If values are all midnight, show date only; else show full timestamp.
            try:
                has_time = (dt_ser.dt.hour.ne(0) | dt_ser.dt.minute.ne(0) | dt_ser.dt.second.ne(0)).any()
            except Exception:
                has_time = True

            if has_time:
                out[c] = dt_ser.dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                out[c] = dt_ser.dt.strftime("%Y-%m-%d")

            converted_cols.append(str(c))
            continue

        # Period dtype -> string
        # (pandas is deprecating `is_period_dtype`, so use dtype instance check)
        if isinstance(s.dtype, pd.PeriodDtype):
            out[c] = s.astype(str)
            converted_cols.append(str(c))
            continue

        # Object columns that contain Timestamp/date/datetime OR look like date columns
        # (some transforms can produce mixed object columns where only some rows are Timestamps)
        if s.dtype == "object":
            col_name = str(c).lower()
            sample = s.dropna().head(25).tolist()

            looks_like_date_col = ("date" in col_name) or col_name.endswith("_dt")
            has_dt_objects = any(isinstance(v, (pd.Timestamp, dt.datetime, dt.date)) for v in sample)

            if looks_like_date_col or has_dt_objects:
                parsed = pd.to_datetime(s, errors="coerce")

                # If we successfully parsed at least one value, keep it as datetime64.
                # Otherwise, fall back to string to avoid Arrow choking on mixed objects.
                if parsed.notna().any():
                    out[c] = parsed
                    try:
                        out[c] = out[c].dt.tz_localize(None)
                    except Exception:
                        pass
                    converted_cols.append(str(c))
                else:
                    out[c] = s.astype(str)
                    converted_cols.append(str(c))

    if debug_mode and debug_label and converted_cols:
        # log to stderr so it shows up in streamlit_stderr.log
        print(f"[arrow_safe] {debug_label}: converted cols -> {converted_cols}", file=sys.stderr, flush=True)

    return out


def st_dataframe_stretch(*args, **kwargs):
    """Render a dataframe with Streamlit-version-safe full-width behavior."""
    try:
        return st.dataframe(*args, width="stretch", **kwargs)
    except TypeError:
        return st.dataframe(*args, use_container_width=True, **kwargs)


def st_chart_stretch(chart, **kwargs):
    """Render charts with Streamlit-version-safe full-width behavior."""
    try:
        return st.altair_chart(chart, width="stretch", **kwargs)
    except TypeError:
        return st.altair_chart(chart, use_container_width=True, **kwargs)


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="Owner SDE Dashboard", layout="wide")
st.title("💰 Owner SDE & Profit Dashboard")
st.caption("Focus: Owner Net Profit & Seller's Discretionary Earnings (Year 1)")

# ---------------------------------------------------------
# SETTINGS (persisted)
# ---------------------------------------------------------
_settings = _load_app_settings()

# Defaults (used for reproducibility + UAT payload)
if "cogs_prefixes_str" not in st.session_state:
    st.session_state["cogs_prefixes_str"] = str(
        _settings.get("cogs_prefixes_str", "704,705,706,707,708")
    )

# ---------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------
st.sidebar.header("Configuration")

# Debug Mode
try:
    # Streamlit query params can be list-like depending on version
    qp_debug = st.query_params.get("debug")
    if isinstance(qp_debug, list):
        qp_debug = qp_debug[0] if qp_debug else ""
    query_debug = str(qp_debug).strip().lower() in {"1", "true", "yes"}
except Exception:
    query_debug = False

# Sidebar toggle default ON if ?debug=1
sidebar_debug = st.sidebar.toggle("Debug mode", value=query_debug)
# If URL contains debug=1, always enable debug to preserve test harness.
debug_mode = bool(query_debug or sidebar_debug)

# Dates
report_start = dt.date(2025, 8, 1)
st.sidebar.caption(f"Core P&L start: {report_start.isoformat()}")
year_1_end = st.sidebar.date_input("Year 1 End", value=dt.date(2026, 6, 30))
today = st.sidebar.date_input("Current Report Date", value=dt.date.today())

st.sidebar.markdown("---")

# Addbacks are configured in the Addbacks tab (rules editor).
custom_addback_tokens: list[str] = []

st.sidebar.markdown("---")

# Classification Tuning
with st.sidebar.expander("Classification Tuning"):
    def _on_cogs_prefixes_change():
        _persist_app_settings({"cogs_prefixes_str": st.session_state.get("cogs_prefixes_str", "")})

    st.text_input(
        "COGS account prefixes (comma separated)",
        key="cogs_prefixes_str",
        on_change=_on_cogs_prefixes_change,
    )
    cogs_prefixes = {x.strip() for x in st.session_state["cogs_prefixes_str"].split(",") if x.strip()}

ledger_file = st.sidebar.file_uploader("Upload QuickBooks Ledger Export", type=["csv", "xlsx", "xls"])

# ---------------------------------------------------------
# DATA PROCESSING
# ---------------------------------------------------------
if ledger_file:
    with st.spinner("Processing Ledger..."):
        try:
            df = load_ledger(ledger_file)

            if debug_mode:
                with st.expander("Debug: Ledger load", expanded=False):
                    st.write("Ledger loaded?", df is not None)
                    if df is not None:
                        st.write(f"Rows: {len(df)}")

            # Force date type again to prevent PyArrow errors
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")

            if debug_mode:
                with st.expander("Debug: Ledger columns / head", expanded=False):
                    st.write("Columns:", df.columns.tolist())
                    st_dataframe_stretch(
                        make_arrow_safe(df.head(), debug_label="LEDGER_HEAD", debug_mode=debug_mode)
                    )

                    # 🔍 Sanity checks
                    if "amount" in df.columns:
                        st.write("amount sample:", df["amount"].head())
                        st.write("total amount:", float(df["amount"].sum()))

                    if "account_type" in df.columns:
                        st.write("account_type counts:", df["account_type"].value_counts())
                        income_mask = df["account_type"].str.contains("income", case=False, na=False)
                        st.write(
                            "raw income sum (amount):",
                            float(df.loc[income_mask, "amount"].sum())
                        )
            
            if df is not None:
                # 1. Classify
                df = classify_transactions(df, cogs_prefixes=cogs_prefixes)
                
                # 2. Addbacks
                file_rules: list[dict] = []
                try:
                    if ADDBACK_RULES_PATH.exists():
                        file_rules = json.loads(ADDBACK_RULES_PATH.read_text(encoding="utf-8"))
                        if not isinstance(file_rules, list):
                            file_rules = []
                except Exception:
                    file_rules = []

                rules = rules_to_jsonable(default_rules()) + file_rules
                df = detect_addbacks(df, custom_tokens=custom_addback_tokens, rules=rules)

                # 3. Compute report-window metrics (QB P&L style): report_start -> today
                report_mask = (df["date"].dt.date >= report_start) & (df["date"].dt.date <= today)
                is_pnl = df.get("is_pnl", pd.Series([True] * len(df), index=df.index)).astype(bool)
                report_df = df.loc[report_mask & is_pnl].copy()
                qb_pnl_metrics = get_period_metrics(report_df, report_start, today)

                # 4. Optional legacy overhead add-ins (prior calendar month)
                first_of_report_month = report_start.replace(day=1)
                legacy_end = first_of_report_month - dt.timedelta(days=1)
                legacy_start = legacy_end.replace(day=1)

                legacy_overhead_included_total = 0.0
                selected_legacy_overhead_rows_count = 0
                with st.sidebar.expander("July overhead add-in (optional)"):
                    include_legacy_overhead = st.checkbox(
                        "Include selected July overhead transactions",
                        value=False,
                        help=(
                            "Default OFF. When enabled, you can explicitly select July overhead transactions "
                            "to include as an add-in adjustment to match an owner-cost view. July revenue is never included."
                        ),
                    )

                    if include_legacy_overhead:
                        is_pnl = df.get("is_pnl", pd.Series([True] * len(df), index=df.index)).astype(bool)
                        legacy_overhead_mask = (
                            (df["date"].dt.date >= legacy_start)
                            & (df["date"].dt.date <= legacy_end)
                            & (df.get("is_overhead", False).astype(bool))
                            & is_pnl
                        )
                        legacy_df = df.loc[legacy_overhead_mask, ["_row_id", "date", "account", "name", "memo", "amount"]].copy()
                        legacy_df = legacy_df.sort_values("date")

                        if legacy_df.empty:
                            st.caption("No July overhead transactions found.")
                        else:
                            editor = legacy_df.copy()
                            editor.insert(0, "include", False)
                            edited = st.data_editor(
                                editor,
                                hide_index=True,
                                disabled=["_row_id", "date", "account", "name", "memo", "amount"],
                                column_config={
                                    "include": st.column_config.CheckboxColumn("Include"),
                                    "amount": st.column_config.NumberColumn("Amount", format="$%.2f"),
                                },
                            )

                            selected_ids = set(
                                edited.loc[edited["include"].astype(bool), "_row_id"].astype(str).tolist()
                            )
                            selected_legacy_overhead_rows_count = len(selected_ids)

                            if selected_ids:
                                legacy_overhead_included_total = compute_legacy_overhead_addins(
                                    df,
                                    legacy_start=legacy_start,
                                    legacy_end=legacy_end,
                                    included_row_ids=selected_ids,
                                )

                active_metrics = apply_legacy_overhead_addins(
                    qb_pnl_metrics,
                    legacy_overhead_included_total=legacy_overhead_included_total,
                )
                
                # 5. Calculate Run Rates (based on report window actuals)
                days_run_rate = (today - report_start).days
                run_rates = calculate_run_rates(qb_pnl_metrics, days_run_rate)
                
                # 6. Forecast Year 1
                # Remaining Year 1 = Today -> 6/30/26
                days_remaining = (year_1_end - today).days

                # Forecast base is the active (possibly legacy-adjusted) metrics.
                forecast, months_rem = forecast_year_1(active_metrics, run_rates, days_remaining)
                
                # ---------------------------------------------------------
                # DASHBOARD
                # ---------------------------------------------------------
                
                # --- TOP METRICS ---
                st.subheader("Year-1 Outlook")
                
                col1, col2, col3, col4 = st.columns(4)
                
                sde_ytd = active_metrics["sde"]
                sde_proj = forecast["sde"]

                net_ytd = active_metrics["net_profit"]
                net_proj = forecast["net_profit"]
                
                col1.metric("SDE (Report Window)", currency(sde_ytd))
                col2.metric("Projected Year-1 SDE", currency(sde_proj), delta=currency(sde_proj - sde_ytd))
                col3.metric("Net Profit (Report Window)", currency(net_ytd))
                col4.metric("Projected Year-1 Net Profit", currency(net_proj))

                # Owner vs QB bridge (always shown; small + scannable)
                with st.expander("Reconciliation Bridge (Report Net vs Legacy Add-ins)", expanded=False):
                    qb_net_start_plus = float(qb_pnl_metrics.get("net_profit", 0.0))
                    legacy_overhead = float(legacy_overhead_included_total)
                    adjusted_net = float(active_metrics.get("net_profit", 0.0))

                    bridge_rows = [
                        {"Step": f"QB Net Profit ({report_start}+) ", "Amount": qb_net_start_plus},
                        {"Step": "Less: Legacy overhead add-ins (prior month)", "Amount": -legacy_overhead},
                        {"Step": "Equals: Net Profit (Adjusted)", "Amount": adjusted_net},
                    ]
                    bridge_df = pd.DataFrame(bridge_rows)
                    bridge_df_safe = make_arrow_safe(
                        bridge_df,
                        debug_label="RECONCILIATION_BRIDGE",
                        debug_mode=debug_mode,
                    )
                    st_dataframe_stretch(bridge_df_safe.style.format({"Amount": currency}))

                if debug_mode:
                    try:
                        st.markdown("#### Debug: Net Profit Reconciliation")
                        st.code(
                            "\n".join(
                                [
                                    f"QB Net (Report window): {float(qb_pnl_metrics.get('net_profit', 0.0))}",
                                    f"Legacy overhead add-ins: {float(legacy_overhead_included_total)}",
                                    f"Adjusted Net: {float(active_metrics.get('net_profit', 0.0))}",
                                ]
                            )
                        )
                    except Exception as e:
                        st.caption(f"Debug reconciliation failed: {e}")

                # --- UAT Metrics JSON Block (for Playwright / Cline) ---
                if debug_mode:
                    try:
                        uat_payload = {
                            "config": {
                                "report_start": str(report_start),
                                "current_date": str(today),
                                "year_1_end": str(year_1_end),
                                "cogs_prefixes": sorted(list(cogs_prefixes)),
                                "july_overhead_window": {
                                    "start": str(legacy_start),
                                    "end": str(legacy_end),
                                },
                                "july_overhead_selected_count": int(selected_legacy_overhead_rows_count),
                            },
                            "dashboard_metrics": {
                                "ytd_sde": float(active_metrics.get("sde", 0.0)),
                                "proj_sde": float(forecast.get("sde", 0.0)),
                                "ytd_net": float(active_metrics.get("net_profit", 0.0)),
                                "proj_net": float(forecast.get("net_profit", 0.0)),
                            },
                            "qb_pnl_metrics_report_window": {
                                "net_profit": float(qb_pnl_metrics.get("net_profit", 0.0)),
                                "sde": float(qb_pnl_metrics.get("sde", 0.0)),
                                "revenue": float(qb_pnl_metrics.get("revenue", 0.0)),
                                "cogs": float(qb_pnl_metrics.get("cogs", 0.0)),
                                "overhead": float(qb_pnl_metrics.get("overhead", 0.0)),
                                "other_expense": float(qb_pnl_metrics.get("other_expense", 0.0)),
                            },
                            "reconciliation_bridge": {
                                "qb_net_start_plus": float(qb_pnl_metrics.get("net_profit", 0.0)),
                                "legacy_overhead_included": float(legacy_overhead_included_total),
                                "adjusted_net": float(active_metrics.get("net_profit", 0.0)),
                            },
                            "run_rates": {
                                "monthly_revenue": float(run_rates.get("revenue", 0.0)),
                                "monthly_net": float(run_rates.get("net_profit", 0.0)),
                                "monthly_sde": float(run_rates.get("sde", 0.0)),
                            },
                        }

                        st.markdown("#### UAT Metrics (Machine Readable)")
                        st.code(
                            "UAT_METRICS_START\n"
                            + json.dumps(uat_payload, indent=2)
                            + "\nUAT_METRICS_END",
                            language="json",
                        )
                    except Exception as e:
                        st.warning(f"UAT metrics generation failed: {e}")

                # --- Simplified UI (3 tabs) ---
                tab_overview, tab_addbacks, tab_recon = st.tabs([
                    "Overview",
                    "Addbacks",
                    "Reconciliation",
                ])

                # Precompute monthly KPIs for the report window
                monthly_kpis = compute_monthly_kpis(report_df, owner_revenue_start=report_start)
                if monthly_kpis.empty:
                    monthly_view = monthly_kpis
                else:
                    start_period = pd.Period(report_start, freq="M")
                    monthly_view = monthly_kpis.loc[monthly_kpis["month"] >= start_period].copy()

                with tab_overview:
                    if debug_mode:
                        st.caption("TAB_OK::OVERVIEW")
                    try:
                        st.subheader("Overview")
                        c1, c2, c3 = st.columns(3)
                        c1.metric("Revenue (8/1+)", currency(float(qb_pnl_metrics.get("revenue", 0.0))))
                        c2.metric("Net Profit (Adjusted)", currency(float(active_metrics.get("net_profit", 0.0))))
                        c3.metric("SDE (Adjusted)", currency(float(active_metrics.get("sde", 0.0))))

                        st.markdown("#### Monthly Revenue / Net / SDE")
                        if monthly_view.empty:
                            st.info("No monthly data available for the report window.")
                        else:
                            series_df = monthly_view[["month_str", "revenue", "net_profit", "sde"]].copy()
                            series_df = series_df.set_index("month_str")
                            st.line_chart(series_df)

                            st.markdown("#### Margin % (Net, SDE)")
                            margin_df = monthly_view[["month_str", "net_margin_pct", "sde_margin_pct"]].copy()
                            margin_df.rename(
                                columns={
                                    "net_margin_pct": "Net Margin %",
                                    "sde_margin_pct": "SDE Margin %",
                                },
                                inplace=True,
                            )
                            margin_df["Net Margin %"] = pd.to_numeric(margin_df["Net Margin %"], errors="coerce")
                            margin_df["SDE Margin %"] = pd.to_numeric(margin_df["SDE Margin %"], errors="coerce")
                            st.line_chart(margin_df.set_index("month_str"))

                            st.markdown("#### Monthly Summary")
                            summary = monthly_view[[
                                "month_str",
                                "revenue",
                                "cogs",
                                "overhead",
                                "other_expense",
                                "net_profit",
                                "addbacks",
                                "sde",
                            ]].copy()
                            summary.rename(columns={"month_str": "Month"}, inplace=True)
                            summary_safe = make_arrow_safe(summary, debug_label="OVERVIEW_MONTHLY_SUMMARY", debug_mode=debug_mode)
                            st_dataframe_stretch(
                                summary_safe.style.format(
                                    {
                                        "revenue": currency,
                                        "cogs": currency,
                                        "overhead": currency,
                                        "other_expense": currency,
                                        "net_profit": currency,
                                        "addbacks": currency,
                                        "sde": currency,
                                    }
                                )
                            )
                    except Exception as e:
                        st.error(f"TAB_ERROR::OVERVIEW: {e}")
                        if debug_mode:
                            st.exception(e)

                with tab_addbacks:
                    if debug_mode:
                        st.caption("TAB_OK::ADDBACKS")
                    try:
                        st.subheader("Addbacks")
                        st.markdown("#### Rules (local JSON, optional)")
                        st.caption("Rules are loaded from data/addback_rules.json if present. The built-in payroll rule is always applied.")

                        existing_text = "[]"
                        try:
                            if ADDBACK_RULES_PATH.exists():
                                existing_text = ADDBACK_RULES_PATH.read_text(encoding="utf-8")
                        except Exception:
                            existing_text = "[]"

                        rules_text = st.text_area(
                            "Addback rules JSON (list of objects)",
                            value=existing_text,
                            height=220,
                        )

                        c_save, c_hint = st.columns([1, 3])
                        with c_save:
                            if st.button("Save rules"):
                                try:
                                    parsed = json.loads(rules_text or "[]")
                                    if not isinstance(parsed, list):
                                        raise ValueError("Rules JSON must be a list")
                                    ADDBACK_RULES_PATH.parent.mkdir(parents=True, exist_ok=True)
                                    ADDBACK_RULES_PATH.write_text(json.dumps(parsed, indent=2) + "\n", encoding="utf-8")
                                    st.success("Saved rules to data/addback_rules.json")
                                except Exception as e:
                                    st.error(f"Could not save rules: {e}")

                        with c_hint:
                            st.code(
                                json.dumps(
                                    [
                                        {
                                            "name": "owner_salary_exact",
                                            "name_contains": ["nathan"],
                                            "amount": 4000,
                                            "amount_tolerance": 0.01,
                                        }
                                    ],
                                    indent=2,
                                ),
                                language="json",
                            )

                        st.markdown("#### Addbacks by Month")
                        addback_rows = report_df.loc[report_df.get("sde_addback_flag", False).astype(bool)].copy()
                        if addback_rows.empty:
                            st.info("No addbacks detected in the report window.")
                        else:
                            addback_rows["month"] = addback_rows["date"].dt.to_period("M")
                            by_month = (
                                addback_rows.groupby("month")
                                .agg(
                                    addbacks=("amount", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0.0).abs().sum())),
                                    count=("amount", "size"),
                                )
                                .reset_index()
                            )
                            by_month["month_str"] = by_month["month"].dt.to_timestamp().dt.strftime("%b %Y")
                            by_month = by_month[["month_str", "addbacks", "count"]].rename(columns={"month_str": "Month"})
                            by_month_safe = make_arrow_safe(by_month, debug_label="ADDBACKS_BY_MONTH", debug_mode=debug_mode)
                            st_dataframe_stretch(by_month_safe.style.format({"addbacks": currency}))
                    except Exception as e:
                        st.error(f"TAB_ERROR::ADDBACKS: {e}")
                        if debug_mode:
                            st.exception(e)

                with tab_recon:
                    if debug_mode:
                        st.caption("TAB_OK::RECONCILIATION")
                    try:
                        st.subheader("Reconciliation")
                        st.caption("QB-style bridge showing adjustments from QB P&L (8/1+) to app-adjusted figures.")

                        qb_net_start_plus = float(qb_pnl_metrics.get("net_profit", 0.0))
                        legacy_overhead = float(legacy_overhead_included_total)
                        adjusted_net = float(active_metrics.get("net_profit", 0.0))

                        bridge_rows = [
                            {"Step": f"QB Net Profit ({report_start} to {today})", "Amount": qb_net_start_plus},
                            {"Step": f"Less: July overhead add-in (selected: {selected_legacy_overhead_rows_count})", "Amount": -legacy_overhead},
                            {"Step": "Equals: App Net Profit (Adjusted)", "Amount": adjusted_net},
                        ]
                        bridge_df = pd.DataFrame(bridge_rows)
                        bridge_df_safe = make_arrow_safe(bridge_df, debug_label="RECONCILIATION_BRIDGE", debug_mode=debug_mode)
                        st_dataframe_stretch(bridge_df_safe.style.format({"Amount": currency}))

                        st.markdown("#### Notes")
                        st.write("• Core P&L window is fixed to start 2025-08-01.")
                        st.write("• July add-in impacts overhead/net/SDE; July revenue is never included.")
                    except Exception as e:
                        st.error(f"TAB_ERROR::RECONCILIATION: {e}")
                        if debug_mode:
                            st.exception(e)

                if debug_mode:
                    with st.expander("Debug details", expanded=False):
                        st.markdown("#### Ledger (head)")
                        st_dataframe_stretch(make_arrow_safe(df.head(50), debug_label="LEDGER_HEAD", debug_mode=debug_mode))

        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
            st.exception(e)

else:
    st.info("Please upload the QuickBooks Ledger Export (CSV/Excel) to begin.")
