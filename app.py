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
from src.reconciliation import (
    normalize_bank_register,
    normalize_qb_ledger_for_bank,
    match_qb_and_bank,
    compute_cash_vs_accrual_summary
)


APP_SETTINGS_PATH = Path(__file__).resolve().parent / "config" / "app_settings.json"


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
st.session_state.setdefault("show_kpi_lab", False)
st.session_state.setdefault("kpi_include_july", False)

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
report_start = st.sidebar.date_input("Report Start Date", value=dt.date(2025, 8, 1))
year_1_end = st.sidebar.date_input("Year 1 End", value=dt.date(2026, 6, 30))
today = st.sidebar.date_input("Current Report Date", value=dt.date.today())

st.sidebar.markdown("---")

# Addback Configuration
with st.sidebar.expander("Addback Configuration"):
    custom_addback_str = st.text_input("Addback keywords (match Name/Memo)", value="")
    custom_addback_tokens = [x.strip() for x in custom_addback_str.split(",") if x.strip()]

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
                    st.dataframe(
                        make_arrow_safe(df.head(), debug_label="LEDGER_HEAD", debug_mode=debug_mode),
                        use_container_width=True,
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
                df = detect_addbacks(df, custom_tokens=custom_addback_tokens)

                # 3. Compute report-window metrics (QB P&L style): report_start -> today
                report_mask = (df["date"].dt.date >= report_start) & (df["date"].dt.date <= today)
                report_df = df.loc[report_mask].copy()
                qb_pnl_metrics = get_period_metrics(report_df, report_start, today)

                # 4. Optional legacy overhead add-ins (prior calendar month)
                first_of_report_month = report_start.replace(day=1)
                legacy_end = first_of_report_month - dt.timedelta(days=1)
                legacy_start = legacy_end.replace(day=1)

                legacy_overhead_mask = (
                    (df["date"].dt.date >= legacy_start)
                    & (df["date"].dt.date <= legacy_end)
                    & (df.get("is_overhead", False))
                )
                legacy_overhead_accounts = (
                    df.loc[legacy_overhead_mask, "account"].astype(str).dropna().unique().tolist()
                    if "account" in df.columns
                    else []
                )
                legacy_overhead_accounts = sorted([a for a in legacy_overhead_accounts if a and a.lower() != "nan"])

                with st.sidebar.expander("Legacy Overhead Add-ins (pre-start)"):
                    include_legacy_overhead = st.checkbox(
                        "Include selected overhead accounts from prior month",
                        value=False,
                        help=(
                            "Subtracts the selected prior-month overhead from Net Profit/SDE to match a "
                            "Year-1 owner-cost view. Report window remains Report Start Date -> Current Report Date."
                        ),
                    )
                    selected_legacy_overhead_accounts = st.multiselect(
                        f"Overhead accounts to include ({legacy_start} to {legacy_end})",
                        options=legacy_overhead_accounts,
                        default=[],
                        disabled=(not legacy_overhead_accounts),
                    )

                legacy_overhead_included_total = 0.0
                if include_legacy_overhead and selected_legacy_overhead_accounts:
                    legacy_overhead_included_total = compute_legacy_overhead_addins(
                        df,
                        legacy_start=legacy_start,
                        legacy_end=legacy_end,
                        included_accounts=set(selected_legacy_overhead_accounts),
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
                    st.dataframe(
                        bridge_df_safe.style.format({"Amount": currency}),
                        use_container_width=True,
                    )

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
                                "include_july": bool(st.session_state.get("kpi_include_july", False)),
                                "show_kpi_lab": bool(st.session_state.get("show_kpi_lab", False)),
                                "legacy_overhead_window": {
                                    "start": str(legacy_start),
                                    "end": str(legacy_end),
                                },
                                "legacy_overhead_selected_count": int(len(selected_legacy_overhead_accounts)),
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

                # --- DETAILED VIEWS ---

                tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
                    "📈 Forecast & Run Rates",
                    "🔍 Addbacks Analysis",
                    "📋 Data Inspection",
                    "📑 Project Billing Digital Twin",
                    "📚 Accounts & SDE Tuning",
                    "⚖️ Reconciliation",
                    "📊 KPI Explorer",
                ])
                
                with tab1:
                    if debug_mode:
                        st.caption("TAB_OK::FORECAST")
                    try:
                        st.markdown("#### Monthly Run-Rates (based on Aug+)")
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("Monthly Revenue", currency(run_rates["revenue"]))
                        c2.metric("Monthly SDE", currency(run_rates["sde"]))
                        c3.metric("Monthly Net Profit", currency(run_rates["net_profit"]))
                        c4.metric("Monthly Expenses (Total)", currency(run_rates["cogs"] + run_rates["overhead"] + run_rates["other_expense"]))
                        
                        # Forecast explainability widget
                        with st.expander("🤔 Why is the forecast so large?", expanded=False):
                            st.markdown("**Forecast Calculation Breakdown:**")
                            
                            # Calculate the components
                            days_run_rate = (today - report_start).days
                            months_run_rate = days_run_rate / 30.44  # Average days per month
                            days_remaining = (year_1_end - today).days
                            months_remaining = days_remaining / 30.44
                            
                            ytd_sde = active_metrics["sde"]
                            monthly_sde = run_rates["sde"]
                            projected_sde = forecast["sde"]
                            
                            col_exp1, col_exp2 = st.columns(2)
                            
                            with col_exp1:
                                st.metric("Days in run-rate window", f"{days_run_rate:.0f}")
                                st.metric("Months in run-rate", f"{months_run_rate:.1f}")
                                st.metric("YTD SDE (Actual)", currency(ytd_sde))
                                
                            with col_exp2:
                                st.metric("Days remaining in Year 1", f"{days_remaining:.0f}")
                                st.metric("Months remaining", f"{months_remaining:.1f}")
                                st.metric("Monthly run-rate SDE", currency(monthly_sde))
                            
                            st.markdown("**Exact Arithmetic:**")
                            forecast_addition = monthly_sde * months_remaining
                            st.write(f"• YTD SDE: {currency(ytd_sde)}")
                            st.write(f"• + Forecast addition: {currency(monthly_sde)} × {months_remaining:.1f} months = {currency(forecast_addition)}")
                            st.write(f"• **= Projected Year-1 SDE: {currency(projected_sde)}**")
                            
                            if projected_sde > ytd_sde * 3:
                                st.warning("⚠️ Projected SDE is >3x YTD. Consider if current run-rate is sustainable.")
                        
                        st.markdown("---")
                        st.markdown("#### Year-1 Forecast Breakdown")
                        
                        # Create a comparison dataframe
                        f_data = {
                            "Metric": ["Revenue", "COGS", "Overhead", "Other Expense", "Net Profit", "Addbacks", "SDE"],
                            "YTD (Actual)": [owner_metrics[k.lower().replace(" ", "_")] for k in ["Revenue", "COGS", "Overhead", "Other Expense", "Net Profit", "Addbacks", "SDE"]],
                            "Remaining (Fcst)": [forecast[k.lower().replace(" ", "_")] - owner_metrics[k.lower().replace(" ", "_")] for k in ["Revenue", "COGS", "Overhead", "Other Expense", "Net Profit", "Addbacks", "SDE"]],
                            "Year-1 Total": [forecast[k.lower().replace(" ", "_")] for k in ["Revenue", "COGS", "Overhead", "Other Expense", "Net Profit", "Addbacks", "SDE"]]
                        }
                        f_df = pd.DataFrame(f_data)
                        
                        # Formatting
                        f_df_safe = make_arrow_safe(
                            f_df,
                            debug_label="FORECAST_BREAKDOWN",
                            debug_mode=debug_mode,
                        )
                        st.dataframe(
                            f_df_safe.style.format(
                                {
                                    "YTD (Actual)": currency,
                                    "Remaining (Fcst)": currency,
                                    "Year-1 Total": currency,
                                }
                            ),
                            use_container_width=True,
                        )
                        
                        # Scenario Sliders
                        st.markdown("### What-If Scenarios (Year-1 Impact)")
                        sc_c1, sc_c2 = st.columns(2)
                        rev_adj = sc_c1.slider("Revenue Adjustment %", -20, 20, 0) / 100
                        addback_adj = sc_c2.slider("Addbacks Adjustment %", -20, 20, 0) / 100
                        
                        # Simple recalc
                        base_rev = forecast["revenue"]
                        base_sde = forecast["sde"]
                        
                        # Only adjust the Forecast portion or the Total? Usually Total for "What if Year 1 Revenue was X%"
                        new_rev = base_rev * (1 + rev_adj)
                        
                        # Addbacks
                        base_addbacks = forecast["addbacks"]
                        new_addbacks = base_addbacks * (1 + addback_adj)
                        
                        # SDE change = Rev Change + Addback Change (assuming variable costs scale? Prompt says simple +/- %)
                        # Let's assume Net Profit scales with Rev (Margin constant) -> SDE scales
                        # Or simply add the absolute revenue diff to bottom line?
                        # "Revenue +/- %" usually implies Gross Profit impact.
                        # Let's calculate Gross Profit impact.
                        base_gp_margin = forecast["gross_profit"] / base_rev if base_rev else 0
                        gp_impact = (new_rev - base_rev) * base_gp_margin
                        
                        new_sde = base_sde + gp_impact + (new_addbacks - base_addbacks)
                        
                        st.metric(f"Year-1 SDE (Scenario)", currency(new_sde), delta=currency(new_sde - base_sde))
                    except Exception as e:
                        st.error(f"TAB_ERROR::FORECAST: {e}")
                        st.exception(e)

                with tab2:
                    if debug_mode:
                        st.caption("TAB_OK::ADDBACKS")
                    try:
                        st.markdown("#### Addbacks Analysis")
                        st.caption("This shows transaction rows flagged as potential owner addbacks.")

                        st.markdown("#### Detected Addbacks")
                        addback_df = df[df["sde_addback_flag"]].copy()
                        
                        if not addback_df.empty:
                            addback_display = make_arrow_safe(
                                addback_df[["date", "name", "memo", "amount", "sde_addback_reason"]],
                                debug_label="ADDBACKS_TABLE",
                                debug_mode=debug_mode,
                            )
                            st.dataframe(
                                addback_display.style.format({"amount": currency}),
                                use_container_width=True,
                            )
                            
                            # Summary by reason
                            st.markdown("##### Addbacks by Reason")
                            reason_sum = addback_df.groupby("sde_addback_reason")["amount"].sum().reset_index()
                            reason_sum_safe = make_arrow_safe(
                                reason_sum,
                                debug_label="ADDBACKS_BY_REASON",
                                debug_mode=debug_mode,
                            )
                            st.dataframe(
                                reason_sum_safe.style.format({"amount": currency}),
                                use_container_width=True,
                            )
                        else:
                            st.info("No addbacks detected with current rules (XNP, NG, NATHAN).")
                    except Exception as e:
                        st.error(f"TAB_ERROR::ADDBACKS: {e}")
                        st.exception(e)

                with tab3:
                    if debug_mode:
                        st.caption("TAB_OK::DATA_INSPECTION")
                    try:
                        st.markdown("#### Data Inspection")

                        # Format common money columns if present
                        money_cols = [c for c in ["amount", "debit", "credit"] if c in df.columns]
                        if money_cols:
                            fmt = {c: currency for c in money_cols}
                            df_safe = make_arrow_safe(df, debug_label="DATA_INSPECTION", debug_mode=debug_mode)
                            st.dataframe(df_safe.style.format(fmt), use_container_width=True)
                        else:
                            st.dataframe(
                                make_arrow_safe(df, debug_label="DATA_INSPECTION_RAW", debug_mode=debug_mode),
                                use_container_width=True,
                            )

                        if debug_mode:
                            st.markdown("### Debug Info")
                            st.write(f"Total Rows: {len(df)}")
                            st.write("Columns:", df.columns.tolist())
                            if "date" in df.columns and not df.empty:
                                # Avoid Arrow serialization warnings from Series.describe() returning Timestamp objects
                                st.write("Date Column Stats:")
                                st.code(df["date"].describe().to_string())
                            else:
                                st.write("Date Column Stats:", "Empty/Missing")
                    except Exception as e:
                        st.error(f"TAB_ERROR::DATA_INSPECTION: {e}")
                        st.exception(e)

                with tab4:
                    if debug_mode:
                        st.caption("TAB_OK::BILLING")
                    try:
                        st.markdown("### Project Billing Digital Twin (Green Sheets → Invoices)")

                        green_file = st.file_uploader(
                            "Upload Green Sheet data (CSV/Excel)",
                            type=["csv", "xlsx", "xls"],
                            key="green_sheet_upload",
                        )

                        if green_file is not None:
                            # Read raw file
                            if green_file.name.lower().endswith(".csv"):
                                raw_gs = pd.read_csv(green_file)
                            else:
                                raw_gs = pd.read_excel(green_file)

                            # Normalize
                            gs = load_green_sheets(raw_gs)

                            st.markdown("#### Preview – Normalized Green Sheet Data")
                            st.dataframe(
                                make_arrow_safe(gs.head(100), debug_label="GREEN_SHEETS_PREVIEW", debug_mode=debug_mode),
                                use_container_width=True,
                            )

                            # Determine default billing period: current month to 'today'
                            default_start = today.replace(day=1)
                            default_end = today

                            colA, colB = st.columns(2)
                            period_start = colA.date_input("Billing period start", value=default_start)
                            period_end = colB.date_input("Billing period end", value=default_end)

                            # Derive job list from gs
                            jobs = sorted(gs["job"].dropna().astype(str).unique().tolist())
                            st.markdown("#### Job Billing Config (Cost-Plus Defaults)")

                            default_oh = 0.10
                            default_fee = 0.05

                            job_configs: list[JobBillingConfig] = []
                            for job in jobs:
                                c1, c2, c3 = st.columns(3)
                                with c1:
                                    st.write(job)
                                with c2:
                                    oh = st.number_input(
                                        f"{job} OH %",
                                        min_value=0.0,
                                        max_value=1.0,
                                        value=default_oh,
                                        step=0.01,
                                    )
                                with c3:
                                    fee = st.number_input(
                                        f"{job} Fee %",
                                        min_value=0.0,
                                        max_value=1.0,
                                        value=default_fee,
                                        step=0.01,
                                    )
                                job_configs.append(JobBillingConfig(job=job, overhead_pct=oh, profit_pct=fee))

                            if st.button("Compute Billing from Green Sheets"):
                                period_df = compute_period_billing(gs, job_configs, period_start, period_end)

                                st.subheader("Computed Invoices by Job")
                                from src.utils import currency  # ensure this import exists at top-level too if not already

                                period_df_safe = make_arrow_safe(
                                    period_df,
                                    debug_label="BILLING_PERIOD_DF",
                                    debug_mode=debug_mode,
                                )
                                st.dataframe(
                                    period_df_safe.style.format(
                                        {
                                            "materials": currency,
                                            "labor": currency,
                                            "supervision": currency,
                                            "overhead_amount": currency,
                                            "profit_amount": currency,
                                            "invoice_total": currency,
                                        }
                                    ),
                                    use_container_width=True,
                                )

                                st.markdown("##### Total for this billing run")
                                total = float(period_df["invoice_total"].sum()) if not period_df.empty else 0.0
                                st.metric("Sum of all job invoices", currency(total))

                                # Billing Validation: Green Sheets vs QB
                                st.markdown("---")
                                st.markdown("### Billing Validation (Green Sheets vs QB)")

                                # QB Revenue for the period
                                qb_revenue = df[
                                    (df['classification'] == 'revenue') &
                                    (df['date'] >= pd.to_datetime(period_start)) &
                                    (df['date'] <= pd.to_datetime(period_end)) &
                                    (df['amount'] < 0)  # QB revenue is negative
                                ].copy()

                                qb_agg = qb_revenue.groupby('class').agg({'amount': 'sum'}).reset_index()
                                qb_agg['qb_revenue'] = -qb_agg['amount']  # Make positive
                                qb_agg = qb_agg[['class', 'qb_revenue']]
                                
                                # Enhanced validation: November invoice matching
                                st.markdown("#### November Invoice Validation")
                                try:
                                    # Look for November invoices specifically
                                    nov_start = dt.date(2025, 11, 1)
                                    nov_end = dt.date(2025, 11, 30)
                                    
                                    nov_invoices = df[
                                        (df.get('account_type', '').str.contains('income', case=False, na=False)) &
                                        (df['date'] >= pd.to_datetime(nov_start)) &
                                        (df['date'] <= pd.to_datetime(nov_end)) &
                                        (df.get('memo', '').str.contains('invoice', case=False, na=False) |
                                         df.get('name', '').str.contains('invoice', case=False, na=False) |
                                         df.get('type', '').str.contains('invoice', case=False, na=False)) &
                                        (df['amount'] < 0)  # Income is negative
                                    ].copy()
                                    
                                    if not nov_invoices.empty:
                                        nov_by_job = nov_invoices.groupby('class').agg({'amount': 'sum'}).reset_index()
                                        nov_by_job['qb_nov_invoices'] = -nov_by_job['amount']  # Make positive
                                        nov_by_job = nov_by_job[['class', 'qb_nov_invoices']]
                                        
                                        # Compare with Green Sheet November billing
                                        nov_green = period_df[period_df['job'].notna()]  # Assume period covers Nov
                                        nov_comparison = nov_green[['job', 'invoice_total']].merge(
                                            nov_by_job,
                                            left_on='job',
                                            right_on='class', 
                                            how='outer'
                                        ).fillna(0)
                                        
                                        nov_comparison['nov_delta'] = nov_comparison['invoice_total'] - nov_comparison['qb_nov_invoices']
                                        total_nov_delta = nov_comparison['nov_delta'].sum()
                                        
                                        st.metric("November Invoice Delta (Green Sheet - QB)", currency(total_nov_delta))
                                        
                                        if abs(total_nov_delta) > 1000:  # Threshold for concern
                                            st.warning(f"⚠️ Large November invoice variance: {currency(total_nov_delta)}")
                                        
                                        # Show November detail if requested
                                        if st.checkbox("Show November invoice details"):
                                            st.dataframe(
                                                make_arrow_safe(
                                                    nov_comparison[['job', 'class', 'invoice_total', 'qb_nov_invoices', 'nov_delta']],
                                                    debug_label="NOV_INVOICE_COMPARISON",
                                                    debug_mode=debug_mode
                                                ).style.format({
                                                    'invoice_total': currency,
                                                    'qb_nov_invoices': currency,
                                                    'nov_delta': currency
                                                }),
                                                use_container_width=True
                                            )
                                    else:
                                        st.info("No November invoices found in QB data")
                                        
                                except Exception as e:
                                    st.warning(f"November validation failed: {e}")
                                
                                st.markdown("#### Full Period Comparison")

                                # Merge with period_df (Green Sheet invoices)
                                comparison_df = period_df[['job', 'invoice_total']].merge(
                                    qb_agg,
                                    left_on='job',
                                    right_on='class',
                                    how='outer'
                                ).fillna(0)

                                comparison_df['delta'] = comparison_df['invoice_total'] - comparison_df['qb_revenue']

                                st.markdown("#### Summary Comparison")
                                comparison_display = make_arrow_safe(
                                    comparison_df[["job", "invoice_total", "qb_revenue", "delta"]],
                                    debug_label="BILLING_COMPARISON",
                                    debug_mode=debug_mode,
                                )
                                st.dataframe(
                                    comparison_display.style.format(
                                        {
                                            "invoice_total": currency,
                                            "qb_revenue": currency,
                                            "delta": currency,
                                        }
                                    ),
                                    use_container_width=True,
                                )

                                # Drill-down expanders
                                for _, row in comparison_df.iterrows():
                                    job_name = row['job'] or row['class'] or "Unknown"
                                    with st.expander(f"Job: {job_name} Details", expanded=False):
                                        # Green Sheet details
                                        st.markdown("**Green Sheet Costs:**")
                                        gs_job = gs[gs['job'] == job_name]
                                        if not gs_job.empty:
                                            gs_job_display = make_arrow_safe(
                                                gs_job[["date", "amount", "cost_type", "description"]],
                                                debug_label=f"GREEN_SHEETS_JOB::{job_name}",
                                                debug_mode=debug_mode,
                                            )
                                            st.dataframe(
                                                gs_job_display.style.format({"amount": currency}),
                                                use_container_width=True,
                                            )
                                        else:
                                            st.info("No Green Sheet data for this job.")

                                        # QB Invoice details
                                        st.markdown("**QB Invoice Lines:**")
                                        qb_job = qb_revenue[qb_revenue['class'] == job_name]
                                        if not qb_job.empty:
                                            qb_job_display = make_arrow_safe(
                                                qb_job[["date", "num", "name", "memo", "amount"]],
                                                debug_label=f"QB_REVENUE_JOB::{job_name}",
                                                debug_mode=debug_mode,
                                            )
                                            st.dataframe(
                                                qb_job_display.style.format({"amount": currency}),
                                                use_container_width=True,
                                            )
                                        else:
                                            st.info("No QB revenue for this job.")
                        else:
                            st.info("Upload a Green Sheet export to compute job billing.")
                    except Exception as e:
                        st.error(f"TAB_ERROR::BILLING: {e}")
                        st.exception(e)

                with tab5:
                    if debug_mode:
                        st.caption("TAB_OK::ACCOUNTS_TUNING")
                    try:
                        st.markdown("### Accounts & SDE Tuning")
                        st.caption("This summarizes totals by account/category. Use it to spot miscodings to fix in QuickBooks.")

                        # Session state for account addbacks
                        if "account_addbacks_list" not in st.session_state:
                            st.session_state["account_addbacks_list"] = []

                        acct_addbacks = set(st.session_state["account_addbacks_list"])

                        # Build account summary
                        acct_df = build_account_summary(df, report_start, today, addback_accounts=acct_addbacks)

                        # Summary metrics
                        st.metric("Accounts (non-zero)", (acct_df["ytd_amount"] != 0).sum())
                        st.metric("Total Revenue Accounts", (acct_df["classification"] == "revenue").sum())
                        st.metric("Total Addback Accounts", acct_df["is_addback"].sum())

                        # Account table with checkboxes
                        st.markdown("#### Account-level Addback Overrides")

                        updated_addbacks = set(acct_addbacks)
                        for _, row in acct_df.sort_values("ytd_amount", ascending=False).iterrows():
                            account = row["account"] or "(No account)"
                            col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 2])
                            with col1:
                                st.write(account)
                            with col2:
                                st.write(row["account_type"])
                            with col3:
                                st.write(row["classification"])
                            with col4:
                                st.write(f"${row['ytd_amount']:,.2f}")
                            with col5:
                                checked = st.checkbox("Addback", key=f"acct_addback_{account}", value=row["is_addback"])
                                if checked:
                                    updated_addbacks.add(str(account))
                                else:
                                    if account in updated_addbacks:
                                        updated_addbacks.remove(account)

                        # Store back to session state
                        st.session_state["account_addbacks_list"] = list(updated_addbacks)

                        # Recompute metrics with overrides
                        owner_metrics_adjusted = get_owner_metrics(
                            df,
                            report_start,
                            today,
                            owner_revenue_start=report_start,
                            addback_account_overrides=updated_addbacks,
                        )

                        owner_metrics_adjusted = apply_legacy_overhead_addins(
                            owner_metrics_adjusted,
                            legacy_overhead_included_total=float(legacy_overhead_included_total),
                        )

                        # Show adjusted SDE
                        st.markdown("#### Adjusted SDE with Account-level Addbacks")
                        colA, colB = st.columns(2)
                        colA.metric("Adjusted YTD SDE", currency(owner_metrics_adjusted["sde"]))
                        colB.metric("Adjusted YTD Net Profit", currency(owner_metrics_adjusted["net_profit"]))
                    except Exception as e:
                        st.error(f"TAB_ERROR::ACCOUNTS_TUNING: {e}")
                        st.exception(e)

                with tab6:
                    if debug_mode:
                        st.caption("TAB_OK::RECONCILIATION")
                    try:
                        st.markdown("### Cash vs Accrual & Bank Reconciliation")
                        st.info(
                            "This reconciliation view compares a bank export to the selected QB bank account. "
                            "If you uploaded a QB Transaction Detail report, matches may be near-zero because transactions are split across accounts. "
                            "For best results, export a QB Bank Register for the same account/date range (future enhancement)."
                        )

                        bank_file_upload = st.file_uploader("Upload Bank Activity Export (CSV)", type=["csv"])
                        
                        if bank_file_upload is not None:
                            # 1. Load and normalize Bank CSV
                            try:
                                raw_bank_df = pd.read_csv(bank_file_upload)
                                bank_df = normalize_bank_register(raw_bank_df)
                                
                                st.success(f"Loaded Bank Export with {len(bank_df)} transactions.")
                                
                                # 2. Filter QB Ledger for Bank Account transactions
                                # Identify potential bank accounts
                                bank_accounts = df[df['account'].astype(str).str.contains('PNC', case=False, na=False)]['account'].unique()
                                
                                if len(bank_accounts) > 0:
                                    selected_account = st.selectbox("Select QB Bank Account to Reconcile:", bank_accounts)
                                    
                                    # Filter for specific account but keep structure needed for normalization
                                    # normalize_qb_ledger_for_bank expects: date, amount, memo, name, account
                                    # We filter from the main df which has these (account column has name)
                                    qb_bank_df_raw = df[df['account'] == selected_account].copy()
                                    qb_bank_df = normalize_qb_ledger_for_bank(qb_bank_df_raw)
                                    
                                    st.write(f"Selected QB Account has {len(qb_bank_df)} transactions.")
                                    if len(qb_bank_df) < 50:
                                        st.warning(
                                            "Selected QB account has very few rows. If you uploaded a QB Transaction Detail export, "
                                            "bank matching may not work well. Consider exporting a QB Bank Register instead."
                                        )
                                    
                                    # 3. Run Reconciliation
                                    date_window = st.slider("Date Matching Window (Days)", 1, 30, 14)
                                    
                                    match_res = match_qb_and_bank(qb_bank_df, bank_df, date_tolerance_days=date_window)
                                    
                                    matched = match_res.matched
                                    unmatched_bank = match_res.unmatched_bank
                                    unmatched_qb = match_res.unmatched_qb
                                    
                                    # 4. Display Metrics
                                    m1, m2, m3 = st.columns(3)
                                    m1.metric("Matched Transactions", len(matched))
                                    m2.metric("Unmatched Bank Items", len(unmatched_bank), delta=f"{currency(unmatched_bank['amount'].sum())}")
                                    m3.metric("Unmatched QB Items", len(unmatched_qb), delta=f"{currency(unmatched_qb['amount'].sum())}")
                                    
                                    # 5. Cash vs Accrual Summary (for the Bank Period)
                                    if not bank_df.empty:
                                        bank_start = bank_df['date'].min().date()
                                        bank_end = bank_df['date'].max().date()
                                        st.info(f"Bank Data Period: {bank_start} to {bank_end}")
                                        
                                        # Use the new summary function
                                        # We need the FULL QB Ledger (df) for Accrual P&L, not just the bank account rows
                                        # BUT compute_accrual_pl_from_qb expects standardized columns? 
                                        # It expects: date, amount, account_type
                                        # Our main 'df' has these.
                                        
                                        summary = compute_cash_vs_accrual_summary(df, bank_df, bank_start, bank_end)
                                        
                                        st.markdown("#### Cash (Bank) vs Accrual (QB P&L) - Selected Period")
                                        
                                        # Display as a nice table
                                        comp_data = {
                                            "Metric": ["Revenue", "Expenses", "Net Income"],
                                            "Cash Basis (Bank)": [summary['cash']['revenue_cash'], summary['cash']['expenses_cash'], summary['cash']['net_cash']],
                                            "Accrual P&L (QB)": [summary['accrual']['revenue_accrual'], summary['accrual']['expenses_accrual'], summary['accrual']['net_accrual']],
                                            "Difference (Accrual - Cash)": [summary['diff']['revenue_diff'], summary['diff']['expenses_diff'], summary['diff']['net_diff']]
                                        }
                                        comp_df_safe = make_arrow_safe(
                                            pd.DataFrame(comp_data),
                                            debug_label="CASH_VS_ACCRUAL_SUMMARY",
                                            debug_mode=debug_mode,
                                        )
                                        st.dataframe(
                                            comp_df_safe.style.format(
                                                {
                                                    "Cash Basis (Bank)": currency,
                                                    "Accrual P&L (QB)": currency,
                                                    "Difference (Accrual - Cash)": currency,
                                                }
                                            ),
                                            use_container_width=True,
                                        )

                                    # 6. Drill Down
                                    with st.expander("View Unmatched Bank Transactions (Possible Missing in QB)", expanded=True):
                                        unmatched_bank_display = make_arrow_safe(
                                            unmatched_bank.sort_values("date", ascending=False),
                                            debug_label="UNMATCHED_BANK",
                                            debug_mode=debug_mode,
                                        )
                                        st.dataframe(
                                            unmatched_bank_display.style.format({"amount": currency}),
                                            use_container_width=True,
                                        )
                                        
                                    with st.expander("View Unmatched QB Transactions (Timing or Errors)"):
                                        unmatched_qb_display = make_arrow_safe(
                                            unmatched_qb.sort_values("date", ascending=False)[
                                                ["date", "amount", "description"]
                                            ],
                                            debug_label="UNMATCHED_QB",
                                            debug_mode=debug_mode,
                                        )
                                        st.dataframe(
                                            unmatched_qb_display.style.format({"amount": currency}),
                                            use_container_width=True,
                                        )
                                        
                                else:
                                    st.warning("No 'PNC' accounts found in QB Ledger.")
                                    
                            except Exception as e:
                                st.error(f"Error processing bank file: {e}")
                                st.exception(e)
                        else:
                            st.info("Upload Bank CSV to begin reconciliation.")
                    except Exception as e:
                        st.error(f"TAB_ERROR::RECONCILIATION: {e}")
                        st.exception(e)

                with tab7:
                    if debug_mode:
                        st.caption("TAB_OK::KPI_EXPLORER")
                    try:
                        st.markdown("### KPI Explorer")

                        show_kpi_lab = st.checkbox(
                            "Show KPI Lab (experimental)",
                            key="show_kpi_lab",
                            value=bool(st.session_state.get("show_kpi_lab", False)),
                            help="Enables Benchmark KPI tables + charts. When off, existing KPI Explorer UI remains unchanged.",
                        )

                        # Shared monthly KPI table (month + classification aggregation)
                        monthly_kpis = compute_monthly_kpis(df, owner_revenue_start=report_start)

                        if show_kpi_lab:
                            st.markdown("### Benchmark KPIs (MoM + Margins)")

                            include_july = st.checkbox(
                                "Include prior month",
                                key="kpi_include_july",
                                value=bool(st.session_state.get("kpi_include_july", False)),
                                help="Include the calendar month immediately before Report Start Date.",
                            )

                            # Default view: Aug+ months; optionally include prior month (July)
                            if monthly_kpis.empty:
                                monthly_kpis_view = monthly_kpis
                            else:
                                start_period = pd.Period(report_start, freq="M")
                                cutoff = start_period - 1 if include_july else start_period
                                monthly_kpis_view = monthly_kpis.loc[monthly_kpis["month"] >= cutoff].copy()

                            try:
                                if monthly_kpis_view.empty:
                                    st.info("No monthly data available.")
                                else:
                                    def _to_opt_num(x):
                                        try:
                                            if x is None or pd.isna(x):
                                                return None
                                            return float(x)
                                        except Exception:
                                            return None

                                    # Formatted table (currency + percent) with MoM deltas
                                    table = monthly_kpis_view[[
                                        "month_str",
                                        "revenue",
                                        "cogs",
                                        "gross_profit",
                                        "gross_margin_pct",
                                        "overhead",
                                        "other_expense",
                                        "net_profit",
                                        "net_margin_pct",
                                        "addbacks",
                                        "sde",
                                        "sde_margin_pct",
                                        "revenue_mom_delta",
                                        "revenue_mom_pct",
                                        "net_profit_mom_delta",
                                        "net_profit_mom_pct",
                                        "sde_mom_delta",
                                        "sde_mom_pct",
                                        "cogs_pct",
                                        "overhead_pct",
                                    ]].copy()

                                    table.rename(
                                        columns={
                                            "month_str": "Month",
                                            "revenue": "Revenue",
                                            "cogs": "COGS",
                                            "gross_profit": "Gross Profit",
                                            "gross_margin_pct": "Gross Margin %",
                                            "overhead": "Overhead",
                                            "other_expense": "Other",
                                            "net_profit": "Net Profit",
                                            "net_margin_pct": "Net Margin %",
                                            "addbacks": "Addbacks",
                                            "sde": "SDE",
                                            "sde_margin_pct": "SDE Margin %",
                                            "revenue_mom_delta": "Revenue MoM Δ$",
                                            "revenue_mom_pct": "Revenue MoM Δ%",
                                            "net_profit_mom_delta": "Net Profit MoM Δ$",
                                            "net_profit_mom_pct": "Net Profit MoM Δ%",
                                            "sde_mom_delta": "SDE MoM Δ$",
                                            "sde_mom_pct": "SDE MoM Δ%",
                                            "cogs_pct": "COGS %",
                                            "overhead_pct": "Overhead %",
                                        },
                                        inplace=True,
                                    )

                                    # Normalize percent columns to (float|None) so we can render "—" reliably
                                    for pc in [
                                        "Gross Margin %",
                                        "Net Margin %",
                                        "SDE Margin %",
                                        "Revenue MoM Δ%",
                                        "Net Profit MoM Δ%",
                                        "SDE MoM Δ%",
                                        "COGS %",
                                        "Overhead %",
                                    ]:
                                        if pc in table.columns:
                                            table[pc] = table[pc].map(_to_opt_num)

                                    table_safe = make_arrow_safe(
                                        table,
                                        debug_label="KPI_LAB_BENCHMARK_TABLE",
                                        debug_mode=debug_mode,
                                    )
                                    st.dataframe(
                                        table_safe.style.format(
                                            {
                                                "Revenue": currency,
                                                "COGS": currency,
                                                "Gross Profit": currency,
                                                "Overhead": currency,
                                                "Other": currency,
                                                "Net Profit": currency,
                                                "Addbacks": currency,
                                                "SDE": currency,
                                                "Revenue MoM Δ$": currency,
                                                "Net Profit MoM Δ$": currency,
                                                "SDE MoM Δ$": currency,
                                                "Gross Margin %": lambda x: "—" if x is None else f"{x:.1%}",
                                                "Net Margin %": lambda x: "—" if x is None else f"{x:.1%}",
                                                "SDE Margin %": lambda x: "—" if x is None else f"{x:.1%}",
                                                "Revenue MoM Δ%": lambda x: "—" if x is None else f"{x:.1%}",
                                                "Net Profit MoM Δ%": lambda x: "—" if x is None else f"{x:.1%}",
                                                "SDE MoM Δ%": lambda x: "—" if x is None else f"{x:.1%}",
                                                "COGS %": lambda x: "—" if x is None else f"{x:.1%}",
                                                "Overhead %": lambda x: "—" if x is None else f"{x:.1%}",
                                            }
                                        ),
                                        use_container_width=True,
                                    )

                                    st.markdown("#### Charts")

                                    # Revenue + Net Profit by month
                                    trend_df = monthly_kpis_view[["month_str", "revenue", "net_profit"]].copy()
                                    trend_df.rename(columns={"month_str": "Month"}, inplace=True)
                                    trend_safe = make_arrow_safe(
                                        trend_df,
                                        debug_label="KPI_LAB_TREND_REV_NET",
                                        debug_mode=debug_mode,
                                    ).set_index("Month")
                                    st.line_chart(trend_safe)

                                    # Gross Margin %, Net Margin %, SDE Margin % by month
                                    margin_trend = monthly_kpis_view[[
                                        "month_str",
                                        "gross_margin_pct",
                                        "net_margin_pct",
                                        "sde_margin_pct",
                                    ]].copy()
                                    margin_trend.rename(columns={
                                        "month_str": "Month",
                                        "gross_margin_pct": "Gross Margin %",
                                        "net_margin_pct": "Net Margin %",
                                        "sde_margin_pct": "SDE Margin %",
                                    }, inplace=True)

                                    for c in ["Gross Margin %", "Net Margin %", "SDE Margin %"]:
                                        margin_trend[c] = pd.to_numeric(margin_trend[c], errors="coerce")

                                    margin_safe = make_arrow_safe(
                                        margin_trend,
                                        debug_label="KPI_LAB_TREND_MARGINS",
                                        debug_mode=debug_mode,
                                    ).set_index("Month")
                                    st.line_chart(margin_safe)

                                    # Stacked bar: COGS/Overhead/Other by month
                                    exp_df = monthly_kpis_view[["month_str", "cogs", "overhead", "other_expense"]].copy()
                                    exp_df.rename(columns={"month_str": "Month", "other_expense": "other"}, inplace=True)
                                    try:
                                        import altair as alt

                                        exp_long = exp_df.melt(
                                            id_vars=["Month"],
                                            value_vars=["cogs", "overhead", "other"],
                                            var_name="expense_type",
                                            value_name="amount",
                                        )
                                        exp_long_safe = make_arrow_safe(
                                            exp_long,
                                            debug_label="KPI_LAB_EXP_LONG",
                                            debug_mode=debug_mode,
                                        )
                                        chart = (
                                            alt.Chart(exp_long_safe)
                                            .mark_bar()
                                            .encode(
                                                x=alt.X("Month:N", title="Month"),
                                                y=alt.Y("sum(amount):Q", title="Amount"),
                                                color=alt.Color("expense_type:N", title="Expense"),
                                            )
                                        )
                                        st.altair_chart(chart, use_container_width=True)
                                    except Exception:
                                        exp_safe = make_arrow_safe(
                                            exp_df,
                                            debug_label="KPI_LAB_EXP_BAR_FALLBACK",
                                            debug_mode=debug_mode,
                                        ).set_index("Month")
                                        st.bar_chart(exp_safe)

                                    with st.expander("Month / Job / Account Drilldowns", expanded=False):
                                        month_options = monthly_kpis_view["month_str"].tolist()
                                        selected_month_str = st.selectbox(
                                            "Select month",
                                            month_options,
                                            index=len(month_options) - 1,
                                            key="kpi_lab_month_select",
                                        )
                                        selected_period = monthly_kpis_view.loc[
                                            monthly_kpis_view["month_str"] == selected_month_str, "month"
                                        ].iloc[0]

                                        df_month = df.copy()
                                        df_month["month"] = df_month["date"].dt.to_period("M")
                                        month_subset = df_month.loc[df_month["month"] == selected_period].copy()

                                        jobs = sorted(month_subset.get("class", pd.Series(dtype=object)).dropna().unique().tolist())
                                        selected_job = st.selectbox(
                                            "Select job",
                                            ["(All)"] + jobs,
                                            key="kpi_lab_job_select",
                                        )
                                        if selected_job != "(All)":
                                            month_subset = month_subset.loc[month_subset.get("class") == selected_job].copy()

                                        accounts = sorted(month_subset.get("account", pd.Series(dtype=object)).dropna().astype(str).unique().tolist())
                                        selected_account = st.selectbox(
                                            "Select account",
                                            ["(All)"] + accounts,
                                            key="kpi_lab_account_select",
                                        )
                                        if selected_account != "(All)":
                                            month_subset = month_subset.loc[month_subset.get("account").astype(str) == selected_account].copy()

                                        if month_subset.empty:
                                            st.info("No rows for this selection.")
                                        else:
                                            cols = [c for c in ["date", "class", "account", "name", "memo", "amount", "classification"] if c in month_subset.columns]
                                            st.dataframe(
                                                make_arrow_safe(
                                                    month_subset[cols].sort_values("date"),
                                                    debug_label="KPI_LAB_DRILLDOWN",
                                                    debug_mode=debug_mode,
                                                ).style.format({"amount": currency}),
                                                use_container_width=True,
                                            )
                            except Exception as e:
                                st.error("Benchmark KPIs failed to render. Try disabling KPI Lab, or re-uploading the ledger.")
                                if debug_mode:
                                    st.exception(e)
                        
                        # Monthly aggregation
                        df_monthly = df.copy()
                        df_monthly['month'] = df_monthly['date'].dt.to_period('M')
                        df_monthly['month_label'] = df_monthly['month'].dt.to_timestamp().dt.strftime('%b %Y')

                        # Robust aggregation using the classification column
                        by_class = (
                            df_monthly
                            .groupby(["month", "classification"])
                            .agg(amount=("amount", "sum"))
                            .reset_index()
                        )

                        # Pivot
                        monthly_pivot = by_class.pivot_table(
                            index='month',
                            columns='classification',
                            values='amount',
                            aggfunc='sum',
                            fill_value=0
                        ).reset_index()

                        # Map capitalized classification to lowercase columns
                        rename_map = {
                            "Revenue": "revenue",
                            "COGS": "cogs",
                            "Overhead": "overhead",
                            "Other Expense": "other_expense",
                            "Other": "other_expense",
                        }
                        monthly_pivot.rename(columns=rename_map, inplace=True)

                        # Ensure columns exist
                        for col in ['revenue', 'cogs', 'overhead', 'other_expense']:
                            if col not in monthly_pivot.columns:
                                monthly_pivot[col] = 0.0

                        # Calculate Net Profit & SDE
                        # Revenue is negative in QB. Expenses positive.
                        # We want positive Revenue for display.
                        monthly_pivot['revenue_display'] = -monthly_pivot['revenue']
                        monthly_pivot['net_profit'] = monthly_pivot['revenue_display'] - (monthly_pivot['cogs'] + monthly_pivot['overhead'] + monthly_pivot['other_expense'])

                        # Addbacks per month
                        addback_agg = df_monthly[df_monthly['sde_addback_flag']].groupby('month').agg(addbacks=('amount', 'sum')).reset_index()
                        
                        monthly_pivot = monthly_pivot.merge(addback_agg, on='month', how='left').fillna(0)
                        monthly_pivot['sde'] = monthly_pivot['net_profit'] + monthly_pivot['addbacks']
                        
                        # Month label for display
                        monthly_pivot['month_str'] = monthly_pivot['month'].dt.to_timestamp().dt.strftime('%b %Y')

                        if show_kpi_lab:
                            st.markdown("#### Monthly Net Profit & SDE Trend")
                            chart_data = monthly_pivot[['month_str', 'net_profit', 'sde']].set_index('month_str')
                            st.bar_chart(chart_data)

                        # Monthly summaries with expanders
                        st.markdown("#### Monthly Breakdowns")
                        monthly_pivot = monthly_pivot.sort_values('month')
                        
                        for _, row in monthly_pivot.iterrows():
                            month_str = str(row['month_str'])
                            with st.expander(f"{month_str} Summary", expanded=False):
                                col1, col2, col3, col4 = st.columns(4)
                                col1.metric("Revenue", currency(row['revenue_display']))
                                col2.metric("COGS", currency(row['cogs']))
                                col3.metric("Overhead", currency(row['overhead']))
                                col4.metric("Net Profit", currency(row['net_profit']))

                                # Job-level profitability for this month
                                month_mask = df_monthly['month'] == row['month']
                                
                                # Job Pivot
                                job_subset = df_monthly[month_mask]
                                if not job_subset.empty:
                                    job_by_class = job_subset.groupby(['class', 'classification']).agg(amount=('amount', 'sum')).reset_index()
                                    
                                    job_pivot = job_by_class.pivot_table(
                                        index='class',
                                        columns='classification',
                                        values='amount',
                                        aggfunc='sum',
                                        fill_value=0
                                    ).reset_index()
                                    
                                    # Rename and Ensure cols
                                    job_pivot.rename(columns=rename_map, inplace=True)
                                    for col in ['revenue', 'cogs', 'overhead', 'other_expense']:
                                        if col not in job_pivot.columns:
                                            job_pivot[col] = 0.0
                                    
                                    # Calc Net Profit (Flip Revenue)
                                    job_pivot['revenue_display'] = -job_pivot['revenue']
                                    job_pivot['net_profit'] = job_pivot['revenue_display'] - (job_pivot['cogs'] + job_pivot['overhead'] + job_pivot['other_expense'])
                                    
                                    st.markdown("**Job-Level Profitability:**")
                                    job_display = job_pivot[[
                                        'class',
                                        'revenue_display',
                                        'cogs',
                                        'overhead',
                                        'net_profit',
                                    ]].rename(columns={'revenue_display': 'revenue'})
                                    job_display_safe = make_arrow_safe(
                                        job_display,
                                        debug_label=f"KPI_JOB_PROFIT::{month_str}",
                                        debug_mode=debug_mode,
                                    )
                                    st.dataframe(
                                        job_display_safe.style.format(
                                            {
                                                "revenue": currency,
                                                "cogs": currency,
                                                "overhead": currency,
                                                "net_profit": currency,
                                            }
                                        ),
                                        use_container_width=True,
                                    )

                        # YTD Addbacks breakdown
                        if show_kpi_lab:
                            st.markdown("#### YTD Addbacks Breakdown")
                            addback_ytd = df[df['sde_addback_flag']].groupby('sde_addback_reason')['amount'].sum().reset_index()
                            if not addback_ytd.empty:
                                addback_ytd = addback_ytd.sort_values('amount', ascending=True)
                                st.bar_chart(addback_ytd.set_index('sde_addback_reason'))
                            else:
                                st.info("No addbacks detected.")

                        # Job-level drill-down
                        st.markdown("#### Job-Level Drill-Down")
                        jobs = sorted(df['class'].dropna().unique().tolist())
                        if jobs:
                            selected_job = st.selectbox("Select Job for Drill-Down", jobs, key="job_drilldown")
                            if selected_job:
                                job_df = df[df['class'] == selected_job].copy()
                                job_df_display = make_arrow_safe(
                                    job_df[["date", "name", "memo", "amount", "classification"]],
                                    debug_label=f"JOB_DRILLDOWN::{selected_job}",
                                    debug_mode=debug_mode,
                                )
                                st.dataframe(
                                    job_df_display.style.format({"amount": currency}),
                                    use_container_width=True,
                                )
                        else:
                            st.info("No job classes found.")

                    except Exception as e:
                        st.error(f"TAB_ERROR::KPI_EXPLORER: {e}")
                        if debug_mode:
                            st.exception(e)

        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
            st.exception(e)

else:
    st.info("Please upload the QuickBooks Ledger Export (CSV/Excel) to begin.")
