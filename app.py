import streamlit as st
import pandas as pd
import datetime as dt
import numpy as np

from src.utils import currency
from src.data_loader import load_ledger
from src.business_logic import (
    classify_transactions,
    detect_addbacks,
    get_owner_metrics,
    get_period_metrics
)
from src.forecasting import calculate_run_rates, forecast_year_1

def normalize_ledger_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Standardize column names (lowercase, underscores).
    - Drop the 'unnamed: x' junk columns from the Excel export.
    - Remove duplicate columns (keep the first occurrence).
    """
    df = df.copy()

    # 1) Normalize names: strip + lowercase + spaces -> underscores
    df.columns = [
        str(c).strip().lower().replace(" ", "_")
        for c in df.columns
    ]

    # 2) Drop the 'unnamed: x' noise columns
    keep_cols = [c for c in df.columns if not c.startswith("unnamed")]
    df = df[keep_cols].copy()

    # 3) Drop duplicate column names (keep the first occurrence)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    return df

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="Owner SDE Dashboard", layout="wide")
st.title("💰 Owner SDE & Profit Dashboard")
st.caption("Focus: Owner Net Profit & Seller's Discretionary Earnings (Year 1)")

# ---------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------
st.sidebar.header("Configuration")

# Dates
acq_date = st.sidebar.date_input("Acquisition Date", value=dt.date(2025, 7, 1))
owner_rev_start = st.sidebar.date_input("Owner Revenue Start", value=dt.date(2025, 8, 1))
year_1_end = st.sidebar.date_input("Year 1 End", value=dt.date(2026, 6, 30))
today = st.sidebar.date_input("Current Report Date", value=dt.date.today())

st.sidebar.markdown("---")

# Addback Configuration
with st.sidebar.expander("Addback Configuration"):
    custom_addback_str = st.text_input("Custom Addback Tokens (comma separated)", value="")
    custom_addback_tokens = [x.strip() for x in custom_addback_str.split(",") if x.strip()]

st.sidebar.markdown("---")
ledger_file = st.sidebar.file_uploader("Upload QuickBooks Ledger Export", type=["csv", "xlsx", "xls"])

# ---------------------------------------------------------
# DATA PROCESSING
# ---------------------------------------------------------
if ledger_file:
    with st.spinner("Processing Ledger..."):
        try:
            df = load_ledger(ledger_file)
            
            st.write("DEBUG: Ledger loaded?", df is not None)
            if df is not None:
                st.write(f"DEBUG: Rows: {len(df)}")
                
            # 🔧 Clean the columns here
            df = normalize_ledger_columns(df)
            
            # Force date type again to prevent PyArrow errors
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            
            st.write("DEBUG: Columns:", df.columns.tolist())
            st.dataframe(df.head())
            
            # 🔍 Sanity checks
            if "amount" in df.columns:
                st.write("DEBUG: amount sample:", df["amount"].head())
                st.write("DEBUG: total amount:", float(df["amount"].sum()))

            if "account_type" in df.columns:
                st.write("DEBUG: account_type counts:", df["account_type"].value_counts())
                income_mask = df["account_type"].str.contains("income", case=False, na=False)
                st.write(
                    "DEBUG: raw income sum (amount):",
                    float(df.loc[income_mask, "amount"].sum())
                )
            
            if df is not None:
                # 1. Classify
                df = classify_transactions(df)
                
                # 2. Addbacks
                df = detect_addbacks(df, custom_tokens=custom_addback_tokens)
                
                # 3. Calculate Owner Metrics (YTD)
                # Owner Period: 7/1 -> Today
                # Metrics logic handles excluding July Revenue based on owner_rev_start
                owner_metrics = get_owner_metrics(df, acq_date, today, owner_revenue_start=owner_rev_start)
                
                # 4. Calculate Run Rates
                # We need metrics specifically for the Run Rate Period (Aug -> Today)
                # Logic: Forecast based on "Aug+ actuals"
                # So let's compute metrics just for Aug -> Today for run-rate purposes
                run_rate_df = df[(df["date"] >= pd.to_datetime(owner_rev_start)) & (df["date"] <= pd.to_datetime(today))]
                run_rate_metrics = get_period_metrics(run_rate_df, owner_rev_start, today)
                
                days_run_rate = (today - owner_rev_start).days
                run_rates = calculate_run_rates(run_rate_metrics, days_run_rate)
                
                # 5. Forecast Year 1
                # Remaining Year 1 = Today -> 6/30/26
                days_remaining = (year_1_end - today).days
                
                # Note: We base forecast on YTD (which includes July Expenses) + (RunRate * Remaining)
                # This correctly forecasts "Total Year 1 Owner Costs"
                # And "Total Year 1 Owner Revenue" (since July Rev was 0 in YTD).
                forecast, months_rem = forecast_year_1(owner_metrics, run_rates, days_remaining)
                
                # ---------------------------------------------------------
                # DASHBOARD
                # ---------------------------------------------------------
                
                # --- TOP METRICS ---
                st.subheader("Owner Year-1 Outlook")
                
                col1, col2, col3, col4 = st.columns(4)
                
                sde_ytd = owner_metrics["sde"]
                sde_proj = forecast["sde"]
                
                net_ytd = owner_metrics["net_profit"]
                net_proj = forecast["net_profit"]
                
                col1.metric("YTD Owner SDE", currency(sde_ytd))
                col2.metric("Projected Year-1 SDE", currency(sde_proj), delta=currency(sde_proj - sde_ytd))
                col3.metric("YTD Net Profit", currency(net_ytd))
                col4.metric("Projected Year-1 Net Profit", currency(net_proj))
                
                # --- DETAILED VIEWS ---
                
                tab1, tab2, tab3 = st.tabs(["📈 Forecast & Run Rates", "🔍 Addbacks Analysis", "📋 Data Inspection"])
                
                with tab1:
                    st.markdown("#### Monthly Run-Rates (based on Aug+)")
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Monthly Revenue", currency(run_rates["revenue"]))
                    c2.metric("Monthly SDE", currency(run_rates["sde"]))
                    c3.metric("Monthly Net Profit", currency(run_rates["net_profit"]))
                    c4.metric("Monthly Expenses (Total)", currency(run_rates["cogs"] + run_rates["overhead"] + run_rates["other_expense"]))
                    
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
                    st.dataframe(f_df.style.format({
                        "YTD (Actual)": currency,
                        "Remaining (Fcst)": currency,
                        "Year-1 Total": currency
                    }), use_container_width=True)
                    
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

                with tab2:
                    st.markdown("#### Detected Addbacks")
                    addback_df = df[df["sde_addback_flag"]].copy()
                    
                    if not addback_df.empty:
                        st.dataframe(addback_df[["date", "name", "memo", "amount", "sde_addback_reason"]].style.format({"amount": currency}))
                        
                        # Summary by reason
                        st.markdown("##### Addbacks by Reason")
                        reason_sum = addback_df.groupby("sde_addback_reason")["amount"].sum().reset_index()
                        st.dataframe(reason_sum.style.format({"amount": currency}))
                    else:
                        st.info("No addbacks detected with current rules (XNP, NG, NATHAN).")

                with tab3:
                    st.markdown("#### Raw Ledger (Classified)")
                    st.dataframe(df)
                    
                    st.markdown("### Debug Info")
                    st.write(f"Total Rows: {len(df)}")
                    st.write("Columns:", df.columns.tolist())
                    st.write("Date Column Stats:", df["date"].describe() if "date" in df.columns and not df.empty else "Empty/Missing")
        except Exception as e:
            st.error(f"An error occurred during processing: {e}")
            st.exception(e)

else:
    st.info("Please upload the QuickBooks Ledger Export (CSV/Excel) to begin.")
