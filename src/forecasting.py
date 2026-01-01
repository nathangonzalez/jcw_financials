import pandas as pd
import numpy as np

def calculate_run_rates(period_metrics: dict, days_in_period: int) -> dict:
    if days_in_period <= 0:
        # Return zeros for all keys
        return {k: 0.0 for k in period_metrics}
    
    # Daily rates
    daily = {k: period_metrics[k] / days_in_period for k in period_metrics}
    
    # Monthly rates (365.25 / 12 = 30.4375)
    avg_days_per_month = 30.4375
    monthly = {k: daily[k] * avg_days_per_month for k in period_metrics}
    
    return monthly

def forecast_year_1(owner_metrics: dict, run_rates: dict, days_remaining: int) -> tuple[dict, float]:
    avg_days_per_month = 30.4375
    
    # Convert monthly run rates back to daily
    daily = {k: run_rates[k] / avg_days_per_month for k in run_rates}
    
    # Forecast remaining
    rem_days = max(days_remaining, 0)
    rem = {k: daily[k] * rem_days for k in daily}
    
    # Combine YTD + Remaining
    total = {k: owner_metrics.get(k, 0.0) + rem.get(k, 0.0) for k in owner_metrics}
    
    # Recalculate Gross Profit for consistency
    total["gross_profit"] = total["revenue"] - total["cogs"]
    
    # Recalculate Net Profit for consistency? The run rate for net profit is based on (rev - exp).
    # (Rev_ytd + Rev_rem) - (Exp_ytd + Exp_rem) = (Rev_ytd - Exp_ytd) + (Rev_rem - Exp_rem)
    # = Net_ytd + Net_rem.
    # So it should be consistent mathematically, but floating point might differ slightly.
    # Let's trust the addition.
    
    months_remaining = rem_days / avg_days_per_month
    
    return total, months_remaining
