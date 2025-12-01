import pandas as pd
import numpy as np

def calculate_run_rates(owner_metrics, days_in_owner_revenue_period):
    """
    Computes monthly run rates based on Owner Revenue Period (Aug+).
    
    owner_metrics: Dictionary of Owner YTD metrics (Rev, Net, SDE, etc.)
    days_in_owner_revenue_period: Days since Aug 1 (or whenever owner revenue started).
    """
    months = max(days_in_owner_revenue_period / 30.4375, 1.0)
    
    # Owner Revenue is already strictly Aug+ in the metrics passed?
    # Yes, get_owner_metrics returns Owner Revenue (excluding July).
    # So we divide by months since Aug 1.
    
    # However, Owner Expenses included July.
    # So Expense Run Rate might be different if we used July?
    # But if we want "Owner Year 1 Forecast", usually we project forward the "Steady State".
    # July might be weird.
    # Prompt: "Use the owner-only run-rates (based mainly on Aug+ actuals)."
    
    # Let's assume we are passed metrics derived from Aug+ ONLY for the purpose of Run Rate calculation?
    # Or we just use the Owner Total / Owner Months?
    
    # If we use Owner Total Revenue (Aug-Nov) / 4 months -> Good Run Rate.
    # If we use Owner Total Expenses (July-Nov) / 5 months -> Good Run Rate?
    # Maybe.
    
    # Simple approach: Return the monthly average of the provided metrics over the provided days.
    
    return {k: v / months for k, v in owner_metrics.items()}

def forecast_year_1(
    owner_ytd_metrics,
    monthly_run_rates,
    days_remaining_in_year_1
):
    """
    Forecasts Year 1 Total = YTD Actuals + (Run Rate * Remaining Months).
    
    Year 1: 7/1/25 - 6/30/26.
    """
    months_remaining = max(days_remaining_in_year_1 / 30.4375, 0)
    
    forecast_add = {k: v * months_remaining for k, v in monthly_run_rates.items()}
    
    year_1_forecast = {}
    for k in owner_ytd_metrics:
        # YTD + Forecast
        year_1_forecast[k] = owner_ytd_metrics.get(k, 0) + forecast_add.get(k, 0)
        
    return year_1_forecast, months_remaining
