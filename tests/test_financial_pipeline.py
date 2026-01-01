import pytest
import pandas as pd
import datetime as dt
from src.business_logic import (
    classify_transactions,
    detect_addbacks,
    get_owner_metrics,
    get_period_metrics
)
from src.forecasting import calculate_run_rates, forecast_year_1

@pytest.fixture
def sample_ledger():
    data = [
        # Date, Account, Type, Name, Memo, Amount
        # July (Pre-Owner Revenue)
        ("2025-07-01", "Sales", "Income", "Customer A", "Inv 101", 1000.0), # Should be excluded from owner metrics
        ("2025-07-15", "COGS Material", "Cost of Goods Sold", "Supplier X", "Materials", -500.0), # Included
        ("2025-07-20", "Rent", "Expense", "Landlord", "July Rent", -1000.0), # Included overhead
        
        # August (Owner Revenue Start)
        ("2025-08-01", "Sales", "Income", "Customer B", "Inv 102", 2000.0), # Included
        ("2025-08-05", "COGS Labor", "Cost of Goods Sold", "Worker Y", "Labor", -600.0), # Included
        ("2025-08-10", "Utilities", "Expense", "Power Co", "August Power", -200.0), # Included overhead
        
        # Addback
        ("2025-08-15", "Owner Salary", "Expense", "Nathan", "Owner Draw", -3000.0), # Addback
        
        # Other Expense
        ("2025-08-20", "Interest", "Other Expense", "Bank", "Loan Interest", -50.0), # Other Expense
    ]
    df = pd.DataFrame(data, columns=["date", "account", "account_type", "name", "memo", "amount"])
    df["date"] = pd.to_datetime(df["date"])
    return df

def test_pipeline(sample_ledger):
    df = sample_ledger
    
    # 1. Classify
    df = classify_transactions(df)
    assert df.loc[0, "is_revenue"] == True
    assert df.loc[1, "is_cogs"] == True
    assert df.loc[2, "is_overhead"] == True
    assert df.loc[7, "is_other_expense"] == True
    
    # 2. Detect Addbacks
    df = detect_addbacks(df)
    # Row 6 is "Nathan" -> should be flagged
    assert df.loc[6, "sde_addback_flag"] == True
    assert "token=nathan" in df.loc[6, "sde_addback_reason"]
    
    # 3. Owner Metrics
    owner_period_start = dt.date(2025, 7, 1)
    owner_revenue_start = dt.date(2025, 8, 1)
    current_date = dt.date(2025, 8, 31)
    
    metrics = get_owner_metrics(df, owner_period_start, current_date, owner_revenue_start)
    
    # Revenue: Should only include Aug ($2000). July ($1000) excluded.
    assert metrics["revenue"] == 2000.0
    
    # COGS: Owner-period COGS is Aug only. July COGS is tracked separately as legacy_cogs.
    assert metrics["cogs"] == 600.0
    assert metrics["legacy_cogs"] == 500.0
    
    # Overhead (core P&L): Aug Utils (-200) + Owner Salary (-3000) = -3200 -> Positive 3200
    # July overhead is tracked separately for optional add-in handling.
    assert metrics["overhead"] == 3200.0
    assert metrics["legacy_july_included_overhead"] == 1000.0
    
    # Other Expense: Aug Interest (-50) -> Positive 50
    assert metrics["other_expense"] == 50.0
    
    # Net Profit: Rev - (COGS + Overhead + Other)
    # 2000 - (600 + 3200 + 50) = 2000 - 3850 = -1850
    assert metrics["net_profit"] == -1850.0
    
    # Addbacks: Owner Salary (-3000) -> Positive 3000
    assert metrics["addbacks"] == 3000.0
    
    # SDE: Net Profit + Addbacks = -1850 + 3000 = 1150
    assert metrics["sde"] == 1150.0
    
    # 4. Period Metrics (Run Rate Basis: Aug 1 - Aug 31)
    # Filter df first
    run_rate_mask = (df["date"].dt.date >= owner_revenue_start) & (df["date"].dt.date <= current_date)
    run_rate_df = df[run_rate_mask]
    
    rr_metrics = get_period_metrics(run_rate_df, owner_revenue_start, current_date)
    
    # Aug only:
    # Rev: 2000
    # COGS: 600
    # Overhead: 200 + 3000 = 3200
    # Other: 50
    # Net: 2000 - (600 + 3200 + 50) = 2000 - 3850 = -1850
    # Addbacks: 3000
    # SDE: -1850 + 3000 = 1150
    
    assert rr_metrics["revenue"] == 2000.0
    assert rr_metrics["sde"] == 1150.0
    
    # 5. Run Rates
    days_in_aug = 31
    run_rates = calculate_run_rates(rr_metrics, days_in_aug)
    
    # Monthly Rev = 2000 / 31 * 30.4375 approx 1963.7
    expected_rev_rr = (2000 / 31) * 30.4375
    assert abs(run_rates["revenue"] - expected_rev_rr) < 0.01
    
    # 6. Forecast
    # Remaining days in Year 1. Let's say 10 months remaining (approx 304 days).
    days_remaining = 304
    forecast, months = forecast_year_1(metrics, run_rates, days_remaining)
    
    # Forecast Rev = YTD (2000) + (Daily RR * 304)
    # Daily RR = 2000/31
    rem_rev = (2000 / 31) * 304
    assert abs(forecast["revenue"] - (2000 + rem_rev)) < 0.01
    
    # SDE Forecast
    # YTD SDE (1150) + Rem SDE
    # Rem SDE = (1150 / 31) * 304
    rem_sde = (1150 / 31) * 304
    assert abs(forecast["sde"] - (1150 + rem_sde)) < 0.01
