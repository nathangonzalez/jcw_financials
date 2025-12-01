import pytest
import pandas as pd
import numpy as np
from src.business_logic import (
    classify_transactions,
    detect_addbacks,
    compute_metrics,
    get_owner_metrics
)

@pytest.fixture
def sample_df():
    data = {
        "date": pd.to_datetime(["2025-07-15", "2025-07-20", "2025-08-05", "2025-08-10", "2025-08-15", "2025-08-20"]),
        "account_type": ["Income", "Expense", "Income", "Expense", "Expense", "Expense"],
        "account": ["Sales", "705.140 · POOL", "Sales", "804 · Rent", "Meals", "Materials"],
        "acct_code": ["", "705", "", "804", "", ""],
        "name": ["Cust A", "Vendor A", "Cust B", "Landlord", "Rest A", "Vendor B"],
        "memo": ["", "", "", "", "Lunch with Nathan", "xnp materials"],
        "amount": [-1000, 200, -2000, 500, 100, 300]
    }
    df = pd.DataFrame(data)
    df["is_expense"] = df["account_type"] == "Expense"
    return df

def test_classify_transactions(sample_df):
    df = classify_transactions(sample_df)
    
    # Row 0: Income
    assert df.loc[0, "is_revenue"] == True
    
    # Row 1: 705 -> COGS
    assert df.loc[1, "expense_class"] == "COGS"
    
    # Row 3: 804 -> Overhead
    assert df.loc[3, "expense_class"] == "Overhead"
    
    # Row 4: No code, Expense -> Other
    assert df.loc[4, "expense_class"] == "Other"

def test_detect_addbacks(sample_df):
    df = classify_transactions(sample_df)
    df = detect_addbacks(df)
    
    # Row 4: "Lunch with Nathan" -> Addback
    assert df.loc[4, "sde_addback_flag"] == True
    assert "NATHAN" in df.loc[4, "sde_addback_reason"]
    
    # Row 5: "xnp materials" -> Addback
    assert df.loc[5, "sde_addback_flag"] == True
    assert "XNP" in df.loc[5, "sde_addback_reason"]
    
    # Row 1: Normal COGS -> False
    assert df.loc[1, "sde_addback_flag"] == False

def test_detect_addbacks_custom(sample_df):
    df = classify_transactions(sample_df)
    custom = ["vendor", "cust"]
    df = detect_addbacks(df, custom_tokens=custom)
    
    # Row 1: "Vendor A" (Expense) -> Should be flagged if it contains "vendor"
    # Row 1 Name="Vendor A". "vendor" in name.
    # It is COGS (Expense Class), is it "is_expense"? 
    # classify_transactions sets is_expense=True for COGS too.
    assert df.loc[1, "sde_addback_flag"] == True
    assert "CUSTOM:VENDOR" in df.loc[1, "sde_addback_reason"]

def test_compute_metrics(sample_df):
    df = classify_transactions(sample_df)
    df = detect_addbacks(df)
    metrics = compute_metrics(df)
    
    # Revenue: -(-1000) + -(-2000) = 3000
    assert metrics["revenue"] == 3000.0
    
    # COGS: Row 1 (200)
    assert metrics["cogs"] == 200.0
    
    # Overhead: Row 3 (500)
    assert metrics["overhead"] == 500.0
    
    # Other: Row 4 (100) + Row 5 (300) = 400
    assert metrics["other_expense"] == 400.0
    
    # Gross Profit: 3000 - 200 = 2800
    assert metrics["gross_profit"] == 2800.0
    
    # Net Profit: 2800 - 500 - 400 = 1900
    assert metrics["net_profit"] == 1900.0
    
    # Addbacks: Row 4 (100) + Row 5 (300) = 400
    assert metrics["addbacks"] == 400.0
    
    # SDE: 1900 + 400 = 2300
    assert metrics["sde"] == 2300.0

def test_get_owner_metrics(sample_df):
    """
    Acquisition: 2025-07-01
    Owner Revenue: Aug+
    Owner Expense: July+
    """
    df = classify_transactions(sample_df)
    df = detect_addbacks(df)
    
    # Call Owner Metrics
    # Current date covers all data
    metrics = get_owner_metrics(df, "2025-07-01", "2025-08-30")
    
    # Owner Revenue: Exclude July (-1000). Include Aug (-2000).
    # So Owner Rev = 2000.
    assert metrics["revenue"] == 2000.0
    
    # Owner Expenses:
    # COGS: Exclude July (200). Include Aug (None). -> 0
    # Overhead: Include July+ (500 in Aug). -> 500
    # Other: Include July+ (400 in Aug). -> 400
    # Total Exp = 900
    
    # Owner Net Profit: 2000 - 900 = 1100
    assert metrics["net_profit"] == 1100.0
    
    # Owner Addbacks: 400 (both in Aug)
    # SDE: 1100 + 400 = 1500
    assert metrics["sde"] == 1500.0
