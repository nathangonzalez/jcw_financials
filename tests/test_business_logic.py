import datetime as dt

import pandas as pd
import pytest

from src.business_logic import (
    classify_transactions,
    detect_addbacks,
    get_owner_metrics,
    compute_legacy_overhead_addins,
    apply_legacy_overhead_addins,
)


@pytest.fixture
def sample_df_qb_signs() -> pd.DataFrame:
    """Small synthetic ledger using QB sign convention:

    - Income is negative (credits)
    - Expenses are positive (debits)
    """
    data = [
        ("2025-07-15", "Sales", "Income", "Cust A", "Inv 1", -1000.0),
        ("2025-07-20", "Materials", "Cost of Goods Sold", "Vendor A", "Materials", 200.0),
        ("2025-08-05", "Sales", "Income", "Cust B", "Inv 2", -2000.0),
        ("2025-08-10", "Rent", "Expense", "Landlord", "Rent", 500.0),
        ("2025-08-15", "Meals", "Expense", "Rest A", "Lunch with Nathan", 100.0),
        ("2025-08-20", "Other", "Other Expense", "Vendor B", "xnp materials", 300.0),
    ]
    df = pd.DataFrame(data, columns=["date", "account", "account_type", "name", "memo", "amount"])
    df["date"] = pd.to_datetime(df["date"])
    return df


def test_classify_transactions_flags(sample_df_qb_signs: pd.DataFrame):
    df = classify_transactions(sample_df_qb_signs)

    # Income rows
    assert bool(df.loc[0, "is_revenue"]) is True
    assert bool(df.loc[2, "is_revenue"]) is True

    # COGS row
    assert bool(df.loc[1, "is_cogs"]) is True

    # Expense rows
    assert bool(df.loc[3, "is_overhead"]) is True
    assert bool(df.loc[4, "is_overhead"]) is True

    # Other expense row
    assert bool(df.loc[5, "is_other_expense"]) is True


def test_detect_addbacks_tokens(sample_df_qb_signs: pd.DataFrame):
    df = classify_transactions(sample_df_qb_signs)
    df = detect_addbacks(df)

    # Nathan token
    assert bool(df.loc[4, "sde_addback_flag"]) is True
    assert "token=nathan" in df.loc[4, "sde_addback_reason"]

    # xnp token
    assert bool(df.loc[5, "sde_addback_flag"]) is True
    assert "token=xnp" in df.loc[5, "sde_addback_reason"]


def test_get_owner_metrics_owner_revenue_start(sample_df_qb_signs: pd.DataFrame):
    df = classify_transactions(sample_df_qb_signs)
    df = detect_addbacks(df)

    owner_period_start = dt.date(2025, 7, 1)
    owner_revenue_start = dt.date(2025, 8, 1)
    current_date = dt.date(2025, 8, 31)

    metrics = get_owner_metrics(
        df,
        owner_period_start=owner_period_start,
        current_date=current_date,
        owner_revenue_start=owner_revenue_start,
    )

    # Revenue should include Aug only: -(-2000) = 2000
    assert metrics["revenue"] == 2000.0

    # COGS should include Aug only (none in this dataset)
    assert metrics["cogs"] == 0.0

    # Legacy COGS is July 200
    assert metrics["legacy_cogs"] == 200.0

    # Overhead includes Aug expense rows: 500 + 100 = 600
    assert metrics["overhead"] == 600.0

    # Other expense includes Aug other expense: 300 (July other expense is excluded by owner_revenue_start)
    assert metrics["other_expense"] == 300.0

    # Net profit = 2000 - (0 + 600 + 300) = 1100
    assert metrics["net_profit"] == 1100.0

    # Addbacks: both addback rows are expenses (positive) => 100 + 300 = 400
    assert metrics["addbacks"] == 400.0

    # SDE = 1100 + 400 = 1500
    assert metrics["sde"] == 1500.0


def test_classify_transactions_cogs_by_account_prefix():
    df = pd.DataFrame(
        [
            ("2025-08-10", "705.120 · LIGHTING", "Expense", "Vendor", "Lighting", 100.0),
            ("2025-08-10", "865 · Rent Expense", "Expense", "Landlord", "Rent", 50.0),
            ("2025-08-10", "Construction Income", "Income", "Customer", "Inv", -500.0),
        ],
        columns=["date", "account", "account_type", "name", "memo", "amount"],
    )
    df["date"] = pd.to_datetime(df["date"])

    out = classify_transactions(df)

    # lighting row => COGS by 705 prefix
    assert bool(out.loc[0, "is_cogs"]) is True
    assert out.loc[0, "classification"] == "COGS"

    # rent row => overhead (not in cogs prefixes)
    assert bool(out.loc[1, "is_overhead"]) is True
    assert out.loc[1, "classification"] == "Overhead"

    # income row => revenue
    assert bool(out.loc[2, "is_revenue"]) is True
    assert out.loc[2, "classification"] == "Revenue"


def test_compute_legacy_overhead_addins_selected_accounts_only():
    df = pd.DataFrame(
        [
            ("2025-07-10", "865 · Rent Expense", "Expense", "Landlord", "Rent", 50.0),
            ("2025-07-11", "865 · Rent Expense", "Expense", "Landlord", "Rent", 25.0),
            ("2025-07-12", "900 · Misc Expense", "Expense", "Vendor", "Stuff", 10.0),
            ("2025-07-13", "Other", "Other Expense", "Vendor", "Other", 99.0),
            ("2025-08-01", "865 · Rent Expense", "Expense", "Landlord", "Aug rent", 123.0),
        ],
        columns=["date", "account", "account_type", "name", "memo", "amount"],
    )
    df["date"] = pd.to_datetime(df["date"])
    df = classify_transactions(df)

    legacy_total = compute_legacy_overhead_addins(
        df,
        legacy_start=dt.date(2025, 7, 1),
        legacy_end=dt.date(2025, 7, 31),
        included_accounts={"865 · Rent Expense"},
    )

    # Includes only July overhead rows for the selected account (50 + 25)
    assert legacy_total == 75.0


def test_apply_legacy_overhead_addins_adjusts_net_and_sde():
    base = {
        "revenue": 1000.0,
        "cogs": 100.0,
        "overhead": 200.0,
        "other_expense": 50.0,
        "addbacks": 10.0,
        "gross_profit": 900.0,
        "net_profit": 650.0,
        "sde": 660.0,
    }

    out = apply_legacy_overhead_addins(base, legacy_overhead_included_total=75.0)

    assert out["overhead"] == 275.0
    assert out["net_profit"] == 575.0
    assert out["sde"] == 585.0
