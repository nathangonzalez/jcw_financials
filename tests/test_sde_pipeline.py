import datetime as dt
import pandas as pd

from src.business_logic import classify_transactions, detect_addbacks, get_owner_metrics


def test_sde_pipeline():
    # Synthetic ledger data: July and August
    # Amounts follow QB convention: Debits positive, Credits negative
    data = [
        # July: Expenses included, revenue excluded
        ("2025-07-10", "Materials Expense", "Cost of Goods Sold", "Vendor X", "Materials", 1000.0),  # COGS (debit)
        ("2025-07-15", "Construction Income", "Income", "Customer A", "Job 1", -5000.0),  # Revenue, but pre-owner_revenue_start (credit)

        # August: Revenue included, expenses included
        ("2025-08-05", "Construction Income", "Income", "Customer B", "Job 2", -3000.0),  # Revenue (credit)
        ("2025-08-10", "Materials Expense", "Cost of Goods Sold", "Vendor Y", "More Materials", 1500.0),  # COGS (debit)
        ("2025-08-15", "Rent", "Expense", "Landlord", "Monthly Rent", 2000.0),  # Overhead (debit)
        ("2025-08-20", "Owner Salary", "Expense", "Nathan", "Owner Pay", 4000.0),  # Overhead + Addback (debit)
    ]
    df = pd.DataFrame(data, columns=["date", "account", "account_type", "name", "memo", "amount"])
    df["date"] = pd.to_datetime(df["date"])

    # Classify and detect addbacks
    df = classify_transactions(df)
    df = detect_addbacks(df, custom_tokens=[])

    # Assert addback flagged
    assert df.loc[5, "sde_addback_flag"] == True  # Nathan

    # Get owner metrics
    owner_period_start = dt.date(2025, 7, 1)
    current_date = dt.date(2025, 12, 1)  # Covers all
    owner_revenue_start = dt.date(2025, 8, 1)

    metrics = get_owner_metrics(df, owner_period_start, current_date, owner_revenue_start)

    # Revenue: Only Aug (-3000) -> -(-3000) = 3000. July (-5000) excluded.
    assert metrics["revenue"] == 3000.0

    # COGS: Only Aug (1500), July (1000) is legacy
    assert metrics["cogs"] == 1500.0

    # Legacy COGS: July (1000)
    assert metrics["legacy_cogs"] == 1000.0

    # Overhead: Aug Rent (2000) + Owner Salary (4000) = 6000
    assert metrics["overhead"] == 6000.0

    # Other Expense: 0
    assert metrics["other_expense"] == 0.0

    # Gross Profit: 3000 - 1500 = 1500
    assert metrics["gross_profit"] == 1500.0

    # Net Profit: 3000 - (1500 + 6000 + 0) = 3000 - 7500 = -4500
    assert metrics["net_profit"] == -4500.0

    # Addbacks: Owner Salary (4000)
    assert metrics["addbacks"] == 4000.0

    # SDE: -4500 + 4000 = -500
    assert metrics["sde"] == -500.0


def test_account_level_addbacks():
    # Use the same synthetic data
    # Amounts follow QB convention: Debits positive, Credits negative
    data = [
        # July: Expenses included, revenue excluded
        ("2025-07-10", "Materials Expense", "Cost of Goods Sold", "Vendor X", "Materials", 1000.0),  # COGS (debit)
        ("2025-07-15", "Construction Income", "Income", "Customer A", "Job 1", -5000.0),  # Revenue, but pre-owner_revenue_start (credit)

        # August: Revenue included, expenses included
        ("2025-08-05", "Construction Income", "Income", "Customer B", "Job 2", -3000.0),  # Revenue (credit)
        ("2025-08-10", "Materials Expense", "Cost of Goods Sold", "Vendor Y", "More Materials", 1500.0),  # COGS (debit)
        ("2025-08-15", "Rent", "Expense", "Landlord", "Monthly Rent", 2000.0),  # Overhead (debit)
        ("2025-08-20", "Owner Salary", "Expense", "Nathan", "Owner Pay", 4000.0),  # Overhead + Addback (debit)
    ]
    df = pd.DataFrame(data, columns=["date", "account", "account_type", "name", "memo", "amount"])
    df["date"] = pd.to_datetime(df["date"])

    # Classify and detect addbacks
    df = classify_transactions(df)
    df = detect_addbacks(df, custom_tokens=[])

    owner_period_start = dt.date(2025, 7, 1)
    current_date = dt.date(2025, 12, 1)
    owner_revenue_start = dt.date(2025, 8, 1)

    # Test without account overrides
    metrics_none = get_owner_metrics(df, owner_period_start, current_date, owner_revenue_start)
    assert metrics_none["sde"] == -500.0  # Updated calculation

    # Test with account override: add "Rent" as addback (normally not, but override)
    # Rent is 2000, so addback += 2000, SDE increases by 2000
    addback_overrides = {"Rent"}
    metrics_override = get_owner_metrics(df, owner_period_start, current_date, owner_revenue_start, addback_account_overrides=addback_overrides)
    assert metrics_override["sde"] == -500.0 + 2000.0  # -500 + 2000 = 1500


def test_legacy_cogs_excluded_from_owner_metrics():
    import datetime as dt
    from src.business_logic import get_owner_metrics

    # Synthetic data:
    # - July COGS: 100,000 (should be legacy)
    # - Aug COGS:  200,000 (owner)
    # - Aug Revenue: 500,000
    # - Overhead:  50,000 (July+; all owner)

    data = [
        # July legacy COGS (pre-owner revenue)
        {"date": "2025-07-10", "amount": 100000.0, "account_type": "Cost of Goods Sold", "is_cogs": True, "is_overhead": False, "is_other_expense": False, "is_revenue": False},
        # Aug owner COGS
        {"date": "2025-08-10", "amount": 200000.0, "account_type": "Cost of Goods Sold", "is_cogs": True, "is_overhead": False, "is_other_expense": False, "is_revenue": False},
        # Aug Revenue (QB sign: negative)
        {"date": "2025-08-10", "amount": -500000.0, "account_type": "Income", "is_revenue": True, "is_cogs": False, "is_overhead": False, "is_other_expense": False},
        # July Overhead (owner)
        {"date": "2025-07-15", "amount": 50000.0, "account_type": "Expense", "is_overhead": True, "is_cogs": False, "is_other_expense": False, "is_revenue": False},
    ]
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    owner_period_start = dt.date(2025, 7, 1)
    owner_revenue_start = dt.date(2025, 8, 1)
    current_date = dt.date(2025, 12, 31)

    metrics = get_owner_metrics(
        df,
        owner_period_start=owner_period_start,
        current_date=current_date,
        owner_revenue_start=owner_revenue_start,
    )

    revenue = metrics["revenue"]
    cogs = metrics["cogs"]
    overhead = metrics["overhead"]
    net_profit = metrics["net_profit"]
    legacy_cogs = metrics.get("legacy_cogs", 0.0)

    # Expectations:
    # Revenue: 500,000 (flip sign from -500,000)
    assert round(revenue, 2) == 500000.0

    # Owner COGS: 200,000 (Aug only)
    assert round(cogs, 2) == 200000.0

    # Legacy COGS: 100,000 (July only)
    assert round(legacy_cogs, 2) == 100000.0

    # Overhead: 50,000 (July+)
    assert round(overhead, 2) == 50000.0

    # Net Profit = revenue - (owner COGS + overhead)
    #            = 500,000 - (200,000 + 50,000) = 250,000
    assert round(net_profit, 2) == 250000.0


def test_legacy_july_job_cost_prefix_excluded_but_overhead_included():
    import datetime as dt
    from src.business_logic import classify_transactions, detect_addbacks, get_owner_metrics

    # QB sign convention: income negative, expenses positive
    data = [
        # July window (pre-owner revenue start)
        ("2025-07-05", "705.140 · POOL", "Expense", "Vendor", "Legacy job cost", 1000.0),
        ("2025-07-06", "Rent", "Expense", "Landlord", "July rent", 500.0),
        ("2025-07-07", "Interest", "Other Expense", "Bank", "July interest", 200.0),
        # Aug+ window
        ("2025-08-02", "Construction Income", "Income", "Cust", "Job", -3000.0),
        ("2025-08-03", "705.140 · POOL", "Expense", "Vendor", "Owner job cost", 700.0),
        ("2025-08-04", "Rent", "Expense", "Landlord", "Aug rent", 400.0),
        ("2025-08-05", "Interest", "Other Expense", "Bank", "Aug interest", 50.0),
    ]
    df = pd.DataFrame(data, columns=["date", "account", "account_type", "name", "memo", "amount"])
    df["date"] = pd.to_datetime(df["date"])

    df = classify_transactions(df)
    df = detect_addbacks(df)

    owner_period_start = dt.date(2025, 7, 1)
    owner_revenue_start = dt.date(2025, 8, 1)
    current_date = dt.date(2025, 8, 31)

    metrics = get_owner_metrics(
        df,
        owner_period_start=owner_period_start,
        current_date=current_date,
        owner_revenue_start=owner_revenue_start,
        exclude_legacy_july_job_costs=True,
        legacy_job_cost_prefixes={"705"},
    )

    # July 705.* should NOT impact overhead/net profit
    assert metrics["legacy_july_excluded_job_cost"] == 1000.0
    assert metrics["legacy_july_included_overhead"] == 500.0

    # July other expense excluded (legacy window is overhead only)
    assert metrics["other_expense"] == 50.0

    # Aug+ expenses still included
    # Note: 705.* is treated as COGS via account prefix classification.
    assert metrics["cogs"] == 700.0
    assert metrics["overhead"] == 500.0 + 400.0  # July rent + Aug rent
    assert metrics["revenue"] == 3000.0

    # Net profit = 3000 - (cogs(0) + overhead(1600) + other(50)) = 1350
    assert metrics["net_profit"] == 1350.0
