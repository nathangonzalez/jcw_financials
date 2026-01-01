# tests/test_billing.py
import datetime as dt
import pandas as pd

from src.billing import load_green_sheets, JobBillingConfig, compute_period_billing


def test_simple_billing_two_jobs():
    # Minimal synthetic green-sheet data to validate math
    raw = pd.DataFrame(
        {
            "job": ["Howard", "Howard", "Lynn"],
            "date": ["2025-11-05", "2025-11-10", "2025-11-07"],
            "amount": [10_000, 5_000, 20_000],
            "cost_type": ["material", "labor", "material"],
        }
    )

    gs = load_green_sheets(raw)

    configs = [
        JobBillingConfig(job="Howard", overhead_pct=0.10, profit_pct=0.05),
        JobBillingConfig(job="Lynn", overhead_pct=0.10, profit_pct=0.05),
    ]

    start = dt.date(2025, 11, 1)
    end = dt.date(2025, 11, 30)

    df = compute_period_billing(gs, configs, start, end)

    # Howard: base = 10k + 5k = 15k
    # OH 10% of 15k = 1.5k
    # Profit 5% of (15k + 1.5k) = 825
    # Total = 15000 + 1500 + 825 = 17,325
    howard = df[df["job"] == "Howard"].iloc[0]
    assert howard["materials"] == 10_000.0
    assert howard["labor"] == 5_000.0
    assert howard["supervision"] == 0.0
    assert howard["overhead_amount"] == 1_500.0
    assert howard["profit_amount"] == 825.0
    assert howard["invoice_total"] == 17_325.0

    # Lynn: base = 20k
    # OH 10% of 20k = 2k
    # Profit 5% of (20k + 2k) = 1.1k
    # Total = 20k + 2k + 1.1k = 23,100
    lynn = df[df["job"] == "Lynn"].iloc[0]
    assert lynn["materials"] == 20_000.0
    assert lynn["labor"] == 0.0
    assert lynn["supervision"] == 0.0
    assert lynn["overhead_amount"] == 2_000.0
    assert lynn["profit_amount"] == 1_100.0
    assert lynn["invoice_total"] == 23_100.0

    # Combined
    assert df["invoice_total"].sum() == 40_425.0
