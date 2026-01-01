import datetime as dt

import pandas as pd

from src.kpi_lab import compute_monthly_kpis


def test_compute_monthly_kpis_margins_and_mom_deltas():
    owner_revenue_start = dt.date(2025, 8, 1)

    # QB sign convention: income negative, expenses positive
    rows = [
        # July (pre-owner revenue): revenue should be excluded
        ("2025-07-10", "Revenue", -999.0, True, False),
        ("2025-07-11", "Overhead", 100.0, False, False),
        # Aug
        ("2025-08-05", "Revenue", -1000.0, True, False),
        ("2025-08-06", "COGS", 200.0, False, False),
        ("2025-08-07", "Overhead", 100.0, False, False),
        ("2025-08-08", "Overhead", 50.0, False, True),  # addback
        # Sep
        ("2025-09-05", "Revenue", -2000.0, True, False),
        ("2025-09-06", "COGS", 400.0, False, False),
        ("2025-09-07", "Overhead", 200.0, False, False),
        ("2025-09-08", "Overhead", 0.0, False, True),
    ]

    df = pd.DataFrame(rows, columns=["date", "classification", "amount", "is_revenue", "sde_addback_flag"])
    df["date"] = pd.to_datetime(df["date"])

    monthly = compute_monthly_kpis(df, owner_revenue_start=owner_revenue_start)

    # Aug expectations
    aug = monthly.loc[monthly["month_str"] == "Aug 2025"].iloc[0]
    assert aug["revenue"] == 1000.0
    assert aug["cogs"] == 200.0
    assert aug["overhead"] == 150.0
    assert aug["other_expense"] == 0.0
    assert aug["net_profit"] == 650.0
    assert aug["addbacks"] == 50.0
    assert aug["sde"] == 700.0

    # Net profit formula matches dashboard: gross_profit - overhead - other_expense
    assert aug["net_profit"] == aug["gross_profit"] - aug["overhead"] - aug["other_expense"]

    # Margins
    assert round(float(aug["gross_margin_pct"]), 4) == 0.8
    assert round(float(aug["net_margin_pct"]), 4) == 0.65
    assert round(float(aug["sde_margin_pct"]), 4) == 0.7
    assert round(float(aug["overhead_pct"]), 4) == 0.15
    assert round(float(aug["cogs_pct"]), 4) == 0.2

    # Sep expectations + MoM deltas from Aug
    sep = monthly.loc[monthly["month_str"] == "Sep 2025"].iloc[0]
    assert sep["revenue"] == 2000.0
    assert sep["net_profit"] == 1400.0  # 2000 - (400 + 200)
    assert sep["sde"] == 1400.0

    assert sep["revenue_mom_delta"] == 1000.0
    assert round(float(sep["revenue_mom_pct"]), 4) == 1.0

    assert sep["net_profit_mom_delta"] == 750.0
    assert round(float(sep["net_profit_mom_pct"]), 4) == round(750.0 / 650.0, 4)

    assert sep["sde_mom_delta"] == 700.0
    assert round(float(sep["sde_mom_pct"]), 4) == 1.0
