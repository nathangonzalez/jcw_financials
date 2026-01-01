import pandas as pd
import datetime as dt
from src.account_view import build_account_summary, classify_row


def test_classify_row():
    # Test classification logic
    row1 = pd.Series({"account_type": "Income"})
    assert classify_row(row1) == "revenue"

    row2 = pd.Series({"account_type": "Cost of Goods Sold"})
    assert classify_row(row2) == "cogs"

    row3 = pd.Series({"account_type": "Expense"})
    assert classify_row(row3) == "overhead"

    row4 = pd.Series({"account_type": "Other Expense"})
    assert classify_row(row4) == "other"

    row5 = pd.Series({"account_type": "Liability"})
    assert classify_row(row5) == "other"


def test_build_account_summary():
    # Synthetic data
    data = [
        ("2025-07-10", "Revenue Account", "Income", 1000.0),
        ("2025-07-15", "COGS Account", "Cost of Goods Sold", -500.0),
        ("2025-08-05", "Overhead Account", "Expense", -200.0),
        ("2025-08-10", "Other Account", "Other Expense", -50.0),
    ]
    df = pd.DataFrame(data, columns=["date", "account", "account_type", "amount"])
    df["date"] = pd.to_datetime(df["date"])

    start = dt.date(2025, 7, 1)
    end = dt.date(2025, 12, 1)

    # Test without addback_accounts
    summary = build_account_summary(df, start, end)
    assert len(summary) == 4
    assert summary.loc[summary["account"] == "Revenue Account", "classification"].iloc[0] == "revenue"
    assert summary.loc[summary["account"] == "COGS Account", "classification"].iloc[0] == "cogs"
    assert summary.loc[summary["account"] == "Overhead Account", "classification"].iloc[0] == "overhead"
    assert summary.loc[summary["account"] == "Other Account", "classification"].iloc[0] == "other"
    assert summary["ytd_amount"].sum() == 250.0  # 1000 -500 -200 -50
    assert not summary["is_addback"].any()

    # Test with addback_accounts
    addbacks = {"COGS Account", "Overhead Account"}
    summary2 = build_account_summary(df, start, end, addback_accounts=addbacks)
    assert summary2.loc[summary2["account"] == "COGS Account", "is_addback"].iloc[0] == True
    assert summary2.loc[summary2["account"] == "Overhead Account", "is_addback"].iloc[0] == True
    assert summary2.loc[summary2["account"] == "Revenue Account", "is_addback"].iloc[0] == False
