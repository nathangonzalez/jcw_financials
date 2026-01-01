from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

import pandas as pd

Classification = Literal["revenue", "cogs", "overhead", "other"]

@dataclass
class AccountRow:
    account: str
    account_type: str
    classification: Classification
    ytd_amount: float
    is_addback: bool

def classify_row(row: pd.Series) -> Classification:
    # Use the same logic as classify_transactions, but for display.
    atype = str(row.get("account_type", "")).lower()
    if "income" in atype or "revenue" in atype:
        return "revenue"
    if "cost of goods sold" in atype:
        return "cogs"
    if "other expense" in atype:
        return "other"
    if "expense" in atype or "overhead" in atype:
        return "overhead"
    return "other"

def build_account_summary(
    df: pd.DataFrame,
    owner_period_start,
    current_date,
    addback_accounts: set[str] | None = None,
) -> pd.DataFrame:
    """
    Returns a dataframe with one row per account:
      account, account_type, classification, ytd_amount, is_addback
    """
    if addback_accounts is None:
        addback_accounts = set()

    # filter to owner period window
    if "date" in df.columns:
        mask = (df["date"] >= pd.to_datetime(owner_period_start)) & (
            df["date"] <= pd.to_datetime(current_date)
        )
        df = df.loc[mask].copy()

    # group by account + account_type
    grouped = (
        df.groupby(["account", "account_type"], dropna=False)["amount"]
        .sum()
        .reset_index()
    )

    grouped["classification"] = grouped.apply(classify_row, axis=1)
    grouped["ytd_amount"] = grouped["amount"].astype(float)
    grouped["is_addback"] = grouped["account"].astype(str).isin(addback_accounts)

    return grouped[["account", "account_type", "classification", "ytd_amount", "is_addback"]]
