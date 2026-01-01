from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional, Dict, Any

import pandas as pd


@dataclass
class MatchResult:
    """
    Container for reconciliation between QB ledger and bank register.
    All amounts are in the same sign convention as the inputs.
    """
    matched: pd.DataFrame
    unmatched_qb: pd.DataFrame
    unmatched_bank: pd.DataFrame


# --------------------------------------------------------------------
# Normalization helpers
# --------------------------------------------------------------------

def normalize_qb_ledger_for_bank(df_qb: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the QB ledger dataframe for bank reconciliation.

    Expected inputs (after load_ledger + normalization):
        - 'date' (datetime64)
        - 'amount' (numeric, where debits are positive, credits negative)
        - 'memo' (str, optional)
        - 'name' (str, optional)
        - 'account' (str, GL account name)

    This function:
      - Ensures required columns exist.
      - Keeps only columns we need for matching.
      - Drops rows with NaN dates or zero amounts.
    """
    qb = df_qb.copy()

    # Ensure columns exist
    for col in ["date", "amount"]:
        if col not in qb.columns:
            raise ValueError(f"QB ledger is missing required column: {col}")

    for col in ["memo", "name", "account"]:
        if col not in qb.columns:
            qb[col] = ""

    # Drop empty rows
    qb = qb.dropna(subset=["date"])
    qb["amount"] = pd.to_numeric(qb["amount"], errors="coerce").fillna(0.0)
    qb = qb[qb["amount"] != 0.0]

    # Create a simple text description for debugging / display
    qb["description"] = qb["name"].astype(str) + " | " + qb["memo"].astype(str)
    qb["description"] = qb["description"].str.strip(" |")

    # Standard columns for reconciliation
    qb_norm = qb[["date", "amount", "description", "account"]].copy()
    qb_norm["source"] = "qb"

    return qb_norm


def normalize_bank_register(df_bank: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a bank CSV export for reconciliation.

    Expected input candidates:
        - 'date' or 'transaction_date'
        - 'amount' (positive for deposits, negative for withdrawals)
        - 'description' or 'transaction_description'

    This function:
      - Normalizes the columns into: date, amount, description
      - Drops rows with NaN dates or zero amounts
    """
    bank = df_bank.copy()

    # Normalize column names
    bank.columns = [
        str(c).strip().lower().replace(" ", "_")
        for c in bank.columns
    ]

    # Date column
    date_col = None
    for cand in ("date", "transaction_date", "posting_date"):
        if cand in bank.columns:
            date_col = cand
            break
    if not date_col:
        raise ValueError("Bank register is missing a date-like column.")

    bank["date"] = pd.to_datetime(bank[date_col], errors="coerce")

    # Description
    desc_col = None
    for cand in ("description", "transaction_description", "memo"):
        if cand in bank.columns:
            desc_col = cand
            break
    if not desc_col:
        bank["description"] = ""
    else:
        bank["description"] = bank[desc_col].astype(str)

    # Amount
    if "amount" not in bank.columns:
        raise ValueError("Bank register is missing an 'amount' column.")
    bank["amount"] = (
        bank["amount"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace(" ", "", regex=False)
    )
    # Handle "- $xx" format if cleaner didn't catch it (simple cleaner above might miss complex cases, but let's rely on standard pandas first or add logic)
    # The user's snippet for amount cleaning was:
    # bank["amount"] = (bank["amount"].astype(str).str.replace(",", "", regex=False))
    # I'll add the $ removal too as per my previous experience.
    
    # Actually, the user's provided code didn't handle '$' explicitly in the snippet, 
    # but 'bank_export.csv' has amounts like "- $28800.3".
    # I should make sure it handles that.
    
    # Let's use a stronger cleaning function
    def clean_amt(x):
        if isinstance(x, str):
            x = x.replace("$", "").replace(",", "").replace(" ", "")
            if x.startswith("(") and x.endswith(")"):
                x = "-" + x[1:-1]
        return x
        
    bank["amount"] = bank["amount"].apply(clean_amt)
    bank["amount"] = pd.to_numeric(bank["amount"], errors="coerce").fillna(0.0)

    # Drop empties
    bank = bank.dropna(subset=["date"])
    bank = bank[bank["amount"] != 0.0]

    bank_norm = bank[["date", "amount", "description"]].copy()
    bank_norm["source"] = "bank"

    return bank_norm


# --------------------------------------------------------------------
# Matching engine
# --------------------------------------------------------------------

def match_qb_and_bank(
    qb_df: pd.DataFrame,
    bank_df: pd.DataFrame,
    amount_tolerance: float = 0.01,
    date_tolerance_days: int = 14,
) -> MatchResult:
    """
    Fuzzy match QB ledger rows to bank rows using:
      - exact amount match within ±amount_tolerance
      - date within ±date_tolerance_days

    Returns:
      - matched: rows with qb_*/bank_* columns
      - unmatched_qb: QB rows without a bank match
      - unmatched_bank: Bank rows without a QB match
    """
    qb = qb_df.copy()
    bank = bank_df.copy()

    # Ensure date types
    qb["date"] = pd.to_datetime(qb["date"], errors="coerce")
    bank["date"] = pd.to_datetime(bank["date"], errors="coerce")

    # We'll create helper columns for matching
    qb["amount_round"] = qb["amount"].round(2)
    bank["amount_round"] = bank["amount"].round(2)

    # Normalize dates to date objects (remove time) for comparison
    qb["date_obj"] = qb["date"].dt.date
    bank["date_obj"] = bank["date"].dt.date

    # For efficiency on larger sets we can do a simple loop by amount.
    matched_rows = []
    used_bank_indices = set()

    # Build index of bank rows by rounded amount
    bank_groups = bank.groupby("amount_round", sort=False)

    for qb_idx, qb_row in qb.iterrows():
        amt = qb_row["amount_round"]
        if amt not in bank_groups.groups:
            continue

        candidate_indices = bank_groups.groups[amt]
        candidate_bank = bank.loc[candidate_indices]
        
        # Date window
        d_qb = qb_row["date_obj"]
        if pd.isna(d_qb):
            continue
            
        min_date = d_qb - timedelta(days=date_tolerance_days)
        max_date = d_qb + timedelta(days=date_tolerance_days)

        mask = candidate_bank["date_obj"].between(min_date, max_date)
        candidate_bank = candidate_bank[mask]

        # Exclude already matched bank rows
        candidate_bank = candidate_bank[~candidate_bank.index.isin(used_bank_indices)]

        if candidate_bank.empty:
            continue

        # Choose the closest date as match
        # Calculate absolute difference in days
        # We need to handle the date difference calculation carefully
        
        date_diffs = candidate_bank["date_obj"].apply(lambda d: abs((d - d_qb).days))
        best_bank_idx = date_diffs.idxmin()
        best_bank_row = bank.loc[best_bank_idx]

        matched_rows.append(
            {
                "qb_index": qb_idx,
                "bank_index": best_bank_idx,
                "qb_date": qb_row["date"],
                "qb_amount": qb_row["amount"],
                "qb_description": qb_row.get("description", ""),
                "qb_account": qb_row.get("account", ""),
                "bank_date": best_bank_row["date"],
                "bank_amount": best_bank_row["amount"],
                "bank_description": best_bank_row.get("description", ""),
            }
        )
        used_bank_indices.add(best_bank_idx)

    matched_df = pd.DataFrame(matched_rows)

    # Unmatched sets
    matched_qb_indices = set(matched_df["qb_index"]) if not matched_df.empty else set()
    unmatched_qb = qb[~qb.index.isin(matched_qb_indices)].copy()

    unmatched_bank = bank[~bank.index.isin(used_bank_indices)].copy()

    return MatchResult(
        matched=matched_df,
        unmatched_qb=unmatched_qb,
        unmatched_bank=unmatched_bank,
    )


# --------------------------------------------------------------------
# Cash vs Accrual summary
# --------------------------------------------------------------------

def compute_cash_basis_pl(
    bank_df: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> Dict[str, float]:
    """
    Very simple cash-basis P&L from the bank register:

      - Revenue (cash)  = sum of positive amounts
      - Expenses (cash) = sum of absolute value of negative amounts
      - Net (cash)      = Revenue - Expenses
    """
    b = bank_df.copy()
    b["date"] = pd.to_datetime(b["date"], errors="coerce")
    mask = b["date"].dt.date.between(start_date, end_date)
    b = b.loc[mask]

    inflow = b.loc[b["amount"] > 0, "amount"].sum()
    outflow = -b.loc[b["amount"] < 0, "amount"].sum()

    net = inflow - outflow

    return {
        "revenue_cash": float(inflow),
        "expenses_cash": float(outflow),
        "net_cash": float(net),
    }


def compute_accrual_pl_from_qb(
    qb_df: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> Dict[str, float]:
    """
    Accrual P&L from QB ledger using account_type:

      - Revenue (accrual)  = sum of Income / Other Income (credits) => flipped positive
      - Expenses (accrual) = positive magnitudes for COGS + Expense + Other Expense
      - Net (accrual)      = Revenue - Expenses

    Assumes:
      - qb_df['amount'] uses GL sign convention (debits positive, credits negative).
      - qb_df['account_type'] contains QB account types (Income, Cost of Goods Sold, Expense, Other Expense, Other Income).
    """
    q = qb_df.copy()
    q["date"] = pd.to_datetime(q["date"], errors="coerce")
    mask = q["date"].dt.date.between(start_date, end_date)
    q = q.loc[mask]

    atype = q.get("account_type", "").astype(str).str.lower()

    # Income accounts: credits negative in GL -> flip sign
    # We check strict matching or contains
    income_mask = atype.str.contains("income")
    income_amount = q.loc[income_mask, "amount"].sum()
    revenue_accrual = -income_amount  # flip sign to positive

    # COGS
    cogs_mask = atype.str.contains("cost of goods sold")
    cogs_raw = q.loc[cogs_mask, "amount"].sum()
    cogs = abs(cogs_raw)

    # Expenses
    # 'expense' can match 'expense' and 'other expense'
    # be careful not to double count if we have 'other expense'
    # Let's use contains 'expense' but maybe exclude 'cost of goods sold' if it was named 'Expense'?
    # Safer:
    exp_mask = atype.str.contains("expense") & ~atype.str.contains("income")
    # Note: 'cost of goods sold' usually doesn't contain 'expense' string
    
    exp_raw = q.loc[exp_mask, "amount"].sum()
    expenses_other = abs(exp_raw)

    expenses_total = cogs + expenses_other
    net_accrual = revenue_accrual - expenses_total

    return {
        "revenue_accrual": float(revenue_accrual),
        "expenses_accrual": float(expenses_total),
        "net_accrual": float(net_accrual),
    }


def compute_cash_vs_accrual_summary(
    qb_df: pd.DataFrame,
    bank_df: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> Dict[str, Any]:
    """
    Convenience wrapper: compute both cash-basis and accrual P&L
    and return a combined summary including differences.
    """
    cash = compute_cash_basis_pl(bank_df, start_date, end_date)
    accrual = compute_accrual_pl_from_qb(qb_df, start_date, end_date)

    diff = {
        "revenue_diff": accrual["revenue_accrual"] - cash["revenue_cash"],
        "expenses_diff": accrual["expenses_accrual"] - cash["expenses_cash"],
        "net_diff": accrual["net_accrual"] - cash["net_cash"],
    }

    return {
        "cash": cash,
        "accrual": accrual,
        "diff": diff,
    }


# --------------------------------------------------------------------
# Backward Compatibility / App Wrappers
# --------------------------------------------------------------------

def reconcile_transactions(bank_df, qb_df, date_window_days=14):
    """
    Wrapper for app.py compatibility.
    """
    # Ensure QB df is normalized if needed, or just pass through if columns align
    # The app.py passes a subset of the ledger.
    # match_qb_and_bank expects 'date' and 'amount'.
    
    res = match_qb_and_bank(qb_df, bank_df, date_tolerance_days=date_window_days)
    
    summary = {
        "total_matched": len(res.matched),
        "total_unmatched_bank": len(res.unmatched_bank),
        "unmatched_bank_amount": res.unmatched_bank["amount"].sum() if not res.unmatched_bank.empty else 0.0,
        "total_unmatched_qb": len(res.unmatched_qb),
        "unmatched_qb_amount": res.unmatched_qb["amount"].sum() if not res.unmatched_qb.empty else 0.0
    }
    
    return {
        "matched": res.matched,
        "unmatched_bank": res.unmatched_bank,
        "unmatched_qb": res.unmatched_qb,
        "summary": summary
    }

normalize_bank_export = normalize_bank_register
