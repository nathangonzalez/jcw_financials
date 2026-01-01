from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import re


# Common date patterns seen in exports
# - MM/DD/YYYY (optionally with a time suffix)
# - YYYY-MM-DD (optionally with a time or "T" suffix)
# - YYYY/MM/DD (optionally with a time suffix)
_MDY_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}($|\s)")
_YMD_RE = re.compile(r"^\d{4}-\d{2}-\d{2}($|\s|T)")
_YMD_SLASH_RE = re.compile(r"^\d{4}/\d{1,2}/\d{1,2}($|\s)")


def parse_date_series(s: pd.Series) -> pd.Series:
    """Parse mixed-format date strings into tz-naive pandas datetimes.

    This avoids pandas falling back to slow/ambiguous dateutil inference.
    Supports common QB exports:
    - MM/DD/YYYY
    - YYYY-MM-DD (optionally with a time suffix)
    """
    s_str = s.astype(str).str.strip()

    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")

    mdy = s_str.str.match(_MDY_RE)
    if mdy.any():
        # Strip any time component
        out.loc[mdy] = pd.to_datetime(
            s_str.loc[mdy].str.split().str[0],
            format="%m/%d/%Y",
            errors="coerce",
        )

    ymd = s_str.str.match(_YMD_RE)
    if ymd.any():
        # allow "YYYY-MM-DD ..." or "YYYY-MM-DDT..." by slicing first 10 chars
        out.loc[ymd] = pd.to_datetime(
            s_str.loc[ymd].str.slice(0, 10),
            format="%Y-%m-%d",
            errors="coerce",
        )

    ymd_slash = s_str.str.match(_YMD_SLASH_RE)
    if ymd_slash.any():
        out.loc[ymd_slash] = pd.to_datetime(
            s_str.loc[ymd_slash].str.split().str[0],
            format="%Y/%m/%d",
            errors="coerce",
        )

    remaining = out.isna()
    if remaining.any():
        # Prefer pandas' mixed parser if available (avoids "Could not infer format" warning)
        try:
            out.loc[remaining] = pd.to_datetime(s_str.loc[remaining], format="mixed", errors="coerce")
        except TypeError:
            out.loc[remaining] = pd.to_datetime(s_str.loc[remaining], errors="coerce")

    # Force tz-naive
    try:
        out = out.dt.tz_localize(None)
    except Exception:
        pass

    return out

def normalize_ledger_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Standardize column names (lowercase, underscores).
    - Drop the 'unnamed: x' junk columns from the Excel export.
    - Remove duplicate columns (keep the first occurrence).
    """
    df = df.copy()

    # 1) Normalize names: strip + lowercase + spaces -> underscores
    df.columns = [
        str(c).strip().lower().replace(" ", "_")
        for c in df.columns
    ]

    # 2) Drop the 'unnamed: x' noise columns, and explicit 'nan' strings
    keep_cols = [
        c for c in df.columns 
        if not c.startswith("unnamed") and c != "nan" and c != ""
    ]
    df = df[keep_cols].copy()

    # 3) Drop duplicate column names (keep the first occurrence)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    return df

def load_ledger(uploaded_file: Any) -> pd.DataFrame:
    """
    Load a QuickBooks CSV/XLSX ledger export and normalize it into a usable
    transaction table.

    This version is robust to QB exports where the *first* row is not the real
    header (e.g. it contains sample data or a funky title row). It:

    - Reads the file with header=None.
    - Scans the first ~20 rows for a row containing both 'Date' and 'Account'.
    - Uses that row as the header.
    - Drops rows above the header.
    - Normalizes column names.
    - Ensures we have: date, account, account_type, name, memo, amount.
    """
    name = getattr(uploaded_file, "name", "ledger").lower()
    suffix = Path(name).suffix

    # 1) Read raw file, no header
    if suffix == ".csv":
        # Try encodings
        encodings = ["utf-8", "cp1252", "latin1"]
        raw = None
        for enc in encodings:
            try:
                uploaded_file.seek(0)
                raw = pd.read_csv(uploaded_file, header=None, dtype=str, encoding=enc, sep=',', engine='python', on_bad_lines='skip')
                break
            except UnicodeDecodeError:
                continue
        
        if raw is None:
             # If all else fails, try one last time with error replacement
             uploaded_file.seek(0)
             raw = pd.read_csv(uploaded_file, header=None, dtype=str, encoding="utf-8", encoding_errors="replace", sep=',', engine='python', on_bad_lines='skip')

    elif suffix in {".xlsx", ".xls"}:
        raw = pd.read_excel(uploaded_file, sheet_name=0, header=None, dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    # 2) Find the header row: a row containing 'date' and 'account'
    header_row_idx = None
    for i in range(min(30, len(raw))):
        row_vals = raw.iloc[i].astype(str).str.strip().str.lower()
        if ("date" in row_vals.values) and ("account" in row_vals.values):
            header_row_idx = i
            break

    if header_row_idx is None:
        # Fallback: assume first row is header
        header = raw.iloc[0]
        df = raw.iloc[1:].copy()
    else:
        header = raw.iloc[header_row_idx]
        df = raw.iloc[header_row_idx + 1 :].copy()

    # 3) Apply header
    df.columns = header

    # 4) Normalize column names using our helper (this drops junk columns)
    df = normalize_ledger_columns(df)

    # 5) Drop fully empty rows (now that junk columns are gone)
    df = df.dropna(how="all")

    # 6) Pick a date column
    date_col = None
    for cand in ("date", "txn_date", "transaction_date", "posting_date"):
        if cand in df.columns:
            date_col = cand
            break

    if date_col:
        df["date"] = parse_date_series(df[date_col])
    else:
        df["date"] = pd.NaT

    # Drop rows with no valid date
    df = df.dropna(subset=["date"])

    # 7) Ensure numeric amount
    def clean_num(x):
        if isinstance(x, str):
            return x.replace(",", "").replace("$", "").strip()
        return x

    if "amount" in df.columns:
        # Clean commas if present
        df["amount"] = df["amount"].apply(clean_num)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    else:
        # Clean commas from debit/credit columns
        raw_debit = df.get("debit", pd.Series(dtype=str))
        if hasattr(raw_debit, "apply"):
             raw_debit = raw_debit.apply(clean_num)
        
        raw_credit = df.get("credit", pd.Series(dtype=str))
        if hasattr(raw_credit, "apply"):
             raw_credit = raw_credit.apply(clean_num)

        debit = pd.to_numeric(raw_debit, errors="coerce").fillna(0.0)
        credit = pd.to_numeric(raw_credit, errors="coerce").fillna(0.0)
        df["amount"] = debit - credit

    # 8) Ensure required text columns exist
    for col in ("account", "account_type", "name", "memo"):
        if col not in df.columns:
            df[col] = ""

    # Stable row id for UI selection / reconciliation
    # (kept as string to avoid Arrow dtype edge cases)
    df["_row_id"] = pd.Series(range(len(df)), index=df.index).astype(str)

    return df
