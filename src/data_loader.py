import pandas as pd
import numpy as np
from src.utils import read_csv_flex, parse_money, normalize_dates, extract_account_code

def load_ledger(file):
    """
    Loads the single ledger export (CSV or Excel).
    Normalizes columns to canonical schema.
    Smartly handles Excel sheets (skips empty 'Export Tips').
    """
    if file is None:
        return None

    # Custom Excel Loading Logic
    if hasattr(file, "name") and (file.name.lower().endswith(".xlsx") or file.name.lower().endswith(".xls")):
        try:
            # Try standard Sheet1 first
            df = pd.read_excel(file, sheet_name="Sheet1")
        except Exception:
            df = None
            
        # Fallback: Scan sheets if Sheet1 failed or is empty
        if df is None or df.empty:
            try:
                xls = pd.ExcelFile(file)
                for sheet in xls.sheet_names:
                    # Skip known bad sheets
                    if "tips" in sheet.lower():
                        continue
                        
                    temp_df = pd.read_excel(file, sheet_name=sheet)
                    # Check if it looks like data (has rows and maybe a Date column or similar)
                    # We don't know exact col names yet, but check for non-empty
                    if not temp_df.empty and len(temp_df.columns) > 3:
                        df = temp_df
                        break
            except Exception:
                pass
                
        if df is None:
             # Final fallback to default
             df = read_csv_flex(file)
    else:
        # CSV
        df = read_csv_flex(file)

    if df is None:
        return None

    df = df.copy()
    
    # Handle potential header issues (e.g. report titles in first few rows)
    # If columns are Ints, or don't contain "date", look for header
    # Convert to string first to avoid AttributeError
    cols_str = df.columns.astype(str).str.strip().str.lower()
    
    if not any("date" in c for c in cols_str):
        # Try finding the header row
        # Scan first 20 rows
        for i, row in df.head(20).iterrows():
            row_str = row.astype(str).str.strip().str.lower()
            # Look for "date" AND ("account" OR "amount" OR "debit" OR "credit" OR "type")
            has_date = row_str.str.contains("date", case=False).any()
            has_other = row_str.str.contains("account|amount|debit|credit|type|name", regex=True, case=False).any()
            
            if has_date and has_other:
                # Found header at index i
                # Reload with header at i+1 (since 0-indexed, but read_csv header param is 0-based row index)
                # Actually, we can just set columns and slice
                df.columns = row.astype(str).str.strip().str.lower()
                df = df.iloc[i+1:].reset_index(drop=True)
                break
    else:
        df.columns = cols_str

    # Rename map based on "Transaction Detail by Account" or "Custom Detail Transaction Report"
    # Typical QB export columns: Date, Type, Num, Name, Memo, Account, Clr, Split, Amount, Balance
    # Sometimes "Debit", "Credit" instead of Amount.
    # "Account Type" might be present if customized, or we might need to infer it? 
    # Prompt says "Account Type" is a column.
    
    # Map (target_col: [list of possible source cols])
    map_rules = {
        "date": ["date", "trans date", "transaction date", "txn date"],
        "txn_type": ["type", "transaction type", "txn type"],
        "num": ["num", "ref", "reference", "no."],
        "name": ["name", "source name", "payee", "name address"],
        "memo": ["memo", "description"],
        "account": ["account"],
        "account_type": ["account type", "type of account", "acct type"],
        "class": ["class"],
        "debit": ["debit"],
        "credit": ["credit"],
        "amount": ["amount", "balance", "total"] # Balance is usually running balance, but sometimes used as amount? Be careful.
        # Actually "Amount" is standard. "Balance" is usually not what we want (running total).
    }
    
    # Flatten to source -> target
    col_map = {}
    for target, sources in map_rules.items():
        for s in sources:
            col_map[s] = target
            
    # Apply renaming if columns exist
    new_cols = {}
    for col in df.columns:
        col_clean = col.strip().lower()
        # Direct match (already canonical)
        if col_clean in map_rules.keys():
            continue
        # Dictionary match
        if col_clean in col_map:
            new_cols[col] = col_map[col_clean]
            
    df.rename(columns=new_cols, inplace=True)
    
    # Special case: If 'amount' missing, but 'debit'/'credit' missing too, check for 'amount' aliases again carefully?
    # The loop above handles it.
    
    df.rename(columns=new_cols, inplace=True)
    
    # Deduplicate columns (keep first)
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Ensure required columns exist (fill missing with None/NaN)
    required_cols = ["date", "txn_type", "num", "name", "memo", "account", "account_type", "class", "debit", "credit", "amount"]
    for c in required_cols:
        if c not in df.columns:
            df[c] = np.nan
            
    # Normalize Date
    df["date"] = normalize_dates(df["date"])
    
    # Force datetime64[ns] to satisfy PyArrow
    # Errors='coerce' here again just to be safe against any leftover objects
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    # Filter rows with invalid dates (e.g. blank lines, total rows)
    df = df.dropna(subset=["date"])
    
    # Normalize Money
    for c in ["amount", "debit", "credit"]:
        df[c] = parse_money(df[c])
        
    # If Amount is missing but Debit/Credit present
    # QB Logic: Debit is usually positive in expense, Credit positive in income?
    # Or simply Amount = Debit - Credit?
    # Prompt says: "In this ledger, income lines often have negative amount (credits). Define revenue_amount = -amount"
    # So we assume 'amount' column exists or we calculate it. 
    # Usually QB export has 'Amount' (signed).
    
    if df["amount"].isna().all() and (not df["debit"].isna().all() or not df["credit"].isna().all()):
        df["debit"] = df["debit"].fillna(0)
        df["credit"] = df["credit"].fillna(0)
        # Standard Accounting: Asset/Exp Debit+, Liab/Eq/Inc Credit+
        # But in a transaction report, "Amount" column is usually signed from perspective of the account?
        # Let's assume Amount is provided or calculated as Debit - Credit (standard generic approach)
        df["amount"] = df["debit"] - df["credit"]

    # Fill strings
    for c in ["txn_type", "name", "memo", "account", "account_type", "class"]:
        df[c] = df[c].fillna("").astype(str)

    # Extract Account Code
    df["acct_code"] = df["account"].apply(extract_account_code)

    return df
