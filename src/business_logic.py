import pandas as pd
import numpy as np

# ---------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------
COGS_CODES = {"701", "705", "706", "707", "708", "710", "716"}

def is_overhead(acct_code, acct_name):
    """
    Overhead classification logic:
    - Starts with "8"
    - "711" (IRA/benefits)
    - Starts with "74" (uniforms, small tools)
    - "66910" (bank charges)
    - Name "Miscellaneous"
    """
    if not acct_code:
        return acct_name.lower() == "miscellaneous"
        
    if acct_code.startswith("8"): return True
    if acct_code == "711": return True
    if acct_code.startswith("74"): return True
    if acct_code == "66910": return True
    if acct_name.lower() == "miscellaneous": return True
    
    return False

# ---------------------------------------------------------
# CLASSIFICATION
# ---------------------------------------------------------

def classify_transactions(df):
    """
    Classifies rows into:
    - Revenue (Income)
    - Expense Class: COGS, Overhead, Other
    """
    if df is None:
        return None
        
    df = df.copy()
    
    # Initialize columns (ensure they exist even if df is empty)
    if "is_revenue" not in df.columns:
        df["is_revenue"] = False
    if "expense_class" not in df.columns:
        df["expense_class"] = None
        
    if df.empty:
        return df
    
    # Initialize columns
    df["is_revenue"] = False
    df["expense_class"] = None # COGS, Overhead, Other
    
    # Pre-compute lowercase columns
    acct_type = df["account_type"].str.lower()
    acct_name = df["account"].str.lower()
    
    # 1. Revenue Detection
    # Logic: 
    # - Account Type contains "income"
    # - OR Account Name contains "income", "sales", "revenue"
    # - OR Account Code starts with "4" (standard Chart of Accounts)
    df["is_revenue"] = (
        acct_type.str.contains("income") | 
        acct_name.str.contains("income") | 
        acct_name.str.contains("sales") | 
        acct_name.str.contains("revenue") |
        (df["acct_code"].str.startswith("4", na=False))
    )
    
    # 2. Expense Detection (Preliminary)
    # Logic:
    # - Account Type contains "expense", "cogs", "cost of goods"
    # - OR Account Code starts with 5, 6, 7, 8, 9
    # - OR Account Name contains "expense"
    
    # Note: COGS codes are 7xx. Overhead 8xx.
    # Standard CoA: 5=COGS, 6=Exp, 7=Other Income/Exp? 
    # User CoA: 7xx = COGS, 8xx = Overhead.
    
    code_is_expense = (
        df["acct_code"].str.startswith("6", na=False) | 
        df["acct_code"].str.startswith("7", na=False) | 
        df["acct_code"].str.startswith("8", na=False) | 
        df["acct_code"].str.startswith("9", na=False)
    )
    
    exp_mask = (
        acct_type.str.contains("expense") | 
        acct_type.str.contains("cogs") | 
        acct_type.str.contains("cost of goods") |
        code_is_expense
    )
    
    # We need a temporary is_expense col for row processing
    df["is_expense"] = exp_mask & (~df["is_revenue"]) # Ensure mutual exclusivity
    
    def classify_row(row):
        if not row["is_expense"]:
            return None
            
        code = row["acct_code"]
        acct_str = row["account"]
        
        if code in COGS_CODES:
            return "COGS"
            
        if is_overhead(code, acct_str):
            return "Overhead"
            
        return "Other"

    # Apply classification
    df["expense_class"] = df.apply(classify_row, axis=1)
    
    return df

# ---------------------------------------------------------
# ADDBACKS
# ---------------------------------------------------------

def detect_addbacks(df, custom_tokens=None):
    """
    Flags SDE addbacks based on memo/name tokens:
    - "xnp", "nathan"
    - " ng", startswith "ng"
    - Any tokens provided in custom_tokens list
    """
    if df is None:
        return None
        
    df = df.copy()
    
    if custom_tokens is None:
        custom_tokens = []
    
    # Initialize columns (ensure they exist even if df is empty)
    if "sde_addback_flag" not in df.columns:
        df["sde_addback_flag"] = False
    if "sde_addback_reason" not in df.columns:
        df["sde_addback_reason"] = ""
        
    if df.empty:
        return df
    
    # Initialize columns
    df["sde_addback_flag"] = False
    df["sde_addback_reason"] = ""
    
    # Only Expenses are addbacks usually? 
    # Prompt 4.1: "A row is a candidate SDE addback if: account_type == 'Expense' and ..."
    if "is_expense" not in df.columns:
         # Re-derive if missing (should rely on classify_transactions being run first)
         df["is_expense"] = df["account_type"].str.lower().str.contains("expense") | df["account_type"].str.lower().str.contains("cogs")

    # Pre-compute lower case
    memo_l = df["memo"].str.lower()
    name_l = df["name"].str.lower() # Payee name
    
    # Tokens
    # "xnp" in memo or name
    mask_xnp = memo_l.str.contains("xnp") | name_l.str.contains("xnp")
    
    # "nathan" in memo or name
    mask_nathan = memo_l.str.contains("nathan") | name_l.str.contains("nathan")
    
    # " ng" in memo/name OR startswith "ng"
    # regex for " ng" or "^ng"
    mask_ng = memo_l.str.contains(r"(?:^|\s)ng") | name_l.str.contains(r"(?:^|\s)ng")
    
    # Custom tokens
    mask_custom = pd.Series([False] * len(df), index=df.index)
    if custom_tokens:
        for token in custom_tokens:
            t = token.lower().strip()
            if t:
                mask_custom |= memo_l.str.contains(t, regex=False) | name_l.str.contains(t, regex=False)
    
    overall_mask = (mask_xnp | mask_nathan | mask_ng | mask_custom) & df["is_expense"]
    
    df.loc[overall_mask, "sde_addback_flag"] = True
    
    # Label reasons
    # We can do this iteratively or vectorized
    reasons = []
    for i, row in df.iterrows():
        # Check custom tokens logic here too or rely on flag?
        # If flag is True, we want to know WHY.
        
        # Optimization: Only check if flag is True?
        # Or check if mask matched.
        if not row["sde_addback_flag"]:
            reasons.append("")
            continue
            
        r_list = []
        m = str(row["memo"]).lower()
        n = str(row["name"]).lower()
        
        if "xnp" in m or "xnp" in n: r_list.append("XNP")
        if "nathan" in m or "nathan" in n: r_list.append("NATHAN")
        if " ng" in m or m.startswith("ng") or " ng" in n or n.startswith("ng"): r_list.append("NG")
        
        for token in custom_tokens:
            t = token.lower().strip()
            if t and (t in m or t in n):
                r_list.append(f"CUSTOM:{t.upper()}")
        
        reasons.append(", ".join(list(set(r_list))))
        
    df["sde_addback_reason"] = reasons
    
    return df

# ---------------------------------------------------------
# CALCULATIONS
# ---------------------------------------------------------

def compute_metrics(df):
    """
    Aggregates metrics for the provided dataframe (which is assumed to be filtered by date already).
    """
    if df is None or df.empty:
        return {
            "revenue": 0.0,
            "cogs": 0.0,
            "overhead": 0.0,
            "other_expense": 0.0,
            "gross_profit": 0.0,
            "net_profit": 0.0,
            "addbacks": 0.0,
            "sde": 0.0
        }
        
    # Revenue: sum of -amount where is_revenue is True
    # (Assumes negative amount = credit = income)
    rev_df = df[df["is_revenue"]]
    revenue = -rev_df["amount"].sum()
    
    # Expenses: sum of amount (positive debit = expense)
    cogs = df[df["expense_class"] == "COGS"]["amount"].sum()
    overhead = df[df["expense_class"] == "Overhead"]["amount"].sum()
    other = df[df["expense_class"] == "Other"]["amount"].sum()
    
    gross_profit = revenue - cogs
    net_profit = gross_profit - overhead - other
    
    # Addbacks
    addbacks = df[df["sde_addback_flag"]]["amount"].sum()
    
    sde = net_profit + addbacks
    
    return {
        "revenue": revenue,
        "cogs": cogs,
        "overhead": overhead,
        "other_expense": other,
        "gross_profit": gross_profit,
        "net_profit": net_profit,
        "addbacks": addbacks,
        "sde": sde
    }

def get_period_metrics(df, start_date, end_date):
    """
    Filters DF by date and computes metrics.
    """
    if df is None: return None
    
    mask = (df["date"] >= pd.to_datetime(start_date)) & (df["date"] <= pd.to_datetime(end_date))
    sub_df = df.loc[mask]
    
    return compute_metrics(sub_df)

def get_owner_metrics(df, acquisition_date, current_date, owner_revenue_start=None):
    """
    Owner Metrics:
    - Start from acquisition_date (7/1/25)
    - Exclude Revenue from Seller Month (July)
    - Include All Expenses from 7/1/25
    - If owner_revenue_start provided, use it for COGS/Rev start.
    """
    if df is None: return None
    
    # 1. Filter all data from 7/1 onwards
    mask_all = (df["date"] >= pd.to_datetime(acquisition_date)) & (df["date"] <= pd.to_datetime(current_date))
    owner_df = df.loc[mask_all].copy()
    
    # 2. Identify Seller Revenue (July)
    # If owner_revenue_start is None, derive from Acq Date + 1 Month?
    # Or assume Seller Period ends just before owner_revenue_start
    
    if owner_revenue_start:
        seller_end = pd.to_datetime(owner_revenue_start) - pd.Timedelta(days=1)
    else:
        # Default fallback: Acq Date + 1 Month End?
        # Actually, let's just stick to the previous logic if not provided.
        seller_end = pd.to_datetime(acquisition_date) + pd.offsets.MonthEnd(0)
        owner_revenue_start = seller_end + pd.Timedelta(days=1)
    
    seller_rev_mask = (owner_df["is_revenue"]) & (owner_df["date"] <= seller_end)
    
    full_metrics = compute_metrics(owner_df)
    
    # Seller Revenue (July)
    # Note: We only exclude REVENUE. We KEEP expenses.
    seller_mask = (df["date"] >= pd.to_datetime(acquisition_date)) & (df["date"] <= seller_end)
    seller_df = df.loc[seller_mask]
    seller_metrics = compute_metrics(seller_df)
    
    owner_revenue = full_metrics["revenue"] - seller_metrics["revenue"]
    
    # Owner Costs Logic
    # Owner COGS: COGS rows with date >= owner_revenue_start
    # Owner Overhead & Other: All rows from date >= acquisition_date
    
    owner_start_date = pd.to_datetime(owner_revenue_start)
    
    # Calculate Owner COGS (Aug+)
    owner_cogs_mask = (owner_df["date"] >= owner_start_date)
    owner_cogs_df = owner_df.loc[owner_cogs_mask]
    owner_cogs_metrics = compute_metrics(owner_cogs_df)
    owner_cogs = owner_cogs_metrics["cogs"]
    
    # Overhead & Other are Full Period (July+) -> full_metrics["overhead"] is correct.
    owner_overhead = full_metrics["overhead"]
    owner_other = full_metrics["other_expense"]
    
    owner_gross = owner_revenue - owner_cogs
    owner_net = owner_gross - owner_overhead - owner_other
    
    owner_addbacks = full_metrics["addbacks"] # Includes July addbacks? Prompt: "Including all business costs from 7/1 onward."
    # Yes, if I pay for Nathan's July expense, it's my cost, and my addback.
    
    owner_sde = owner_net + owner_addbacks
    
    return {
        "revenue": owner_revenue,
        "cogs": owner_cogs,
        "overhead": owner_overhead,
        "other_expense": owner_other,
        "gross_profit": owner_gross,
        "net_profit": owner_net,
        "addbacks": owner_addbacks,
        "sde": owner_sde
    }
