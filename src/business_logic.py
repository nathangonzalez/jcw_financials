import pandas as pd
import numpy as np
import re


def _net_to_positive(total: float) -> float:
    """Convert a signed net total into a positive magnitude.

    This codebase needs to handle two common ledger sign conventions:
    - QuickBooks GL exports often represent Income as negative (credits) and Expenses as positive (debits).
    - Some test fixtures / other exports represent Income as positive and Expenses as negative.

    For reporting, we want Revenue/COGS/Overhead/etc. as positive magnitudes.
    """
    try:
        total_f = float(total)
    except Exception:
        return 0.0

    return -total_f if total_f < 0 else total_f


def account_code_prefix(account: str) -> str | None:
    """Extract the first 3 digits of an account code.

    Examples:
        "705.140 · POOL" -> "705"
        " 704.000 - Something" -> "704"
    """
    if account is None:
        return None
    m = re.match(r"^\s*(\d{3})", str(account))
    return m.group(1) if m else None


def extract_account_prefix(account: str) -> str | None:
    """Extract leading digits from an account string.

    Rules:
    - If account is None/NaN -> None
    - Take leading digits before the first '.' or whitespace

    Examples:
        "705.120 · LIGHTING" -> "705"
        "705.120 LIGHTING" -> "705"
        "865 · Rent Expense" -> "865"
        "Rent" -> None
    """
    if account is None:
        return None
    s = str(account)
    if s.strip() == "" or s.strip().lower() == "nan":
        return None
    m = re.match(r"^\s*(\d+)", s)
    return m.group(1) if m else None

def classify_transactions(df: pd.DataFrame, cogs_prefixes: set[str] | None = None) -> pd.DataFrame:
    """Classify ledger rows into revenue/cogs/overhead/other.

    QuickBooks Transaction Detail exports often mark job costs as account_type="Expense".
    We therefore use account-code prefixes (e.g., 704-708) to identify COGS.
    """
    df = df.copy()

    if cogs_prefixes is None:
        cogs_prefixes = {"704", "705", "706", "707", "708"}

    atype = (
        df["account_type"]
        if "account_type" in df.columns
        else pd.Series([""] * len(df), index=df.index)
    ).astype(str).str.lower().fillna("")

    account_series = (
        df["account"]
        if "account" in df.columns
        else pd.Series([""] * len(df), index=df.index)
    ).astype(str)
    account_lower = account_series.str.lower().fillna("")

    df["account_prefix"] = account_series.map(extract_account_prefix)

    # Initialize flags
    df["is_revenue"] = False
    df["is_cogs"] = False
    df["is_overhead"] = False
    df["is_other_expense"] = False

    # Revenue
    revenue_mask = atype.str.contains("income") | account_lower.str.contains("income")
    df.loc[revenue_mask, "is_revenue"] = True

    # Other expense / other income (do not mark as overhead)
    other_exp_mask = atype.str.contains("other expense") | atype.str.contains("other income")
    df.loc[other_exp_mask & (~revenue_mask), "is_other_expense"] = True

    # Expense-like rows (expense/cogs) that are not already classified
    expense_like = atype.str.contains("expense") | atype.str.contains("cost of goods sold") | atype.str.contains("cogs")
    remaining = expense_like & (~df["is_revenue"]) & (~df["is_other_expense"])

    # COGS:
    # - Always treat explicit COGS account types as COGS
    # - Also treat Expense rows as COGS when account_prefix matches configured job-cost prefixes
    is_explicit_cogs = atype.str.contains("cost of goods sold") | atype.str.fullmatch(r"\s*cogs\s*")
    is_cogs_prefix = df["account_prefix"].isin(cogs_prefixes)

    cogs_mask = remaining & (is_explicit_cogs | is_cogs_prefix)
    overhead_mask = remaining & (~cogs_mask)

    df.loc[cogs_mask, "is_cogs"] = True
    df.loc[overhead_mask, "is_overhead"] = True

    # Ensure mutual exclusivity
    df.loc[df["is_revenue"], ["is_cogs", "is_overhead", "is_other_expense"]] = False
    df.loc[df["is_other_expense"], ["is_cogs", "is_overhead"]] = False
    df.loc[df["is_cogs"], ["is_overhead"]] = False

    # Classification string column
    df["classification"] = np.select(
        [
            df["is_revenue"],
            df["is_cogs"],
            df["is_overhead"],
            df["is_other_expense"],
        ],
        ["Revenue", "COGS", "Overhead", "Other"],
        default=np.where(atype.eq(""), "Unclassified", "Other"),
    )

    return df

def detect_addbacks(df: pd.DataFrame, custom_tokens=None) -> pd.DataFrame:
    df = df.copy()
    if custom_tokens is None:
        custom_tokens = []

    df["sde_addback_flag"] = False
    df["sde_addback_reason"] = ""

    # Base tokens
    # Use word boundaries for short tokens to avoid matching substrings like "Buildi(ng)"
    # We'll use regex for all just to be safe and consistent.
    # "xnp" might be a code, so maybe keep it loose? But likely distinct.
    raw_tokens = ["xnp", "ng", "nathan", "owner", "personal"] + [t.lower() for t in custom_tokens]

    # We'll check name and memo
    # Ensure they are strings (handled in data_loader, but good to be safe)
    name_col = df["name"].astype(str).str.lower() if "name" in df.columns else pd.Series([""] * len(df))
    memo_col = df["memo"].astype(str).str.lower() if "memo" in df.columns else pd.Series([""] * len(df))

    for t in raw_tokens:
        if not t: continue

        # Escape special regex chars just in case, then wrap in word boundaries
        # For very short tokens like 'ng', boundary is critical.
        # For 'xnp', probably also good.
        pattern = f"\\b{pd.io.common.re.escape(t)}\\b"

        hit = name_col.str.contains(pattern, regex=True, na=False) | memo_col.str.contains(pattern, regex=True, na=False)

        # Update flag
        df.loc[hit, "sde_addback_flag"] = True

        # Update reason (append if multiple matches, or just set?)
        # Prompt says: "AUTO: token=<...>" or a small concatenation.
        # Let's just append token if not present.
        # This is a bit tricky with vectorization.
        # Simple approach: apply? No, slow.
        # Let's just set it for the hit rows.

        # We can iterate and update.
        existing_reasons = df.loc[hit, "sde_addback_reason"]
        new_reasons = existing_reasons + (existing_reasons.apply(lambda x: ", " if x else "") + f"token={t}")
        df.loc[hit, "sde_addback_reason"] = new_reasons

    return df

def get_owner_metrics(
    df: pd.DataFrame,
    owner_period_start,
    current_date,
    owner_revenue_start,
    addback_account_overrides: set[str] | None = None,
    exclude_legacy_july_job_costs: bool = True,
    legacy_job_cost_prefixes: set[str] | None = None,
) -> dict:
    # 1. Filter to Owner Period window
    mask_period = (df["date"].dt.date >= owner_period_start) & (df["date"].dt.date <= current_date)
    df_window = df[mask_period].copy()

    # 2. Revenue (apply owner_revenue_start)
    # Only count rows where is_revenue is True AND date >= owner_revenue_start
    rev_mask = df_window["is_revenue"] & (df_window["date"].dt.date >= owner_revenue_start)

    # Normalize sign so Revenue is always a positive magnitude.
    revenue_raw = df_window.loc[rev_mask, "amount"].sum()
    revenue = _net_to_positive(revenue_raw)

    # 3. Expenses
    # - For date >= owner_revenue_start: include all expenses normally.
    # - For owner_period_start <= date < owner_revenue_start (legacy July window):
    #   include overhead only, excluding configured job-cost prefixes.
    # Normalize sign so each bucket is always a positive magnitude.

    july_mask = (df_window["date"].dt.date >= owner_period_start) & (df_window["date"].dt.date < owner_revenue_start)
    aug_plus_mask = df_window["date"].dt.date >= owner_revenue_start

    if legacy_job_cost_prefixes is None:
        legacy_job_cost_prefixes = {"704", "705", "706", "707", "708"}

    # Compute job-cost mask (prefix-based) for July window.
    acct_series = df_window.get("account", pd.Series([""] * len(df_window), index=df_window.index)).astype(str)
    prefixes = acct_series.map(account_code_prefix)
    is_job_cost_prefix = prefixes.isin(legacy_job_cost_prefixes)
    legacy_july_job_cost_mask = july_mask & is_job_cost_prefix & (df_window["is_overhead"] | df_window["is_other_expense"] | df_window["is_cogs"])

    legacy_july_total_expense_raw = df_window.loc[
        july_mask & (df_window["is_overhead"] | df_window["is_other_expense"] | df_window["is_cogs"]),
        "amount",
    ].sum()
    legacy_july_total_expense = _net_to_positive(legacy_july_total_expense_raw)

    legacy_july_excluded_job_cost_raw = df_window.loc[legacy_july_job_cost_mask, "amount"].sum()
    legacy_july_excluded_job_cost = _net_to_positive(legacy_july_excluded_job_cost_raw)

    # COGS: owner period is Aug+ only; July is tracked separately (legacy)
    cogs_owner_mask = df_window["is_cogs"] & aug_plus_mask
    cogs_legacy_mask = df_window["is_cogs"] & july_mask

    cogs_raw = df_window.loc[cogs_owner_mask, "amount"].sum()
    legacy_cogs_raw = df_window.loc[cogs_legacy_mask, "amount"].sum()

    cogs = _net_to_positive(cogs_raw)
    legacy_cogs = _net_to_positive(legacy_cogs_raw)

    # Overhead:
    # - Aug+ overhead: include all
    # - July overhead: include only non-job-cost overhead if exclusion enabled
    overhead_aug_raw = df_window.loc[df_window["is_overhead"] & aug_plus_mask, "amount"].sum()

    if exclude_legacy_july_job_costs:
        july_overhead_mask = df_window["is_overhead"] & july_mask & (~is_job_cost_prefix)
    else:
        july_overhead_mask = df_window["is_overhead"] & july_mask

    legacy_july_included_overhead_raw = df_window.loc[july_overhead_mask, "amount"].sum()
    legacy_july_included_overhead = _net_to_positive(legacy_july_included_overhead_raw)

    overhead_raw = overhead_aug_raw + legacy_july_included_overhead_raw

    # Other Expense:
    # Spec: July legacy window includes overhead only, so exclude July other_expense.
    other_expense_raw = df_window.loc[df_window["is_other_expense"] & aug_plus_mask, "amount"].sum()

    overhead = _net_to_positive(overhead_raw)
    other_expense = _net_to_positive(other_expense_raw)

    # 4. Addbacks
    # Start with token-based flags
    addback_mask = df_window.get("sde_addback_flag", pd.Series([False] * len(df_window))).copy()
    # Add account-level overrides
    if addback_account_overrides:
        acct_mask = df_window["account"].astype(str).isin(addback_account_overrides)
        addback_mask = addback_mask | acct_mask

    # Addbacks should be a positive magnitude.
    # To reduce double-counting in ledgers that include both debit/credit lines, prefer
    # whichever sign is present (QB exports tend to use + for expenses; some fixtures use -).
    addback_series = df_window.loc[addback_mask, "amount"]
    if (addback_series > 0).any():
        addbacks = float(addback_series[addback_series > 0].sum())
    else:
        addbacks = float(-addback_series[addback_series < 0].sum())

    # 5. Metrics
    gross_profit = revenue - cogs
    net_profit = revenue - (cogs + overhead + other_expense)

    # SDE = Net Profit + Addbacks
    sde = net_profit + addbacks

    return {
        "revenue": revenue,
        "cogs": cogs,
        "legacy_cogs": legacy_cogs,
        "overhead": overhead,
        "other_expense": other_expense,
        "gross_profit": gross_profit,
        "net_profit": net_profit,
        "addbacks": addbacks,
        "sde": sde,
        "legacy_july_total_expense": legacy_july_total_expense,
        "legacy_july_excluded_job_cost": legacy_july_excluded_job_cost,
        "legacy_july_included_overhead": legacy_july_included_overhead,
    }


def compute_legacy_overhead_addins(
    df: pd.DataFrame,
    legacy_start,
    legacy_end,
    included_accounts: set[str] | None = None,
) -> float:
    """Compute prior-period overhead to include as an add-in.

    This is used by the app to optionally subtract selected *prior-month* overhead
    from net profit (i.e., treat it as part of the year-1 owner-cost view).

    Assumptions:
    - df has a datetime-like 'date' column.
    - df has 'amount' and 'is_overhead' columns (produced by classify_transactions).
    - included_accounts, when provided, matches df['account'] values.
    """
    if df is None or len(df) == 0:
        return 0.0

    if "date" not in df.columns or "amount" not in df.columns:
        return 0.0

    if "is_overhead" not in df.columns:
        return 0.0

    d = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(d["date"]):
        d["date"] = pd.to_datetime(d["date"], errors="coerce")

    mask = (d["date"].dt.date >= legacy_start) & (d["date"].dt.date <= legacy_end) & (d["is_overhead"])
    if included_accounts:
        mask = mask & d.get("account", pd.Series([""] * len(d), index=d.index)).astype(str).isin(included_accounts)

    legacy_overhead_raw = d.loc[mask, "amount"].sum()
    return float(_net_to_positive(legacy_overhead_raw))


def apply_legacy_overhead_addins(metrics: dict, legacy_overhead_included_total: float = 0.0) -> dict:
    """Apply legacy overhead add-ins to a metrics dict.

    The add-in behaves like additional overhead expense:
    - overhead increases
    - net_profit decreases
    - sde recomputes from (net_profit + addbacks)
    """
    out = dict(metrics or {})

    try:
        legacy_total = float(legacy_overhead_included_total or 0.0)
    except Exception:
        legacy_total = 0.0

    if legacy_total == 0.0:
        return out

    revenue = float(out.get("revenue", 0.0) or 0.0)
    cogs = float(out.get("cogs", 0.0) or 0.0)
    overhead = float(out.get("overhead", 0.0) or 0.0) + legacy_total
    other_expense = float(out.get("other_expense", 0.0) or 0.0)
    addbacks = float(out.get("addbacks", 0.0) or 0.0)

    out["overhead"] = overhead
    out["gross_profit"] = revenue - cogs
    out["net_profit"] = revenue - (cogs + overhead + other_expense)
    out["sde"] = out["net_profit"] + addbacks
    out["legacy_overhead_included"] = legacy_total

    return out

def get_period_metrics(df_period: pd.DataFrame, start_date, end_date) -> dict:
    """
    Caller has already filtered df_period.
    We just sum up based on flags.
    """
    # Assuming df_period is already filtered to the desired date range.

    # Revenue as positive magnitude
    revenue_raw = df_period.loc[df_period["is_revenue"], "amount"].sum()
    revenue = _net_to_positive(revenue_raw)

    # Expenses as positive magnitude
    cogs_raw = df_period.loc[df_period["is_cogs"], "amount"].sum()
    overhead_raw = df_period.loc[df_period["is_overhead"], "amount"].sum()
    other_expense_raw = df_period.loc[df_period["is_other_expense"], "amount"].sum()

    cogs = _net_to_positive(cogs_raw)
    overhead = _net_to_positive(overhead_raw)
    other_expense = _net_to_positive(other_expense_raw)

    # Addbacks as positive magnitude (prefer one sign to reduce double counting)
    addback_series = df_period.loc[df_period["sde_addback_flag"], "amount"]
    if (addback_series > 0).any():
        addbacks = float(addback_series[addback_series > 0].sum())
    else:
        addbacks = float(-addback_series[addback_series < 0].sum())

    gross_profit = revenue - cogs
    net_profit = revenue - (cogs + overhead + other_expense)
    sde = net_profit + addbacks

    return {
        "revenue": revenue,
        "cogs": cogs,
        "overhead": overhead,
        "other_expense": other_expense,
        "gross_profit": gross_profit,
        "net_profit": net_profit,
        "addbacks": addbacks,
        "sde": sde,
    }
