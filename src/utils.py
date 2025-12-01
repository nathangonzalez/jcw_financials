import pandas as pd
import numpy as np
import re

def read_csv_flex(file, **kwargs):
    """
    Try utf-8 first, then latin-1. Works with Streamlit uploaders.
    Also supports Excel files if the file object has a name ending in .xlsx or .xls
    """
    if file is None:
        return None
    
    # Check for Excel
    if hasattr(file, "name") and (file.name.lower().endswith(".xlsx") or file.name.lower().endswith(".xls")):
        return pd.read_excel(file, **kwargs)

    try:
        return pd.read_csv(file, **kwargs)
    except UnicodeDecodeError:
        try:
            file.seek(0)
        except Exception:
            pass
        return pd.read_csv(file, encoding="latin-1", **kwargs)

def parse_money(series):
    """
    Convert strings like '($29,500)' or '$1,234.50' to floats.
    Leaves numeric values alone.
    """
    def _parse(x):
        if isinstance(x, (int, float, np.number)):
            return float(x)
        if not isinstance(x, str):
            return np.nan
        s = x.strip()
        if s == "":
            return np.nan
        # Handle QuickBooks negative format: (123.45)
        neg = s.startswith("(") and s.endswith(")")
        s = s.replace("(", "").replace(")", "")
        s = s.replace("$", "").replace(",", "")
        try:
            val = float(s)
        except ValueError:
            return np.nan
        return -val if neg else val

    return series.apply(_parse)

def normalize_dates(series):
    """
    Convert a series to datetime, handling errors gracefully.
    Handles mixed types, strings, and potentially numeric Excel serials.
    """
    # First try standard conversion
    dt_series = pd.to_datetime(series, errors="coerce")
    
    # If we have mostly NaT, check if it might be numeric Excel serials (approx > 20000 for recent dates)
    if dt_series.isna().sum() > 0.5 * len(series):
        # Try numeric conversion
        try:
            # Clean non-numeric characters if strings?
            # Just force numeric coercion first
            numeric_series = pd.to_numeric(series, errors="coerce")
            # Excel epoch is usually 1899-12-30
            dt_series_numeric = pd.to_datetime(numeric_series, unit='D', origin='1899-12-30', errors='coerce')
            
            # Fill original NaTs with the numeric conversion result where valid
            dt_series = dt_series.fillna(dt_series_numeric)
        except Exception:
            pass
            
    return dt_series

def extract_account_code(account_str):
    """
    Extracts the leading code from an account string.
    e.g. "705.140 · POOL" -> "705"
         "804-03 · Truck Allowance" -> "804"
    """
    if not isinstance(account_str, str):
        return None
    
    # Match simple start of string digits
    # "705.140" -> 705, "804-03" -> 804
    # Regex: Start, digits, maybe dot or dash, more digits? 
    # The prompt examples show splitting at '.' or '-' or ' '
    
    # Take the first chunk before space, dot, or dash?
    # "705.140" -> "705" if we split by dot.
    # "804-03" -> "804" if we split by dash.
    
    # Let's try to grab the first contiguous block of digits.
    # But "705.140" is a code. The root is "705"? Or is the code "705.140"?
    # Prompt: "705.140 · POOL" -> 705. "804-03 · Truck Allowance" -> 804.
    # So it seems we want the part before the first non-digit separator (dot or dash).
    
    parts = re.split(r'[.\-\s]', account_str.strip())
    if parts and parts[0].isdigit():
        return parts[0]
    return None

# Formatters
currency = "${:,.0f}".format
currency_1 = "${:,.1f}".format
currency_2 = "${:,.2f}".format
