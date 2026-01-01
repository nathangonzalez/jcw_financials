#!/usr/bin/env python3
"""
Compute KPI actuals from QB Transaction Detail export (source of truth).

This script loads the Transaction Detail CSV and outputs JSON with financial metrics
for two periods:
- Owner period: 7/1 → current date
- Run-rate period: 8/1 → current date

Implements the July nuance: exclude July COGS (legacy), include July overhead.
Matches the same math used by get_owner_metrics() and calculate_run_rates().
"""

import argparse
import json
import pandas as pd
import datetime as dt
from typing import Dict, Any
import sys
import os
from pathlib import Path

# Ensure repo root is on sys.path so `src.*` imports work when running as a script.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.business_logic import classify_transactions, detect_addbacks, _net_to_positive
from src.data_loader import parse_date_series


def load_transaction_detail(file_path: str) -> pd.DataFrame:
    """Load and clean QB Transaction Detail export."""
    try:
        # Read CSV, skip the date range summary row
        df = pd.read_csv(file_path)
        
        # Drop the summary row (usually row 1 with date range)
        df = df.dropna(subset=['Type']).copy()
        
        # Clean column names (remove leading/trailing spaces, handle unicode)
        df.columns = df.columns.str.strip()
        
        # Standardize column names to match expected format
        column_mapping = {
            'Date': 'date',
            'Name': 'name', 
            'Memo': 'memo',
            'Account': 'account',
            'Class': 'class',
            'Amount': 'amount',
            'Account Type': 'account_type'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # Parse date column
        if 'date' in df.columns:
            df['date'] = parse_date_series(df['date'])
        else:
            raise ValueError("No date column found in transaction detail")
            
        # Convert amount to numeric
        if 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
        else:
            raise ValueError("No amount column found in transaction detail")
            
        # Fill NaN values in text columns
        for col in ['name', 'memo', 'account', 'class', 'account_type']:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)
        
        return df
        
    except Exception as e:
        raise ValueError(f"Failed to load transaction detail from {file_path}: {e}")


def compute_period_kpis(df: pd.DataFrame, start_date: dt.date, end_date: dt.date, 
                       revenue_start: dt.date = None) -> Dict[str, float]:
    """
    Compute KPIs for a given period.
    
    Args:
        df: Transaction detail dataframe
        start_date: Period start date  
        end_date: Period end date
        revenue_start: When to start counting revenue (for July nuance)
    """
    if revenue_start is None:
        revenue_start = start_date
        
    # Filter to period
    mask_period = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
    df_period = df[mask_period].copy()
    
    if len(df_period) == 0:
        return {
            'revenue': 0.0,
            'cogs': 0.0, 
            'legacy_cogs': 0.0,
            'overhead': 0.0,
            'other_expense': 0.0,
            'addbacks': 0.0,
            'net_profit': 0.0,
            'sde': 0.0,
            'gross_profit': 0.0
        }
    
    # Classify transactions
    df_period = classify_transactions(df_period)
    df_period = detect_addbacks(df_period)
    
    # Revenue (only count from revenue_start forward)
    rev_mask = df_period['is_revenue'] & (df_period['date'].dt.date >= revenue_start)
    revenue_raw = df_period.loc[rev_mask, 'amount'].sum()
    revenue = _net_to_positive(revenue_raw)
    
    # COGS with July nuance
    # Owner COGS: revenue_start forward
    cogs_owner_mask = df_period['is_cogs'] & (df_period['date'].dt.date >= revenue_start)
    cogs_raw = df_period.loc[cogs_owner_mask, 'amount'].sum()
    cogs = _net_to_positive(cogs_raw)
    
    # Legacy COGS: before revenue_start (typically July)
    cogs_legacy_mask = df_period['is_cogs'] & (df_period['date'].dt.date >= start_date) & (df_period['date'].dt.date < revenue_start)
    legacy_cogs_raw = df_period.loc[cogs_legacy_mask, 'amount'].sum()
    legacy_cogs = _net_to_positive(legacy_cogs_raw)
    
    # Overhead and other expenses (include all period)
    overhead_raw = df_period.loc[df_period['is_overhead'], 'amount'].sum()
    overhead = _net_to_positive(overhead_raw)
    
    other_expense_raw = df_period.loc[df_period['is_other_expense'], 'amount'].sum() 
    other_expense = _net_to_positive(other_expense_raw)
    
    # Addbacks
    addback_mask = df_period.get('sde_addback_flag', pd.Series([False] * len(df_period)))
    addback_series = df_period.loc[addback_mask, 'amount']
    if (addback_series > 0).any():
        addbacks = float(addback_series[addback_series > 0].sum())
    else:
        addbacks = float(-addback_series[addback_series < 0].sum())
    
    # Calculate metrics
    gross_profit = revenue - cogs
    net_profit = revenue - (cogs + overhead + other_expense)
    sde = net_profit + addbacks
    
    return {
        'revenue': float(revenue),
        'cogs': float(cogs),
        'legacy_cogs': float(legacy_cogs), 
        'overhead': float(overhead),
        'other_expense': float(other_expense),
        'addbacks': float(addbacks),
        'net_profit': float(net_profit),
        'sde': float(sde),
        'gross_profit': float(gross_profit)
    }


def main():
    parser = argparse.ArgumentParser(description='Compute KPI actuals from QB Transaction Detail')
    parser.add_argument('transaction_file', help='Path to QB Transaction Detail CSV file')
    parser.add_argument('--current-date', help='Current date (YYYY-MM-DD)', default=None)
    parser.add_argument('--owner-period-start', help='Owner period start date (YYYY-MM-DD)', default='2025-07-01')
    parser.add_argument('--owner-revenue-start', help='Owner revenue start date (YYYY-MM-DD)', default='2025-08-01')
    parser.add_argument('--output', '-o', help='Output JSON file path (default: stdout)')
    
    args = parser.parse_args()
    
    # Parse dates
    if args.current_date:
        current_date = dt.datetime.strptime(args.current_date, '%Y-%m-%d').date()
    else:
        current_date = dt.date.today()
        
    owner_period_start = dt.datetime.strptime(args.owner_period_start, '%Y-%m-%d').date()
    owner_revenue_start = dt.datetime.strptime(args.owner_revenue_start, '%Y-%m-%d').date()
    
    try:
        # Load transaction detail
        df = load_transaction_detail(args.transaction_file)
        
        # Compute owner period KPIs (7/1 → current)
        owner_kpis = compute_period_kpis(
            df, 
            start_date=owner_period_start,
            end_date=current_date,
            revenue_start=owner_revenue_start
        )
        
        # Compute run-rate period KPIs (8/1 → current) 
        run_rate_kpis = compute_period_kpis(
            df,
            start_date=owner_revenue_start, 
            end_date=current_date,
            revenue_start=owner_revenue_start
        )
        
        # Create output
        result = {
            'metadata': {
                'source_file': args.transaction_file,
                'computed_at': dt.datetime.now().isoformat(),
                'current_date': current_date.isoformat(),
                'owner_period_start': owner_period_start.isoformat(),
                'owner_revenue_start': owner_revenue_start.isoformat()
            },
            'owner_period': owner_kpis,
            'run_rate_period': run_rate_kpis,
            'summary': {
                'owner_period_days': (current_date - owner_period_start).days,
                'run_rate_period_days': (current_date - owner_revenue_start).days,
                'july_legacy_cogs_excluded': float(owner_kpis['legacy_cogs']),
                'july_overhead_included': True
            }
        }
        
        # Output result
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"KPI results written to {args.output}")
        else:
            print(json.dumps(result, indent=2))
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()