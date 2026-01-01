import pandas as pd
import datetime as dt
from pathlib import Path
from src.data_loader import load_ledger
from src.business_logic import (
    classify_transactions,
    detect_addbacks,
    get_owner_metrics,
    get_period_metrics
)
from src.forecasting import calculate_run_rates, forecast_year_1
from src.utils import currency

def analyze():
    print("--- Starting Analysis ---")
    
    # 1. Load Data
    file_path = Path("Bank Ledger through 11142025.csv")
    print(f"Loading {file_path}...")
    
    # Wrap file for load_ledger
    class FileWrapper:
        def __init__(self, path):
            self.path = path
            self.name = str(path)
        
        def seek(self, *args):
            return self.f.seek(*args)
            
        def read(self, *args):
            return self.f.read(*args)

        def __enter__(self):
            self.f = open(self.path, 'rb')
            return self
            
        def __exit__(self, *args):
            self.f.close()

    # We need to pass an open file object that also has a .name attribute
    with open(file_path, 'rb') as f:
        # Create a dummy object that proxies to f but has .name
        class UploadedFile:
            def __init__(self, file_obj, name):
                self._file = file_obj
                self.name = name
            def __getattr__(self, attr):
                return getattr(self._file, attr)
        
        uploaded = UploadedFile(f, file_path.name)
        df = load_ledger(uploaded)

    if df is None:
        print("Failed to load ledger.")
        return

    print(f"Loaded {len(df)} rows.")
    
    # Force date
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    # 2. Classify
    print("Classifying transactions...")
    df = classify_transactions(df)
    
    # 3. Addbacks
    print("Detecting addbacks...")
    df = detect_addbacks(df)
    
    # Inspect addbacks
    addbacks_df = df[df["sde_addback_flag"]]
    print(f"Found {len(addbacks_df)} addback transactions.")
    if not addbacks_df.empty:
        print("Sample addbacks:")
        print(addbacks_df[["date", "name", "memo", "amount", "sde_addback_reason"]].head(10))
        print(f"Total Addback Amount: {addbacks_df['amount'].sum()}")

    # 4. Metrics
    print("Calculating metrics...")
    
    # Config (matching app defaults)
    owner_rev_start = dt.date(2025, 8, 1)
    year_1_end = dt.date(2026, 6, 30)
    today = dt.date.today()
    
    owner_period_start = df["date"].min().date()
    
    owner_metrics = get_owner_metrics(
        df, 
        owner_period_start, 
        today, 
        owner_revenue_start=owner_rev_start
    )
    
    # 5. Run Rates
    run_rate_df = df[(df["date"] >= pd.to_datetime(owner_rev_start)) & (df["date"] <= pd.to_datetime(today))]
    run_rate_metrics = get_period_metrics(run_rate_df, owner_rev_start, today)
    
    days_run_rate = (today - owner_rev_start).days
    run_rates = calculate_run_rates(run_rate_metrics, days_run_rate)
    
    # 6. Forecast
    days_remaining = (year_1_end - today).days
    forecast, months_rem = forecast_year_1(owner_metrics, run_rates, days_remaining)
    
    # --- REPORT ---
    print("\n" + "="*50)
    print("FINANCIAL ANALYSIS REPORT")
    print("="*50)
    
    print(f"\nReport Date: {today}")
    print(f"Owner Revenue Start: {owner_rev_start}")
    print(f"Year 1 End: {year_1_end}")
    
    print("\n--- YTD Owner Metrics (Jul 1 - Today) ---")
    print(f"Revenue:      {currency(owner_metrics['revenue'])}")
    print(f"SDE:          {currency(owner_metrics['sde'])}")
    print(f"Net Profit:   {currency(owner_metrics['net_profit'])}")
    print(f"Addbacks:     {currency(owner_metrics['addbacks'])}")
    
    print("\n--- Monthly Run Rates (based on Aug 1 - Today) ---")
    print(f"Revenue:      {currency(run_rates['revenue'])} /mo")
    print(f"SDE:          {currency(run_rates['sde'])} /mo")
    print(f"Net Profit:   {currency(run_rates['net_profit'])} /mo")
    
    print("\n--- Year 1 Forecast (Projected) ---")
    print(f"Total Revenue:    {currency(forecast['revenue'])}")
    print(f"Total SDE:        {currency(forecast['sde'])}")
    print(f"Total Net Profit: {currency(forecast['net_profit'])}")
    
    print("\n" + "="*50)

if __name__ == "__main__":
    analyze()
