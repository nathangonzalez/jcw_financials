import pandas as pd
import numpy as np
from datetime import timedelta
from src.data_loader import load_ledger

def clean_currency(val):
    if isinstance(val, str):
        val = val.replace('$', '').replace(',', '').strip()
        # Handle "- $28800.3" format in bank export
        if val.startswith('- '):
            val = '-' + val[2:]
        elif val.startswith('+ '):
            val = val[2:]
        elif val.startswith('(') and val.endswith(')'):
            val = '-' + val[1:-1]
    return pd.to_numeric(val, errors='coerce')

def load_bank_export(path):
    print(f"Loading Bank Export: {path}")
    df = pd.read_csv(path)
    # Columns: Transaction Date, Transaction Description, Amount
    
    df['amount'] = df['Amount'].apply(clean_currency)
    df['date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')
    
    # Bank Export:
    # - $xx means Money Out (Withdrawal/Check) -> Negative in DataFrame
    # + $xx means Money In (Deposit) -> Positive in DataFrame
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
    return df

def load_qb_export(path):
    print(f"Loading QB Export: {path}")
    # Use the project's data loader which handles the specific format of the QB export
    # This handles the CSV format correctly
    with open(path, 'rb') as f:
        # Mock UploadedFile
        class UploadedFile:
            def __init__(self, file_obj, name):
                self._file = file_obj
                self.name = name
            def __getattr__(self, attr):
                return getattr(self._file, attr)
            def seek(self, *args):
                return self._file.seek(*args)
            def read(self, *args):
                return self._file.read(*args)

        uploaded = UploadedFile(f, path)
        df = load_ledger(uploaded)
    
    print(f"Total QB rows loaded: {len(df)}")
    print(f"QB Columns: {df.columns.tolist()}")
    if 'account' in df.columns:
         # Normalize column names to match what we expect or print them
         print(f"Unique Accounts in QB (Sample): {df['account'].unique()[:10]}")
    
    # Filter for Bank Account transactions
    # Look for 'PNC' in 'account' column
    bank_mask = df['account'].astype(str).str.contains('PNC', case=False, na=False)
    df_bank = df[bank_mask].copy()
    
    # Amount is already parsed by load_ledger (Debit - Credit)
    # Date is already parsed
    
    # Ensure amount is float
    df_bank['amount'] = pd.to_numeric(df_bank['amount'], errors='coerce').fillna(0.0)
    
    print(f"QB Bank Transactions found: {len(df_bank)}")
    return df_bank

def reconcile(bank_df, qb_df):
    print("\n--- Reconciling ---")
    
    # Bank: Date, Amount, Description
    # QB: Date, Amount, Num, Name, Memo
    
    # We match on Amount and Date (within +/- 5 days)
    
    # Add 'matched' flag
    bank_df['matched'] = False
    qb_df['matched'] = False
    
    matches = []
    
    # Iterate through Bank transactions
    for i, bank_row in bank_df.iterrows():
        amt = bank_row['amount']
        date = bank_row['date']
        
        if pd.isna(amt) or pd.isna(date):
            continue
            
        # Find potential matches in QB
        # Same amount, date within buffer
        # Increased buffer to 14 days to account for check clearing times
        date_min = date - timedelta(days=14)
        date_max = date + timedelta(days=14)
        
        # QB Amounts might be inverted?
        # In Bank Export: Payment = Negative.
        # In QB GL Export for Bank Account: Credit = Negative?
        # Let's assume signs match first.
        
        # Try exact sign match
        candidates = qb_df[
            (~qb_df['matched']) & 
            (np.isclose(qb_df['amount'], amt, atol=0.01)) & 
            (qb_df['date'] >= date_min) & 
            (qb_df['date'] <= date_max)
        ]
        
        if candidates.empty:
            # Try inverted sign
            candidates = qb_df[
                (~qb_df['matched']) & 
                (np.isclose(qb_df['amount'], -amt, atol=0.01)) & 
                (qb_df['date'] >= date_min) & 
                (qb_df['date'] <= date_max)
            ]
        
        if not candidates.empty:
            # Pick best match (closest date)
            candidates = candidates.copy()
            candidates['date_diff'] = (candidates['date'] - date).abs()
            best_match = candidates.sort_values('date_diff').iloc[0]
            
            qb_idx = best_match.name
            qb_df.at[qb_idx, 'matched'] = True
            bank_df.at[i, 'matched'] = True
            
            matches.append({
                'Bank_Date': date,
                'Bank_Desc': bank_row['Transaction Description'],
                'Amount': amt,
                'QB_Date': best_match['date'],
                'QB_Name': best_match['name'] if 'name' in best_match else '',
                'QB_Num': best_match['num'] if 'num' in best_match else ''
            })
            
    print(f"Matched {len(matches)} transactions.")
    
    # Unmatched Bank
    unmatched_bank = bank_df[~bank_df['matched']]
    print(f"\nUnmatched Bank Transactions: {len(unmatched_bank)}")
    if not unmatched_bank.empty:
        print(unmatched_bank[['Transaction Date', 'Transaction Description', 'Amount']].head(10))
        print(f"Total Unmatched Bank Amount: {unmatched_bank['amount'].sum()}")

    # Unmatched QB
    unmatched_qb = qb_df[~qb_df['matched']]
    print(f"\nUnmatched QB Transactions: {len(unmatched_qb)}")
    if not unmatched_qb.empty:
        cols = ['date', 'num', 'name', 'amount']
        cols = [c for c in cols if c in unmatched_qb.columns]
        print(unmatched_qb[cols].head(10))
        print(f"Total Unmatched QB Amount: {unmatched_qb['amount'].sum()}")
        
    return matches, unmatched_bank, unmatched_qb

if __name__ == "__main__":
    bank_file = "bank_export.csv"
    # Use the existing CSV file which is the QB export
    qb_file = "Bank Ledger through 11142025.csv"
    
    bank_df = load_bank_export(bank_file)
    qb_df = load_qb_export(qb_file)
    
    reconcile(bank_df, qb_df)
