#!/usr/bin/env python3
"""
Unit tests for compute_kpis.py script.

Tests the July nuance: exclude July COGS (legacy), include July overhead.
"""

import unittest
import pandas as pd
import datetime as dt
import tempfile
import os
import sys
import json

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from compute_kpis import compute_period_kpis, load_transaction_detail


class TestComputeKpis(unittest.TestCase):
    
    def create_test_transaction_detail(self) -> pd.DataFrame:
        """Create synthetic transaction detail data for testing July/Aug nuance."""
        
        data = [
            # July transactions
            {
                'Type': 'Bill',
                'Date': '07/15/2025',
                'Name': 'Legacy Supplier',
                'Memo': 'July COGS - should be excluded from owner metrics',
                'Account': '500 - Cost of Goods',
                'Amount': 1000.0,  # Positive = expense in QB
                'Account Type': 'Cost of Goods Sold'
            },
            {
                'Type': 'Bill', 
                'Date': '07/20/2025',
                'Name': 'Office Rent',
                'Memo': 'July overhead - should be INCLUDED',
                'Account': '600 - Rent',
                'Amount': 2000.0,
                'Account Type': 'Expense'
            },
            # August transactions (owner period)
            {
                'Type': 'Invoice',
                'Date': '08/05/2025', 
                'Name': 'Client A',
                'Memo': 'Project revenue',
                'Account': '400 - Revenue',
                'Amount': -5000.0,  # Negative = income in QB
                'Account Type': 'Income'
            },
            {
                'Type': 'Bill',
                'Date': '08/10/2025',
                'Name': 'Materials Supplier',
                'Memo': 'August COGS - should be included',
                'Account': '500 - Cost of Goods',
                'Amount': 800.0,
                'Account Type': 'Cost of Goods Sold'
            },
            {
                'Type': 'Bill',
                'Date': '08/15/2025',
                'Name': 'Office Utilities',
                'Memo': 'August overhead',
                'Account': '610 - Utilities', 
                'Amount': 300.0,
                'Account Type': 'Expense'
            },
            # September transaction
            {
                'Type': 'Invoice',
                'Date': '09/01/2025',
                'Name': 'Client B', 
                'Memo': 'September revenue',
                'Account': '400 - Revenue',
                'Amount': -3000.0,
                'Account Type': 'Income'
            }
        ]
        
        df = pd.DataFrame(data)
        
        # Convert to expected format
        df['date'] = pd.to_datetime(df['Date'])
        df['name'] = df['Name']
        df['memo'] = df['Memo'] 
        df['account'] = df['Account']
        df['amount'] = df['Amount']
        df['account_type'] = df['Account Type']
        
        # Fill missing columns
        df['class'] = ''
        
        return df
    
    def test_july_cogs_exclusion(self):
        """Test that July COGS is excluded from owner period but July overhead is included."""
        
        df = self.create_test_transaction_detail()
        
        # Owner period: 7/1 → 9/30, but revenue starts 8/1
        owner_start = dt.date(2025, 7, 1)
        current_date = dt.date(2025, 9, 30)
        revenue_start = dt.date(2025, 8, 1)
        
        kpis = compute_period_kpis(df, owner_start, current_date, revenue_start)
        
        # Verify July COGS exclusion
        self.assertEqual(kpis['cogs'], 800.0, "Should only include August COGS (800), not July COGS (1000)")
        self.assertEqual(kpis['legacy_cogs'], 1000.0, "Should track July legacy COGS separately")
        
        # Verify July overhead inclusion  
        self.assertEqual(kpis['overhead'], 2300.0, "Should include both July (2000) and August (300) overhead")
        
        # Verify revenue (starts Aug 1)
        self.assertEqual(kpis['revenue'], 8000.0, "Should include Aug (5000) + Sep (3000) revenue")
        
        # Verify net profit calculation
        expected_net = 8000.0 - (800.0 + 2300.0)  # Revenue - (Owner COGS + All Overhead)
        self.assertEqual(kpis['net_profit'], expected_net, f"Net profit should be {expected_net}")
        
    def test_run_rate_period(self):
        """Test run-rate period calculation (8/1 forward only)."""
        
        df = self.create_test_transaction_detail()
        
        # Run-rate period: 8/1 → 9/30
        run_rate_start = dt.date(2025, 8, 1)
        current_date = dt.date(2025, 9, 30)
        
        kpis = compute_period_kpis(df, run_rate_start, current_date, run_rate_start)
        
        # Should exclude ALL July transactions
        self.assertEqual(kpis['cogs'], 800.0, "Run-rate COGS should only include August")
        self.assertEqual(kpis['legacy_cogs'], 0.0, "Run-rate period has no legacy COGS") 
        self.assertEqual(kpis['overhead'], 300.0, "Run-rate overhead should only include August")
        self.assertEqual(kpis['revenue'], 8000.0, "Run-rate revenue: Aug + Sep")
        
    def test_csv_loading(self):
        """Test loading from actual CSV file format."""
        
        # Create temporary CSV file with QB Transaction Detail format
        csv_content = ''',"Type","Date","Num","Name","Memo","Account","Class","Debit","Credit","Amount","Account Type"
"Jul 1 - Sep 30, 25",,,,,,,,,,,
,"Invoice","08/01/25",,"Test Client","Test revenue","400 - Income",,"",1000.00,-1000.00,"Income"
,"Bill","08/05/25",,"Test Vendor","Test COGS","500 - COGS","",500.00,"",500.00,"Cost of Goods Sold"
,"Bill","07/15/25",,"Legacy Vendor","July COGS","500 - COGS","",200.00,"",200.00,"Cost of Goods Sold"
,"Bill","07/20/25",,"Rent Co","July Rent","600 - Rent","",800.00,"",800.00,"Expense"'''
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            temp_path = f.name
            
        try:
            df = load_transaction_detail(temp_path)
            
            # Verify data loaded correctly
            self.assertGreater(len(df), 0, "Should load transaction rows")
            self.assertIn('date', df.columns, "Should have date column")
            self.assertIn('amount', df.columns, "Should have amount column")
            self.assertIn('account_type', df.columns, "Should have account_type column")
            
            # Test July nuance on loaded data
            kpis = compute_period_kpis(
                df, 
                dt.date(2025, 7, 1),
                dt.date(2025, 8, 31), 
                dt.date(2025, 8, 1)
            )
            
            self.assertEqual(kpis['revenue'], 1000.0, "Should parse revenue correctly")
            self.assertEqual(kpis['cogs'], 500.0, "Should exclude July COGS")
            self.assertEqual(kpis['legacy_cogs'], 200.0, "Should track July COGS separately")
            self.assertEqual(kpis['overhead'], 800.0, "Should include July overhead")
            
        finally:
            os.unlink(temp_path)


if __name__ == '__main__':
    unittest.main()