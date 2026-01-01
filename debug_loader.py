import pandas as pd
from src.data_loader import load_ledger
from pathlib import Path

def debug_load():
    file_path = Path("Bank Ledger through 11142025.csv")
    print(f"Loading {file_path}...")
    
    # 1. First, let's inspect the raw file content first few lines
    print("\n--- First 10 lines of raw file ---")
    with open(file_path, 'r') as f:
        for i in range(10):
            print(repr(f.readline()))
            
    # 2. Now try our loader
    print("\n--- Running load_ledger ---")
    # We need to mock the Streamlit file uploader object since load_ledger expects it
    # It essentially needs to have a 'name' attribute and be readable
    class MockFile:
        def __init__(self, path):
            self.path = path
            self.name = str(path)
            
        def __getattr__(self, attr):
             # Delegate to file object for read methods
             return getattr(open(self.path, 'rb'), attr)
             
    # Actually, load_ledger calls pd.read_csv(uploaded_file). 
    # If we pass a file path string to read_csv, it works. 
    # But load_ledger checks the 'name' attribute to determine extension.
    # So let's just pass the MockFile which wraps the path, but we might need to actually open it
    # if read_csv expects a file-like object when not a path string.
    # BUT, our load_ledger uses 'uploaded_file' directly in read_csv.
    # If uploaded_file is a Streamlit UploadedFile, it acts like a BytesIO.
    # If we pass an open file object, it should work.
    
    with open(file_path, 'rb') as f:
        # Create a wrapper to add the .name attribute which load_ledger checks
        class FileWrapper:
            def __init__(self, file_obj, name):
                self.file_obj = file_obj
                self.name = name
            
            def read(self, *args, **kwargs):
                return self.file_obj.read(*args, **kwargs)
                
            def seek(self, *args, **kwargs):
                return self.file_obj.seek(*args, **kwargs)
            
            def tell(self, *args, **kwargs):
                return self.file_obj.tell(*args, **kwargs)
                
            def __iter__(self):
                return self.file_obj.__iter__()

        wrapped_file = FileWrapper(f, "Bank Ledger through 11142025.csv")
        
        try:
            df = load_ledger(wrapped_file)
            
            if df is not None:
                print(f"\nSUCCESS: DataFrame loaded with {len(df)} rows")
                print("\nColumns found:", df.columns.tolist())
                print("\nFirst 5 rows:")
                print(df.head())
                print("\nNon-null counts:")
                print(df.count())
            else:
                print("\nFAILURE: load_ledger returned None")
                
        except Exception as e:
            print(f"\nERROR running load_ledger: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    debug_load()
