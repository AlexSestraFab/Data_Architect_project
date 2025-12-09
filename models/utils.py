import pandas as pd
import hashlib

def generate_hash_key(value):
    
    if pd.isna(value):
        return None
    
    return hashlib.sha256(str(value).encode('utf-8')).hexdigest()