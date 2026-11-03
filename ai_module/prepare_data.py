# =============================================================================
# File        : prepare_data.py
# Module      : ai_module
# Owner       : Amine
# Description : Loads the raw CICIDS2017 CSV files, cleans them (drops inf/NaN
#               values, removes duplicates, normalises column names), then splits
#               the Monday benign capture into a train set and a validation set.
#               The cleaned data is written as Parquet to HDFS or to
#               data/parquet/ when running locally.
# Input       : data/*.csv  (CICIDS2017 raw files — not committed)
# Output      : data/parquet/train.parquet
#               data/parquet/validation.parquet
# =============================================================================
# ai_module/prepare_data.py
# Load CICIDS2017 CSVs, clean (inf/NaN, duplicates, column names), split Monday benign into train/validation, write Parquet to HDFS (or data/parquet locally).
import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

DATA_DIR = os.environ.get("DATA_DIR", "data")   
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)

DAY_FILES = {
    "monday": ["Monday-WorkingHours.pcap_ISCX.csv"],
    "tuesday": ["Tuesday-WorkingHours.pcap_ISCX.csv"],
    "wednesday": ["Wednesday-workingHours.pcap_ISCX.csv"],
    "thursday": ["Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv",
                "Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv"],
    "friday": ["Friday-WorkingHours-Morning.pcap_ISCX.csv", 
            "Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv",
            "Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv"],
}

RANDOM_STATE = 42
VAL_SIZE = 0.2

def clean(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    #1 strip spaces from column names
    df.columns = df.columns.str.strip()
    
    #2 Replace inf/-inf with NaN and drop rows with NaN values
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    #3 Drop rows with NaN values
    before_drop = len(df)
    df.dropna(inplace=True)
    print(f"[{filename}] NaN dropped: {before_drop - len(df):,}")
    
    #4 Drop duplicate rows
    before_drop = len(df)
    df.drop_duplicates(inplace=True)
    print(f"[{filename}] Duplicates dropped: {before_drop - len(df):,}")
    print(f"    [{filename}] Remaining: {len(df):,} rows")

    return df

def load_day(day: str, filenames: list) -> pd.DataFrame:
    dfs = []
    for filename in filenames:
        filepath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filepath):
            print(f"  [SKIP] {filename} not found.")
            continue
        print(f"Loading {filepath}...")
        df = pd.read_csv(filepath, low_memory=False)
        df = clean(df, filename)
        dfs.append(df)
    
    if not dfs:
        print(f"No data loaded for {day}.")
        return pd.DataFrame()  # Return an empty DataFrame if no files were loaded
    
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"[{day}] Combined DataFrame shape: {combined_df.shape}")
    return combined_df

def save_parquet(df: pd.DataFrame, filename: str) -> None:
    parquet_path = os.path.join(PARQUET_DIR, f"{filename}.parquet")
    df.to_parquet(parquet_path, index=False)
    print(f"Saved -> {parquet_path} ({len(df):,} rows)")
    
def main():
    print("=" * 60)
    print("FlowGuard - prepare_data.py")
    print("=" * 60)
    
    for day, filenames in DAY_FILES.items():
        print(f"\n[{day.upper()}]")
        df = load_day(day, filenames)
        if df.empty:
            print(f"No data to save for {day}.")
            continue
        
        if day == "monday":
            # Monday is benign-only — train + validation split
            benign = df[df['Label'] == 'BENIGN']
            print(f"  Benign rows: {len(benign):,}")
            
            train_df, val_df = train_test_split(
                benign, 
                test_size=VAL_SIZE, 
                random_state=RANDOM_STATE
            )
            print(f"  Train rows: {len(train_df):,}, Validation rows: {len(val_df):,}")
            save_parquet(train_df, "train")
            save_parquet(val_df, "validation")        
        else:
            # Other days may contain attacks — save the entire DataFrame
            print(f"  Label counts:\n{df['Label'].value_counts().to_string()}")
            save_parquet(df, f"test_{day}")
            
        print("\n" + "=" * 60)
        print("Done.")
        print("=" * 60)
        
if __name__ == "__main__":
    main()
            