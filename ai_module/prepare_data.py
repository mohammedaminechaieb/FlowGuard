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
