# =============================================================================
# File        : train_baselines.py
# Module      : ai_module
# Owner       : Amine
# Description : Trains three offline baseline models on the same benign
#               train/validation split used by the autoencoder:
#                 - Isolation Forest (unsupervised)
#                 - One-Class SVM   (unsupervised)
#                 - Random Forest   (supervised upper bound, uses attack labels)
#               Results are saved for comparison in evaluate.py.
# Input       : data/parquet/train.parquet
#               data/parquet/validation.parquet
#               data/parquet/test_wednesday.parquet  (attack labels for RF)
#               data/parquet/test_friday.parquet
# Output      : ai_module/artifacts/iforest.pkl
#               ai_module/artifacts/ocsvm.pkl
#               ai_module/artifacts/rf.pkl
# =============================================================================
# ai_module/train_baselines.py
# Offline baselines on the same split: Isolation Forest, One-Class SVM, and Random Forest as a supervised upper bound.
