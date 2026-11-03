# app/core/data_loader.py
# Reads a CICIDS2017 CSV (or a sample of it) for the app: strips column-name spaces, removes
# inf and NaN values, keeps the feature columns in the training order, returns the label column
# separately. Same cleaning rules as ai_module/prepare_data.py.
