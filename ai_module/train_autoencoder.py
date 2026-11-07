# =============================================================================
# File        : train_autoencoder.py
# Module      : ai_module
# Owner       : Amine
# Description : Fits a StandardScaler on the benign training split, then trains
#               a PyTorch autoencoder (encoder-decoder) on those scaled flows.
#               The autoencoder is then used to detect anomalies in the validation set.
#               The anomaly threshold is chosen as the 95th-percentile MSE on
#               the benign validation split so that all attack days are unseen
#               at threshold-selection time. Exports three artefacts used by the
#               Spark streaming job.
# Input       : data/parquet/train.parquet
#               data/parquet/validation.parquet
# Output      : ai_module/artifacts/autoencoder.onnx
#               ai_module/artifacts/scaler.pkl
#               ai_module/artifacts/threshold.json
# =============================================================================
# ai_module/train_autoencoder.py
# Fit StandardScaler on benign train, train the PyTorch autoencoder, choose the threshold from benign validation error, export autoencoder.onnx, scaler.pkl and threshold.json to ai_module/artifacts/.

import os
import json
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
import pickle

# path to input and output files
DATA_DIR = os.environ.get("DATA_DIR", "data")
PARQUET_DIR = os.path.join(DATA_DIR, "parquet")
ARTIFACTS_DIR = os.environ.get("ARTIFACTS_DIR", "ai_module/artifacts")
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

# Hyperparameters
BATCH_SIZE = 256
EPOCHS = 30
LEARNING_RATE = 1e-3
THRESHOLD_PERCENTILE = 95
RANDOM_SEED = 42

torch.manual_seed(RANDOM_SEED)  
np.random.seed(RANDOM_SEED)

def load_data():
    print("Loading parquet files...")
    train_df = pd.read_parquet(os.path.join(PARQUET_DIR, "train.parquet"))
    validation_df = pd.read_parquet(os.path.join(PARQUET_DIR, "validation.parquet"))
    
    X_train = train_df.drop(columns=["Label"]).values.astype(np.float32)
    X_val   = validation_df.drop(columns=["Label"]).values.astype(np.float32)
    
    print(f"Train data shape: {X_train.shape}")
    print(f"Validation data shape: {X_val.shape}")
    
    return X_train, X_val

def fit_scaler(X_train: np.ndarray) -> StandardScaler:
    print("Fitting StandardScaler on benign training data...")
    scaler = StandardScaler()
    scaler.fit(X_train)
    
    # Save the scaler
    scaler_path = os.path.join(ARTIFACTS_DIR, "scaler.pkl")
    with open(scaler_path, "wb") as f:
        pickle.dump(scaler, f)
    print(f"Scaler saved to {scaler_path}")
    
    return scaler

class Autoencoder(nn.Module):
    def __init__(self, input_dim: int):
        super().__init__()
        
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 16),
            nn.ReLU()
        )
        
        self.decoder = nn.Sequential(
            nn.Linear(16, 32),
            nn.ReLU(),
            nn.Linear(32, 64),
            nn.ReLU(),
            nn.Linear(64, input_dim),
        )
        
    def forward(self, x):
        return self.decoder(self.encoder(x))
        
def train(model: Autoencoder, X_train_scaled: np.ndarray) -> Autoencoder:
    print("Training autoencoder...")
    
    dataset = TensorDataset(torch.tensor(X_train_scaled))
    dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)
    
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
    criterion = nn.MSELoss()
    
    model.train()
    for epoch in range(EPOCHS):
        total_loss = 0.0
        for (batch,) in dataloader:
            optimizer.zero_grad()
            outputs = model(batch)
            loss = criterion(outputs, batch)
            loss.backward()
            optimizer.step()
            total_loss += loss.item() * len(batch)
            
        avg_loss = total_loss / len(X_train_scaled)
        if epoch % 5 == 0 or epoch == 1:
            print(f"  Epoch {epoch:03d}/{EPOCHS}  loss={avg_loss:.6f}")
            
    return model

def compute_threshold(model: Autoencoder, X_val_scaled: np.ndarray) -> float:
    print("Computing anomaly threshold from validation data...")
    
    model.eval()
    with torch.no_grad():
        X_tensor = torch.tensor(X_val_scaled)
        reconstructed = model(X_tensor).numpy()
        
    mse_per_flow = np.mean((X_val_scaled - reconstructed) ** 2, axis=1)
        
    threshold = float(np.percentile(mse_per_flow, THRESHOLD_PERCENTILE)) 
    print(f"  Validation MSE p{THRESHOLD_PERCENTILE}: {threshold:.6f}")
    
    path = os.path.join(ARTIFACTS_DIR, "threshold.json")
    with open(path, "w") as f:
        json.dump({"threshold": threshold, "percentile": THRESHOLD_PERCENTILE}, f)
    print(f"Threshold saved to {path}")
    
    return threshold

def export_onnx(model: Autoencoder, input_dim: int):
    print("Exporting autoencoder to ONNX format...")
    
    model.eval()
    dummy_input = torch.randn(1, input_dim)
    
    path = os.path.join(ARTIFACTS_DIR, "autoencoder.onnx")
    torch.onnx.export(
        model,
        dummy_input,
        path,
        input_names=["input"],
        output_names=["output"],
        dynamic_axes={"input": {0: "batch_size"}, "output": {0: "batch_size"}},
        opset_version=11
    )
    print(f"  Saved → {path}")
    
def main():
    print("=" * 60)
    print("FlowGuard - train_autoencoder.py")
    print("=" * 60)
    
    X_train, X_val = load_data()
    scaler = fit_scaler(X_train)
    X_train_scaled = scaler.transform(X_train).astype(np.float32)
    X_val_scaled = scaler.transform(X_val).astype(np.float32)
    
    input_dim = X_train_scaled.shape[1] 
    model = Autoencoder(input_dim)
    
    model = train(model, X_train_scaled)
    threshold = compute_threshold(model, X_val_scaled)
    export_onnx(model, input_dim)
    
    print("\n" + "=" * 60)
    print(f"Done. Artifacts saved to {ARTIFACTS_DIR}/")
    print("  autoencoder.onnx  —  ONNX model")
    print("  scaler.pkl        —  StandardScaler")
    print(f"  threshold.json    —  MSE threshold = {threshold:.6f}")
    print("=" * 60)
    
if __name__ == "__main__":
    main()