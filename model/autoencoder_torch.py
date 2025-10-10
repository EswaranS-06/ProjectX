import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import os

class Autoencoder(nn.Module):
    def __init__(self, input_dim):
        super(Autoencoder, self).__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, input_dim // 2),
            nn.ReLU(),
            nn.Linear(input_dim // 2, input_dim // 4),
            nn.ReLU()
        )
        self.decoder = nn.Sequential(
            nn.Linear(input_dim // 4, input_dim // 2),
            nn.ReLU(),
            nn.Linear(input_dim // 2, input_dim),
            nn.Sigmoid()
        )

    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded


class AutoencoderTrainer:
    def __init__(self, input_dim):
        if not input_dim:
            raise ValueError("input_dim must be provided")
        self.model = Autoencoder(input_dim)
        self.criterion = nn.MSELoss()
        self.optimizer = optim.Adam(self.model.parameters(), lr=1e-3)

    def train(self, numeric_df, epochs=20):
        X = torch.tensor(numeric_df.values, dtype=torch.float32)
        self.model.train()
        for epoch in range(epochs):
            self.optimizer.zero_grad()
            output = self.model(X)
            loss = self.criterion(output, X)
            loss.backward()
            self.optimizer.step()
            print(f"Epoch {epoch+1}/{epochs}, Loss: {loss.item()}")

        os.makedirs("model/models", exist_ok=True)
        torch.save(self.model.state_dict(), "model/models/autoencoder.pth")
        print("[+] Autoencoder trained and saved.")

    def predict(self, numeric_df):
        X = torch.tensor(numeric_df.values, dtype=torch.float32)
        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(X)
            loss = torch.mean((X - reconstructed) ** 2, dim=1).numpy()
        return {"autoencoder_label": (loss > np.percentile(loss, 90)).astype(int)}
