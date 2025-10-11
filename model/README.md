# 🔍 Log Anomaly Detection (Hybrid ML Model)

This project implements a **hybrid anomaly detection system** that identifies unusual patterns in system logs using a combination of **unsupervised machine learning models** — Isolation Forest, DBSCAN, and a Neural Autoencoder (PyTorch).  
It integrates statistical log features, time windowing, and model-based anomaly detection for robust cybersecurity insights.

---

## 📁 Project Structure

```
ProjectX/
│
├── main.py                      # Main entry point (log ingestion → feature extraction → training → prediction)
│
├── felog/                       # Log feature pipeline (parsing, windowing, feature extraction)
│
├── model/
│   ├── autoencoder_torch.py      # Deep autoencoder for log reconstruction error detection
│   ├── detector.py               # IsolationForest + DBSCAN + Autoencoder + Ensemble logic
│   └── models/                   # Saved trained model files (.pth, .pkl)
│
├── logs/
│   └── Linux_2k.log             # Example log dataset
│
└── anomaly_results_with_features.csv  # Output file (features + anomaly labels)
```

---

## ⚙️ Features Extracted per Window

Each log window (e.g., 60 seconds) is converted into numeric features:

| Feature | Description |
|----------|-------------|
| `window_start` | Start time of the time window |
| `window_end` | End time of the time window |
| `event_count` | Number of log events within the window |
| `unique_messages` | Count of distinct log message texts |
| `distinct_hosts` | Number of unique hostnames |
| `distinct_processes` | Number of unique process names |
| `avg_msg_length` | Average message text length |
| `failed_auth_count` | Count of “failed password” occurrences |
| `invalid_user_count` | Count of “invalid user” messages |
| `entropy_tokens` | Token entropy (complexity measure of messages) |

---

## 🧩 Models Used

| Model | Type | Role |
|--------|------|------|
| **Isolation Forest** | Tree ensemble | Detects sparse anomalies in feature space |
| **DBSCAN** | Density-based clustering | Identifies noise/outlier points not part of any cluster |
| **Autoencoder** | Neural network | Learns to reconstruct normal log patterns; high reconstruction loss = anomaly |
| **Ensemble Layer** | Rule-based | Marks a log window anomalous if ≥ 2 models agree it’s abnormal |

---

## 🚀 How It Works

### 1️⃣ Data Ingestion & Parsing
- Raw logs are read using the **`LogFeaturePipeline`**.
- Logs are grouped into **time windows** (default: 60 seconds).
- Each window produces statistical & semantic features.

### 2️⃣ Feature Normalization
- Numeric features are standardized using **`StandardScaler`**.
- This improves stability for models like Autoencoder and DBSCAN.

### 3️⃣ Model Training (`ModelTrainer`)
- **Isolation Forest** and **DBSCAN** are fitted on the scaled features.
- **Autoencoder** is trained with reconstruction loss using PyTorch.
- All models are saved in `model/models/`.

### 4️⃣ Anomaly Prediction (`ModelPredictor`)
- Each model predicts anomaly scores independently.
- Ensemble combines results:
  ```
  ensemble_anomaly = 1 if (IF + DBSCAN + AE) ≥ 2
  ```

### 5️⃣ Output
- Final labeled dataset saved as:
  ```
  anomaly_results_with_features.csv
  ```

---

## 📊 Output Columns

| Column | Description |
|---------|-------------|
| *(All original feature columns)* | From the feature pipeline |
| `isolation_forest_label` | 1 = anomaly detected by IsolationForest |
| `dbscan_label` | 1 = anomaly detected by DBSCAN |
| `autoencoder_label` | 1 = anomaly detected by Autoencoder |
| `ensemble_anomaly` | 1 = majority vote anomaly |

---

## 🧪 Example Run

```bash
# Activate environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install torch scikit-learn pandas numpy

# Run pipeline
python main.py
```

You’ll see training progress for the autoencoder:

```
Epoch 1/20, Loss: 0.085
Epoch 2/20, Loss: 0.063
...
[+] Autoencoder trained and saved.
[+] Models trained and saved.
✅ Saved anomaly results with features to: anomaly_results_with_features.csv
```

---

## 📈 Visualization

You can visualize anomalies later:

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("anomaly_results_with_features.csv")

plt.figure(figsize=(10,5))
plt.plot(df.index, df['ensemble_anomaly'], 'r.', label='Anomalies')
plt.title("Detected Anomalies per Window")
plt.xlabel("Window Index")
plt.ylabel("Anomaly (1=Yes)")
plt.legend()
plt.show()
```

---

## 🧠 Troubleshooting

| Issue | Possible Fix |
|--------|---------------|
| `AttributeError: 'numpy.ndarray' object has no attribute 'values'` | Ensure scaling step returns a DataFrame, not array |
| Anomalies seem under-detected | Try lowering Autoencoder threshold or IsolationForest contamination |
| DBSCAN not detecting outliers | Tune `eps` and `min_samples` parameters |
| PyTorch errors | Verify `input_dim` > 0 and model folder path exists |

---

## 🛡️ Future Enhancements

- Incremental learning with online log streams  
- BERT embeddings for semantic anomaly detection  
- Visualization dashboard using Streamlit  
- Dynamic threshold optimization

---

**Author:** Jeevitha  
**Version:** 1.0   
