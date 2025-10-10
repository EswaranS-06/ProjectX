import os
import numpy as np
import pandas as pd

from felog import LogFeaturePipeline
from model.detector import ModelTrainer, ModelPredictor
from model.autoencoder_torch import AutoencoderTrainer


def main():
    # Step 1 — Parse logs & extract features
    p = LogFeaturePipeline(window_seconds=60, enable_logging=False)
    p.ingest_from_file(br"logs\Linux_2k.log")

    df_parsed = p.parse()
    print("Parsed DataFrame sample:")
    print(df_parsed[['timestamp', 'host', 'process', 'message']].head())

    df_features = p.run()
    print("\nWindowed Features sample:")
    print(df_features.head())

    # Step 2 — Keep numeric columns for ML
    numeric_df = df_features.select_dtypes(include=[np.number])

    # Step 3 — Train Autoencoder, Isolation Forest, DBSCAN
    ae_trainer = AutoencoderTrainer(input_dim=numeric_df.shape[1])
    trainer = ModelTrainer()
    trainer.train_all(numeric_df, ae_trainer)

    # Step 4 — Predict anomalies
    predictor = ModelPredictor(ae_trainer=ae_trainer)
    ensemble_df = predictor.ensemble_predict(numeric_df)

    # Step 5 — Append ML results to features
    output_df = df_features.copy()  # keep extra window features
    output_df['isolation_forest_label'] = predictor.predict(numeric_df, model_name="isolation_forest")["isolation_forest_label"]
    output_df['dbscan_label'] = predictor.predict(numeric_df, model_name="dbscan")["dbscan_label"]
    output_df['autoencoder_label'] = predictor.predict(numeric_df, model_name="autoencoder")["autoencoder_label"]
    output_df['ensemble_anomaly'] = ensemble_df['ensemble_anomaly']

    # Step 6 — Save results
    output_path = "anomaly_results_with_features.csv"
    output_df.to_csv(output_path, index=False)
    print(f"\n✅ Saved anomaly results with features to: {output_path}")

    # Step 7 — Summary
    print("\n--- Anomaly Detection Summary ---")
    print(f"Total records: {len(output_df)}")
    print(f"Total anomalies found: {output_df['ensemble_anomaly'].sum()}")
    print("\nEnsemble anomaly distribution:")
    print(output_df['ensemble_anomaly'].value_counts())
    print("\nSample anomaly predictions:")
    print(output_df.head(20))


if __name__ == "__main__":
    main()
