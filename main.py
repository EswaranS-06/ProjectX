# main.py

from felog import LogParser, FeatureEngineering
from model.detector import ModelTrainer, ModelPredictor
import os

if __name__ == "__main__":
    # --- Step 1: Parse Logs ---
    log_path =br"C:\Users\JEEVI\Desktop\ProjectX\logs" #os.path.join(os.getcwd(), "logs", "Linux_2k.log")
    parser = LogParser()
    print(f"Reading logs from file: {log_path}")
    parser.from_folder(log_path)
    df_parsed = parser.normalize()

    print(f"Normalization complete. Parsed: {df_parsed.shape[0]} entries")

    # --- Step 2: Feature Engineering ---
    fe = FeatureEngineering(df_parsed, window_seconds=60)
    features_df = fe.get_features()

    print(f"Generated features: {features_df.shape}")

    # --- Step 3: Train Models ---
    trainer = ModelTrainer()
    trainer.train_all(features_df)

    # --- Step 4: Load Models & Predict ---
    predictor = ModelPredictor()

    # Individual predictions
    for model_name in ["isolation_forest", "one_class_svm", "dbscan"]:
        print(f"Running prediction with {model_name}...")
        result_df = predictor.predict(features_df, model_name=model_name)
        print(result_df[[f"{model_name}_label", f"{model_name}_score"]].head())

    


    # Ensemble prediction
    print("\nRunning ensemble prediction...")
    ensemble_df = predictor.ensemble_predict(features_df)
    print(ensemble_df[["ensemble_anomaly"]].head())

    
     # Step 6 — Save results
    predictor.save_results(ensemble_df, file_name="anomaly_results.csv")