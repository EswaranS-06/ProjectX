import os
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.cluster import DBSCAN


class ModelTrainer:
    def __init__(self, model_dir=None):
        base_path = os.path.dirname(os.path.abspath(__file__))
        self.model_dir = model_dir or os.path.join(base_path, "models")
        os.makedirs(self.model_dir, exist_ok=True)

    def _get_model_path(self, model_name: str):
        return os.path.join(self.model_dir, f"{model_name}.pkl")

    def _prepare_features(self, df: pd.DataFrame):
        numeric_df = df.select_dtypes(include=['number'])
        return numeric_df.dropna()

    def train_all(self, features_df: pd.DataFrame):
        features_df = self._prepare_features(features_df)
        models = {}

        iso_model = IsolationForest(contamination=0.05, random_state=42)
        iso_model.fit(features_df)
        joblib.dump(iso_model, self._get_model_path("isolation_forest"))
        models["isolation_forest"] = iso_model

        svm_model = OneClassSVM(kernel='rbf', nu=0.05)
        svm_model.fit(features_df)
        joblib.dump(svm_model, self._get_model_path("one_class_svm"))
        models["one_class_svm"] = svm_model

        dbscan_model = DBSCAN(eps=0.5, min_samples=5)
        models["dbscan"] = dbscan_model

        print(f"[+] Models trained and saved in: {self.model_dir}")
        return models


class ModelPredictor:
    def __init__(self, model_dir=None):
        base_path = os.path.dirname(os.path.abspath(__file__))
        self.model_dir = model_dir or os.path.join(base_path, "models")
        self.models = {}
        self._load_models()

    def _get_model_path(self, model_name: str):
        return os.path.join(self.model_dir, f"{model_name}.pkl")

    def _prepare_features(self, df: pd.DataFrame):
        numeric_df = df.select_dtypes(include=['number'])
        return numeric_df.dropna()

    def _load_models(self):
        expected_models = ["isolation_forest", "one_class_svm"]
        for m in expected_models:
            model_path = self._get_model_path(m)
            if os.path.exists(model_path):
                self.models[m] = joblib.load(model_path)
            else:
                print(f"[!] Warning: {m} model not found, please train it first.")
        self.models["dbscan"] = DBSCAN(eps=0.5, min_samples=5)

    def predict(self, features_df: pd.DataFrame, model_name: str):
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not available.")
        model = self.models[model_name]
        features_df_numeric = self._prepare_features(features_df)
        df_out = features_df.copy()

        if model_name == "isolation_forest":
            labels = model.predict(features_df_numeric)
            scores = model.decision_function(features_df_numeric)

        elif model_name == "one_class_svm":
            labels = model.predict(features_df_numeric)
            scores = model.decision_function(features_df_numeric)

        elif model_name == "dbscan":
            cluster_labels = model.fit_predict(features_df_numeric)
            labels = np.where(cluster_labels == -1, -1, 1)
            scores = -1 * (cluster_labels == -1).astype(int)

        df_out[f"{model_name}_label"] = labels
        df_out[f"{model_name}_score"] = scores
        return df_out

    def ensemble_predict(self, features_df: pd.DataFrame):
        df_out = features_df.copy()
        label_arrays = []

        for model_name in ["isolation_forest", "one_class_svm", "dbscan"]:
            pred_df = self.predict(features_df, model_name=model_name)
            label_arrays.append(pred_df[f"{model_name}_label"].replace({-1: 1, 1: 0}).values)
            df_out[f"{model_name}_label"] = pred_df[f"{model_name}_label"]
            df_out[f"{model_name}_score"] = pred_df[f"{model_name}_score"]

        label_arrays = np.array(label_arrays)
        final = (np.sum(label_arrays, axis=0) >= 1).astype(int)
        df_out["ensemble_anomaly"] = final
        return df_out

    def save_results(self, df: pd.DataFrame, file_name="anomaly_results.csv"):
        """
        Save DataFrame results to CSV.
        """
        path = os.path.join(self.model_dir, file_name)
        df.to_csv(path, index=False)
        print(f"[+] Results saved to: {path}")
