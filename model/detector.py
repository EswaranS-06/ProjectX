from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
import numpy as np
import pickle
from model.autoencoder_torch import AutoencoderTrainer


class ModelTrainer:
    def __init__(self):
        self.models = {}

    def train_all(self, features_df, ae_trainer):
        # Isolation Forest
        from sklearn.ensemble import IsolationForest
        iso_forest = IsolationForest(contamination=0.1)
        iso_forest.fit(features_df)
        self.models["isolation_forest"] = iso_forest

        # DBSCAN
        from sklearn.cluster import DBSCAN
        dbscan = DBSCAN(eps=0.5, min_samples=5)
        dbscan.fit(features_df)
        self.models["dbscan"] = dbscan

        # Autoencoder
        ae_trainer.train(features_df, epochs=20)
        self.models["autoencoder"] = ae_trainer
        print("[+] Models trained and saved.")

    def get_model(self, name):
        return self.models.get(name, None)



class ModelPredictor:
    def __init__(self, ae_trainer):
        self.ae_trainer = ae_trainer
        with open("model/models/isolation_forest.pkl", "rb") as f:
            self.isolation_forest = pickle.load(f)
        with open("model/models/dbscan.pkl", "rb") as f:
            self.dbscan = pickle.load(f)

    def predict(self, numeric_df, model_name):
        if model_name == "isolation_forest":
            labels = self.isolation_forest.predict(numeric_df)
            return {"isolation_forest_label": (labels == -1).astype(int)}

        elif model_name == "dbscan":
            labels = self.dbscan.fit_predict(numeric_df)
            return {"dbscan_label": (labels == -1).astype(int)}

        elif model_name == "autoencoder":
            return self.ae_trainer.predict(numeric_df)

    def ensemble_predict(self, numeric_df):
        iso = self.predict(numeric_df, "isolation_forest")["isolation_forest_label"]
        db = self.predict(numeric_df, "dbscan")["dbscan_label"]
        ae = self.predict(numeric_df, "autoencoder")["autoencoder_label"]

        ensemble = (iso + db + ae) >= 2  # majority voting
        return {"ensemble_anomaly": ensemble.astype(int)}
