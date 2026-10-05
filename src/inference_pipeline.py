import warnings
warnings.filterwarnings("ignore")

import os
import sys
import time
import joblib
import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config import Config
from metrics_exporter import MetricsServer, metrics

cfg = Config()

MODEL_PATH = os.path.join(cfg.models_dir, "best_model.pkl")
SCALER_PATH = os.path.join(cfg.models_dir, "scaler.pkl")
FEATURE_COLS = [
    "Time", "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9",
    "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18",
    "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27",
    "V28", "Amount"
]


class FraudDetectionPipeline:
    def __init__(self, enable_metrics: bool = False):
        self.model = joblib.load(MODEL_PATH)
        self.scaler = joblib.load(SCALER_PATH)
        self.scale_cols = cfg.scale_cols
        self.enable_metrics = enable_metrics
        print(f"Modelo: {type(self.model).__name__}")
        if enable_metrics:
            metrics.start()
            print("  -> Métricas Prometheus ativadas")

    def predict_batch(self, df: pd.DataFrame) -> pd.DataFrame:
        df_scaled = df.copy()
        df_scaled[self.scale_cols] = self.scaler.transform(df_scaled[self.scale_cols])
        X = df_scaled[FEATURE_COLS].values

        result = df[["Time", "Amount"]].copy()
        result["prediction"] = self.model.predict(X)
        result["fraud_probability"] = self.model.predict_proba(X)[:, 1]
        result["alert"] = result["prediction"].apply(
            lambda x: "🚨 FRAUDE" if x == 1 else "✅ NORMAL"
        )

        if self.enable_metrics:
            for _, row in result.iterrows():
                metrics.record_prediction(
                    prediction=row["prediction"],
                    probability=row["fraud_probability"],
                    amount=row["Amount"],
                    latency=0.0
                )

        return result

    def predict_row(self, row: pd.Series) -> dict:
        df = pd.DataFrame([row])
        result = self.predict_batch(df)
        return result.iloc[0].to_dict()

    def run_streaming_simulation(self, df: pd.DataFrame, n_samples: int = 5000, delay: float = 0.005):
        print(f"\n--- STREAMING SIMULATION ({n_samples} transações) ---\n")
        df_sample = df.sample(n=n_samples, random_state=42).reset_index(drop=True)
        start = time.time()
        frauds_found = 0

        batch_size = 100
        for i in range(0, n_samples, batch_size):
            batch = df_sample.iloc[i:i + batch_size]
            t0 = time.time()
            results = self.predict_batch(batch)
            batch_latency = time.time() - t0

            if self.enable_metrics:
                for _, res in results.iterrows():
                    metrics.record_prediction(
                        prediction=res["prediction"],
                        probability=res["fraud_probability"],
                        amount=res["Amount"],
                        latency=batch_latency / len(batch)
                    )

            for _, res in results.iterrows():
                if res["prediction"] == 1:
                    frauds_found += 1
                    idx = i + results.index.get_loc(res.name)
                    print(
                        f"[{idx:6d}] ${res['Amount']:.2f} | {res['alert']} | "
                        f"Prob: {res['fraud_probability']:.4f}"
                    )

            if delay > 0:
                time.sleep(delay)

        elapsed = time.time() - start
        print(f"\nProcessadas {n_samples} em {elapsed:.1f}s | "
              f"Fraudes: {frauds_found} | "
              f"Taxa: {n_samples/elapsed:.0f} msg/s")

    def run_batch(self, df: pd.DataFrame):
        print(f"\n--- BATCH PROCESSING ({len(df)} transações) ---\n")
        start = time.time()
        results = self.predict_batch(df)
        elapsed = time.time() - start

        frauds = results[results["prediction"] == 1]
        print(f"Tempo: {elapsed:.1f}s | "
              f"Fraudes: {len(frauds)} | "
              f"Taxa: {len(df)/elapsed:.0f} tps")
        return results

    def run_continuous_stream(self, df: pd.DataFrame, batch_size: int = 100, delay: float = 1.0):
        print(f"\n--- CONTINUOUS STREAM (com métricas Prometheus) ---")
        print(f"Servidor de métricas em http://localhost:8000/metrics\n")

        df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)
        n = len(df_shuffled)
        i = 0

        try:
            while True:
                batch = df_shuffled.iloc[i:i + batch_size]
                if len(batch) == 0:
                    i = 0
                    continue

                t0 = time.time()
                results = self.predict_batch(batch)
                elapsed = time.time() - t0

                frauds = results[results["prediction"] == 1]
                print(
                    f"[{time.strftime('%H:%M:%S')}] Batch {i//batch_size + 1}: "
                    f"{len(batch)} transações | "
                    f"{len(frauds)} fraudes | "
                    f"{elapsed*1000:.1f}ms"
                )

                for _, res in frauds.iterrows():
                    metrics.record_prediction(
                        prediction=res["prediction"],
                        probability=res["fraud_probability"],
                        amount=res["Amount"],
                        latency=elapsed / len(batch)
                    )

                metrics.set_stream_lag(n - i)
                i = (i + batch_size) % n
                time.sleep(delay)

        except KeyboardInterrupt:
            print("\nStream encerrado.")

    def validate(self, df: pd.DataFrame, n_samples: int = 50000):
        from sklearn.metrics import (
            classification_report, confusion_matrix,
            roc_auc_score, average_precision_score
        )

        print(f"\n--- VALIDAÇÃO ({n_samples} amostras) ---")
        df_sample = df.sample(n=n_samples, random_state=42).reset_index(drop=True)
        y_true = df_sample["Class"].values

        start = time.time()
        results = self.predict_batch(df_sample)
        elapsed = time.time() - start

        y_pred = results["prediction"].values
        y_proba = results["fraud_probability"].values

        print(f"Tempo: {elapsed:.1f}s | Taxa: {n_samples/elapsed:.0f} tps")
        print(f"ROC-AUC: {roc_auc_score(y_true, y_proba):.4f}")
        print(f"Avg Precision: {average_precision_score(y_true, y_proba):.4f}")
        print(f"\nClassification Report:")
        print(classification_report(y_true, y_pred, target_names=["Normal", "Fraude"]))
        print(f"Confusion Matrix:")
        print(confusion_matrix(y_true, y_pred))


def main():
    print("=" * 50)
    print("PIPELINE DE DETECÇÃO DE FRAUDES")
    print("Arquitetura Big Data + MLOps")
    print("=" * 50)

    df = pd.read_csv(cfg.raw_data_path)
    print(f"Dataset: {len(df)} transações, {df['Class'].sum()} fraudes "
          f"({df['Class'].mean()*100:.4f}%)")

    pipeline = FraudDetectionPipeline(enable_metrics=True)

    while True:
        print("\n" + "-" * 40)
        print("1 - Simular streaming (5k amostras)")
        print("2 - Processamento batch completo")
        print("3 - Validação (50k amostras)")
        print("4 - Stream contínuo (com métricas)")
        print("0 - Sair")
        choice = input("Opção: ").strip()

        if choice == "1":
            pipeline.run_streaming_simulation(df)
        elif choice == "2":
            pipeline.run_batch(df)
        elif choice == "3":
            pipeline.validate(df)
        elif choice == "4":
            pipeline.run_continuous_stream(df)
        elif choice == "0":
            break


if __name__ == "__main__":
    main()
