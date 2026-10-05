"""Item H (apoio) - registra no servidor MLflow do compose a avaliação dos modelos já salvos.

Uso: venv/bin/python medicoes/scripts/H_registrar_mlflow.py
NÃO retreina: carrega models/{lr,dt,rf}_best.pkl, refaz a mesma divisão
treino/teste de train_model.py (test_size=0.2, random_state=42, estratificada),
aplica o scaler salvo e calcula as métricas no conjunto de teste.
"""
import os, sys, time
sys.path.insert(0, "src")
import joblib, mlflow, pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, precision_score, recall_score
from config import Config

cfg = Config()
mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
mlflow.set_experiment(cfg.mlflow_experiment_name)
df = pd.read_csv(cfg.raw_data_path)
X, y = df.drop(columns=cfg.features_to_drop), df[cfg.target_column]
_, X_test, _, y_test = train_test_split(X, y, test_size=cfg.test_size, random_state=cfg.random_state, stratify=y)
X_test = X_test.copy()
X_test[cfg.scale_cols] = joblib.load("models/scaler.pkl").transform(X_test[cfg.scale_cols])
linhas = []
for nome in ("lr", "dt", "rf"):
    caminho = f"models/{nome}_best.pkl"
    modelo = joblib.load(caminho)
    with mlflow.start_run(run_name=f"{nome.upper()}_reavaliacao_modelo_salvo") as run:
        est = modelo.steps[-1][1]
        mlflow.log_params({"model_type": nome, "origem": caminho, "retreinado": False,
                           **{f"model__{k}": v for k, v in est.get_params().items() if k in ("C", "solver", "class_weight", "max_depth", "min_samples_split", "n_estimators")}})
        t0 = time.time()
        pred = modelo.predict(X_test)
        proba = modelo.predict_proba(X_test)[:, 1]
        m = {"roc_auc": roc_auc_score(y_test, proba), "avg_precision": average_precision_score(y_test, proba),
             "f1_score": f1_score(y_test, pred), "precision": precision_score(y_test, pred),
             "recall": recall_score(y_test, pred), "tempo_predicao_teste_s": time.time() - t0}
        mlflow.log_metrics(m)
        mlflow.log_artifact(caminho, artifact_path="model")
        linhas.append({"modelo": nome, "run_id": run.info.run_id, **m})
        print(nome, m)
os.makedirs("medicoes/H_capturas", exist_ok=True)
pd.DataFrame(linhas).to_csv("medicoes/H_capturas/mlflow_runs_registrados.csv", index=False)
